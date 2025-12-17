from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Dict, List, Optional, Set
from zoneinfo import ZoneInfo

from core.ops_db import OpsDB


@dataclass(frozen=True)
class QuietDecision:
    allowed: bool
    reason: Optional[str]
    rule_id: Optional[int]


class QuietWindowsService:
    """Централизованный фильтр 'окна тишины' для роботов.

    Хранит правила в robot_ops.db (операционная БД робота) и отвечает на вопрос:
      - можно ли сейчас роботу отправить приказ на вход/выход.

    Поддерживаемые типы правил:
      - daily: повторяющееся окно по дням недели и локальному времени (с TZ)
      - once:  разовое окно по UTC интервалу

    Таблица: quiet_windows (в robot_ops.db).
    """

    def __init__(self, *, ops_db: OpsDB, cache_ttl_seconds: float = 30.0) -> None:
        self.ops_db = ops_db
        self.cache_ttl_seconds = float(cache_ttl_seconds)

        self._cache_loaded_at: Optional[datetime] = None
        self._rules_by_robot: Dict[str, List[dict]] = {}

    # -------------------------
    # Schema / seeding
    # -------------------------

    async def ensure_schema(self) -> None:
        await self.ops_db.execute(
            """
            CREATE TABLE IF NOT EXISTS quiet_windows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                enabled INTEGER NOT NULL DEFAULT 1,

                robot_id TEXT NOT NULL,          -- конкретный robot_id или '*'
                rule_kind TEXT NOT NULL,         -- 'daily' | 'once'

                tz TEXT,                         -- IANA TZ (для daily), напр. 'America/New_York'
                weekdays TEXT,                   -- '0,1,2,3,4' (Mon=0..Sun=6). NULL => любые дни
                start_time TEXT,                 -- 'HH:MM' (для daily)
                end_time TEXT,                   -- 'HH:MM' (для daily)

                start_utc TEXT,                  -- ISO8601 (для once), напр. '2025-12-03T15:00:00+00:00'
                end_utc TEXT,                    -- ISO8601 (для once)

                block_entries INTEGER NOT NULL DEFAULT 1,
                block_exits INTEGER NOT NULL DEFAULT 0,

                reason TEXT NOT NULL,
                note TEXT
            );
            """,
            commit=True,
        )
        await self.ops_db.execute(
            "CREATE INDEX IF NOT EXISTS idx_quiet_robot ON quiet_windows(robot_id);",
            commit=True,
        )

        self._cache_loaded_at = None
        self._rules_by_robot = {}

    async def seed_default_rth_open(self, *, robot_id: str) -> None:
        """Сидируем базовое правило RTH open, если для robot_id нет ни одного правила.

        Это нужно, чтобы перенос quiet windows из стратегии в оркестратор не требовал ручной миграции сразу.
        Если ты добавишь правила вручную — сидер больше ничего не тронет.
        """
        row = await self.ops_db.fetchone(
            "SELECT COUNT(1) FROM quiet_windows WHERE robot_id = ?;",
            (robot_id,),
        )
        if row and int(row[0]) > 0:
            return

        await self.ops_db.execute(
            """
            INSERT INTO quiet_windows (
                enabled, robot_id, rule_kind, tz, weekdays, start_time, end_time,
                block_entries, block_exits, reason, note
            )
            VALUES (?, ?, 'daily', ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                1,
                robot_id,
                "America/New_York",
                "0,1,2,3,4",
                "09:30",
                "10:00",
                1,
                0,
                "quiet_main_session_open",
                "Первые 30 минут после старта основной сессии (RTH open).",
            ),
            commit=True,
        )

        # invalidate cache
        self._cache_loaded_at = None
        self._rules_by_robot = {}

    # -------------------------
    # Public API
    # -------------------------

    async def evaluate(self, *, robot_id: str, now_utc: datetime, action_type: str) -> QuietDecision:
        """action_type: 'entry' | 'exit'"""
        if now_utc.tzinfo is None:
            raise ValueError("now_utc must be timezone-aware (UTC)")

        if action_type not in {"entry", "exit"}:
            raise ValueError("action_type must be 'entry' or 'exit'")

        await self._ensure_cache(now_utc)

        rules: list[dict] = []
        # '*' - общие правила, затем конкретные
        rules.extend(self._rules_by_robot.get("*", []))
        rules.extend(self._rules_by_robot.get(robot_id, []))

        for r in rules:
            if not r["enabled"]:
                continue

            if action_type == "entry" and not r["block_entries"]:
                continue
            if action_type == "exit" and not r["block_exits"]:
                continue

            if self._rule_is_active(r, now_utc):
                return QuietDecision(allowed=False, reason=r.get("reason"), rule_id=r.get("id"))

        return QuietDecision(allowed=True, reason=None, rule_id=None)

    async def get_enabled_rules_for_robot(
        self,
        *,
        robot_id: str,
        now_utc: datetime,
        include_global: bool = True,
    ) -> list[dict]:
        """Вернуть список включённых правил (enabled=1) для робота.

        Используется для логов/диагностики. Возвращает правила из кэша сервиса.
        include_global=True добавляет правила для robot_id='*' (общие).
        """
        if now_utc.tzinfo is None:
            raise ValueError("now_utc must be timezone-aware (UTC)")

        await self._ensure_cache(now_utc)

        rules: list[dict] = []
        if include_global:
            rules.extend(self._rules_by_robot.get("*", []))
        rules.extend(self._rules_by_robot.get(robot_id, []))

        # Защитимся от случайных внешних мутаций.
        return [dict(r) for r in rules]

    # -------------------------
    # Cache / load
    # -------------------------

    async def _ensure_cache(self, now_utc: datetime) -> None:
        if self._cache_loaded_at is None:
            await self._load_cache()
            self._cache_loaded_at = now_utc
            return

        age = (now_utc - self._cache_loaded_at).total_seconds()
        if age >= self.cache_ttl_seconds:
            await self._load_cache()
            self._cache_loaded_at = now_utc

    async def _load_cache(self) -> None:
        rows = await self.ops_db.fetchall(
            """
            SELECT
                id, enabled, robot_id, rule_kind,
                tz, weekdays, start_time, end_time,
                start_utc, end_utc,
                block_entries, block_exits,
                reason, note
            FROM quiet_windows
            WHERE enabled = 1;
            """
        )

        rules_by_robot: Dict[str, List[dict]] = {}
        for row in rows:
            (
                rid,
                enabled,
                robot_id,
                rule_kind,
                tz,
                weekdays,
                start_time_s,
                end_time_s,
                start_utc_s,
                end_utc_s,
                block_entries,
                block_exits,
                reason,
                note,
            ) = row

            rule = {
                "id": int(rid),
                "enabled": bool(enabled),
                "robot_id": str(robot_id),
                "rule_kind": str(rule_kind),
                "tz": str(tz) if tz else None,
                "weekdays": self._parse_weekdays(weekdays),
                "start_time": self._parse_hhmm(start_time_s) if start_time_s else None,
                "end_time": self._parse_hhmm(end_time_s) if end_time_s else None,
                "start_utc": self._parse_iso_dt(start_utc_s) if start_utc_s else None,
                "end_utc": self._parse_iso_dt(end_utc_s) if end_utc_s else None,
                "block_entries": bool(block_entries),
                "block_exits": bool(block_exits),
                "reason": str(reason) if reason else None,
                "note": str(note) if note else None,
            }
            rules_by_robot.setdefault(rule["robot_id"], []).append(rule)

        self._rules_by_robot = rules_by_robot

    # -------------------------
    # Matching
    # -------------------------

    def _rule_is_active(self, rule: dict, now_utc: datetime) -> bool:
        kind = rule.get("rule_kind")
        if kind == "daily":
            return self._is_active_daily(rule, now_utc)
        if kind == "once":
            return self._is_active_once(rule, now_utc)
        return False

    def _is_active_once(self, rule: dict, now_utc: datetime) -> bool:
        start_dt = rule.get("start_utc")
        end_dt = rule.get("end_utc")
        if start_dt is None or end_dt is None:
            return False
        return start_dt <= now_utc < end_dt

    def _is_active_daily(self, rule: dict, now_utc: datetime) -> bool:
        tz_name = rule.get("tz") or "UTC"
        tz = ZoneInfo(tz_name)

        dt_local = now_utc.astimezone(tz)

        weekdays: Optional[Set[int]] = rule.get("weekdays")
        if weekdays and dt_local.weekday() not in weekdays:
            return False

        start_t: Optional[time] = rule.get("start_time")
        end_t: Optional[time] = rule.get("end_time")
        if start_t is None or end_t is None:
            return False

        now_t = dt_local.timetz().replace(tzinfo=None)

        start_minutes = start_t.hour * 60 + start_t.minute
        end_minutes = end_t.hour * 60 + end_t.minute
        now_minutes = now_t.hour * 60 + now_t.minute

        if start_minutes == end_minutes:
            return False

        if start_minutes < end_minutes:
            # обычное окно в пределах суток: [start, end)
            return (now_minutes >= start_minutes) and (now_minutes < end_minutes)

        # окно через полночь: [start, 24h) U [0, end)
        return (now_minutes >= start_minutes) or (now_minutes < end_minutes)

    # -------------------------
    # Parsers
    # -------------------------

    def _parse_weekdays(self, s: Optional[str]) -> Optional[Set[int]]:
        if not s:
            return None
        out: Set[int] = set()
        for part in str(s).split(","):
            part = part.strip()
            if not part:
                continue
            try:
                n = int(part)
            except ValueError:
                continue
            if 0 <= n <= 6:
                out.add(n)
        return out or None

    def _parse_hhmm(self, s: str) -> time:
        s = str(s).strip()
        hh, mm = s.split(":")
        return time(hour=int(hh), minute=int(mm))

    def _parse_iso_dt(self, s: str) -> datetime:
        dt = datetime.fromisoformat(str(s))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
