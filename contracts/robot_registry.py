"""
Robot registry + runtime switches.

Registry:
  - Declares all robots that exist in the codebase (robot_id -> spec factory).

Switches:
  - Persisted enable/disable flags stored in SQLite (we reuse robot_ops.db).
  - Can be toggled without code changes.
  - Can be applied live: trading loops periodically re-check enabled flag.

Note:
  This module imports strategy spec factories, so it is a composition/wiring layer.
  It is placed in contracts/ by project convention to keep robot wiring near bot_spec.py.
"""

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from contracts.bot_spec import RobotSpec

from strategies.pattern_pirson.spec import get_robot_spec as get_pattern_pirson_spec


ROBOT_SPEC_FACTORIES: dict[str, Callable[[], RobotSpec]] = {
    "pattern_pirson": get_pattern_pirson_spec,
}


def load_all_robot_specs() -> list[RobotSpec]:
    """Build and validate RobotSpec instances for all registered robots (no filtering)."""
    if not ROBOT_SPEC_FACTORIES:
        raise RuntimeError("ROBOT_SPEC_FACTORIES is empty: no robots registered.")

    specs: list[RobotSpec] = []
    seen: set[str] = set()

    for expected_robot_id, factory in ROBOT_SPEC_FACTORIES.items():
        spec = factory()

        if spec.robot_id != expected_robot_id:
            raise ValueError(
                "Robot registry mismatch: key robot_id != spec.robot_id "
                f"({expected_robot_id!r} != {spec.robot_id!r}). "
                "Fix strategies/<robot_id>/spec.py or ROBOT_SPEC_FACTORIES."
            )

        if spec.robot_id in seen:
            raise ValueError(f"Duplicate robot_id in registry: {spec.robot_id!r}")

        seen.add(spec.robot_id)
        specs.append(spec)

    specs.sort(key=lambda s: s.robot_id)
    return specs


def format_robot_specs_for_log(specs: list[RobotSpec]) -> str:
    lines: list[str] = []
    for s in specs:
        lines.append(
            "- "
            f"robot_id={s.robot_id} | "
            f"active_future={s.active_future_symbol} | "
            f"instrument_root={s.instrument_root} | "
            f"trade_qty={s.trade_qty} | "
            f"order_ref={s.order_ref}"
        )
    return "\n".join(lines)


@dataclass(slots=True)
class RobotSwitchesStore:
    """Persistent on/off switches for robots stored in SQLite."""

    db_path: str
    cache_ttl_seconds: int = 10

    _cache_loaded_at_utc: datetime | None = None
    _cache_enabled_by_robot: dict[str, bool] | None = None

    def ensure_schema(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS robot_switches (
                    robot_id TEXT PRIMARY KEY,
                    enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
                    updated_at_utc TEXT NOT NULL
                );
                """
            )
            conn.commit()

    def seed_defaults(self, robot_ids: list[str], default_enabled: bool = True) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        enabled_int = 1 if default_enabled else 0

        with sqlite3.connect(self.db_path) as conn:
            for rid in robot_ids:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO robot_switches(robot_id, enabled, updated_at_utc)
                    VALUES (?, ?, ?);
                    """,
                    (rid, enabled_int, now),
                )
            conn.commit()

        # сбрасываем кэш, чтобы новые строки сразу учитывались
        self._cache_loaded_at_utc = None
        self._cache_enabled_by_robot = None

    def _refresh_cache_if_needed(self) -> None:
        now = datetime.now(timezone.utc)
        if self._cache_loaded_at_utc is not None and self._cache_enabled_by_robot is not None:
            age = (now - self._cache_loaded_at_utc).total_seconds()
            if age < self.cache_ttl_seconds:
                return

        enabled_by_robot: dict[str, bool] = {}
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT robot_id, enabled FROM robot_switches;").fetchall()
            for robot_id, enabled_int in rows:
                enabled_by_robot[str(robot_id)] = bool(int(enabled_int))

        self._cache_enabled_by_robot = enabled_by_robot
        self._cache_loaded_at_utc = now

    def is_enabled(self, robot_id: str) -> bool:
        self._refresh_cache_if_needed()
        if self._cache_enabled_by_robot is None:
            raise RuntimeError("Robot switches cache not initialized.")
        # Если робот отсутствует в таблице — это ошибка конфигурации (fail-fast).
        if robot_id not in self._cache_enabled_by_robot:
            raise KeyError(
                f"Robot {robot_id!r} is not present in robot_switches table. "
                "Run seed_defaults() during startup."
            )
        return self._cache_enabled_by_robot[robot_id]

    def enabled_robot_ids(self, known_robot_ids: list[str]) -> set[str]:
        self._refresh_cache_if_needed()
        if self._cache_enabled_by_robot is None:
            raise RuntimeError("Robot switches cache not initialized.")

        enabled: set[str] = set()
        for rid in known_robot_ids:
            if rid not in self._cache_enabled_by_robot:
                raise KeyError(
                    f"Robot {rid!r} is not present in robot_switches table. "
                    "Run seed_defaults() during startup."
                )
            if self._cache_enabled_by_robot[rid]:
                enabled.add(rid)
        return enabled

    def set_enabled(self, robot_id: str, enabled: bool) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        enabled_int = 1 if enabled else 0

        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """
                UPDATE robot_switches
                SET enabled = ?, updated_at_utc = ?
                WHERE robot_id = ?;
                """,
                (enabled_int, now, robot_id),
            )
            if cur.rowcount != 1:
                raise KeyError(f"Robot {robot_id!r} not found in robot_switches.")
            conn.commit()

        self._cache_loaded_at_utc = None
        self._cache_enabled_by_robot = None
