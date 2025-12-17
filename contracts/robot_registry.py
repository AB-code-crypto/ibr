"""
Robot registry + runtime switches.

Registry:
  - Declares all robots that exist in the codebase (robot_id -> spec factory).

Switches:
  - Persisted enable/disable flags stored in SQLite (robot_ops.db).
  - Can be toggled without code changes.
  - Can be applied live: trading loops periodically re-check enabled flag.

Note:
  This module imports strategy spec factories, so it is a composition/wiring layer.
  It is placed in contracts/ by project convention to keep robot wiring near bot_spec.py.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from contracts.bot_spec import RobotSpec
from core.ops_db import OpsDB

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
            f"instrument={s.active_future_symbol} | "
            f"trade_qty={s.trade_qty} | "
            f"order_ref={s.order_ref}"
        )
    return "\n".join(lines)


@dataclass(slots=True)
class RobotSwitchesStore:
    """Persistent on/off switches for robots stored in SQLite (robot_switches table)."""

    ops_db: OpsDB
    cache_ttl_seconds: int = 10

    _cache_loaded_at_utc: datetime | None = None
    _cache_enabled_by_robot: dict[str, bool] | None = None

    async def ensure_schema(self) -> None:
        await self.ops_db.execute(
            """
            CREATE TABLE IF NOT EXISTS robot_switches (
                robot_id TEXT PRIMARY KEY,
                enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
                updated_at_utc TEXT NOT NULL
            );
            """,
            commit=True,
        )

    async def seed_defaults(self, robot_ids: list[str], default_enabled: bool = True) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        enabled_int = 1 if default_enabled else 0

        await self.ops_db.executemany(
            """
            INSERT OR IGNORE INTO robot_switches(robot_id, enabled, updated_at_utc)
            VALUES (?, ?, ?);
            """,
            [(rid, enabled_int, now) for rid in robot_ids],
            commit=True,
        )

        # сбрасываем кэш, чтобы новые строки сразу учитывались
        self._cache_loaded_at_utc = None
        self._cache_enabled_by_robot = None

    async def _refresh_cache_if_needed(self) -> None:
        now = datetime.now(timezone.utc)
        if self._cache_loaded_at_utc is not None and self._cache_enabled_by_robot is not None:
            age = (now - self._cache_loaded_at_utc).total_seconds()
            if age < self.cache_ttl_seconds:
                return

        rows = await self.ops_db.fetchall("SELECT robot_id, enabled FROM robot_switches;")
        enabled_by_robot: dict[str, bool] = {}
        for robot_id, enabled_int in rows:
            enabled_by_robot[str(robot_id)] = bool(int(enabled_int))

        self._cache_enabled_by_robot = enabled_by_robot
        self._cache_loaded_at_utc = now

    async def is_enabled(self, robot_id: str) -> bool:
        await self._refresh_cache_if_needed()
        if self._cache_enabled_by_robot is None:
            raise RuntimeError("Robot switches cache not initialized.")

        # Если робот отсутствует в таблице — это ошибка конфигурации (fail-fast).
        if robot_id not in self._cache_enabled_by_robot:
            raise KeyError(
                f"Robot {robot_id!r} is not present in robot_switches table. "
                "Run seed_defaults() during startup."
            )
        return self._cache_enabled_by_robot[robot_id]

    async def enabled_robot_ids(self, known_robot_ids: list[str]) -> set[str]:
        await self._refresh_cache_if_needed()
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

    async def set_enabled(self, robot_id: str, enabled: bool) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        enabled_int = 1 if enabled else 0

        cur = await self.ops_db.execute(
            """
            UPDATE robot_switches
            SET enabled = ?, updated_at_utc = ?
            WHERE robot_id = ?;
            """,
            (enabled_int, now, robot_id),
            commit=True,
        )

        if cur.rowcount != 1:
            raise KeyError(f"Robot {robot_id!r} not found in robot_switches.")

        self._cache_loaded_at_utc = None
        self._cache_enabled_by_robot = None