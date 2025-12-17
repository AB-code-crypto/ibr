import logging
from pathlib import Path
from typing import Union, Optional

import aiosqlite

logger = logging.getLogger(__name__)


async def connect_sqlite(
    db_path: Union[Path, str],
    *,
    journal_mode: str = "WAL",
    synchronous: str = "NORMAL",
    foreign_keys: bool = True,
    busy_timeout_ms: int = 5000,
) -> aiosqlite.Connection:
    """Create and configure an aiosqlite connection.

    This is the single place where we standardize SQLite PRAGMA settings
    for all project databases (prices.sqlite3, robot_ops.db, etc.).
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = await aiosqlite.connect(path)

    await apply_sqlite_pragmas(
        conn,
        journal_mode=journal_mode,
        synchronous=synchronous,
        foreign_keys=foreign_keys,
        busy_timeout_ms=busy_timeout_ms,
    )
    await conn.commit()

    logger.info("SQLite connected: %s", path)
    return conn


async def apply_sqlite_pragmas(
    conn: aiosqlite.Connection,
    *,
    journal_mode: str = "WAL",
    synchronous: str = "NORMAL",
    foreign_keys: bool = True,
    busy_timeout_ms: int = 5000,
) -> None:
    """Apply project-wide PRAGMAs to a given SQLite connection."""
    # WAL is important for concurrent readers/writers with async workloads.
    await conn.execute(f"PRAGMA journal_mode={journal_mode};")
    await conn.execute(f"PRAGMA synchronous={synchronous};")
    await conn.execute(f"PRAGMA foreign_keys={'ON' if foreign_keys else 'OFF'};")
    await conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)};")
