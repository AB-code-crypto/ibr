import asyncio
import logging
from pathlib import Path
from typing import Optional, Sequence, Union

import aiosqlite

from core.sqlite import connect_sqlite

logger = logging.getLogger(__name__)


class OpsDB:
    """Async SQLite connection wrapper for operational DB (robot_ops.db).

    Goals:
    - Connection is created in robot.py and passed into services (switches, quiet windows).
    - Single place to standardize PRAGMAs and lifecycle (connect/close).
    - Serialize concurrent DB operations via asyncio.Lock (aiosqlite is not fully concurrent).
    """

    def __init__(self, db_path: Union[Path, str]) -> None:
        self.db_path = Path(db_path)
        self._conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    @property
    def is_connected(self) -> bool:
        return self._conn is not None

    async def connect(self) -> None:
        if self._conn is not None:
            return
        self._conn = await connect_sqlite(self.db_path)

    async def close(self) -> None:
        if self._conn is None:
            return
        await self._conn.close()
        self._conn = None
        logger.info("OpsDB connection closed.")

    def _require_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("OpsDB is not connected")
        return self._conn

    async def execute(
        self,
        sql: str,
        params: Sequence[object] | None = None,
        *,
        commit: bool = False,
    ) -> aiosqlite.Cursor:
        conn = self._require_conn()
        if params is None:
            params = ()

        async with self._lock:
            cur = await conn.execute(sql, params)
            if commit:
                await conn.commit()
            return cur

    async def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[Sequence[object]],
        *,
        commit: bool = False,
    ) -> None:
        conn = self._require_conn()
        async with self._lock:
            await conn.executemany(sql, seq_of_params)
            if commit:
                await conn.commit()

    async def fetchone(self, sql: str, params: Sequence[object] | None = None) -> Optional[tuple]:
        cur = await self.execute(sql, params)
        return await cur.fetchone()

    async def fetchall(self, sql: str, params: Sequence[object] | None = None) -> list[tuple]:
        cur = await self.execute(sql, params)
        return await cur.fetchall()
