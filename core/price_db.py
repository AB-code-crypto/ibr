import asyncio
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List

import aiosqlite  # pip install aiosqlite


logger = logging.getLogger(__name__)

# Разрешаем только простые имена таблиц (типа SVIX, TMF, MNQ_2025 и т.п.)
_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass(slots=True)
class PriceBar:
    """
    Одна 5-секундная свеча/бар.
    ts — timestamp бара (в секундах UTC, старт или конец интервала — выберем в price_get).
    """
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


class PriceDB:
    """
    Простой async-коннектор к SQLite для хранения цен.

    - Один файл БД (PRICE_DB_PATH).
    - Для каждого инструмента — отдельная таблица с именем инструмента.
    - Схема таблицы:
        ts INTEGER PRIMARY KEY,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low  REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self._conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    # ---------- Жизненный цикл ----------

    async def connect(self) -> None:
        """
        Открыть соединение с БД, подготовить режим WAL.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.db_path)
        # Минимальные pragma для нормальной работы под нагрузкой
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA synchronous=NORMAL;")
        await self._conn.execute("PRAGMA foreign_keys=ON;")
        await self._conn.commit()
        logger.info("PriceDB connected: %s", self.db_path)

    async def close(self) -> None:
        """
        Закрыть соединение.
        """
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
            logger.info("PriceDB connection closed.")

    # ---------- Внутреннее ----------

    def _table_name(self, instrument: str) -> str:
        """
        Проверяем/нормализуем имя таблицы.
        Сейчас: допускаем только A-Z, a-z, 0-9 и '_'.
        """
        if not _TABLE_RE.match(instrument):
            raise ValueError(f"Недопустимое имя инструмента для таблицы: {instrument!r}")
        return instrument

    async def ensure_table(self, instrument: str) -> None:
        """
        Создать таблицу для инструмента, если её ещё нет.
        """
        if self._conn is None:
            raise RuntimeError("PriceDB is not connected")

        table = self._table_name(instrument)
        sql = f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            ts INTEGER PRIMARY KEY,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low  REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL
        );
        """
        async with self._lock:
            await self._conn.execute(sql)
            await self._conn.commit()

    # ---------- Запись ----------

    async def insert_bar(self, instrument: str, bar: PriceBar) -> None:
        """
        Вставить (или обновить) один бар.
        INSERT OR REPLACE по ts, чтобы можно было перезаписать бар, если он доформировался.
        """
        if self._conn is None:
            raise RuntimeError("PriceDB is not connected")

        table = self._table_name(instrument)
        sql = f"""
        INSERT OR REPLACE INTO "{table}" (ts, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        params = (bar.ts, bar.open, bar.high, bar.low, bar.close, bar.volume)

        async with self._lock:
            await self._conn.execute(sql, params)
            await self._conn.commit()

    # ---------- Чтение ----------

    async def fetch_bars(
        self,
        instrument: str,
        from_ts: Optional[int] = None,
        to_ts: Optional[int] = None,
        limit: Optional[int] = None,
        desc: bool = False,
    ) -> List[PriceBar]:
        """
        Получить бары по инструменту с фильтрацией по времени.
        - from_ts / to_ts — границы по ts (включительно).
        - limit — ограничение по количеству строк (после сортировки).
        - desc=True — последние бары (ORDER BY ts DESC).
        """
        if self._conn is None:
            raise RuntimeError("PriceDB is not connected")

        table = self._table_name(instrument)
        conditions = []
        params: list = []

        if from_ts is not None:
            conditions.append("ts >= ?")
            params.append(from_ts)
        if to_ts is not None:
            conditions.append("ts <= ?")
            params.append(to_ts)

        where_sql = ""
        if conditions:
            where_sql = "WHERE " + " AND ".join(conditions)

        order_sql = "DESC" if desc else "ASC"
        limit_sql = f"LIMIT {int(limit)}" if limit is not None else ""

        sql = (
            f'SELECT ts, open, high, low, close, volume FROM "{table}" '
            f"{where_sql} "
            f"ORDER BY ts {order_sql} "
            f"{limit_sql}"
        )

        async with self._lock:
            cursor = await self._conn.execute(sql, params)
            rows = await cursor.fetchall()

        return [PriceBar(*row) for row in rows]
