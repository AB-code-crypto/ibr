import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Iterable, Union

import aiosqlite  # pip install aiosqlite

logger = logging.getLogger(__name__)

# Разрешаем только простые имена таблиц (типа SVIX, TMF, MNQ_2025 и т.п.)
_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass(slots=True)
class PriceBar:
    """
    Одна свеча/бар (по умолчанию 5 секунд, но класс не привязан к таймфрейму).

    ts  — UTC timestamp (секунды с начала эпохи, целое число).
    open/high/low/close/volume — стандартные поля OHLCV.
    """
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    @classmethod
    def from_datetime(
            cls,
            dt: datetime,
            open: float,
            high: float,
            low: float,
            close: float,
            volume: float = 0.0,
    ) -> "PriceBar":
        """
        Удобный конструктор: принимает datetime и переводит его в UTC timestamp.
        Если dt без tzinfo, считаем, что это уже UTC.
        """
        if dt.tzinfo is None:
            dt_utc = dt.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt.astimezone(timezone.utc)
        ts = int(dt_utc.timestamp())
        return cls(ts=ts, open=open, high=high, low=low, close=close, volume=volume)

    def to_datetime_utc(self) -> datetime:
        """
        Вернуть время бара как datetime в UTC.
        """
        return datetime.fromtimestamp(self.ts, tz=timezone.utc)


class PriceDB:
    """
    Async-коннектор к SQLite для хранения ценовых баров.

    - Один файл БД (db_path).
    - Для каждого инструмента — отдельная таблица с именем инструмента.
    - Схема таблицы (общая для всех инструментов):
        ts INTEGER PRIMARY KEY,  -- UTC epoch seconds
        open   REAL NOT NULL,
        high   REAL NOT NULL,
        low    REAL NOT NULL,
        close  REAL NOT NULL,
        volume REAL NOT NULL
    """

    def __init__(self, db_path: Union[Path, str]) -> None:
        self.db_path = Path(db_path)
        self._conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Жизненный цикл                                                     #
    # ------------------------------------------------------------------ #

    @property
    def is_connected(self) -> bool:
        return self._conn is not None

    async def connect(self) -> None:
        """
        Открыть соединение с БД, при отсутствии файла — создать его.

        Включаем WAL, NORMAL synchronous и foreign_keys для адекватной работы
        под нагрузкой и в асинхронной среде.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.db_path)
        # aiosqlite по умолчанию возвращает кортежи, нам этого достаточно
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA synchronous=NORMAL;")
        await self._conn.execute("PRAGMA foreign_keys=ON;")
        await self._conn.commit()
        logger.info("PriceDB connected: %s", self.db_path)

    async def close(self) -> None:
        """
        Закрыть соединение с БД.
        """
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
            logger.info("PriceDB connection closed.")

    # ------------------------------------------------------------------ #
    # Внутреннее                                                         #
    # ------------------------------------------------------------------ #

    def _table_name(self, instrument: str) -> str:
        """
        Нормализация/валидация имени таблицы.

        Допускаем только буквы/цифры/подчёркивание, чтобы исключить инъекции.
        """
        if not _TABLE_RE.match(instrument):
            raise ValueError(f"Недопустимое имя инструмента для таблицы: {instrument!r}")
        return instrument

    # ------------------------------------------------------------------ #
    # Создание таблиц                                                    #
    # ------------------------------------------------------------------ #

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
            open   REAL NOT NULL,
            high   REAL NOT NULL,
            low    REAL NOT NULL,
            close  REAL NOT NULL,
            volume REAL NOT NULL
        );
        """
        async with self._lock:
            await self._conn.execute(sql)
            await self._conn.commit()

    # ------------------------------------------------------------------ #
    # Запись                                                             #
    # ------------------------------------------------------------------ #

    async def insert_bar(self, instrument: str, bar: PriceBar) -> None:
        """
        Вставить (или обновить) один бар.

        Используем INSERT OR REPLACE по PRIMARY KEY (ts), чтобы можно было
        перезаписать бар, если он доформировался или был пересчитан.
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

    async def insert_bars(self, instrument: str, bars: Iterable[PriceBar]) -> int:
        """
        Вставить несколько баров за один коммит.
        Возвращает количество реально вставленных строк.

        Это удобно для backfill'а истории из IB.
        """
        if self._conn is None:
            raise RuntimeError("PriceDB is not connected")

        bars_list = list(bars)
        if not bars_list:
            return 0

        table = self._table_name(instrument)
        sql = f"""
        INSERT OR REPLACE INTO "{table}" (ts, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        params_seq = [
            (b.ts, b.open, b.high, b.low, b.close, b.volume) for b in bars_list
        ]

        async with self._lock:
            await self._conn.executemany(sql, params_seq)
            await self._conn.commit()

        return len(bars_list)

    # ------------------------------------------------------------------ #
    # Чтение                                                             #
    # ------------------------------------------------------------------ #

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

        - from_ts / to_ts — границы по ts (включительно, UTC epoch seconds).
        - limit — ограничение по количеству строк (после сортировки).
        - desc=True — вернуть бары в порядке DESC (обычно для "последних N").
        """
        if self._conn is None:
            raise RuntimeError("PriceDB is not connected")

        table = self._table_name(instrument)
        conditions: List[str] = []
        params: List[object] = []

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

    async def get_last_ts(self, instrument: str) -> Optional[int]:
        """
        Вернуть timestamp (UTC epoch seconds) последнего бара по инструменту.

        Если таблица пустая или не существует — вернёт None (при условии, что
        ensure_table уже вызывался где-то выше).
        """
        if self._conn is None:
            raise RuntimeError("PriceDB is not connected")

        table = self._table_name(instrument)
        sql = f'SELECT MAX(ts) FROM "{table}"'

        async with self._lock:
            cursor = await self._conn.execute(sql)
            row = await cursor.fetchone()

        if not row:
            return None

        ts = row[0]
        if ts is None:
            return None

        return int(ts)

    async def get_last_bar(self, instrument: str) -> Optional[PriceBar]:
        """
        Вернуть последний бар по инструменту, либо None, если данных нет.
        """
        if self._conn is None:
            raise RuntimeError("PriceDB is not connected")

        table = self._table_name(instrument)
        sql = f'SELECT ts, open, high, low, close, volume FROM "{table}" ORDER BY ts DESC LIMIT 1'

        async with self._lock:
            cursor = await self._conn.execute(sql)
            row = await cursor.fetchone()

        if not row:
            return None

        return PriceBar(*row)

    async def get_last_bar_datetime(self, instrument: str) -> Optional[datetime]:
        """
        Вернуть время последнего бара как datetime в UTC, либо None.
        Удобно для расчёта "гапа" истории при backfill'е.
        """
        ts = await self.get_last_ts(instrument)
        if ts is None:
            return None
        return datetime.fromtimestamp(ts, tz=timezone.utc)
