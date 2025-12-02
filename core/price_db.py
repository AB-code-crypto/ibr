import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Iterable, Union

import aiosqlite

logger = logging.getLogger(__name__)

# Разрешаем только простые имена таблиц (типа SVIX, TMF, MNQ_2025 и т.п.)
_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass(slots=True)
class PriceBar:
    """
    Одна свеча/бар.

    time_utc — время бара в UTC (datetime с tzinfo=UTC).
    open/high/low/close/volume — стандартные поля OHLCV.
    """
    time_utc: datetime
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
        Удобный конструктор: принимает datetime и приводит его к UTC.
        Если dt без tzinfo, считаем, что это уже UTC.
        """
        if dt.tzinfo is None:
            dt_utc = dt.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt.astimezone(timezone.utc)
        return cls(time_utc=dt_utc, open=open, high=high, low=low, close=close, volume=volume)


class PriceDB:
    """
    Async-коннектор к SQLite для хранения ценовых баров.

    - Один файл БД (db_path).
    - Для каждого инструмента — отдельная таблица с именем инструмента.
    - Схема таблицы (общая для всех инструментов):
        time_utc TEXT PRIMARY KEY,  -- 'YYYY-MM-DD HH:MM:SS' в UTC
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

    @staticmethod
    def _dt_to_str(dt: datetime) -> str:
        """
        datetime (любая tz) -> строка 'YYYY-MM-DD HH:MM:SS' в UTC.
        """
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _str_to_dt(s: str) -> datetime:
        """
        Строка 'YYYY-MM-DD HH:MM:SS' (UTC) -> datetime с tzinfo=UTC.
        """
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)

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
            time_utc TEXT PRIMARY KEY,
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

        Используем INSERT OR REPLACE по PRIMARY KEY (time_utc), чтобы можно было
        перезаписать бар, если он доформировался или был пересчитан.
        """
        if self._conn is None:
            raise RuntimeError("PriceDB is not connected")

        table = self._table_name(instrument)
        sql = f"""
        INSERT OR REPLACE INTO "{table}" (time_utc, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        time_str = self._dt_to_str(bar.time_utc)
        params = (time_str, bar.open, bar.high, bar.low, bar.close, bar.volume)

        async with self._lock:
            await self._conn.execute(sql, params)
            await self._conn.commit()

    async def insert_bars(self, instrument: str, bars: Iterable[PriceBar]) -> int:
        """
        Вставить несколько баров за один коммит.
        Возвращает количество реально вставленных баров.
        """
        if self._conn is None:
            raise RuntimeError("PriceDB is not connected")

        bars_list = list(bars)
        if not bars_list:
            return 0

        table = self._table_name(instrument)
        sql = f"""
        INSERT OR REPLACE INTO "{table}" (time_utc, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        params_seq = [
            (self._dt_to_str(b.time_utc), b.open, b.high, b.low, b.close, b.volume)
            for b in bars_list
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
            from_dt: Optional[datetime] = None,
            to_dt: Optional[datetime] = None,
            limit: Optional[int] = None,
            desc: bool = False,
    ) -> List[PriceBar]:
        """
        Получить бары по инструменту с фильтрацией по времени.

        - from_dt / to_dt — границы по времени (datetime).
        - limit — ограничение по количеству строк (после сортировки).
        - desc=True — вернуть бары в порядке DESC (обычно для "последних N").
        """
        if self._conn is None:
            raise RuntimeError("PriceDB is not connected")

        table = self._table_name(instrument)
        conditions: List[str] = []
        params: List[object] = []

        if from_dt is not None:
            conditions.append("time_utc >= ?")
            params.append(self._dt_to_str(from_dt))
        if to_dt is not None:
            conditions.append("time_utc <= ?")
            params.append(self._dt_to_str(to_dt))

        where_sql = ""
        if conditions:
            where_sql = "WHERE " + " AND ".join(conditions)

        order_sql = "DESC" if desc else "ASC"
        limit_sql = f"LIMIT {int(limit)}" if limit is not None else ""

        sql = (
            f'SELECT time_utc, open, high, low, close, volume FROM "{table}" '
            f"{where_sql} "
            f"ORDER BY time_utc {order_sql} "
            f"{limit_sql}"
        )

        async with self._lock:
            cursor = await self._conn.execute(sql, params)
            rows = await cursor.fetchall()

        bars: List[PriceBar] = []
        for time_str, o, h, l, c, v in rows:
            dt = self._str_to_dt(time_str)
            bars.append(PriceBar(time_utc=dt, open=o, high=h, low=l, close=c, volume=v))

        return bars

    async def get_last_bar_datetime(self, instrument: str) -> Optional[datetime]:
        """
        Вернуть время последнего бара как datetime в UTC, либо None.
        Удобно для расчёта "гапа" истории при backfill'е.
        """
        if self._conn is None:
            raise RuntimeError("PriceDB is not connected")

        table = self._table_name(instrument)
        sql = f'SELECT MAX(time_utc) FROM "{table}"'

        async with self._lock:
            cursor = await self._conn.execute(sql)
            row = await cursor.fetchone()

        if not row or row[0] is None:
            return None

        return self._str_to_dt(row[0])

    async def get_last_bar(self, instrument: str) -> Optional[PriceBar]:
        """
        Вернуть последний бар по инструменту, либо None, если данных нет.
        """
        if self._conn is None:
            raise RuntimeError("PriceDB is not connected")

        table = self._table_name(instrument)
        sql = f'SELECT time_utc, open, high, low, close, volume FROM "{table}" ORDER BY time_utc DESC LIMIT 1'

        async with self._lock:
            cursor = await self._conn.execute(sql)
            row = await cursor.fetchone()

        if not row:
            return None

        time_str, o, h, l, c, v = row
        dt = self._str_to_dt(time_str)
        return PriceBar(time_utc=dt, open=o, high=h, low=l, close=c, volume=v)

    async def get_last_ts(self, instrument: str) -> Optional[int]:
        """
        Опциональный helper: вернуть время последнего бара как epoch seconds.
        Может пригодиться для быстрого сравнения/экспорта.
        """
        dt = await self.get_last_bar_datetime(instrument)
        if dt is None:
            return None
        dt_utc = dt.astimezone(timezone.utc)
        return int(dt_utc.timestamp())
