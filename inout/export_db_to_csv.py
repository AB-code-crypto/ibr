"""
Разовая утилита экспорта истории цен из SQLite в CSV по дням и по инструментам.

Структура вывода (ОБРАТИ ВНИМАНИЕ: history в КОРНЕ проекта):

history/
    <instrument>/
        <instrument>_YYYY-MM-DD.csv

Формат CSV:
time_utc,open,high,low,close,volume
YYYY-MM-DD HH:MM:SS, ..., ..., ..., ..., ...

Запуск из корня проекта:
    python inout/export_db_to_csv.py
"""

import asyncio
import logging
import csv
from datetime import datetime, timedelta
from pathlib import Path
import sys

import aiosqlite

# Корень проекта = родительская папка для inout/
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Гарантируем, что корень проекта есть в sys.path для импорта core.*
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.config import PRICE_DB_PATH  # noqa: E402

logger = logging.getLogger(__name__)

# Корневая папка для архивов: <root>/history
HISTORY_DIR = PROJECT_ROOT / "history"


async def export_table(conn: aiosqlite.Connection, table: str) -> None:
    """
    Экспортировать один инструмент (одну таблицу) в набор CSV по дням.
    """
    # Выясняем минимальное и максимальное время в таблице
    sql_min_max = f'SELECT MIN(time_utc), MAX(time_utc) FROM "{table}"'
    cursor = await conn.execute(sql_min_max)
    row = await cursor.fetchone()
    await cursor.close()

    min_ts, max_ts = row
    if min_ts is None or max_ts is None:
        logger.info('Table "%s": no data, skipping', table)
        return

    # Парсим строки времени в datetime (UTC)
    # Формат соответствует PriceDB._dt_to_str: "YYYY-MM-DD HH:MM:SS" :contentReference[oaicite:0]{index=0}
    first_dt = datetime.strptime(min_ts, "%Y-%m-%d %H:%M:%S")
    last_dt = datetime.strptime(max_ts, "%Y-%m-%d %H:%M:%S")

    # Границы по датам (UTC)
    current_date = first_dt.date()
    last_date = last_dt.date()

    # Папка для данного инструмента
    instrument_dir = HISTORY_DIR / table
    instrument_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        'Table "%s": exporting from %s to %s (%d days)',
        table,
        first_dt,
        last_dt,
        (last_date - current_date).days + 1,
    )

    while current_date <= last_date:
        day_start_dt = datetime.combine(current_date, datetime.min.time())
        next_day_dt = day_start_dt + timedelta(days=1)

        day_start_str = day_start_dt.strftime("%Y-%m-%d %H:%M:%S")
        next_day_str = next_day_dt.strftime("%Y-%m-%d %H:%M:%S")

        # Забираем все бары за один день
        sql_day = (
            f'SELECT time_utc, open, high, low, close, volume '
            f'FROM "{table}" '
            f'WHERE time_utc >= ? AND time_utc < ? '
            f'ORDER BY time_utc ASC'
        )

        cursor = await conn.execute(sql_day, (day_start_str, next_day_str))
        rows = await cursor.fetchall()
        await cursor.close()

        if rows:
            csv_name = f"{table}_{current_date.isoformat()}.csv"
            csv_path = instrument_dir / csv_name

            with csv_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                # Заголовок в точном соответствии со схемой БД
                writer.writerow(["time_utc", "open", "high", "low", "close", "volume"])
                writer.writerows(rows)

            logger.info(
                'Table "%s": wrote %d rows to %s',
                table,
                len(rows),
                csv_path,
            )

        else:
            logger.info('Table "%s": no rows for %s, skipping file', table, current_date)

        current_date = current_date + timedelta(days=1)


async def export_all() -> None:
    """
    Основная функция экспорта всех инструментов (всех таблиц) в CSV.
    """
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    db_path = Path(PRICE_DB_PATH)
    if not db_path.exists():
        raise FileNotFoundError(f"Price DB not found at {db_path}")

    logger.info("Connecting to SQLite DB: %s", db_path)

    async with aiosqlite.connect(db_path) as conn:
        # Получаем список таблиц; исключаем служебные sqlite_*
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        rows = await cursor.fetchall()
        await cursor.close()

        tables = [row[0] for row in rows if not row[0].startswith("sqlite_")]

        if not tables:
            logger.warning("No user tables found in DB, nothing to export.")
            return

        logger.info("Found %d tables: %s", len(tables), ", ".join(tables))

        for table in tables:
            try:
                await export_table(conn, table)
            except Exception as e:
                logger.error(
                    'Failed to export table "%s": %s',
                    table,
                    e,
                )


def main() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )

    asyncio.run(export_all())


if __name__ == "__main__":
    main()
