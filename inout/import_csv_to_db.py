"""
Утилита импорта истории цен из CSV (history/*/*.csv) в SQLite-БД.

Структура входных данных (history в КОРНЕ проекта):

history/
    <instrument>/
        <instrument>_YYYY-MM-DD.csv

Формат CSV:
time_utc,open,high,low,close,volume
YYYY-MM-DD HH:MM:SS, ..., ..., ..., ..., ...

Рекомендуется запускать на ПУСТОЙ БД (старый файл prices.sqlite3 удалить или переименовать),
чтобы не было конфликтов/дубликатов.

Запуск из корня проекта:
    python inout/import_csv_to_db.py
"""

import asyncio
import logging
import csv
from datetime import datetime, timezone
from pathlib import Path
import sys

from core.config import PRICE_DB_PATH  # noqa: E402
from core.price_db import PriceDB, PriceBar  # noqa: E402

logger = logging.getLogger(__name__)

# Корень проекта = родительская папка для inout/
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Гарантируем, что корень проекта есть в sys.path для импорта core.*
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Папка history в корне проекта
HISTORY_DIR = PROJECT_ROOT / "history"


async def import_instrument(db: PriceDB, instrument_dir: Path) -> None:
    """
    Импортировать один инструмент (одну папку history/<instrument>) в БД.
    Имя папки = имя таблицы в БД.
    """
    if not instrument_dir.is_dir():
        return

    symbol = instrument_dir.name
    logger.info('Importing instrument "%s" from %s', symbol, instrument_dir)

    # Убедимся, что таблица существует и имеет правильную структуру
    await db.ensure_table(symbol)

    # Собираем все CSV-файлы по этому инструменту
    # Сортируем по имени, чтобы шли по датам по возрастанию
    csv_files = sorted(instrument_dir.glob(f"{symbol}_*.csv"))
    if not csv_files:
        logger.info('No CSV files found for "%s", skipping', symbol)
        return

    total_rows = 0
    batch: list[PriceBar] = []
    batch_size = 1000

    for csv_path in csv_files:
        logger.info('  Reading %s', csv_path.name)

        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            # пропустим заголовок
            header = next(reader, None)
            # ожидаем ["time_utc", "open", "high", "low", "close", "volume"]
            # но жёстко не валидируем, лишь логируем при отличии :contentReference[oaicite:1]{index=1}
            if header and header != ["time_utc", "open", "high", "low", "close", "volume"]:
                logger.warning(
                    '  Unexpected CSV header in %s: %r',
                    csv_path.name,
                    header,
                )

            for row in reader:
                if len(row) != 6:
                    logger.warning(
                        '  Bad row (len=%d) in %s: %r',
                        len(row),
                        csv_path.name,
                        row,
                    )
                    continue

                (
                    time_str,
                    open_str,
                    high_str,
                    low_str,
                    close_str,
                    volume_str,
                ) = row

                try:
                    # Формат мы сами определили в экспортёре:
                    # "YYYY-MM-DD HH:MM:SS" (UTC)
                    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    dt = dt.replace(tzinfo=timezone.utc)

                    o = float(open_str)
                    h = float(high_str)
                    l = float(low_str)
                    c = float(close_str)
                    v = float(volume_str)

                except Exception as e:
                    logger.warning(
                        '  Failed to parse row in %s: %r (%s)',
                        csv_path.name,
                        row,
                        e,
                    )
                    continue

                bar = PriceBar.from_datetime(
                    dt,
                    o,
                    h,
                    l,
                    c,
                    v,
                )
                batch.append(bar)
                total_rows += 1

                if len(batch) >= batch_size:
                    inserted = await db.insert_bars(symbol, batch)
                    logger.info(
                        '  Inserted %d bars into "%s" (batch flush), total_rows=%d',
                        inserted,
                        symbol,
                        total_rows,
                    )
                    batch.clear()

    # финальный flush
    if batch:
        inserted = await db.insert_bars(symbol, batch)
        logger.info(
            '  Inserted %d bars into "%s" (final flush), total_rows=%d',
            inserted,
            symbol,
            total_rows,
        )
        batch.clear()

    logger.info(
        'Instrument "%s": import completed, total_rows=%d',
        symbol,
        total_rows,
    )


async def import_all() -> None:
    """
    Основная функция: пройти по папкам history/* и импортировать все инструменты в БД.
    """
    db_path = Path(PRICE_DB_PATH)
    logger.info("Target SQLite DB: %s", db_path)

    # Поднимаем PriceDB (он сам создаст файл при необходимости)
    db = PriceDB(str(db_path))
    await db.connect()

    try:
        if not HISTORY_DIR.exists():
            logger.error("History directory %s does not exist", HISTORY_DIR)
            return

        instrument_dirs = sorted(
            p for p in HISTORY_DIR.iterdir() if p.is_dir()
        )

        if not instrument_dirs:
            logger.warning("No instrument directories found in %s", HISTORY_DIR)
            return

        logger.info(
            "Found %d instrument directories: %s",
            len(instrument_dirs),
            ", ".join(d.name for d in instrument_dirs),
        )

        for instr_dir in instrument_dirs:
            try:
                await import_instrument(db, instr_dir)
            except Exception as e:
                logger.error(
                    'Failed to import instrument "%s": %s',
                    instr_dir.name,
                    e,
                )

    finally:
        await db.close()


def main() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )

    asyncio.run(import_all())


if __name__ == "__main__":
    main()
