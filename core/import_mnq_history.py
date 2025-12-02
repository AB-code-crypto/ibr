"""
Разовая утилита импорта истории MNQ* из папки MNQ_5s_ALL_daily в SQLite-базу цен.

Структура каталогов (относительно корня проекта ibr/):

Каждый CSV-файл содержит 5-секундные бары за сутки.

Предполагаемые колонки (регистронезависимо, через auto-detect):
    - время: datetime / time / timestamp / Date / date
    - open:  open
    - high:  high
    - low:   low
    - close: close
    - volume: volume / vol

Формат времени — любой ISO-формат, который понимает datetime.fromisoformat().
Если tzinfo отсутствует, считаем, что это уже UTC.
"""

import asyncio
import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from core.config import PRICE_DB_PATH
from core.price_db import PriceDB, PriceBar

logger = logging.getLogger(__name__)

# Корень проекта = родительская папка от core/
ROOT_DIR = Path(__file__).resolve().parent.parent

# Каталог с историческими CSV
CSV_ROOT = ROOT_DIR / "MNQ_5s_ALL_daily"

# Кандидаты для имён колонок (регистронезависимо)
TIME_COL_CANDIDATES = ("datetime", "time", "timestamp", "date", "datetime_utc")
OPEN_COL_CANDIDATES = ("open",)
HIGH_COL_CANDIDATES = ("high",)
LOW_COL_CANDIDATES = ("low",)
CLOSE_COL_CANDIDATES = ("close",)
VOLUME_COL_CANDIDATES = ("volume", "vol")


def _detect_columns(fieldnames: Iterable[str]) -> Dict[str, str]:
    """
    Определить реальные имена колонок в CSV по спискам кандидатов.

    Возвращает словарь:
        {
            "time": "<имя_колонки_в_CSV>",
            "open": "<...>",
            "high": "<...>",
            "low": "<...>",
            "close": "<...>",
            "volume": "<...>",
        }

    Бросает ValueError, если что-то критичное не найдено.
    """
    normalized = {name.lower(): name for name in fieldnames if name is not None}

    def pick(candidates: Tuple[str, ...], what: str, required: bool = True) -> Optional[str]:
        for cand in candidates:
            if cand.lower() in normalized:
                return normalized[cand.lower()]
        if required:
            raise ValueError(f"Не удалось найти колонку для {what!r} среди {list(normalized.keys())!r}")
        return None

    time_col = pick(TIME_COL_CANDIDATES, "time")
    open_col = pick(OPEN_COL_CANDIDATES, "open")
    high_col = pick(HIGH_COL_CANDIDATES, "high")
    low_col = pick(LOW_COL_CANDIDATES, "low")
    close_col = pick(CLOSE_COL_CANDIDATES, "close")
    volume_col = pick(VOLUME_COL_CANDIDATES, "volume", required=False)

    return {
        "time": time_col,
        "open": open_col,
        "high": high_col,
        "low": low_col,
        "close": close_col,
        "volume": volume_col,
    }


def _parse_datetime_utc(value: str) -> datetime:
    """
    Преобразовать строку даты/времени в datetime с tzinfo=UTC.

    Используем datetime.fromisoformat(). Если tzinfo отсутствует, считаем это UTC.
    """
    raw = value.strip()
    if not raw:
        raise ValueError("Пустое значение времени")

    try:
        dt = datetime.fromisoformat(raw)
    except Exception as exc:
        raise ValueError(f"Не удалось распарсить дату/время из {raw!r}: {exc}") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt


def _parse_float(value: str, what: str, default: Optional[float] = None) -> float:
    """
    Аккуратно преобразовать строку в float. Если строка пустая и задан default,
    возвращает default. Иначе бросает ValueError.
    """
    if value is None:
        if default is not None:
            return default
        raise ValueError(f"Пустое значение для {what}")

    raw = value.strip()
    if raw == "":
        if default is not None:
            return default
        raise ValueError(f"Пустое значение для {what}")

    try:
        return float(raw)
    except Exception as exc:
        raise ValueError(f"Не удалось преобразовать {what}={raw!r} к float: {exc}") from exc


def parse_csv_file(csv_path: Path) -> List[PriceBar]:
    """
    Прочитать CSV-файл и вернуть список PriceBar.

    При ошибке парсинга отдельной строки — логируем warning и пропускаем строку.
    При критической ошибке (нет колонок) — даём исключение наверх.
    """
    logger.info("Parsing CSV: %s", csv_path)

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"Файл {csv_path} не содержит заголовок колонок")

        columns = _detect_columns(reader.fieldnames)

        time_col = columns["time"]
        open_col = columns["open"]
        high_col = columns["high"]
        low_col = columns["low"]
        close_col = columns["close"]
        volume_col = columns["volume"]

        bars: List[PriceBar] = []

        for idx, row in enumerate(reader, start=1):
            try:
                dt = _parse_datetime_utc(row[time_col])

                o = _parse_float(row[open_col], "open")
                h = _parse_float(row[high_col], "high")
                l = _parse_float(row[low_col], "low")
                c = _parse_float(row[close_col], "close")

                if volume_col is not None:
                    v = _parse_float(row[volume_col], "volume", default=0.0)
                else:
                    v = 0.0

                bars.append(
                    PriceBar.from_datetime(
                        dt,
                        open=o,
                        high=h,
                        low=l,
                        close=c,
                        volume=v,
                    )
                )
            except Exception as exc:
                logger.warning(
                    "Skip row %d in %s due to parse error: %s",
                    idx,
                    csv_path.name,
                    exc,
                )
                continue

    logger.info("Parsed %d bars from %s", len(bars), csv_path.name)
    return bars


async def import_history() -> None:
    """
    Основная корутина импорта:

    - открывает PriceDB;
    - проходит по каталогам внутри MNQ_5s_ALL_daily (каждый = инструмент/таблица);
    - читает все *.csv, парсит их и вставляет бары в таблицу.
    """
    if not CSV_ROOT.exists() or not CSV_ROOT.is_dir():
        raise RuntimeError(f"Каталог с историей не найден: {CSV_ROOT}")

    db = PriceDB(PRICE_DB_PATH)
    await db.connect()

    try:
        # Перебираем подкаталоги: MNQM5, MNQU5, MNQZ5 и т.п.
        for instrument_dir in sorted(CSV_ROOT.iterdir()):
            if not instrument_dir.is_dir():
                continue

            instrument = instrument_dir.name
            logger.info("=== Importing instrument: %s ===", instrument)

            # Создаём таблицу для инструмента, если её ещё нет
            await db.ensure_table(instrument)

            total_inserted = 0

            # Перебираем CSV-файлы внутри папки инструмента
            for csv_path in sorted(instrument_dir.glob("*.csv")):
                try:
                    bars = parse_csv_file(csv_path)
                except Exception as exc:
                    logger.error(
                        "Ошибка при разборе файла %s: %s",
                        csv_path,
                        exc,
                    )
                    continue

                if not bars:
                    logger.info("No bars parsed from %s, skipping.", csv_path.name)
                    continue

                # Вставляем пачкой через PriceDB
                inserted = await db.insert_bars(instrument, bars)
                total_inserted += inserted
                logger.info(
                    "Inserted %d bars from %s into table %s",
                    inserted,
                    csv_path.name,
                    instrument,
                )

            logger.info(
                "=== Completed instrument %s: total inserted %d bars ===",
                instrument,
                total_inserted,
            )
    finally:
        await db.close()
        logger.info("PriceDB connection closed.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger.info("Starting MNQ history import from %s", CSV_ROOT)

    asyncio.run(import_history())

    logger.info("MNQ history import finished.")


if __name__ == "__main__":
    main()
