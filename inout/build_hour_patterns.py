"""
Утилита для построения сырых часовых паттернов (hour_patterns) из БД цен.

Логика:
- Берём список фьючерсов из futures_for_history.
- Для каждого контракта проверяем, что таблица существует и в ней есть данные.
- Идём по барам в порядке времени, режем на часовые окна.
- Для каждого часа считаем лог-доходности по close и сохраняем в patterns.db.

Скрипт ничего не знает про кластерацию, только готовит банк сырых паттернов.
"""

import logging
import math
import sqlite3
from array import array
from datetime import datetime, timezone
from typing import Iterable, Tuple

from core.config import PRICE_DB_PATH, PATTERNS_DB_PATH, futures_for_history
from strategies.pattern_pirson.patterns_db import init_patterns_db, get_connection as get_patterns_conn

logger = logging.getLogger(__name__)

# Стандартные буквенные коды месяцев для фьючерсов (CME style)
_FUTURES_MONTH_CODES = set("FGHJKMNQUVXZ")


def _get_base_instrument(contract: str) -> str:
    """
    Выделить базовый тикер (root) из имени фьючерсного контракта.

    Идея:
        <ROOT><MONTH_CODE><YEAR_DIGIT>

    Примеры:
        'MNQM5'  -> 'MNQ'
        'MNQU5'  -> 'MNQ'
        'MNQZ5'  -> 'MNQ'
        'ESM5'   -> 'ES'
        'SVIX'   -> 'SVIX'  (если нет месячного суффикса).

    Алгоритм:
        - ищем первую букву из набора MONTH_CODES, после которой сразу идёт цифра;
        - всё, что слева от неё, считаем базовым тикером;
        - если ничего не нашли (не фьючерсный формат) — падаем в резервный вариант:
          берём все начальные буквы до первой цифры.
    """
    n = len(contract)

    # Попытка №1: распознать шаблон <ROOT><MONTH><DIGIT>
    for i, ch in enumerate(contract):
        if ch in _FUTURES_MONTH_CODES and i + 1 < n and contract[i + 1].isdigit():
            root = contract[:i]
            if root:
                return root
            # если по какой-то причине слева ничего нет — выходим в fallback
            break

    # Попытка №2 (fallback): просто берём начальный буквенный префикс до первой цифры.
    prefix_chars = []
    for ch in contract:
        if ch.isalpha():
            prefix_chars.append(ch)
        else:
            break

    root = "".join(prefix_chars)
    return root or contract


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?;",
        (table_name,),
    )
    return cur.fetchone() is not None


def _iter_bars_sorted(
    conn: sqlite3.Connection, table_name: str
) -> Iterable[Tuple[int, float]]:
    """
    Итерируем бары по таблице в порядке времени.

    Схема таблицы цен (см. PriceDB):
        time_utc TEXT PRIMARY KEY,  -- 'YYYY-MM-DD HH:MM:SS' в UTC
        open   REAL NOT NULL,
        high   REAL NOT NULL,
        low    REAL NOT NULL,
        close  REAL NOT NULL,
        volume REAL NOT NULL

    Здесь читаем только time_utc и close.

    Возвращаем:
        (ts, close),
    где ts — epoch seconds (UTC).
    """
    sql = f'SELECT time_utc, close FROM "{table_name}" ORDER BY time_utc ASC;'
    cur = conn.execute(sql)
    for time_str, close in cur:
        # time_str в формате 'YYYY-MM-DD HH:MM:SS' UTC
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)
        ts = int(dt.timestamp())
        yield ts, float(close)


def _process_contract(
    price_conn: sqlite3.Connection,
    patterns_conn: sqlite3.Connection,
    contract: str,
) -> None:
    """
    Нарезать бары контракта на часы и сохранить в hour_patterns.
    """
    if not _table_exists(price_conn, contract):
        logger.info("Таблица %s отсутствует в price DB — пропускаем.", contract)
        return

    logger.info("Обработка контракта %s", contract)

    instrument = _get_base_instrument(contract)
    logger.info("  Базовый инструмент (root) для %s -> %s", contract, instrument)

    # INSERT OR IGNORE: если час уже есть, повторно его не пишем
    insert_sql = """
        INSERT OR IGNORE INTO hour_patterns (
            instrument, contract, session_block,
            hour_start_utc, bars_count, returns_blob, std_hour, meta
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """

    cur_patterns = patterns_conn.cursor()

    current_hour_start_ts: int | None = None
    closes: list[float] = []

    def flush_hour() -> None:
        nonlocal current_hour_start_ts, closes
        if current_hour_start_ts is None:
            return
        if len(closes) < 2:
            # Меньше двух баров — смысла нет
            closes = []
            current_hour_start_ts = None
            return

        # Лог-доходности по close
        returns: list[float] = []
        for i in range(1, len(closes)):
            c0 = closes[i - 1]
            c1 = closes[i]
            if c0 <= 0 or c1 <= 0:
                continue
            returns.append(math.log(c1 / c0))

        if not returns:
            closes = []
            current_hour_start_ts = None
            return

        bars_count = len(closes)

        # Оценка std за час (для будущих фич, не критично)
        if len(returns) >= 2:
            mean_ret = sum(returns) / len(returns)
            std_hour = math.sqrt(
                sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1)
            )
        else:
            std_hour = 0.0

        # Кодируем в BLOB как float32
        arr = array("f", returns)
        returns_blob = arr.tobytes()

        dt = datetime.fromtimestamp(current_hour_start_ts, tz=timezone.utc)
        hour_start_utc = dt.strftime("%Y-%m-%d %H:%M:%S")
        session_block = dt.hour // 3

        cur_patterns.execute(
            insert_sql,
            (
                instrument,
                contract,
                session_block,
                hour_start_utc,
                bars_count,
                returns_blob,
                std_hour,
                None,  # meta
            ),
        )

        closes = []
        current_hour_start_ts = None

    # Основной проход по барам
    for ts, close in _iter_bars_sorted(price_conn, contract):
        hour_start_ts = (ts // 3600) * 3600

        if current_hour_start_ts is None:
            current_hour_start_ts = hour_start_ts
            closes = [close]
            continue

        if hour_start_ts != current_hour_start_ts:
            # Переключились на новый час — сбрасываем предыдущий
            flush_hour()
            current_hour_start_ts = hour_start_ts
            closes = [close]
        else:
            closes.append(close)

    # Сброс последнего часа
    flush_hour()
    patterns_conn.commit()
    logger.info("Контракт %s обработан.", contract)


def build_all_patterns() -> None:
    """
    Основная точка входа: построить паттерны по всем фьючерсам из futures_for_history.
    """
    init_patterns_db()  # гарантируем наличие patterns.db и таблиц

    price_conn = sqlite3.connect(PRICE_DB_PATH)
    patterns_conn = get_patterns_conn(PATTERNS_DB_PATH)

    try:
        for contract in futures_for_history.keys():
            _process_contract(price_conn, patterns_conn, contract)
    finally:
        price_conn.close()
        patterns_conn.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    build_all_patterns()


if __name__ == "__main__":
    main()
