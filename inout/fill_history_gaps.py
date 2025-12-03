"""
Утилита для докачки "дыр" в истории 5-секундных баров из IB.

Шаги работы:
1. Сканируем БД цен на предмет дыр по каждому инструменту из futures_for_history.
2. Для каждой найденной дыры делаем запросы historicalData к IB (5-секундные бары).
3. Сохраняем полученные бары в SQLite через PriceDB.

Особенности:
- Работаем только с 5-секундными барами.
- "Штатные" перерывы рынка (ровно N часов, начинаются/заканчиваются на ровном часе)
  считаем НЕ дырами и пропускаем (аналогично scan_history_gaps.py).
- Если IB по какому-то интервалу не отдаёт бары (праздник/нет торгов) — просто
  логируем и идём дальше.
- Не лезем в историю глубже ~6 месяцев от момента запуска скрипта:
  интервалы дыр, полностью лежащие раньше этой границы, игнорируем; частично
  пересекающие границу — обрезаем по границе.

Скрипт НЕ делает никаких торговых действий, только докачивает историю.
"""

import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

from ib_insync import IB, Future

from core.config import PRICE_DB_PATH, futures_for_history
from core.price_db import PriceDB, PriceBar

# Интервал дыры в виде (start_ts, end_ts) — обе границы в секундах UNIX time (UTC)
GapInterval = Tuple[int, int]

logger = logging.getLogger(__name__)


# ============================================================================
# Утилиты для работы с SQLite и поиска дыр
# ============================================================================

def _to_unix_ts(value) -> int:
    """
    Преобразование значения из БД в целочисленный UNIX timestamp (секунды).

    Поддерживает:
    - int / float (считаем, что это уже UNIX-время);
    - str с датой/временем (формат 'YYYY-MM-DD HH:MM:SS').

    При строках считаем, что время в UTC.
    """
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    raise TypeError(f"Неизвестный тип для time_utc: {type(value)!r}, значение: {value!r}")


def _is_scheduled_gap(
    start_missing_ts: int,
    end_missing_ts: int,
    step_sec: int,
) -> bool:
    """
    Общий фильтр "штатных" перерывов.

    Правило:
    - длительность дыры (по количеству недостающих баров) кратна часу, и
    - интервал начинается ровно в HH:00:00 (UTC)
      ИЛИ заканчивается ровно в HH:59:55 (UTC).

    В таких случаях считаем, что это не дыра, а штатный перерыв
    (клиринг, выходные, праздники и т.п.).
    """
    missing_bars = (end_missing_ts - start_missing_ts) // step_sec + 1
    if missing_bars <= 0:
        return False

    duration_sec = missing_bars * step_sec
    if duration_sec % 3600 != 0:
        return False

    start_dt = datetime.fromtimestamp(start_missing_ts, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_missing_ts, tz=timezone.utc)

    starts_on_hour = (start_dt.minute == 0 and start_dt.second == 0)
    ends_before_hour = (end_dt.minute == 59 and end_dt.second == 55)

    return starts_on_hour or ends_before_hour


def scan_instrument_gaps_sqlite(
    conn: sqlite3.Connection,
    symbol: str,
    expected_step_sec: int = 5,
) -> Optional[Tuple[List[GapInterval], int, int, int]]:
    """
    Сканирует одну таблицу (инструмент) на предмет дыр в истории.

    Параметры:
        conn              - открытое соединение с SQLite
        symbol            - имя инструмента (и таблицы), например 'MNQZ5'
        expected_step_sec - ожидаемый шаг между барами в секундах (по умолчанию 5)

    Возвращает:
        (gaps, first_ts, last_ts, bars_count) или None, если данных нет.

        gaps       - список интервалов пропусков [(start_ts, end_ts), ...]
        first_ts   - метка времени первого бара (UNIX seconds)
        last_ts    - метка времени последнего бара (UNIX seconds)
        bars_count - всего баров в таблице

    Если таблица пустая — возвращает None.
    """
    sql = f'SELECT time_utc FROM "{symbol}" ORDER BY time_utc ASC;'
    cur = conn.execute(sql)

    first_row = cur.fetchone()
    if first_row is None:
        return None

    first_ts = _to_unix_ts(first_row[0])
    last_ts = first_ts
    bars_count = 1

    gaps: List[GapInterval] = []

    for row in cur:
        ts = _to_unix_ts(row[0])
        bars_count += 1

        dt = ts - last_ts
        if dt > expected_step_sec:
            # есть разрыв
            start_missing = last_ts + expected_step_sec
            end_missing = ts - expected_step_sec
            if start_missing <= end_missing:
                if not _is_scheduled_gap(start_missing, end_missing, step_sec=expected_step_sec):
                    gaps.append((start_missing, end_missing))

        last_ts = ts

    return gaps, first_ts, last_ts, bars_count


def scan_all_gaps(
    db_path,
    symbols: Optional[Sequence[str]] = None,
    expected_step_sec: int = 5,
) -> Dict[str, List[GapInterval]]:
    """
    Просканировать все (или заданные) инструменты и вернуть словарь:
        {symbol: [ (start_ts, end_ts), ... ], ... }
    """
    conn = sqlite3.connect(str(db_path))
    try:
        if symbols is None:
            symbols = list(futures_for_history.keys())

        result: Dict[str, List[GapInterval]] = {}
        for symbol in symbols:
            try:
                gaps_info = scan_instrument_gaps_sqlite(
                    conn,
                    symbol,
                    expected_step_sec=expected_step_sec,
                )
            except sqlite3.OperationalError as exc:
                logger.error("Ошибка при чтении таблицы %s: %s", symbol, exc)
                continue

            if gaps_info is None:
                continue

            gaps, first_ts, last_ts, bars_count = gaps_info
            if gaps:
                result[symbol] = gaps

        return result
    finally:
        conn.close()


# ============================================================================
# Клиент IB и маппинг инструментов
# ============================================================================

@dataclass(slots=True)
class InstrumentInfo:
    symbol: str           # код инструмента / имя таблицы, например MNQZ5
    ib_symbol: str        # корневой тикер для IB, например MNQ
    contract_month: str   # YYYYMM
    exchange: str = "CME"
    currency: str = "USD"


def build_instrument_info(symbol: str) -> InstrumentInfo:
    """
    Собрать информацию об инструменте из futures_for_history.

    Сейчас:
    - contract_month — из config.futures_for_history[symbol]['contract_month'].
    - ib_symbol:
        * по умолчанию считаем, что последние два символа локального кода —
          месяц+год (MNQZ5 -> MNQ), то есть ib_symbol = symbol[:-2];
        * при необходимости можно явно прописать ib_symbol в futures_for_history.
    """
    cfg = futures_for_history.get(symbol)
    if cfg is None:
        raise KeyError(f"Инструмент {symbol!r} не найден в futures_for_history")

    contract_month = cfg["contract_month"]

    # MNQZ5 -> MNQ, ESZ5 -> ES, MESZ5 -> MES, 6EZ5 -> 6E и т.п.
    if len(symbol) > 2:
        root_symbol = symbol[:-2]
    else:
        root_symbol = symbol

    # Даём возможность переопределить ib_symbol в конфиге, если понадобится.
    ib_symbol = cfg.get("ib_symbol", root_symbol)

    exchange = cfg.get("exchange", "CME")
    currency = cfg.get("currency", "USD")

    return InstrumentInfo(
        symbol=symbol,
        ib_symbol=ib_symbol,
        contract_month=contract_month,
        exchange=exchange,
        currency=currency,
    )


def build_ib_future(info: InstrumentInfo) -> Future:
    """
    Собрать IB-контракт для фьючерса.
    """
    return Future(
        symbol=info.ib_symbol,
        lastTradeDateOrContractMonth=info.contract_month,
        exchange=info.exchange,
        currency=info.currency,
    )


# ============================================================================
# Логика докачки истории для одного инструмента
# ============================================================================

async def fetch_bars_chunk(
    ib: IB,
    contract: Future,
    end_dt: datetime,
    duration_sec: int,
) -> List:
    """
    Запросить у IB исторические бары для одного окна.

    - end_dt — datetime (UTC) конца окна.
    - duration_sec — длительность окна в секундах (не более 3600 для 5-сек. баров).
    """
    duration_str = f"{duration_sec} S"
    bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime=end_dt,
        durationStr=duration_str,
        barSizeSetting="5 secs",
        whatToShow="TRADES",
        useRTH=False,
        formatDate=2,        # datetime в bar.date
        keepUpToDate=False,
    )
    return bars


async def fill_gaps_for_instrument(
    ib: IB,
    db: PriceDB,
    info: InstrumentInfo,
    gaps: List[GapInterval],
    step_sec: int = 5,
    max_chunk_sec: int = 3600,
) -> None:
    """
    Докачать все указанные дыры по одному инструменту.

    Для каждой дыры:
    - бьём интервал на куски не длиннее max_chunk_sec (по умолчанию 1 час);
    - для каждого куска делаем historicalData-запрос к IB;
    - фильтруем бары строго по интервалу дыры;
    - сохраняем их в БД через PriceDB.insert_bars.

    Если по какому-то куску IB не вернул ни одного бара — считаем, что там либо
    нет торгов, либо праздник/ночной простой, и просто двигаемся дальше.
    """
    if not gaps:
        return

    logger.info("Докачка %d дыр для инструмента %s", len(gaps), info.symbol)

    contract = build_ib_future(info)
    # Квалифицируем контракт у IB (подтянуть точные данные).
    qualified = await ib.qualifyContractsAsync(contract)
    if not qualified:
        logger.error("Не удалось квалифицировать контракт для %s (symbol=%s, month=%s, exch=%s)",
                     info.symbol, info.ib_symbol, info.contract_month, info.exchange)
        return
    contract = qualified[0]

    await db.ensure_table(info.symbol)

    total_inserted = 0

    for idx, (start_ts, end_ts) in enumerate(gaps, start=1):
        gap_start_utc = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        gap_end_utc = datetime.fromtimestamp(end_ts, tz=timezone.utc)

        logger.info(
            "[%s] Дыра %d/%d: %s -> %s (примерно %.2f часов)",
            info.symbol,
            idx,
            len(gaps),
            gap_start_utc.isoformat(),
            gap_end_utc.isoformat(),
            (end_ts - start_ts + step_sec) / 3600.0,
        )

        cursor_dt = gap_end_utc

        while cursor_dt > gap_start_utc:
            remaining_sec = int((cursor_dt - gap_start_utc).total_seconds()) + step_sec
            duration_sec = min(max_chunk_sec, max(step_sec, remaining_sec))

            try:
                bars = await fetch_bars_chunk(ib, contract, cursor_dt, duration_sec)
            except Exception as exc:
                logger.error(
                    "[%s] Ошибка запроса истории (конец %s, %d сек): %s",
                    info.symbol,
                    cursor_dt.isoformat(),
                    duration_sec,
                    exc,
                )
                # На всякий случай двигаемся дальше назад по времени,
                # чтобы не зациклиться на одном и том же окне.
                cursor_dt -= timedelta(seconds=duration_sec)
                continue

            if not bars:
                logger.info(
                    "[%s] По окну до %s (%d сек) IB не вернул баров (возможно, праздник/нет торгов).",
                    info.symbol,
                    cursor_dt.isoformat(),
                    duration_sec,
                )
                # Эвристика: если в окне нет ни одного бара, считаем, что дальше назад
                # по этой дыре смысла лезть нет — выходим из цикла по окну.
                break

            price_bars: List[PriceBar] = []

            gap_start_dt_utc = gap_start_utc
            gap_end_dt_utc = gap_end_utc

            for bar in bars:
                dt = bar.date  # datetime

                pb = PriceBar.from_datetime(
                    dt,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=float(getattr(bar, "volume", 0.0) or 0.0),
                )

                if pb.time_utc < gap_start_dt_utc or pb.time_utc > gap_end_dt_utc:
                    continue

                price_bars.append(pb)

            if not price_bars:
                logger.info(
                    "[%s] В окне до %s (%d сек) не оказалось баров внутри дыры (все снаружи интервала).",
                    info.symbol,
                    cursor_dt.isoformat(),
                    duration_sec,
                )
                cursor_dt -= timedelta(seconds=duration_sec)
                continue

            inserted = await db.insert_bars(info.symbol, price_bars)
            total_inserted += inserted

            logger.info(
                "[%s] Вставлено баров: %d (накопительно: %d)",
                info.symbol,
                inserted,
                total_inserted,
            )

            cursor_dt -= timedelta(seconds=duration_sec)

    logger.info(
        "Докачка для %s завершена, всего добавлено баров: %d",
        info.symbol,
        total_inserted,
    )


# ============================================================================
# Основной сценарий
# ============================================================================

async def main(symbols: Optional[Sequence[str]] = None) -> None:
    """
    Основная точка входа.

    - Опционально можно передать список инструментов (tickers), которые нужно
      докачать. Если не передан — берём все из futures_for_history.

    Дополнительно:
    - не докачиваем историю глубже, чем примерно 6 месяцев от момента запуска
      (отбрасываем/обрезаем соответствующие дыры).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if symbols is None:
        symbols = list(futures_for_history.keys())

    logger.info("Старт докачки истории. Инструменты: %s", ", ".join(symbols))

    # 1. Находим дыры в БД.
    gaps_by_symbol = scan_all_gaps(PRICE_DB_PATH, symbols=symbols, expected_step_sec=5)

    if not gaps_by_symbol:
        logger.info("Дыр в истории не найдено — делать нечего.")
        return

    # Ограничение глубины: не лезем дальше, чем ~6 месяцев назад.
    now_utc = datetime.now(timezone.utc)
    cutoff_dt = now_utc - timedelta(days=180)
    cutoff_ts = int(cutoff_dt.timestamp())
    logger.info(
        "Отсекаем дыры глубже ~6 месяцев. Граница (UTC): %s",
        cutoff_dt.isoformat(),
    )

    filtered: Dict[str, List[GapInterval]] = {}
    for symbol, gaps in gaps_by_symbol.items():
        clipped_gaps: List[GapInterval] = []
        for start_ts, end_ts in gaps:
            # Дыра полностью старше cut-off → игнорируем.
            if end_ts < cutoff_ts:
                continue
            # Частично пересекает границу → обрезаем.
            if start_ts < cutoff_ts:
                start_ts = cutoff_ts
            if start_ts <= end_ts:
                clipped_gaps.append((start_ts, end_ts))
        if clipped_gaps:
            filtered[symbol] = clipped_gaps

    gaps_by_symbol = filtered

    if not gaps_by_symbol:
        logger.info(
            "После отсечения интервалов старше ~6 месяцев дыр для докачки не осталось."
        )
        return

    logger.info(
        "Найдены дыры по инструментам (после отсечения >6 месяцев): %s",
        ", ".join(gaps_by_symbol.keys()),
    )

    # 2. Подключаемся к БД (async PriceDB) и к IB.
    db = PriceDB(PRICE_DB_PATH)
    await db.connect()

    ib = IB()
    try:
        # Параметры подключения при необходимости можно вынести в config.
        logger.info("Подключаемся к IB...")
        await ib.connectAsync(host="127.0.0.1", port=7496, clientId=101)
        logger.info("Подключение к IB установлено: %s", ib.isConnected())

        # 3. По очереди докачиваем дыры для каждого инструмента.
        for symbol, gaps in gaps_by_symbol.items():
            info = build_instrument_info(symbol)
            await fill_gaps_for_instrument(
                ib,
                db,
                info,
                gaps,
                step_sec=5,
                max_chunk_sec=3600,
            )

    finally:
        if ib.isConnected():
            ib.disconnect()
        await db.close()

    logger.info("Докачка истории завершена.")


if __name__ == "__main__":
    import sys

    # Можно передать тикеры через аргументы командной строки:
    #   python -m inout.fill_history_gaps MNQZ5 MNQM5
    # Если аргументов нет — будут обработаны все из futures_for_history.
    tickers = sys.argv[1:] or None
    asyncio.run(main(tickers))
