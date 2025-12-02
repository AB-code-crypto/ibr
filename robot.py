import asyncio
import logging
import signal
from datetime import timedelta

from ib_insync import Future

from core.ib_connect import IBConnect
from core.telegram import TelegramChannel
from core.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID_COMMON,
    TELEGRAM_CHAT_ID_TRADING,
    PRICE_DB_PATH,
)
from core.price_db import PriceDB
from core.ib_monitoring import (
    build_initial_connect_message,
    hourly_status_loop,
)
from core.price_get import PriceCollector, InstrumentConfig

logger = logging.getLogger(__name__)

# Список фьючерсов, для которых качаем историю (5-секундные бары)
symbols_for_history_data = ["MNQZ5", "MNQH6", "MNQM6", "MNQU6", "MNQZ6"]

# Карта квартальных кодов месяца для MNQ
_MNQ_MONTH_CODES = {
    "H": "03",  # March
    "M": "06",  # June
    "U": "09",  # September
    "Z": "12",  # December
}


def _make_mnq_future(symbol_code: str) -> Future:
    """
    Преобразовать строку вида 'MNQZ5' в контракт IB для фьючерса MNQ.

    Допущения:
      - первые 3 символа — базовый тикер ('MNQ');
      - предпоследний символ — код месяца (H/M/U/Z);
      - последний символ — последняя цифра года ('5' -> 2025, '6' -> 2026 и т.д.);
      - торгуем сейчас в 2020-х, поэтому берём 2020 + digit.
    """
    if len(symbol_code) < 5:
        raise ValueError(f"Unsupported MNQ future code: {symbol_code!r}")

    root = symbol_code[:3]  # 'MNQ'
    month_code = symbol_code[3:-1]  # 'Z', 'H', 'M', 'U'
    year_digit = symbol_code[-1]  # '5', '6', ...

    if root != "MNQ":
        raise ValueError(f"Unexpected root symbol (expected 'MNQ'): {symbol_code!r}")

    if month_code not in _MNQ_MONTH_CODES:
        raise ValueError(
            f"Unknown month code in symbol {symbol_code!r}: {month_code!r}"
        )

    month = _MNQ_MONTH_CODES[month_code]

    # Простое допущение: '5' -> 2025, '6' -> 2026 и т.п.
    year = 2020 + int(year_digit)
    last_trade_str = f"{year}{month}"  # например, '202512'

    return Future(
        symbol=root,
        lastTradeDateOrContractMonth=last_trade_str,
        exchange="CME",
        currency="USD",
    )


async def main() -> None:
    """
    Главный вход в робота.

    Сейчас:
      - поднимаем коннектор к IB;
      - поднимаем два Telegram-канала (общий и торговый);
      - поднимаем SQLite-БД для цен;
      - регистрируем обработчики сигналов (SIGINT/SIGTERM) -> stop_event;
      - при первом успешном подключении:
          * шлём сообщение с портфелем;
          * докачиваем историю 5-секундных баров по списку фьючерсов;
      - раз в час шлём статус соединения в общий канал (на начале часа);
      - ждём сигнал остановки и корректно всё закрываем.
    """
    # 0. БД цен (SQLite)
    db = PriceDB(PRICE_DB_PATH)
    await db.connect()

    # 1. Коннектор к IB
    ib = IBConnect(
        host="127.0.0.1",
        port=7496,
        client_id=101,
    )

    # 2. Telegram-каналы
    tg_common = TelegramChannel(
        bot_token=TELEGRAM_BOT_TOKEN,
        chat_id=TELEGRAM_CHAT_ID_COMMON,
    )
    tg_trading = TelegramChannel(
        bot_token=TELEGRAM_BOT_TOKEN,
        chat_id=TELEGRAM_CHAT_ID_TRADING,
    )

    # 3. Событие остановки и обработчики сигналов
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # На платформах без нормальных сигналов (Windows и т.п.) просто пропускаем
            pass

    # Список фоновых задач для аккуратного shutdown
    tasks: list[asyncio.Task] = []

    # 4. Запускаем фоновую задачу коннектора
    connector_task = asyncio.create_task(ib.run_forever(), name="ib_connector")
    tasks.append(connector_task)

    # 5. Ждём первичного подключения к IB
    connected = await ib.wait_connected(timeout=15)
    if connected:
        logger.info("IB initial connection established, server_time=%s", ib.server_time)

        # даём IB чуть времени получить портфель/аккаунт (без доп. запросов)
        await asyncio.sleep(1.0)

        # 5.1. Первое сообщение с портфелем
        text = build_initial_connect_message(ib)
        await tg_common.send(text)

        # 5.2. Синхронизация истории 5-секундных баров по списку фьючерсов
        collector = PriceCollector(ib=ib.client, db=db)

        instrument_configs: list[InstrumentConfig] = []
        for code in symbols_for_history_data:
            try:
                contract = _make_mnq_future(code)
            except ValueError as e:
                logger.error("Skip symbol %s: %s", code, e)
                continue

            cfg = InstrumentConfig(
                name=code,  # имя таблицы в БД = код фьючерса
                contract=contract,
                history_lookback=timedelta(days=1),  # сколько истории качать, если БД пустая
            )
            instrument_configs.append(cfg)

        if instrument_configs:
            logger.info(
                "Starting historical backfill for instruments: %s",
                ", ".join(cfg.name for cfg in instrument_configs),
            )
            results = await collector.sync_many(
                instrument_configs,
                chunk_seconds=3600,  # по часу 5-секундных баров за запрос
                cancel_event=stop_event,  # уважать запрос на остановку
            )
            logger.info("Historical backfill results: %r", results)
        else:
            logger.warning(
                "No valid instrument configs built for history backfill; "
                "symbols_for_history_data=%r",
                symbols_for_history_data,
            )

    else:
        logger.error(
            "IB initial connection NOT established within timeout; "
            "run_forever() продолжит попытки переподключения."
        )
        await tg_common.send(
            "IB-робот: не удалось подключиться к TWS в отведённое время ⚠️"
        )

    # 6. Фоновая задача: статус соединения раз в час (в начале часа)
    hourly_task = asyncio.create_task(
        hourly_status_loop(ib, tg_common),
        name="hourly_status",
    )
    tasks.append(hourly_task)

    # 7. Ожидание сигнала остановки
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("Main task cancelled, initiating shutdown.")
    finally:
        # 8. Корректно закрываем всё

        # 8.1. Сообщение в общий канал о том, что робот останавливается
        try:
            await tg_common.send("IB-робот: остановка работы (shutdown) ⏹")
        except Exception:
            pass

        # 8.2. Останавливаем коннектор
        try:
            await ib.shutdown()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        # 8.3. Закрываем Telegram-сессии
        await tg_common.close()
        await tg_trading.close()

        # 8.4. Закрываем БД цен
        await db.close()

        logger.info("Robot shutdown completed.")


if __name__ == "__main__":
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )

    # Здесь больше не ловим KeyboardInterrupt — SIGINT обрабатывается внутри main()
    asyncio.run(main())
