import asyncio
import logging
import signal
from datetime import timedelta, timezone

from ib_insync import Future

from core.ib_connect import IBConnect
from core.telegram import TelegramChannel
from core.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID_COMMON,
    TELEGRAM_CHAT_ID_TRADING,
    PRICE_DB_PATH,
    futures_for_history,
)
from core.price_db import PriceDB
from core.ib_monitoring import (
    build_initial_connect_message,
    hourly_status_loop,
)
from core.price_get import PriceCollector, InstrumentConfig, PriceStreamer

logger = logging.getLogger(__name__)


def _format_dt_utc(dt) -> str:
    """
    Красивое форматирование времени бара для сообщений в Telegram:
    YYYY-MM-DD HH:MM:SS UTC+0
    """
    if dt.tzinfo is None:
        dt_utc = dt.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC+0")


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
          * шлём в Telegram состояние БД по инструментам (последние даты баров);
          * загружаем/дозагружаем историю 5-секундных баров по списку фьючерсов;
          * шлём в Telegram результат загрузки (сколько баров добавлено и до какой даты);
          * запускаем real-time стриминг 5-секундных баров по тем же инструментам;
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

        for code, meta in futures_for_history.items():
            contract_month = meta.get("contract_month")
            if not contract_month:
                logger.error(
                    "Skip symbol %s: missing contract_month in futures_for_history",
                    code,
                )
                continue

            # Простейшая валидация формата 'YYYYMM'
            if not isinstance(contract_month, str) or len(contract_month) != 6 or not contract_month.isdigit():
                logger.error(
                    "Skip symbol %s: invalid contract_month=%r (expected 'YYYYMM')",
                    code,
                    contract_month,
                )
                continue

            # Единый способ построения IB-контракта:
            # symbol='MNQ', lastTradeDateOrContractMonth='YYYYMM'
            contract = Future(
                symbol="MNQ",
                lastTradeDateOrContractMonth=contract_month,
                exchange="CME",
                currency="USD",
            )

            cfg = InstrumentConfig(
                name=code,  # имя таблицы в БД = код фьючерса
                contract=contract,
                history_lookback=timedelta(days=1),  # fallback, если history_start не задан
                history_start=meta.get("history_start"),  # явный старт истории
                expiry=meta.get("expiry"),  # пока не используем, но поле есть
            )
            instrument_configs.append(cfg)

        if instrument_configs:
            # 5.2.a. Статус БД перед загрузкой
            pre_lines: list[str] = []
            for cfg in instrument_configs:
                # На всякий случай убеждаемся, что таблица есть
                await db.ensure_table(cfg.name)
                last_dt = await db.get_last_bar_datetime(cfg.name)
                if last_dt is not None:
                    pre_lines.append(
                        f"{cfg.name}: данные в БД до {_format_dt_utc(last_dt)}."
                    )

            if pre_lines:
                pre_msg = "IB-робот: состояние исторических данных перед загрузкой:\n" + "\n".join(pre_lines)
                await tg_common.send(pre_msg)

            # 5.2.b. Запускаем загрузку истории
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

            # 5.2.c. Статус БД после загрузки (только там, где что-то добавили)
            post_lines: list[str] = []
            for cfg in instrument_configs:
                inserted = results.get(cfg.name, 0)
                if inserted <= 0:
                    continue

                last_dt_after = await db.get_last_bar_datetime(cfg.name)
                if last_dt_after is not None:
                    last_dt_str = _format_dt_utc(last_dt_after)
                else:
                    last_dt_str = "нет данных"

                post_lines.append(
                    f"{cfg.name}: добавлено баров: {inserted} , последний: {last_dt_str}."
                )

            if post_lines:
                post_msg = "IB-робот: загрузка истории завершена:\n" + "\n".join(post_lines)
                await tg_common.send(post_msg)

            # 5.3. Запускаем real-time стриминг 5-секундных баров по всем инструментам
            streamer = PriceStreamer(ib=ib.client, db=db)
            for cfg in instrument_configs:
                stream_task = asyncio.create_task(
                    streamer.stream_bars(cfg, cancel_event=stop_event),
                    name=f"price_stream_{cfg.name}",
                )
                tasks.append(stream_task)
        else:
            logger.warning(
                "No valid instrument configs built for history backfill; "
                "futures_for_history=%r",
                futures_for_history,
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

        # 8.2. Останавливаем коннектор и все фоновые задачи (включая стримеры)
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
