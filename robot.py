import asyncio
import logging
import signal

from core.ib_connect import IBConnect
from core.telegram import TelegramChannel
from core.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID_COMMON,
    TELEGRAM_CHAT_ID_TRADING,
)
from core.ib_monitoring import (
    build_initial_connect_message,
    hourly_status_loop,
)


logger = logging.getLogger(__name__)


async def main() -> None:
    """
    Главный вход в робота.

    Сейчас:
      - поднимаем коннектор к IB;
      - поднимаем два Telegram-канала (общий и торговый);
      - шлём первое сообщение с портфелем при подключении;
      - раз в час шлём статус соединения в общий канал (на начале часа);
      - ждём сигнал остановки и корректно всё закрываем.
    """

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

    # Список фоновых задач для аккуратного shutdown
    tasks: list[asyncio.Task] = []

    # 3. Запускаем фоновую задачу коннектора
    connector_task = asyncio.create_task(ib.run_forever(), name="ib_connector")
    tasks.append(connector_task)

    # 4. Ждём первичного подключения к IB
    connected = await ib.wait_connected(timeout=15)
    if connected:
        logger.info("IB initial connection established, server_time=%s", ib.server_time)

        # даём IB чуть времени получить портфель/аккаунт (без доп. запросов)
        await asyncio.sleep(1.0)

        text = build_initial_connect_message(ib)
        await tg_common.send(text)
    else:
        logger.error(
            "IB initial connection NOT established within timeout; "
            "run_forever() продолжит попытки переподключения."
        )
        await tg_common.send(
            "IB-робот: не удалось подключиться к TWS в отведённое время ⚠️"
        )

    # 5. Фоновая задача: статус соединения раз в час (в начале часа)
    hourly_task = asyncio.create_task(
        hourly_status_loop(ib, tg_common),
        name="hourly_status",
    )
    tasks.append(hourly_task)

    # 6. Ожидание сигналов остановки
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Для платформ, где нет нормальных сигналов (например, Windows)
            pass

    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("Main task cancelled, initiating shutdown.")
    finally:
        # 7. Корректно закрываем всё

        # 7.1. Сообщение в общий канал о том, что робот останавливается
        try:
            await tg_common.send("IB-робот: остановка работы (shutdown) ⏹")
        except Exception:
            pass

        # 7.2. Останавливаем коннектор
        try:
            await ib.shutdown()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        # 7.3. Закрываем Telegram-сессии
        await tg_common.close()
        await tg_trading.close()

        logger.info("Robot shutdown completed.")


if __name__ == "__main__":
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, exiting.")
