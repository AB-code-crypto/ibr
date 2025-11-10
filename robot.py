import asyncio
import signal

from core.ib_connect import IBConnect


async def main():
    ib = IBConnect(port=7496, client_id=101)

    # Фоновая таска коннектора
    runner = asyncio.create_task(ib.run_forever(), name="ib_runner")

    # Ждём коннекта (не обязательно, но удобно для логов/простейших проверок)
    await ib.wait_connected(timeout=15)
    print("CONNECTED:", ib.is_connected, "server_time:", ib.server_time)

    # Сигналы на корректную остановку
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # На некоторых платформах сигналов может не быть
            pass

    try:
        # Ждём сигнала остановки
        await stop_event.wait()
    except asyncio.CancelledError:
        # Если PyCharm жёстко отменил таску main()
        pass
    finally:
        # Гасим аккуратно
        try:
            await ib.shutdown()
        finally:
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)


if __name__ == "__main__":
    # Оборачиваем в try/except чтобы подавить KeyboardInterrupt красивее
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # PyCharm Stop часто генерирует KeyboardInterrupt — ок.
        pass
