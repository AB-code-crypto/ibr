import asyncio
import logging
from datetime import timezone
from typing import Optional

from ib_insync import IB  # pip install ib_insync

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


class IBConnect:
    """
    Async-коннектор к IB (ib_insync) с автопереподключением, heartbeat и корректным shutdown.

    Публичный интерфейс:
    - run_forever()     — главный цикл поддержания соединения
    - wait_connected()  — ожидание подключения
    - shutdown()        — корректная остановка
    - is_connected      — свойство: состояние соединения
    - server_time       — последний server time (UTC epoch)
    - client            — официальный доступ к низкоуровневому ib_insync.IB
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7496,
        client_id: int = 101,
        connect_timeout: float = 15.0,
        keepalive_sec: float = 30.0,
        max_backoff: float = 30.0,
    ):
        # Параметры соединения
        self.host = host
        self.port = port
        self.client_id = client_id
        self.connect_timeout = connect_timeout
        self.keepalive_sec = keepalive_sec
        self.max_backoff = max_backoff

        # Низкоуровневый клиент IB — внутреннее поле
        self._ib = IB()

        # События/флаги состояния
        self._connected_evt = asyncio.Event()
        self._disconnected_evt = asyncio.Event()
        self._shutdown = False
        self._server_time_utc: Optional[int] = None

        # Подписка на событие разрыва соединения
        self._ib.disconnectedEvent += self._on_ib_disconnected

        # Keepalive
        self._keepalive_task: Optional[asyncio.Task] = None
        self._backoff = 1.0

    # --------- Публичные свойства ---------

    @property
    def is_connected(self) -> bool:
        """True, если IB-соединение активно."""
        return bool(self._ib.isConnected())

    @property
    def server_time(self) -> Optional[int]:
        """Последний server time (UTC epoch секунд)."""
        return self._server_time_utc

    @property
    def client(self) -> IB:
        """
        Официальный доступ к низкоуровневому ib_insync.IB.

        Использовать его, если нужно вызвать какие-то специфические методы IB,
        которые мы пока не обернули в IBConnect (подписки, ордера и т.д.).
        """
        return self._ib

    # --------- Основной цикл ---------

    async def run_forever(self) -> None:
        """
        Главный цикл коннектора:
        - пытается подключиться к IB (с backoff-повторами)
        - держит heartbeat
        - переподключается при разрывах
        """
        while not self._shutdown:
            if not self.is_connected:
                try:
                    await self._connect_once()
                    self._backoff = 1.0
                    logger.info("IB connected.")
                except Exception as e:
                    if self._shutdown:
                        break
                    logger.error(f"Connect attempt failed: {e}")
                    await asyncio.sleep(self._backoff)
                    self._backoff = min(self._backoff * 2.0, self.max_backoff)
                    continue

            try:
                # Ждём или разрыва, или тайм-аута для keepalive
                await asyncio.wait_for(self._disconnected_evt.wait(), timeout=self.keepalive_sec)
            except asyncio.TimeoutError:
                # Тайм-аут — делаем heartbeat
                await self._do_keepalive()
            else:
                # Получили сигнал разрыва
                if self._shutdown:
                    break
                logger.warning("Disconnected. Will attempt to reconnect…")
                await self._teardown()
                self._connected_evt.clear()
                self._disconnected_evt.clear()

        # Финальная зачистка
        await self._teardown()
        logger.info("IBConnect stopped.")

    async def wait_connected(self, timeout: Optional[float] = None) -> bool:
        """
        Дождаться подключения. Возвращает True/False по тайм-ауту.
        """
        try:
            await asyncio.wait_for(self._connected_evt.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def shutdown(self) -> None:
        """
        Грейсфул-остановка:
        - помечаем shutdown
        - будим run_forever
        - аккуратно отключаемся от IB и останавливаем heartbeat
        """
        if self._shutdown:
            return
        self._shutdown = True
        self._disconnected_evt.set()
        # Даем loop'у тик, чтобы run_forever увидел события
        await asyncio.sleep(0)
        await self._teardown()

    # --------- Внутренние методы подключения / закрытия ---------

    async def _connect_once(self) -> None:
        """Одна попытка подключения к IB."""
        logger.info(f"Connecting to IB {self.host}:{self.port} clientId={self.client_id}")
        await asyncio.wait_for(
            self._ib.connectAsync(self.host, self.port, clientId=self.client_id),
            timeout=self.connect_timeout,
        )
        if not self.is_connected:
            raise RuntimeError("Not connected after connectAsync()")

        # Первый heartbeat + запуск фонового keepalive
        await self._do_keepalive(initial=True)
        self._start_keepalive()
        self._connected_evt.set()

    async def _teardown(self) -> None:
        """Остановить keepalive и разорвать соединение."""
        # Остановить keepalive (идемпотентно)
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            self._keepalive_task = None

        # Разорвать соединение (идемпотентно)
        if self._ib.isConnected():
            try:
                self._ib.disconnect()
            except Exception as e:
                logger.debug(f"Exception on IB.disconnect(): {e}")

        self._server_time_utc = None

    # --------- Keepalive / heartbeat ---------

    def _start_keepalive(self) -> None:
        """Запустить фоновую таску heartbeat."""
        if self._keepalive_task is None:
            loop = asyncio.get_running_loop()
            self._keepalive_task = loop.create_task(self._keepalive_loop())

    async def _keepalive_loop(self) -> None:
        """Фоновый цикл heartbeat: раз в keepalive_sec шлём reqCurrentTimeAsync()."""
        try:
            while not self._shutdown and self.is_connected:
                await asyncio.sleep(self.keepalive_sec)
                await self._do_keepalive()
        except asyncio.CancelledError:
            pass

    async def _do_keepalive(self, initial: bool = False) -> None:
        """
        Одно heartbeat-действие:
        - вызываем reqCurrentTimeAsync()
        - обновляем server_time
        - при ошибке помечаем разрыв для переподключения
        """
        if not self.is_connected or self._shutdown:
            return
        try:
            dt = await self._ib.reqCurrentTimeAsync()
            if dt is not None:
                if dt.tzinfo is None:
                    epoch = int(dt.replace(tzinfo=timezone.utc).timestamp())
                else:
                    epoch = int(dt.timestamp())
                self._server_time_utc = epoch
                if initial:
                    logger.info(f"Server time (UTC): {epoch}")
        except Exception as e:
            logger.error(f"Keepalive failed: {e}")
            self._mark_disconnected()

    # --------- События разрыва ---------

    def _on_ib_disconnected(self) -> None:
        """Коллбек ib_insync при разрыве соединения."""
        self._mark_disconnected()

    def _mark_disconnected(self) -> None:
        """Пометить соединение как разорванное и разбудить основной цикл."""
        if not self._disconnected_evt.is_set():
            self._disconnected_evt.set()
        if self._connected_evt.is_set():
            self._connected_evt.clear()
