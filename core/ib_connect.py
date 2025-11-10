from __future__ import annotations

import asyncio
import logging
from datetime import timezone
from typing import Optional

from ib_insync import IB

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


class IBConnect:
    """
    Async-коннектор к IB (ib_insync) с автопереподключением и корректным shutdown.
    Без датакласса параметров, без hasattr — всё явно.
    """

    def __init__(
            self,
            host: str = "127.0.0.1",
            port: int = 7496,
            client_id: int = 101,
            connect_timeout: float = 15.0,
            keepalive_sec: float = 30.0,
            max_backoff: float = 30.0,
            loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        # Параметры соединения — как прямые атрибуты
        self.host = host
        self.port = port
        self.client_id = client_id
        self.connect_timeout = connect_timeout
        self.keepalive_sec = keepalive_sec
        self.max_backoff = max_backoff

        self.loop = loop or asyncio.get_event_loop()
        self.ib = IB()

        self._connected_evt = asyncio.Event()
        self._disconnected_evt = asyncio.Event()
        self._shutdown = False
        self._server_time_utc: Optional[int] = None

        # Событие разрыва — подключаемся напрямую
        self.ib.disconnectedEvent += self._on_ib_disconnected

        # Keepalive
        self._keepalive_task: Optional[asyncio.Task] = None
        self._backoff = 1.0

    # --------- Свойства ---------

    @property
    def is_connected(self) -> bool:
        return bool(self.ib.isConnected())

    @property
    def server_time(self) -> Optional[int]:
        return self._server_time_utc

    # --------- Основной цикл ---------

    async def run_forever(self) -> None:
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
                await asyncio.wait_for(self._disconnected_evt.wait(), timeout=self.keepalive_sec)
            except asyncio.TimeoutError:
                await self._do_keepalive()
            else:
                if self._shutdown:
                    break
                logger.warning("Disconnected. Will attempt to reconnect…")
                await self._teardown()
                self._connected_evt.clear()
                self._disconnected_evt.clear()

        await self._teardown()
        logger.info("IBConnect stopped.")

    async def wait_connected(self, timeout: Optional[float] = None) -> bool:
        try:
            await asyncio.wait_for(self._connected_evt.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def shutdown(self) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        self._disconnected_evt.set()
        await asyncio.sleep(0)
        await self._teardown()

    # --------- Внутреннее ---------

    async def _connect_once(self) -> None:
        logger.info(f"Connecting to IB {self.host}:{self.port} clientId={self.client_id}")
        await asyncio.wait_for(
            self.ib.connectAsync(self.host, self.port, clientId=self.client_id),
            timeout=self.connect_timeout,
        )
        if not self.is_connected:
            raise RuntimeError("Not connected after connectAsync()")

        await self._do_keepalive(initial=True)
        self._start_keepalive()
        self._connected_evt.set()

    async def _teardown(self) -> None:
        # Остановить keepalive (идемпотентно)
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            self._keepalive_task = None

        # Разорвать соединение (идемпотентно)
        if self.ib.isConnected():
            try:
                self.ib.disconnect()
            except Exception as e:
                logger.debug(f"Exception on IB.disconnect(): {e}")

        self._server_time_utc = None

    def _start_keepalive(self) -> None:
        if self._keepalive_task is None:
            self._keepalive_task = self.loop.create_task(self._keepalive_loop())

    async def _keepalive_loop(self) -> None:
        try:
            while not self._shutdown and self.is_connected:
                await asyncio.sleep(self.keepalive_sec)
                await self._do_keepalive()
        except asyncio.CancelledError:
            pass

    async def _do_keepalive(self, initial: bool = False) -> None:
        if not self.is_connected or self._shutdown:
            return
        try:
            dt = await self.ib.reqCurrentTimeAsync()
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

    # --------- События ---------

    def _on_ib_disconnected(self) -> None:
        self._mark_disconnected()

    def _mark_disconnected(self) -> None:
        if not self._disconnected_evt.is_set():
            self._disconnected_evt.set()
        if self._connected_evt.is_set():
            self._connected_evt.clear()
