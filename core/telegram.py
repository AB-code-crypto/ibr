import asyncio
import logging
from typing import Optional

import aiohttp  # pip install aiohttp


logger = logging.getLogger(__name__)


class TelegramChannel:
    """
    Простой async-клиент для одного Telegram-чата.

    Использование:
        tg = TelegramChannel(token, chat_id)
        await tg.send("текст")
        ...
        await tg.close()
    """

    def __init__(
        self,
        bot_token: str,
        chat_id: int,
        *,
        request_timeout: float = 10.0,
    ) -> None:
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._request_timeout = request_timeout

        self._base_url = f"https://api.telegram.org/bot{bot_token}"
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()  # на случай одновременных send из разных тасок

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._request_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def send(
        self,
        text: str,
        *,
        parse_mode: Optional[str] = None,
        disable_notification: bool = False,
    ) -> None:
        """
        Отправка простого текстового сообщения в чат.
        """
        if not text:
            return

        async with self._lock:
            session = await self._get_session()
            url = f"{self._base_url}/sendMessage"
            payload = {
                "chat_id": self._chat_id,
                "text": text,
                "disable_notification": disable_notification,
            }
            if parse_mode:
                payload["parse_mode"] = parse_mode

            try:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error(
                            "Telegram send failed: status=%s, body=%s",
                            resp.status,
                            body,
                        )
            except asyncio.CancelledError:
                # если нас гасят при завершении — просто выходим
                raise
            except Exception as e:
                logger.error("Telegram send error: %s", e)

    async def close(self) -> None:
        """
        Закрыть HTTP-сессию. Вызывать при завершении работы робота.
        """
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
