import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from ib_insync import IB, Trade

log = logging.getLogger(__name__)


@dataclass(slots=True)
class IBError:
    """
    Ошибка от IB API (приходит через errorEvent).

    Для ордеров reqId обычно = orderId (именно так IB сообщает order-related ошибки),
    но на практике можно поймать id=-1 или ошибки без привязки к ордеру.
    """
    id: int
    code: int
    message: str
    time_utc: datetime


@dataclass(slots=True)
class AcceptanceResult:
    order_id: int
    status: str
    accepted: bool
    timed_out: bool
    checked_at_utc: datetime
    error: Optional[IBError] = None


@dataclass(slots=True)
class DoneResult:
    order_id: int
    status: str
    done: bool
    timed_out: bool
    checked_at_utc: datetime
    error: Optional[IBError] = None


class OrderMonitor:
    """
    Инфраструктурный слой: слушает события IB (ошибки) и предоставляет await-ожидалки,
    НЕ принимая торговых решений.

    Примечание:
      - Статусы читаем из Trade (trade.orderStatus.status, trade.isDone()).
      - Причина reject обычно приходит через errorEvent.
    """

    # Считаем "принято" (ордер встал)
    _ACCEPTED_STATUSES: frozenset[str] = frozenset({
        "PreSubmitted",
        "Submitted",
        # Переходные: на некоторых инструментах/сессиях может висеть здесь,
        # но по факту ордер уже "в системе" и появится в TWS.
        "PendingSubmit",
        "ApiPending",
    })

    # Считаем "терминал"
    _TERMINAL_STATUSES: frozenset[str] = frozenset({
        "Filled",
        "Cancelled",
        "ApiCancelled",
        "Inactive",
        "Rejected",
    })

    # Явный отказ
    _REJECT_STATUSES: frozenset[str] = frozenset({
        "Rejected",
        "Inactive",
        "Cancelled",
        "ApiCancelled",
    })

    def __init__(self, ib: IB) -> None:
        self._ib = ib
        self._errors_by_id: dict[int, IBError] = {}

        # Подписка на ошибки IB (ключевой канал диагностики reject).
        self._ib.errorEvent += self._on_error  # type: ignore[operator]

    @property
    def ib(self) -> IB:
        return self._ib

    def last_error(self, order_id: int) -> Optional[IBError]:
        return self._errors_by_id.get(int(order_id))

    def _on_error(self, *args) -> None:
        """
        Поддерживаем разные сигнатуры errorEvent:
          - (reqId, errorCode, errorString, contract)
          - (ErrorObject,) где у него есть поля id/errorCode/errorString/contract
        """
        try:
            if len(args) == 1:
                err = args[0]
                req_id = int(getattr(err, "id"))
                code = int(getattr(err, "errorCode"))
                msg = str(getattr(err, "errorString"))
            else:
                req_id = int(args[0])
                code = int(args[1])
                msg = str(args[2])
        except Exception:  # noqa: BLE001
            log.exception("Failed to parse IB errorEvent args=%r", args)
            return

        error = IBError(
            id=req_id,
            code=code,
            message=msg,
            time_utc=datetime.now(timezone.utc),
        )
        self._errors_by_id[req_id] = error
        log.warning("IB error: id=%s code=%s msg=%s", req_id, code, msg)

    async def wait_for_accept(
        self,
        trade: Trade,
        *,
        timeout: float = 5.0,
        poll_interval: float = 0.10,
    ) -> AcceptanceResult:
        """
        Ждём подтверждение постановки (accept) без ожидания исполнения.

        Успех:
          - статус в _ACCEPTED_STATUSES.

        Отказ:
          - статус в _REJECT_STATUSES.

        Таймаут:
          - ничего из вышеперечисленного не наступило за timeout.
        """
        loop_time = asyncio.get_running_loop().time
        deadline = loop_time() + float(timeout)

        order_id = int(trade.order.orderId)
        while True:
            status = str(trade.orderStatus.status or "")
            now = datetime.now(timezone.utc)

            if status in self._ACCEPTED_STATUSES:
                return AcceptanceResult(
                    order_id=order_id,
                    status=status,
                    accepted=True,
                    timed_out=False,
                    checked_at_utc=now,
                    error=self.last_error(order_id),
                )

            if status in self._REJECT_STATUSES:
                return AcceptanceResult(
                    order_id=order_id,
                    status=status,
                    accepted=False,
                    timed_out=False,
                    checked_at_utc=now,
                    error=self.last_error(order_id),
                )

            if loop_time() >= deadline:
                return AcceptanceResult(
                    order_id=order_id,
                    status=status,
                    accepted=False,
                    timed_out=True,
                    checked_at_utc=now,
                    error=self.last_error(order_id),
                )

            await asyncio.sleep(float(poll_interval))

    async def wait_for_done(
        self,
        trade: Trade,
        *,
        timeout: float = 60.0,
        poll_interval: float = 0.10,
    ) -> DoneResult:
        """
        Ждём терминал trade.isDone() / статус в _TERMINAL_STATUSES.
        """
        loop_time = asyncio.get_running_loop().time
        deadline = loop_time() + float(timeout)

        order_id = int(trade.order.orderId)
        while True:
            status = str(trade.orderStatus.status or "")
            now = datetime.now(timezone.utc)

            if trade.isDone() or status in self._TERMINAL_STATUSES:
                return DoneResult(
                    order_id=order_id,
                    status=status,
                    done=True,
                    timed_out=False,
                    checked_at_utc=now,
                    error=self.last_error(order_id),
                )

            if loop_time() >= deadline:
                return DoneResult(
                    order_id=order_id,
                    status=status,
                    done=False,
                    timed_out=True,
                    checked_at_utc=now,
                    error=self.last_error(order_id),
                )

            await asyncio.sleep(float(poll_interval))
