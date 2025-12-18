import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional

from ib_insync import IB, Contract, Fill, CommissionReport, Trade, Future, Stock, Forex  # type: ignore

from trading.ib_order_api import IBOrderApi, PlaceOrderReceipt, CancelOrderReceipt, BracketOrders
from trading.order_monitor import OrderMonitor, AcceptanceResult, DoneResult, IBError

log = logging.getLogger(__name__)

Side = Literal["BUY", "SELL"]
WaitMode = Literal["none", "accept", "done"]


class OrderRejectedError(RuntimeError):
    def __init__(self, *, order_id: int, status: str, error: Optional[IBError]) -> None:
        msg = f"Order rejected: order_id={order_id}, status={status}"
        if error is not None:
            msg += f", ib_code={error.code}, ib_msg={error.message}"
        super().__init__(msg)
        self.order_id = order_id
        self.status = status
        self.error = error


class OrderTimeoutError(TimeoutError):
    def __init__(self, *, order_id: int, stage: str, status: str) -> None:
        super().__init__(f"Order timeout at stage={stage}: order_id={order_id}, status={status}")
        self.order_id = order_id
        self.stage = stage
        self.status = status


@dataclass(slots=True)
class TradeFillInfo:
    """
    Детализация отдельного исполнения (fill) — для логов, аналитики и отладки.
    Совместимо по смыслу с TradeFillInfo из trade_engine.py.
    """
    exec_id: str
    time: Optional[datetime]
    price: float
    size: float
    commission: Optional[float]
    realized_pnl: Optional[float]


@dataclass(slots=True)
class OrderPlacement:
    """
    Нормализованный результат постановки (и, опционально, ожидания) ордера.
    """
    receipt: PlaceOrderReceipt
    acceptance: Optional[AcceptanceResult] = None
    done: Optional[DoneResult] = None
    fills: list[TradeFillInfo] = field(default_factory=list)
    fills_count: int = 0
    total_commission: float = 0.0
    realized_pnl: float = 0.0
    avg_fill_price: Optional[float] = None


class OrderService:
    """
    Верхний слой:
      - формирует/квалифицирует контракты,
      - строит ордера через IBOrderApi,
      - отправляет,
      - получает фидбек (accept/done) через OrderMonitor,
      - принимает базовые решения: reject/timeout -> исключение (по умолчанию).

    ВАЖНО:
      - Сервис НЕ запускает сетевой цикл IB. Это делает ваш IBConnect.run_forever().
    """

    def __init__(self, ib: IB, *, api: Optional[IBOrderApi] = None, monitor: Optional[OrderMonitor] = None) -> None:
        self._ib = ib
        self._api = api or IBOrderApi(ib)
        self._monitor = monitor or OrderMonitor(ib)

    @property
    def ib(self) -> IB:
        return self._ib

    @property
    def api(self) -> IBOrderApi:
        return self._api

    @property
    def monitor(self) -> OrderMonitor:
        return self._monitor

    # --------
    # Contract factory / resolver
    # --------

    async def qualify(self, contract: Contract) -> Contract:
        """
        Гарантируем, что у контракта есть conId и он корректно разрешён в IB.
        """
        if getattr(contract, "conId", 0):
            return contract

        res = await self._ib.qualifyContractsAsync(contract)
        if not res:
            raise RuntimeError(f"qualifyContractsAsync returned empty for contract={contract!r}")
        return res[0]

    async def future(self, *, local_symbol: str, exchange: str = "CME", currency: str = "USD") -> Contract:
        c = Future(localSymbol=local_symbol, exchange=exchange, currency=currency)
        return await self.qualify(c)

    async def stock(self, *, symbol: str, exchange: str = "SMART", currency: str = "USD") -> Contract:
        c = Stock(symbol=symbol, exchange=exchange, currency=currency)
        return await self.qualify(c)

    async def forex(self, *, pair: str) -> Contract:
        c = Forex(pair)
        return await self.qualify(c)

    # --------
    # Core placement
    # --------

    async def place(
            self,
            *,
            contract: Contract,
            order,
            order_ref: str,
            wait: WaitMode = "accept",
            accept_timeout: float = 5.0,
            done_timeout: float = 60.0,
            poll_interval: float = 0.10,
    ) -> OrderPlacement:
        """
        Универсальная постановка "любого" ордера (order может быть Order или наследник).

        wait:
          - none: только отправка (receipt)
          - accept: ждём подтверждение постановки (PreSubmitted/Submitted/PendingSubmit/ApiPending) или reject/timeout
          - done: ждём завершение (Filled/Cancelled/Inactive/Rejected/...) или timeout
        """
        contract_q = await self.qualify(contract)
        receipt = await self._api.place_order(contract_q, order, order_ref=order_ref)

        placement = OrderPlacement(receipt=receipt)

        if wait == "none":
            return placement

        acc = await self._monitor.wait_for_accept(
            receipt.trade,
            timeout=accept_timeout,
            poll_interval=poll_interval,
        )
        placement.acceptance = acc

        if not acc.accepted:
            if acc.timed_out:
                raise OrderTimeoutError(order_id=acc.order_id, stage="accept", status=acc.status)
            raise OrderRejectedError(order_id=acc.order_id, status=acc.status, error=acc.error)

        if wait == "done":
            done = await self._monitor.wait_for_done(
                receipt.trade,
                timeout=done_timeout,
                poll_interval=poll_interval,
            )
            placement.done = done
            if not done.done:
                raise OrderTimeoutError(order_id=done.order_id, stage="done", status=done.status)

            fills = list(receipt.trade.fills)
            placement.fills = self._collect_fill_infos(fills)
            placement.fills_count = len(fills)
            placement.total_commission, placement.realized_pnl = self._aggregate_commission_and_pnl(fills)
            placement.avg_fill_price = self._avg_fill_price(fills)

        return placement

    # --------
    # Convenience wrappers for common orders
    # --------

    async def buy_market(
            self,
            *,
            contract: Contract,
            quantity: int,
            order_ref: str,
            wait: WaitMode = "done",
            accept_timeout: float = 5.0,
            done_timeout: float = 60.0,
    ) -> OrderPlacement:
        o = self._api.build_market(action="BUY", quantity=int(quantity))
        return await self.place(
            contract=contract,
            order=o,
            order_ref=order_ref,
            wait=wait,
            accept_timeout=accept_timeout,
            done_timeout=done_timeout,
        )

    async def sell_market(
            self,
            *,
            contract: Contract,
            quantity: int,
            order_ref: str,
            wait: WaitMode = "done",
            accept_timeout: float = 5.0,
            done_timeout: float = 60.0,
    ) -> OrderPlacement:
        o = self._api.build_market(action="SELL", quantity=int(quantity))
        return await self.place(
            contract=contract,
            order=o,
            order_ref=order_ref,
            wait=wait,
            accept_timeout=accept_timeout,
            done_timeout=done_timeout,
        )

    async def buy_limit(
            self,
            *,
            contract: Contract,
            quantity: int,
            limit_price: float,
            order_ref: str,
            ttl_seconds: Optional[int] = None,
            time_in_force: str = "DAY",
            wait: WaitMode = "accept",
    ) -> OrderPlacement:
        o = self._api.build_limit(
            action="BUY",
            quantity=int(quantity),
            limit_price=float(limit_price),
            ttl_seconds=ttl_seconds,
            time_in_force=time_in_force,
        )
        return await self.place(contract=contract, order=o, order_ref=order_ref, wait=wait)

    async def sell_limit(
            self,
            *,
            contract: Contract,
            quantity: int,
            limit_price: float,
            order_ref: str,
            ttl_seconds: Optional[int] = None,
            time_in_force: str = "DAY",
            wait: WaitMode = "accept",
    ) -> OrderPlacement:
        o = self._api.build_limit(
            action="SELL",
            quantity=int(quantity),
            limit_price=float(limit_price),
            ttl_seconds=ttl_seconds,
            time_in_force=time_in_force,
        )
        return await self.place(contract=contract, order=o, order_ref=order_ref, wait=wait)

    async def place_bracket_limit(
            self,
            *,
            contract: Contract,
            action: Side,
            quantity: int,
            limit_price: float,
            take_profit_price: Optional[float],
            stop_loss_price: Optional[float],
            order_ref: str,
            ttl_seconds: Optional[int] = None,
            time_in_force: str = "DAY",
            accept_timeout: float = 5.0,
            atomic: bool = True,
    ) -> list[OrderPlacement]:
        """
        Сценарий: parent LMT + (TP LMT) + (SL STP), TP/SL в OCA.

        atomic=True:
          если какой-то ордер не принят (reject/timeout), отменяем остальные из этой связки.
        """
        bracket: BracketOrders = self._api.build_bracket_limit(
            action=action,
            quantity=int(quantity),
            limit_price=float(limit_price),
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
            ttl_seconds=ttl_seconds,
            time_in_force=time_in_force,
        )
        self._api.assign_bracket_ids(bracket)

        receipts = await self._api.place_bracket(
            contract=await self.qualify(contract),
            bracket=bracket,
            order_ref=order_ref,
        )

        results: list[OrderPlacement] = []
        for r in receipts:
            acc = await self._monitor.wait_for_accept(r.trade, timeout=accept_timeout)
            placement = OrderPlacement(receipt=r, acceptance=acc)
            results.append(placement)

        if atomic:
            bad = [p for p in results if not (p.acceptance and p.acceptance.accepted)]
            if bad:
                for p in results:
                    await self._api.cancel_order(p.receipt.order_id)

                bad0 = bad[0]
                acc0 = bad0.acceptance
                if acc0 and acc0.timed_out:
                    raise OrderTimeoutError(order_id=acc0.order_id, stage="accept", status=acc0.status)
                raise OrderRejectedError(
                    order_id=bad0.receipt.order_id,
                    status=acc0.status if acc0 else "",
                    error=acc0.error if acc0 else None,
                )

        return results

    async def place_oca_orders(
            self,
            *,
            contract: Contract,
            orders: list,
            oca_group: str,
            oca_type: int = 1,
            order_ref: str,
            accept_timeout: float = 5.0,
            atomic: bool = True,
    ) -> list[OrderPlacement]:
        """
        Поставить набор ордеров в OCA-группу (классический механизм IB для OCO).

        atomic=True:
          если какой-то ордер не принят (reject/timeout), отменяем остальные.
        """
        receipts = await self._api.place_oca_group(
            contract=await self.qualify(contract),
            orders=orders,
            oca_group=oca_group,
            oca_type=int(oca_type),
            order_ref=order_ref,
        )

        results: list[OrderPlacement] = []
        for r in receipts:
            acc = await self._monitor.wait_for_accept(r.trade, timeout=accept_timeout)
            results.append(OrderPlacement(receipt=r, acceptance=acc))

        if atomic:
            bad = [p for p in results if not (p.acceptance and p.acceptance.accepted)]
            if bad:
                for p in results:
                    await self._api.cancel_order(p.receipt.order_id)

                bad0 = bad[0]
                acc0 = bad0.acceptance
                if acc0 and acc0.timed_out:
                    raise OrderTimeoutError(order_id=acc0.order_id, stage="accept", status=acc0.status)
                raise OrderRejectedError(
                    order_id=bad0.receipt.order_id,
                    status=acc0.status if acc0 else "",
                    error=acc0.error if acc0 else None,
                )

        return results

    async def place_oco_orders(
            self,
            *,
            contract: Contract,
            orders: list,
            oco_group: str,
            order_ref: str,
            oca_type: int = 1,
            accept_timeout: float = 5.0,
            atomic: bool = True,
    ) -> list[OrderPlacement]:
        """
        Алиас к place_oca_orders (в терминологии IB это OCA, но по смыслу — OCO).
        """
        return await self.place_oca_orders(
            contract=contract,
            orders=orders,
            oca_group=oco_group,
            oca_type=oca_type,
            order_ref=order_ref,
            accept_timeout=accept_timeout,
            atomic=atomic,
        )

    # --------
    # Cancel helpers (ручное управление открытыми ордерами)
    # --------

    async def cancel_order_id(self, order_id: int) -> CancelOrderReceipt:
        """Отправить запрос отмены ордера по orderId (без ожидания статуса)."""
        return await self._api.cancel_order(int(order_id))

    async def cancel_order_ids(self, order_ids: list[int]) -> list[CancelOrderReceipt]:
        """Отправить запросы отмены для набора orderId (без ожидания статуса)."""
        return await self._api.cancel_orders([int(x) for x in order_ids])

    @staticmethod
    def _is_limitish_order(order) -> bool:
        """
        Практическое определение "лимитного" ордера:
        всё, что несёт limit-компонент (LMT/STP LMT/LIT/LOC/LOO/…),
        либо имеет поле lmtPrice.
        """
        ot = str(getattr(order, "orderType", "") or "").upper()
        if ot in {"LMT", "STP LMT", "LIT", "LOC", "LOO", "TRAIL LIMIT"}:
            return True
        return getattr(order, "lmtPrice", None) is not None

    def open_order_ids(self, *, only_limitish: bool = False, order_ref: Optional[str] = None) -> list[int]:
        """
        Вернуть список orderId по текущим openTrades().

        only_limitish=True:
          вернёт только "лимитные" (см. _is_limitish_order).
        order_ref:
          если задан, фильтруем по order.orderRef (удобно чистить свои тестовые/стратегические ордера).
        """
        ids: list[int] = []
        for t in list(self._ib.openTrades()):
            o = t.order
            oid = int(getattr(o, "orderId", 0) or 0)
            if not oid:
                continue

            if order_ref is not None and str(getattr(o, "orderRef", "") or "") != str(order_ref):
                continue

            if only_limitish and not self._is_limitish_order(o):
                continue

            ids.append(oid)

        # стабилизируем порядок и убираем дубликаты
        return sorted(set(ids))

    async def cancel_open_limit_orders(self, *, order_ref: Optional[str] = None) -> list[CancelOrderReceipt]:
        """
        Отменить все текущие "лимитные" ордера (по openTrades()).
        По умолчанию отменяет ВСЕ лимитные ордера, видимые для этого IB clientId.

        Если задан order_ref — снимаем только ордера с таким orderRef.
        """
        ids = self.open_order_ids(only_limitish=True, order_ref=order_ref)
        if not ids:
            return []
        return await self.cancel_order_ids(ids)

    async def cancel_all_open_orders(self, *, order_ref: Optional[str] = None) -> list[CancelOrderReceipt]:
        """
        Отменить все открытые ордера (по openTrades()).

        Если задан order_ref — снимаем только ордера с таким orderRef.
        """
        ids = self.open_order_ids(only_limitish=False, order_ref=order_ref)
        if not ids:
            return []
        return await self.cancel_order_ids(ids)

    async def global_cancel(self) -> None:
        """Отправить IB reqGlobalCancel() (глобальная отмена всех открытых ордеров в аккаунте)."""
        self._ib.reqGlobalCancel()
        await asyncio.sleep(0)

    # --------
    # Helpers: fills aggregation
    # --------

    @staticmethod
    def _collect_fill_infos(fills: list[Fill]) -> list[TradeFillInfo]:
        """
        Преобразовать ib_insync Fill -> TradeFillInfo (детализация исполнений).

        Важно:
          - CommissionReport может приходить не сразу, поэтому поля commission/realized_pnl могут быть None.
        """
        result: list[TradeFillInfo] = []
        for f in fills:
            execu = getattr(f, "execution", None)
            if execu is None:
                continue

            cr: Optional[CommissionReport] = getattr(f, "commissionReport", None)

            result.append(
                TradeFillInfo(
                    exec_id=str(getattr(execu, "execId", "")),
                    time=getattr(execu, "time", None),
                    price=float(getattr(execu, "price", 0.0) or 0.0),
                    size=float(getattr(execu, "shares", 0.0) or 0.0),
                    commission=float(cr.commission) if (cr is not None and cr.commission is not None) else None,
                    realized_pnl=float(cr.realizedPNL) if (cr is not None and cr.realizedPNL is not None) else None,
                )
            )
        return result

    @staticmethod
    def _aggregate_commission_and_pnl(fills: list[Fill]) -> tuple[float, float]:
        total_commission = 0.0
        realized_pnl = 0.0
        for f in fills:
            cr: Optional[CommissionReport] = getattr(f, "commissionReport", None)
            if cr is None:
                continue
            if cr.commission:
                total_commission += float(cr.commission)
            if cr.realizedPNL:
                realized_pnl += float(cr.realizedPNL)
        return total_commission, realized_pnl

    @staticmethod
    def _avg_fill_price(fills: list[Fill]) -> Optional[float]:
        if not fills:
            return None
        total_qty = 0.0
        total_notional = 0.0
        for f in fills:
            execu = getattr(f, "execution", None)
            if execu is None:
                continue
            shares = float(getattr(execu, "shares", 0.0) or 0.0)
            price = float(getattr(execu, "price", 0.0) or 0.0)
            total_qty += shares
            total_notional += shares * price
        if total_qty <= 0:
            return None
        return total_notional / total_qty


if __name__ == "__main__":
    """
    Ручной прогон OrderService (без авто-отмены):

    - поднимаем отдельный IBConnect (clientId=102, чтобы не конфликтовать с боевым 101);
    - квалифицируем контракт MNQH6;
    - ставим ордер выбранного типа и печатаем receipt + (опционально) статус accept/done.

    Важно:
      - BUY LIMIT выше рынка исполнится сразу.
      - Чтобы лимитник "повис" в стакане: BUY ниже рынка, SELL выше рынка.
    """
    import logging
    from ib_insync import Future
    from core.ib_connect import IBConnect  # только для теста

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # ====== НАСТРОЙКИ ТЕСТА ======
    HOST = "127.0.0.1"
    PORT = 7496
    CLIENT_ID = 102

    # Контракт (перешли на MNQH6)
    CONTRACT = Future(
        localSymbol="MNQH6",
        exchange="CME",
        currency="USD",
    )

    ORDER_REF = "order-service-test"

    # Что ставим:
    #   "MKT"  - market
    #   "LMT"  - limit
    ORDER_TYPE = "LMT"  # поменяйте на "MKT", если хотите

    SIDE: Side = "BUY"
    QTY = 1

    # Для LIMIT: поставьте цену вручную.
    LIMIT_PRICE = 25100.0

    # Для TTL на лимитнике (GTD). None = не задаём.
    TTL_SECONDS: Optional[int] = None

    # Как ждать:
    #   "none"   - просто отправили, дальше смотрите в TWS сами
    #   "accept" - дождаться, что ордер принят (Submitted/PreSubmitted/...)
    #   "done"   - дождаться терминала (Filled/Cancelled/Rejected/...)
    WAIT_MODE: WaitMode = "accept"

    # Таймауты ожидания статусов (используются только если WAIT_MODE != "none")
    ACCEPT_TIMEOUT_SEC = 5.0
    DONE_TIMEOUT_SEC = 60.0


    async def _main_test() -> None:
        ib_conn = IBConnect(host=HOST, port=PORT, client_id=CLIENT_ID)

        connector_task = asyncio.create_task(
            ib_conn.run_forever(),
            name="ib_connector_order_service_test",
        )

        try:
            connected = await ib_conn.wait_connected(timeout=15)
            if not connected:
                print("Не удалось подключиться к IB за 15 секунд.")
                return

            print(f"Подключились к IB, server_time={ib_conn.server_time}")

            svc = OrderService(ib_conn.client)

            # Квалифицируем контракт (важно для стабильной работы и диагностики)
            c = await svc.qualify(CONTRACT)

            if ORDER_TYPE == "MKT":
                order = svc.api.build_market(
                    action=SIDE,
                    quantity=QTY,
                )
            elif ORDER_TYPE == "LMT":
                order = svc.api.build_limit(
                    action=SIDE,
                    quantity=QTY,
                    limit_price=float(LIMIT_PRICE),
                    ttl_seconds=TTL_SECONDS,
                    time_in_force="GTD" if TTL_SECONDS else "DAY",
                )
            else:
                raise ValueError(f"Unknown ORDER_TYPE={ORDER_TYPE!r}")

            placement = await svc.place(
                contract=c,
                order=order,
                order_ref=ORDER_REF,
                wait=WAIT_MODE,
                accept_timeout=ACCEPT_TIMEOUT_SEC,
                done_timeout=DONE_TIMEOUT_SEC,
            )

            print(
                "PLACED:",
                {
                    "order_id": placement.receipt.order_id,
                    "order_ref": placement.receipt.order_ref,
                    "placed_at_utc": placement.receipt.placed_at_utc.isoformat(),
                    "accept_status": placement.acceptance.status if placement.acceptance else None,
                    "accept_error": (
                        {"code": placement.acceptance.error.code, "msg": placement.acceptance.error.message}
                        if placement.acceptance and placement.acceptance.error
                        else None
                    ),
                    "done_status": placement.done.status if placement.done else None,
                },
            )

        finally:
            await ib_conn.shutdown()
            connector_task.cancel()
            await asyncio.gather(connector_task, return_exceptions=True)


    asyncio.run(_main_test())