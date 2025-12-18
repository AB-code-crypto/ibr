import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Mapping, Optional, Sequence

from ib_insync import IB, Contract, Trade
from ib_insync.order import (
    Order,
    MarketOrder,
    LimitOrder,
    StopOrder,
    StopLimitOrder,
)

log = logging.getLogger(__name__)

Side = Literal["BUY", "SELL"]


# =========================
# DTOs (тонкий контракт)
# =========================

@dataclass(slots=True)
class PlaceOrderRequest:
    contract: Contract
    order: Order
    order_ref: str


@dataclass(slots=True)
class PlaceOrderReceipt:
    order_id: int
    order_ref: str
    placed_at_utc: datetime
    trade: Trade


@dataclass(slots=True)
class CancelOrderReceipt:
    order_id: int
    cancel_requested_at_utc: datetime


@dataclass(slots=True)
class BracketOrders:
    parent: Order
    take_profit: Optional[Order]
    stop_loss: Optional[Order]


# =========================
# IB Order API (адаптер)
# =========================

class IBOrderApi:
    """
    Тонкий API-адаптер для IB (ib_insync).

    Принципы:
      - Только сборка ордеров + placeOrder/cancelOrder.
      - Никаких ожиданий статусов, ретраев, бизнес-правил, рисков и т.д.
      - Разрешённая "логика" здесь — это техническая сериализация параметров
        (например, преобразование ttl_seconds -> goodTillDate для GTD).

    Этот слой должен быть максимально стабильным и легко тестируемым.
    """

    def __init__(self, ib: IB, logger: Optional[logging.Logger] = None) -> None:
        self._ib = ib
        self._log = logger or log

    @property
    def ib(self) -> IB:
        return self._ib

    # -------------------------
    # Low-level primitives
    # -------------------------

    def next_order_id(self) -> int:
        """
        Зарезервировать следующий orderId у клиента IB.

        Используется для bracket/OCA, когда важно заранее выставить parentId
        и transmit-флаги до отправки.
        """
        return int(self._ib.client.getReqId())

    async def place_order(self, contract: Contract, order: Order, *, order_ref: str) -> PlaceOrderReceipt:
        """
        Отправить ордер в IB.

        Возвращает receipt отправки. "Принят/отклонён/исполнен" определяется
        отдельным слоем по событиям orderStatus/errorEvent.

        Важно: этот метод асинхронный, но не ждёт брокера — он лишь отправляет
        запрос и отдаёт управление event loop.
        """
        if not order_ref:
            raise ValueError("order_ref must be a non-empty string")

        order.orderRef = order_ref

        placed_at = datetime.now(timezone.utc)
        trade: Trade = self._ib.placeOrder(contract, order)

        # Отдать управление event loop (placeOrder может быть очень быстрым, но сеть/сокет — не гарантируется)
        await asyncio.sleep(0)

        order_id = int(trade.order.orderId)

        return PlaceOrderReceipt(
            order_id=order_id,
            order_ref=order_ref,
            placed_at_utc=placed_at,
            trade=trade,
        )

    async def cancel_order(self, order_id: int) -> CancelOrderReceipt:
        """
        Запросить отмену ордера по orderId.

        Примечание: используем низкоуровневый клиент, т.к. cancelOrder в IB-клиенте
        адресуется именно по orderId.
        """
        self._ib.client.cancelOrder(int(order_id))
        await asyncio.sleep(0)
        return CancelOrderReceipt(
            order_id=int(order_id),
            cancel_requested_at_utc=datetime.now(timezone.utc),
        )

    async def cancel_orders(self, order_ids: Sequence[int]) -> list[CancelOrderReceipt]:
        receipts: list[CancelOrderReceipt] = []
        for oid in order_ids:
            receipts.append(await self.cancel_order(int(oid)))
        return receipts

    # -------------------------
    # Builders: TIF / GTD / kwargs
    # -------------------------

    def _apply_order_kwargs(self, order: Order, order_kwargs: Optional[Mapping[str, Any]]) -> None:
        if not order_kwargs:
            return

        for key, value in order_kwargs.items():
            # Неизвестные поля должны "взрываться" сразу (никаких тихих опечаток).
            if not hasattr(order, key):
                raise AttributeError(f"IB Order has no attribute {key!r}")
            setattr(order, key, value)

    def _format_good_till_date_utc(self, dt: datetime) -> str:
        """
        Формат goodTillDate согласно IB: `YYYYMMDD-HH:MM:SS` (UTC).
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%Y%m%d-%H:%M:%S")

    def _apply_time_in_force(
            self,
            order: Order,
            *,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
    ) -> None:
        """
        Техническая установка TIF/GTD.

        - Если задан ttl_seconds или good_till -> tif=GTD + goodTillDate.
        - Иначе, если задан time_in_force -> order.tif = time_in_force.
        """
        if ttl_seconds is not None and ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")

        if ttl_seconds is not None and good_till is not None:
            raise ValueError("Use either ttl_seconds or good_till, not both")

        if ttl_seconds is not None:
            good_till = datetime.now(timezone.utc) + timedelta(seconds=int(ttl_seconds))

        if good_till is not None:
            order.tif = "GTD"
            order.goodTillDate = self._format_good_till_date_utc(good_till)
            return

        if time_in_force is not None:
            order.tif = str(time_in_force)

    # -------------------------
    # Builders: core order types
    # -------------------------

    def build_market(
            self,
            *,
            action: Side,
            quantity: int,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        order = MarketOrder(action, int(quantity))
        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def build_limit(
            self,
            *,
            action: Side,
            quantity: int,
            limit_price: float,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        order = LimitOrder(action, int(quantity), float(limit_price))
        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def build_stop(
            self,
            *,
            action: Side,
            quantity: int,
            stop_price: float,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        order = StopOrder(action, int(quantity), float(stop_price))
        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def build_stop_limit(
            self,
            *,
            action: Side,
            quantity: int,
            stop_price: float,
            limit_price: float,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        order = StopLimitOrder(action, int(quantity), float(limit_price), float(stop_price))
        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def build_trailing_stop(
            self,
            *,
            action: Side,
            quantity: int,
            trailing_percent: Optional[float] = None,
            trailing_amount: Optional[float] = None,
            trail_stop_price: Optional[float] = None,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        """
        Trailing Stop (orderType="TRAIL").

        В вашей версии ib_insync нет TrailingStopOrder, поэтому собираем через базовый Order.
        Тонкие параметры (как именно биржа/маршрут интерпретирует auxPrice/trailStopPrice)
        можно уточнять/переопределять через order_kwargs.
        """
        if trailing_percent is not None and trailing_amount is not None:
            raise ValueError("Use either trailing_percent or trailing_amount, not both")

        o = Order()
        o.action = action
        o.totalQuantity = int(quantity)
        o.orderType = "TRAIL"

        if trailing_percent is not None:
            o.trailingPercent = float(trailing_percent)

        if trailing_amount is not None:
            # Для TRAIL это часто auxPrice (trail amount).
            o.auxPrice = float(trailing_amount)

        if trail_stop_price is not None:
            o.trailStopPrice = float(trail_stop_price)

        self._apply_time_in_force(o, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(o, order_kwargs)
        return o

    # -------------------------
    # Builders: OCA / Bracket
    # -------------------------

    def apply_oca_group(
            self,
            orders: Sequence[Order],
            *,
            oca_group: str,
            oca_type: int = 1,
    ) -> None:
        """
        Присвоить группе ордеров OCA-параметры (one-cancels-all).

        ocaType:
          1 = CANCEL_WITH_BLOCK (классическое "один исполнился — остальные снять")
        """
        if not oca_group:
            raise ValueError("oca_group must be a non-empty string")

        for o in orders:
            o.ocaGroup = oca_group
            o.ocaType = int(oca_type)

    def build_bracket_limit(
            self,
            *,
            action: Side,
            quantity: int,
            limit_price: float,
            take_profit_price: Optional[float] = None,
            stop_loss_price: Optional[float] = None,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
            take_profit_order_kwargs: Optional[Mapping[str, Any]] = None,
            stop_loss_order_kwargs: Optional[Mapping[str, Any]] = None,
            oca_group: Optional[str] = None,
            oca_type: int = 1,
    ) -> BracketOrders:
        """
        Bracket на базе LIMIT (parent) + TP (limit) + SL (stop).
        """
        parent = self.build_limit(
            action=action,
            quantity=quantity,
            limit_price=limit_price,
            time_in_force=time_in_force,
            ttl_seconds=ttl_seconds,
            good_till=good_till,
            order_kwargs=order_kwargs,
        )

        parent.transmit = False

        tp: Optional[Order] = None
        sl: Optional[Order] = None

        exit_action: Side = "SELL" if action == "BUY" else "BUY"
        children: list[Order] = []

        if take_profit_price is not None:
            tp = self.build_limit(
                action=exit_action,
                quantity=quantity,
                limit_price=float(take_profit_price),
                time_in_force=time_in_force,
                ttl_seconds=ttl_seconds,
                good_till=good_till,
                order_kwargs=take_profit_order_kwargs,
            )
            tp.transmit = False
            children.append(tp)

        if stop_loss_price is not None:
            sl = self.build_stop(
                action=exit_action,
                quantity=quantity,
                stop_price=float(stop_loss_price),
                time_in_force=time_in_force,
                ttl_seconds=ttl_seconds,
                good_till=good_till,
                order_kwargs=stop_loss_order_kwargs,
            )
            sl.transmit = False
            children.append(sl)

        if len(children) >= 2:
            if oca_group is None:
                oca_group = f"OCA_BRACKET_{self.next_order_id()}"
            self.apply_oca_group(children, oca_group=oca_group, oca_type=oca_type)

        if children:
            children[-1].transmit = True
        else:
            parent.transmit = True

        return BracketOrders(parent=parent, take_profit=tp, stop_loss=sl)

    # -------------------------
    # Convenience: placement helpers (STILL NO waiting)
    # -------------------------

    async def place_simple(
            self,
            *,
            contract: Contract,
            order: Order,
            order_ref: str,
    ) -> tuple[PlaceOrderRequest, PlaceOrderReceipt]:
        req = PlaceOrderRequest(contract=contract, order=order, order_ref=order_ref)
        rec = await self.place_order(contract, order, order_ref=order_ref)
        return req, rec

    def assign_bracket_ids(self, bracket: BracketOrders) -> None:
        parent_id = self.next_order_id()
        bracket.parent.orderId = int(parent_id)

        children: list[Order] = []
        if bracket.take_profit is not None:
            children.append(bracket.take_profit)
        if bracket.stop_loss is not None:
            children.append(bracket.stop_loss)

        for child in children:
            child_id = self.next_order_id()
            child.orderId = int(child_id)
            child.parentId = int(parent_id)

    async def place_bracket(
            self,
            *,
            contract: Contract,
            bracket: BracketOrders,
            order_ref: str,
    ) -> list[PlaceOrderReceipt]:
        self.assign_bracket_ids(bracket)

        orders: list[Order] = [bracket.parent]
        if bracket.take_profit is not None:
            orders.append(bracket.take_profit)
        if bracket.stop_loss is not None:
            orders.append(bracket.stop_loss)

        receipts: list[PlaceOrderReceipt] = []
        for o in orders:
            receipts.append(await self.place_order(contract, o, order_ref=order_ref))
        return receipts

    async def place_oca_group(
            self,
            *,
            contract: Contract,
            orders: Sequence[Order],
            order_ref: str,
    ) -> list[PlaceOrderReceipt]:
        receipts: list[PlaceOrderReceipt] = []
        for o in orders:
            receipts.append(await self.place_order(contract, o, order_ref=order_ref))
        return receipts


if __name__ == "__main__":
    """
    Ручная проверка отправки ордеров через IBOrderApi.

    Принципы:
      - Никаких CLI-аргументов.
      - Каждый запуск выполняет ОДИН выбранный сценарий (чтобы тестировать "по одному").
      - Никакой авто-отмены: вы смотрите глазами в TWS, а отмену делаете отдельным кейсом.

    ВАЖНО:
      - Код реально ОТПРАВЛЯЕТ ордера в IB (через TWS/IB Gateway).
      - MARKET-ордера исполняются сразу. Используйте их осторожно.
    """

    import logging
    from ib_insync import Future, IB, OrderState
    from core.ib_connect import IBConnect  # импорт только для ручного теста

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    # =========================
    # Базовые настройки (меняйте тут)
    # =========================

    HOST = "127.0.0.1"
    PORT = 7496
    CLIENT_ID = 102  # отдельный clientId для тестов (чтобы не конфликтовать с роботом)

    CONTRACT = Future(
        localSymbol="MNQH6",
        exchange="CME",
        currency="USD",
    )

    ORDER_REF = "manual-test-ib-order-api"

    # Базовая цена (поставьте близко к текущей цене инструмента)
    BASE_PRICE = 25175.0

    # Отступы в ПУНКТАХ (points) от BASE_PRICE.
    # В этом тестере "1 пункт" = 1.0 единица цены инструмента (то, что вы видите в котировке TWS).
    # Пример:
    #   BASE_PRICE=25056.0, GAP_POINTS=10.0  -> BUY LMT=25046.0, SELL LMT=25066.0
    GAP_POINTS = 100.0  # для LIMIT
    STOP_GAP_POINTS = 100.0  # для STOP / trigger
    STOP_LIMIT_OFFSET_POINTS = 5.0  # STP LMT: лимит "хуже" стоп-триггера на это число пунктов
    LIT_LIMIT_OFFSET_POINTS = 5.0  # LIT: лимит "хуже" триггера на это число пунктов

    # TTL для проверки GTD (лимитники/стопы). None -> не использовать GTD.
    LIMIT_TTL_SECONDS: int | None = 30

    # Для TRAIL: trailing_amount в пунктах (price units), а не в процентах
    TRAIL_AMOUNT_POINTS = 10.0

    # Квалификация контракта и печать minTick (очень помогает при "лимитники не ставятся")
    QUALIFY_CONTRACT = True
    PRINT_CONTRACT_DETAILS = True
    ROUND_PRICES_TO_MIN_TICK = True
    PRINT_IB_EVENTS = True

    # Выбор теста:
    # - "MENU" -> спросить в консоли
    # - иначе -> выполнить конкретный сценарий (ключ из списка ниже)
    TEST_CASE = "MENU"


    # =========================
    # Вспомогательные функции
    # =========================

    def _calc_prices(base: float) -> dict[str, float]:
        return {
            "limit_buy": base - float(GAP_POINTS),
            "limit_sell": base + float(GAP_POINTS),
            "stop_buy": base + float(STOP_GAP_POINTS),
            "stop_sell": base - float(STOP_GAP_POINTS),

            # Для LIT: лимит чуть "хуже" триггера (настраивается константой)
            "lit_buy_limit": (base + float(STOP_GAP_POINTS)) + float(LIT_LIMIT_OFFSET_POINTS),
            "lit_sell_limit": (base - float(STOP_GAP_POINTS)) - float(LIT_LIMIT_OFFSET_POINTS),
        }


    def _choose_case_menu(cases: list[tuple[str, str]]) -> str:
        print("\nДоступные сценарии:")
        for i, (key, desc) in enumerate(cases, start=1):
            print(f"  {i:>2}. {key:<26} — {desc}")

        while True:
            raw = input("\nВведите номер или ключ сценария: ").strip()
            if not raw:
                continue

            if raw.isdigit():
                n = int(raw)
                if 1 <= n <= len(cases):
                    return cases[n - 1][0]
                print("Неверный номер.")
                continue

            keys = {k for k, _ in cases}
            if raw in keys:
                return raw

            print("Неверный ввод. Повторите.")


    def _make_mit(*, side: Side, quantity: int, trigger_price: float) -> Order:
        o = Order()
        o.action = side
        o.totalQuantity = int(quantity)
        o.orderType = "MIT"
        o.auxPrice = float(trigger_price)
        return o


    def _make_lit(*, side: Side, quantity: int, trigger_price: float, limit_price: float) -> Order:
        o = Order()
        o.action = side
        o.totalQuantity = int(quantity)
        o.orderType = "LIT"
        o.auxPrice = float(trigger_price)
        o.lmtPrice = float(limit_price)
        return o


    def _apply_ttl_if_needed(order: Order, *, ttl: int | None) -> None:
        if ttl is None:
            return
        api._apply_time_in_force(order, ttl_seconds=int(ttl))


    def _new_oca_group(prefix: str) -> str:
        return f"{prefix}_{api.next_order_id()}"


    def _is_limitish(order: Order) -> bool:
        """
        "Лимитный" в практическом смысле: всё, что несёт lmtPrice/limit-компонент.
        Это нужно для тестового сценария CANCEL_ALL_LIMITS.
        """
        ot = str(getattr(order, "orderType", "")).upper()
        if ot in {"LMT", "STP LMT", "LIT", "LOC", "LOO", "TRAIL LIMIT"}:
            return True
        lp = getattr(order, "lmtPrice", None)
        return lp is not None


    def _print_open_trades(ib: IB) -> list[int]:
        """
        Печатает текущие открытые трейды/ордера, возвращает список orderId.
        """
        trades = list(ib.openTrades())
        if not trades:
            print("\nОткрытых ордеров нет (ib.openTrades() пуст).")
            return []

        print("\nОткрытые ордера (openTrades):")
        order_ids: list[int] = []
        for t in trades:
            o = t.order
            st = getattr(t, "orderStatus", None)
            status = getattr(st, "status", None)
            oid = int(getattr(o, "orderId", 0) or 0)
            order_ids.append(oid)

            print(
                "  - "
                f"orderId={oid} | "
                f"type={getattr(o, 'orderType', None)!r} | "
                f"action={getattr(o, 'action', None)!r} | "
                f"qty={getattr(o, 'totalQuantity', None)!r} | "
                f"lmtPrice={getattr(o, 'lmtPrice', None)!r} | "
                f"auxPrice={getattr(o, 'auxPrice', None)!r} | "
                f"tif={getattr(o, 'tif', None)!r} | "
                f"goodTillDate={getattr(o, 'goodTillDate', None)!r} | "
                f"status={status!r}"
            )
        return order_ids


    # =========================
    # Сценарии тестирования
    # =========================

    CASES: list[tuple[str, str]] = [
        # Рыночные
        ("MARKET_BUY", "BUY Market"),
        ("MARKET_SELL", "SELL Market"),

        # Лимитные
        ("LIMIT_BUY", "BUY Limit (обычный)"),
        ("LIMIT_SELL", "SELL Limit (обычный)"),
        ("LIMIT_BUY_TTL", "BUY Limit с TTL (GTD)"),
        ("LIMIT_SELL_TTL", "SELL Limit с TTL (GTD)"),

        # Стопы
        ("STOP_BUY", "BUY Stop (обычный)"),
        ("STOP_SELL", "SELL Stop (обычный)"),
        ("STOP_LIMIT_BUY", "BUY Stop-Limit"),
        ("STOP_LIMIT_SELL", "SELL Stop-Limit"),

        # Trailing
        ("TRAIL_BUY", "BUY Trailing Stop (TRAIL)"),
        ("TRAIL_SELL", "SELL Trailing Stop (TRAIL)"),

        # Touched
        ("MIT_BUY", "BUY Market-If-Touched (MIT)"),
        ("MIT_SELL", "SELL Market-If-Touched (MIT)"),
        ("LIT_BUY", "BUY Limit-If-Touched (LIT)"),
        ("LIT_SELL", "SELL Limit-If-Touched (LIT)"),

        # Связки
        ("BRACKET_LIMIT_BUY", "Bracket: BUY Limit + TP/SL (TP/SL в OCA)"),
        ("OCO_TWO_LIMITS", "OCO(OCA): BUY Limit и SELL Limit (один исполнился — второй отменился)"),
        ("OCO_ENTRY_BUY", "OCO(OCA) entry: BUY Limit ниже + BUY Stop выше"),
        ("OCO_ENTRY_SELL", "OCO(OCA) entry: SELL Limit выше + SELL Stop ниже"),

        # Инфо / отмена
        ("LIST_OPEN_ORDERS", "Показать открытые ордера (openTrades)"),
        ("CANCEL_ORDER_ID", "Снять ордер по orderId (вводится вручную)"),
        ("CANCEL_ALL_LIMITS", "Снять ВСЕ лимитные ордера (по openTrades, limit-ish)"),
        ("GLOBAL_CANCEL", "IB reqGlobalCancel(): запросить отмену ВСЕХ открытых ордеров"),

        # Диагностика
        ("WHATIF_LIMIT_BUY", "What-If: расчёт маржи/комиссий для BUY Limit (без размещения)"),
    ]


    async def _main_test() -> None:
        ib_conn = IBConnect(host=HOST, port=PORT, client_id=CLIENT_ID)
        connector_task = asyncio.create_task(ib_conn.run_forever(), name="ib_connector_test")

        try:
            connected = await ib_conn.wait_connected(timeout=15)
            if not connected:
                print("Не удалось подключиться к IB за 15 секунд.")
                return

            print(f"Подключились к IB, server_time={ib_conn.server_time}")

            global api
            api = IBOrderApi(ib_conn.client)
            ib: IB = api.ib

            # Ловим ошибки/отказы от IB прямо в консоль — это первое место, где видно, почему лимитник не принялся.
            if PRINT_IB_EVENTS:
                def _on_error(reqId: int, errorCode: int, errorString: str, contract: Contract) -> None:
                    c = getattr(contract, "localSymbol", None) or getattr(contract, "symbol", None)
                    print(f"[IB ERROR] reqId={reqId} code={errorCode} contract={c!r} msg={errorString}")

                ib.errorEvent += _on_error

                def _on_status(trade: Trade) -> None:
                    o = trade.order
                    st = trade.orderStatus
                    print(
                        f"[IB STATUS] orderId={getattr(o, 'orderId', None)} "
                        f"status={getattr(st, 'status', None)!r} filled={getattr(st, 'filled', None)} "
                        f"remaining={getattr(st, 'remaining', None)} avgFillPrice={getattr(st, 'avgFillPrice', None)}"
                    )

                ib.orderStatusEvent += _on_status

            # В 90% случаев "лимитники не ставятся" — это либо не квалифицированный контракт, либо неправильный шаг цены (minTick).
            min_tick: float | None = None
            if QUALIFY_CONTRACT:
                qualified = await ib.qualifyContractsAsync(CONTRACT)
                if not qualified:
                    print("qualifyContractsAsync вернул пусто. Проверьте CONTRACT (localSymbol/exchange/currency).")
                    return

            if PRINT_CONTRACT_DETAILS:
                details = await ib.reqContractDetailsAsync(CONTRACT)
                if details:
                    cd = details[0]
                    min_tick = float(getattr(cd, "minTick", 0.0) or 0.0) or None
                    print(
                        "ContractDetails: "
                        f"conId={getattr(CONTRACT, 'conId', None)} "
                        f"localSymbol={getattr(CONTRACT, 'localSymbol', None)!r} "
                        f"exchange={getattr(CONTRACT, 'exchange', None)!r} "
                        f"minTick={min_tick!r}"
                    )
                else:
                    print("reqContractDetailsAsync вернул пусто (не получили ContractDetails).")

            def _round_to_tick(x: float) -> float:
                if not (ROUND_PRICES_TO_MIN_TICK and min_tick):
                    return float(x)
                mt = float(min_tick)
                return round(round(float(x) / mt) * mt, 10)

            prices = {k: _round_to_tick(v) for k, v in _calc_prices(float(BASE_PRICE)).items()}

            case = TEST_CASE
            if case == "MENU":
                case = _choose_case_menu(CASES)

            print(f"\n=== RUN CASE: {case} ===")

            async def _send_one(name: str, order: Order) -> PlaceOrderReceipt:
                _req, rec = await api.place_simple(contract=CONTRACT, order=order, order_ref=ORDER_REF)
                tif = getattr(order, "tif", None)
                gtd = getattr(order, "goodTillDate", None)
                print(
                    f"[{name}] placed order_id={rec.order_id}, "
                    f"orderType={getattr(order, 'orderType', None)!r}, tif={tif!r}, goodTillDate={gtd!r}"
                )
                return rec

            if case == "MARKET_BUY":
                o = api.build_market(action="BUY", quantity=1)
                await _send_one(case, o)

            elif case == "MARKET_SELL":
                o = api.build_market(action="SELL", quantity=1)
                await _send_one(case, o)

            elif case == "LIMIT_BUY":
                o = api.build_limit(action="BUY", quantity=1, limit_price=prices["limit_buy"])
                await _send_one(case, o)

            elif case == "LIMIT_SELL":
                o = api.build_limit(action="SELL", quantity=1, limit_price=prices["limit_sell"])
                await _send_one(case, o)

            elif case == "LIMIT_BUY_TTL":
                o = api.build_limit(action="BUY", quantity=1, limit_price=prices["limit_buy"], ttl_seconds=LIMIT_TTL_SECONDS)
                await _send_one(case, o)

            elif case == "LIMIT_SELL_TTL":
                o = api.build_limit(action="SELL", quantity=1, limit_price=prices["limit_sell"], ttl_seconds=LIMIT_TTL_SECONDS)
                await _send_one(case, o)

            elif case == "STOP_BUY":
                o = api.build_stop(action="BUY", quantity=1, stop_price=prices["stop_buy"])
                await _send_one(case, o)

            elif case == "STOP_SELL":
                o = api.build_stop(action="SELL", quantity=1, stop_price=prices["stop_sell"])
                await _send_one(case, o)

            elif case == "STOP_LIMIT_BUY":
                o = api.build_stop_limit(action="BUY", quantity=1, stop_price=prices["stop_buy"],
                                         limit_price=prices["stop_buy"] + float(STOP_LIMIT_OFFSET_POINTS))
                await _send_one(case, o)

            elif case == "STOP_LIMIT_SELL":
                o = api.build_stop_limit(action="SELL", quantity=1, stop_price=prices["stop_sell"],
                                         limit_price=prices["stop_sell"] - float(STOP_LIMIT_OFFSET_POINTS))
                await _send_one(case, o)

            elif case == "TRAIL_BUY":
                trailing_amount = float(TRAIL_AMOUNT_POINTS)
                trailing_amount = _round_to_tick(trailing_amount)
                o = api.build_trailing_stop(action="BUY", quantity=1, trailing_amount=trailing_amount)
                await _send_one(case, o)

            elif case == "TRAIL_SELL":
                trailing_amount = float(TRAIL_AMOUNT_POINTS)
                trailing_amount = _round_to_tick(trailing_amount)
                o = api.build_trailing_stop(action="SELL", quantity=1, trailing_amount=trailing_amount)
                await _send_one(case, o)

            elif case == "MIT_BUY":
                o = _make_mit(side="BUY", quantity=1, trigger_price=prices["stop_buy"])
                _apply_ttl_if_needed(o, ttl=LIMIT_TTL_SECONDS)
                await _send_one(case, o)

            elif case == "MIT_SELL":
                o = _make_mit(side="SELL", quantity=1, trigger_price=prices["stop_sell"])
                _apply_ttl_if_needed(o, ttl=LIMIT_TTL_SECONDS)
                await _send_one(case, o)

            elif case == "LIT_BUY":
                o = _make_lit(side="BUY", quantity=1, trigger_price=prices["stop_buy"], limit_price=prices["lit_buy_limit"])
                _apply_ttl_if_needed(o, ttl=LIMIT_TTL_SECONDS)
                await _send_one(case, o)

            elif case == "LIT_SELL":
                o = _make_lit(side="SELL", quantity=1, trigger_price=prices["stop_sell"], limit_price=prices["lit_sell_limit"])
                _apply_ttl_if_needed(o, ttl=LIMIT_TTL_SECONDS)
                await _send_one(case, o)

            elif case == "BRACKET_LIMIT_BUY":
                br = api.build_bracket_limit(
                    action="BUY",
                    quantity=1,
                    limit_price=prices["limit_buy"],
                    take_profit_price=prices["limit_sell"],
                    stop_loss_price=prices["stop_sell"],
                    ttl_seconds=LIMIT_TTL_SECONDS,
                )
                receipts = await api.place_bracket(contract=CONTRACT, bracket=br, order_ref=ORDER_REF)
                print(f"[{case}] placed order_ids={[r.order_id for r in receipts]}")

            elif case == "OCO_TWO_LIMITS":
                o1 = api.build_limit(action="BUY", quantity=1, limit_price=prices["limit_buy"], ttl_seconds=LIMIT_TTL_SECONDS)
                o2 = api.build_limit(action="SELL", quantity=1, limit_price=prices["limit_sell"], ttl_seconds=LIMIT_TTL_SECONDS)

                group = _new_oca_group("OCO_LIMITS")
                api.apply_oca_group([o1, o2], oca_group=group, oca_type=1)

                receipts = await api.place_oca_group(contract=CONTRACT, orders=[o1, o2], order_ref=ORDER_REF)
                print(f"[{case}] ocaGroup={group!r}, placed order_ids={[r.order_id for r in receipts]}")

            elif case == "OCO_ENTRY_BUY":
                limit_o = api.build_limit(action="BUY", quantity=1, limit_price=prices["limit_buy"], ttl_seconds=LIMIT_TTL_SECONDS)
                stop_o = api.build_stop(action="BUY", quantity=1, stop_price=prices["stop_buy"], ttl_seconds=LIMIT_TTL_SECONDS)

                group = _new_oca_group("OCO_ENTRY_BUY")
                api.apply_oca_group([limit_o, stop_o], oca_group=group, oca_type=1)

                receipts = await api.place_oca_group(contract=CONTRACT, orders=[limit_o, stop_o], order_ref=ORDER_REF)
                print(f"[{case}] ocaGroup={group!r}, placed order_ids={[r.order_id for r in receipts]}")

            elif case == "OCO_ENTRY_SELL":
                limit_o = api.build_limit(action="SELL", quantity=1, limit_price=prices["limit_sell"], ttl_seconds=LIMIT_TTL_SECONDS)
                stop_o = api.build_stop(action="SELL", quantity=1, stop_price=prices["stop_sell"], ttl_seconds=LIMIT_TTL_SECONDS)

                group = _new_oca_group("OCO_ENTRY_SELL")
                api.apply_oca_group([limit_o, stop_o], oca_group=group, oca_type=1)

                receipts = await api.place_oca_group(contract=CONTRACT, orders=[limit_o, stop_o], order_ref=ORDER_REF)
                print(f"[{case}] ocaGroup={group!r}, placed order_ids={[r.order_id for r in receipts]}")

            elif case == "LIST_OPEN_ORDERS":
                _print_open_trades(ib)

            elif case == "CANCEL_ORDER_ID":
                raw = input("Введите orderId для отмены: ").strip()
                if not raw.isdigit():
                    print("Нужно целое число orderId.")
                    return
                oid = int(raw)
                rec = await api.cancel_order(oid)
                print(f"Cancel request sent: orderId={rec.order_id}, at={rec.cancel_requested_at_utc.isoformat()}")

            elif case == "CANCEL_ALL_LIMITS":
                trades = list(ib.openTrades())
                limit_ids: list[int] = []
                for t in trades:
                    o = t.order
                    oid = int(getattr(o, "orderId", 0) or 0)
                    if oid and _is_limitish(o):
                        limit_ids.append(oid)

                limit_ids = sorted(set(limit_ids))
                print(f"Найдено лимитных ордеров для отмены: {limit_ids}")
                if not limit_ids:
                    return

                s = input("Подтвердите отмену ВСЕХ этих лимитных ордеров (YES): ").strip()
                if s != "YES":
                    print("Отменено пользователем.")
                    return

                receipts = await api.cancel_orders(limit_ids)
                print(f"Cancel requests sent for order_ids={[r.order_id for r in receipts]}")

            elif case == "GLOBAL_CANCEL":
                s = input("Подтвердите GLOBAL CANCEL всех ордеров в аккаунте (YES): ").strip()
                if s != "YES":
                    print("Отменено пользователем.")
                    return
                ib.reqGlobalCancel()
                await asyncio.sleep(0)
                print("IB reqGlobalCancel() отправлен.")

            elif case == "WHATIF_LIMIT_BUY":
                o = api.build_limit(action="BUY", quantity=1, limit_price=prices["limit_buy"], ttl_seconds=LIMIT_TTL_SECONDS)
                state: OrderState = ib.whatIfOrder(CONTRACT, o)
                print("[WHATIF] orderState:")
                for field in (
                        "initMarginBefore", "initMarginChange", "initMarginAfter",
                        "maintMarginBefore", "maintMarginChange", "maintMarginAfter",
                        "equityWithLoanBefore", "equityWithLoanChange", "equityWithLoanAfter",
                        "commission", "minCommission", "maxCommission", "warningText",
                ):
                    if hasattr(state, field):
                        print(f"  - {field}: {getattr(state, field)!r}")

            else:
                raise ValueError(f"Unknown TEST_CASE: {case!r}")

        finally:
            await ib_conn.shutdown()
            connector_task.cancel()
            await asyncio.gather(connector_task, return_exceptions=True)


    asyncio.run(_main_test())
