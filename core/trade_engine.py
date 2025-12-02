import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Optional, List

from ib_insync import IB, Contract, MarketOrder, Trade, Fill, CommissionReport

log = logging.getLogger(__name__)

Side = Literal["BUY", "SELL"]


@dataclass(slots=True)
class TradeRequest:
    """
    Запрос на открытие/закрытие позиции.
    Это "снимок намерения" в момент отправки ордера.
    """
    contract: Contract
    action: Side
    quantity: int
    expected_price: Optional[float] = None
    submitted_at: datetime = datetime.now(timezone.utc)


@dataclass(slots=True)
class TradeFillInfo:
    """
    Информация по отдельному исполнению (fill).
    """
    exec_id: str
    time: Optional[datetime]
    price: float
    size: float
    commission: Optional[float]
    realized_pnl: Optional[float]


@dataclass(slots=True)
class TradeResult:
    """
    Итог по ордеру после его завершения.
    """
    order_id: int
    status: str
    filled: float
    avg_fill_price: float
    fills: List[TradeFillInfo]
    total_commission: float
    realized_pnl: float


class TradeEngine:
    """
    Простой торговый движок поверх уже существующего IB-подключения.

    Ничего не знает о стратегиях и Telegram – только ставит ордера
    и возвращает данные о:
      - факте отправки (TradeRequest),
      - итоговом исполнении (TradeResult).
    """

    def __init__(self, ib: IB, logger: Optional[logging.Logger] = None) -> None:
        self._ib = ib
        self._log = logger or log

    @property
    def ib(self) -> IB:
        """
        Прямой доступ к объекту IB при необходимости.
        """
        return self._ib

    async def _wait_trade_done(self, trade: Trade, timeout: float = 60.0) -> None:
        """
        Дождаться завершения ордера (Filled / Cancelled / Inactive).

        ВАЖНО: не трогаем внутренний event loop ib_insync (никаких waitOnUpdate),
        просто опрашиваем trade.isDone(), пока наш IBConnect.run_forever()
        крутит сетевой цикл в фоне.
        """
        loop_time = asyncio.get_running_loop().time
        deadline = loop_time() + timeout

        while not trade.isDone():
            if loop_time() >= deadline:
                raise TimeoutError(
                    f"Order {trade.order.orderId} is not done after {timeout} seconds"
                )
            await asyncio.sleep(0.1)

    @staticmethod
    def _collect_fills(trade: Trade) -> list[TradeFillInfo]:
        """
        Собрать информацию по всем fills и комиссиям.
        """
        fills: list[Fill] = list(trade.fills)

        result: list[TradeFillInfo] = []
        for f in fills:
            cr: Optional[CommissionReport] = getattr(f, "commissionReport", None)

            result.append(
                TradeFillInfo(
                    exec_id=f.execution.execId,
                    time=f.execution.time,
                    price=f.execution.price,
                    size=f.execution.shares,
                    commission=cr.commission if cr is not None else None,
                    realized_pnl=cr.realizedPNL if cr is not None else None,
                )
            )

        return result

    @staticmethod
    def _aggregate_commission_and_pnl(
        fills: list[TradeFillInfo],
    ) -> tuple[float, float]:
        """
        Посчитать суммарную комиссию и реализованный PnL по списку fills.
        """
        total_commission = 0.0
        realized_pnl = 0.0

        for f in fills:
            if f.commission is not None:
                total_commission += f.commission
            if f.realized_pnl is not None:
                realized_pnl += f.realized_pnl

        return total_commission, realized_pnl

    async def market_order(
        self,
        contract: Contract,
        action: Side,
        quantity: int,
        expected_price: Optional[float] = None,
        timeout: float = 60.0,
    ) -> tuple[TradeRequest, TradeResult]:
        """
        Поставить маркет-ордер и дождаться завершения.

        Возвращает:
          - TradeRequest: факт отправки ордера;
          - TradeResult: итог исполнения.

        Ничего не шлет в Telegram, только данные. Комиссии и realized PnL берутся
        из fills/commissionReport (если IB их отдает).
        """
        if quantity <= 0:
            raise ValueError("quantity must be positive")

        request = TradeRequest(
            contract=contract,
            action=action,
            quantity=quantity,
            expected_price=expected_price,
            submitted_at=datetime.now(timezone.utc),
        )

        local_symbol = contract.localSymbol or contract.symbol

        self._log.info(
            "Sending market order: %s %s x %s (expected_price=%s)",
            action,
            local_symbol,
            quantity,
            expected_price,
        )

        order = MarketOrder(action, quantity)
        trade: Trade = self._ib.placeOrder(contract, order)

        # Ждем, пока ордер полностью обработается
        await self._wait_trade_done(trade, timeout=timeout)

        # Собираем fills и комиссии
        fills = self._collect_fills(trade)
        total_commission, realized_pnl = self._aggregate_commission_and_pnl(fills)

        status = trade.orderStatus.status
        filled = float(trade.orderStatus.filled or 0.0)
        avg_price = float(trade.orderStatus.avgFillPrice or 0.0)

        self._log.info(
            "Order %s done: status=%s, filled=%.0f, avg_price=%.2f, "
            "total_commission=%.2f, realized_pnl=%.2f",
            trade.order.orderId,
            status,
            filled,
            avg_price,
            total_commission,
            realized_pnl,
        )

        result = TradeResult(
            order_id=trade.order.orderId,
            status=status,
            filled=filled,
            avg_fill_price=avg_price,
            fills=fills,
            total_commission=total_commission,
            realized_pnl=realized_pnl,
        )

        return request, result

    async def buy_market(
        self,
        contract: Contract,
        quantity: int,
        expected_price: Optional[float] = None,
        timeout: float = 60.0,
    ) -> tuple[TradeRequest, TradeResult]:
        """
        Маркет-покупка.
        """
        return await self.market_order(
            contract=contract,
            action="BUY",
            quantity=quantity,
            expected_price=expected_price,
            timeout=timeout,
        )

    async def sell_market(
        self,
        contract: Contract,
        quantity: int,
        expected_price: Optional[float] = None,
        timeout: float = 60.0,
    ) -> tuple[TradeRequest, TradeResult]:
        """
        Маркет-продажа.
        """
        return await self.market_order(
            contract=contract,
            action="SELL",
            quantity=quantity,
            expected_price=expected_price,
            timeout=timeout,
        )


if __name__ == "__main__":
    # ТЕСТОВЫЙ СЦЕНАРИЙ:
    # - поднимаем отдельный IBConnect (clientId=102, чтобы не конфликтовать с роботом 101);
    # - покупаем 1 контракт MNQZ5 маркет-ордером;
    # - выводим в консоль:
    #     * данные TradeRequest (факт отправки),
    #     * данные TradeResult (итог исполнения).

    import logging
    from ib_insync import Future
    from core.ib_connect import IBConnect  # импорт только для теста

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    async def _main_test() -> None:
        ib_conn = IBConnect(
            host="127.0.0.1",
            port=7496,
            client_id=102,  # отдельный clientId для теста
        )

        connector_task = asyncio.create_task(
            ib_conn.run_forever(),
            name="ib_connector_test",
        )

        try:
            connected = await ib_conn.wait_connected(timeout=15)
            if not connected:
                print("Не удалось подключиться к IB за 15 секунд.")
                return

            print(f"Подключились к IB, server_time={ib_conn.server_time}")

            engine = TradeEngine(ib_conn.client)

            # Тестируем маркет-покупку 1 контракта MNQZ5.
            contract = Future(
                localSymbol="MNQZ5",
                exchange="CME",
                currency="USD",
            )

            print("Отправляем маркет-покупку 1 MNQZ5...")

            req, res = await engine.sell_market(
                contract=contract,
                quantity=1,
                expected_price=None,
            )

            # Сообщение о факте отправки (по данным TradeRequest)
            print(
                f"Заявка отправлена: {req.action} {req.quantity} x "
                f"{contract.localSymbol or contract.symbol}, "
                f"ожид. цена={req.expected_price}, "
                f"время отправки={req.submitted_at}"
            )

            # Итог исполнения (по данным TradeResult)
            print(
                f"Исполнение: order_id={res.order_id}, status={res.status}, "
                f"filled={res.filled}, avg_price={res.avg_fill_price}, "
                f"commission={res.total_commission}, "
                f"realized_pnl={res.realized_pnl}"
            )

        finally:
            await ib_conn.shutdown()
            connector_task.cancel()
            await asyncio.gather(connector_task, return_exceptions=True)

    asyncio.run(_main_test())
