import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List

from .ib_connect import IBConnect
from .telegram import TelegramChannel

logger = logging.getLogger(__name__)


def build_initial_connect_message(ib: IBConnect) -> str:
    """
    Формируем первое сообщение при успешном подключении:
    - статус соединения;
    - server_time (если уже есть);
    - краткая сводка по аккаунту и портфелю без доп. запросов к IB.
    """
    client = ib.client

    portfolio_items = list(client.portfolio())
    account_values = list(client.accountValues())

    lines: List[str] = []
    lines.append("IB-робот: соединение установлено ✅")

    # Server time, если уже успели получить
    if ib.server_time:
        dt_utc = datetime.fromtimestamp(ib.server_time, tz=timezone.utc)
        lines.append(f"Server time (UTC): {dt_utc:%Y-%m-%d %H:%M:%S}")

    # Аккаунты
    accounts = {item.account for item in portfolio_items if getattr(item, "account", None)}
    if not accounts and account_values:
        accounts = {v.account for v in account_values if getattr(v, "account", None)}
    if accounts:
        lines.append("Аккаунт(ы): " + ", ".join(sorted(accounts)))

    # Денежные средства по валютам (TotalCashValue / CashBalance)
    cash_map = {}
    for v in account_values or []:
        tag = getattr(v, "tag", "")
        if tag not in ("TotalCashValue", "CashBalance"):
            continue
        try:
            value = float(v.value)
        except (TypeError, ValueError):
            continue
        currency = getattr(v, "currency", "")
        cash_map[currency] = cash_map.get(currency, 0.0) + value

    if cash_map:
        lines.append("Денежные средства:")
        for curr, val in cash_map.items():
            lines.append(f"  {curr}: {val:,.2f}")

    # Позиции портфеля
    if portfolio_items:
        lines.append("Текущие позиции:")
        total_mv = 0.0
        total_upnl = 0.0
        for item in portfolio_items:
            contract = item.contract
            symbol = getattr(contract, "symbol", "N/A")
            position = item.position
            avg_cost = item.averageCost
            mkt_price = item.marketPrice
            upnl = item.unrealizedPNL
            mv = item.marketValue

            total_mv += float(mv)
            total_upnl += float(upnl)

            lines.append(
                f"  {symbol}: qty={position:.0f}, "
                f"avg={avg_cost:.4f}, mkt={mkt_price:.4f}, UPNL={upnl:,.2f}"
            )

        lines.append(f"Суммарная стоимость портфеля: {total_mv:,.2f}")
        lines.append(f"Суммарный нереализованный PnL: {total_upnl:,.2f}")
    else:
        lines.append("Портфель: открытых позиций нет.")

    return "\n".join(lines)


async def hourly_status_loop(
        ib: IBConnect,
        tg_common: TelegramChannel,
) -> None:
    """
    Раз в час (на начале каждого часа) отправляем статус соединения в общий канал.
    """
    try:
        while True:
            # Ждём до начала следующего часа
            now = datetime.now()
            next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            delay = (next_hour - now).total_seconds()
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break

            # Формируем статус
            status = "есть, всё ок ✅" if ib.is_connected else "НЕТ ❌"
            ts_local = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            lines = [
                f"IB-робот: статус на {ts_local}",
                f"Соединение с TWS: {status}",
            ]

            if ib.server_time:
                dt_utc = datetime.fromtimestamp(ib.server_time, tz=timezone.utc)
                lines.append(f"Server time (UTC): {dt_utc:%Y-%m-%d %H:%M:%S}")

            text = "\n".join(lines)

            try:
                await tg_common.send(text)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Hourly status send failed: %s", e)
                continue
    except asyncio.CancelledError:
        # Нормальное завершение при shutdown
        pass
