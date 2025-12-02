import asyncio
import logging
from dataclasses import dataclass

from core.ib_connect import IBConnect
from core.telegram import TelegramChannel

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PositionKey:
    """
    Ключ для идентификации позиции в портфеле.

    Берём:
      - счёт (account),
      - localSymbol (для фьючерса это как раз MNQZ5 и т.п.),
      - валюту инструмента.
    """
    account: str
    local_symbol: str
    currency: str


def _make_key(pos) -> PositionKey:
    """
    Построить ключ позиции из объекта ib_insync.Position.
    """
    contract = pos.contract

    local_symbol = getattr(contract, "localSymbol", None) or getattr(
        contract, "symbol", ""
    ) or str(getattr(contract, "conId", ""))

    currency = getattr(contract, "currency", "") or ""

    return PositionKey(
        account=pos.account,
        local_symbol=local_symbol,
        currency=currency,
    )


def _fmt_qty(qty: float) -> str:
    """
    Красивое форматирование количества: +5, -3, +0, -0.5 и т.п.
    """
    q = float(qty)
    if q.is_integer():
        val = int(q)
    else:
        val = q
    sign = "+" if val > 0 else ""
    return f"{sign}{val}"


async def portfolio_monitor_loop(
        ib: IBConnect,
        tg_common: TelegramChannel,
        cancel_event: asyncio.Event,
        poll_interval: float = 10.0,
) -> None:
    """
    Периодически опрашивает позиции в IB и сообщает в общий Telegram-канал
    об изменениях портфеля (открытие/закрытие/изменение/разворот позиций).

    Состояние портфеля хранится только в памяти, между запусками робота не сохраняется.
    """
    logger.info(
        "Portfolio monitor started with poll_interval=%.1f seconds",
        poll_interval,
    )

    last_snapshot: dict[PositionKey, float] | None = None

    try:
        while not cancel_event.is_set():
            # Ждём живого подключения к IB
            if not ib.is_connected:
                await asyncio.sleep(poll_interval)
                continue

            try:
                positions = ib.client.positions()
            except Exception as exc:
                logger.error("Portfolio monitor: failed to get positions: %s", exc)
                await asyncio.sleep(poll_interval)
                continue

            # Собираем текущий снимок: только непустые позиции
            current: dict[PositionKey, float] = {}
            for pos in positions:
                qty = float(getattr(pos, "position", 0.0) or 0.0)
                if qty == 0:
                    continue

                key = _make_key(pos)
                current[key] = qty

            if last_snapshot is None:
                # Первая инициализация — просто запоминаем состояние, ничего не шлём:
                # стартовый портфель уже ушёл через build_initial_connect_message.
                last_snapshot = current
            else:
                changes: list[str] = []

                all_keys = set(last_snapshot.keys()) | set(current.keys())

                for key in sorted(
                        all_keys,
                        key=lambda k: (k.account, k.local_symbol, k.currency),
                ):
                    prev_qty = last_snapshot.get(key, 0.0)
                    curr_qty = current.get(key, 0.0)

                    if prev_qty == curr_qty:
                        continue

                    prev_s = _fmt_qty(prev_qty)
                    curr_s = _fmt_qty(curr_qty)

                    if prev_qty == 0 and curr_qty != 0:
                        # Новая позиция
                        changes.append(
                            f"Новая позиция:\nсчёт {key.account}\n{curr_s} {key.local_symbol} ({key.currency})"
                        )
                    elif prev_qty != 0 and curr_qty == 0:
                        # Позиция закрыта
                        changes.append(
                            f"Позиция закрыта:\nсчёт {key.account}\nбыло {prev_s} {key.local_symbol} ({key.currency}), стало 0"
                        )
                    elif prev_qty * curr_qty < 0:
                        # Разворот (смена знака позиции)
                        changes.append(
                            f"Разворот позиции:\nсчёт {key.account}\nбыло {prev_s}, стало {curr_s} {key.local_symbol} ({key.currency}),"
                        )
                    else:
                        # Изменение размера позиции
                        changes.append(
                            f"Изменение позиции\nсчёт {key.account}\nбыло {prev_s}, стало {curr_s} {key.local_symbol} ({key.currency}),"
                        )

                if changes:
                    text = "Изменение портфеля:\n" + "\n".join(changes)
                    try:
                        await tg_common.send(text)
                    except Exception as exc:
                        logger.error(
                            "Portfolio monitor: failed to send Telegram message: %s",
                            exc,
                        )

                # Обновляем baseline
                last_snapshot = current

            await asyncio.sleep(poll_interval)

    except asyncio.CancelledError:
        logger.info("Portfolio monitor task cancelled, exiting.")
        raise
    except Exception as exc:
        logger.error("Portfolio monitor crashed with error: %s", exc)
    finally:
        logger.info("Portfolio monitor stopped.")
