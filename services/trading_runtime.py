import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from contracts.bot_spec import RobotSpec, Strategy, StrategySignal
from contracts.robot_registry import RobotSwitchesStore
from core.ib_connect import IBConnect
from core.price_get import InstrumentConfig
from core.telegram import TelegramChannel
from core.trade_engine import TradeEngine
from services.quiet_windows import QuietWindowsService
from services.telegram_formatters import _format_dt_local

logger = logging.getLogger(__name__)

# Анти-спам для сообщений "сигнал заблокирован окном тишины"
BLOCKED_NOTICE_COOLDOWN_SECONDS = 300


@dataclass
class _QuietState:
    active: bool = False
    reason: Optional[str] = None
    last_notified_active: bool = False
    last_notified_reason: Optional[str] = None


async def trading_loop(
        *,
        ib: IBConnect,
        trade_engine: TradeEngine,
        strategy: Strategy,
        cfg: InstrumentConfig,
        tg_trading: TelegramChannel,
        tg_common: TelegramChannel,
        quiet_service: QuietWindowsService,
        switches: RobotSwitchesStore,
        robot_spec: RobotSpec,
        stop_event: asyncio.Event,
) -> None:
    robot_id = robot_spec.robot_id
    instrument_root = robot_spec.instrument_root

    position_side: str = "flat"  # "flat" | "long" | "short"
    position_qty: int = 0
    entry_hour_start: datetime | None = None

    quiet_state = _QuietState()
    quiet_tz = ZoneInfo("America/New_York")

    last_blocked_notice_at: Optional[datetime] = None
    last_blocked_notice_reason: Optional[str] = None

    logger.info(
        "Trading loop started: robot_id=%s, contract=%s, instrument_root=%s, qty=%s, order_ref=%s",
        robot_id,
        cfg.name,
        instrument_root,
        robot_spec.trade_qty,
        robot_spec.order_ref,
    )

    # Выравнивание по 5-сек сетке
    while not stop_event.is_set():
        now = datetime.now(timezone.utc)
        sleep_rem = 5 - (now.second % 5)
        if sleep_rem <= 0 or sleep_rem > 5:
            sleep_rem = 5
        await asyncio.sleep(sleep_rem)
        break

    while not stop_event.is_set():
        if not ib.is_connected:
            await ib.wait_connected(timeout=None)
            await asyncio.sleep(1.0)
            continue

        # LIVE-выключатель: если робот отключили — запрещаем новые входы сразу.
        enabled = await switches.is_enabled(robot_id)
        if not enabled and position_side == "flat":
            try:
                await tg_common.send(f"[{robot_id}] Робот отключён выключателем (robot_switches). Торговый цикл остановлен.")
            except Exception:
                logger.exception("Failed to notify robot disabled: %s", robot_id)
            return

        now = datetime.now(timezone.utc)

        # 1) Сообщения о тишине (entry)
        entry_decision = await quiet_service.evaluate(robot_id=robot_id, now_utc=now, action_type="entry")
        quiet_state.active = not entry_decision.allowed
        quiet_state.reason = entry_decision.reason

        if quiet_state.active and (not quiet_state.last_notified_active or quiet_state.last_notified_reason != quiet_state.reason):
            msg = (
                f"[{robot_id}] Окно тишины: вход в сделки запрещён.\n"
                f"Причина: {quiet_state.reason or 'n/a'}\n"
                f"Сейчас (NY): {_format_dt_local(now, quiet_tz)}"
            )
            try:
                await tg_common.send(msg)
            except Exception:
                logger.exception("Failed to notify quiet window start.")
            quiet_state.last_notified_active = True
            quiet_state.last_notified_reason = quiet_state.reason

        if (not quiet_state.active) and quiet_state.last_notified_active:
            msg = (
                f"[{robot_id}] Окно тишины завершено: вход в сделки снова разрешён.\n"
                f"Причина: {quiet_state.last_notified_reason or 'n/a'}\n"
                f"Сейчас (NY): {_format_dt_local(now, quiet_tz)}"
            )
            try:
                await tg_common.send(msg)
            except Exception:
                logger.exception("Failed to notify quiet window end.")
            quiet_state.last_notified_active = False
            quiet_state.last_notified_reason = None

        # --- Если есть позиция: управляем выходом, даже если робота выключили ---
        if position_side in ("long", "short") and entry_hour_start is not None:
            exit_time = entry_hour_start.replace(minute=59, second=50, microsecond=0)

            if now >= exit_time:
                exit_decision = await quiet_service.evaluate(robot_id=robot_id, now_utc=now, action_type="exit")
                if not exit_decision.allowed:
                    hard_grace = timedelta(minutes=2)
                    if now < (exit_time + hard_grace):
                        should_notify = True
                        if last_blocked_notice_at is not None:
                            if (now - last_blocked_notice_at).total_seconds() < BLOCKED_NOTICE_COOLDOWN_SECONDS and last_blocked_notice_reason == (
                                    exit_decision.reason or ""):
                                should_notify = False
                        if should_notify:
                            try:
                                await tg_common.send(
                                    f"[{robot_id}] Выход заблокирован окном тишины.\n"
                                    f"Инструмент: {cfg.name}\n"
                                    f"Позиция: {position_side} {position_qty}\n"
                                    f"Причина: {exit_decision.reason or 'n/a'}"
                                )
                            except Exception:
                                logger.exception("Failed to notify exit blocked.")
                            last_blocked_notice_at = now
                            last_blocked_notice_reason = exit_decision.reason or ""
                        await asyncio.sleep(1.0)
                        continue

                    try:
                        await tg_common.send(
                            f"[{robot_id}] ВНИМАНИЕ: выход был заблокирован, но превышен лимит задержки. Принудительный выход.\n"
                            f"Инструмент: {cfg.name}\n"
                            f"Причина тишины: {exit_decision.reason or 'n/a'}"
                        )
                    except Exception:
                        pass

                side = "SELL" if position_side == "long" else "BUY"
                try:
                    exit_time_str = exit_time.strftime("%Y-%m-%d %H:%M:%S UTC+0")
                    await tg_trading.send(
                        f"[{robot_id}] ТС {instrument_root}\n"
                        f"Выход из позиции по времени.\n"
                        f"Позиция: {position_side} {position_qty} x {cfg.name}\n"
                        f"Плановое время выхода: {exit_time_str}"
                    )

                    _, res = await trade_engine.market_order(
                        contract=cfg.contract,
                        action=side,
                        quantity=position_qty,
                        expected_price=None,
                        timeout=60.0,
                        order_ref=f"{robot_spec.order_ref}_exit",
                    )

                    await tg_trading.send(
                        f"[{robot_id}] ТС {instrument_root}\n"
                        "Позиция закрыта.\n"
                        f"Инструмент: {cfg.name}\n"
                        f"Направление: {side}\n"
                        f"Объём (filled): {res.filled}\n"
                        f"Средняя цена: {res.avg_fill_price:.2f}\n"
                        f"Статус: {res.status}\n"
                        f"Результат (realized PnL): {res.realized_pnl:.2f}\n"
                        f"Комиссия по ордеру: {res.total_commission:.2f}"
                    )

                    position_side = "flat"
                    position_qty = 0
                    entry_hour_start = None
                except Exception as e:
                    logger.exception("Ошибка при попытке выхода из позиции: %s", e)

                await asyncio.sleep(1.0)
                continue

            await asyncio.sleep(1.0)
            continue

        # --- Если робота выключили и позиции нет: до сюда мы не дойдём (return выше) ---
        # --- Ищем сигнал на вход ---
        if not enabled:
            # enabled=False и позиция flat уже обработаны; сюда попадём только если отключили ровно между проверками.
            await asyncio.sleep(1.0)
            continue

        try:
            sig: StrategySignal = strategy.evaluate_for_contract(cfg.name)
        except Exception as e:
            logger.exception("Ошибка в стратегии (%s): %s", robot_id, e)
            await asyncio.sleep(1.0)
            continue

        if sig.action == "flat":
            await asyncio.sleep(1.0)
            continue

        now_dt = sig.now_time_utc or datetime.now(timezone.utc)
        hour_start_dt = now_dt.replace(minute=0, second=0, microsecond=0)

        entry_decision = await quiet_service.evaluate(robot_id=robot_id, now_utc=now_dt, action_type="entry")
        if not entry_decision.allowed:
            should_notify = True
            if last_blocked_notice_at is not None:
                if (now_dt - last_blocked_notice_at).total_seconds() < BLOCKED_NOTICE_COOLDOWN_SECONDS and last_blocked_notice_reason == (
                        entry_decision.reason or ""):
                    should_notify = False

            if should_notify:
                try:
                    await tg_common.send(
                        f"[{robot_id}] Вход заблокирован окном тишины.\n"
                        f"Инструмент: {cfg.name}\n"
                        f"Сигнал стратегии: {sig.action.upper()} (reason={sig.reason})\n"
                        f"Причина тишины: {entry_decision.reason or 'n/a'}"
                    )
                except Exception:
                    logger.exception("Failed to notify entry blocked.")
                last_blocked_notice_at = now_dt
                last_blocked_notice_reason = entry_decision.reason or ""

            await asyncio.sleep(1.0)
            continue

        side = "BUY" if sig.action == "long" else "SELL"

        try:
            stat_str = ""
            if sig.stats:
                n = int(sig.stats.get("n", 0))
                p_up = float(sig.stats.get("p_up", 0.0))
                mean_ret = float(sig.stats.get("mean_ret", 0.0))
                stat_str = f"n={n}, p_up={p_up:.3f}, mean_ret={mean_ret:.5f}"

            sim_str = f"{sig.similarity:.3f}" if sig.similarity is not None else "n/a"

            msg_parts = [
                f"[{robot_id}] ТС {instrument_root}",
                f"Сигнал: {sig.action.upper()} по {cfg.name}",
                f"Причина: {sig.reason}",
                f"Слот внутри часа: {sig.slot_sec} с" if sig.slot_sec is not None else "Слот внутри часа: n/a",
                f"Похожесть (similarity): {sim_str}",
            ]
            if stat_str:
                msg_parts.append(f"Статистика слота: {stat_str}")

            await tg_trading.send("\n".join(msg_parts))

            _, res = await trade_engine.market_order(
                contract=cfg.contract,
                action=side,
                quantity=robot_spec.trade_qty,
                expected_price=None,
                timeout=60.0,
                order_ref=robot_spec.order_ref,
            )

            await tg_trading.send(
                f"[{robot_id}] ТС {instrument_root}\n"
                "Вход в позицию выполнен.\n"
                f"Инструмент: {cfg.name}\n"
                f"Направление: {side}\n"
                f"Объём (filled): {res.filled}\n"
                f"Средняя цена: {res.avg_fill_price:.2f}\n"
                f"Статус: {res.status}"
            )

            if res.filled > 0:
                position_side = "long" if side == "BUY" else "short"
                position_qty = int(res.filled)
                entry_hour_start = hour_start_dt

        except Exception as e:
            logger.exception("Ошибка при попытке входа в позицию (%s): %s", robot_id, e)

        await asyncio.sleep(1.0)
