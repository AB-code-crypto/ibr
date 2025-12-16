import asyncio
import logging
import signal
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from ib_insync import Future

from contracts.bot_spec import RobotSpec, Strategy, StrategySignal
from contracts.robot_registry import (
    RobotSwitchesStore,
    load_all_robot_specs,
    format_robot_specs_for_log,
)

from core.ib_connect import IBConnect
from core.telegram import TelegramChannel
from core.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID_COMMON,
    TELEGRAM_CHAT_ID_TRADING,
    PRICE_DB_PATH,
    ROBOT_OPS_DB_PATH,
    futures_for_history,
)
from core.price_db import PriceDB
from core.ib_monitoring import (
    build_initial_connect_message,
    hourly_status_loop,
)
from core.price_get import PriceCollector, InstrumentConfig, PriceStreamer
from core.portfolio_monitor import portfolio_monitor_loop
from core.trade_engine import TradeEngine
from services.quiet_windows import QuietWindowsService

logger = logging.getLogger(__name__)

# Анти-спам для сообщений "сигнал заблокирован окном тишины"
BLOCKED_NOTICE_COOLDOWN_SECONDS = 300


def _format_dt_utc(dt) -> str:
    if dt.tzinfo is None:
        dt_utc = dt.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC+0")


def _format_dt_local(dt_utc: datetime, tz: ZoneInfo) -> str:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    dt_local = dt_utc.astimezone(tz)
    return dt_local.strftime("%Y-%m-%d %H:%M:%S")


async def price_streamer_loop(
        ib: IBConnect,
        db: PriceDB,
        cfg: InstrumentConfig,
        stop_event: asyncio.Event,
) -> None:
    streamer = PriceStreamer(ib=ib.client, db=db)
    collector = PriceCollector(ib=ib.client, db=db)

    while not stop_event.is_set():
        if not ib.is_connected:
            await ib.wait_connected(timeout=None)
            if stop_event.is_set():
                break
            await asyncio.sleep(1.0)
            continue

        try:
            inserted = await collector.sync_history_for(
                cfg,
                chunk_seconds=3600,
                cancel_event=stop_event,
            )
            if inserted > 0:
                logger.info(
                    "price_streamer_loop[%s]: gap backfilled before streaming, inserted=%d",
                    cfg.name,
                    inserted,
                )
        except Exception as e:
            logger.error(
                "price_streamer_loop[%s]: error while backfilling history before streaming: %s",
                cfg.name,
                e,
            )
            await asyncio.sleep(2.0)
            continue

        try:
            await streamer.stream_bars(cfg, cancel_event=stop_event)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(
                "price_streamer_loop[%s]: error in stream_bars: %s",
                cfg.name,
                e,
            )
            await asyncio.sleep(2.0)


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
        enabled = switches.is_enabled(robot_id)
        if not enabled and position_side == "flat":
            try:
                await tg_common.send(f"[{robot_id}] Робот отключён выключателем (robot_switches). Торговый цикл остановлен.")
            except Exception:
                logger.exception("Failed to notify robot disabled: %s", robot_id)
            return

        now = datetime.now(timezone.utc)

        # 1) Сообщения о тишине (entry)
        entry_decision = quiet_service.evaluate(robot_id=robot_id, now_utc=now, action_type="entry")
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
                exit_decision = quiet_service.evaluate(robot_id=robot_id, now_utc=now, action_type="exit")
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

        entry_decision = quiet_service.evaluate(robot_id=robot_id, now_utc=now_dt, action_type="entry")
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


def _startup_registry_message(all_specs: list[RobotSpec], enabled_specs: list[RobotSpec], ops_db_path: str) -> str:
    enabled_ids = ", ".join([s.robot_id for s in enabled_specs]) if enabled_specs else "none"
    lines = [
        "IB-робот: запуск.",
        f"ops_db: {ops_db_path}",
        f"robots_registered: {len(all_specs)}",
        f"robots_enabled: {len(enabled_specs)} ({enabled_ids})",
        "",
        "Реестр (все):",
        format_robot_specs_for_log(all_specs),
    ]
    if enabled_specs and len(enabled_specs) != len(all_specs):
        lines.append("")
        lines.append("Активные (enabled):")
        lines.append(format_robot_specs_for_log(enabled_specs))
    return "\n".join(lines)


async def main() -> None:
    # 1) БД цен
    db = PriceDB(PRICE_DB_PATH)
    await db.connect()

    # 2) robot_ops.db (операционная БД; также используем для robot_switches/quiet_windows)
    ops_db_path = str(ROBOT_OPS_DB_PATH)

    # 3) Реестр роботов
    all_specs = load_all_robot_specs()
    known_robot_ids = [s.robot_id for s in all_specs]

    # 4) Выключатели
    switches = RobotSwitchesStore(db_path=ops_db_path, cache_ttl_seconds=10)
    switches.ensure_schema()
    switches.seed_defaults(known_robot_ids, default_enabled=True)

    enabled_ids = switches.enabled_robot_ids(known_robot_ids)
    enabled_specs = [s for s in all_specs if s.robot_id in enabled_ids]

    # 5) Quiet windows сервис (правила по robot_id)
    quiet_service = QuietWindowsService(db_path=ops_db_path, cache_ttl_seconds=30.0)
    quiet_service.ensure_schema()
    for spec in all_specs:
        quiet_service.seed_default_rth_open(robot_id=spec.robot_id)

    # 6) Коннектор к IB
    ib = IBConnect(
        host="127.0.0.1",
        port=7496,
        client_id=101,
    )

    # 7) Telegram
    tg_common = TelegramChannel(
        bot_token=TELEGRAM_BOT_TOKEN,
        chat_id=TELEGRAM_CHAT_ID_COMMON,
    )
    tg_trading = TelegramChannel(
        bot_token=TELEGRAM_BOT_TOKEN,
        chat_id=TELEGRAM_CHAT_ID_TRADING,
    )

    try:
        await tg_common.send(_startup_registry_message(all_specs, enabled_specs, ops_db_path))
    except Exception:
        logger.exception("Failed to send startup registry message.")

    # 8) stop_event + signals
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    tasks: list[asyncio.Task] = []

    connector_task = asyncio.create_task(ib.run_forever(), name="ib_connector")
    tasks.append(connector_task)

    connected = await ib.wait_connected(timeout=15)

    if connected:
        await asyncio.sleep(1.0)
        await tg_common.send(build_initial_connect_message(ib))

        if not enabled_specs:
            await tg_common.send("IB-робот: нет активных роботов (all disabled). Торговля не запущена.")
        else:
            # 9) Собираем уникальные активные контракты, нужные enabled-роботам
            active_symbols = sorted({s.active_future_symbol for s in enabled_specs})

            # 9.1) Проверяем, что для одного active_future_symbol нет конфликтующих instrument_root
            symbol_to_root: dict[str, str] = {}
            for spec in enabled_specs:
                existing = symbol_to_root.get(spec.active_future_symbol)
                if existing is None:
                    symbol_to_root[spec.active_future_symbol] = spec.instrument_root
                elif existing != spec.instrument_root:
                    raise ValueError(
                        "Conflict: same active_future_symbol used with different instrument_root. "
                        f"symbol={spec.active_future_symbol!r}, roots={existing!r} vs {spec.instrument_root!r}"
                    )

            collector = PriceCollector(ib=ib.client, db=db)
            instrument_configs: list[InstrumentConfig] = []

            for symbol_code in active_symbols:
                meta = futures_for_history[symbol_code]  # fail-fast если символа нет в конфиге

                contract_month = meta["contract_month"]
                if not isinstance(contract_month, str) or len(contract_month) != 6 or not contract_month.isdigit():
                    raise ValueError(
                        f"Invalid contract_month for {symbol_code!r}: {contract_month!r} (expected 'YYYYMM')."
                    )

                contract = Future(
                    symbol=symbol_to_root[symbol_code],
                    lastTradeDateOrContractMonth=contract_month,
                    exchange="CME",
                    currency="USD",
                )

                cfg = InstrumentConfig(
                    name=symbol_code,
                    contract=contract,
                    history_lookback=timedelta(days=1),
                    history_start=meta.get("history_start"),
                    expiry=meta.get("expiry"),
                )
                instrument_configs.append(cfg)

            # 10) Backfill истории только по нужным активным символам
            pre_lines: list[str] = []
            for cfg_i in instrument_configs:
                await db.ensure_table(cfg_i.name)
                last_dt = await db.get_last_bar_datetime(cfg_i.name)
                if last_dt is not None:
                    pre_lines.append(f"{cfg_i.name}: данные в БД до {_format_dt_utc(last_dt)}.")
            if pre_lines:
                await tg_common.send("IB-робот: состояние истории перед загрузкой:\n" + "\n".join(pre_lines))

            results = await collector.sync_many(
                instrument_configs,
                chunk_seconds=3600,
                cancel_event=stop_event,
            )

            post_lines: list[str] = []
            for cfg_i in instrument_configs:
                inserted = results.get(cfg_i.name, 0)
                if inserted <= 0:
                    continue
                last_dt_after = await db.get_last_bar_datetime(cfg_i.name)
                last_dt_str = _format_dt_utc(last_dt_after) if last_dt_after is not None else "нет данных"
                post_lines.append(f"{cfg_i.name}: добавлено баров: {inserted} , последний: {last_dt_str}.")
            if post_lines:
                await tg_common.send("IB-робот: загрузка истории завершена:\n" + "\n".join(post_lines))

            # 11) Реалтайм-стриминг по каждому активному символу
            cfg_by_symbol = {cfg_i.name: cfg_i for cfg_i in instrument_configs}
            for cfg_i in instrument_configs:
                tasks.append(
                    asyncio.create_task(
                        price_streamer_loop(ib, db, cfg_i, stop_event),
                        name=f"price_stream_loop_{cfg_i.name}",
                    )
                )

            # 12) Торговые циклы по каждому роботу
            trade_engine = TradeEngine(ib.client, logger=logging.getLogger("TradeEngine"))

            for spec in enabled_specs:
                active_cfg = cfg_by_symbol[spec.active_future_symbol]
                strategy = spec.strategy_factory()

                tasks.append(
                    asyncio.create_task(
                        trading_loop(
                            ib=ib,
                            trade_engine=trade_engine,
                            strategy=strategy,
                            cfg=active_cfg,
                            tg_trading=tg_trading,
                            tg_common=tg_common,
                            quiet_service=quiet_service,
                            switches=switches,
                            robot_spec=spec,
                            stop_event=stop_event,
                        ),
                        name=f"trading_loop_{spec.robot_id}_{active_cfg.name}",
                    )
                )

                await tg_common.send(
                    f"[{spec.robot_id}] Запущен.\n"
                    f"active_future: {spec.active_future_symbol}\n"
                    f"instrument_root: {spec.instrument_root}\n"
                    f"trade_qty: {spec.trade_qty}\n"
                    f"order_ref: {spec.order_ref}"
                )

    else:
        await tg_common.send("IB-робот: не удалось подключиться к TWS в отведённое время ⚠️")

    tasks.append(asyncio.create_task(hourly_status_loop(ib, tg_common), name="hourly_status"))
    tasks.append(asyncio.create_task(portfolio_monitor_loop(ib, tg_common, stop_event), name="portfolio_monitor"))

    try:
        await stop_event.wait()
    finally:
        try:
            await tg_common.send("IB-робот: остановка работы (shutdown) ⏹")
        except Exception:
            pass

        try:
            await ib.shutdown()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        await tg_common.close()
        await tg_trading.close()
        await db.close()


if __name__ == "__main__":
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )

    asyncio.run(main())
