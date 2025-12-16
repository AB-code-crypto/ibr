import asyncio
import logging
import signal
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from ib_insync import Future

from core.ib_connect import IBConnect
from core.telegram import TelegramChannel
from core.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID_COMMON,
    TELEGRAM_CHAT_ID_TRADING,
    PRICE_DB_PATH,
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

from strategies.pattern_pirson import PatternStrategy, PatternSignal

logger = logging.getLogger(__name__)

# Уникальный идентификатор робота внутри системы (для orderRef, логов, quiet windows)
ROBOT_ID = "pattern_pirson"

# Текущий рабочий фьючерс, для которого запускаем real-time стриминг И торговлю
ACTIVE_FUTURE_SYMBOL = "MNQZ5"

# Параметры торговой ТС (пока в robot.py; позже вынесем в spec.py робота)
PATTERN_INSTRUMENT_ROOT = "MNQ"  # root инструмента в patterns.db
PATTERN_ORDER_REF = ROBOT_ID  # метка ордеров в IB (orderRef)
PATTERN_TRADE_QTY = 1  # размер позиции (контрактов)

# Анти-спам для сообщений "сигнал заблокирован окном тишины"
BLOCKED_NOTICE_COOLDOWN_SECONDS = 300


def _format_dt_utc(dt) -> str:
    """
    Красивое форматирование времени бара для сообщений в Telegram:
    YYYY-MM-DD HH:MM:SS UTC+0
    """
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
    """
    Фоновый цикл, который:
      - ждёт активного соединения с IB;
      - перед стримингом догружает пропущенную историю 5-секундных баров;
      - запускает PriceStreamer.stream_bars для инструмента cfg;
      - при разрыве соединения или ошибке в стримере
        ждёт переподключения и перезапускает backfill+стриминг.

    Останавливается, когда выставлен stop_event.
    """
    streamer = PriceStreamer(ib=ib.client, db=db)
    collector = PriceCollector(ib=ib.client, db=db)

    while not stop_event.is_set():
        # 1. Ждём, пока IB будет подключен
        if not ib.is_connected:
            await ib.wait_connected(timeout=None)
            if stop_event.is_set():
                break
            # даём IB секунду на синхронизацию портфеля/аккаунта
            await asyncio.sleep(1.0)
            continue

        # 2. Перед запуском стриминга — догружаем недостающую историю
        try:
            inserted = await collector.sync_history_for(
                cfg,
                chunk_seconds=3600,  # по часу 5-секундных баров за запрос
                cancel_event=stop_event,  # уважать запрос на остановку
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

        # 3. Запускаем стриминг
        try:
            await streamer.stream_bars(cfg, cancel_event=stop_event)
        except asyncio.CancelledError:
            # Нормальный shutdown
            raise
        except Exception as e:
            logger.error(
                "price_streamer_loop[%s]: error in stream_bars: %s",
                cfg.name,
                e,
            )
            await asyncio.sleep(2.0)

        # Цикл пойдёт с начала


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
        strategy: PatternStrategy,
        cfg: InstrumentConfig,
        tg_trading: TelegramChannel,
        tg_common: TelegramChannel,
        quiet_service: QuietWindowsService,
        robot_id: str,
        stop_event: asyncio.Event,
) -> None:
    """
    Торговый цикл по паттерн-стратегии.

    Важно:
      - стратегии НЕ ходят в quiet windows DB;
      - фильтрацию "можно ли сейчас отправить приказ" делает робот (оркестратор),
        через QuietWindowsService;
      - стратегия возвращает сигнал/интент, робот решает, разрешено ли исполнение.

    Логика:
      - Пока нет позиции: раз в ~5 секунд спрашиваем стратегию.
        Если она даёт сигнал long/short, проверяем quiet windows (entry).
        Если entry разрешён — открываем позицию PATTERN_TRADE_QTY маркетом.
      - Пока позиция открыта: ждём времени выхода (59:50 от часа входа) и закрываем позицию.
        Quiet windows для выхода по умолчанию НЕ должны мешать, но опция block_exits поддержана.
    """
    position_side: str = "flat"  # "flat" | "long" | "short"
    position_qty: int = 0
    entry_hour_start: datetime | None = None

    quiet_state = _QuietState()
    quiet_tz = ZoneInfo("America/New_York")

    last_blocked_notice_at: Optional[datetime] = None
    last_blocked_notice_reason: Optional[str] = None

    logger.info(
        "Trading loop started for %s (instrument_root=%s, qty=%s, robot_id=%s)",
        cfg.name,
        strategy.instrument,
        PATTERN_TRADE_QTY,
        robot_id,
    )

    # Небольшое выравнивание по 5-секундной сетке
    while not stop_event.is_set():
        now = datetime.now(timezone.utc)
        sleep_rem = 5 - (now.second % 5)
        if sleep_rem <= 0 or sleep_rem > 5:
            sleep_rem = 5
        await asyncio.sleep(sleep_rem)
        break

    while not stop_event.is_set():
        # 0. Ждём соединение с IB
        if not ib.is_connected:
            await ib.wait_connected(timeout=None)
            await asyncio.sleep(1.0)
            continue

        now = datetime.now(timezone.utc)

        # 1) Пишем в tg_common при входе/выходе из "тишины" (entry-blocked)
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

        # --- Если есть открытая позиция: проверяем время выхода ---
        if position_side in ("long", "short") and entry_hour_start is not None:
            exit_time = entry_hour_start.replace(minute=59, second=50, microsecond=0)

            if now >= exit_time:
                # Проверка quiet windows для выхода (поддерживаем block_exits, но по умолчанию там 0)
                exit_decision = quiet_service.evaluate(robot_id=robot_id, now_utc=now, action_type="exit")
                if not exit_decision.allowed:
                    # Для безопасности не зависаем в позиции бесконечно: если просрочили выход сильно — выходим принудительно
                    hard_grace = timedelta(minutes=2)
                    if now < (exit_time + hard_grace):
                        # Логируем в общий канал (не чаще cooldown)
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

                    # Просрочили выход > 2 минут: принудительно выходим (risk control)
                    try:
                        await tg_common.send(
                            f"[{robot_id}] ВНИМАНИЕ: выход был заблокирован, но превышен лимит задержки.\n"
                            f"Выполняю принудительный выход.\n"
                            f"Инструмент: {cfg.name}\n"
                            f"Причина тишины: {exit_decision.reason or 'n/a'}"
                        )
                    except Exception:
                        pass

                side = "SELL" if position_side == "long" else "BUY"
                try:
                    exit_time_str = exit_time.strftime("%Y-%m-%d %H:%M:%S UTC+0")
                    msg = (
                        f"[{robot_id}] ТС MNQ\n"
                        f"Выход из позиции по времени.\n"
                        f"Позиция: {position_side} {position_qty} x {cfg.name}\n"
                        f"Плановое время выхода: {exit_time_str}"
                    )
                    await tg_trading.send(msg)

                    _, res = await trade_engine.market_order(
                        contract=cfg.contract,
                        action=side,
                        quantity=position_qty,
                        expected_price=None,
                        timeout=60.0,
                        order_ref=f"{robot_id}_exit",
                    )

                    logger.info(
                        "Exit order done for %s: side=%s, filled=%.0f, avg_price=%.2f, "
                        "status=%s, commission=%.2f, realized_pnl=%.2f",
                        cfg.name,
                        side,
                        res.filled,
                        res.avg_fill_price,
                        res.status,
                        res.total_commission,
                        res.realized_pnl,
                    )

                    await tg_trading.send(
                        f"[{robot_id}] ТС MNQ\n"
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

        # --- Позиции нет: ищем сигнал на вход ---

        try:
            signal: PatternSignal = strategy.evaluate_for_contract(cfg.name)
        except Exception as e:
            logger.exception("Ошибка в pattern-стратегии: %s", e)
            await asyncio.sleep(1.0)
            continue

        if signal.action == "flat":
            await asyncio.sleep(1.0)
            continue

        # Сейчас мы в flat: рассматриваем вход
        now_dt = signal.now_time_utc or datetime.now(timezone.utc)

        # Для safety: проверим, что часовая логика ещё валидна
        hour_start_dt = now_dt.replace(minute=0, second=0, microsecond=0)
        offset_sec = int((now_dt - hour_start_dt).total_seconds())
        if offset_sec < strategy.min_entry_minutes_after_hour_start * 60:
            await asyncio.sleep(1.0)
            continue

        last_entry_sec = (60 - strategy.min_entry_minutes_before_hour_end) * 60
        if offset_sec > last_entry_sec:
            await asyncio.sleep(1.0)
            continue

        # Фильтр quiet windows (entry)
        entry_decision = quiet_service.evaluate(robot_id=robot_id, now_utc=now_dt, action_type="entry")
        if not entry_decision.allowed:
            # Уведомление в tg_common (не чаще cooldown)
            should_notify = True
            if last_blocked_notice_at is not None:
                if (now_dt - last_blocked_notice_at).total_seconds() < BLOCKED_NOTICE_COOLDOWN_SECONDS and last_blocked_notice_reason == (
                        entry_decision.reason or ""):
                    should_notify = False

            if should_notify:
                msg = (
                    f"[{robot_id}] Вход заблокирован окном тишины.\n"
                    f"Инструмент: {cfg.name}\n"
                    f"Сигнал стратегии: {signal.action.upper()} (reason={signal.reason})\n"
                    f"Причина тишины: {entry_decision.reason or 'n/a'}"
                )
                try:
                    await tg_common.send(msg)
                except Exception:
                    logger.exception("Failed to notify entry blocked.")
                last_blocked_notice_at = now_dt
                last_blocked_notice_reason = entry_decision.reason or ""

            await asyncio.sleep(1.0)
            continue

        # На этом этапе стратегия хочет входить и quiet window разрешает
        side = "BUY" if signal.action == "long" else "SELL"

        try:
            # Уведомление в торговый канал (сигнал/вход)
            stat_str = ""
            if signal.stats:
                n = int(signal.stats.get("n", 0))
                p_up = float(signal.stats.get("p_up", 0.0))
                mean_ret = float(signal.stats.get("mean_ret", 0.0))
                stat_str = f"n={n}, p_up={p_up:.3f}, mean_ret={mean_ret:.5f}"

            sim_str = f"{signal.similarity:.3f}" if signal.similarity is not None else "n/a"

            msg_parts = [
                f"[{robot_id}] ТС MNQ",
                f"Сигнал: {signal.action.upper()} по {cfg.name}",
                f"Причина: {signal.reason}",
                f"Слот внутри часа: {signal.slot_sec} с" if signal.slot_sec is not None else "Слот внутри часа: n/a",
                f"Похожесть (similarity): {sim_str}",
            ]
            if stat_str:
                msg_parts.append(f"Статистика слота: {stat_str}")

            await tg_trading.send("\n".join(msg_parts))

            # Отправляем маркет-ордер (фактический вход)
            _, res = await trade_engine.market_order(
                contract=cfg.contract,
                action=side,
                quantity=PATTERN_TRADE_QTY,
                expected_price=None,
                timeout=60.0,
                order_ref=robot_id,
            )

            logger.info(
                "Entry order done for %s: side=%s, filled=%.0f, avg_price=%.2f, status=%s",
                cfg.name,
                side,
                res.filled,
                res.avg_fill_price,
                res.status,
            )

            await tg_trading.send(
                f"[{robot_id}] ТС MNQ\n"
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
            logger.exception("Ошибка при попытке входа в позицию: %s", e)

        await asyncio.sleep(1.0)


async def main() -> None:
    """
    Главный вход в робота.
    """
    # 0. БД цен (SQLite)
    db = PriceDB(PRICE_DB_PATH)
    await db.connect()

    # 0.1. Quiet windows DB (отдельный SQLite файл рядом с PRICE_DB_PATH)
    quiet_db_path = str(Path(PRICE_DB_PATH).with_name("quiet_windows.db"))
    quiet_service = QuietWindowsService(db_path=quiet_db_path, cache_ttl_seconds=30.0)
    quiet_service.ensure_schema()
    # Сидируем базовое окно тишины, если пользователь ещё не добавил правил вручную
    quiet_service.seed_default_rth_open(robot_id=ROBOT_ID)

    # 1. Коннектор к IB
    ib = IBConnect(
        host="127.0.0.1",
        port=7496,
        client_id=101,
    )

    # 2. Telegram-каналы
    tg_common = TelegramChannel(
        bot_token=TELEGRAM_BOT_TOKEN,
        chat_id=TELEGRAM_CHAT_ID_COMMON,
    )
    tg_trading = TelegramChannel(
        bot_token=TELEGRAM_BOT_TOKEN,
        chat_id=TELEGRAM_CHAT_ID_TRADING,
    )

    # 3. Событие остановки и обработчики сигналов
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    tasks: list[asyncio.Task] = []

    # 4. Коннектор IB
    connector_task = asyncio.create_task(ib.run_forever(), name="ib_connector")
    tasks.append(connector_task)

    # 5. Ждём первичного подключения
    connected = await ib.wait_connected(timeout=15)
    active_cfg: InstrumentConfig | None = None

    if connected:
        logger.info("IB initial connection established, server_time=%s", ib.server_time)
        await asyncio.sleep(1.0)

        # 5.1. Первое сообщение с портфелем
        text = build_initial_connect_message(ib)
        await tg_common.send(text)

        # 5.2. Синхронизация истории
        collector = PriceCollector(ib=ib.client, db=db)

        instrument_configs: list[InstrumentConfig] = []

        for code, meta in futures_for_history.items():
            contract_month = meta.get("contract_month")
            if not contract_month:
                logger.error("Skip symbol %s: missing contract_month in futures_for_history", code)
                continue

            if not isinstance(contract_month, str) or len(contract_month) != 6 or not contract_month.isdigit():
                logger.error("Skip symbol %s: invalid contract_month=%r (expected 'YYYYMM')", code, contract_month)
                continue

            contract = Future(
                symbol=PATTERN_INSTRUMENT_ROOT,
                lastTradeDateOrContractMonth=contract_month,
                exchange="CME",
                currency="USD",
            )

            cfg = InstrumentConfig(
                name=code,
                contract=contract,
                history_lookback=timedelta(days=1),
                history_start=meta.get("history_start"),
                expiry=meta.get("expiry"),
            )
            instrument_configs.append(cfg)

        if instrument_configs:
            pre_lines: list[str] = []
            for cfg_i in instrument_configs:
                await db.ensure_table(cfg_i.name)
                last_dt = await db.get_last_bar_datetime(cfg_i.name)
                if last_dt is not None:
                    pre_lines.append(f"{cfg_i.name}: данные в БД до {_format_dt_utc(last_dt)}.")

            if pre_lines:
                await tg_common.send(
                    "IB-робот: состояние исторических данных перед загрузкой:\n" + "\n".join(pre_lines)
                )

            logger.info(
                "Starting historical backfill for instruments: %s",
                ", ".join(cfg_i.name for cfg_i in instrument_configs),
            )
            results = await collector.sync_many(
                instrument_configs,
                chunk_seconds=3600,
                cancel_event=stop_event,
            )
            logger.info("Historical backfill results: %r", results)

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

            # 5.3. Реалтайм-стриминг
            active_cfg = next((cfg_i for cfg_i in instrument_configs if cfg_i.name == ACTIVE_FUTURE_SYMBOL), None)
            if active_cfg is not None:
                price_stream_task = asyncio.create_task(
                    price_streamer_loop(ib, db, active_cfg, stop_event),
                    name=f"price_stream_loop_{active_cfg.name}",
                )
                tasks.append(price_stream_task)
                logger.info("Started real-time streaming supervisor for active future %s", ACTIVE_FUTURE_SYMBOL)
            else:
                logger.warning("Active future %s not found; real-time streaming not started", ACTIVE_FUTURE_SYMBOL)
        else:
            logger.warning("No valid instrument configs built for history backfill; futures_for_history=%r", futures_for_history)

        # 5.4. Торговый цикл
        if active_cfg is not None:
            pattern_strategy = PatternStrategy(PATTERN_INSTRUMENT_ROOT)
            trade_engine = TradeEngine(ib.client, logger=logging.getLogger("TradeEngine"))

            trading_task = asyncio.create_task(
                trading_loop(
                    ib=ib,
                    trade_engine=trade_engine,
                    strategy=pattern_strategy,
                    cfg=active_cfg,
                    tg_trading=tg_trading,
                    tg_common=tg_common,
                    quiet_service=quiet_service,
                    robot_id=ROBOT_ID,
                    stop_event=stop_event,
                ),
                name=f"trading_loop_{active_cfg.name}",
            )
            tasks.append(trading_task)
            logger.info("Started trading loop for %s with pattern strategy on %s", active_cfg.name, PATTERN_INSTRUMENT_ROOT)
        else:
            logger.warning("Trading loop not started: active_cfg is None.")
    else:
        logger.error("IB initial connection NOT established within timeout; run_forever() will keep retrying.")
        await tg_common.send("IB-робот: не удалось подключиться к TWS в отведённое время ⚠️")

    # 6. Статус соединения раз в час
    hourly_task = asyncio.create_task(hourly_status_loop(ib, tg_common), name="hourly_status")
    tasks.append(hourly_task)

    # 6.1. Мониторинг портфеля
    portfolio_task = asyncio.create_task(
        portfolio_monitor_loop(ib, tg_common, stop_event),
        name="portfolio_monitor",
    )
    tasks.append(portfolio_task)

    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("Main task cancelled, initiating shutdown.")
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

        logger.info("Robot shutdown completed.")


if __name__ == "__main__":
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )

    asyncio.run(main())
