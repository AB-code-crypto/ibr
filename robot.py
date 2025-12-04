import asyncio
import logging
import signal
from datetime import datetime, timedelta, timezone

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

from strategy.pattern_strategy import PatternStrategy, PatternSignal

logger = logging.getLogger(__name__)

# Текущий рабочий фьючерс, для которого запускаем real-time стриминг И торговлю
ACTIVE_FUTURE_SYMBOL = "MNQZ5"

# Параметры торговой ТС
PATTERN_INSTRUMENT_ROOT = "MNQ"          # root инструмента в patterns.db
PATTERN_ORDER_REF = "MNQ_pattern_v1"     # метка ордеров в IB
PATTERN_TRADE_QTY = 1                    # размер позиции (контрактов)


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
                chunk_seconds=3600,        # по часу 5-секундных баров за запрос
                cancel_event=stop_event,   # уважать запрос на остановку
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


async def trading_loop(
        ib: IBConnect,
        trade_engine: TradeEngine,
        strategy: PatternStrategy,
        cfg: InstrumentConfig,
        tg_trading: TelegramChannel,
        stop_event: asyncio.Event,
) -> None:
    """
    Торговый цикл по паттерн-стратегии.

    Логика:
      - Пока нет позиции: раз в ~5 секунд спрашиваем стратегию.
        Если она даёт сигнал long/short, открываем позицию PATTERN_TRADE_QTY
        маркетом и запоминаем час начала.
      - Пока позиция открыта: не даём новых входов, просто ждём времени
        выхода (59:50 от часа входа) и закрываем позицию маркетом.
    """
    position_side: str = "flat"        # "flat" | "long" | "short"
    position_qty: int = 0
    entry_hour_start: datetime | None = None

    logger.info(
        "Trading loop started for %s (root=%s, qty=%s)",
        cfg.name,
        strategy.instrument,
        PATTERN_TRADE_QTY,
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

        # --- Если есть открытая позиция: проверяем время выхода ---
        if position_side in ("long", "short") and entry_hour_start is not None:
            exit_time = entry_hour_start.replace(minute=59, second=50, microsecond=0)

            # Если по каким-то причинам час сменился, а мы всё ещё в позиции –
            # закрываем как можно быстрее.
            if now >= exit_time:
                side = "SELL" if position_side == "long" else "BUY"
                try:
                    msg = (
                        f"ТС MNQ: выходим из позиции {position_side} {position_qty} x "
                        f"{cfg.name} по времени (target exit {exit_time.strftime('%Y-%m-%d %H:%M:%S UTC+0')})."
                    )
                    await tg_trading.send(msg)

                    _, res = await trade_engine.market_order(
                        contract=cfg.contract,
                        action=side,
                        quantity=position_qty,
                        expected_price=None,
                        timeout=60.0,
                        order_ref=PATTERN_ORDER_REF + "_exit",
                    )

                    logger.info(
                        "Exit order done for %s: side=%s, filled=%.0f, avg_price=%.2f, status=%s",
                        cfg.name,
                        side,
                        res.filled,
                        res.avg_fill_price,
                        res.status,
                    )

                    await tg_trading.send(
                        f"ТС MNQ: позиция закрыта (side={side}, filled={res.filled}, "
                        f"avg={res.avg_fill_price:.2f}, status={res.status})."
                    )

                    position_side = "flat"
                    position_qty = 0
                    entry_hour_start = None
                except Exception as e:
                    logger.exception("Ошибка при попытке выхода из позиции: %s", e)
                # После выхода (или попытки) ждём до следующего цикла
                await asyncio.sleep(1.0)
                continue

            # Пока не время выхода – просто ждём следующего тика
            await asyncio.sleep(1.0)
            continue

        # --- Позиции нет: ищем сигнал на вход ---

        try:
            signal: PatternSignal = strategy.evaluate_for_contract(cfg.name)
        except Exception as e:
            logger.exception("Ошибка в pattern-стратегии: %s", e)
            await asyncio.sleep(1.0)
            continue

        # Если стратегия явно не даёт вход – ничего не делаем
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

        # На этом этапе стратегия хочет входить и по времени всё ок
        side = "BUY" if signal.action == "long" else "SELL"

        try:
            # Уведомление в торговый канал (сигнал)
            stat_str = ""
            if signal.stats:
                n = int(signal.stats.get("n", 0))
                p_up = float(signal.stats.get("p_up", 0.0))
                mean_ret = float(signal.stats.get("mean_ret", 0.0))
                stat_str = f" n={n}, p_up={p_up:.3f}, mean_ret={mean_ret:.5f}"

            if signal.similarity is not None:
                sim_str = f"{signal.similarity:.3f}"
            else:
                sim_str = "n/a"

            msg = (
                f"ТС MNQ: сигнал {signal.action.upper()} по {cfg.name} "
                f"(reason={signal.reason}, block_slot={signal.slot_sec}, "
                f"sim={sim_str}{stat_str})."
            )
            await tg_trading.send(msg)

            # Отправляем маркет-ордер (фактический вход)
            _, res = await trade_engine.market_order(
                contract=cfg.contract,
                action=side,
                quantity=PATTERN_TRADE_QTY,
                expected_price=None,
                timeout=60.0,
                order_ref=PATTERN_ORDER_REF,
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
                f"ТС MNQ: вход выполнен (side={side}, filled={res.filled}, "
                f"avg={res.avg_fill_price:.2f}, status={res.status})."
            )

            # Фиксируем позицию во внутреннем состоянии
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

    Сейчас:
      - поднимаем коннектор к IB;
      - поднимаем два Telegram-канала (общий и торговый);
      - поднимаем SQLite-БД для цен;
      - регистрируем обработчики сигналов (SIGINT/SIGTERM) -> stop_event;
      - при первом успешном подключении:
          * шлём сообщение с портфелем;
          * шлём в Telegram состояние БД по инструментам (последние даты баров);
          * загружаем/дозагружаем историю 5-секундных баров по списку фьючерсов;
          * шлём в Telegram результат загрузки (сколько баров добавлено и до какой даты);
          * запускаем real-time стриминг 5-секундных баров по текущему рабочему фьючерсу;
          * запускаем торговый цикл по паттерн-стратегии для ACTIVE_FUTURE_SYMBOL;
      - раз в час шлём статус соединения в общий канал (на начале часа);
      - мониторим изменения портфеля и шлём их в общий канал;
      - ждём сигнал остановки и корректно всё закрываем.
    """
    # 0. БД цен (SQLite)
    db = PriceDB(PRICE_DB_PATH)
    await db.connect()

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
            # На платформах без нормальных сигналов (Windows и т.п.) просто пропускаем
            pass

    # Список фоновых задач для аккуратного shutdown
    tasks: list[asyncio.Task] = []

    # 4. Запускаем фоновую задачу коннектора
    connector_task = asyncio.create_task(ib.run_forever(), name="ib_connector")
    tasks.append(connector_task)

    # 5. Ждём первичного подключения к IB
    connected = await ib.wait_connected(timeout=15)
    active_cfg: InstrumentConfig | None = None

    if connected:
        logger.info("IB initial connection established, server_time=%s", ib.server_time)

        # даём IB чуть времени получить портфель/аккаунт (без доп. запросов)
        await asyncio.sleep(1.0)

        # 5.1. Первое сообщение с портфелем
        text = build_initial_connect_message(ib)
        await tg_common.send(text)

        # 5.2. Синхронизация истории 5-секундных баров по списку фьючерсов
        collector = PriceCollector(ib=ib.client, db=db)

        instrument_configs: list[InstrumentConfig] = []

        for code, meta in futures_for_history.items():
            contract_month = meta.get("contract_month")
            if not contract_month:
                logger.error(
                    "Skip symbol %s: missing contract_month in futures_for_history",
                    code,
                )
                continue

            # Простейшая валидация формата 'YYYYMM'
            if not isinstance(contract_month, str) or len(contract_month) != 6 or not contract_month.isdigit():
                logger.error(
                    "Skip symbol %s: invalid contract_month=%r (expected 'YYYYMM')",
                    code,
                    contract_month,
                )
                continue

            # Единый способ построения IB-контракта:
            # symbol='MNQ', lastTradeDateOrContractMonth='YYYYMM'
            contract = Future(
                symbol=PATTERN_INSTRUMENT_ROOT,
                lastTradeDateOrContractMonth=contract_month,
                exchange="CME",
                currency="USD",
            )

            cfg = InstrumentConfig(
                name=code,  # имя таблицы в БД = код фьючерса
                contract=contract,
                history_lookback=timedelta(days=1),  # fallback, если history_start не задан
                history_start=meta.get("history_start"),  # явный старт истории
                expiry=meta.get("expiry"),  # пока не используем, но поле есть
            )
            instrument_configs.append(cfg)

        if instrument_configs:
            # 5.2.a. Статус БД перед загрузкой
            pre_lines: list[str] = []
            for cfg in instrument_configs:
                await db.ensure_table(cfg.name)
                last_dt = await db.get_last_bar_datetime(cfg.name)
                if last_dt is not None:
                    pre_lines.append(
                        f"{cfg.name}: данные в БД до {_format_dt_utc(last_dt)}."
                    )

            if pre_lines:
                pre_msg = "IB-робот: состояние исторических данных перед загрузкой:\n" + "\n".join(pre_lines)
                await tg_common.send(pre_msg)

            # 5.2.b. Запускаем загрузку истории
            logger.info(
                "Starting historical backfill for instruments: %s",
                ", ".join(cfg.name for cfg in instrument_configs),
            )
            results = await collector.sync_many(
                instrument_configs,
                chunk_seconds=3600,  # по часу 5-секундных баров за запрос
                cancel_event=stop_event,  # уважать запрос на остановку
            )
            logger.info("Historical backfill results: %r", results)

            # 5.2.c. Статус БД после загрузки (только там, где что-то добавили)
            post_lines: list[str] = []
            for cfg in instrument_configs:
                inserted = results.get(cfg.name, 0)
                if inserted <= 0:
                    continue

                last_dt_after = await db.get_last_bar_datetime(cfg.name)
                if last_dt_after is not None:
                    last_dt_str = _format_dt_utc(last_dt_after)
                else:
                    last_dt_str = "нет данных"

                post_lines.append(
                    f"{cfg.name}: добавлено баров: {inserted} , последний: {last_dt_str}."
                )

            if post_lines:
                post_msg = "IB-робот: загрузка истории завершена:\n" + "\n".join(post_lines)
                await tg_common.send(post_msg)

            # 5.3. Запускаем real-time стриминг 5-секундных баров
            active_cfg = next(
                (cfg for cfg in instrument_configs if cfg.name == ACTIVE_FUTURE_SYMBOL),
                None,
            )
            if active_cfg is not None:
                price_stream_task = asyncio.create_task(
                    price_streamer_loop(ib, db, active_cfg, stop_event),
                    name=f"price_stream_loop_{active_cfg.name}",
                )
                tasks.append(price_stream_task)
                logger.info(
                    "Started real-time streaming supervisor for active future %s",
                    ACTIVE_FUTURE_SYMBOL,
                )
            else:
                logger.warning(
                    "Active future %s not found in instrument_configs; real-time streaming not started",
                    ACTIVE_FUTURE_SYMBOL,
                )
        else:
            logger.warning(
                "No valid instrument configs built for history backfill; "
                "futures_for_history=%r",
                futures_for_history,
            )

        # 5.4. Если есть активный конфиг и кластера по MNQ – запускаем торговый цикл
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
                    stop_event=stop_event,
                ),
                name=f"trading_loop_{active_cfg.name}",
            )
            tasks.append(trading_task)
            logger.info(
                "Started trading loop for %s with pattern strategy on %s",
                active_cfg.name,
                PATTERN_INSTRUMENT_ROOT,
            )
        else:
            logger.warning("Trading loop not started: active_cfg is None.")

    else:
        logger.error(
            "IB initial connection NOT established within timeout; "
            "run_forever() продолжит попытки переподключения."
        )
        await tg_common.send(
            "IB-робот: не удалось подключиться к TWS в отведённое время ⚠️"
        )

    # 6. Фоновая задача: статус соединения раз в час (в начале часа)
    hourly_task = asyncio.create_task(
        hourly_status_loop(ib, tg_common),
        name="hourly_status",
    )
    tasks.append(hourly_task)

    # 6.1. Фоновая задача: мониторинг портфеля
    portfolio_task = asyncio.create_task(
        portfolio_monitor_loop(ib, tg_common, stop_event),
        name="portfolio_monitor",
    )
    tasks.append(portfolio_task)

    # 7. Ожидание сигнала остановки
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("Main task cancelled, initiating shutdown.")
    finally:
        # 8. Корректно закрываем всё

        # 8.1. Сообщение в общий канал о том, что робот останавливается
        try:
            await tg_common.send("IB-робот: остановка работы (shutdown) ⏹")
        except Exception:
            pass

        # 8.2. Останавливаем коннектор и все фоновые задачи (включая стримеры и мониторинги)
        try:
            await ib.shutdown()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        # 8.3. Закрываем Telegram-сессии
        await tg_common.close()
        await tg_trading.close()

        # 8.4. Закрываем БД цен
        await db.close()

        logger.info("Robot shutdown completed.")


if __name__ == "__main__":
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )

    # Здесь больше не ловим KeyboardInterrupt — SIGINT обрабатывается внутри main()
    asyncio.run(main())
