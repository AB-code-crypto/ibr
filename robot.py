import asyncio
import logging
import signal
from datetime import timedelta


from contracts.robot_registry import RobotSwitchesStore, load_all_robot_specs
from core.ops_db import OpsDB
from core.config import (
    PRICE_DB_PATH,
    ROBOT_OPS_DB_PATH,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID_COMMON,
    TELEGRAM_CHAT_ID_TRADING,
    futures_for_history,
)
from core.ib_connect import IBConnect
from core.ib_monitoring import build_initial_connect_message, hourly_status_loop
from core.portfolio_monitor import portfolio_monitor_loop
from core.price_db import PriceDB
from core.price_get import PriceCollector
from core.telegram import TelegramChannel
from core.trade_engine import TradeEngine
from services.price_streaming_runtime import price_streamer_loop
from services.instruments_loader import build_instruments_plan
from services.quiet_windows import QuietWindowsService
from services.telegram_formatters import _format_dt_utc, _startup_registry_message
from services.trading_runtime import trading_loop

logger = logging.getLogger(__name__)


async def main() -> None:
    # 1) БД цен
    db = PriceDB(PRICE_DB_PATH)
    await db.connect()

    # 2) robot_ops.db (операционная БД; также используем для robot_switches/quiet_windows)
    ops_db = OpsDB(ROBOT_OPS_DB_PATH)
    await ops_db.connect()
    ops_db_path = str(ops_db.db_path)

    # 3) Реестр роботов
    all_specs = load_all_robot_specs()
    known_robot_ids = [s.robot_id for s in all_specs]

    # 4) Выключатели
    switches = RobotSwitchesStore(ops_db=ops_db, cache_ttl_seconds=10)
    await switches.ensure_schema()
    await switches.seed_defaults(known_robot_ids, default_enabled=True)

    enabled_ids = await switches.enabled_robot_ids(known_robot_ids)
    enabled_specs = [s for s in all_specs if s.robot_id in enabled_ids]

    # 5) Quiet windows сервис (правила по robot_id)
    quiet_service = QuietWindowsService(ops_db=ops_db, cache_ttl_seconds=30.0)
    await quiet_service.ensure_schema()
    for spec in all_specs:
        await quiet_service.seed_default_rth_open(robot_id=spec.robot_id)

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
        startup_msg = await _startup_registry_message(all_specs, enabled_specs, ops_db_path, quiet_service)
        await tg_common.send(startup_msg)
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
            # 9) Universe (instruments plan): robot.py не должен знать, как именно собирать контракты.
            plan = build_instruments_plan(
                enabled_specs=enabled_specs,
                futures_for_history=futures_for_history,
            )
            instrument_configs = plan.instrument_configs
            cfg_by_symbol = plan.cfg_by_symbol

            collector = PriceCollector(ib=ib.client, db=db)

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
        await ops_db.close()


if __name__ == "__main__":
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )

    asyncio.run(main())
