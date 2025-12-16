import asyncio
import logging

from core.ib_connect import IBConnect
from core.price_db import PriceDB
from core.price_get import InstrumentConfig, PriceCollector, PriceStreamer

logger = logging.getLogger(__name__)


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

