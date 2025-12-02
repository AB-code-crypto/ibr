import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence, Dict, Iterable

from ib_insync import IB, Contract

from .price_db import PriceDB, PriceBar

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class InstrumentConfig:
    """
    Конфигурация инструмента для коллектора/стримера цен.

    name            — логическое имя инструмента / имя таблицы в БД.
    contract        — ib_insync.Contract (Future, Stock и т.п.).
    history_lookback — fallback, сколько истории хотим иметь "назад" от текущего момента,
                       если БД пустая и history_start не задан.
    history_start   — явная дата старта истории (UTC); если задана и БД пустая, качаем от неё.
    expiry          — дата экспирации контракта (UTC), пока только на будущее.
    """
    name: str
    contract: Contract
    history_lookback: timedelta = timedelta(days=1)
    history_start: Optional[datetime] = None
    expiry: Optional[datetime] = None


class PriceCollector:
    """
    Сборщик исторических 5-секундных баров из IB в SQLite.

    Ответственность:
      - для указанного инструмента:
        * создать таблицу (PriceDB.ensure_table),
        * посмотреть последний бар в БД,
        * догрузить недостающую историю до "сейчас" кусками по chunk_seconds,
        * записать бары в БД в формате UTC (через PriceDB.insert_bars).

    Никакой логики Telegram/робота/аналитики здесь нет — только IB + PriceDB.
    """

    def __init__(self, ib: IB, db: PriceDB) -> None:
        self.ib = ib
        self.db = db
        self.log = logging.getLogger(__name__ + ".PriceCollector")

    # ------------------------------------------------------------------ #
    # Публичный интерфейс                                                #
    # ------------------------------------------------------------------ #

    async def sync_history_for(
        self,
        cfg: InstrumentConfig,
        *,
        chunk_seconds: int = 3600,
        cancel_event: Optional[asyncio.Event] = None,
    ) -> int:
        """
        Догрузить историю 5-секундных баров для одного инструмента.

        Работает так:
          1. Убедиться, что IB подключен.
          2. Создать таблицу в БД (если её нет).
          3. Узнать последний бар в БД.
          4. Определить нижнюю границу истории (history_start или history_lookback).
          5. Запрашивать у IB историю "с конца в прошлое" кусками по chunk_seconds,
             пока не закроем gap.
          6. Записать бары в БД.

        cancel_event, если передан, позволяет аккуратно прервать цикл backfill.

        Возвращает количество вставленных баров.
        """
        if not self.ib.isConnected():
            raise RuntimeError("IB is not connected; call PriceCollector after IBConnect.connect()")

        name = cfg.name

        if cancel_event is not None and cancel_event.is_set():
            self.log.info("PriceCollector[%s]: cancellation requested before start; skipping", name)
            return 0

        # 1. Таблица в БД
        await self.db.ensure_table(name)

        # 2. Последний бар в БД (если есть)
        last_dt: Optional[datetime] = await self.db.get_last_bar_datetime(name)
        now_utc = datetime.now(timezone.utc)

        if last_dt is None:
            # БД пустая
            if cfg.history_start is not None:
                hs = cfg.history_start
                # Нормализуем к UTC
                if hs.tzinfo is None:
                    hs = hs.replace(tzinfo=timezone.utc)
                else:
                    hs = hs.astimezone(timezone.utc)

                lower_bound = hs
                self.log.info(
                    "PriceCollector[%s]: empty DB, will backfill from explicit history_start %s to now (%s)",
                    name,
                    lower_bound,
                    now_utc,
                )
            else:
                lower_bound = now_utc - cfg.history_lookback
                self.log.info(
                    "PriceCollector[%s]: empty DB, will backfill from %s (now - history_lookback=%s) to now (%s)",
                    name,
                    lower_bound,
                    cfg.history_lookback,
                    now_utc,
                )
        else:
            # БД уже содержит часть истории: докачиваем gap от last_dt до "сейчас"
            lower_bound = last_dt
            self.log.info(
                "PriceCollector[%s]: last bar in DB at %s, will backfill up to now (%s)",
                name,
                last_dt,
                now_utc,
            )

        # Если по какой-то причине lower_bound >= now_utc, докачивать нечего
        if lower_bound >= now_utc:
            self.log.info("PriceCollector[%s]: no gap to backfill", name)
            return 0

        inserted_total = 0
        current_end = now_utc
        duration_str = f"{int(chunk_seconds)} S"

        while current_end > lower_bound:
            if cancel_event is not None and cancel_event.is_set():
                self.log.info(
                    "PriceCollector[%s]: cancellation requested; stopping backfill loop.",
                    name,
                )
                break

            end_str = self._format_end_datetime(current_end)

            self.log.info(
                "PriceCollector[%s]: requesting history chunk end=%s, duration=%s",
                name,
                end_str,
                duration_str,
            )

            try:
                bars = await self.ib.reqHistoricalDataAsync(
                    cfg.contract,
                    endDateTime=end_str,
                    durationStr=duration_str,
                    barSizeSetting="5 secs",
                    whatToShow="TRADES",
                    useRTH=False,
                    formatDate=2,  # datetime
                    keepUpToDate=False,
                    chartOptions=[],
                )
            except Exception as e:
                self.log.error(
                    "PriceCollector[%s]: historical request failed at end=%s: %s",
                    name,
                    end_str,
                    e,
                )
                break

            if cancel_event is not None and cancel_event.is_set():
                self.log.info(
                    "PriceCollector[%s]: cancellation requested after historical request; stopping.",
                    name,
                )
                break

            if not bars:
                self.log.info(
                    "PriceCollector[%s]: no historical bars returned at end=%s, stopping.",
                    name,
                    end_str,
                )
                break

            new_bars: list[PriceBar] = []
            earliest_dt: Optional[datetime] = None

            for bar in bars:
                bar_dt = getattr(bar, "date", None)
                if bar_dt is None:
                    self.log.warning(
                        "PriceCollector[%s]: bar without .date field, skipping: %r", name, bar
                    )
                    continue

                if earliest_dt is None or bar_dt < earliest_dt:
                    earliest_dt = bar_dt

                volume = float(getattr(bar, "volume", 0.0) or 0.0)
                pb = PriceBar.from_datetime(
                    bar_dt,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    volume,
                )
                new_bars.append(pb)

            if not new_bars:
                self.log.info(
                    "PriceCollector[%s]: no valid bars parsed for end=%s, stopping.",
                    name,
                    end_str,
                )
                break

            inserted = await self.db.insert_bars(name, new_bars)
            inserted_total += inserted

            self.log.info(
                "PriceCollector[%s]: inserted %d bars (total=%d) for chunk ending %s",
                name,
                inserted,
                inserted_total,
                end_str,
            )

            if earliest_dt is None:
                break

            # следующий запрос делаем "до earliest_dt - 1 сек"
            current_end = earliest_dt - timedelta(seconds=1)

            if current_end <= lower_bound:
                break

        self.log.info(
            "PriceCollector[%s]: finished backfill, total inserted=%d",
            name,
            inserted_total,
        )
        return inserted_total

    async def sync_many(
        self,
        configs: Sequence[InstrumentConfig],
        *,
        chunk_seconds: int = 3600,
        cancel_event: Optional[asyncio.Event] = None,
    ) -> Dict[str, int]:
        """
        Последовательно синхронизирует историю для набора инструментов.

        Возвращает dict: {instrument_name: inserted_count}.
        """
        result: Dict[str, int] = {}

        for cfg in configs:
            if cancel_event is not None and cancel_event.is_set():
                self.log.info(
                    "PriceCollector: cancellation requested before instrument %s; stopping.",
                    cfg.name,
                )
                break

            try:
                inserted = await self.sync_history_for(
                    cfg,
                    chunk_seconds=chunk_seconds,
                    cancel_event=cancel_event,
                )
            except Exception as e:
                self.log.error(
                    "PriceCollector: failed to sync history for %s: %s",
                    cfg.name,
                    e,
                )
                inserted = 0

            result[cfg.name] = inserted

        return result

    # ------------------------------------------------------------------ #
    # Внутреннее                                                         #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _format_end_datetime(dt: datetime) -> str:
        """
        Привести datetime к формату, который ожидает IB в endDateTime.
        'YYYYMMDD HH:MM:SS' в UTC.
        """
        if dt.tzinfo is None:
            dt_utc = dt.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%Y%m%d %H:%M:%S")


# ---------------------------------------------------------------------- #
# PriceStreamer — real-time 5-секундные бары                             #
# ---------------------------------------------------------------------- #

class PriceStreamer:
    """
    Стример real-time 5-секундных баров из IB в SQLite.

    Использует IB.reqRealTimeBars (5-секундные бары — стандартный режим IB)
    и складывает новые бары в очередь, откуда async-цикл забирает их и пишет в БД.
    """

    def __init__(self, ib: IB, db: PriceDB) -> None:
        self.ib = ib
        self.db = db
        self.log = logging.getLogger(__name__ + ".PriceStreamer")

    async def stream_bars(
        self,
        cfg: InstrumentConfig,
        *,
        cancel_event: Optional[asyncio.Event] = None,
        insert_batch_size: int = 100,
    ) -> None:
        """
        Подписаться на real-time 5-секундные бары по одному инструменту и
        писать их в SQLite, пока не выставлен cancel_event.

        Работает в одном event loop вместе с остальным роботом.
        """
        name = cfg.name

        if not self.ib.isConnected():
            raise RuntimeError("IB is not connected; call PriceStreamer after IBConnect.connect()")

        await self.db.ensure_table(name)

        # Очередь для мостика "callback -> async loop"
        queue: asyncio.Queue = asyncio.Queue()

        # Подписка на real-time бары
        try:
            rt_bars = self.ib.reqRealTimeBars(
                cfg.contract,
                whatToShow="TRADES",
                useRTH=False,
                realTimeBarsOptions=[],
            )
        except Exception as e:
            self.log.error("PriceStreamer[%s]: reqRealTimeBars failed: %s", name, e)
            return

        self.log.info("PriceStreamer[%s]: subscribed to real-time bars", name)

        def on_update(bars, hasNewBar: bool) -> None:
            if not hasNewBar:
                return
            try:
                bar = bars[-1]
                queue.put_nowait(bar)
            except Exception as exc:  # логируем, но не роняем стример
                self.log.error("PriceStreamer[%s]: failed to enqueue RT bar: %s", name, exc)

        rt_bars.updateEvent += on_update

        batch: list[PriceBar] = []

        try:
            while cancel_event is None or not cancel_event.is_set():
                try:
                    # ждём новый бар, но с таймаутом, чтобы периодически делать flush
                    bar = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    if batch:
                        await self._flush_batch(name, batch)
                    continue

                # В ib_insync RealTimeBar.time — datetime в UTC.
                bar_dt = getattr(bar, "time", None)
                if bar_dt is None:
                    self.log.warning("PriceStreamer[%s]: RT bar without .time, skipping: %r", name, bar)
                    continue

                if bar_dt.tzinfo is None:
                    bar_dt = bar_dt.replace(tzinfo=timezone.utc)
                else:
                    bar_dt = bar_dt.astimezone(timezone.utc)

                volume = float(getattr(bar, "volume", 0.0) or 0.0)

                pb = PriceBar.from_datetime(
                    bar_dt,
                    getattr(bar, "open", getattr(bar, "open_", None)),
                    bar.high,
                    bar.low,
                    bar.close,
                    volume,
                )
                batch.append(pb)

                if len(batch) >= insert_batch_size:
                    await self._flush_batch(name, batch)

            # cancel_event выставлен — финальный flush
            if batch:
                await self._flush_batch(name, batch)

        except asyncio.CancelledError:
            # Нормальное завершение задачи при shutdown
            self.log.info("PriceStreamer[%s]: cancelled, shutting down", name)
            if batch:
                try:
                    await self._flush_batch(name, batch)
                except Exception as e:
                    self.log.error(
                        "PriceStreamer[%s]: error during final flush on cancel: %s",
                        name,
                        e,
                    )
            raise
        finally:
            # Отписываем callback и отменяем подписку на real-time бары
            try:
                rt_bars.updateEvent -= on_update
            except Exception:
                pass
            try:
                self.ib.cancelRealTimeBars(rt_bars)
            except Exception as e:
                self.log.warning(
                    "PriceStreamer[%s]: error cancelling real-time bars: %s",
                    name,
                    e,
                )
            self.log.info("PriceStreamer[%s]: stream finished", name)

    async def _flush_batch(self, name: str, batch: list[PriceBar]) -> None:
        """
        Вспомогательная функция: записать пачку баров в БД и очистить список.
        """
        if not batch:
            return
        inserted = await self.db.insert_bars(name, batch)
        self.log.info(
            "PriceStreamer[%s]: inserted %d real-time bars",
            name,
            inserted,
        )
        batch.clear()
