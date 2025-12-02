import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence, Dict

from ib_insync import IB, Contract

from .price_db import PriceDB, PriceBar


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class InstrumentConfig:
    """
    Конфигурация инструмента для коллектора цен.

    name            — логическое имя инструмента / имя таблицы в БД.
    contract        — ib_insync.Contract (Future, Stock и т.п.).
    history_lookback — сколько истории хотим иметь "назад" от текущего момента,
                       если БД пустая (по умолчанию 1 день).
    """
    name: str
    contract: Contract
    history_lookback: timedelta = timedelta(days=1)


class PriceCollector:
    """
    Сборщик исторических 5-секундных баров из IB в SQLite.

    Ответственность:
      - для указанного инструмента:
        * создать таблицу (PriceDB.ensure_table),
        * посмотреть последний бар в БД,
        * докачать недостающую историю до "сейчас" кусками по chunk_seconds,
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
        Докачать историю 5-секундных баров для одного инструмента.

        Логика:
          - создаём таблицу в БД (если её ещё нет);
          - берём время последнего бара в БД:
              * если БД пустая — считаем, что хотим history_lookback назад от "сейчас";
              * если есть бары — хотим заполнить gap от last_dt до "сейчас";
          - запускаем цикл запросов к IB с durationStr = '{chunk_seconds} S',
            двигаясь назад от "сейчас" до нижней границы (last_dt / lookback);
          - на каждой итерации:
              * забираем бары,
              * фильтруем только те, что строго > lower_bound,
              * конвертируем в PriceBar (UTC),
              * пачкой пишем в БД.

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
            # БД пустая: хотим history_lookback назад от "сейчас"
            target_start = now_utc - cfg.history_lookback
            lower_bound = target_start
            self.log.info(
                "PriceCollector[%s]: empty DB, will backfill from %s to now (%s)",
                name,
                target_start,
                now_utc,
            )
        else:
            # БД уже содержит часть истории: докачиваем gap от last_dt до "сейчас"
            target_start = last_dt
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
                    formatDate=2,      # datetime
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

                if not isinstance(bar_dt, datetime):
                    self.log.warning(
                        "PriceCollector[%s]: unexpected bar.date=%r, skipping",
                        name,
                        bar_dt,
                    )
                    continue

                # Приводим к UTC
                if bar_dt.tzinfo is None:
                    bar_dt = bar_dt.replace(tzinfo=timezone.utc)
                else:
                    bar_dt = bar_dt.astimezone(timezone.utc)

                # Фильтруем всё, что уже есть или старше target_start
                if bar_dt <= lower_bound:
                    continue

                pb = PriceBar.from_datetime(
                    bar_dt,
                    float(bar.open),
                    float(bar.high),
                    float(bar.low),
                    float(bar.close),
                    float(getattr(bar, "volume", 0.0) or 0.0),
                )
                new_bars.append(pb)

                if earliest_dt is None or bar_dt < earliest_dt:
                    earliest_dt = bar_dt

            if not new_bars:
                self.log.info(
                    "PriceCollector[%s]: chunk at end=%s contained no new bars above %s, stopping.",
                    name,
                    end_str,
                    lower_bound,
                )
                break

            # Сортируем по времени на всякий случай
            new_bars.sort(key=lambda b: b.time_utc)

            await self.db.insert_bars(name, new_bars)
            inserted_total += len(new_bars)

            self.log.info(
                "PriceCollector[%s]: inserted %d new bars (%s .. %s)",
                name,
                len(new_bars),
                new_bars[0].time_utc,
                new_bars[-1].time_utc,
            )

            # Если earliest_dt не найден (хотя не должно), выходим
            if earliest_dt is None:
                break

            # Если уже дошли до нижней границы — выходим
            if earliest_dt <= lower_bound:
                break

            # Двигаем конец интервала назад и делаем паузу, чтобы не ловить pacing-лимиты
            current_end = earliest_dt - timedelta(seconds=1)
            await asyncio.sleep(0.5)

        self.log.info(
            "PriceCollector[%s]: backfill completed, inserted total %d bars",
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
        Докачать историю сразу для нескольких инструментов (последовательно).

        Возвращает словарь:
            {cfg.name: вставлено_баров}

        cancel_event, если передан, позволяет прервать процесс посередине списка.
        """
        results: Dict[str, int] = {}

        for cfg in configs:
            if cancel_event is not None and cancel_event.is_set():
                self.log.info(
                    "PriceCollector: cancellation requested before %s; stopping sync_many.",
                    cfg.name,
                )
                break

            count = await self.sync_history_for(
                cfg,
                chunk_seconds=chunk_seconds,
                cancel_event=cancel_event,
            )
            results[cfg.name] = count

        return results

    # ------------------------------------------------------------------ #
    # Вспомогательные методы                                            #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _format_end_datetime(dt: datetime) -> str:
        """
        Преобразование datetime в формат строки, понятный IB API.

        Формат: 'YYYYMMDD HH:MM:SS UTC'
        """
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%Y%m%d %H:%M:%S UTC")
