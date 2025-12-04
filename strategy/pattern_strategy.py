import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np

from core.config import PRICE_DB_PATH, PATTERNS_DB_PATH
from core.patterns_db import get_connection as get_patterns_conn

logger = logging.getLogger(__name__)

# Слоты (секунды от начала часа), для которых мы уже посчитали stats_json
SLOT_SECONDS = [1800, 2100, 2400, 2700, 3000, 3300]


@dataclass
class ClusterRuntimeInfo:
    cluster_db_id: int
    center: np.ndarray  # shape = (M,)
    stats_by_slot: Dict[int, Dict[str, float]]  # slot_sec -> stats dict
    bars_per_segment: int
    session_block: int


@dataclass
class PatternSignal:
    action: str  # "long", "short", "flat"
    reason: str
    cluster_db_id: Optional[int]
    similarity: Optional[float]
    slot_sec: Optional[int]
    stats: Optional[Dict[str, float]]
    now_time_utc: Optional[datetime]


class PatternStrategy:
    """
    ТС по паттернам на основе кластеров из patterns.db.

    Типичный сценарий:
      strat = PatternStrategy("MNQ")
      signal = strat.evaluate_for_contract("MNQZ5")

    Дальше по signal.action / signal.reason ты решаешь,
    отправлять ли ордер и в какую сторону.
    """

    def __init__(
            self,
            instrument: str,
            bars_per_segment: int = 1,
            min_entry_minutes_after_hour_start: int = 30,
            min_entry_minutes_before_hour_end: int = 5,
            min_similarity: float = 0.7,
            min_observations: int = 30,
            prob_threshold: float = 0.6,
            mean_ret_threshold: float = 0.0005,
    ) -> None:
        """
        :param instrument: базовый тикер (например, "MNQ").
        :param bars_per_segment: сколько 5-сек баров в одном сегменте паттерна
                                 (должно совпадать с кластеризацией, сейчас =1).
        :param min_entry_minutes_after_hour_start: не входим раньше, чем через N минут
                                                   после начала часа (по ТЗ — 30).
        :param min_entry_minutes_before_hour_end: не входим позже, чем за N минут до конца
                                                  часа (по ТЗ — 5).
        :param min_similarity: минимальная похожесть (косинус / корреляция) с кластером.
        :param min_observations: минимальное число наблюдений в слоте, чтобы доверять stats.
        :param prob_threshold: порог вероятности роста/падения (p_up >= prob_threshold -> long).
        :param mean_ret_threshold: минимальный по модулю ожидаемый доход, чтобы входить.
        """
        self.instrument = instrument
        self.bars_per_segment = bars_per_segment
        self.min_entry_minutes_after_hour_start = min_entry_minutes_after_hour_start
        self.min_entry_minutes_before_hour_end = min_entry_minutes_before_hour_end
        self.min_similarity = min_similarity
        self.min_observations = min_observations
        self.prob_threshold = prob_threshold
        self.mean_ret_threshold = mean_ret_threshold

        self._clusters_by_block: Dict[int, List[ClusterRuntimeInfo]] = {}
        self._max_segments: Optional[int] = None  # длина центров (M)
        self._load_clusters()

    # ---------------- Публичный метод: оценка сигнала ----------------

    def evaluate_for_contract(self, contract: str) -> PatternSignal:
        """
        Оценить ситуацию по последнему бару контракта и выдать торговый сигнал.

        contract: имя таблицы в price DB, например "MNQZ5".

        Функция синхронная и использует sqlite3 (блокирующая).
        Для интеграции в чисто async-конвейер её можно вызывать в отдельном потоке
        или потом адаптировать под aiosqlite.
        """
        # 1. Берём последний бар по контракту
        try:
            price_conn = sqlite3.connect(PRICE_DB_PATH)
        except Exception as e:
            logger.error("Не удалось открыть price DB: %s", e)
            return PatternSignal(
                action="flat",
                reason=f"price_db_error: {e}",
                cluster_db_id=None,
                similarity=None,
                slot_sec=None,
                stats=None,
                now_time_utc=None,
            )

        try:
            row = price_conn.execute(
                f'SELECT time_utc, close FROM "{contract}" ORDER BY time_utc DESC LIMIT 1;'
            ).fetchone()
            if not row:
                return PatternSignal(
                    action="flat",
                    reason="no_bars_for_contract",
                    cluster_db_id=None,
                    similarity=None,
                    slot_sec=None,
                    stats=None,
                    now_time_utc=None,
                )

            time_str, last_close = row
            now_dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )

            hour_start_dt = now_dt.replace(minute=0, second=0, microsecond=0)
            offset_sec = int((now_dt - hour_start_dt).total_seconds())

            # 2. Проверяем окно входа по времени
            if offset_sec < self.min_entry_minutes_after_hour_start * 60:
                return PatternSignal(
                    action="flat",
                    reason="too_early_in_hour",
                    cluster_db_id=None,
                    similarity=None,
                    slot_sec=None,
                    stats=None,
                    now_time_utc=now_dt,
                )

            last_entry_sec = (60 - self.min_entry_minutes_before_hour_end) * 60
            if offset_sec > last_entry_sec:
                return PatternSignal(
                    action="flat",
                    reason="too_late_in_hour",
                    cluster_db_id=None,
                    similarity=None,
                    slot_sec=None,
                    stats=None,
                    now_time_utc=now_dt,
                )

            # 3. Определяем слот внутри часа
            slot_candidates = [s for s in SLOT_SECONDS if s <= offset_sec]
            if not slot_candidates:
                return PatternSignal(
                    action="flat",
                    reason="no_valid_slot_for_offset",
                    cluster_db_id=None,
                    similarity=None,
                    slot_sec=None,
                    stats=None,
                    now_time_utc=now_dt,
                )
            slot_sec = max(slot_candidates)

            # 4. Загружаем бары текущего часа
            hour_start_str = hour_start_dt.strftime("%Y-%m-%d %H:%M:%S")
            now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
            cur = price_conn.execute(
                f'''
                SELECT time_utc, close
                FROM "{contract}"
                WHERE time_utc >= ? AND time_utc <= ?
                ORDER BY time_utc ASC;
                ''',
                (hour_start_str, now_str),
            )
            rows = cur.fetchall()
            if len(rows) < 2:
                return PatternSignal(
                    action="flat",
                    reason="not_enough_bars_in_hour",
                    cluster_db_id=None,
                    similarity=None,
                    slot_sec=slot_sec,
                    stats=None,
                    now_time_utc=now_dt,
                )

            closes = [float(r[1]) for r in rows]
        finally:
            price_conn.close()

        # 5. Строим текущий паттерн
        try:
            pattern_vec = self._build_current_pattern_vector(closes)
        except ValueError as e:
            return PatternSignal(
                action="flat",
                reason=f"pattern_build_error: {e}",
                cluster_db_id=None,
                similarity=None,
                slot_sec=slot_sec,
                stats=None,
                now_time_utc=now_dt,
            )

        if pattern_vec is None:
            return PatternSignal(
                action="flat",
                reason="pattern_vec_none",
                cluster_db_id=None,
                similarity=None,
                slot_sec=slot_sec,
                stats=None,
                now_time_utc=now_dt,
            )

        # 6. Находим ближайший кластер для соответствующего session_block
        session_block = hour_start_dt.hour // 3
        clusters = self._clusters_by_block.get(session_block)
        if not clusters:
            return PatternSignal(
                action="flat",
                reason=f"no_clusters_for_block_{session_block}",
                cluster_db_id=None,
                similarity=None,
                slot_sec=slot_sec,
                stats=None,
                now_time_utc=now_dt,
            )

        best_cluster: Optional[ClusterRuntimeInfo] = None
        best_sim: float = -1.0

        for cl in clusters:
            sim = self._similarity_with_cluster(pattern_vec, cl.center)
            if sim is None:
                continue
            if sim > best_sim:
                best_sim = sim
                best_cluster = cl

        if best_cluster is None or best_sim < self.min_similarity:
            return PatternSignal(
                action="flat",
                reason="no_cluster_with_enough_similarity",
                cluster_db_id=None if best_cluster is None else best_cluster.cluster_db_id,
                similarity=None if best_cluster is None else best_sim,
                slot_sec=slot_sec,
                stats=None,
                now_time_utc=now_dt,
            )

        # 7. Берём статистику по выбранному кластеру и слоту
        stats = best_cluster.stats_by_slot.get(slot_sec)
        if not stats:
            return PatternSignal(
                action="flat",
                reason="no_stats_for_slot",
                cluster_db_id=best_cluster.cluster_db_id,
                similarity=best_sim,
                slot_sec=slot_sec,
                stats=None,
                now_time_utc=now_dt,
            )

        n = int(stats.get("n", 0))
        if n < self.min_observations:
            return PatternSignal(
                action="flat",
                reason=f"not_enough_observations_in_slot_{n}",
                cluster_db_id=best_cluster.cluster_db_id,
                similarity=best_sim,
                slot_sec=slot_sec,
                stats=stats,
                now_time_utc=now_dt,
            )

        p_up = float(stats.get("p_up", 0.0))
        mean_ret = float(stats.get("mean_ret", 0.0))

        # 8. Решаем направление сделки
        action = "flat"
        reason = "no_edge"

        if p_up >= self.prob_threshold and mean_ret >= self.mean_ret_threshold:
            action = "long"
            reason = "cluster_long_edge"
        elif p_up <= (1.0 - self.prob_threshold) and mean_ret <= -self.mean_ret_threshold:
            action = "short"
            reason = "cluster_short_edge"

        return PatternSignal(
            action=action,
            reason=reason,
            cluster_db_id=best_cluster.cluster_db_id,
            similarity=best_sim,
            slot_sec=slot_sec,
            stats=stats,
            now_time_utc=now_dt,
        )

    # ---------------- Внутренние методы ----------------

    def _load_clusters(self) -> None:
        """
        Загружаем кластеры для self.instrument из patterns.db.
        """
        conn = get_patterns_conn(PATTERNS_DB_PATH)
        try:
            cur = conn.execute(
                """
                SELECT id, session_block, cluster_index, bars_per_segment, center_blob, stats_json
                FROM hour_clusters
                WHERE instrument = ?
                ORDER BY session_block ASC, cluster_index ASC;
                """,
                (self.instrument,),
            )
            rows = cur.fetchall()

            if not rows:
                logger.warning("В hour_clusters нет записей для инструмента %s", self.instrument)
                return

            clusters_by_block: Dict[int, List[ClusterRuntimeInfo]] = {}
            max_segments: Optional[int] = None

            for row in rows:
                cluster_db_id = int(row[0])
                session_block = int(row[1])
                cluster_index = int(row[2])
                bars_per_segment = int(row[3])
                center_blob = bytes(row[4])
                stats_json = str(row[5])

                if bars_per_segment != self.bars_per_segment:
                    logger.warning(
                        "bars_per_segment в кластере (%s) != ожидаемого (%s), "
                        "пропускаю кластер id=%s",
                        bars_per_segment,
                        self.bars_per_segment,
                        cluster_db_id,
                    )
                    continue

                center = np.frombuffer(center_blob, dtype=np.float32)
                if center.ndim != 1 or center.size == 0:
                    continue

                if max_segments is None or center.size > max_segments:
                    max_segments = center.size

                try:
                    data = json.loads(stats_json)
                except json.JSONDecodeError:
                    data = {"slots": {}}

                slots_raw = data.get("slots", {})
                stats_by_slot: Dict[int, Dict[str, float]] = {}
                for k, v in slots_raw.items():
                    try:
                        slot_sec = int(k)
                    except ValueError:
                        continue
                    if not isinstance(v, dict):
                        continue
                    stats_by_slot[slot_sec] = v

                info = ClusterRuntimeInfo(
                    cluster_db_id=cluster_db_id,
                    center=center.astype(np.float32),
                    stats_by_slot=stats_by_slot,
                    bars_per_segment=bars_per_segment,
                    session_block=session_block,
                )
                clusters_by_block.setdefault(session_block, []).append(info)

            self._clusters_by_block = clusters_by_block
            self._max_segments = max_segments

            logger.info(
                "Загружено кластеров для %s: %s (blocks=%s)",
                self.instrument,
                sum(len(v) for v in clusters_by_block.values()),
                sorted(clusters_by_block.keys()),
            )
        finally:
            conn.close()

    def _build_current_pattern_vector(self, closes: List[float]) -> Optional[np.ndarray]:
        """
        Построить вектор паттерна по текущим барам часа.

        Логика максимально совместима по духу с кластеризацией:
          - лог-доходности,
          - кумулятивная сумма,
          - сегменты по bars_per_segment,
          - нормализация (zero-mean / unit-std).
        """
        if len(closes) < 2:
            return None

        closes_arr = np.asarray(closes, dtype=np.float64)
        prev = closes_arr[:-1]
        curr = closes_arr[1:]
        mask = (prev > 0.0) & (curr > 0.0)
        if not np.all(mask):
            prev = prev[mask]
            curr = curr[mask]
        if prev.size == 0 or curr.size == 0:
            return None

        returns = np.log(curr / prev).astype(np.float32)
        seg_count = returns.size // self.bars_per_segment
        if seg_count <= 0:
            return None

        if self._max_segments is not None:
            seg_count = min(seg_count, self._max_segments)

        R = np.cumsum(returns)
        indices = (np.arange(seg_count, dtype=np.int64) + 1) * self.bars_per_segment - 1
        indices = np.clip(indices, 0, R.size - 1)
        vec = R[indices].astype(np.float32)

        mean = float(vec.mean())
        std = float(vec.std())
        if std > 0.0:
            vec = (vec - mean) / std
        else:
            vec = vec - mean

        return vec

    def _similarity_with_cluster(self, pattern_vec: np.ndarray, center: np.ndarray) -> Optional[float]:
        """
        Оценка похожести текущего паттерна на центр кластера.

        Используем косинусную меру:
          - обрезаем центр до длины pattern_vec (или наоборот),
          - пере-нормализуем обе выборки до zero-mean / unit-std,
          - возвращаем cos(theta) в [-1, 1].
        """
        if pattern_vec.size == 0:
            return None

        L = min(pattern_vec.size, center.size)
        if L <= 0:
            return None

        x = pattern_vec[:L].astype(np.float64)
        c = center[:L].astype(np.float64)

        # Нормализация
        x_mean = float(x.mean())
        x_std = float(x.std())
        if x_std > 0.0:
            x = (x - x_mean) / x_std
        else:
            x = x - x_mean

        c_mean = float(c.mean())
        c_std = float(c.std())
        if c_std > 0.0:
            c = (c - c_mean) / c_std
        else:
            c = c - c_mean

        x_norm = float(np.linalg.norm(x))
        c_norm = float(np.linalg.norm(c))
        if x_norm == 0.0 or c_norm == 0.0:
            return None

        sim = float(np.dot(x, c) / (x_norm * c_norm))
        return sim
