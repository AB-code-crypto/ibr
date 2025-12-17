"""Pattern strategy (Pearson-like matching via cosine similarity on standardized vectors)."""

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, Optional

from core.config import PRICE_DB_PATH, PATTERNS_DB_PATH

from .constants import SLOT_SECONDS
from .features import build_current_pattern_vector
from .similarity import cosine_similarity_normalized
from .patterns_repository import PatternsRepository
from .types import ClusterRuntimeInfo, PatternSignal

logger = logging.getLogger(__name__)


class PatternStrategy:
    """ТС по паттернам на основе кластеров из patterns.db.

    Public API stays stable:
      - PatternStrategy(instrument_root)
      - evaluate_for_contract("MNQZ5") -> PatternSignal
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
        self.instrument = instrument
        self.bars_per_segment = bars_per_segment
        self.min_entry_minutes_after_hour_start = min_entry_minutes_after_hour_start
        self.min_entry_minutes_before_hour_end = min_entry_minutes_before_hour_end
        self.min_similarity = min_similarity
        self.min_observations = min_observations
        self.prob_threshold = prob_threshold
        self.mean_ret_threshold = mean_ret_threshold

        self._clusters_by_block: Dict[int, tuple[ClusterRuntimeInfo, ...]] = {}
        self._max_segments: Optional[int] = None

        # Load patterns snapshot once at startup (no SQLite in hot path)
        self._patterns_repo = PatternsRepository(
            patterns_db_path=PATTERNS_DB_PATH,
            instrument=self.instrument,
            bars_per_segment=self.bars_per_segment,
        )
        snap = self._patterns_repo.load_snapshot()
        self._clusters_by_block = snap.clusters_by_block
        self._max_segments = snap.max_segments

    def evaluate_for_contract(self, contract: str) -> PatternSignal:
        """Evaluate the current state for a contract table from price.db."""
        try:
            price_conn = sqlite3.connect(PRICE_DB_PATH)
        except Exception as e:
            logger.error("price_db open error: %s", e)
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

            time_str, _last_close = row
            now_dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            hour_start_dt = now_dt.replace(minute=0, second=0, microsecond=0)
            offset_sec = int((now_dt - hour_start_dt).total_seconds())

            # 2) Entry window inside the hour
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

            # 3) Slot within hour
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

            # 4) Load hour bars (from hour start to now_dt)
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

        # 5) Build runtime vector
        pattern_vec = build_current_pattern_vector(
            closes,
            bars_per_segment=self.bars_per_segment,
            max_segments=self._max_segments,
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

        # 6) Pick best cluster within the 3-hour session block
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
            sim = cosine_similarity_normalized(pattern_vec, cl.center)
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

        # 7) Stats for (cluster, slot)
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