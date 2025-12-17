import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from .storage import load_clusters
from .types import ClusterRuntimeInfo

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PatternsSnapshot:
    """Immutable in-memory snapshot of patterns for PatternStrategy.

    Important: this snapshot is intended to be loaded once at strategy startup.
    Hot-path (evaluate every 5 seconds) must not touch SQLite.
    """

    instrument: str
    bars_per_segment: int
    loaded_at_utc: datetime

    clusters_by_block: Dict[int, Tuple[ClusterRuntimeInfo, ...]]
    max_segments: Optional[int]


class PatternsRepository:
    """Loads patterns from patterns.db once and serves them from memory."""

    def __init__(
        self,
        *,
        patterns_db_path: str,
        instrument: str,
        bars_per_segment: int,
    ) -> None:
        self._patterns_db_path = patterns_db_path
        self._instrument = instrument
        self._bars_per_segment = bars_per_segment
        self._snapshot: Optional[PatternsSnapshot] = None

    def load_snapshot(self) -> PatternsSnapshot:
        if self._snapshot is not None:
            return self._snapshot

        clusters_by_block_list, max_segments = load_clusters(
            patterns_db_path=self._patterns_db_path,
            instrument=self._instrument,
            bars_per_segment=self._bars_per_segment,
        )

        clusters_by_block: Dict[int, Tuple[ClusterRuntimeInfo, ...]] = {
            int(block): tuple(items) for block, items in clusters_by_block_list.items()
        }

        snap = PatternsSnapshot(
            instrument=self._instrument,
            bars_per_segment=self._bars_per_segment,
            loaded_at_utc=datetime.now(timezone.utc),
            clusters_by_block=clusters_by_block,
            max_segments=max_segments,
        )

        if not snap.clusters_by_block:
            raise RuntimeError(
                f"patterns snapshot is empty: instrument={self._instrument!r} (no clusters loaded)"
            )
        if snap.max_segments is None:
            logger.warning(
                "patterns snapshot: max_segments is None (instrument=%s). Check patterns build step.",
                self._instrument,
            )

        logger.info(
            "Patterns snapshot loaded: instrument=%s blocks=%s max_segments=%s",
            self._instrument,
            len(snap.clusters_by_block),
            snap.max_segments,
        )

        self._snapshot = snap
        return snap

    def get_snapshot(self) -> PatternsSnapshot:
        if self._snapshot is None:
            raise RuntimeError("Patterns snapshot not loaded. Call load_snapshot() during strategy init.")
        return self._snapshot