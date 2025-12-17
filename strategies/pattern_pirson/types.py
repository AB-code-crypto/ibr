"""Types used by the pattern_pirson strategy."""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

import numpy as np


@dataclass(slots=True)
class ClusterRuntimeInfo:
    """Cluster center + precomputed slot statistics (loaded from patterns.db)."""

    cluster_db_id: int
    center: np.ndarray  # shape=(M,)
    stats_by_slot: Dict[int, Dict[str, float]]  # slot_sec -> stats dict
    bars_per_segment: int
    session_block: int


@dataclass(slots=True)
class PatternSignal:
    """Decision produced by the strategy for the current moment."""

    action: str  # "long" | "short" | "flat"
    reason: str

    cluster_db_id: Optional[int]
    similarity: Optional[float]
    slot_sec: Optional[int]
    stats: Optional[Dict[str, float]]
    now_time_utc: Optional[datetime]