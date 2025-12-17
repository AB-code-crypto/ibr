"""Load precomputed clusters from patterns.db."""

import json
import logging
from typing import Dict, List, Optional, Tuple

import numpy as np

from .patterns_db import get_connection as get_patterns_conn
from .types import ClusterRuntimeInfo

logger = logging.getLogger(__name__)


def load_clusters(
    *,
    patterns_db_path: str,
    instrument: str,
    bars_per_segment: int,
) -> Tuple[Dict[int, List[ClusterRuntimeInfo]], Optional[int]]:
    """Load hour_clusters for given instrument.

    Returns:
        (clusters_by_block, max_segments)

    clusters_by_block:
        session_block(int 0..7) -> list[ClusterRuntimeInfo]
    max_segments:
        Maximum vector length among all centers (used to align runtime vectors).
    """
    conn = get_patterns_conn(patterns_db_path)
    try:
        cur = conn.execute(
            """
            SELECT id, session_block, cluster_index, bars_per_segment, center_blob, stats_json
            FROM hour_clusters
            WHERE instrument = ?
            ORDER BY session_block ASC, cluster_index ASC;
            """,
            (instrument,),
        )
        rows = cur.fetchall()

        if not rows:
            logger.warning("hour_clusters: no records for instrument=%s", instrument)
            return {}, None

        clusters_by_block: Dict[int, List[ClusterRuntimeInfo]] = {}
        max_segments: Optional[int] = None

        for row in rows:
            cluster_db_id = int(row[0])
            session_block = int(row[1])
            # cluster_index = int(row[2])  # not needed at runtime
            bps = int(row[3])
            center_blob = bytes(row[4])
            stats_json = str(row[5])

            if bps != bars_per_segment:
                logger.warning(
                    "hour_clusters: bars_per_segment mismatch (cluster=%s, expected=%s), "
                    "skip cluster id=%s",
                    bps,
                    bars_per_segment,
                    cluster_db_id,
                )
                continue

            center = np.frombuffer(center_blob, dtype=np.float32)
            if center.ndim != 1 or center.size == 0:
                continue

            if max_segments is None or center.size > max_segments:
                max_segments = center.size

            stats_by_slot = _decode_stats(stats_json)

            info = ClusterRuntimeInfo(
                cluster_db_id=cluster_db_id,
                center=center.astype(np.float32),
                stats_by_slot=stats_by_slot,
                bars_per_segment=bps,
                session_block=session_block,
            )
            clusters_by_block.setdefault(session_block, []).append(info)

        logger.info(
            "Loaded clusters for %s: total=%s, blocks=%s",
            instrument,
            sum(len(v) for v in clusters_by_block.values()),
            sorted(clusters_by_block.keys()),
        )
        return clusters_by_block, max_segments
    finally:
        conn.close()


def _decode_stats(stats_json: str) -> Dict[int, Dict[str, float]]:
    """Decode stats_json (slots stored as string keys)."""
    try:
        data = json.loads(stats_json)
    except json.JSONDecodeError:
        return {}

    slots_raw = data.get("slots", {})
    if not isinstance(slots_raw, dict):
        return {}

    out: Dict[int, Dict[str, float]] = {}
    for k, v in slots_raw.items():
        try:
            slot_sec = int(k)
        except (TypeError, ValueError):
            continue
        if not isinstance(v, dict):
            continue
        out[slot_sec] = v
    return out