"""Feature extraction for pattern_pirson strategy."""

from typing import List, Optional

import numpy as np


def build_current_pattern_vector(
        closes: List[float],
        *,
        bars_per_segment: int,
        max_segments: Optional[int],
) -> Optional[np.ndarray]:
    """Build normalized hour-shape vector from closes.

    Pipeline (kept consistent with offline clustering):
      - log returns
      - cumulative sum
      - sample at segment boundaries (bars_per_segment)
      - normalize to zero-mean / unit-std

    Returns:
      np.ndarray(float32) of shape (L,) or None if insufficient data.
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

    seg_count = returns.size // bars_per_segment
    if seg_count <= 0:
        return None

    if max_segments is not None:
        seg_count = min(seg_count, max_segments)

    R = np.cumsum(returns)

    indices = (np.arange(seg_count, dtype=np.int64) + 1) * bars_per_segment - 1
    indices = np.clip(indices, 0, R.size - 1)
    vec = R[indices].astype(np.float32)

    mean = float(vec.mean())
    std = float(vec.std())
    if std > 0.0:
        vec = (vec - mean) / std
    else:
        vec = vec - mean

    return vec
