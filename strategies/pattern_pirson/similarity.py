"""Similarity measures for runtime cluster matching."""

from typing import Optional

import numpy as np


def cosine_similarity_normalized(x: np.ndarray, c: np.ndarray) -> Optional[float]:
    """Cosine similarity after per-vector standardization (zero-mean / unit-std).

    Returns value in [-1, 1] or None if undefined.
    """
    if x.size == 0:
        return None

    L = min(int(x.size), int(c.size))
    if L <= 0:
        return None

    xx = x[:L].astype(np.float64)
    cc = c[:L].astype(np.float64)

    # Standardize both vectors independently
    xx_mean = float(xx.mean())
    xx_std = float(xx.std())
    if xx_std > 0.0:
        xx = (xx - xx_mean) / xx_std
    else:
        xx = xx - xx_mean

    cc_mean = float(cc.mean())
    cc_std = float(cc.std())
    if cc_std > 0.0:
        cc = (cc - cc_mean) / cc_std
    else:
        cc = cc - cc_mean

    xx_norm = float(np.linalg.norm(xx))
    cc_norm = float(np.linalg.norm(cc))
    if xx_norm == 0.0 or cc_norm == 0.0:
        return None

    return float(np.dot(xx, cc) / (xx_norm * cc_norm))
