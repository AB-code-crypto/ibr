"""
Export conditional intrahour forward statistics for MNQ (5s bars) from price.db.

Purpose
-------
You already have hour-level unconditional stats (hour_summary.csv / hour_profiles.csv).
This export adds CONDITIONAL stats, answering questions like:

- If at minute t the path is below its q10 band, does the next 5/10/15 minutes
  tend to revert (mean-reversion) or continue (momentum)?
- Same for being above q90.

We compute, for each hour-of-day (UTC) and selected offsets within the hour:
- event "below_q10" (path[t] <= q10[t])
- event "above_q90" (path[t] >= q90[t])
Then for multiple forward horizons H we compute the distribution of:
    fwd_return_pct = path[t+H] - path[t]
where path is intrahour relative move in %:
    path[t] = (close[t] / close[0] - 1) * 100

Missing bars are NaN and excluded from stats (no interpolation).

Outputs
-------
artifacts/hourly_export_conditional/conditional_detailed.csv
  One row per (hour_utc, offset_sec, event, horizon_sec) with distribution metrics.

artifacts/hourly_export_conditional/conditional_summary.csv
  Aggregated per (hour_utc, event, horizon_sec) across all offsets.

How to run
----------
Run inside your ibr project (so core.config is importable):
    python ./inout/export_hourly_conditional.py

Configuration
-------------
Edit constants in the CONFIG section below. No CLI args by design.
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


# =========================
# CONFIG
# =========================

from core.config import PRICE_DB_PATH  # explicit dependency

INSTRUMENT_PREFIX = "MNQ"

OUT_DIR = Path("artifacts/hourly_export_conditional")

BAR_SECONDS = 5
POINTS_PER_HOUR = 3600 // BAR_SECONDS  # 720

# Evaluate conditional stats not on every 5s step (too many rows),
# but on a fixed step. You can set it to 5 for full resolution.
OFFSET_STEP_SECONDS = 30  # 30s step (=> 120 offsets per hour)

# Forward horizons (seconds). Must be multiples of BAR_SECONDS.
HORIZONS_SECONDS = (60, 300, 600, 900, 1800)  # 1m, 5m, 10m, 15m, 30m

# Pointwise bands for event definition (q10/q90 by default)
EVENT_Q_LOW = 0.10
EVENT_Q_HIGH = 0.90

# Quantiles for forward-return distribution in the export
FWD_RETURN_QUANTILES = (0.10, 0.25, 0.50, 0.75, 0.90)

# Optional time filter (UTC strings "YYYY-MM-DD HH:MM:SS"). None = use all.
START_UTC = None
END_UTC = None

# (For your later PnL calculations, not used in stats here)
COMMISSION_USD_PER_SIDE = 0.62
MNQ_DOLLARS_PER_POINT = 2.0


# =========================
# DATA MODEL
# =========================

@dataclass(frozen=True, slots=True)
class Segment:
    hour_utc: int
    day_utc: datetime  # midnight UTC
    values_pct: np.ndarray  # (POINTS_PER_HOUR,), float with NaN gaps (%)


# =========================
# DB LOADING
# =========================

def _list_tables(conn: sqlite3.Connection, prefix: str) -> List[str]:
    cur = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name LIKE ?
        ORDER BY name;
        """,
        (f"{prefix}%",),
    )
    return [str(r[0]) for r in cur.fetchall()]


def _read_table(conn: sqlite3.Connection, table: str, chunksize: int = 250_000) -> pd.DataFrame:
    query = f'SELECT time_utc, close FROM "{table}" ORDER BY time_utc;'
    chunks: List[pd.DataFrame] = []
    for chunk in pd.read_sql_query(query, conn, chunksize=chunksize):
        chunks.append(chunk)
    if not chunks:
        return pd.DataFrame(columns=["time_utc", "close"])
    return pd.concat(chunks, ignore_index=True)


def _parse_prices(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    t = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
    c = pd.to_numeric(df["close"], errors="coerce")

    out = pd.DataFrame({"time_utc": t, "close": c})
    out = out.dropna(subset=["time_utc", "close"])
    return out


def _apply_time_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if START_UTC is not None:
        start = pd.Timestamp(START_UTC, tz="UTC")
        df = df.loc[df["time_utc"] >= start]
    if END_UTC is not None:
        end = pd.Timestamp(END_UTC, tz="UTC")
        df = df.loc[df["time_utc"] <= end]
    return df


def load_all_prices() -> pd.DataFrame:
    db_path = Path(PRICE_DB_PATH)
    conn = sqlite3.connect(str(db_path))
    try:
        tables = _list_tables(conn, INSTRUMENT_PREFIX)
        if not tables:
            raise RuntimeError(f"No tables found in {db_path} with prefix {INSTRUMENT_PREFIX!r}")

        frames: List[pd.DataFrame] = []
        for t in tables:
            raw = _read_table(conn, t)
            df = _parse_prices(raw)
            if not df.empty:
                frames.append(df)

        if not frames:
            raise RuntimeError(f"All matching tables are empty for prefix {INSTRUMENT_PREFIX!r}")

        out = pd.concat(frames, ignore_index=True)
        out = out.sort_values("time_utc")
        out = out.drop_duplicates(subset=["time_utc"], keep="last")
        out = out[["time_utc", "close"]].reset_index(drop=True)
        out = _apply_time_filter(out)
        return out
    finally:
        conn.close()


# =========================
# SEGMENTATION
# =========================

def build_segments(df: pd.DataFrame) -> List[Segment]:
    if df.empty:
        return []

    df = df.copy()
    df["hour_utc"] = df["time_utc"].dt.hour
    df["date_utc"] = df["time_utc"].dt.floor("D")  # midnight UTC (tz-aware)

    segments: List[Segment] = []

    for (date_midnight, hour), g in df.groupby(["date_utc", "hour_utc"], sort=True):
        start = pd.Timestamp(date_midnight) + pd.Timedelta(hours=int(hour))
        idx = pd.date_range(start=start, periods=POINTS_PER_HOUR, freq=f"{BAR_SECONDS}s", tz="UTC")

        s = g.set_index("time_utc")["close"].reindex(idx)
        if s.isna().all():
            continue

        base = s.iloc[0]
        if pd.isna(base):
            base = s.dropna().iloc[0]

        rel_pct = ((s / base) - 1.0) * 100.0
        segments.append(
            Segment(
                hour_utc=int(hour),
                day_utc=pd.Timestamp(date_midnight).to_pydatetime().replace(tzinfo=timezone.utc),
                values_pct=rel_pct.to_numpy(dtype=float, copy=True),
            )
        )

    return segments


# =========================
# CONDITIONAL STATS
# =========================

def _quantiles_1d(x: np.ndarray, qs: Tuple[float, ...]) -> List[float]:
    x = x[np.isfinite(x)]
    if x.size == 0:
        return [float("nan")] * len(qs)
    return [float(np.quantile(x, q)) for q in qs]


def _safe_mean(x: np.ndarray) -> float:
    x = x[np.isfinite(x)]
    return float(np.mean(x)) if x.size else float("nan")


def _safe_std(x: np.ndarray) -> float:
    x = x[np.isfinite(x)]
    return float(np.std(x)) if x.size else float("nan")


def _safe_median(x: np.ndarray) -> float:
    x = x[np.isfinite(x)]
    return float(np.median(x)) if x.size else float("nan")


def _p_pos(x: np.ndarray) -> float:
    x = x[np.isfinite(x)]
    if x.size == 0:
        return float("nan")
    return float(np.mean(x > 0.0))


def _row_stats(fwd: np.ndarray) -> Dict[str, float]:
    out: Dict[str, float] = {
        "n": int(np.count_nonzero(np.isfinite(fwd))),
        "mean_pct": _safe_mean(fwd),
        "std_pct": _safe_std(fwd),
        "median_pct": _safe_median(fwd),
        "p_pos": _p_pos(fwd),
    }
    qs = _quantiles_1d(fwd, FWD_RETURN_QUANTILES)
    for i, q in enumerate(FWD_RETURN_QUANTILES):
        out[f"q{int(q*100):02d}_pct"] = float(qs[i])
    return out


def compute_conditional_exports(segments: List[Segment]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    by_hour: Dict[int, List[np.ndarray]] = {h: [] for h in range(24)}
    for seg in segments:
        by_hour[seg.hour_utc].append(seg.values_pct)

    step_points = max(1, int(OFFSET_STEP_SECONDS // BAR_SECONDS))
    horizons_points = [int(h // BAR_SECONDS) for h in HORIZONS_SECONDS]
    if any((hp * BAR_SECONDS) != h for hp, h in zip(horizons_points, HORIZONS_SECONDS)):
        raise ValueError("All horizons must be multiples of BAR_SECONDS.")

    max_h = max(horizons_points)
    max_t = POINTS_PER_HOUR - max_h - 1
    if max_t <= 0:
        raise ValueError("Horizon too large for hour length.")

    offsets_idx = np.arange(0, max_t + 1, step_points, dtype=int)
    offsets_sec = offsets_idx * BAR_SECONDS

    detailed_rows: List[Dict[str, object]] = []

    for hour in range(24):
        segs = by_hour[hour]
        if not segs:
            continue

        mat = np.vstack(segs)  # (N, T) in %

        with np.errstate(all="ignore"):
            q_low_t = np.nanquantile(mat, EVENT_Q_LOW, axis=0)
            q_high_t = np.nanquantile(mat, EVENT_Q_HIGH, axis=0)

        for t_pos, t in enumerate(offsets_idx):
            v_t = mat[:, t]
            ql = q_low_t[t]
            qh = q_high_t[t]

            finite_t = np.isfinite(v_t)

            below_mask = finite_t & (v_t <= ql) if np.isfinite(ql) else np.zeros_like(finite_t, dtype=bool)
            above_mask = finite_t & (v_t >= qh) if np.isfinite(qh) else np.zeros_like(finite_t, dtype=bool)

            for hp, hsec in zip(horizons_points, HORIZONS_SECONDS):
                v_f = mat[:, t + hp]
                finite_f = np.isfinite(v_f)
                valid = finite_t & finite_f

                fwd = v_f - v_t

                fwd_below = np.where(below_mask & valid, fwd, np.nan)
                stats_below = _row_stats(fwd_below)
                detailed_rows.append(
                    {
                        "hour_utc": hour,
                        "offset_sec": int(offsets_sec[t_pos]),
                        "minute": float(offsets_sec[t_pos] / 60.0),
                        "event": "below_q10",
                        "horizon_sec": int(hsec),
                        "event_rate": float(np.mean(below_mask[finite_t])) if np.count_nonzero(finite_t) else float("nan"),
                        **stats_below,
                    }
                )

                fwd_above = np.where(above_mask & valid, fwd, np.nan)
                stats_above = _row_stats(fwd_above)
                detailed_rows.append(
                    {
                        "hour_utc": hour,
                        "offset_sec": int(offsets_sec[t_pos]),
                        "minute": float(offsets_sec[t_pos] / 60.0),
                        "event": "above_q90",
                        "horizon_sec": int(hsec),
                        "event_rate": float(np.mean(above_mask[finite_t])) if np.count_nonzero(finite_t) else float("nan"),
                        **stats_above,
                    }
                )

    detailed_df = pd.DataFrame(detailed_rows)

    agg_cols = ["mean_pct", "median_pct", "std_pct", "p_pos"] + [f"q{int(q*100):02d}_pct" for q in FWD_RETURN_QUANTILES]
    summary_df = (
        detailed_df.groupby(["hour_utc", "event", "horizon_sec"], as_index=False)
        .agg(
            offsets_n=("offset_sec", "nunique"),
            rows_n=("n", "size"),
            n_total=("n", "sum"),
            event_rate_mean=("event_rate", "mean"),
            **{c: (c, "mean") for c in agg_cols},
        )
        .sort_values(["hour_utc", "event", "horizon_sec"])
    )

    commission_roundtrip_points = (COMMISSION_USD_PER_SIDE * 2.0) / MNQ_DOLLARS_PER_POINT
    detailed_df["commission_roundtrip_points"] = float(commission_roundtrip_points)
    summary_df["commission_roundtrip_points"] = float(commission_roundtrip_points)

    return detailed_df, summary_df


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_all_prices()
    segments = build_segments(df)

    detailed_df, summary_df = compute_conditional_exports(segments)

    detailed_path = OUT_DIR / "conditional_detailed.csv"
    summary_path = OUT_DIR / "conditional_summary.csv"

    detailed_df.to_csv(detailed_path, index=False)
    summary_df.to_csv(summary_path, index=False)

    print(f"Done. Wrote: {detailed_path}")
    print(f"Done. Wrote: {summary_path}")


if __name__ == "__main__":
    main()