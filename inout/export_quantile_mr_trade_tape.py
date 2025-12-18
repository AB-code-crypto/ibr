"""
Export a trade tape for a quantile mean-reversion (time-of-day) strategy on MNQ 5s bars.

Why this export
---------------
The existing exports:
- hour_profiles.csv    (pointwise q10/q50/q90 bands per hour)
- conditional_summary  (average forward returns given "below q10" / "above q90")

are good for intuition, but not sufficient to evaluate an actual strategy because a strategy needs:
- a concrete entry rule (touch vs cross, first signal vs multiple)
- a concrete exit rule (time exit vs median reversion exit, etc.)
- realized PnL in POINTS / USD, net of commission (+ optional slippage)
- MAE/MFE to size stops/targets and understand tail risk
- a train/test split to reduce look-ahead bias

This script produces a trade-level dataset ("trade tape") and a summary table.

Outputs
-------
artifacts/hourly_trade_tape/trades.csv
  One row per simulated trade.

artifacts/hourly_trade_tape/summary.csv
  Aggregated performance by (hour_utc, direction, horizon_sec, exit_mode).

Notes
-----
- We use CLOSE prices (no bid/ask). Add slippage if you want.
- Missing bars are NOT interpolated. If entry/exit prices are missing, the trade is skipped.
- Quantile bands are estimated on a TRAIN subset if TRAIN_END_UTC is set; otherwise on all data.

No CLI args by design. Edit CONFIG below.
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# =========================
# CONFIG
# =========================

from core.config import PRICE_DB_PATH  # explicit dependency

INSTRUMENT_PREFIX = "MNQ"

OUT_DIR = Path("artifacts/hourly_trade_tape")

BAR_SECONDS = 5
POINTS_PER_HOUR = 3600 // BAR_SECONDS  # 720

# Quantiles for bands (entry triggers + median target)
Q_LOW = 0.10
Q_MID = 0.50
Q_HIGH = 0.90

# Strategy style: mean-reversion
# below q10 -> LONG, above q90 -> SHORT

# Entry rule
MIN_ENTRY_OFFSET_SEC = 60          # ignore the first minute of each hour
ENTRY_CROSS_ONLY = True            # True = require crossing the band; False = touch is enough
MAX_ONE_TRADE_PER_HOUR = True      # True = take only the first signal in a given hour (either side)

# Exit rule
HORIZONS_SECONDS = (300, 900, 1800)  # evaluate multiple fixed holds
EXIT_ON_MEDIAN_REVERSION = True      # if True, exit early on reaching q50 (median band)

# Costs
COMMISSION_USD_PER_SIDE = 0.62
MNQ_DOLLARS_PER_POINT = 2.0

# Optional slippage in POINTS per side (1 tick = 0.25). Set 0 if you don't want it.
SLIPPAGE_POINTS_PER_SIDE = 0.25

# Train/test split for band estimation (UTC string "YYYY-MM-DD HH:MM:SS")
# - If TRAIN_END_UTC is None, bands are computed on all data (in-sample, faster).
# - If set, bands are computed only on segments strictly before TRAIN_END_UTC.
#   Trades are still exported for ALL data with a "set" column: train/test.
TRAIN_END_UTC = None  # e.g. "2025-01-01 00:00:00"

# Optional time filter for data loading (UTC strings). None = all.
START_UTC = None
END_UTC = None


# =========================
# DATA MODEL
# =========================

@dataclass(frozen=True, slots=True)
class Segment:
    date_midnight_utc: datetime     # midnight UTC (python datetime)
    hour_utc: int
    close: np.ndarray               # (POINTS_PER_HOUR,) float, NaN gaps
    open_price: float               # first available close within hour


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
# SEGMENTS
# =========================

def build_segments(df: pd.DataFrame) -> List[Segment]:
    if df.empty:
        return []

    df = df.copy()
    df["hour_utc"] = df["time_utc"].dt.hour
    df["date_utc"] = df["time_utc"].dt.floor("D")  # midnight UTC

    segments: List[Segment] = []

    for (date_midnight, hour), g in df.groupby(["date_utc", "hour_utc"], sort=True):
        start = pd.Timestamp(date_midnight) + pd.Timedelta(hours=int(hour))
        idx = pd.date_range(start=start, periods=POINTS_PER_HOUR, freq=f"{BAR_SECONDS}s", tz="UTC")

        s = g.set_index("time_utc")["close"].reindex(idx)
        if s.isna().all():
            continue

        open_price = s.iloc[0]
        if pd.isna(open_price):
            open_price = s.dropna().iloc[0]

        # date_midnight here is pandas Timestamp; normalize to python datetime (tz-aware)
        date_midnight_dt = pd.Timestamp(date_midnight).to_pydatetime().replace(tzinfo=timezone.utc)

        segments.append(
            Segment(
                date_midnight_utc=date_midnight_dt,
                hour_utc=int(hour),
                close=s.to_numpy(dtype=float, copy=True),
                open_price=float(open_price),
            )
        )

    return segments


def _segment_start_utc(seg: Segment) -> datetime:
    # seg.date_midnight_utc is python datetime => use python timedelta (no to_pydatetime)
    return seg.date_midnight_utc.replace(tzinfo=timezone.utc) + timedelta(hours=seg.hour_utc)


def _segment_set(seg: Segment) -> str:
    if TRAIN_END_UTC is None:
        return "all"
    cut = pd.Timestamp(TRAIN_END_UTC, tz="UTC").to_pydatetime()
    return "train" if _segment_start_utc(seg) < cut else "test"


# =========================
# BANDS
# =========================

def compute_bands(segments: List[Segment]) -> Dict[int, Dict[str, np.ndarray]]:
    """
    Returns per-hour arrays:
      bands[hour]["q10"], bands[hour]["q50"], bands[hour]["q90"]  each shape (T,) in % from hour open.
    """
    by_hour: Dict[int, List[np.ndarray]] = {h: [] for h in range(24)}

    for seg in segments:
        if TRAIN_END_UTC is not None and _segment_set(seg) != "train":
            continue
        r = (seg.close / seg.open_price - 1.0) * 100.0  # % from hour open
        by_hour[seg.hour_utc].append(r)

    bands: Dict[int, Dict[str, np.ndarray]] = {}
    for h in range(24):
        arrs = by_hour[h]
        if not arrs:
            continue
        mat = np.vstack(arrs)  # (N,T)
        with np.errstate(all="ignore"):
            q10 = np.nanquantile(mat, Q_LOW, axis=0)
            q50 = np.nanquantile(mat, Q_MID, axis=0)
            q90 = np.nanquantile(mat, Q_HIGH, axis=0)
        bands[h] = {"q10": q10, "q50": q50, "q90": q90}

    return bands


# =========================
# TRADE SIM
# =========================

def _find_cross(
    r: np.ndarray,
    band: np.ndarray,
    direction: str,
    min_idx: int,
    max_idx: int,
) -> Optional[int]:
    """
    direction:
      - "below" => crossing down through band
      - "above" => crossing up through band
    Returns entry index (t) or None.
    """
    start = max(min_idx, 1)
    end = max_idx

    for t in range(start, end + 1):
        prev = r[t - 1]
        curr = r[t]
        b_prev = band[t - 1]
        b_curr = band[t]

        if not (np.isfinite(prev) and np.isfinite(curr) and np.isfinite(b_prev) and np.isfinite(b_curr)):
            continue

        if ENTRY_CROSS_ONLY:
            if direction == "below":
                if prev > b_prev and curr <= b_curr:
                    return t
            else:
                if prev < b_prev and curr >= b_curr:
                    return t
        else:
            if direction == "below":
                if curr <= b_curr:
                    return t
            else:
                if curr >= b_curr:
                    return t

    return None


def _exit_idx(
    r: np.ndarray,
    q50: np.ndarray,
    entry_idx: int,
    horizon_idx: int,
    side: str,
) -> Tuple[int, str]:
    """
    side: "LONG" or "SHORT"
    Returns (exit_idx, exit_reason).
    """
    time_exit = entry_idx + horizon_idx
    time_exit = min(time_exit, len(r) - 1)

    if not EXIT_ON_MEDIAN_REVERSION:
        return time_exit, "time"

    if side == "LONG":
        for t in range(entry_idx + 1, time_exit + 1):
            if np.isfinite(r[t]) and np.isfinite(q50[t]) and r[t] >= q50[t]:
                return t, "q50"
    else:
        for t in range(entry_idx + 1, time_exit + 1):
            if np.isfinite(r[t]) and np.isfinite(q50[t]) and r[t] <= q50[t]:
                return t, "q50"

    return time_exit, "time"


def _mae_mfe_points(close: np.ndarray, entry_idx: int, exit_idx: int, entry_price: float, side: str) -> Tuple[float, float]:
    window = close[entry_idx : exit_idx + 1]
    window = window[np.isfinite(window)]
    if window.size == 0:
        return float("nan"), float("nan")

    min_p = float(np.min(window))
    max_p = float(np.max(window))

    if side == "LONG":
        mae = entry_price - min_p
        mfe = max_p - entry_price
    else:
        mae = max_p - entry_price
        mfe = entry_price - min_p

    return float(mae), float(mfe)


def export_trades(segments: List[Segment], bands: Dict[int, Dict[str, np.ndarray]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    commission_roundtrip_points = (COMMISSION_USD_PER_SIDE * 2.0) / MNQ_DOLLARS_PER_POINT
    slippage_roundtrip_points = SLIPPAGE_POINTS_PER_SIDE * 2.0
    cost_points = commission_roundtrip_points + slippage_roundtrip_points

    trades: List[Dict[str, object]] = []

    min_idx = int(MIN_ENTRY_OFFSET_SEC // BAR_SECONDS)

    for seg in segments:
        h = seg.hour_utc
        if h not in bands:
            continue

        r = (seg.close / seg.open_price - 1.0) * 100.0
        q10 = bands[h]["q10"]
        q50 = bands[h]["q50"]
        q90 = bands[h]["q90"]

        seg_start = _segment_start_utc(seg)
        day_str = seg_start.date().isoformat()

        for horizon_sec in HORIZONS_SECONDS:
            horizon_idx = int(horizon_sec // BAR_SECONDS)
            if horizon_idx <= 0:
                continue

            max_entry_idx = (POINTS_PER_HOUR - 1) - horizon_idx
            if max_entry_idx <= min_idx:
                continue

            candidates: List[Tuple[int, str, str]] = []  # (entry_idx, side, event)

            entry_below = _find_cross(r, q10, direction="below", min_idx=min_idx, max_idx=max_entry_idx)
            if entry_below is not None:
                candidates.append((entry_below, "LONG", "below_q10"))

            entry_above = _find_cross(r, q90, direction="above", min_idx=min_idx, max_idx=max_entry_idx)
            if entry_above is not None:
                candidates.append((entry_above, "SHORT", "above_q90"))

            if not candidates:
                continue

            if MAX_ONE_TRADE_PER_HOUR:
                candidates.sort(key=lambda x: x[0])
                chosen = [candidates[0]]
            else:
                chosen = sorted(candidates, key=lambda x: x[0])

            for entry_idx, side, event in chosen:
                entry_price = seg.close[entry_idx]
                if not np.isfinite(entry_price):
                    continue

                exit_idx, exit_reason = _exit_idx(r, q50, entry_idx=entry_idx, horizon_idx=horizon_idx, side=side)
                exit_price = seg.close[exit_idx]
                if not np.isfinite(exit_price):
                    continue

                if side == "LONG":
                    pnl_points_gross = float(exit_price - entry_price)
                else:
                    pnl_points_gross = float(entry_price - exit_price)

                pnl_points_net = pnl_points_gross - cost_points
                pnl_usd_net = pnl_points_net * MNQ_DOLLARS_PER_POINT

                mae_points, mfe_points = _mae_mfe_points(seg.close, entry_idx, exit_idx, float(entry_price), side=side)

                trades.append(
                    {
                        "set": _segment_set(seg),
                        "date_utc": day_str,
                        "hour_utc": h,
                        "hour_msk": (h + 3) % 24,
                        "event": event,
                        "direction": side,
                        "horizon_sec": int(horizon_sec),
                        "entry_offset_sec": int(entry_idx * BAR_SECONDS),
                        "exit_offset_sec": int(exit_idx * BAR_SECONDS),
                        "exit_reason": exit_reason,
                        "open_price": float(seg.open_price),
                        "entry_price": float(entry_price),
                        "exit_price": float(exit_price),
                        "pnl_points_gross": float(pnl_points_gross),
                        "pnl_points_net": float(pnl_points_net),
                        "pnl_usd_net": float(pnl_usd_net),
                        "cost_points": float(cost_points),
                        "commission_roundtrip_points": float(commission_roundtrip_points),
                        "slippage_roundtrip_points": float(slippage_roundtrip_points),
                        "mae_points": float(mae_points),
                        "mfe_points": float(mfe_points),
                    }
                )

                if MAX_ONE_TRADE_PER_HOUR:
                    break

    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        return trades_df, pd.DataFrame()

    def _profit_factor(x: pd.Series) -> float:
        pos = x[x > 0].sum()
        neg = -x[x < 0].sum()
        if neg <= 0:
            return float("inf") if pos > 0 else float("nan")
        return float(pos / neg)

    grp_cols = ["set", "hour_utc", "hour_msk", "direction", "event", "horizon_sec", "exit_reason"]
    summary_df = (
        trades_df.groupby(grp_cols, as_index=False)
        .agg(
            trades_n=("pnl_points_net", "size"),
            win_rate=("pnl_points_net", lambda s: float((s > 0).mean())),
            pnl_points_mean=("pnl_points_net", "mean"),
            pnl_points_median=("pnl_points_net", "median"),
            pnl_points_sum=("pnl_points_net", "sum"),
            pnl_usd_sum=("pnl_usd_net", "sum"),
            mae_points_mean=("mae_points", "mean"),
            mfe_points_mean=("mfe_points", "mean"),
            profit_factor=("pnl_points_net", _profit_factor),
            avg_entry_min=("entry_offset_sec", lambda s: float(np.mean(s) / 60.0)),
        )
        .sort_values(["set", "pnl_points_sum"], ascending=[True, False])
    )

    return trades_df, summary_df


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_all_prices()
    segments = build_segments(df)

    if TRAIN_END_UTC is not None:
        train_segments = [s for s in segments if _segment_set(s) == "train"]
        if not train_segments:
            raise RuntimeError(
                "TRAIN_END_UTC is set, but train segment set is empty. "
                "Adjust START_UTC/END_UTC or TRAIN_END_UTC."
            )

    bands = compute_bands(segments)

    trades_df, summary_df = export_trades(segments, bands)

    trades_path = OUT_DIR / "trades.csv"
    summary_path = OUT_DIR / "summary.csv"

    trades_df.to_csv(trades_path, index=False)
    summary_df.to_csv(summary_path, index=False)

    print(f"Done. Wrote: {trades_path}")
    print(f"Done. Wrote: {summary_path}")
    print(f"Trades: {len(trades_df)}")


if __name__ == "__main__":
    main()