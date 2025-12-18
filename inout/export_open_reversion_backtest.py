"""
Simulate an "open reversion" hourly strategy on MNQ 5-second bars and export a trade tape.

Strategy (as requested)
-----------------------
At the start of each UTC hour:
  - Define OPEN = first available close in the hour (5s series).
  - Place two ENTRY limit orders (OCO):
      1) BUY limit at OPEN - D_down
      2) SELL limit at OPEN + D_up
    The first order that gets filled cancels the other.
  - After fill, place TAKE PROFIT back toward OPEN:
      - LONG: sell at OPEN - TP_OFFSET_TICKS * TICK_SIZE
      - SHORT: buy at OPEN + TP_OFFSET_TICKS * TICK_SIZE
    (TP_OFFSET_TICKS=0 means exact OPEN).
  - No stop.
  - If TP not hit by end of hour, close at the last available close (time-exit).

Key idea
--------
You must choose entry distances D_down/D_up. This script derives them per hour-of-day
from historical distributions of intrahour extremes (min/max from OPEN) using quantiles.

Outputs
-------
artifacts/open_reversion_backtest/trades.csv
  One row per trade.

artifacts/open_reversion_backtest/summary.csv
  Aggregated performance per (hour_utc, quantile_level, direction).

artifacts/open_reversion_backtest/params_by_hour.csv
  The actual entry distances used for each hour and each quantile level.

No CLI args. Edit CONFIG below.
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

OUT_DIR = Path("artifacts/open_reversion_backtest")

BAR_SECONDS = 5
POINTS_PER_HOUR = 3600 // BAR_SECONDS  # 720

# Tick size for MNQ
TICK_SIZE = 0.25

# Entry distances are derived per-hour from distributions of extremes using these quantiles.
ENTRY_QUANTILES = (0.75, 0.90, 0.95)

# If True: use symmetric distance for both sides = max(D_down, D_up)
USE_SYMMETRIC_DISTANCE = False

# TP at OPEN +/- offset ticks (inside OPEN improves fill probability)
TP_OFFSET_TICKS = 1

# Costs (commission always applies; slippage is optional)
COMMISSION_USD_PER_SIDE = 0.62
MNQ_DOLLARS_PER_POINT = 2.0

# Slippage model (in ticks):
# - For limit fills we usually assume 0.
# - For time-exit (market close) you may want to model 1 tick.
SLIPPAGE_TICKS_ENTRY = 0
SLIPPAGE_TICKS_TP = 0
SLIPPAGE_TICKS_TIME_EXIT = 1

# Optional time filter (UTC strings). None = use all.
START_UTC = None
END_UTC = None


# =========================
# DATA MODEL
# =========================

@dataclass(frozen=True, slots=True)
class Segment:
    start_utc: datetime          # hour start (UTC)
    hour_utc: int
    close: np.ndarray            # (POINTS_PER_HOUR,) float with NaN gaps
    open_price: float            # first available close


# =========================
# HELPERS
# =========================

def _round_to_tick(price: float) -> float:
    return float(np.round(price / TICK_SIZE) * TICK_SIZE)


def _ceil_to_tick(price: float) -> float:
    return float(np.ceil(price / TICK_SIZE) * TICK_SIZE)


def _floor_to_tick(price: float) -> float:
    return float(np.floor(price / TICK_SIZE) * TICK_SIZE)


def _commission_roundtrip_points() -> float:
    return float((COMMISSION_USD_PER_SIDE * 2.0) / MNQ_DOLLARS_PER_POINT)


def _slippage_roundtrip_points(exit_reason: str) -> float:
    """
    exit_reason: "tp" or "time"
    """
    entry = SLIPPAGE_TICKS_ENTRY * TICK_SIZE
    if exit_reason == "tp":
        exit_ = SLIPPAGE_TICKS_TP * TICK_SIZE
    else:
        exit_ = SLIPPAGE_TICKS_TIME_EXIT * TICK_SIZE
    return float(entry + exit_)


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

        start_dt = pd.Timestamp(start).to_pydatetime().replace(tzinfo=timezone.utc)

        segments.append(
            Segment(
                start_utc=start_dt,
                hour_utc=int(hour),
                close=s.to_numpy(dtype=float, copy=True),
                open_price=float(open_price),
            )
        )

    return segments


# =========================
# DISTANCES BY HOUR
# =========================

def _extremes_pct(seg: Segment) -> Tuple[float, float]:
    """
    Returns (min_pct, max_pct) relative to OPEN, in %.
    min_pct <= 0, max_pct >= 0
    """
    r = (seg.close / seg.open_price - 1.0) * 100.0
    r = r[np.isfinite(r)]
    if r.size == 0:
        return float("nan"), float("nan")
    return float(np.min(r)), float(np.max(r))


def compute_entry_distances_by_hour(
    segments: List[Segment],
    quantiles: Tuple[float, ...],
) -> pd.DataFrame:
    """
    Build per-hour distributions of extremes:
      down_mag_pct = -min_pct
      up_mag_pct   = +max_pct

    Then compute quantiles for both sides.
    """
    rows: List[Dict[str, object]] = []

    by_hour_down: Dict[int, List[float]] = {h: [] for h in range(24)}
    by_hour_up: Dict[int, List[float]] = {h: [] for h in range(24)}

    for seg in segments:
        min_pct, max_pct = _extremes_pct(seg)
        if not (np.isfinite(min_pct) and np.isfinite(max_pct)):
            continue
        by_hour_down[seg.hour_utc].append(-min_pct)  # magnitude in %
        by_hour_up[seg.hour_utc].append(max_pct)

    for h in range(24):
        downs = np.array(by_hour_down[h], dtype=float)
        ups = np.array(by_hour_up[h], dtype=float)
        if downs.size == 0 or ups.size == 0:
            continue

        for q in quantiles:
            d_down = float(np.quantile(downs, q))
            d_up = float(np.quantile(ups, q))

            if USE_SYMMETRIC_DISTANCE:
                d = max(d_down, d_up)
                d_down = d
                d_up = d

            rows.append(
                {
                    "hour_utc": h,
                    "quantile": float(q),
                    "d_down_pct": d_down,
                    "d_up_pct": d_up,
                    "segments_n": int(downs.size),
                }
            )

    return pd.DataFrame(rows)


# =========================
# BACKTEST
# =========================

def _first_hit_index(close: np.ndarray, long_entry: float, short_entry: float) -> Tuple[Optional[int], Optional[str]]:
    """
    Returns (idx, direction) where direction is "LONG" or "SHORT".
    LONG triggers when close <= long_entry.
    SHORT triggers when close >= short_entry.
    """
    for i, px in enumerate(close):
        if not np.isfinite(px):
            continue
        if px <= long_entry:
            return i, "LONG"
        if px >= short_entry:
            return i, "SHORT"
    return None, None


def _tp_hit_index(close: np.ndarray, start_idx: int, tp_price: float, direction: str) -> Optional[int]:
    """
    LONG TP hits when close >= tp_price.
    SHORT TP hits when close <= tp_price.
    """
    for i in range(start_idx + 1, len(close)):
        px = close[i]
        if not np.isfinite(px):
            continue
        if direction == "LONG":
            if px >= tp_price:
                return i
        else:
            if px <= tp_price:
                return i
    return None


def _last_valid_price(close: np.ndarray) -> Optional[float]:
    valid = close[np.isfinite(close)]
    if valid.size == 0:
        return None
    return float(valid[-1])


def run_backtest(
    segments: List[Segment],
    params_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    params_df columns:
      hour_utc, quantile, d_down_pct, d_up_pct
    """
    params_map: Dict[Tuple[int, float], Tuple[float, float]] = {}
    for r in params_df.itertuples(index=False):
        params_map[(int(r.hour_utc), float(r.quantile))] = (float(r.d_down_pct), float(r.d_up_pct))

    commission_points = _commission_roundtrip_points()

    trades: List[Dict[str, object]] = []

    for seg in segments:
        open_px = _round_to_tick(seg.open_price)
        end_px = _last_valid_price(seg.close)
        if end_px is None:
            continue
        end_px = _round_to_tick(end_px)

        for q in ENTRY_QUANTILES:
            key = (seg.hour_utc, float(q))
            if key not in params_map:
                continue

            d_down_pct, d_up_pct = params_map[key]

            d_down_points = open_px * (d_down_pct / 100.0)
            d_up_points = open_px * (d_up_pct / 100.0)

            # Convert to tick-aligned entry prices.
            long_entry = _floor_to_tick(open_px - d_down_points)   # buy lower
            short_entry = _ceil_to_tick(open_px + d_up_points)     # sell higher

            # OCO: first fill wins
            entry_idx, direction = _first_hit_index(seg.close, long_entry=long_entry, short_entry=short_entry)
            if entry_idx is None:
                continue

            entry_px = long_entry if direction == "LONG" else short_entry

            # TP: slightly inside OPEN for higher fill probability
            if direction == "LONG":
                tp_px = _floor_to_tick(open_px - TP_OFFSET_TICKS * TICK_SIZE)
            else:
                tp_px = _ceil_to_tick(open_px + TP_OFFSET_TICKS * TICK_SIZE)

            tp_idx = _tp_hit_index(seg.close, start_idx=entry_idx, tp_price=tp_px, direction=direction)

            if tp_idx is not None:
                exit_reason = "tp"
                exit_idx = tp_idx
                exit_px = tp_px
            else:
                exit_reason = "time"
                exit_idx = POINTS_PER_HOUR - 1
                exit_px = end_px

            # Slippage in points (model)
            slippage_points = _slippage_roundtrip_points(exit_reason=exit_reason)

            if direction == "LONG":
                pnl_gross_points = float(exit_px - entry_px)
            else:
                pnl_gross_points = float(entry_px - exit_px)

            pnl_net_points = pnl_gross_points - commission_points - slippage_points

            # MAE/MFE from entry to exit (in points)
            window = seg.close[entry_idx : exit_idx + 1]
            window = window[np.isfinite(window)]
            if window.size:
                min_p = float(np.min(window))
                max_p = float(np.max(window))
                if direction == "LONG":
                    mae = entry_px - min_p
                    mfe = max_p - entry_px
                else:
                    mae = max_p - entry_px
                    mfe = entry_px - min_p
            else:
                mae = float("nan")
                mfe = float("nan")

            trades.append(
                {
                    "date_utc": seg.start_utc.date().isoformat(),
                    "hour_utc": int(seg.hour_utc),
                    "hour_msk": int((seg.hour_utc + 3) % 24),
                    "quantile": float(q),
                    "direction": direction,
                    "open_price": float(open_px),
                    "entry_offset_sec": int(entry_idx * BAR_SECONDS),
                    "entry_price": float(entry_px),
                    "tp_price": float(tp_px),
                    "exit_reason": exit_reason,
                    "exit_offset_sec": int((exit_idx) * BAR_SECONDS),
                    "exit_price": float(exit_px),
                    "pnl_points_gross": float(pnl_gross_points),
                    "pnl_points_net": float(pnl_net_points),
                    "commission_points": float(commission_points),
                    "slippage_points": float(slippage_points),
                    "mae_points": float(mae),
                    "mfe_points": float(mfe),
                    "d_down_pct": float(d_down_pct),
                    "d_up_pct": float(d_up_pct),
                }
            )

    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        return trades_df, pd.DataFrame()

    def _profit_factor(x: pd.Series) -> float:
        pos = x[x > 0].sum()
        neg = -x[x < 0].sum()
        if neg <= 0:
            return float("inf") if pos > 0 else float("nan")
        return float(pos / neg)

    summary_df = (
        trades_df.groupby(["hour_utc", "hour_msk", "quantile", "direction"], as_index=False)
        .agg(
            trades_n=("pnl_points_net", "size"),
            win_rate=("pnl_points_net", lambda s: float((s > 0).mean())),
            tp_hit_rate=("exit_reason", lambda s: float((s == "tp").mean())),
            pnl_points_mean=("pnl_points_net", "mean"),
            pnl_points_median=("pnl_points_net", "median"),
            pnl_points_sum=("pnl_points_net", "sum"),
            mae_points_mean=("mae_points", "mean"),
            mae_points_q90=("mae_points", lambda s: float(np.quantile(s.dropna().to_numpy(), 0.90)) if s.dropna().size else float("nan")),
            mfe_points_mean=("mfe_points", "mean"),
            profit_factor=("pnl_points_net", _profit_factor),
            avg_entry_min=("entry_offset_sec", lambda s: float(np.mean(s) / 60.0)),
        )
        .sort_values(["pnl_points_sum"], ascending=False)
    )

    return trades_df, summary_df


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_all_prices()
    segments = build_segments(df)

    params_df = compute_entry_distances_by_hour(segments, quantiles=ENTRY_QUANTILES)

    trades_df, summary_df = run_backtest(segments, params_df)

    (OUT_DIR / "params_by_hour.csv").write_text(params_df.to_csv(index=False), encoding="utf-8")
    (OUT_DIR / "trades.csv").write_text(trades_df.to_csv(index=False), encoding="utf-8")
    (OUT_DIR / "summary.csv").write_text(summary_df.to_csv(index=False), encoding="utf-8")

    print(f"Done. Segments: {len(segments)}")
    print(f"Trades: {len(trades_df)}")
    print(f"Wrote: {OUT_DIR / 'params_by_hour.csv'}")
    print(f"Wrote: {OUT_DIR / 'trades.csv'}")
    print(f"Wrote: {OUT_DIR / 'summary.csv'}")


if __name__ == "__main__":
    main()