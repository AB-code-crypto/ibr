"""
Export hourly intrahour profiles (MNQ 5s) from price.db into analysis-friendly CSV.

Outputs:
1) artifacts/hourly_export/hour_summary.csv
   One row per hour-of-day (UTC) with summary distribution metrics.

2) artifacts/hourly_export/hour_profiles.csv
   One row per (hour-of-day, offset) with pointwise quantiles/median/mean/std of intrahour path.

Definitions:
- For each day and each hour-of-day, we take the 5-second close series for that hour (720 points).
- We compute the intrahour relative move in %: (close / close0 - 1) * 100.
- Missing bars remain NaN and are excluded from statistics at that offset.

How to use:
- Run inside the ibr project (so core.config is importable).
- Then upload hour_summary.csv and hour_profiles.csv here for strategy design.

Configuration: edit the constants in the CONFIG section below.
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# =========================
# CONFIG
# =========================

from core.config import PRICE_DB_PATH  # explicit dependency

INSTRUMENT_PREFIX = "MNQ"
OUT_DIR = Path("artifacts/hourly_export")

BAR_SECONDS = 5
POINTS_PER_HOUR = 3600 // BAR_SECONDS  # 720

# Pointwise quantiles for the profile curve
PROFILE_QUANTILES = (0.10, 0.25, 0.50, 0.75, 0.90)

# Summary quantiles for "end-of-hour" and excursions
SUMMARY_QUANTILES = (0.10, 0.25, 0.50, 0.75, 0.90)

# Optional time filter (UTC strings "YYYY-MM-DD HH:MM:SS"). None = use all.
START_UTC = None
END_UTC = None


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
                df["source_table"] = t
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
# STATS + EXPORT
# =========================

def _nanquantiles(mat: np.ndarray, qs: Tuple[float, ...]) -> np.ndarray:
    with np.errstate(all="ignore"):
        return np.nanquantile(mat, qs, axis=0)


def _nanquantiles_1d(x: np.ndarray, qs: Tuple[float, ...]) -> List[float]:
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


def export_hourly_stats(segments: List[Segment]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    by_hour: Dict[int, List[np.ndarray]] = {h: [] for h in range(24)}
    for seg in segments:
        by_hour[seg.hour_utc].append(seg.values_pct)

    offsets_sec = np.arange(POINTS_PER_HOUR) * BAR_SECONDS

    # ---- Profiles (pointwise) ----
    profile_rows: List[Dict[str, object]] = []

    for hour in range(24):
        segs = by_hour[hour]
        if not segs:
            continue

        mat = np.vstack(segs)  # (N, T)
        qs = _nanquantiles(mat, PROFILE_QUANTILES)  # (Q, T)
        mean = np.nanmean(mat, axis=0)
        std = np.nanstd(mat, axis=0)
        n_obs = np.sum(np.isfinite(mat), axis=0).astype(int)

        for i in range(POINTS_PER_HOUR):
            row: Dict[str, object] = {
                "hour_utc": hour,
                "offset_sec": int(offsets_sec[i]),
                "minute": float(offsets_sec[i] / 60.0),
                "n_obs": int(n_obs[i]),
                "mean_pct": float(mean[i]) if np.isfinite(mean[i]) else float("nan"),
                "std_pct": float(std[i]) if np.isfinite(std[i]) else float("nan"),
            }
            for q_idx, q in enumerate(PROFILE_QUANTILES):
                key = f"q{int(q*100):02d}_pct"
                val = qs[q_idx, i]
                row[key] = float(val) if np.isfinite(val) else float("nan")
            profile_rows.append(row)

    profiles_df = pd.DataFrame(profile_rows)
    profiles_path = OUT_DIR / "hour_profiles.csv"
    profiles_df.to_csv(profiles_path, index=False)

    # ---- Summary per hour ----
    summary_rows: List[Dict[str, object]] = []

    for hour in range(24):
        segs = by_hour[hour]
        if not segs:
            continue

        mat = np.vstack(segs)  # (N, T)
        finite = np.isfinite(mat)
        missing_pct = (np.count_nonzero(~finite) / mat.size) * 100.0 if mat.size else float("nan")

        # End-of-hour move (%)
        end_vals = mat[:, -1]
        end_q = _nanquantiles_1d(end_vals, SUMMARY_QUANTILES)
        end_mean = _safe_mean(end_vals)
        end_std = _safe_std(end_vals)
        end_p_up = float(np.mean(end_vals[np.isfinite(end_vals)] > 0.0)) if np.isfinite(end_vals).any() else float("nan")

        # Intrahour excursions (%)
        max_up = np.nanmax(mat, axis=1)      # MFE for long
        min_down = np.nanmin(mat, axis=1)    # MAE for long (negative)
        mae_long = -min_down                 # positive
        mfe_long = max_up                    # positive

        mfe_short = -min_down                # positive (down move helps short)
        mae_short = max_up                   # positive (up move hurts short)

        mae_q = _nanquantiles_1d(mae_long, SUMMARY_QUANTILES)
        mfe_q = _nanquantiles_1d(mfe_long, SUMMARY_QUANTILES)

        mfe_short_q = _nanquantiles_1d(mfe_short, SUMMARY_QUANTILES)
        mae_short_q = _nanquantiles_1d(mae_short, SUMMARY_QUANTILES)

        # Pointwise dispersion summary (%)
        point_std = np.nanstd(mat, axis=0)
        avg_point_std = float(np.nanmean(point_std)) if np.isfinite(point_std).any() else float("nan")
        max_point_std = float(np.nanmax(point_std)) if np.isfinite(point_std).any() else float("nan")

        summary: Dict[str, object] = {
            "hour_utc": hour,
            "segments_n": len(segs),
            "missing_pct": float(missing_pct),
            "end_mean_pct": float(end_mean),
            "end_std_pct": float(end_std),
            "end_p_up": float(end_p_up),
            "dispersion_std_avg_pct": float(avg_point_std),
            "dispersion_std_max_pct": float(max_point_std),
        }

        for i, q in enumerate(SUMMARY_QUANTILES):
            summary[f"end_q{int(q*100):02d}_pct"] = float(end_q[i])

        for i, q in enumerate(SUMMARY_QUANTILES):
            summary[f"mfe_long_q{int(q*100):02d}_pct"] = float(mfe_q[i])
            summary[f"mae_long_q{int(q*100):02d}_pct"] = float(mae_q[i])
            summary[f"mfe_short_q{int(q*100):02d}_pct"] = float(mfe_short_q[i])
            summary[f"mae_short_q{int(q*100):02d}_pct"] = float(mae_short_q[i])

        summary_rows.append(summary)

    summary_df = pd.DataFrame(summary_rows).sort_values("hour_utc")
    summary_path = OUT_DIR / "hour_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    print(f"Done. Wrote: {profiles_path}")
    print(f"Done. Wrote: {summary_path}")


def main() -> None:
    df = load_all_prices()
    segments = build_segments(df)
    export_hourly_stats(segments)


if __name__ == "__main__":
    main()