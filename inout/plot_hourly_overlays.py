"""
Hourly overlay diagnostics for MNQ (5s bars) from price.db.

Produces 24 pages (hours 00..23 UTC). For each hour:
- A density "heatmap" of intrahour relative moves (close/close0 - 1) in %, overlaid across all days.
  Darker = more frequent values.
- Median and quantile bands over time (computed pointwise across segments).
- A "паспорт часа" (RU) with summary stats, placed at the top with an opaque background.

Notes:
- Missing bars inside an hour are left as NaN; they do not contribute to density or quantiles.
- Tables are merged by prefix (e.g., MNQ*) and de-duplicated by timestamp (handles roll tables).

Configuration: edit the constants in the CONFIG section below.
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.colors import LogNorm


# =========================
# CONFIG
# =========================

INSTRUMENT_PREFIX = "MNQ"
OUT_DIR = Path("artifacts/hourly_overlays")

BAR_SECONDS = 5
POINTS_PER_HOUR = 3600 // BAR_SECONDS  # 720 for 5s

# Y-range is set from these quantiles of all intrahour values for the hour, then made symmetric around 0.
YLIM_QUANTILES = (0.01, 0.99)

# Heatmap resolution
Y_BINS = 180

# Heatmap intensity scaling (counts)
USE_LOG_NORM = True
LOG_NORM_VMIN = 1  # counts; 1 means single observations are visible

# Axes scaling for Y (supports negatives). Use "symlog" for better visibility of small moves.
Y_SCALE = "symlog"  # "linear" | "symlog"
SYMLOG_LINTHRESH_PCT = 0.10  # linear threshold around 0 in % units
SYMLOG_LINSCALE = 1.0

# Overlay: median + quantile band (pointwise across segments)
SHOW_QUANTILES = True
QUANTILE_BAND = (0.25, 0.75)  # q_low, q_high
SHOW_MEDIAN = True

# Text sizes
PASSPORT_FONT_SIZE = 7
TITLE_FONT_SIZE = 11
LABEL_FONT_SIZE = 9

# If you prefer the old spaghetti plot instead of heatmap:
# PLOT_MODE = "heatmap" | "spaghetti"
PLOT_MODE = "heatmap"
SPAGHETTI_ALPHA = 0.10
SPAGHETTI_LINEWIDTH = 0.7
SPAGHETTI_MAX_LINES = 400  # sampled per hour (keeps plot readable)


# =========================
# DATA LOADING
# =========================

@dataclass(frozen=True, slots=True)
class Segment:
    hour: int
    day: datetime  # midnight UTC of the day
    values: np.ndarray  # shape=(POINTS_PER_HOUR,), float with NaN gaps (fraction, not %)


def _parse_time_series(df: pd.DataFrame) -> pd.DataFrame:
    if "time_utc" not in df.columns or "close" not in df.columns:
        raise ValueError(f"Expected columns time_utc and close, got: {list(df.columns)}")

    t = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
    df = df.copy()
    df["time_utc"] = t
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["time_utc", "close"])
    return df


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
        chunks.append(_parse_time_series(chunk))
    if not chunks:
        return pd.DataFrame(columns=["time_utc", "close"])
    return pd.concat(chunks, ignore_index=True)


def _load_all_prices(db_path: Path, instrument_prefix: str) -> pd.DataFrame:
    conn = sqlite3.connect(str(db_path))
    try:
        tables = _list_tables(conn, instrument_prefix)
        if not tables:
            raise RuntimeError(f"No tables found in {db_path} with prefix {instrument_prefix!r}")

        frames: List[pd.DataFrame] = []
        for t in tables:
            df = _read_table(conn, t)
            if not df.empty:
                df["source_table"] = t
                frames.append(df)

        if not frames:
            raise RuntimeError(f"All matching tables are empty for prefix {instrument_prefix!r}")

        out = pd.concat(frames, ignore_index=True)
        out = out.sort_values("time_utc")

        # Deduplicate by timestamp (handles roll overlaps)
        out = out.drop_duplicates(subset=["time_utc"], keep="last")
        out = out[["time_utc", "close"]].reset_index(drop=True)
        return out
    finally:
        conn.close()


def _build_segments(df: pd.DataFrame) -> List[Segment]:
    if df.empty:
        return []

    df = df.copy()
    df["hour"] = df["time_utc"].dt.hour
    df["date"] = df["time_utc"].dt.floor("D")  # midnight UTC (tz-aware)

    segments: List[Segment] = []

    for (date_midnight, hour), g in df.groupby(["date", "hour"], sort=True):
        start = pd.Timestamp(date_midnight) + pd.Timedelta(hours=int(hour))
        idx = pd.date_range(start=start, periods=POINTS_PER_HOUR, freq=f"{BAR_SECONDS}s", tz="UTC")

        s = g.set_index("time_utc")["close"].reindex(idx)
        if s.isna().all():
            continue

        base = s.iloc[0]
        if pd.isna(base):
            # If first bar is missing, use first available close as base.
            base = s.dropna().iloc[0]

        rel = (s / base) - 1.0  # fraction
        segments.append(
            Segment(
                hour=int(hour),
                day=pd.Timestamp(date_midnight).to_pydatetime().replace(tzinfo=timezone.utc),
                values=rel.to_numpy(dtype=float, copy=True),
            )
        )

    return segments


# =========================
# STATS + PLOTTING
# =========================

def _compute_stats(mat: np.ndarray) -> Dict[str, float]:
    # mat: (N, T) fraction
    finite = np.isfinite(mat)
    total_points = mat.size
    missing_points = int(np.count_nonzero(~finite))
    missing_pct = (missing_points / total_points) * 100.0 if total_points else float("nan")

    point_std = np.nanstd(mat, axis=0)
    avg_point_std = float(np.nanmean(point_std)) if np.isfinite(point_std).any() else float("nan")
    max_point_std = float(np.nanmax(point_std)) if np.isfinite(point_std).any() else float("nan")
    end_point_std = float(point_std[-1]) if np.isfinite(point_std[-1]) else float("nan")

    end_vals = mat[:, -1]
    end_vals = end_vals[np.isfinite(end_vals)]
    end_mean = float(np.mean(end_vals)) if end_vals.size else float("nan")
    end_std = float(np.std(end_vals)) if end_vals.size else float("nan")
    end_med = float(np.median(end_vals)) if end_vals.size else float("nan")
    end_q25 = float(np.quantile(end_vals, 0.25)) if end_vals.size else float("nan")
    end_q75 = float(np.quantile(end_vals, 0.75)) if end_vals.size else float("nan")

    max_abs_per_seg = np.nanmax(np.abs(mat), axis=1)
    max_abs_per_seg = max_abs_per_seg[np.isfinite(max_abs_per_seg)]
    max_abs_mean = float(np.mean(max_abs_per_seg)) if max_abs_per_seg.size else float("nan")
    max_abs_med = float(np.median(max_abs_per_seg)) if max_abs_per_seg.size else float("nan")

    return {
        "missing_pct": missing_pct,
        "avg_point_std": avg_point_std,
        "max_point_std": max_point_std,
        "end_point_std": end_point_std,
        "end_mean": end_mean,
        "end_std": end_std,
        "end_med": end_med,
        "end_q25": end_q25,
        "end_q75": end_q75,
        "max_abs_mean": max_abs_mean,
        "max_abs_med": max_abs_med,
    }


def _symmetric_bound_by_quantiles(mat: np.ndarray, q_low: float, q_high: float) -> Optional[float]:
    flat = mat[np.isfinite(mat)]
    if flat.size == 0:
        return None
    lo = float(np.quantile(flat, q_low))
    hi = float(np.quantile(flat, q_high))
    bound = max(abs(lo), abs(hi))
    if not np.isfinite(bound) or bound <= 0:
        return None
    return bound


def _pointwise_quantiles(mat: np.ndarray, q_low: float, q_high: float) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    # Returns (q_low, median, q_high) as fraction arrays of shape (T,)
    with np.errstate(all="ignore"):
        ql = np.nanquantile(mat, q_low, axis=0)
        qm = np.nanquantile(mat, 0.50, axis=0)
        qh = np.nanquantile(mat, q_high, axis=0)
    return ql, qm, qh


def _apply_y_scale(ax) -> None:
    if Y_SCALE == "symlog":
        ax.set_yscale(
            "symlog",
            linthresh=float(SYMLOG_LINTHRESH_PCT),
            linscale=float(SYMLOG_LINSCALE),
        )


def _overlay_quantiles(ax, mat: np.ndarray) -> None:
    if not (SHOW_QUANTILES or SHOW_MEDIAN):
        return

    q_low, q_high = QUANTILE_BAND
    ql, qm, qh = _pointwise_quantiles(mat, q_low, q_high)

    x_minutes = (np.arange(POINTS_PER_HOUR) * BAR_SECONDS) / 60.0
    y_ql = ql * 100.0
    y_qm = qm * 100.0
    y_qh = qh * 100.0

    # Band + median. Use high-contrast lines for readability over the heatmap.
    if SHOW_QUANTILES:
        ax.fill_between(x_minutes, y_ql, y_qh, alpha=0.18)
        ax.plot(x_minutes, y_ql, linewidth=1.2, color="white", alpha=0.8)
        ax.plot(x_minutes, y_qh, linewidth=1.2, color="white", alpha=0.8)

    if SHOW_MEDIAN:
        ax.plot(x_minutes, y_qm, linewidth=2.0, color="white", alpha=0.95)


def _plot_heatmap(ax, mat: np.ndarray, hour: int, n_segments: int) -> None:
    # mat in fraction, shape (N, T)
    bound = _symmetric_bound_by_quantiles(mat, YLIM_QUANTILES[0], YLIM_QUANTILES[1])
    if bound is None:
        bound = 0.005  # 0.5% fallback

    y_min = -bound
    y_max = bound

    # Build counts per time-step (column) into y-bins
    y_edges = np.linspace(y_min, y_max, Y_BINS + 1)
    x_edges_min = np.linspace(0.0, 60.0, POINTS_PER_HOUR + 1)

    counts = np.zeros((Y_BINS, POINTS_PER_HOUR), dtype=np.int32)

    for t in range(POINTS_PER_HOUR):
        col = mat[:, t]
        col = col[np.isfinite(col)]
        if col.size == 0:
            continue
        hist, _ = np.histogram(col, bins=y_edges)
        counts[:, t] = hist.astype(np.int32)

    norm = None
    if USE_LOG_NORM:
        norm = LogNorm(vmin=LOG_NORM_VMIN, vmax=max(LOG_NORM_VMIN + 1, int(counts.max())))

    mesh = ax.pcolormesh(
        x_edges_min,
        (y_edges * 100.0),  # show % on axis
        counts,
        shading="auto",
        norm=norm,
    )

    ax.axhline(0.0, linewidth=1.0)
    ax.set_title(
        f"MNQ: плотность траекторий {hour:02d}:00–{hour:02d}:59 UTC (n={n_segments})",
        fontsize=TITLE_FONT_SIZE,
    )
    ax.set_xlabel("Минуты от начала часа", fontsize=LABEL_FONT_SIZE)
    ax.set_ylabel("Изменение от открытия часа, %", fontsize=LABEL_FONT_SIZE)
    ax.grid(True, linewidth=0.4, alpha=0.25)

    _apply_y_scale(ax)
    _overlay_quantiles(ax, mat)

    plt.colorbar(mesh, ax=ax, pad=0.01, fraction=0.045)


def _plot_spaghetti(ax, mat: np.ndarray, hour: int, n_segments: int) -> None:
    x_minutes = (np.arange(POINTS_PER_HOUR) * BAR_SECONDS) / 60.0

    if mat.shape[0] > SPAGHETTI_MAX_LINES:
        idx = np.random.choice(mat.shape[0], size=SPAGHETTI_MAX_LINES, replace=False)
        plot_mat = mat[idx]
    else:
        plot_mat = mat

    for row in plot_mat:
        ax.plot(x_minutes, row * 100.0, alpha=SPAGHETTI_ALPHA, linewidth=SPAGHETTI_LINEWIDTH)

    bound = _symmetric_bound_by_quantiles(mat, YLIM_QUANTILES[0], YLIM_QUANTILES[1])
    if bound is not None:
        ax.set_ylim((-bound * 100.0, bound * 100.0))

    ax.axhline(0.0, linewidth=1.0)
    ax.set_title(
        f"MNQ: наложение линий {hour:02d}:00–{hour:02d}:59 UTC (n={n_segments})",
        fontsize=TITLE_FONT_SIZE,
    )
    ax.set_xlabel("Минуты от начала часа", fontsize=LABEL_FONT_SIZE)
    ax.set_ylabel("Изменение от открытия часа, %", fontsize=LABEL_FONT_SIZE)
    ax.grid(True, linewidth=0.4, alpha=0.25)

    _apply_y_scale(ax)
    _overlay_quantiles(ax, mat)


def _passport_text(stats: Dict[str, float], n_segments: int) -> str:
    return (
        f"Сегментов: {n_segments}\n"
        f"Пропуски точек: {stats['missing_pct']:.1f}%\n"
        f"Разброс std(ср/макс/конец): "
        f"{stats['avg_point_std']*100:.3f}% / {stats['max_point_std']*100:.3f}% / {stats['end_point_std']*100:.3f}%\n"
        f"Конец часа среднее±std: {stats['end_mean']*100:.3f}% ± {stats['end_std']*100:.3f}%\n"
        f"Конец часа медиана [q25,q75]: {stats['end_med']*100:.3f}% "
        f"[{stats['end_q25']*100:.3f}%, {stats['end_q75']*100:.3f}%]\n"
        f"Макс |движение| ср/мед: {stats['max_abs_mean']*100:.3f}% / {stats['max_abs_med']*100:.3f}%"
    )


def _render_hour(hour: int, segments: Sequence[Segment], out_dir: Path, pdf: PdfPages) -> None:
    if not segments:
        return

    mat = np.vstack([s.values for s in segments])  # (N, T), fraction
    stats = _compute_stats(mat)

    fig = plt.figure(figsize=(12, 6))
    ax = fig.add_subplot(111)

    if PLOT_MODE == "spaghetti":
        _plot_spaghetti(ax, mat, hour, len(segments))
    else:
        _plot_heatmap(ax, mat, hour, len(segments))

    # Passport box: place at the very top, opaque background.
    ax.text(
        0.02,
        0.995,
        _passport_text(stats, len(segments)),
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=PASSPORT_FONT_SIZE,
        bbox=dict(boxstyle="round", facecolor="white", edgecolor="black", alpha=1.0, pad=0.35),
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    png_path = out_dir / f"hour_{hour:02d}.png"
    fig.savefig(png_path, dpi=160, bbox_inches="tight")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    try:
        from core.config import PRICE_DB_PATH as DEFAULT_DB
    except Exception as e:
        raise RuntimeError("Could not import core.config.PRICE_DB_PATH; set DB path manually.") from e

    db_path = Path(DEFAULT_DB)
    out_dir = OUT_DIR

    df = _load_all_prices(db_path=db_path, instrument_prefix=INSTRUMENT_PREFIX)
    segments = _build_segments(df)

    by_hour: Dict[int, List[Segment]] = {h: [] for h in range(24)}
    for seg in segments:
        by_hour[seg.hour].append(seg)

    out_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = out_dir / "hourly_overlays.pdf"

    with PdfPages(pdf_path) as pdf:
        for h in range(24):
            _render_hour(h, by_hour[h], out_dir, pdf)

    print(f"Done. Wrote PNGs to: {out_dir}")
    print(f"PDF: {pdf_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())