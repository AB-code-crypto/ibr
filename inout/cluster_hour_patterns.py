"""
Утилита оффлайн-кластеризации часовых паттернов.

Шаги:
1. Читает hour_patterns из patterns.db.
2. Для каждого (instrument, session_block):
   - строит фиксированной длины векторы формы часа
     (кумулятивные лог-доходности, нормализованные, с параметром bars_per_segment).
   - запускает k-means (K кластеров, K по умолчанию = 20, но не больше числа часов).
   - создаёт записи в hour_clusters с центрами кластеров.
   - проставляет cluster_id для hour_patterns.
   - считает stats_json (статистика продолжения до конца часа по фиксированным слотам).

Слоты:
- 30, 35, 40, 45, 50, 55 минут от начала часа (в секундах: 1800, 2100, 2400, 2700, 3000, 3300).
"""

import json
import logging
import math
import sqlite3
from array import array
from typing import Dict, List, Tuple

import numpy as np

from core.config import PATTERNS_DB_PATH
from core.patterns_db import init_patterns_db, get_connection as get_patterns_conn

logger = logging.getLogger(__name__)

# Слоты (секунды от начала часа), где считаем статистику продолжения
SLOT_SECONDS = [1800, 2100, 2400, 2700, 3000, 3300]

# Параметры по умолчанию
DEFAULT_BARS_PER_SEGMENT = 1
DEFAULT_K = 20


def _load_instruments(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute("SELECT DISTINCT instrument FROM hour_patterns ORDER BY instrument;")
    return [row[0] for row in cur.fetchall()]


def _load_block_rows(
    conn: sqlite3.Connection, instrument: str, session_block: int
) -> List[Tuple[int, bytes, int, str]]:
    """
    Забрать все строки по (instrument, session_block).

    Возвращаем список кортежей:
        (id, returns_blob, bars_count, hour_start_utc)
    """
    cur = conn.execute(
        """
        SELECT id, returns_blob, bars_count, hour_start_utc
        FROM hour_patterns
        WHERE instrument = ? AND session_block = ?
        ORDER BY hour_start_utc ASC;
        """,
        (instrument, session_block),
    )
    return [(int(r[0]), bytes(r[1]), int(r[2]), str(r[3])) for r in cur.fetchall()]


def _build_vectors_for_block(
    rows: List[Tuple[int, bytes, int, str]],
    bars_per_segment: int,
) -> Tuple[np.ndarray, List[int]]:
    """
    Построить матрицу X (N x M) для k-means по конкретному блоку.

    Для каждого часа:
      - декодируем returns_blob (float32),
      - считаем кумулятивную сумму R,
      - разбиваем на сегменты по bars_per_segment,
      - берём значения R в конце каждого сегмента,
      - нормализуем (вычесть mean и поделить на std, если std > 0).

    Все часы приводим к одной длине M = min_segments по блоку
    (лишний хвост отбрасываем у более "длинных" часов).
    """
    if not rows:
        return np.empty((0, 0), dtype=np.float32), []

    all_returns = []
    ids: List[int] = []

    segments_counts: List[int] = []
    for row_id, blob, bars_count, _ in rows:
        # Декодируем float32 из BLOB
        returns = np.frombuffer(blob, dtype=np.float32)
        if returns.size == 0:
            continue
        segments_count = returns.size // bars_per_segment
        if segments_count <= 0:
            continue
        segments_counts.append(segments_count)
        all_returns.append(returns)
        ids.append(row_id)

    if not all_returns:
        return np.empty((0, 0), dtype=np.float32), []

    min_segments = min(segments_counts)
    if min_segments <= 0:
        return np.empty((0, 0), dtype=np.float32), []

    N = len(all_returns)
    M = min_segments
    X = np.empty((N, M), dtype=np.float32)

    for idx, returns in enumerate(all_returns):
        # Кумулятивные лог-доходности (форма часа)
        R = np.cumsum(returns)
        # Берём значения в конце каждого сегмента
        # Сегмент s: индекс = (s+1)*bars_per_segment - 1
        indices = (np.arange(M, dtype=np.int64) + 1) * bars_per_segment - 1
        indices = np.clip(indices, 0, R.size - 1)
        vec = R[indices]

        # Нормализация: zero-mean / unit-std
        mean = float(vec.mean())
        std = float(vec.std())
        if std > 0:
            vec = (vec - mean) / std
        else:
            vec = vec - mean

        X[idx, :] = vec

    return X, ids


def _kmeans(
    X: np.ndarray,
    k: int,
    max_iter: int = 100,
    random_state: int = 42,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Простейшая реализация k-means (по Евклидовой метрике), на нормализованных векторах.

    На вход:
        X: (N x M) — матрица наблюдений,
        k: число кластеров.

    Возвращает:
        centers: (k x M) — центры кластеров,
        labels: (N,)       — номер кластера для каждого наблюдения.
    """
    N, M = X.shape
    if N == 0 or M == 0:
        raise ValueError("Empty data for k-means")

    k = min(k, N)
    rng = np.random.default_rng(random_state)

    # Инициализация центров случайными точками из X
    indices = rng.choice(N, size=k, replace=False)
    centers = X[indices].copy()

    labels = np.zeros(N, dtype=np.int64)

    for it in range(max_iter):
        # Матрица расстояний: N x k
        # (X - centers)^2 по всем кластерам
        # В лоб: broadcasting
        diff = X[:, None, :] - centers[None, :, :]
        dist2 = np.einsum("ijk,ijk->ij", diff, diff)
        new_labels = dist2.argmin(axis=1)

        if np.array_equal(new_labels, labels) and it > 0:
            break

        labels = new_labels

        # Пересчёт центров
        new_centers = np.empty_like(centers)
        for j in range(k):
            mask = labels == j
            if not np.any(mask):
                # Пустой кластер — реинициализируем случайной точкой
                new_centers[j] = X[rng.integers(0, N)]
            else:
                new_centers[j] = X[mask].mean(axis=0)

        centers = new_centers

    return centers, labels


def _reset_clusters_for_block(
    conn: sqlite3.Connection,
    instrument: str,
    session_block: int,
) -> None:
    """
    Удалить старые кластеры и сбросить cluster_id в hour_patterns для блока.
    """
    conn.execute(
        """
        UPDATE hour_patterns
        SET cluster_id = NULL
        WHERE instrument = ? AND session_block = ?;
        """,
        (instrument, session_block),
    )
    conn.execute(
        """
        DELETE FROM hour_clusters
        WHERE instrument = ? AND session_block = ?;
        """,
        (instrument, session_block),
    )
    conn.commit()


def _insert_clusters_and_update_patterns(
    conn: sqlite3.Connection,
    instrument: str,
    session_block: int,
    centers: np.ndarray,
    labels: np.ndarray,
    pattern_ids: List[int],
    bars_per_segment: int,
) -> List[int]:
    """
    Создать записи в hour_clusters и проставить cluster_id в hour_patterns.

    Возвращает список cluster_db_id длиной K (по cluster_index).
    """
    cur = conn.cursor()

    # Создаём записи в hour_clusters
    cluster_db_ids: List[int] = []
    K = centers.shape[0]
    for cluster_index in range(K):
        center_blob = centers[cluster_index].astype(np.float32).tobytes()
        # Пока stats_json пустой, заполним позже
        stats_json = json.dumps({"slots": {}})
        cur.execute(
            """
            INSERT INTO hour_clusters (
                instrument, session_block, cluster_index,
                bars_per_segment, center_blob, stats_json
            )
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                instrument,
                session_block,
                cluster_index,
                bars_per_segment,
                center_blob,
                stats_json,
            ),
        )
        cluster_db_ids.append(cur.lastrowid)

    # Проставляем cluster_id в hour_patterns
    for pat_id, label in zip(pattern_ids, labels, strict=True):
        cluster_db_id = cluster_db_ids[int(label)]
        cur.execute(
            """
            UPDATE hour_patterns
            SET cluster_id = ?
            WHERE id = ?;
            """,
            (cluster_db_id, pat_id),
        )

    conn.commit()
    return cluster_db_ids


def _compute_cluster_stats_for_block(
    conn: sqlite3.Connection,
    instrument: str,
    session_block: int,
) -> None:
    """
    Для каждого кластера блока собрать статистику продолжения по слотам.

    Для каждого cluster_id:
      - собираем все hours (returns_blob),
      - восстанавливаем синтетический ценовой ряд (start=1.0),
      - для каждого слота считаем доходность (P_end / P_slot - 1),
      - агрегируем: n, p_up, mean, median, min, max,
      - кладём в hour_clusters.stats_json.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, cluster_index
        FROM hour_clusters
        WHERE instrument = ? AND session_block = ?
        ORDER BY cluster_index ASC;
        """,
        (instrument, session_block),
    )
    clusters = cur.fetchall()
    if not clusters:
        return

    for cluster_id, cluster_index in clusters:
        cluster_id = int(cluster_id)
        logger.info(
            "  Считаем статистику для кластера %s (instrument=%s, block=%s)",
            cluster_index,
            instrument,
            session_block,
        )

        cur_patterns = conn.execute(
            """
            SELECT returns_blob
            FROM hour_patterns
            WHERE cluster_id = ?;
            """,
            (cluster_id,),
        )
        rows = cur_patterns.fetchall()
        if not rows:
            # Нет часов в кластере — оставляем пустую статистику
            stats = {"slots": {}}
            cur.execute(
                "UPDATE hour_clusters SET stats_json = ? WHERE id = ?;",
                (json.dumps(stats), cluster_id),
            )
            conn.commit()
            continue

        # Для каждого слота копим список доходностей
        slot_returns: Dict[int, List[float]] = {s: [] for s in SLOT_SECONDS}

        for (blob,) in rows:
            returns = np.frombuffer(bytes(blob), dtype=np.float32)
            if returns.size == 0:
                continue

            # Синтетический ценовой ряд: P[0] = 1.0
            # P[i+1] = P[i] * exp(r_i)
            levels = np.empty(returns.size + 1, dtype=np.float64)
            levels[0] = 1.0
            levels[1:] = np.exp(np.cumsum(returns, dtype=np.float64))

            end_price = float(levels[-1])
            # Реальное количество "шагов" в часе
            # каждый шаг ~5 секунд
            for slot_sec in SLOT_SECONDS:
                slot_idx = int(slot_sec // 5)
                if slot_idx >= levels.size - 1:
                    # В этом часе нет бара на этом слоте — пропускаем
                    continue
                price_at_slot = float(levels[slot_idx])
                if price_at_slot <= 0:
                    continue
                ret = end_price / price_at_slot - 1.0
                slot_returns[slot_sec].append(ret)

        stats_dict: Dict[str, Dict[str, float]] = {"slots": {}}
        for slot_sec in SLOT_SECONDS:
            rets = slot_returns[slot_sec]
            if not rets:
                stats_dict["slots"][str(slot_sec)] = {
                    "n": 0,
                    "p_up": 0.0,
                    "mean_ret": 0.0,
                    "median_ret": 0.0,
                    "min_ret": 0.0,
                    "max_ret": 0.0,
                }
                continue

            rets_sorted = sorted(rets)
            n = len(rets_sorted)
            p_up = sum(1 for r in rets_sorted if r > 0) / n
            mean_ret = sum(rets_sorted) / n
            median_ret = (
                rets_sorted[n // 2]
                if n % 2 == 1
                else 0.5 * (rets_sorted[n // 2 - 1] + rets_sorted[n // 2])
            )
            min_ret = rets_sorted[0]
            max_ret = rets_sorted[-1]

            stats_dict["slots"][str(slot_sec)] = {
                "n": n,
                "p_up": p_up,
                "mean_ret": mean_ret,
                "median_ret": median_ret,
                "min_ret": min_ret,
                "max_ret": max_ret,
            }

        cur.execute(
            """
            UPDATE hour_clusters
            SET stats_json = ?
            WHERE id = ?;
            """,
            (json.dumps(stats_dict), cluster_id),
        )
        conn.commit()


def cluster_all(
    bars_per_segment: int = DEFAULT_BARS_PER_SEGMENT,
    K: int = DEFAULT_K,
) -> None:
    """
    Главная функция: кластеризовать все instrument / session_block в hour_patterns.
    """
    init_patterns_db()
    conn = get_patterns_conn(PATTERNS_DB_PATH)

    try:
        instruments = _load_instruments(conn)
        if not instruments:
            logger.warning("В hour_patterns нет данных — кластеризовать нечего.")
            return

        logger.info("Найдено инструментов: %s", instruments)

        for instrument in instruments:
            logger.info("Инструмент: %s", instrument)
            for session_block in range(8):
                rows = _load_block_rows(conn, instrument, session_block)
                if not rows:
                    continue

                logger.info(
                    "  Блок %s: %s часов, bars_per_segment=%s",
                    session_block,
                    len(rows),
                    bars_per_segment,
                )

                X, pattern_ids = _build_vectors_for_block(rows, bars_per_segment)
                if X.size == 0 or not pattern_ids:
                    logger.info("  Блок %s: недостаточно данных для кластеризации", session_block)
                    continue

                k = min(K, X.shape[0])
                if k <= 0:
                    continue

                logger.info("  Запускаем k-means: N=%s, M=%s, K=%s", X.shape[0], X.shape[1], k)
                centers, labels = _kmeans(X, k=k)

                _reset_clusters_for_block(conn, instrument, session_block)
                cluster_db_ids = _insert_clusters_and_update_patterns(
                    conn,
                    instrument,
                    session_block,
                    centers,
                    labels,
                    pattern_ids,
                    bars_per_segment,
                )
                logger.info(
                    "  Для блока %s создано кластеров: %s (DB ids: %s)",
                    session_block,
                    len(cluster_db_ids),
                    cluster_db_ids,
                )

                _compute_cluster_stats_for_block(conn, instrument, session_block)

    finally:
        conn.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    cluster_all()


if __name__ == "__main__":
    main()
