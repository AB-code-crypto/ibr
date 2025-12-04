import logging
import sqlite3
from pathlib import Path

from core.config import PATTERNS_DB_PATH

logger = logging.getLogger(__name__)


def init_patterns_db(db_path: Path | str | None = None) -> None:
    """
    Инициализация БД паттернов (patterns.db).

    Создаёт файл и таблицы, если их нет. Многократный вызов безопасен.
    """
    if db_path is None:
        db_path = PATTERNS_DB_PATH

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")

        # Сырые часовые паттерны
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hour_patterns (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument      TEXT NOT NULL,
                contract        TEXT NOT NULL,
                session_block   INTEGER NOT NULL,
                hour_start_utc  TEXT NOT NULL,
                bars_count      INTEGER NOT NULL,
                returns_blob    BLOB NOT NULL,
                cluster_id      INTEGER,
                std_hour        REAL,
                meta            TEXT,
                UNIQUE (instrument, contract, hour_start_utc)
            );
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_hour_patterns_instr_block
                ON hour_patterns (instrument, session_block);
            """
        )

        # Кластеры паттернов
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hour_clusters (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument      TEXT NOT NULL,
                session_block   INTEGER NOT NULL,
                cluster_index   INTEGER NOT NULL,
                bars_per_segment INTEGER NOT NULL,
                center_blob     BLOB NOT NULL,
                stats_json      TEXT NOT NULL,
                UNIQUE (instrument, session_block, cluster_index)
            );
            """
        )

        conn.commit()
        logger.info("Patterns DB initialized at %s", db_path)
    finally:
        conn.close()


def get_connection(db_path: Path | str | None = None) -> sqlite3.Connection:
    """
    Удобный хелпер: открыть соединение с patterns.db с нужными PRAGMA.
    """
    if db_path is None:
        db_path = PATTERNS_DB_PATH

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn
