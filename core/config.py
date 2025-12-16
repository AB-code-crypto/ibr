from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Базы данных проекта
PRICE_DB_PATH = DATA_DIR / "prices.sqlite3"
PATTERNS_DB_PATH = DATA_DIR / "patterns.sqlite3"
ROBOT_OPS_DB_PATH = DATA_DIR / "robot_ops.db"

# Telegram bot / channels
TELEGRAM_BOT_TOKEN = "8121278489:AAFrj5FlOQmT4lctIfHOFmkqOqDL60vq5zg"
TELEGRAM_CHAT_ID_TRADING = -1002621383506  # торговый канал
TELEGRAM_CHAT_ID_COMMON = -1003208160378  # общий лог-канал

# Фьючерсы, для которых качаем историю (5-секундные бары).
# ЕДИНЫЙ источник правды:
#  - contract_month  — строка 'YYYYMM' для IB (lastTradeDateOrContractMonth)
#  - history_start   — откуда качать историю, если БД пустая (UTC) - начинаем за 7 календарных дней до даты экспирации уходящего фьюча
#  - expiry          — дата экспирации контракта (UTC), на будущее
futures_for_history = {
    "MNQM5": {
        "contract_month": "202506",
        "history_start": datetime(2025, 3, 12, 0, 0, tzinfo=timezone.utc),
        "expiry": datetime(2025, 6, 19, 8, 30, tzinfo=timezone.utc),
    },
    "MNQU5": {
        "contract_month": "202509",
        "history_start": datetime(2025, 6, 12, 0, 0, tzinfo=timezone.utc),
        "expiry": datetime(2025, 9, 19, 8, 30, tzinfo=timezone.utc),
    },
    "MNQZ5": {
        # история для Z5: с 2025-09-12 (неделя перед экспирацией U5)
        "contract_month": "202512",
        "history_start": datetime(2025, 9, 12, 0, 0, tzinfo=timezone.utc),
        "expiry": datetime(2025, 12, 19, 8, 30, tzinfo=timezone.utc),
    },
    "MNQH6": {
        "contract_month": "202603",
        "history_start": datetime(2025, 12, 12, 0, 0, tzinfo=timezone.utc),
        "expiry": datetime(2026, 3, 20, 8, 30, tzinfo=timezone.utc),
    },
    "MNQM6": {
        "contract_month": "202606",
        "history_start": datetime(2026, 3, 13, 0, 0, tzinfo=timezone.utc),
        "expiry": datetime(2026, 6, 18, 8, 30, tzinfo=timezone.utc),
    },
    "MNQU6": {
        "contract_month": "202609",
        "history_start": datetime(2026, 6, 11, 0, 0, tzinfo=timezone.utc),
        "expiry": None,
    },
    "MNQZ6": {
        "contract_month": "202612",
        "history_start": None,
        "expiry": None,
    },
}
