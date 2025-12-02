from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent
PRICE_DB_PATH = BASE_DIR / "data" / "prices.sqlite3"

# Telegram bot / channels
TELEGRAM_BOT_TOKEN = "8121278489:AAFrj5FlOQmT4lctIfHOFmkqOqDL60vq5zg"
TELEGRAM_CHAT_ID_TRADING = -1002621383506  # торговый канал
TELEGRAM_CHAT_ID_COMMON = -1003208160378  # общий лог-канал

# Фьючерсы, для которых качаем историю (5-секундные бары).
# ЕДИНЫЙ источник правды:
#  - contract_month  — строка 'YYYYMM' для IB (lastTradeDateOrContractMonth)
#  - history_start   — откуда качать историю, если БД пустая (UTC)
#  - expiry          — дата экспирации контракта (UTC), на будущее
futures_for_history = {
    "MNQM5": {
        "contract_month": "202612",
        "history_start": None,
        "expiry": None,
    },
    "MNQU5": {
        "contract_month": "202612",
        "history_start": None,
        "expiry": None,
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
