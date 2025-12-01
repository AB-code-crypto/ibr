from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
PRICE_DB_PATH = BASE_DIR / "data" / "prices.sqlite3"

# Telegram bot / channels
TELEGRAM_BOT_TOKEN = "8121278489:AAFrj5FlOQmT4lctIfHOFmkqOqDL60vq5zg"
TELEGRAM_CHAT_ID_TRADING = -1002621383506  # торговый канал
TELEGRAM_CHAT_ID_COMMON = -1003208160378  # общий лог-канал
