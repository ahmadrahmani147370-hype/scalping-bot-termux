import os, sys, importlib.util
os.environ.setdefault("DB_PATH", "/tmp/bt_dummy.db"); os.environ["DISABLE_BOT"] = "1"
for k in ("TELEGRAM_BOT_TOKEN", "BINANCE_API_KEY", "BINANCE_API_SECRET", "TG_BOT_TOKEN"): os.environ.pop(k, None)
APP_DIR = os.environ.get("APP_DIR", "/workspace/scalping-bot")
sys.path.insert(0, APP_DIR)
_cwd = os.getcwd()
import app as A
