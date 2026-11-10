"""Register the WebApp URL as the bot's menu button (default + owner chat). Prints no secrets."""
import json, os, sys, sqlite3
import httpx
base = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, base)
from app import load_dotenv  # noqa: E402
load_dotenv(os.path.join(base, ".env"))
T = os.environ["TELEGRAM_BOT_TOKEN"]; url = sys.argv[1] if len(sys.argv) > 1 else ""
btn = {"type": "web_app", "text": "Signals", "web_app": {"url": url}} if url else {"type": "commands"}
r = httpx.post(f"https://api.telegram.org/bot{T}/setChatMenuButton", json={"menu_button": btn}, timeout=20).json()
print("setChatMenuButton(default):", r.get("ok"), r.get("description", ""))
try:
    row = sqlite3.connect(os.path.join(base, "data.db")).execute("SELECT v FROM kv WHERE k='owner_id'").fetchone()
    if row:
        r = httpx.post(f"https://api.telegram.org/bot{T}/setChatMenuButton", json={"chat_id": json.loads(row[0]), "menu_button": btn}, timeout=20).json()
        print("setChatMenuButton(owner):", r.get("ok"))
except Exception as e:
    print("owner menu skip:", type(e).__name__)
