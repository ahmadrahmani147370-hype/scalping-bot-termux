#!/data/data/com.termux/files/usr/bin/bash
# Starts backend (+ Telegram bot poller + signal engine) and a cloudflared quick tunnel,
# then registers the tunnel URL as the bot's menu button. Safe to re-run (restarts everything).
set -u
cd "$(dirname "$0")"
DIR="$(pwd)"
[ -f .env ] || { echo "No .env. Run: cp .env.example .env && nano .env"; exit 1; }
set -a; . ./.env; set +a
for v in TELEGRAM_BOT_TOKEN BINANCE_API_KEY BINANCE_API_SECRET; do
  val="${!v:-}"; case "$val" in ""|paste_*) echo "Please set $v in $DIR/.env (nano .env)"; exit 1;; esac
done
PORT="${PORT:-8090}"
[ -x .venv/bin/python ] || { echo "Run install.sh first"; exit 1; }
mkdir -p logs run
bash ./stop.sh >/dev/null 2>&1
command -v termux-wake-lock >/dev/null && termux-wake-lock

URL="${WEBAPP_URL:-}"
if [ -z "$URL" ]; then
  : > logs/tunnel.log
  nohup cloudflared tunnel --no-autoupdate --protocol http2 --url "http://127.0.0.1:$PORT" > logs/tunnel.log 2>&1 &
  echo $! > run/tunnel.pid
  echo "Waiting for cloudflared tunnel..."
  for i in $(seq 1 60); do
    if grep -q "Registered tunnel connection" logs/tunnel.log; then
      URL=$(grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' logs/tunnel.log | head -1); break
    fi; sleep 1
  done
fi
echo -n "$URL" > webapp_url.txt
WEBAPP_URL="$URL" nohup "$DIR/.venv/bin/python" -m uvicorn app:app --host 127.0.0.1 --port "$PORT" > logs/app.log 2>&1 &
echo $! > run/app.pid
sleep 4
.venv/bin/python set_menu.py "$URL"
if [ -n "$URL" ]; then
  echo "Mini app: $URL"
else
  echo "WARNING: tunnel did not connect (see logs/tunnel.log). Bot still works in chat (/signals)."
fi
curl -s "http://127.0.0.1:$PORT/healthz"; echo
echo "Running. Logs: $DIR/logs  |  Stop: bash $DIR/stop.sh"
echo "Keep Termux open (notification 'wake lock' on). Send /start to your bot in Telegram."
