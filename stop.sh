#!/data/data/com.termux/files/usr/bin/bash
cd "$(dirname "$0")"
DIR="$(pwd)"
[ -f .env ] && PORT_ENV=$(grep -E '^PORT=' .env | cut -d= -f2)
PORT="${PORT:-${PORT_ENV:-8090}}"
for f in run/app.pid run/tunnel.pid; do
  [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null; rm -f "$f"
done
pkill -f "$DIR/.venv/bin/python -m uvicorn" 2>/dev/null
pkill -f "cloudflared tunnel --no-autoupdate --protocol http2 --url http://127.0.0.1:$PORT" 2>/dev/null
command -v termux-wake-unlock >/dev/null && termux-wake-unlock
echo "Stopped."
