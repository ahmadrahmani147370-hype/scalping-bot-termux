#!/data/data/com.termux/files/usr/bin/bash
# Termux installer for Scalping Bot. Works from an unzipped folder, or via:
#   curl -fsSL https://raw.githubusercontent.com/ahmadrahmani147370-hype/scalping-bot-termux/main/install.sh | bash
set -e
REPO="${REPO:-ahmadrahmani147370-hype/scalping-bot-termux}"
DIR="${DIR:-$HOME/scalping-bot}"
SRC="${BASH_SOURCE[0]:-}"; HERE=""
[ -n "$SRC" ] && [ -f "$(dirname "$SRC")/app.py" ] && HERE="$(cd "$(dirname "$SRC")" && pwd)"

echo "==> Termux packages"
export DEBIAN_FRONTEND=noninteractive
pkg update -y -o Dpkg::Options::="--force-confnew" </dev/null || true
pkg install -y -o Dpkg::Options::="--force-confnew" python cloudflared procps curl unzip </dev/null

if [ -n "$HERE" ] && [ -f "$HERE/app.py" ]; then
  if [ "$HERE" != "$DIR" ]; then mkdir -p "$DIR"; cp -r "$HERE"/. "$DIR"/; fi
else
  echo "==> Downloading $REPO"
  mkdir -p "$DIR"
  curl -fsSL "https://codeload.github.com/$REPO/tar.gz/refs/heads/main" | tar -xz -C "$DIR" --strip-components=1
fi
cd "$DIR"

echo "==> Python packages (pure Python: starlette, uvicorn, httpx)"
PY=$(command -v python || command -v python3)
[ -x .venv/bin/python ] || "$PY" -m venv .venv
.venv/bin/python -m pip install -q -r requirements.txt

if [ ! -f .env ]; then cp .env.example .env; chmod 600 .env; NEW=1; fi
chmod +x start.sh stop.sh
echo
echo "Installed in $DIR"
[ "${NEW:-0}" = 1 ] && echo "NEXT: add your keys ->  nano $DIR/.env"
echo "THEN: start the bot ->  bash $DIR/start.sh"
