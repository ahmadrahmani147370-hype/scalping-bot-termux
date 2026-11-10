"""Scalping bot v4: Starlette backend (pure Python, Termux-friendly) + Telegram bot (long polling)
+ 200-coin multi-timeframe scanner (Binance USDT-M futures, spot fallback) + spot/futures trading.
Secrets come only from env / .env and are never logged or sent to the frontend."""
import asyncio, hashlib, hmac, json, logging, os, sqlite3, time, urllib.parse
from contextlib import asynccontextmanager
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
import httpx
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

BASE = os.path.dirname(os.path.abspath(__file__))

def load_dotenv(path):
    """Minimal .env loader (KEY=VALUE lines). Real env vars take precedence."""
    if not os.path.exists(path): return
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k, v = line.split("=", 1); k = k.strip(); v = v.strip().strip('"').strip("'")
        if k and v and not os.environ.get(k): os.environ[k] = v

load_dotenv(os.path.join(BASE, ".env"))
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
BN_KEY = os.environ.get("BINANCE_API_KEY", "")
BN_SECRET = os.environ.get("BINANCE_API_SECRET", "")
TEST_MODE = os.environ.get("TEST_MODE") == "1"          # local-only initData bypass for screenshots
DISABLE_BOT = os.environ.get("DISABLE_BOT") == "1"
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE, "data.db"))
URL_FILE = os.path.join(BASE, "webapp_url.txt")
UNIVERSE_SIZE = int(os.environ.get("UNIVERSE_SIZE", "200"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
for noisy in ("httpx", "httpcore"):
    logging.getLogger(noisy).setLevel(logging.WARNING)  # httpx logs URLs, which contain the bot token
log = logging.getLogger("scalp")

TFS = ["1m", "5m", "15m"]                 # signal timeframes
ALL_TFS = ["1m", "5m", "15m", "1h"]        # 1h only used as higher-timeframe confirmation
FETCH_EVERY = {"1m": 15, "5m": 60, "15m": 90, "1h": 300}   # seconds; each divides its candle length -> aligned to closes
CLOSE_LAG = 2.0                            # fetch this many seconds after each aligned boundary (candle close)
STALE_MULT = 2                             # data older than 2x its refresh interval = stale (no alerts / auto)
LIVE_MAX_AGE = 10                          # live price older than this falls back to the kline close
WS_FUT = "wss://fstream.binance.com/market/stream?streams=!miniTicker@arr"
WS_SPOT = "wss://data-stream.binance.vision/stream?streams=!miniTicker@arr"
INC_COOLDOWN = 1800                        # one Telegram health alert per incident type per 30 min
HTF = {"1m": ["5m", "15m"], "5m": ["15m", "1h"], "15m": ["1h"]}
HOT = 80                                   # "80%+" threshold
KLINES = 99                                # limit < 100 -> weight 1 on fapi
STABLES = {"USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "EUR", "USD1", "XUSD", "USDE", "AEUR", "PYUSD", "BFUSD", "RLUSD"}
FAPI = "https://fapi.binance.com"
SPOT_PUBLIC = ["https://data-api.binance.vision", "https://api.binance.com"]
PRIVATE_HOSTS = ["https://api.binance.com", "https://api1.binance.com", "https://api2.binance.com", "https://api3.binance.com"]
DEFAULT_SETTINGS = {"timeframe": "5m", "market": "futures", "leverage": 3, "order_size_usdt": 10.0, "max_open_positions": 3,
                    "daily_loss_limit_usdt": 5.0, "auto_min_confidence": 85, "alerts": True, "alert_threshold": 80,
                    "auto_trade": False, "atr_sl_mult": 1.0, "atr_tp_mult": 1.5,
                    "alerts_fav_only": False, "alert_cooldown_min": 15, "max_alerts_per_min": 10,
                    "max_trades_per_day": 20, "trailing": True, "be_trigger_atr": 0.5, "trail_atr": 1.0,
                    "liq_filter": True, "max_funding_pct": 0.1, "min_qv_musd": 20.0,
                    "btc_guard": True, "btc_move_pct": 1.0, "btc_pause_min": 15, "strong_min_conf": 60}
PAPER_EXPIRE = 60                          # paper signals expire after 60 candles of their timeframe
TF_SEC = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}
MAX_LEVERAGE = 10

# ---------------- DB ----------------
def db():
    c = sqlite3.connect(DB_PATH, timeout=10); c.row_factory = sqlite3.Row; return c

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
        CREATE TABLE IF NOT EXISTS signals(id INTEGER PRIMARY KEY, ts REAL, symbol TEXT, tf TEXT, signal TEXT,
            confidence INTEGER, price REAL, entry REAL, tp REAL, sl REAL, strong INTEGER);
        CREATE TABLE IF NOT EXISTS trades(id INTEGER PRIMARY KEY, ts REAL, symbol TEXT, side TEXT, mode TEXT, tf TEXT,
            qty REAL, price REAL, quote REAL, tp REAL, sl REAL, status TEXT, order_id TEXT, oco_id TEXT,
            exit_price REAL, pnl REAL, closed_ts REAL, error TEXT);
        """)
        c.execute("""CREATE TABLE IF NOT EXISTS paper(id INTEGER PRIMARY KEY, ts REAL, symbol TEXT, tf TEXT, side TEXT,
            confidence INTEGER, entry REAL, tp REAL, sl REAL, status TEXT DEFAULT 'OPEN', closed_ts REAL, exit REAL, last_ot REAL)""")
        c.execute("CREATE INDEX IF NOT EXISTS paper_status ON paper(status)")
        cols = {r[1] for r in c.execute("PRAGMA table_info(trades)")}
        for col, typ in (("market", "TEXT"), ("leverage", "INTEGER"), ("tp_algo", "TEXT"), ("sl_algo", "TEXT"),
                         ("atr", "REAL"), ("be_done", "INTEGER"), ("sl_moved_ts", "REAL")):
            if col not in cols: c.execute(f"ALTER TABLE trades ADD COLUMN {col} {typ}")

def kv_get(k, default=None):
    with db() as c:
        r = c.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return json.loads(r["v"]) if r else default

def kv_set(k, v):
    with db() as c:
        c.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)", (k, json.dumps(v)))

def get_settings():
    s = dict(DEFAULT_SETTINGS); s.update({k: v for k, v in kv_get("settings", {}).items() if k in DEFAULT_SETTINGS})
    s["alert_threshold"] = max(HOT, s["alert_threshold"]); s["auto_min_confidence"] = max(HOT, s["auto_min_confidence"])
    return s

def save_settings(upd):
    s = get_settings()
    for k, v in upd.items():
        if k not in DEFAULT_SETTINGS: continue
        t = type(DEFAULT_SETTINGS[k])
        if t is bool: v = bool(v)
        elif t is int: v = int(float(v))
        elif t is float: v = float(v)
        else: v = str(v)
        s[k] = v
    if s["timeframe"] not in TFS: s["timeframe"] = "5m"
    if s["market"] not in ("futures", "spot"): s["market"] = "futures"
    s["leverage"] = max(1, min(int(s["leverage"]), MAX_LEVERAGE))
    s["order_size_usdt"] = max(5.0, min(s["order_size_usdt"], 1000.0))
    s["max_open_positions"] = max(1, min(s["max_open_positions"], 10))
    s["daily_loss_limit_usdt"] = max(0.5, s["daily_loss_limit_usdt"])
    s["auto_min_confidence"] = max(HOT, min(s["auto_min_confidence"], 99))
    s["alert_threshold"] = max(HOT, min(s["alert_threshold"], 99))
    s["atr_sl_mult"] = max(0.3, min(s["atr_sl_mult"], 5)); s["atr_tp_mult"] = max(0.3, min(s["atr_tp_mult"], 10))
    s["alert_cooldown_min"] = max(1, min(s["alert_cooldown_min"], 240)); s["max_alerts_per_min"] = max(1, min(s["max_alerts_per_min"], 30))
    s["max_trades_per_day"] = max(1, min(s["max_trades_per_day"], 200))
    s["be_trigger_atr"] = max(0.1, min(s["be_trigger_atr"], 5)); s["trail_atr"] = max(0.2, min(s["trail_atr"], 5))
    s["max_funding_pct"] = max(0.01, min(s["max_funding_pct"], 2)); s["min_qv_musd"] = max(0, min(s["min_qv_musd"], 10000))
    s["strong_min_conf"] = max(50, min(s["strong_min_conf"], 95))
    s["btc_move_pct"] = max(0.2, min(s["btc_move_pct"], 10)); s["btc_pause_min"] = max(1, min(s["btc_pause_min"], 240))
    kv_set("settings", s); return s

# ---------------- indicators ----------------
def ema(vals, n):
    """EMA seeded with the SMA of the first n values (TA-Lib / TradingView). Entries before that are None."""
    out = [None] * len(vals)
    if len(vals) < n: return out
    k = 2 / (n + 1); e = sum(vals[:n]) / n; out[n - 1] = e
    for i in range(n, len(vals)): e = vals[i] * k + e * (1 - k); out[i] = e
    return out

def macd_hist(c, fast=12, slow=26, sig=9):
    f, sl = ema(c, fast), ema(c, slow)
    m = [a - b for a, b in zip(f, sl) if b is not None]
    sg = ema(m, sig)
    return [a - b for a, b in zip(m, sg) if b is not None], m, sg

def rsi(closes, n=9):
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]; gains += max(d, 0); losses += max(-d, 0)
    ag, al = gains / n, losses / n
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0)) / n; al = (al * (n - 1) + max(-d, 0)) / n
    if al == 0: return 50.0 if ag == 0 else 100.0
    return 100 - 100 / (1 + ag / al)

def atr(h, l, c, n=14):
    trs = [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, len(c))]
    a = sum(trs[:n]) / n
    for t in trs[n:]: a = (a * (n - 1) + t) / n
    return a

ATR_BAND = {"1m": (0.0004, 0.015), "5m": (0.0008, 0.03), "15m": (0.0012, 0.05), "1h": (0.002, 0.1)}

def candle_patterns(o, h, l, c, e21):
    """Patterns on the last CLOSED candle (-2) plus inside-bar breakout by the live candle (-1)."""
    found = []; vote = 0.0
    def body(i): return abs(c[i] - o[i])
    i, p = -2, -3
    rng = h[i] - l[i]
    if rng > 0:
        b = body(i); up = h[i] - max(o[i], c[i]); lo = min(o[i], c[i]) - l[i]
        if c[p] < o[p] and c[i] > o[i] and c[i] >= o[p] and o[i] <= c[p] and b > body(p):
            found.append("Bullish engulfing"); vote += 1
        elif c[p] > o[p] and c[i] < o[i] and c[i] <= o[p] and o[i] >= c[p] and b > body(p):
            found.append("Bearish engulfing"); vote -= 1
        if b > 0 and lo >= 2 * b and up <= 0.6 * b and c[i] <= e21[i] * 1.002:
            found.append("Hammer / bullish pin bar"); vote += 1
        elif b > 0 and up >= 2 * b and lo <= 0.6 * b and c[i] >= e21[i] * 0.998:
            found.append("Shooting star / bearish pin bar"); vote -= 1
    m, ib = -3, -2  # mother bar, inside bar; live candle breaks out
    if h[ib] <= h[m] and l[ib] >= l[m]:
        if c[-1] > h[m]: found.append("Inside bar breakout up"); vote += 1
        elif c[-1] < l[m]: found.append("Inside bar breakdown"); vote -= 1
    return max(-1.0, min(1.0, vote)), found

def analyze_tf(k, tf, live=None, now=None):
    """k = (o,h,l,c,v,open_times). Closed candles + the open candle; its close/high/low are replaced by the live
    price while that candle is still open. Returns indicator votes and a trend score for higher-TF use."""
    o, h, l, c, v = k[:5]
    if live and len(k) > 5 and (now or time.time()) < k[5][-1] + TF_SEC[tf]:
        c = c[:-1] + [live]; h = h[:-1] + [max(h[-1], live)]; l = l[:-1] + [min(l[-1], live)]
    price = c[-1]
    e9, e21, e50 = ema(c, 9), ema(c, 21), ema(c, 50)
    comps = []  # (name, vote -1..1, weight, is_event, detail)
    if price > e9[-1] > e21[-1] > e50[-1]: tv = 1
    elif price < e9[-1] < e21[-1] < e50[-1]: tv = -1
    elif e9[-1] > e21[-1] and price > e50[-1]: tv = 0.5
    elif e9[-1] < e21[-1] and price < e50[-1]: tv = -0.5
    else: tv = 0
    comps.append(("EMA 9/21/50", tv, 2.0, False, "bull stack" if tv == 1 else "bear stack" if tv == -1 else "mixed"))
    cu = any(e9[-j - 1] <= e21[-j - 1] and e9[-j] > e21[-j] for j in range(1, 4))
    cd = any(e9[-j - 1] >= e21[-j - 1] and e9[-j] < e21[-j] for j in range(1, 4))
    comps.append(("EMA 9/21 cross", 1 if cu else -1 if cd else 0, 1.0, True, "fresh up" if cu else "fresh down" if cd else "none"))
    r = rsi(c, 9)
    rv = (1 if 50 < r <= 70 else 0.5 if 70 < r <= 80 else -0.5 if r > 80 else
          -1 if 30 <= r < 50 else -0.5 if 20 <= r < 30 else 0.5 if r < 20 else 0)
    comps.append(("RSI(9)", rv, 1.0, False, f"{r:.0f}"))
    hist = macd_hist(c)[0]
    mv = (1 if hist[-1] > hist[-2] else 0.5) if hist[-1] > 0 else (-1 if hist[-1] < hist[-2] else -0.5)
    comps.append(("MACD hist", mv, 1.0, False, "rising" if hist[-1] > hist[-2] else "falling"))
    tp_ = [(h[j] + l[j] + c[j]) / 3 for j in range(len(c))]
    vw = sum(t * vv for t, vv in zip(tp_, v)) / max(sum(v), 1e-12)
    comps.append(("VWAP (rolling 99)", 1 if price > vw else -1, 1.0, False, "above" if price > vw else "below"))
    avgv = sum(v[-22:-2]) / 20 or 1e-12
    spike_i = -2 if v[-2] > 2 * avgv else (-1 if v[-1] > 2 * avgv else None)
    w = c[-20:]; mid = sum(w) / 20; sd = (sum((x - mid) ** 2 for x in w) / 20) ** 0.5
    up, lo = mid + 2 * sd, mid - 2 * sd
    if price > up: bv = 1 if spike_i is not None else -0.5
    elif price < lo: bv = -1 if spike_i is not None else 0.5
    else: bv = 0
    comps.append(("Bollinger", bv, 1.0, True, "breakout up" if bv == 1 else "breakdown" if bv == -1 else
                  "stretched" if bv else "inside bands"))
    sv = 0 if spike_i is None else (1 if c[spike_i] >= o[spike_i] else -1)
    comps.append(("Volume spike", sv, 1.0, True, f"{v[spike_i] / avgv:.1f}x" if spike_i is not None else "normal"))
    pv, pats = candle_patterns(o, h, l, c, e21)
    comps.append(("Candles", pv, 1.5, True, ", ".join(pats) or "no pattern"))
    a = atr(h, l, c, 14); atr_pct = a / price if price else 0
    trend = (2 * tv + mv + (1 if price > vw else -1)) / 4
    return {"comps": comps, "trend": trend, "atr": a, "atr_pct": atr_pct, "price": price, "rsi": r, "patterns": pats}

def combine(raw, tf, s):
    """Final signal for tf with multi-timeframe confirmation and ATR volatility filter."""
    a = raw[tf]; comps = a["comps"]
    counted = [(n, vt, w, ev) for n, vt, w, ev, _ in comps if not (ev and vt == 0)]
    tot = sum(w for *_, w, _ in counted)
    score = sum(vt * w for _, vt, w, _ in counted) / tot
    direction = "LONG" if score >= 0.3 else "SHORT" if score <= -0.3 else "NEUTRAL"
    notes = []
    d = 1 if direction == "LONG" else -1 if direction == "SHORT" else 0
    if d:
        agree = sum(w * max(vt * d, 0) for _, vt, w, _ in counted)
        # indicators with no event (no cross / pattern / spike / band break) count as half-weight "missing confirmation"
        missing = sum(0.5 * w for _, vt, w, ev, _ in comps if ev and vt == 0)
        conf = 100 * agree / (tot + missing)
        htf_ok = 0
        for t in HTF[tf]:
            ht = raw.get(t)
            if not ht: continue
            if ht["trend"] * d <= -0.25:
                notes.append(f"{t} trend opposes"); direction = "NEUTRAL"; break
            if ht["trend"] * d >= 0.25: htf_ok += 1; conf += 3
        lo, hi = ATR_BAND[tf]
        if a["atr_pct"] < lo: notes.append("volatility too low"); direction = "NEUTRAL"
        elif a["atr_pct"] > hi: notes.append("volatility very high"); conf *= 0.85
        if not any(ev and vt * d > 0 for _, vt, _, ev in counted):
            conf = min(conf, 79); notes.append("no candle/volume/cross trigger")
        if htf_ok == 0 and direction != "NEUTRAL": conf = min(conf, 79); notes.append("no higher-TF agreement")
    if direction == "NEUTRAL":
        conf = 100 * max(sum(w * max(vt, 0) for _, vt, w, _ in counted), sum(w * max(-vt, 0) for _, vt, w, _ in counted)) / tot
        conf = min(conf, 60)
    conf = int(round(max(0, min(conf, 99))))
    price, at = a["price"], a["atr"]
    tpd = max(s["atr_tp_mult"] * at, price * 0.0025); sld = max(s["atr_sl_mult"] * at, price * 0.0015)
    if direction == "SHORT": tp, sl = price - tpd, price + sld
    else: tp, sl = price + tpd, price - sld
    return {"signal": direction, "confidence": conf, "score": round(score, 3), "price": price, "entry": price, "tp": tp, "sl": sl, "atr": at,
            "atr_pct": round(a["atr_pct"] * 100, 3), "rsi": round(a["rsi"], 1), "notes": notes, "patterns": a["patterns"],
            "indicators": [{"name": n, "vote": vt, "weight": w, "detail": dt} for n, vt, w, _, dt in comps],
            "htf": {t: round(raw[t]["trend"], 2) for t in HTF[tf] if t in raw}}

# ---------------- market data / scanner ----------------
STATE = {"signals": {}, "raw": {}, "tickers": {}, "updated": 0, "errors": [], "key_check": None, "webapp_url": None,
         "bot_username": None, "last_poll": 0, "poll_ok": False, "last_alert": {}, "hot_prev": {},
         "source": None, "universe": [], "info": {}, "universe_ts": 0, "weight": 0, "weight_min": 0, "backoff_until": 0,
         "scan": {}, "fetch_errors": 0, "rate_limit_hits": 0, "requests": 0, "spot_filters": {}, "funding": {},
         "funding_ts": 0, "alert_times": [], "weight_peak": 0, "coverage": {}, "cov_tot": {}, "cov_min": {}, "short_hist": {},
         "gaps": {}, "align_retries": 0, "retries": 0, "max_age_seen": {}, "live_src": None, "rest_price_polls": 0,
         "ws": {"status": "starting", "since": None, "last_msg": 0, "msgs": 0, "reconnects": 0, "up_s": 0.0, "start": time.time(), "url": None},
         "hb": {}, "incidents": {}, "inc_log": [], "inc_last_notify": {}, "restarts": {}, "err_count": {}, "start": time.time(),
         "mood": {}, "pause": {"LONG": 0, "SHORT": 0}, "ready": False, "ex_pnl": None}
KL = {}                     # (sym, tf) -> (o, h, l, c, v, open_times)
KTS = {}                    # (sym, tf) -> local time of the last successful fetch
LIVE = {}                   # sym -> (live price, local receive time)
client: httpx.AsyncClient = None
SEM = asyncio.Semaphore(int(os.environ.get("FETCH_CONCURRENCY", "64")))
POST_LOCK = asyncio.Lock()   # one compute/alert pass at a time
TRADE_LOCK = asyncio.Lock()  # never two order flows at once (manual vs auto vs exits)

def add_err(msg, cat=None):
    cat = cat or msg.split(":")[0].split(" ")[0][:24]
    STATE["err_count"][cat] = STATE["err_count"].get(cat, 0) + 1
    STATE["errors"] = ([{"ts": time.time(), "cat": cat, "msg": msg[:300]}] + STATE["errors"])[:40]; log.warning(msg[:300])

def md_path(p):  # p like "klines", "ticker/24hr", "exchangeInfo"
    return ("/fapi/v1/" if STATE["source"] == "futures" else "/api/v3/") + p

def weight_limit(): return 2400 if STATE["source"] == "futures" else 6000

async def weight_gate():
    while True:
        now = time.time()
        if now < STATE["backoff_until"]:
            await asyncio.sleep(STATE["backoff_until"] - now); continue
        cur_min = int(now // 60)
        if STATE["weight_min"] == cur_min and STATE["weight"] > 0.7 * weight_limit():
            await asyncio.sleep(61 - now % 60); continue
        return

async def md_get(p, params):
    """Public market-data GET with weight tracking and 429/418 backoff (Retry-After honoured)."""
    await weight_gate()
    hosts = [FAPI] if STATE["source"] == "futures" else SPOT_PUBLIC
    last = None
    for host in hosts:
        try:
            r = await client.get(host + md_path(p), params=params, timeout=10)
            STATE["requests"] += 1
            w = r.headers.get("x-mbx-used-weight-1m")
            if w:
                STATE["weight"], STATE["weight_min"] = int(w), int(time.time() // 60)
                STATE["weight_peak"] = max(STATE["weight_peak"], int(w))
            if r.status_code in (429, 418):
                STATE["rate_limit_hits"] += 1
                ra = r.headers.get("retry-after")
                STATE["backoff_until"] = max(STATE["backoff_until"], time.time() + (int(ra) if ra and ra.isdigit() else 60))
                raise RuntimeError(f"rate limited HTTP {r.status_code}")
            if r.status_code == 200: return r.json()
            last = f"{host}{md_path(p)} HTTP {r.status_code}: {r.text[:150]}"
        except RuntimeError: raise
        except Exception as e: last = f"{host} {type(e).__name__}: {e}"
    raise RuntimeError(last)

async def detect_source():
    if os.environ.get("FORCE_SPOT") == "1":
        STATE["source"] = "spot"; return
    try:
        r = await client.get(FAPI + "/fapi/v1/ping", timeout=10)
        if r.status_code == 200:
            STATE["source"] = "futures"; return
        add_err(f"fapi ping HTTP {r.status_code} -> falling back to spot")
    except Exception as e:
        add_err(f"fapi unreachable ({type(e).__name__}) -> falling back to spot")
    STATE["source"] = "spot"

async def refresh_universe():
    if STATE["source"] == "futures":
        info = await md_get("exchangeInfo", {})
        ok = {x["symbol"]: x for x in info["symbols"] if x.get("contractType") == "PERPETUAL" and x.get("quoteAsset") == "USDT"
              and x.get("status") == "TRADING"}
    else:
        info = await md_get("exchangeInfo", {"permissions": "SPOT"})
        ok = {x["symbol"]: x for x in info["symbols"] if x.get("quoteAsset") == "USDT" and x.get("status") == "TRADING"
              and x["baseAsset"] not in STABLES and not x["baseAsset"].endswith(("UP", "DOWN", "BULL", "BEAR"))}
    tick = await md_get("ticker/24hr", {})
    vols = sorted(((float(t.get("quoteVolume") or 0), t["symbol"]) for t in tick if t["symbol"] in ok), reverse=True)
    uni = [s for _, s in vols[:UNIVERSE_SIZE]]
    meta = {}
    for s in uni:
        x = ok[s]; f = {y["filterType"]: y for y in x["filters"]}
        lot = f.get("MARKET_LOT_SIZE") or f.get("LOT_SIZE") or {}
        if not lot or float(lot.get("stepSize", 0)) == 0: lot = f.get("LOT_SIZE", {})
        mn = f.get("MIN_NOTIONAL") or f.get("NOTIONAL") or {}
        meta[s] = {"base": x["baseAsset"], "tick": f["PRICE_FILTER"]["tickSize"], "step": lot.get("stepSize", "1"),
                   "min_qty": float(lot.get("minQty", 0)), "min_notional": float(mn.get("notional", mn.get("minNotional", 5)))}
    for k in [k for k in KL if k[0] not in meta]: KL.pop(k, None); KTS.pop(k, None)
    STATE["universe"], STATE["info"], STATE["universe_ts"] = uni, meta, time.time()
    update_tickers(tick)
    log.info("universe: %d %s symbols", len(uni), STATE["source"])

def update_tickers(tick):
    for t in tick:
        if t["symbol"] in STATE["info"]:
            STATE["tickers"][t["symbol"]] = {"chg": round(float(t.get("priceChangePercent") or 0), 2), "qv": float(t.get("quoteVolume") or 0)}

def period_start(tf, t=None):
    t = time.time() if t is None else t
    return t - t % TF_SEC[tf]

async def fetch_tf(tf):
    """Fetch klines for every coin. Retries (x3) on errors and when the exchange has not opened the new candle yet,
    so each cycle covers all 200 coins with candle-aligned data."""
    errs = 0; ok = 0; short = 0; uni = list(STATE["universe"]); t0 = time.time(); worst_age = 0.0
    async def one(sym):
        nonlocal errs, ok, short, worst_age
        for attempt in range(4):
            try:
                async with SEM:
                    kl = await md_get("klines", {"symbol": sym, "interval": tf, "limit": KLINES})
                if not kl: raise RuntimeError("empty klines")
                if kl[-1][0] / 1000 < period_start(tf) and attempt < 3:
                    STATE["align_retries"] += 1; await asyncio.sleep(1.0); continue   # new candle not open yet
                ots = [x[0] / 1000 for x in kl]
                gaps = sum(1 for a, b in zip(ots, ots[1:]) if b - a != TF_SEC[tf])
                if gaps: STATE["gaps"][f"{sym}|{tf}"] = gaps
                else: STATE["gaps"].pop(f"{sym}|{tf}", None)
                if len(kl) < 60: short += 1; return            # newly listed coin: not enough history
                now = time.time(); prev = KTS.get((sym, tf))
                if prev: worst_age = max(worst_age, now - prev)
                KL[(sym, tf)] = ([float(x[1]) for x in kl], [float(x[2]) for x in kl], [float(x[3]) for x in kl],
                                 [float(x[4]) for x in kl], [float(x[5]) for x in kl], ots)
                KTS[(sym, tf)] = now; ok += 1
                return
            except asyncio.CancelledError: raise
            except Exception as e:
                if attempt == 3:
                    errs += 1
                    if errs <= 3: add_err(f"klines {sym} {tf}: {e}", "klines")
                else:
                    STATE["retries"] += 1; await asyncio.sleep(0.5 * (attempt + 1))
    await asyncio.gather(*[one(s) for s in uni])
    denom = max(1, len(uni) - short); cov = round(100 * ok / denom, 2)
    STATE["coverage"][tf] = cov; STATE["short_hist"][tf] = short
    STATE["cov_min"][tf] = min(STATE["cov_min"].get(tf, 100.0), cov)
    ct = STATE["cov_tot"].setdefault(tf, [0, 0]); ct[0] += ok; ct[1] += denom
    STATE["max_age_seen"][tf] = max(STATE["max_age_seen"].get(tf, 0), round(worst_age, 1))
    STATE["fetch_errors"] += errs
    STATE["scan"][f"fetch_{tf}_s"] = round(time.time() - t0, 2); STATE["scan"][f"last_{tf}"] = time.time()

def best_price(sym, now=None):
    lv = LIVE.get(sym)
    if lv and (now or time.time()) - lv[1] <= LIVE_MAX_AGE: return lv[0]
    return None

def is_stale(sym, tf, now):
    ts = KTS.get((sym, tf)); iv = FETCH_EVERY[tf]
    if not ts or now - ts > STALE_MULT * iv: return True
    k = KL.get((sym, tf))
    return bool(k) and now - (k[5][-1] + TF_SEC[tf]) > iv        # candle should have rolled over by now

def compute_all(s):
    t0 = time.time(); now = t0; out = {tf: {} for tf in TFS}; btc_raw = {}
    for sym in STATE["universe"]:
        lp = best_price(sym, now); raw = {}; stale = {}
        for tf in ALL_TFS:
            k = KL.get((sym, tf))
            if not k: continue
            stale[tf] = is_stale(sym, tf, now)
            try: raw[tf] = analyze_tf(k, tf, lp, now)
            except Exception as e: add_err(f"analyze {sym} {tf}: {type(e).__name__}: {e}", "analyze")
        if sym == "BTCUSDT": btc_raw = raw
        for tf in TFS:
            if tf not in raw: continue
            a = combine(raw, tf, s)
            a["stale"] = stale[tf] or any(stale.get(h, True) for h in HTF[tf])
            a["age"] = round(now - KTS.get((sym, tf), 0), 1); a["price_src"] = "live" if lp else "kline"
            out[tf][sym] = a
        sigs = [out[tf][sym]["signal"] if sym in out[tf] else None for tf in TFS]
        d = 1 if sigs[0] == "LONG" else -1 if sigs[0] == "SHORT" else 0
        h1 = raw.get("1h")
        strong = (bool(d) and all(x == sigs[0] for x in sigs) and h1 is not None and h1["trend"] * d > -0.25
                  and all(out[tf][sym]["confidence"] >= s["strong_min_conf"] for tf in TFS)
                  and not any(out[tf][sym]["stale"] for tf in TFS))
        flags = liq_flags(sym, s)
        for tf in TFS:
            if sym in out[tf]:
                a = out[tf][sym]; a["strong"] = strong; a["flags"] = flags
                a["filtered"] = bool(flags) and s["liq_filter"]
                a["paused"] = side_paused(a["signal"], s, now)
    STATE["signals"] = out; STATE["updated"] = time.time()
    STATE["mood"] = market_mood(out, btc_raw, s, now)
    STATE["scan"]["compute_s"] = round(time.time() - t0, 3)

def side_paused(side, s, now=None):
    return bool(s["btc_guard"]) and side in ("LONG", "SHORT") and STATE["pause"].get(side, 0) > (now or time.time())

def market_mood(out, btc_raw, s, now):
    m = {"tf": {}, "btc": {}, "ts": now}
    for tf in TFS:
        vals = list(out[tf].values()); n = len(vals) or 1
        L = sum(1 for a in vals if a["signal"] == "LONG"); S = sum(1 for a in vals if a["signal"] == "SHORT")
        m["tf"][tf] = {"long_pct": round(100 * L / n, 1), "short_pct": round(100 * S / n, 1),
                       "neutral_pct": round(100 * (len(vals) - L - S) / n, 1), "n": len(vals)}
    for tf in ("15m", "1h"):
        r = btc_raw.get(tf)
        if r: m["btc"][tf] = {"trend": round(r["trend"], 2), "label": "UP" if r["trend"] >= 0.25 else "DOWN" if r["trend"] <= -0.25 else "SIDEWAYS"}
    k1 = KL.get(("BTCUSDT", "1m")); px = best_price("BTCUSDT", now) or (k1[3][-1] if k1 else None)
    m["btc_price"] = px
    m["btc_move_5m"] = round(100 * (px - k1[0][-5]) / k1[0][-5], 3) if k1 and px and len(k1[0]) >= 5 else None
    t5 = m["tf"]["5m"]; b = [x["label"] for x in m["btc"].values()]
    if t5["long_pct"] >= 55 or (b and all(x == "UP" for x in b) and t5["long_pct"] > t5["short_pct"]): m["label"] = "BULLISH"
    elif t5["short_pct"] >= 55 or (b and all(x == "DOWN" for x in b) and t5["short_pct"] > t5["long_pct"]): m["label"] = "BEARISH"
    else: m["label"] = "MIXED"
    m["paused"] = {side: (round(STATE["pause"][side] - now) if side_paused(side, s, now) else 0) for side in ("LONG", "SHORT")}
    m["guard"] = {"on": s["btc_guard"], "pct": s["btc_move_pct"], "min": s["btc_pause_min"]}
    return m

async def update_btc_guard(s):
    mv = STATE["mood"].get("btc_move_5m"); now = time.time()
    if not s["btc_guard"] or mv is None: return
    for side, hit in (("LONG", mv <= -s["btc_move_pct"]), ("SHORT", mv >= s["btc_move_pct"])):
        if hit:
            was = STATE["pause"][side] > now
            STATE["pause"][side] = now + s["btc_pause_min"] * 60
            if not was:
                await notify(f"🛑 BTC sudden move {mv:+.2f}% in ~5m → {side} alerts + auto trades {s['btc_pause_min']} min ke liye PAUSE.")

def liq_flags(sym, s):
    """Funding / liquidity warnings for a symbol (empty list = OK)."""
    out = []; t = STATE["tickers"].get(sym, {}); fr = STATE["funding"].get(sym)
    if fr is not None and abs(fr) * 100 > s["max_funding_pct"]: out.append(f"funding {fr * 100:+.3f}%")
    if t and t.get("qv", 0) < s["min_qv_musd"] * 1e6: out.append(f"low volume {t.get('qv', 0) / 1e6:.0f}M")
    return out

def is_hot(a): return a["signal"] != "NEUTRAL" and a["confidence"] >= HOT and not a.get("filtered") and not a.get("stale")

def favorites(): return kv_get("favorites", [])

def sorted_signals(tf):
    sigs = STATE["signals"].get(tf, {})
    def key(item):
        sym, a = item
        hot = is_hot(a); st = a.get("strong") and not a.get("filtered") and not a.get("stale")
        grp = 0 if hot and st else 1 if hot else 2 if st else 3 if a["signal"] != "NEUTRAL" else 4   # STRONG confluence first
        return (grp, -a["confidence"], -STATE["tickers"].get(sym, {}).get("qv", 0))
    return sorted(sigs.items(), key=key)

async def record_and_alert(s):
    """Record newly-hot (>=80%) signals to history + paper tracker, then send deduped, rate-capped alerts."""
    now = time.time(); fresh = []
    for tf in TFS:
        for sym, a in STATE["signals"].get(tf, {}).items():
            k = (sym, tf); cur = a["signal"] if is_hot(a) else None
            if cur and STATE["hot_prev"].get(k) != cur:
                k1 = KL.get((sym, tf)); last_ot = k1[5][-1] if k1 and len(k1) > 5 else now
                with db() as c:
                    c.execute("INSERT INTO signals(ts,symbol,tf,signal,confidence,price,entry,tp,sl,strong) VALUES(?,?,?,?,?,?,?,?,?,?)",
                              (now, sym, tf, cur, a["confidence"], a["price"], a["entry"], a["tp"], a["sl"], int(a["strong"])))
                    dup = c.execute("SELECT 1 FROM paper WHERE status='OPEN' AND symbol=? AND tf=? AND side=?", (sym, tf, cur)).fetchone()
                    if not dup:   # one open paper position per coin+tf+side (no double counting on 79/80 flicker)
                        c.execute("INSERT INTO paper(ts,symbol,tf,side,confidence,entry,tp,sl,last_ot) VALUES(?,?,?,?,?,?,?,?,?)",
                                  (now, sym, tf, cur, a["confidence"], a["entry"], a["tp"], a["sl"], last_ot))
                fresh.append((sym, tf, a))
            STATE["hot_prev"][k] = cur
    if not s["alerts"] or not fresh: return
    favs = set(favorites()); cd = s["alert_cooldown_min"] * 60
    STATE["alert_times"] = [t for t in STATE["alert_times"] if now - t < 60]
    budget = s["max_alerts_per_min"] - len(STATE["alert_times"])
    send, skipped = [], 0
    for sym, tf, a in sorted(fresh, key=lambda x: (x[0] not in favs, not x[2].get("strong"), -x[2]["confidence"])):
        if a["confidence"] < s["alert_threshold"]: continue
        if s["alerts_fav_only"] and sym not in favs: continue
        if a.get("paused"): continue                      # BTC sudden-move guard
        key = f"{sym}|{tf}|{a['signal']}"
        if now - STATE["last_alert"].get(key, 0) < cd: continue
        if len(send) >= budget: skipped += 1; continue
        STATE["last_alert"][key] = now; send.append((sym, tf, a))
    if not send: return
    STATE["alert_times"] += [now] * len(send)
    lines = [f"🔔 Naye {s['alert_threshold']}%+ signals ({len(send)}) — indicator confidence, win probability nahi"]
    for sym, tf, a in send:
        lines.append(f"{'⭐ ' if sym in favs else ''}{'🟢' if a['signal'] == 'LONG' else '🔴'} {a['signal']} {sym} [{tf}] {a['confidence']}%{' 🔥 STRONG (1m+5m+15m)' if a['strong'] else ''}\n"
                     f"   Entry {fmt_price(sym, a['entry'])} | TP {fmt_price(sym, a['tp'])} | SL {fmt_price(sym, a['sl'])}")
    if skipped: lines.append(f"(+{skipped} aur — per-minute limit; /signals dekhein)")
    await notify("\n".join(lines))

def update_paper():
    """Mark paper signals TP / SL / EXPIRED from cached candles (candles that opened after the signal)."""
    now = time.time(); changes = 0
    with db() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM paper WHERE status='OPEN'")]
        for p in rows:
            k = KL.get((p["symbol"], p["tf"])); status = None; exit_px = None; last_ot = p["last_ot"] or p["ts"]
            if k and len(k) > 5:
                d = 1 if p["side"] == "LONG" else -1
                for i, ot in enumerate(k[5]):
                    if ot <= (p["last_ot"] or p["ts"]): continue
                    hi, lo = k[1][i], k[2][i]
                    sl_hit = (lo <= p["sl"]) if d == 1 else (hi >= p["sl"])
                    tp_hit = (hi >= p["tp"]) if d == 1 else (lo <= p["tp"])
                    if sl_hit: status, exit_px = "SL", p["sl"]; break        # conservative: SL first if both in one candle
                    if tp_hit: status, exit_px = "TP", p["tp"]; break
                    if i < len(k[5]) - 1: last_ot = ot                        # only advance past fully closed candles
            if not status and now - p["ts"] > PAPER_EXPIRE * TF_SEC[p["tf"]]:
                status = "EXPIRED"; exit_px = k[3][-1] if k else None
            if status:
                c.execute("UPDATE paper SET status=?, closed_ts=?, exit=? WHERE id=?", (status, now, exit_px, p["id"])); changes += 1
            elif last_ot != p["last_ot"]:
                c.execute("UPDATE paper SET last_ot=? WHERE id=?", (last_ot, p["id"]))
    return changes

def paper_stats():
    now = time.time(); out = {}
    with db() as c:
        for label, since in (("24h", now - 86400), ("7d", now - 7 * 86400)):
            rows = c.execute("SELECT tf, side, status, COUNT(*) n FROM paper WHERE ts>=? GROUP BY tf, side, status", (since,)).fetchall()
            per = {}
            for r in rows:
                d = per.setdefault(f"{r['tf']}|{r['side']}", {"tf": r["tf"], "side": r["side"], "TP": 0, "SL": 0, "EXPIRED": 0, "OPEN": 0})
                d[r["status"]] = r["n"]
            tot = {"TP": 0, "SL": 0, "EXPIRED": 0, "OPEN": 0}
            for d in per.values():
                d["win_rate"] = round(100 * d["TP"] / (d["TP"] + d["SL"]), 1) if d["TP"] + d["SL"] else None
                for kk in tot: tot[kk] += d[kk]
            tot["win_rate"] = round(100 * tot["TP"] / (tot["TP"] + tot["SL"]), 1) if tot["TP"] + tot["SL"] else None
            out[label] = {"total": tot, "rows": sorted(per.values(), key=lambda x: (TFS.index(x["tf"]), x["side"]))}
    return out

def stats_text():
    st = paper_stats(); lines = ["📈 Paper win-rate (80%+ signals, TP/SL from candles, expire 60 candles)", "Ye PAPER results hain, real trades nahi."]
    for label in ("24h", "7d"):
        t = st[label]["total"]; wr = f"{t['win_rate']}%" if t["win_rate"] is not None else "n/a"
        lines.append(f"\n{label}: win {wr} · TP {t['TP']} · SL {t['SL']} · expired {t['EXPIRED']} · open {t['OPEN']}")
        for r in st[label]["rows"]:
            wr = f"{r['win_rate']}%" if r["win_rate"] is not None else "n/a"
            lines.append(f"  {r['tf']:>3} {r['side']:<5} win {wr:>6} (TP {r['TP']}/SL {r['SL']}, exp {r['EXPIRED']}, open {r['OPEN']})")
    return "\n".join(lines)

async def refresh_funding():
    if STATE["source"] != "futures": return
    j = await md_get("premiumIndex", {})          # one bulk call (weight 10)
    STATE["funding"] = {x["symbol"]: float(x.get("lastFundingRate") or 0) for x in j if x["symbol"] in STATE["info"]}
    STATE["funding_ts"] = time.time()

async def tickers_loop():
    """Universe (hourly), 24h tickers (60s) and funding (5 min)."""
    last_tick = 0
    while True:
        STATE["hb"]["tickers"] = time.time()
        try:
            if not STATE["universe"] or time.time() - STATE["universe_ts"] > 3600: await refresh_universe(); last_tick = time.time()
            elif time.time() - last_tick >= 60: update_tickers(await md_get("ticker/24hr", {})); last_tick = time.time()
            if time.time() - STATE.get("funding_ts", 0) > 300: await refresh_funding()
        except asyncio.CancelledError: raise
        except Exception as e: add_err(f"ticker/universe/funding: {e}", "tickers")
        await asyncio.sleep(5 if not STATE["universe"] else 10)

async def kline_loop(tf):
    """One loop per timeframe, aligned to candle closes: fetches at boundary + CLOSE_LAG, every FETCH_EVERY[tf] s."""
    iv = FETCH_EVERY[tf]; name = f"kl_{tf}"
    while True:
        STATE["hb"][name] = time.time()
        if not STATE["universe"]:
            await asyncio.sleep(1); continue
        if any(KTS.get((x, tf)) for x in STATE["universe"][:10]):
            due = (time.time() - CLOSE_LAG) // iv * iv + iv + CLOSE_LAG
            while time.time() < due:
                await asyncio.sleep(max(0.05, min(5, due - time.time()))); STATE["hb"][name] = time.time()
        await fetch_tf(tf); STATE["hb"][name] = time.time()
        try: await post_scan(tf)
        except asyncio.CancelledError: raise
        except Exception as e: add_err(f"post_scan {tf}: {type(e).__name__}: {e}", "scanner")

async def post_scan(tf):
    if not KL: return
    async with POST_LOCK:
        s = get_settings(); t0 = time.time(); compute_all(s)
        if not STATE["ready"]:
            STATE["ready"] = all(STATE["coverage"].get(t, 0) >= 90 for t in ALL_TFS)
            prime_hot(); return                     # no alert burst for setups that were already hot at start
        await update_btc_guard(s)
        await record_and_alert(s)
        update_paper()
        STATE["scan"]["cycle_s"] = round(time.time() - t0, 3)
    if tf == "1m":
        async with TRADE_LOCK:
            await manage_positions(); await auto_trade()

def prime_hot():
    for tf in TFS:
        for sym, a in STATE["signals"].get(tf, {}).items():
            STATE["hot_prev"][(sym, tf)] = a["signal"] if is_hot(a) else None

async def live_loop():
    """Every 2 s: REST price fallback when the WebSocket is silent, then recompute signals with live prices."""
    while True:
        STATE["hb"]["live"] = time.time()
        try:
            ws_age = time.time() - STATE["ws"]["last_msg"]
            if ws_age > 5 and STATE["universe"]:
                j = await md_get("ticker/price", {}); now = time.time()
                for x in j:
                    if x.get("symbol") in STATE["info"]: LIVE[x["symbol"]] = (float(x["price"]), now)
                STATE["live_src"] = "rest"; STATE["rest_price_polls"] += 1
            elif ws_age <= 5: STATE["live_src"] = "ws"
            if KL and STATE["universe"] and not POST_LOCK.locked(): compute_all(get_settings())
        except asyncio.CancelledError: raise
        except Exception as e: add_err(f"live: {type(e).__name__}: {e}", "live")
        await asyncio.sleep(2)

async def ws_loop():
    """Binance !miniTicker@arr (all symbols, ~1 s). Futures /market/stream path, spot fallback; reconnect w/ backoff."""
    W = STATE["ws"]
    try: import websockets
    except ImportError:
        W["status"] = "websockets not installed (REST price fallback)"
        while True: STATE["hb"]["ws"] = time.time(); await asyncio.sleep(10)
    backoff = 1
    while True:
        STATE["hb"]["ws"] = time.time()
        url = WS_FUT if STATE["source"] == "futures" else WS_SPOT; W["url"] = url.split("?")[0]
        try:
            W["status"] = "connecting"
            async with websockets.connect(url, open_timeout=10, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=2 ** 23) as ws:
                W["status"] = "connected"; W["since"] = time.time(); backoff = 1
                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    now = time.time(); d = json.loads(msg)
                    if isinstance(d, dict): d = d.get("data", d)
                    if isinstance(d, list):
                        for x in d:
                            sym = x.get("s")
                            if sym in STATE["info"]: LIVE[sym] = (float(x["c"]), now)
                    W["last_msg"] = now; W["msgs"] += 1; STATE["hb"]["ws"] = now
        except asyncio.CancelledError: raise
        except Exception as e:
            add_err(f"ws: {type(e).__name__}: {str(e)[:120]}", "ws")
        finally:
            if W.get("since"): W["up_s"] += time.time() - W["since"]; W["since"] = None
            W["status"] = "disconnected"
        W["reconnects"] += 1
        await asyncio.sleep(backoff); backoff = min(backoff * 2, 60)

def ws_uptime_pct():
    W = STATE["ws"]; now = time.time(); up = W["up_s"] + ((now - W["since"]) if W.get("since") else 0)
    return round(100 * up / max(1, now - W["start"]), 2)

# ---------------- health monitor / self-healing ----------------
WATCH = {}   # name -> (factory, max silence s) ; filled in lifespan
TASKS = {}

def start_task(name):
    TASKS[name] = asyncio.create_task(WATCH[name][0]()); STATE["hb"][name] = time.time()

def health_conditions(now):
    c = {}
    if now - STATE["start"] < 120: return c
    for tf in ALL_TFS:
        cov = STATE["coverage"].get(tf, 0); ages = [now - KTS[(x, tf)] for x in STATE["universe"] if (x, tf) in KTS]
        mx = max(ages) if ages else 1e9
        c[f"data {tf}"] = (f"coverage {cov}% · max age {mx:.0f}s" if cov < 95 or mx > STALE_MULT * FETCH_EVERY[tf] else None)
    W = STATE["ws"]
    c["websocket"] = (f"{W['status']} (REST price fallback active)" if now - W["last_msg"] > 60 else None)
    if not DISABLE_BOT:
        c["telegram"] = (f"poller silent {now - STATE['last_poll']:.0f}s" if now - STATE["last_poll"] > 120 or not STATE["poll_ok"] else None)
    try: kv_set("_hb", now); c["database"] = None
    except Exception as e: c["database"] = f"{type(e).__name__}: {e}"
    c["rate limit"] = (f"Binance backoff {STATE['backoff_until'] - now:.0f}s" if STATE["backoff_until"] > now else None)
    return c

async def process_incidents(conds, now):
    inc = STATE["incidents"]
    for name, msg in conds.items():
        cur = inc.get(name)
        if msg:
            if not cur: inc[name] = cur = {"since": now, "msg": msg, "notified": False}
            cur["msg"] = msg
            if (not cur["notified"] and now - cur["since"] >= 30 and now - STATE["inc_last_notify"].get(name, 0) > INC_COOLDOWN):
                cur["notified"] = True; STATE["inc_last_notify"][name] = now
                await notify(f"⚠️ Health alert: {name} — {msg}\nBot khud recover karne ki koshish kar raha hai. /health")
        elif cur:
            inc.pop(name)
            STATE["inc_log"] = ([{"name": name, "since": cur["since"], "end": now, "msg": cur["msg"]}] + STATE["inc_log"])[:20]
            if cur["notified"]: await notify(f"✅ Recovered: {name} ({int(now - cur['since'])}s ke baad theek)")

async def watchdog():
    while True:
        await asyncio.sleep(10); now = time.time()
        try:
            for name, (fac, lim) in WATCH.items():
                t = TASKS.get(name); hb = STATE["hb"].get(name, now)
                dead = t is None or t.done(); silent = now - hb > lim
                if not (dead or silent): continue
                why = "silent %ds" % (now - hb)
                if t is not None and t.done():
                    exc = None if t.cancelled() else t.exception()
                    why = f"crashed: {type(exc).__name__}: {exc}" if exc else "stopped"
                if t is not None and not t.done():
                    t.cancel(); await asyncio.wait([t], timeout=5)    # fully stop before restarting (single poller)
                STATE["restarts"][name] = STATE["restarts"].get(name, 0) + 1
                add_err(f"watchdog: restarted {name} ({why})", "watchdog"); start_task(name)
            await process_incidents(health_conditions(now), now)
        except asyncio.CancelledError: raise
        except Exception as e: add_err(f"watchdog: {type(e).__name__}: {e}", "watchdog")

def fmt_age(x): return "n/a" if x is None else (f"{x:.0f}s" if x < 120 else f"{x / 60:.1f}m")

def health_data():
    now = time.time(); uni = STATE["universe"]; tfs = {}
    for tf in ALL_TFS:
        ages = sorted(now - KTS[(x, tf)] for x in uni if (x, tf) in KTS); ct = STATE["cov_tot"].get(tf, [0, 0])
        tfs[tf] = {"interval_s": FETCH_EVERY[tf], "coverage_last": STATE["coverage"].get(tf), "coverage_min": STATE["cov_min"].get(tf),
                   "coverage_avg": round(100 * ct[0] / ct[1], 2) if ct[1] else None, "max_age_s": round(ages[-1], 1) if ages else None,
                   "median_age_s": round(ages[len(ages) // 2], 1) if ages else None, "max_age_seen_s": STATE["max_age_seen"].get(tf),
                   "have": len(ages), "short_history": STATE["short_hist"].get(tf, 0), "fetch_s": STATE["scan"].get(f"fetch_{tf}_s"),
                   "stale_signals": sum(1 for a in STATE["signals"].get(tf, {}).values() if a.get("stale")) if tf in TFS else None}
    W = STATE["ws"]; live_fresh = sum(1 for x in uni if best_price(x, now) is not None)
    return {"uptime_s": round(now - STATE["start"]), "source": STATE["source"], "coins": len(uni), "ready": STATE["ready"], "tf": tfs,
            "ws": {"status": W["status"], "url": W["url"], "last_msg_age_s": round(now - W["last_msg"], 1) if W["last_msg"] else None,
                   "uptime_pct": ws_uptime_pct(), "reconnects": W["reconnects"], "msgs": W["msgs"]},
            "live_price": {"source": STATE["live_src"], "fresh_coins": live_fresh, "rest_polls": STATE["rest_price_polls"]},
            "rate": {"weight": STATE["weight"], "peak": STATE["weight_peak"], "limit": weight_limit(), "hits_429_418": STATE["rate_limit_hits"],
                     "backoff_s": max(0, round(STATE["backoff_until"] - now)), "requests": STATE["requests"], "retries": STATE["retries"],
                     "align_retries": STATE["align_retries"]},
            "telegram": {"poll_ok": STATE["poll_ok"], "last_poll_age_s": round(now - STATE["last_poll"], 1) if STATE["last_poll"] else None,
                         "disabled": DISABLE_BOT},
            "tasks": {n: {"alive": bool(TASKS.get(n)) and not TASKS[n].done(), "hb_age_s": round(now - STATE["hb"].get(n, now), 1),
                          "restarts": STATE["restarts"].get(n, 0)} for n in WATCH},
            "errors": {"total": sum(STATE["err_count"].values()), "by_type": STATE["err_count"], "fetch_errors": STATE["fetch_errors"],
                       "recent": STATE["errors"][:8]},
            "candle_gaps": len(STATE["gaps"]), "incidents": [{"name": k, **v} for k, v in STATE["incidents"].items()],
            "incident_log": STATE["inc_log"][:10], "scan": STATE["scan"]}

def health_text():
    h = health_data(); w = h["ws"]; r = h["rate"]
    lines = [f"🩺 Health · uptime {fmt_age(h['uptime_s'])} · {h['coins']} coins ({h['source']})"]
    for tf, d in h["tf"].items():
        lines.append(f"{tf:>3}: age max {fmt_age(d['max_age_s'])} (limit {STALE_MULT * d['interval_s']}s) · coverage {d['coverage_last']}% "
                     f"(min {d['coverage_min']}%) · fetch {d['fetch_s']}s" + (f" · stale {d['stale_signals']}" if d["stale_signals"] else ""))
    lines.append(f"WS: {w['status']} · last msg {fmt_age(w['last_msg_age_s'])} · uptime {w['uptime_pct']}% · reconnects {w['reconnects']}")
    lines.append(f"Live price: {h['live_price']['source']} · {h['live_price']['fresh_coins']}/{h['coins']} fresh")
    lines.append(f"Rate: weight {r['weight']}/{r['limit']} (peak {r['peak']}) · 429/418 {r['hits_429_418']} · retries {r['retries']}")
    lines.append(f"Telegram poll: {'disabled' if h['telegram']['disabled'] else 'OK' if h['telegram']['poll_ok'] else 'FAIL'} · errors {h['errors']['total']} · restarts {sum(t['restarts'] for t in h['tasks'].values())}")
    if h["incidents"]: lines.append("⚠️ Open: " + "; ".join(f"{i['name']}: {i['msg']}" for i in h["incidents"]))
    else: lines.append("✅ Koi open incident nahi")
    return "\n".join(lines)

def mood_text():
    m = STATE["mood"]
    if not m: return "Market data abhi load ho raha hai."
    lines = [f"🌡 Market mood: {m['label']}"]
    for tf, b in m["btc"].items(): lines.append(f"BTC {tf}: {b['label']} (trend {b['trend']:+.2f})")
    if m.get("btc_price"): lines.append(f"BTC {m['btc_price']:.1f} · 5m move {m['btc_move_5m']:+.2f}%" if m["btc_move_5m"] is not None else f"BTC {m['btc_price']:.1f}")
    for tf, d in m["tf"].items(): lines.append(f"{tf}: LONG {d['long_pct']}% · SHORT {d['short_pct']}% · neutral {d['neutral_pct']}% ({d['n']} coins)")
    g = m["guard"]; p = m["paused"]
    lines.append(f"BTC guard {'ON' if g['on'] else 'OFF'}: ±{g['pct']}% in 5m → {g['min']} min pause")
    lines.append("⏸ Paused: " + (", ".join(f"{k} ({v // 60}m {v % 60}s)" for k, v in p.items() if v) or "none"))
    return "\n".join(lines)

def strong_list(limit=20):
    out = []
    for sym, a in STATE["signals"].get("5m", {}).items():
        if a.get("strong") and not a.get("filtered"):
            confs = {tf: STATE["signals"][tf][sym]["confidence"] for tf in TFS if sym in STATE["signals"].get(tf, {})}
            out.append((sym, a, confs))
    out.sort(key=lambda x: -sum(x[2].values()))
    return out[:limit]

def strong_text():
    lst = strong_list()
    lines = [f"🔥 STRONG confluence — 1m + 5m + 15m same direction (har TF {get_settings()['strong_min_conf']}%+), 1h opposite nahi", ""]
    if not lst: lines.append("Abhi koi STRONG coin nahi.")
    for sym, a, confs in lst:
        lines.append(f"{'🟢' if a['signal'] == 'LONG' else '🔴'} {a['signal']} {sym} · " + " · ".join(f"{t} {c}%" for t, c in confs.items())
                     + (" ⏸" if a.get("paused") else ""))
        lines.append(f"   5m: E {fmt_price(sym, a['entry'])} | TP {fmt_price(sym, a['tp'])} | SL {fmt_price(sym, a['sl'])}")
    return "\n".join(lines)

def q_step(val, step, rounding=ROUND_DOWN):
    """Snap to an exchange step/tick with Decimal math (no float artefacts). Quantities round down, prices to nearest."""
    st = Decimal(str(step)).normalize()
    q = (Decimal(str(val)) / st).to_integral_value(rounding) * st
    return format(q.quantize(st) if st.as_tuple().exponent < 0 else q.quantize(Decimal(1)), "f")

def fmt_price(sym, p):
    t = STATE["info"].get(sym, {}).get("tick")
    return q_step(p, t, ROUND_HALF_UP) if t else f"{p:.6g}"

def q_price(p, tick): return q_step(p, tick, ROUND_HALF_UP)

# ---------------- Binance signed ----------------
def _sign(params):
    p = {k: v for k, v in params.items() if v is not None}; p["timestamp"] = int(time.time() * 1000); p["recvWindow"] = 10000
    q = urllib.parse.urlencode(p)
    return q + "&signature=" + hmac.new(BN_SECRET.encode(), q.encode(), hashlib.sha256).hexdigest()

async def signed(method, path, params=None):
    """Spot/SAPI signed request. Returns (status, json)."""
    last = (0, {"msg": "no host reachable"})
    for host in PRIVATE_HOSTS:
        try:
            r = await client.request(method, f"{host}{path}?{_sign(params or {})}", headers={"X-MBX-APIKEY": BN_KEY}, timeout=15)
            try: j = r.json()
            except Exception: j = {"msg": r.text[:300]}
            if r.status_code in (451, 403):
                last = (r.status_code, {"msg": f"{host}: HTTP {r.status_code} (geo-block / forbidden) {str(j)[:200]}"}); continue
            return r.status_code, (j if isinstance(j, dict) else {"data": j})
        except Exception as e:
            last = (0, {"msg": f"{host}: {type(e).__name__}: {e}"})
    return last

async def fsigned(method, path, params=None):
    """USDT-M futures signed request. Returns (status, json)."""
    try:
        r = await client.request(method, f"{FAPI}{path}?{_sign(params or {})}", headers={"X-MBX-APIKEY": BN_KEY}, timeout=15)
        try: j = r.json()
        except Exception: j = {"msg": r.text[:300]}
        return r.status_code, (j if isinstance(j, dict) else {"data": j})
    except Exception as e:
        return 0, {"msg": f"fapi: {type(e).__name__}: {e}"}

async def check_key():
    res = {"ts": time.time()}
    st, j = await signed("GET", "/api/v3/account", {"omitZeroBalances": "true"})
    res["account_ok"] = st == 200
    if st != 200: res["account_error"] = j
    st, j = await signed("GET", "/sapi/v1/account/apiRestrictions")
    if st == 200:
        res.update(spot_trading_enabled=j.get("enableSpotAndMarginTrading"), futures_enabled=j.get("enableFutures"),
                   withdrawals_enabled=j.get("enableWithdrawals"), ip_restricted=j.get("ipRestrict"), reading_enabled=j.get("enableReading"))
    else: res["restrictions_error"] = {"http": st, **j}
    st, j = await signed("POST", "/api/v3/order/test", {"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quoteOrderQty": "10"})
    res["order_test_ok"] = st == 200
    if st != 200: res["order_test_error"] = {"http": st, **j}
    st, j = await fsigned("GET", "/fapi/v2/balance")
    res["futures_read_ok"] = st == 200
    if st != 200: res["futures_read_error"] = {"http": st, **j}
    st, j = await fsigned("POST", "/fapi/v1/order/test", {"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": "0.002"})
    res["futures_order_test_ok"] = st == 200
    if st != 200: res["futures_order_test_error"] = {"http": st, **j}
    STATE["key_check"] = res; return res

def can_trade(market):
    kc = STATE.get("key_check") or {}
    return bool(kc.get("futures_order_test_ok")) if market == "futures" else bool(kc.get("order_test_ok"))

# ---------------- trading ----------------
def open_trades(symbol=None):
    with db() as c:
        if symbol: return [dict(r) for r in c.execute("SELECT * FROM trades WHERE status='OPEN' AND symbol=?", (symbol,))]
        return [dict(r) for r in c.execute("SELECT * FROM trades WHERE status='OPEN'")]

def today_pnl():
    start = time.time() - (time.time() % 86400)
    with db() as c:
        r = c.execute("SELECT COALESCE(SUM(pnl),0) p FROM trades WHERE closed_ts>=? AND pnl IS NOT NULL", (start,)).fetchone()
    return float(r["p"])

def trades_today():
    start = time.time() - (time.time() % 86400)
    with db() as c:
        return c.execute("SELECT COUNT(*) n FROM trades WHERE ts>=? AND status!='FAILED'", (start,)).fetchone()["n"]

def risk_status(s=None):
    s = s or get_settings(); pnl = today_pnl(); n_open = len(open_trades()); n_day = trades_today()
    blocks = []
    if pnl <= -s["daily_loss_limit_usdt"]: blocks.append(f"daily loss {pnl:.2f}/-{s['daily_loss_limit_usdt']} USDT")
    if n_open >= s["max_open_positions"]: blocks.append(f"open trades {n_open}/{s['max_open_positions']}")
    if n_day >= s["max_trades_per_day"]: blocks.append(f"trades today {n_day}/{s['max_trades_per_day']}")
    return {"today_pnl": round(pnl, 4), "daily_loss_limit": s["daily_loss_limit_usdt"], "open": n_open, "max_open": s["max_open_positions"],
            "trades_today": n_day, "max_trades_per_day": s["max_trades_per_day"], "blocked": bool(blocks), "reasons": blocks}

def risk_text():
    r = risk_status()
    return ("🛡 Risk guard (UTC din)\n"
            f"Aaj ka PnL: {r['today_pnl']:+.2f} USDT (limit -{r['daily_loss_limit']})\n"
            f"Open trades: {r['open']}/{r['max_open']}\nAaj ke trades: {r['trades_today']}/{r['max_trades_per_day']}\n"
            + (("⛔ Naye trades BLOCK: " + "; ".join(r["reasons"])) if r["blocked"] else "✅ Naye trades allowed"))

def log_trade(**kw):
    kw.setdefault("ts", time.time()); cols = ",".join(kw); qs = ",".join("?" * len(kw))
    with db() as c:
        return c.execute(f"INSERT INTO trades({cols}) VALUES({qs})", list(kw.values())).lastrowid

def upd_trade(tid, **kw):
    with db() as c:
        c.execute(f"UPDATE trades SET {','.join(k + '=?' for k in kw)} WHERE id=?", [*kw.values(), tid])

def fill_price(j):
    q = float(j.get("executedQty") or 0); qq = float(j.get("cummulativeQuoteQty") or j.get("cumQuote") or 0)
    avg = float(j.get("avgPrice") or 0) or (qq / q if q else 0)
    return avg, q, qq

def errtxt(st, j): return f"Binance {st}: {j.get('code', '')} {j.get('msg', '')}".strip()

async def spot_filters(sym):
    if sym in STATE["spot_filters"]: return STATE["spot_filters"][sym]
    st, j = 0, {}
    for host in SPOT_PUBLIC:
        try:
            r = await client.get(host + "/api/v3/exchangeInfo", params={"symbol": sym}, timeout=10)
            if r.status_code == 200: j = r.json(); st = 200; break
        except Exception: pass
    if st != 200 or not j.get("symbols"): return None
    x = j["symbols"][0]; f = {y["filterType"]: y for y in x["filters"]}
    out = {"base": x["baseAsset"], "step": f["LOT_SIZE"]["stepSize"], "tick": f["PRICE_FILTER"]["tickSize"],
           "min_notional": float((f.get("NOTIONAL") or f.get("MIN_NOTIONAL") or {}).get("minNotional", 5))}
    STATE["spot_filters"][sym] = out; return out

async def execute(sym, direction, mode="manual", tf=None):
    """direction LONG/SHORT. If an opposite position is open on sym, this closes it instead. Caller holds TRADE_LOCK."""
    s = get_settings(); tf = tf if tf in TFS else s["timeframe"]; market = s["market"]
    sig = STATE["signals"].get(tf, {}).get(sym)
    if not sig: return {"ok": False, "error": "No signal data for this symbol yet"}
    if sig.get("stale"): return {"ok": False, "error": f"{sym} {tf} data is stale (refresh pending) — try again in a few seconds"}
    ots = open_trades(sym)
    if ots:
        t = ots[0]
        if t["side"] == direction: return {"ok": False, "error": f"Already have an open {direction} on {sym}"}
        return await close_trade(t, f"{mode}: opposite signal")
    r = risk_status(s)
    if r["blocked"]: return {"ok": False, "error": "Risk guard: " + "; ".join(r["reasons"]) + " (resets 00:00 UTC / when trades close)"}
    if market == "futures": return await futures_open(sym, direction, sig, s, mode)
    if direction == "SHORT": return {"ok": False, "error": "Shorts need Futures mode (Settings > Market: Futures)"}
    return await spot_buy(sym, sig, s, mode)

async def spot_buy(sym, sig, s, mode):
    f = await spot_filters(sym)
    if not f: return {"ok": False, "error": f"{sym} is not traded on Binance spot"}
    st, j = await signed("POST", "/api/v3/order", {"symbol": sym, "side": "BUY", "type": "MARKET",
                                                    "quoteOrderQty": f"{s['order_size_usdt']:.2f}", "newOrderRespType": "FULL"})
    if st != 200:
        log_trade(symbol=sym, side="LONG", mode=mode, tf=s["timeframe"], market="spot", status="FAILED", error=errtxt(st, j))
        return {"ok": False, "error": errtxt(st, j)}
    px, qty, quote = fill_price(j)
    tp, sl = px + (sig["tp"] - sig["entry"]), px - (sig["entry"] - sig["sl"])
    sell_qty = q_step(qty * 0.999, f["step"])
    tid = log_trade(symbol=sym, side="LONG", mode=mode, tf=s["timeframe"], market="spot", leverage=1, qty=float(sell_qty), price=px,
                    quote=quote, tp=tp, sl=sl, atr=sig.get("atr"), status="OPEN", order_id=str(j.get("orderId")))
    st2, j2 = await signed("POST", "/api/v3/orderList/oco", {
        "symbol": sym, "side": "SELL", "quantity": sell_qty, "aboveType": "LIMIT_MAKER", "abovePrice": q_price(tp, f["tick"]),
        "belowType": "STOP_LOSS_LIMIT", "belowStopPrice": q_price(sl, f["tick"]), "belowPrice": q_price(sl * 0.998, f["tick"]),
        "belowTimeInForce": "GTC"})
    if st2 == 200:
        upd_trade(tid, oco_id=str(j2.get("orderListId")))
        return {"ok": True, "msg": f"Spot LONG {sell_qty} {sym} @ {px:.6g}. OCO TP {q_step(tp, f['tick'])} / SL {q_step(sl, f['tick'])}"}
    upd_trade(tid, error=f"OCO failed ({j2.get('msg', '')}); tracked exits active")
    return {"ok": True, "msg": f"Spot LONG {qty} {sym} @ {px:.6g}. OCO failed, bot tracks TP/SL itself."}

async def futures_open(sym, direction, sig, s, mode):
    meta = STATE["info"].get(sym) if STATE["source"] == "futures" else None
    if not meta: return {"ok": False, "error": f"{sym} futures metadata unavailable"}
    lev = s["leverage"]; side = "BUY" if direction == "LONG" else "SELL"; exit_side = "SELL" if side == "BUY" else "BUY"
    st, j = await fsigned("POST", "/fapi/v1/marginType", {"symbol": sym, "marginType": "ISOLATED"})
    if st != 200 and j.get("code") != -4046: return {"ok": False, "error": "Set isolated margin failed: " + errtxt(st, j)}
    st, j = await fsigned("POST", "/fapi/v1/leverage", {"symbol": sym, "leverage": lev})
    if st != 200: return {"ok": False, "error": "Set leverage failed: " + errtxt(st, j)}
    price = sig["price"]; notional = s["order_size_usdt"] * lev
    qty = q_step(notional / price, meta["step"])
    if float(qty) <= 0 or float(qty) * price < meta["min_notional"]:
        return {"ok": False, "error": f"Order too small: {notional:.2f} USDT notional < {sym} minimum {meta['min_notional']:.0f} USDT. Raise order size or leverage."}
    st, j = await fsigned("POST", "/fapi/v1/order", {"symbol": sym, "side": side, "type": "MARKET", "quantity": qty, "newOrderRespType": "RESULT"})
    if st != 200:
        log_trade(symbol=sym, side=direction, mode=mode, tf=s["timeframe"], market="futures", leverage=lev, status="FAILED", error=errtxt(st, j))
        return {"ok": False, "error": errtxt(st, j)}
    px, q, quote = fill_price(j); px = px or price; q = q or float(qty)
    d = 1 if direction == "LONG" else -1
    tp, sl = px + d * abs(sig["tp"] - sig["entry"]), px - d * abs(sig["entry"] - sig["sl"])
    tid = log_trade(symbol=sym, side=direction, mode=mode, tf=s["timeframe"], market="futures", leverage=lev, qty=q, price=px,
                    quote=q * px, tp=tp, sl=sl, atr=sig.get("atr"), status="OPEN", order_id=str(j.get("orderId")))
    ids, errs = {}, []
    for kind, typ, trig in (("tp_algo", "TAKE_PROFIT_MARKET", tp), ("sl_algo", "STOP_MARKET", sl)):
        st2, j2 = await fsigned("POST", "/fapi/v1/algoOrder", {"algoType": "CONDITIONAL", "symbol": sym, "side": exit_side, "type": typ,
                               "triggerPrice": q_price(trig, meta["tick"]), "closePosition": "true", "workingType": "MARK_PRICE",
                               "priceProtect": "true"})
        if st2 == 200: ids[kind] = str(j2.get("algoId"))
        else: errs.append(f"{typ}: {j2.get('msg', st2)}")
    upd_trade(tid, **ids, **({"error": "; ".join(errs) + " (tracked exits active)"} if errs else {}))
    msg = f"Futures {direction} {q} {sym} @ {px:.6g} ({lev}x isolated). TP {q_step(tp, meta['tick'])} / SL {q_step(sl, meta['tick'])}"
    return {"ok": True, "msg": msg + (" — exchange TP/SL failed, bot tracks exits" if errs else "")}

async def close_trade(t, reason):
    sym = t["symbol"]
    if t.get("market") == "futures":
        for k in ("tp_algo", "sl_algo"):
            if t.get(k): await fsigned("DELETE", "/fapi/v1/algoOrder", {"algoId": t[k]})
        side = "SELL" if t["side"] == "LONG" else "BUY"
        st, j = await fsigned("POST", "/fapi/v1/order", {"symbol": sym, "side": side, "type": "MARKET",
                              "quantity": q_step(t["qty"], STATE["info"].get(sym, {}).get("step", "1")), "reduceOnly": "true",
                              "newOrderRespType": "RESULT"})
    else:
        if t.get("oco_id"): await signed("DELETE", "/api/v3/orderList", {"symbol": sym, "orderListId": t["oco_id"]})
        f = await spot_filters(sym) or {"step": "0.00001"}
        st, j = await signed("POST", "/api/v3/order", {"symbol": sym, "side": "SELL", "type": "MARKET", "quantity": q_step(t["qty"], f["step"])})
    if st != 200:
        upd_trade(t["id"], error=f"close failed: {j.get('msg')}"); return {"ok": False, "error": errtxt(st, j)}
    px, q, _ = fill_price(j); px = px or t["price"]
    d = 1 if t["side"] == "LONG" else -1
    pnl = (px - t["price"]) * (q or t["qty"]) * d
    upd_trade(t["id"], status="CLOSED", exit_price=px, pnl=pnl, closed_ts=time.time(), error=reason)
    await notify(f"Closed {t['side']} {sym} ({reason}) @ {px:.6g}, PnL {pnl:+.3f} USDT")
    return {"ok": True, "msg": f"Closed {t['side']} {sym} @ {px:.6g}, PnL {pnl:+.3f} USDT"}

def last_price(sym):
    lp = best_price(sym)
    if lp: return lp
    k = KL.get((sym, "1m")); return k[3][-1] if k else None

async def trail_stop(t, px, s):
    """Breakeven then ATR trailing. Updates the exchange stop (futures algo order) or switches to bot-managed exits."""
    a = t.get("atr") or 0
    if not s["trailing"] or not px or a <= 0: return t
    d = 1 if t["side"] == "LONG" else -1; entry = t["price"]; sl = t["sl"]; new_sl = None
    gain = (px - entry) * d
    trig = min(s["be_trigger_atr"] * a, 0.5 * abs(t["tp"] - entry))
    if not t.get("be_done") and gain >= trig and (entry - sl) * d > 0:
        new_sl = entry; t["be_done"] = 1
    if t.get("be_done"):
        cand = px - d * s["trail_atr"] * a
        if (cand - (new_sl if new_sl is not None else sl)) * d > 0.2 * a: new_sl = cand
    if new_sl is None or (new_sl - sl) * d <= 0: return t
    if time.time() - (t.get("sl_moved_ts") or 0) < 30 and new_sl != entry: return t
    sym = t["symbol"]; upd = {"sl": new_sl, "be_done": 1, "sl_moved_ts": time.time()}
    if t.get("market") == "futures" and t.get("sl_algo"):
        meta = STATE["info"].get(sym, {})
        st, j = await fsigned("POST", "/fapi/v1/algoOrder", {"algoType": "CONDITIONAL", "symbol": sym, "side": "SELL" if d == 1 else "BUY",
                              "type": "STOP_MARKET", "triggerPrice": q_price(new_sl, meta.get("tick", "0.0001")), "closePosition": "true",
                              "workingType": "MARK_PRICE", "priceProtect": "true"})
        if st == 200:
            await fsigned("DELETE", "/fapi/v1/algoOrder", {"algoId": t["sl_algo"]}); upd["sl_algo"] = str(j.get("algoId"))
        else:
            upd["error"] = f"trail update failed ({j.get('msg')}); bot-managed SL"   # old exchange SL stays as a backstop
    elif t.get("market") != "futures" and t.get("oco_id"):
        await signed("DELETE", "/api/v3/orderList", {"symbol": sym, "orderListId": t["oco_id"]}); upd["oco_id"] = None
        upd["error"] = "OCO replaced by bot-managed trailing exits"
    upd_trade(t["id"], **upd); t.update(upd)
    if new_sl == entry: await notify(f"🔒 {t['side']} {sym}: SL breakeven par (entry {entry:.6g})")
    return t

async def manage_positions():
    s = get_settings()
    for t in open_trades():
        sym = t["symbol"]; px = last_price(sym)
        try: t = await trail_stop(t, px, s)
        except Exception as e: add_err(f"trail {sym}: {type(e).__name__}: {e}")
        if t.get("market") == "futures":
            st, j = await fsigned("GET", "/fapi/v3/positionRisk", {"symbol": sym})
            if st == 200:
                rows = j.get("data", []); amt = sum(abs(float(r.get("positionAmt", 0))) for r in rows)
                if amt == 0:
                    st2, inc = await fsigned("GET", "/fapi/v1/income", {"symbol": sym, "incomeType": "REALIZED_PNL", "startTime": int(t["ts"] * 1000)})
                    pnl = sum(float(x.get("income", 0)) for x in inc.get("data", [])) if st2 == 200 else None
                    for k in ("tp_algo", "sl_algo"):
                        if t.get(k): await fsigned("DELETE", "/fapi/v1/algoOrder", {"algoId": t[k]})
                    upd_trade(t["id"], status="CLOSED", exit_price=px, pnl=pnl, closed_ts=time.time(), error="exchange TP/SL hit")
                    await notify(f"{t['side']} {sym} closed by TP/SL. Realized PnL {pnl if pnl is None else f'{pnl:+.3f}'} USDT")
                    continue
            if px and (not (t.get("tp_algo") and t.get("sl_algo")) or "bot-managed" in (t.get("error") or "")):
                d = 1 if t["side"] == "LONG" else -1
                if (px - t["tp"]) * d >= 0: await close_trade(t, "take-profit (tracked)")
                elif (px - t["sl"]) * d <= 0: await close_trade(t, "stop-loss (tracked)")
        else:
            if t.get("oco_id"):
                st, j = await signed("GET", "/api/v3/orderList", {"orderListId": t["oco_id"]})
                if st == 200 and j.get("listOrderStatus") == "ALL_DONE":
                    exit_px = px or t["price"]
                    for o in j.get("orders", []):
                        st3, oj = await signed("GET", "/api/v3/order", {"symbol": sym, "orderId": o["orderId"]})
                        if st3 == 200 and oj.get("status") == "FILLED": exit_px = fill_price(oj)[0] or exit_px
                    pnl = (exit_px - t["price"]) * t["qty"]
                    upd_trade(t["id"], status="CLOSED", exit_price=exit_px, pnl=pnl, closed_ts=time.time(), error="OCO filled")
                    await notify(f"{sym} OCO exit @ {exit_px:.6g}, PnL {pnl:+.3f} USDT")
            elif px:
                if px >= t["tp"]: await close_trade(t, "take-profit (tracked)")
                elif px <= t["sl"]: await close_trade(t, "stop-loss (tracked)")

async def auto_trade():
    s = get_settings()
    if not s["auto_trade"]: return
    if not can_trade(s["market"]):
        save_settings({"auto_trade": False}); await notify("Auto Trade switched OFF: API key has no trading permission for " + s["market"]); return
    if today_pnl() <= -s["daily_loss_limit_usdt"]:
        save_settings({"auto_trade": False}); await notify("Daily loss limit hit. Auto Trade switched OFF."); return
    if trades_today() >= s["max_trades_per_day"]: return
    for sym, a in sorted_signals(s["timeframe"]):
        if not is_hot(a) or a["confidence"] < s["auto_min_confidence"] or a.get("paused"): continue
        ots = open_trades(sym)
        if ots and ots[0]["side"] == a["signal"]: continue
        if not ots and len(open_trades()) >= s["max_open_positions"]: continue
        if a["signal"] == "SHORT" and s["market"] == "spot" and not ots: continue
        r = await execute(sym, a["signal"], "auto")
        await notify((f"Auto {a['signal']} {sym}: " + r.get("msg", "")) if r["ok"] else f"Auto {a['signal']} {sym} failed: {r['error']}")
        if not r["ok"] and "-2015" in r.get("error", ""):
            save_settings({"auto_trade": False}); await notify("Auto Trade switched OFF: API key has no trading permission."); return

# ---------------- PnL dashboard ----------------
async def refresh_exchange_pnl(force=False):
    """Futures realized PnL / fees / funding for the last 7 days from Binance income history (cached 120 s)."""
    ex = STATE.get("ex_pnl")
    if not BN_KEY or (ex and not force and time.time() - ex["ts"] < 120): return ex
    out = {"ts": time.time(), "ok": False}
    try:
        tot = {}
        for typ in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE"):
            st, j = await fsigned("GET", "/fapi/v1/income", {"incomeType": typ, "startTime": int((time.time() - 7 * 86400) * 1000), "limit": 1000})
            if st != 200: out["error"] = errtxt(st, j); break
            tot[typ] = round(sum(float(x.get("income", 0)) for x in j.get("data", [])), 4)
        else:
            out.update(ok=True, realized_7d=tot["REALIZED_PNL"], fees_7d=tot["COMMISSION"], funding_7d=tot["FUNDING_FEE"],
                       net_7d=round(sum(tot.values()), 4))
    except Exception as e: out["error"] = f"{type(e).__name__}"
    STATE["ex_pnl"] = out; return out

def pnl_data():
    now = time.time(); day0 = now - now % 86400
    with db() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM trades WHERE status IN ('OPEN','CLOSED') ORDER BY id DESC")]
    def agg(since):
        cl = [r for r in rows if r["status"] == "CLOSED" and r["pnl"] is not None and (r["closed_ts"] or 0) >= since]
        w = sum(1 for r in cl if r["pnl"] > 0); l = sum(1 for r in cl if r["pnl"] < 0)
        return {"realized": round(sum(r["pnl"] for r in cl), 4), "trades": len(cl), "wins": w, "losses": l,
                "win_rate": round(100 * w / (w + l), 1) if w + l else None,
                "best": round(max((r["pnl"] for r in cl), default=0), 4), "worst": round(min((r["pnl"] for r in cl), default=0), 4)}
    opens = []
    for r in rows:
        if r["status"] != "OPEN": continue
        px = last_price(r["symbol"]); d = 1 if r["side"] == "LONG" else -1
        u = (px - r["price"]) * r["qty"] * d if px and r["price"] and r["qty"] else None
        margin = (r["quote"] or 0) / max(1, r["leverage"] or 1)
        opens.append({"id": r["id"], "symbol": r["symbol"], "side": r["side"], "market": r.get("market"), "qty": r["qty"], "entry": r["price"],
                      "price": px, "tp": r["tp"], "sl": r["sl"], "unrealized": round(u, 4) if u is not None else None,
                      "roe_pct": round(100 * u / margin, 2) if u is not None and margin else None, "ts": r["ts"]})
    closed = [{"id": r["id"], "symbol": r["symbol"], "side": r["side"], "entry": r["price"], "exit": r["exit_price"], "pnl": r["pnl"],
               "ts": r["ts"], "closed_ts": r["closed_ts"], "reason": r["error"], "mode": r["mode"]} for r in rows if r["status"] == "CLOSED"][:40]
    return {"today": agg(day0), "7d": agg(now - 7 * 86400), "all": agg(0),
            "unrealized": round(sum(o["unrealized"] or 0 for o in opens), 4), "open": opens, "closed": closed, "exchange": STATE.get("ex_pnl"),
            "note": "Bot PnL = fill price difference x qty (fees/funding excluded). Exchange figures include fees & funding."}

def pnl_text():
    p = pnl_data(); lines = ["💵 PnL (USDT, bot trade history)"]
    for k, lbl in (("today", "Aaj (UTC)"), ("7d", "7 din"), ("all", "All time")):
        a = p[k]; wr = f"{a['win_rate']}%" if a["win_rate"] is not None else "n/a"
        lines.append(f"{lbl}: {a['realized']:+.3f} · {a['trades']} trades · W {a['wins']} / L {a['losses']} · win {wr}")
    lines.append(f"Unrealized (open {len(p['open'])}): {p['unrealized']:+.3f}")
    for o in p["open"][:10]:
        lines.append(f"  {o['side']} {o['symbol']} @ {o['entry']:.6g} → {o['price'] or 0:.6g} · " + (f"{o['unrealized']:+.3f}" if o["unrealized"] is not None else "n/a"))
    ex = p["exchange"]
    if ex and ex.get("ok"): lines.append(f"Binance futures 7d: realized {ex['realized_7d']:+.3f} · fees {ex['fees_7d']:+.3f} · funding {ex['funding_7d']:+.3f} · net {ex['net_7d']:+.3f}")
    elif ex: lines.append(f"Binance futures income: n/a ({ex.get('error', '')[:80]})")
    lines.append("Bot PnL mein fees/funding shamil nahi.")
    return "\n".join(lines)

# ---------------- Telegram ----------------
TG = f"https://api.telegram.org/bot{TG_TOKEN}"

async def tg(method, **params):
    try:
        r = await client.post(f"{TG}/{method}", json=params, timeout=40); return r.json()
    except Exception as e:
        return {"ok": False, "description": type(e).__name__}

def owner_id(): return kv_get("owner_id")

async def notify(text):
    o = owner_id()
    if o and not DISABLE_BOT: await tg("sendMessage", chat_id=o, text=text[:4000])

def webapp_url(): return STATE["webapp_url"]

def hot_list(tf, limit=15): return [(s, a) for s, a in sorted_signals(tf) if is_hot(a)][:limit]

def signals_text(tf):
    sigs = STATE["signals"].get(tf, {})
    if not sigs: return "Scanner is loading (first scan of ~200 coins takes about a minute). Try again shortly."
    hot = hot_list(tf)
    n_long = sum(1 for a in sigs.values() if a["signal"] == "LONG"); n_short = sum(1 for a in sigs.values() if a["signal"] == "SHORT")
    lines = [f"📊 {tf} scalping — {len(sigs)} coins scanned ({STATE['source']})", f"LONG {n_long} · SHORT {n_short} · 80%+: {len(hot)}", ""]
    if not hot:
        lines.append("No 80%+ setups right now. Best candidates:")
        hot = [(s, a) for s, a in sorted_signals(tf) if a["signal"] != "NEUTRAL"][:5]
    for sym, a in hot:
        ic = "🟢" if a["signal"] == "LONG" else "🔴"
        ch = STATE["tickers"].get(sym, {}).get("chg")
        lines.append(f"{ic} {a['signal']} {sym} {a['confidence']}%{' 🔥STRONG' if a['strong'] else ''}{' ⏸' if a.get('paused') else ''}" + (f" ({ch:+.1f}% 24h)" if ch is not None else ""))
        lines.append(f"   E {fmt_price(sym, a['entry'])} | TP {fmt_price(sym, a['tp'])} | SL {fmt_price(sym, a['sl'])}")
    m = STATE.get("mood") or {}
    if m: lines.insert(1, f"Mood {m.get('label')} · BTC 5m {m.get('btc_move_5m') if m.get('btc_move_5m') is not None else 'n/a'}%")
    lines += ["", "Confidence = indicator agreement, not win probability. 🔥STRONG = 1m+5m+15m agree, 1h not opposing. ⏸ = BTC guard pause."]
    return "\n".join(lines)

def signals_kb(tf):
    rows = [[{"text": ("• " if t == tf else "") + t, "callback_data": f"tf:{t}"} for t in TFS]]
    act = [{"text": f"{'🟢' if a['signal'] == 'LONG' else '🔴'} {sym.replace('USDT', '')}", "callback_data": f"ask:{a['signal']}:{sym}:{tf}"}
           for sym, a in hot_list(tf, 9)]
    rows += [act[i:i + 3] for i in range(0, len(act), 3)]
    rows.append([{"text": "🔄 Refresh", "callback_data": f"tf:{tf}"}])
    return {"inline_keyboard": rows}

async def handle_cb(cq):
    frm = cq.get("from", {}); data = cq.get("data", ""); m = cq.get("message", {})
    if frm.get("id") != owner_id():
        await tg("answerCallbackQuery", callback_query_id=cq["id"], text="Private bot"); return
    parts = data.split(":")
    if parts[0] == "tf" and len(parts) > 1 and parts[1] in TFS:
        await tg("editMessageText", chat_id=m["chat"]["id"], message_id=m["message_id"], text=signals_text(parts[1]), reply_markup=signals_kb(parts[1]))
        await tg("answerCallbackQuery", callback_query_id=cq["id"]); return
    if parts[0] == "ask" and len(parts) == 4:
        d, sym, tf = parts[1], parts[2], parts[3]; a = STATE["signals"].get(tf, {}).get(sym, {}); s = get_settings()
        mk = s["market"]
        txt = (f"Confirm {d} {sym} at market?\n{mk.upper()}" + (f" {s['leverage']}x isolated" if mk == "futures" else "") +
               f", margin {s['order_size_usdt']} USDT\nTP {fmt_price(sym, a.get('tp', 0))} / SL {fmt_price(sym, a.get('sl', 0))}")
        await tg("sendMessage", chat_id=m["chat"]["id"], text=txt, reply_markup={"inline_keyboard": [[
            {"text": f"✅ Yes, {d}", "callback_data": f"do:{d}:{sym}:{tf}:{int(time.time())}"}, {"text": "❌ Cancel", "callback_data": "cancel"}]]})
        await tg("answerCallbackQuery", callback_query_id=cq["id"]); return
    if parts[0] == "do" and len(parts) in (3, 5) and parts[1] in ("LONG", "SHORT") and parts[2] in STATE["info"]:
        if len(parts) != 5 or not parts[4].isdigit() or time.time() - int(parts[4]) > 120:
            await tg("answerCallbackQuery", callback_query_id=cq["id"], text="Expired")
            await tg("editMessageText", chat_id=m["chat"]["id"], message_id=m["message_id"], text="⌛ Confirmation expired (2 min). Price badal chuki hogi — /signals se dobara karein.")
            return
        await tg("answerCallbackQuery", callback_query_id=cq["id"], text="Sending order…")
        async with TRADE_LOCK: r = await execute(parts[2], parts[1], "manual", parts[3])
        await tg("editMessageText", chat_id=m["chat"]["id"], message_id=m["message_id"], text=("✅ " + r["msg"]) if r["ok"] else ("❌ " + r["error"])); return
    if parts[0] == "cancel":
        await tg("editMessageText", chat_id=m["chat"]["id"], message_id=m["message_id"], text="Cancelled.")
    await tg("answerCallbackQuery", callback_query_id=cq["id"])

def open_btn():
    u = webapp_url()
    return {"inline_keyboard": [[{"text": "📈 Open Scalping App", "web_app": {"url": u}}]]} if u else None

def key_lines(kc):
    def yn(x): return "ENABLED" if x else "DISABLED"
    return (f"  Account read: {'OK' if kc.get('account_ok') else 'FAIL'}\n  Spot trading: {yn(kc.get('spot_trading_enabled'))}\n"
            f"  Futures: {yn(kc.get('futures_enabled'))} (read {'OK' if kc.get('futures_read_ok') else 'FAIL'}, order test "
            f"{'OK' if kc.get('futures_order_test_ok') else 'FAIL'})\n  Withdrawals: {'ENABLED ⚠️' if kc.get('withdrawals_enabled') else 'disabled ✅'}")

HELP_TEXT = ("⚡ Scalping Bot — commands\n"
    "/signals [1m|5m|15m|strong] — 80%+ LONG/SHORT setups (entry/TP/SL)\n"
    "/strong — 🔥 STRONG confluence: 1m+5m+15m same direction, 1h not opposing\n"
    "/market — market mood: BTC trend, LONG vs SHORT %, BTC sudden-move guard\n"
    "/pnl — realized / unrealized PnL: today, 7d, all time, wins/losses\n"
    "/health — data age per timeframe, WebSocket, rate limit, errors, uptime\n"
    "/alerts on|off|80|85|90|fav|all — auto alerts (default ON, 80%)\n"
    "/stats — paper win-rate (24h / 7d, per timeframe & side)\n"
    "/risk — risk guard: daily loss, open trades, trades/day\n"
    "/fav add|remove SYMBOL, /fav list — favorites / watchlist\n"
    "/balance — spot & futures balances\n"
    "/auto on|off — auto trade (default OFF)\n"
    "/status — scanner, rate-limit & API key status\n"
    "/help — ye list\n\n"
    "Confidence = indicator agreement, win probability nahi. Paper stats real trades nahi hain.")

def norm_sym(x):
    x = x.upper().replace("/", "").strip()
    return x if x.endswith("USDT") else x + "USDT"

def extra_command(cmd, args):
    """New commands; returns reply text or None. Pure function of DB/state so it can be tested without Telegram."""
    s = get_settings()
    if cmd == "/help": return HELP_TEXT
    if cmd == "/stats": return stats_text()
    if cmd == "/risk": return risk_text()
    if cmd == "/strong" or (cmd == "/signals" and args and args[0].lower() == "strong"): return strong_text()
    if cmd == "/market": return mood_text()
    if cmd == "/pnl": return pnl_text()
    if cmd == "/health": return health_text()
    if cmd == "/alerts":
        a = (args[0].lower() if args else "")
        if a in ("on", "off"): s = save_settings({"alerts": a == "on"})
        elif a in ("80", "85", "90"): s = save_settings({"alert_threshold": int(a), "alerts": True})
        elif a == "fav": s = save_settings({"alerts_fav_only": True})
        elif a == "all": s = save_settings({"alerts_fav_only": False})
        elif a: return "Use: /alerts on|off|80|85|90|fav|all"
        return (f"🔔 Alerts {'ON' if s['alerts'] else 'OFF'} · threshold {s['alert_threshold']}% · "
                f"{'sirf favorites' if s['alerts_fav_only'] else 'sab coins'} · cooldown {s['alert_cooldown_min']} min per coin+tf+side · "
                f"max {s['max_alerts_per_min']}/min · 1m/5m/15m")
    if cmd == "/fav":
        favs = favorites(); sub = args[0].lower() if args else "list"
        if sub in ("add", "remove", "rm", "del") and len(args) >= 2:
            sym = norm_sym(args[1])
            if sub == "add":
                if sym not in STATE["info"] and STATE["info"]: return f"{sym} scanner list (top 200) mein nahi hai."
                if sym not in favs: favs.append(sym)
            else: favs = [f for f in favs if f != sym]
            kv_set("favorites", favs)
            return f"⭐ Favorites ({len(favs)}): " + (", ".join(favs) or "empty")
        if sub != "list": return "Use: /fav add SOL · /fav remove SOL · /fav list"
        if not favs: return "⭐ Koi favorite nahi. Add: /fav add SOL"
        lines = ["⭐ Favorites"]
        for sym in favs:
            parts = []
            for tf in TFS:
                a = STATE["signals"].get(tf, {}).get(sym)
                if a: parts.append(f"{tf} {a['signal'][0] if a['signal'] != 'NEUTRAL' else '-'}{a['confidence']}")
            lines.append(f"{sym}: " + (" · ".join(parts) or "no data"))
        return "\n".join(lines)
    return None

async def handle(msg):
    chat = msg.get("chat", {}); frm = msg.get("from", {}); text = (msg.get("text") or "").strip()
    if chat.get("type") != "private" or not text.startswith("/"): return
    cmd, *args = text.split(); cmd = cmd.split("@")[0].lower()
    o = owner_id()
    if o is None and cmd == "/start":
        kv_set("owner_id", frm["id"]); o = frm["id"]; log.info("owner registered")
        await set_menu_button()
    if frm.get("id") != o:
        await tg("sendMessage", chat_id=chat["id"], text="⛔ This is a private bot."); return
    s = get_settings(); cid = chat["id"]
    if cmd == "/pnl": await refresh_exchange_pnl()
    reply = extra_command(cmd, args)
    if reply is not None:
        await tg("sendMessage", chat_id=cid, text=reply[:4000]); return
    if cmd == "/start":
        kb = open_btn()
        await tg("sendMessage", chat_id=cid, text=("⚡ Scalping Bot ready.\nScanning the top ~200 Binance USDT-M futures coins on 1m / 5m / 15m "
                 "with EMA 9/21/50, RSI, MACD, VWAP, Bollinger, volume and candle patterns, plus higher-timeframe confirmation.\n\n"
                 "/signals · /strong · /market · /pnl · /health · /alerts · /stats · /risk · /fav · /balance · /auto · /status · /help\n\nAuto Trade is OFF by default. Alerts ON (80%+). "
                 "Confidence = indicator agreement, not win probability.") + ("" if kb else "\n(Mini app offline; use /signals buttons.)"),
                 **({"reply_markup": kb} if kb else {}))
    elif cmd == "/signals":
        tf = args[0] if args and args[0] in TFS else s["timeframe"]
        await tg("sendMessage", chat_id=cid, text=signals_text(tf), reply_markup=signals_kb(tf))
    elif cmd == "/balance":
        lines = ["💰 Balances"]
        st, j = await signed("GET", "/api/v3/account", {"omitZeroBalances": "true"})
        if st == 200:
            bals = [b for b in j.get("balances", []) if float(b["free"]) + float(b["locked"]) > 0]
            lines.append("Spot: " + (", ".join(f"{b['asset']} {float(b['free']) + float(b['locked']):.6g}" for b in bals[:15]) or "empty"))
        else: lines.append(f"Spot error {st}: {j.get('msg')}")
        st, j = await fsigned("GET", "/fapi/v2/balance")
        if st == 200:
            fb = [b for b in j.get("data", []) if float(b.get("balance", 0)) != 0]
            lines.append("Futures: " + (", ".join(f"{b['asset']} {float(b['balance']):.4f} (avail {float(b['availableBalance']):.4f})" for b in fb) or "empty"))
        else: lines.append(f"Futures error {st}: {j.get('msg')}")
        await tg("sendMessage", chat_id=cid, text="\n".join(lines))
    elif cmd == "/auto":
        if args and args[0].lower() in ("on", "off"):
            on = args[0].lower() == "on"
            if on and not can_trade(s["market"]):
                await tg("sendMessage", chat_id=cid, text=f"⚠️ Cannot enable: the Binance API key has no {s['market']} trading permission (order test failed). Enable it on the key, then /status.")
                return
            save_settings({"auto_trade": on})
            await tg("sendMessage", chat_id=cid, text=f"Auto Trade {'ON ✅' if on else 'OFF'} ({s['market']}, {s['timeframe']}, {s['order_size_usdt']} USDT margin, "
                     f"{s['leverage']}x, min conf {s['auto_min_confidence']}%)")
        else: await tg("sendMessage", chat_id=cid, text=f"Auto Trade is {'ON' if s['auto_trade'] else 'OFF'}. Use /auto on or /auto off")
    elif cmd == "/status":
        kc = await check_key(); sc = STATE["scan"]
        txt = (f"🤖 Status\nData: {STATE['source']} · {len(STATE['universe'])} coins · updated {int(time.time() - STATE['updated'])}s ago\n"
               f"Scan cycle {sc.get('cycle_s')}s · compute {sc.get('compute_s')}s · weight {STATE['weight']}/{weight_limit()} · rate-limit hits {STATE['rate_limit_hits']}\n"
               f"Market: {s['market']} {s['leverage']}x · TF {s['timeframe']} · Auto {'ON' if s['auto_trade'] else 'OFF'}\n"
               f"Order {s['order_size_usdt']} USDT · max pos {s['max_open_positions']} · daily loss limit {s['daily_loss_limit_usdt']} USDT\n"
               f"Open positions {len(open_trades())} · today PnL {today_pnl():+.2f} USDT\n\nBinance key:\n{key_lines(kc)}\nMini app: {webapp_url() or 'n/a'}")
        await tg("sendMessage", chat_id=cid, text=txt)

async def bot_loop():
    fp = hashlib.sha256(TG_TOKEN.encode()).hexdigest()[:16]   # detect a changed bot token (never stores the token)
    if kv_get("tg_fp") != fp:
        j = await tg("getUpdates", offset=-1, timeout=0)
        res = j.get("result") or []
        kv_set("tg_offset", (res[-1]["update_id"] + 1) if res else 0); kv_set("tg_fp", fp)
        log.info("bot token changed or first run: update offset reset")
    offset = kv_get("tg_offset", 0)
    await tg("setMyCommands", commands=[{"command": "start", "description": "Open the scalping app"},
        {"command": "signals", "description": "80%+ signals: /signals 1m|5m|15m|strong"},
        {"command": "strong", "description": "STRONG 1m+5m+15m confluence"}, {"command": "market", "description": "Market mood + BTC guard"},
        {"command": "pnl", "description": "PnL today / 7d / all"}, {"command": "health", "description": "Data freshness & health"}, {"command": "balance", "description": "Spot & futures balances"},
        {"command": "alerts", "description": "Alerts on|off|80|85|90|fav|all"}, {"command": "stats", "description": "Paper win-rate"},
        {"command": "risk", "description": "Risk guard status"}, {"command": "fav", "description": "Favorites: add|remove|list"},
        {"command": "auto", "description": "Auto trade on|off"}, {"command": "status", "description": "Bot, scanner & key status"},
        {"command": "help", "description": "All commands"}])
    while True:
        try:
            STATE["hb"]["bot"] = time.time()
            j = await tg("getUpdates", offset=offset, timeout=25, allowed_updates=["message", "callback_query"])
            STATE["last_poll"] = time.time(); STATE["poll_ok"] = bool(j.get("ok")); STATE["hb"]["bot"] = time.time()
            if not j.get("ok"):
                add_err(f"getUpdates: {j.get('description')}", "telegram"); await asyncio.sleep(5); continue
            for u in j["result"]:
                offset = u["update_id"] + 1; kv_set("tg_offset", offset)
                try:
                    if "callback_query" in u: await handle_cb(u["callback_query"])
                    if "message" in u: await handle(u["message"])
                except Exception as e: add_err(f"handler: {type(e).__name__}: {e}")
        except asyncio.CancelledError: raise
        except Exception as e:
            add_err(f"bot loop: {type(e).__name__}", "telegram"); await asyncio.sleep(5)

async def set_menu_button():
    u = webapp_url()
    if DISABLE_BOT: return
    if not u:
        await tg("setChatMenuButton", menu_button={"type": "commands"}); return
    btn = {"type": "web_app", "text": "Signals", "web_app": {"url": u}}
    r = await tg("setChatMenuButton", menu_button=btn)
    o = owner_id()
    if o: await tg("setChatMenuButton", chat_id=o, menu_button=btn)
    log.info("setChatMenuButton ok=%s", r.get("ok"))

# ---------------- auth ----------------
def validate_init_data(init_data: str):
    if not init_data: return None
    pairs = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
    h = pairs.pop("hash", None)
    if not h: return None
    dcs = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
    secret = hmac.new(b"WebAppData", TG_TOKEN.encode(), hashlib.sha256).digest()
    if not hmac.compare_digest(hmac.new(secret, dcs.encode(), hashlib.sha256).hexdigest(), h): return None
    if time.time() - int(pairs.get("auth_date", 0)) > 86400: return None
    try: return json.loads(pairs.get("user", "{}"))
    except Exception: return None

def auth(request: Request):
    init = request.headers.get("X-Telegram-Init-Data", "")
    if TEST_MODE and init == "test-bypass" and request.client and request.client.host == "127.0.0.1":
        return {"id": owner_id() or 0, "test": True}
    user = validate_init_data(init)
    if not user: raise HTTPException(401, "Invalid Telegram initData. Open the app from the bot.")
    o = owner_id()
    if o is None: raise HTTPException(403, "No owner yet. Send /start to the bot first.")
    if user.get("id") != o: raise HTTPException(403, "This is a private bot.")
    return user

# ---------------- app ----------------
@asynccontextmanager
async def lifespan(app):
    global client
    init_db(); client = httpx.AsyncClient(limits=httpx.Limits(max_connections=100, max_keepalive_connections=80), timeout=httpx.Timeout(15, pool=60))
    STATE["webapp_url"] = os.environ.get("WEBAPP_URL") or (open(URL_FILE).read().strip() if os.path.exists(URL_FILE) else "") or None
    await detect_source()
    WATCH.update({"tickers": (tickers_loop, 200), "ws": (ws_loop, 120), "live": (live_loop, 45)})
    for tf in ALL_TFS: WATCH[f"kl_{tf}"] = ((lambda tf=tf: kline_loop(tf)), STALE_MULT * FETCH_EVERY[tf] + 120)
    for n in list(WATCH): start_task(n)
    try: await check_key()
    except Exception as e: add_err(f"key check: {e}")
    if not DISABLE_BOT:
        me = await tg("getMe"); STATE["bot_username"] = (me.get("result") or {}).get("username")
        await set_menu_button(); WATCH["bot"] = (bot_loop, 150); start_task("bot")
    wd = asyncio.create_task(watchdog())
    yield
    wd.cancel()
    for t in TASKS.values(): t.cancel()
    await asyncio.gather(wd, *TASKS.values(), return_exceptions=True)
    await client.aclose()

J = JSONResponse
FAVSET = set()

def row(sym, a):
    t = STATE["tickers"].get(sym, {})
    return {"symbol": sym, "base": STATE["info"].get(sym, {}).get("base", sym), "signal": a["signal"], "confidence": a["confidence"],
            "hot": is_hot(a), "strong": a["strong"], "price": fmt_price(sym, a["price"]), "entry": fmt_price(sym, a["entry"]),
            "tp": fmt_price(sym, a["tp"]), "sl": fmt_price(sym, a["sl"]), "change24h": t.get("chg"), "qv": t.get("qv"),
            "funding": round(STATE["funding"][sym] * 100, 4) if sym in STATE["funding"] else None,
            "flags": a.get("flags", []), "filtered": a.get("filtered", False), "fav": sym in FAVSET,
            "confluence": bool(a.get("strong")), "stale": bool(a.get("stale")), "age": a.get("age"), "paused": bool(a.get("paused")),
            "price_src": a.get("price_src")}

async def index(request): return FileResponse(os.path.join(BASE, "static", "index.html"), headers={"Cache-Control": "no-store"})

async def health(request):
    h = health_data()
    return J({"ok": not h["incidents"], "updated": STATE["updated"], "source": STATE["source"], "coins": len(STATE["universe"]),
              "coverage": STATE["coverage"], "max_age_s": {tf: d["max_age_s"] for tf, d in h["tf"].items()}, "ws": h["ws"]["status"],
              "weight": STATE["weight"], "weight_peak": STATE["weight_peak"], "rate_limit_hits": STATE["rate_limit_hits"],
              "fetch_errors": STATE["fetch_errors"], "requests": STATE["requests"], "bot_poll_ok": STATE["poll_ok"],
              "funding_symbols": len(STATE["funding"]), "incidents": len(h["incidents"])})

async def api_signals(request: Request):
    auth(request); tf = request.query_params.get("tf", "5m")
    if tf not in TFS: raise HTTPException(400, "tf must be 1m, 5m or 15m")
    global FAVSET
    FAVSET = set(favorites())
    rows = [row(s, a) for s, a in sorted_signals(tf)]
    rows.sort(key=lambda r: not r["fav"])   # stable: favorites pinned on top, keeping hot-first order
    return J({"tf": tf, "updated": STATE["updated"], "refresh_sec": FETCH_EVERY["1m"], "source": STATE["source"], "count": len(rows),
              "hot": sum(1 for r in rows if r["hot"]), "long": sum(1 for r in rows if r["signal"] == "LONG"),
              "short": sum(1 for r in rows if r["signal"] == "SHORT"), "strong": sum(1 for r in rows if r["confluence"] and not r["filtered"]),
              "stale": sum(1 for r in rows if r["stale"]), "mood": STATE.get("mood"), "live_src": STATE["live_src"], "signals": rows})

async def api_signal(request: Request):
    auth(request); tf = request.query_params.get("tf", "5m"); sym = request.query_params.get("symbol", "")
    a = STATE["signals"].get(tf, {}).get(sym)
    if not a: raise HTTPException(404, "unknown symbol")
    global FAVSET
    FAVSET = set(favorites())
    out = row(sym, a); out.update({k: a[k] for k in ("indicators", "notes", "patterns", "htf", "atr_pct", "rsi", "score")})
    out["others"] = {t: STATE["signals"].get(t, {}).get(sym, {}).get("signal") for t in TFS}
    return J(out)

async def api_history(request: Request):
    auth(request)
    with db() as c:
        sig = [dict(r) for r in c.execute("SELECT * FROM signals ORDER BY id DESC LIMIT 150")]
        tr = [dict(r) for r in c.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 100")]
    return J({"signals": sig, "trades": tr})

async def api_settings(request: Request):
    auth(request)
    if request.method == "POST":
        body = await request.json()
        mk = body.get("market", get_settings()["market"])
        if body.get("auto_trade") and not can_trade(mk):
            raise HTTPException(400, f"Binance API key has no {mk} trading permission; Auto Trade cannot be enabled.")
        return J({"settings": save_settings(body)})
    kc = STATE.get("key_check") or {}
    return J({"settings": get_settings(), "open_positions": open_trades(), "today_pnl": today_pnl(), "max_leverage": MAX_LEVERAGE,
              "risk": risk_status(), "favorites": favorites(),
              "key": {k: kc.get(k) for k in ("account_ok", "spot_trading_enabled", "futures_enabled", "futures_read_ok",
                                              "futures_order_test_ok", "withdrawals_enabled", "order_test_ok", "ip_restricted")}})

async def api_trade(request: Request):
    auth(request); body = await request.json()
    sym, d = str(body.get("symbol", "")).upper(), str(body.get("side", "")).upper()
    if sym not in STATE["info"] or d not in ("LONG", "SHORT") or body.get("confirm") is not True:
        raise HTTPException(400, "Bad request")
    if TEST_MODE: return J({"ok": False, "error": "Test mode: no orders placed"})
    async with TRADE_LOCK: return J(await execute(sym, d, "manual", body.get("tf")))

async def api_stats(request: Request):
    auth(request)
    with db() as c:
        recent = [dict(r) for r in c.execute("SELECT * FROM paper ORDER BY id DESC LIMIT 60")]
    return J({"stats": paper_stats(), "recent": recent, "risk": risk_status()})

async def api_fav(request: Request):
    auth(request); body = await request.json()
    sym = norm_sym(str(body.get("symbol", ""))); favs = favorites()
    if sym not in STATE["info"]: raise HTTPException(400, "unknown symbol")
    if body.get("on") and sym not in favs: favs.append(sym)
    if not body.get("on"): favs = [f for f in favs if f != sym]
    kv_set("favorites", favs); return J({"favorites": favs})

async def api_pnl(request: Request):
    auth(request); await refresh_exchange_pnl(); return J(pnl_data())

async def api_health(request: Request):
    auth(request); return J(health_data())

async def http_error(request, exc): return J({"detail": exc.detail}, status_code=exc.status_code)

app = Starlette(routes=[
    Route("/", index), Route("/healthz", health), Route("/api/signals", api_signals), Route("/api/signal", api_signal),
    Route("/api/history", api_history), Route("/api/settings", api_settings, methods=["GET", "POST"]),
    Route("/api/trade", api_trade, methods=["POST"]), Route("/api/stats", api_stats), Route("/api/fav", api_fav, methods=["POST"]),
    Route("/api/pnl", api_pnl), Route("/api/health", api_health),
    Mount("/static", StaticFiles(directory=os.path.join(BASE, "static")), name="static"),
], lifespan=lifespan, exception_handlers={HTTPException: http_error})
