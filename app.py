"""Scalping bot: Starlette backend (pure Python, Termux-friendly) + Telegram bot (long polling) + signal engine + Binance spot trading.
Secrets come only from env vars and are never logged or sent to the frontend."""
import asyncio, hashlib, hmac, json, logging, math, os, sqlite3, time, urllib.parse
from contextlib import asynccontextmanager
from decimal import Decimal, ROUND_DOWN
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
for noisy in ("httpx", "httpcore"):
    logging.getLogger(noisy).setLevel(logging.WARNING)  # httpx logs URLs, which contain the bot token
log = logging.getLogger("scalp")

SYMBOLS = ["BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "AVAX", "LINK", "TRX"]
NAMES = {"BTC": "Bitcoin", "ETH": "Ethereum", "BNB": "BNB", "SOL": "Solana", "XRP": "XRP", "ADA": "Cardano",
         "DOGE": "Dogecoin", "AVAX": "Avalanche", "LINK": "Chainlink", "TRX": "TRON"}
TFS = ["1m", "5m", "15m"]
REFRESH_SEC = 12
PUBLIC_HOSTS = ["https://data-api.binance.vision", "https://api.binance.com", "https://api1.binance.com"]
PRIVATE_HOSTS = ["https://api.binance.com", "https://api1.binance.com", "https://api2.binance.com", "https://api3.binance.com"]
DEFAULT_SETTINGS = {"timeframe": "5m", "order_size_usdt": 10.0, "max_open_positions": 3, "daily_loss_limit_usdt": 5.0,
                    "auto_min_confidence": 70, "alerts": True, "alert_threshold": 75, "auto_trade": False,
                    "atr_sl_mult": 1.0, "atr_tp_mult": 1.5}

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

def kv_get(k, default=None):
    with db() as c:
        r = c.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return json.loads(r["v"]) if r else default

def kv_set(k, v):
    with db() as c:
        c.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)", (k, json.dumps(v)))

def get_settings():
    s = dict(DEFAULT_SETTINGS); s.update(kv_get("settings", {})); return s

def save_settings(upd):
    s = get_settings()
    for k, v in upd.items():
        if k not in DEFAULT_SETTINGS: continue
        t = type(DEFAULT_SETTINGS[k])
        if t is bool: v = bool(v)
        elif t is int: v = int(v)
        elif t is float: v = float(v)
        s[k] = v
    if s["timeframe"] not in TFS: s["timeframe"] = "5m"
    s["order_size_usdt"] = max(5.0, min(s["order_size_usdt"], 1000.0))
    s["max_open_positions"] = max(1, min(s["max_open_positions"], 10))
    s["daily_loss_limit_usdt"] = max(0.5, s["daily_loss_limit_usdt"])
    s["auto_min_confidence"] = max(50, min(s["auto_min_confidence"], 100))
    s["alert_threshold"] = max(50, min(s["alert_threshold"], 100))
    kv_set("settings", s); return s

# ---------------- indicators ----------------
def ema(vals, n):
    k = 2 / (n + 1); out = []; e = vals[0]
    for v in vals: e = v * k + e * (1 - k); out.append(e)
    return out

def rsi(closes, n=7):
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]; gains += max(d, 0); losses += max(-d, 0)
    ag, al = gains / n, losses / n
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0)) / n; al = (al * (n - 1) + max(-d, 0)) / n
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)

def atr(h, l, c, n=14):
    trs = [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, len(c))]
    a = sum(trs[:n]) / n
    for t in trs[n:]: a = (a * (n - 1) + t) / n
    return a

def analyze(kl, s):
    o = [float(k[1]) for k in kl]; h = [float(k[2]) for k in kl]; l = [float(k[3]) for k in kl]
    c = [float(k[4]) for k in kl]; v = [float(k[5]) for k in kl]
    price = c[-1]; votes = []   # (name, vote in [-1,1], weight, detail)
    e9, e21 = ema(c, 9), ema(c, 21)
    cross_up = any(e9[-i - 1] <= e21[-i - 1] and e9[-i] > e21[-i] for i in range(1, 4))
    cross_dn = any(e9[-i - 1] >= e21[-i - 1] and e9[-i] < e21[-i] for i in range(1, 4))
    ev = 1 if e9[-1] > e21[-1] else -1
    votes.append(("EMA 9/21", ev, 1.5 if (cross_up or cross_dn) else 1.0,
                  ("fresh cross " if (cross_up or cross_dn) else "") + ("9>21" if ev > 0 else "9<21")))
    r = rsi(c, 7)
    rv = 1 if r < 30 else -1 if r > 70 else 0.5 if 52 <= r <= 68 else -0.5 if 32 <= r <= 48 else 0
    votes.append(("RSI(7)", rv, 1.0, f"{r:.1f}"))
    m = [a - b for a, b in zip(ema(c, 12), ema(c, 26))]; sig = ema(m, 9); hist = [a - b for a, b in zip(m, sig)]
    mv = (1 if hist[-1] > hist[-2] else 0.5) if hist[-1] > 0 else (-1 if hist[-1] < hist[-2] else -0.5)
    votes.append(("MACD", mv, 1.0, f"hist {'+' if hist[-1] > 0 else ''}{hist[-1]:.4g}"))
    n = min(len(c), 96); tp_ = [(h[i] + l[i] + c[i]) / 3 for i in range(-n, 0)]
    vw = sum(t * vv for t, vv in zip(tp_, v[-n:])) / max(sum(v[-n:]), 1e-12)
    votes.append(("VWAP", 1 if price > vw else -1, 1.0, f"{'above' if price > vw else 'below'} {vw:.6g}"))
    w = c[-20:]; mid = sum(w) / 20; sd = (sum((x - mid) ** 2 for x in w) / 20) ** 0.5
    up, lo = mid + 2 * sd, mid - 2 * sd; pb = (price - lo) / (up - lo) if up > lo else 0.5
    bv = 1 if pb < 0.05 else -1 if pb > 0.95 else 0
    votes.append(("Bollinger", bv, 1.0, f"%B {pb:.2f}"))
    avgv = sum(v[-21:-1]) / 20; spike = v[-1] > 1.8 * avgv or v[-2] > 1.8 * avgv
    sv = (1 if c[-1] >= o[-1] else -1) if spike else 0
    votes.append(("Volume", sv, 1.0, f"{(v[-1] / avgv if avgv else 0):.1f}x avg" + (" spike" if spike else "")))
    tot = sum(wt for _, _, wt, _ in votes)
    bull = sum(max(vt, 0) * wt for _, vt, wt, _ in votes); bear = sum(max(-vt, 0) * wt for _, vt, wt, _ in votes)
    score = (bull - bear) / tot
    if score >= 0.3: signal, conf = "BUY", bull / tot
    elif score <= -0.3: signal, conf = "SELL", bear / tot
    else: signal, conf = "HOLD", max(bull, bear) / tot  # strength of the leading side, which is too weak to act on
    a = atr(h, l, c, 14)
    # tight ATR-based exits, with floors so TP covers Binance spot fees (~0.1% per side)
    tpd = max(s["atr_tp_mult"] * a, price * 0.0025); sld = max(s["atr_sl_mult"] * a, price * 0.0015)
    if signal == "SELL": tp, sl = price - tpd, price + sld
    else: tp, sl = price + tpd, price - sld
    return {"signal": signal, "confidence": int(round(conf * 100)), "score": round(score, 3), "price": price,
            "entry": price, "tp": tp, "sl": sl, "atr": a, "rsi": round(r, 1),
            "indicators": [{"name": nme, "vote": vt, "weight": wt, "detail": d} for nme, vt, wt, d in votes]}

# ---------------- Binance ----------------
STATE = {"signals": {}, "tickers": {}, "updated": 0, "errors": [], "key_check": None, "filters": {},
         "webapp_url": None, "bot_username": None, "last_poll": 0, "poll_ok": False, "last_alert": {}, "prev_sig": {}}
client: httpx.AsyncClient = None

def add_err(msg):
    STATE["errors"] = ([{"ts": time.time(), "msg": msg}] + STATE["errors"])[:20]; log.warning(msg)

async def pub_get(path, params):
    last = None
    for host in PUBLIC_HOSTS:
        try:
            r = await client.get(host + path, params=params, timeout=10)
            if r.status_code == 200: return r.json()
            last = f"{host}{path} HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            last = f"{host}{path} {type(e).__name__}: {e}"
    raise RuntimeError(last)

def _sign(params):
    p = dict(params); p["timestamp"] = int(time.time() * 1000); p["recvWindow"] = 10000
    q = urllib.parse.urlencode(p)
    return q + "&signature=" + hmac.new(BN_SECRET.encode(), q.encode(), hashlib.sha256).hexdigest()

async def signed(method, path, params=None):
    """Returns (status, json). Tries api/api1/api2/api3 on network/geo errors."""
    last = (0, {"msg": "no host reachable"})
    for host in PRIVATE_HOSTS:
        try:
            r = await client.request(method, f"{host}{path}?{_sign(params or {})}", headers={"X-MBX-APIKEY": BN_KEY}, timeout=15)
            try: j = r.json()
            except Exception: j = {"msg": r.text[:300]}
            if r.status_code in (451, 403):
                last = (r.status_code, {"msg": f"{host}: HTTP {r.status_code} (geo-block / forbidden) {str(j)[:200]}"}); continue
            return r.status_code, j
        except Exception as e:
            last = (0, {"msg": f"{host}: {type(e).__name__}: {e}"})
    return last

async def check_key():
    res = {"ts": time.time()}
    st, j = await signed("GET", "/api/v3/account", {"omitZeroBalances": "true"})
    res["account_http"] = st
    if st == 200:
        res.update(account_ok=True, account_canTrade=j.get("canTrade"), account_canWithdraw=j.get("canWithdraw"))
    else: res.update(account_ok=False, account_error=j)
    st, j = await signed("GET", "/sapi/v1/account/apiRestrictions")
    if st == 200:
        res.update(spot_trading_enabled=j.get("enableSpotAndMarginTrading"), withdrawals_enabled=j.get("enableWithdrawals"),
                   ip_restricted=j.get("ipRestrict"), reading_enabled=j.get("enableReading"))
    else: res["restrictions_error"] = {"http": st, **j}
    st, j = await signed("POST", "/api/v3/order/test", {"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quoteOrderQty": "10"})
    res["order_test_http"] = st; res["order_test_ok"] = st == 200
    if st != 200: res["order_test_error"] = j
    STATE["key_check"] = res; return res

async def load_filters():
    j = await pub_get("/api/v3/exchangeInfo", {"symbols": json.dumps([s + "USDT" for s in SYMBOLS], separators=(",", ":"))})
    for sym in j["symbols"]:
        f = {x["filterType"]: x for x in sym["filters"]}
        STATE["filters"][sym["symbol"]] = {"step": f["LOT_SIZE"]["stepSize"], "tick": f["PRICE_FILTER"]["tickSize"],
            "min_notional": float((f.get("NOTIONAL") or f.get("MIN_NOTIONAL") or {}).get("minNotional", 5))}

def q_step(val, step):
    st = Decimal(step).normalize(); return str(((Decimal(str(val)) / st).to_integral_value(ROUND_DOWN) * st).quantize(st))

def fmt_price(sym, p):
    t = STATE["filters"].get(sym + "USDT", {}).get("tick")
    return q_step(p, t) if t else f"{p:.6g}"

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

def log_trade(**kw):
    kw.setdefault("ts", time.time()); cols = ",".join(kw); qs = ",".join("?" * len(kw))
    with db() as c:
        cur = c.execute(f"INSERT INTO trades({cols}) VALUES({qs})", list(kw.values())); return cur.lastrowid

def upd_trade(tid, **kw):
    with db() as c:
        c.execute(f"UPDATE trades SET {','.join(k + '=?' for k in kw)} WHERE id=?", [*kw.values(), tid])

def fill_price(j):
    q = float(j.get("executedQty") or 0); qq = float(j.get("cummulativeQuoteQty") or 0)
    return (qq / q if q else 0), q, qq

async def execute(symbol, side, mode="manual"):
    s = get_settings(); tf = s["timeframe"]; sym = symbol + "USDT"
    sig = STATE["signals"].get(tf, {}).get(symbol)
    if not sig: return {"ok": False, "error": "No signal data yet"}
    if today_pnl() <= -s["daily_loss_limit_usdt"]:
        return {"ok": False, "error": f"Daily loss limit reached ({today_pnl():.2f} USDT). Trading paused until tomorrow (UTC)."}
    f = STATE["filters"].get(sym, {})
    if side == "BUY":
        if len(open_trades()) >= s["max_open_positions"]: return {"ok": False, "error": "Max open positions reached"}
        if open_trades(symbol): return {"ok": False, "error": f"Already have an open {symbol} position"}
        st, j = await signed("POST", "/api/v3/order", {"symbol": sym, "side": "BUY", "type": "MARKET",
                                                        "quoteOrderQty": f"{s['order_size_usdt']:.2f}", "newOrderRespType": "FULL"})
        if st != 200:
            err = f"Binance {st}: {j.get('code', '')} {j.get('msg', '')}"
            log_trade(symbol=symbol, side="BUY", mode=mode, tf=tf, status="FAILED", error=err); return {"ok": False, "error": err}
        px, qty, quote = fill_price(j)
        tp, sl = px + (sig["tp"] - sig["entry"]), px - (sig["entry"] - sig["sl"])
        tid = log_trade(symbol=symbol, side="BUY", mode=mode, tf=tf, qty=qty, price=px, quote=quote, tp=tp, sl=sl,
                        status="OPEN", order_id=str(j.get("orderId")))
        # OCO exit: take-profit LIMIT_MAKER above, STOP_LOSS_LIMIT below
        sell_qty = q_step(qty * 0.999, f.get("step", "0.00001"))  # leave room for fee taken in base asset
        tick = f.get("tick", "0.01")
        st2, j2 = await signed("POST", "/api/v3/orderList/oco", {
            "symbol": sym, "side": "SELL", "quantity": sell_qty,
            "aboveType": "LIMIT_MAKER", "abovePrice": q_step(tp, tick),
            "belowType": "STOP_LOSS_LIMIT", "belowStopPrice": q_step(sl, tick),
            "belowPrice": q_step(sl * 0.998, tick), "belowTimeInForce": "GTC"})
        if st2 == 200:
            upd_trade(tid, oco_id=str(j2.get("orderListId")), qty=float(sell_qty))
            return {"ok": True, "msg": f"Bought {qty} {symbol} @ {px:.6g}. OCO set TP {q_step(tp, tick)} / SL {q_step(sl, tick)}"}
        upd_trade(tid, qty=float(sell_qty), error=f"OCO failed ({j2.get('msg', '')}); tracked exits active")
        return {"ok": True, "msg": f"Bought {qty} {symbol} @ {px:.6g}. OCO failed, bot is tracking TP/SL itself."}
    else:  # SELL = close open position, or sell free balance of the asset
        ots = open_trades(symbol)
        if ots:
            return await close_trade(ots[0], "manual sell" if mode == "manual" else "SELL signal")
        st, j = await signed("GET", "/api/v3/account", {"omitZeroBalances": "true"})
        if st != 200: return {"ok": False, "error": f"Binance {st}: {j.get('msg')}"}
        free = next((float(b["free"]) for b in j.get("balances", []) if b["asset"] == symbol), 0.0)
        qty = q_step(free, f.get("step", "0.00001"))
        if float(qty) * sig["price"] < f.get("min_notional", 5): return {"ok": False, "error": f"No {symbol} balance to sell (spot only, no shorting)"}
        st, j = await signed("POST", "/api/v3/order", {"symbol": sym, "side": "SELL", "type": "MARKET", "quantity": qty})
        if st != 200:
            err = f"Binance {st}: {j.get('code', '')} {j.get('msg', '')}"
            log_trade(symbol=symbol, side="SELL", mode=mode, tf=tf, status="FAILED", error=err); return {"ok": False, "error": err}
        px, q, quote = fill_price(j)
        log_trade(symbol=symbol, side="SELL", mode=mode, tf=tf, qty=q, price=px, quote=quote, status="DONE", order_id=str(j.get("orderId")))
        return {"ok": True, "msg": f"Sold {q} {symbol} @ {px:.6g}"}

async def close_trade(t, reason):
    sym = t["symbol"] + "USDT"
    if t.get("oco_id"):
        await signed("DELETE", "/api/v3/orderList", {"symbol": sym, "orderListId": t["oco_id"]})
    st, j = await signed("POST", "/api/v3/order", {"symbol": sym, "side": "SELL", "type": "MARKET", "quantity": q_step(t["qty"], STATE["filters"].get(sym, {}).get("step", "0.00001"))})
    if st != 200:
        upd_trade(t["id"], error=f"close failed: {j.get('msg')}"); return {"ok": False, "error": f"Binance {st}: {j.get('msg')}"}
    px, q, quote = fill_price(j); pnl = quote - t["price"] * q
    upd_trade(t["id"], status="CLOSED", exit_price=px, pnl=pnl, closed_ts=time.time(), error=reason)
    await notify(f"Closed {t['symbol']} ({reason}) @ {px:.6g}, PnL {pnl:+.3f} USDT")
    return {"ok": True, "msg": f"Closed {t['symbol']} @ {px:.6g}, PnL {pnl:+.3f} USDT"}

async def manage_positions():
    for t in open_trades():
        sig = STATE["signals"].get("1m", {}).get(t["symbol"]); px = sig["price"] if sig else None
        if t.get("oco_id"):
            st, j = await signed("GET", "/api/v3/orderList", {"orderListId": t["oco_id"]})
            if st == 200 and j.get("listOrderStatus") == "ALL_DONE":
                exit_px = px or t["price"]
                for o in j.get("orders", []):
                    st3, oj = await signed("GET", "/api/v3/order", {"symbol": t["symbol"] + "USDT", "orderId": o["orderId"]})
                    if st3 == 200 and oj.get("status") == "FILLED": exit_px = fill_price(oj)[0] or exit_px
                pnl = (exit_px - t["price"]) * t["qty"]
                upd_trade(t["id"], status="CLOSED", exit_price=exit_px, pnl=pnl, closed_ts=time.time(), error="OCO filled")
                await notify(f"{t['symbol']} OCO exit @ {exit_px:.6g}, PnL {pnl:+.3f} USDT")
        elif px:  # tracked exits
            if px >= t["tp"]: await close_trade(t, "take-profit (tracked)")
            elif px <= t["sl"]: await close_trade(t, "stop-loss (tracked)")

async def auto_trade():
    s = get_settings()
    if not s["auto_trade"]: return
    if today_pnl() <= -s["daily_loss_limit_usdt"]:
        save_settings({"auto_trade": False}); await notify("Daily loss limit hit. Auto Trade switched OFF."); return
    sigs = STATE["signals"].get(s["timeframe"], {})
    for sym, sg in sigs.items():
        has = bool(open_trades(sym))
        if sg["signal"] == "BUY" and sg["confidence"] >= s["auto_min_confidence"] and not has:
            if len(open_trades()) >= s["max_open_positions"]: break
            r = await execute(sym, "BUY", "auto")
            await notify(("Auto BUY " + sym + ": " + r.get("msg", "")) if r["ok"] else f"Auto BUY {sym} failed: {r['error']}")
            if not r["ok"] and "-2015" in r.get("error", ""):
                save_settings({"auto_trade": False}); await notify("Auto Trade switched OFF: API key has no trading permission."); return
        elif sg["signal"] == "SELL" and sg["confidence"] >= s["auto_min_confidence"] and has:
            await close_trade(open_trades(sym)[0], "auto: SELL signal")

# ---------------- signal engine ----------------
async def refresh_signals():
    s = get_settings(); sem = asyncio.Semaphore(10)
    async def one(sym, tf):
        async with sem:
            kl = await pub_get("/api/v3/klines", {"symbol": sym + "USDT", "interval": tf, "limit": 120})
            return sym, tf, analyze(kl, s)
    res = await asyncio.gather(*[one(a, tf) for a in SYMBOLS for tf in TFS], return_exceptions=True)
    new = {tf: {} for tf in TFS}
    for r in res:
        if isinstance(r, Exception): add_err(f"klines: {r}"); continue
        sym, tf, a = r; a["name"] = NAMES[sym]; new[tf][sym] = a
    for sym in SYMBOLS:
        ss = [new[tf].get(sym, {}).get("signal") for tf in TFS]
        strong = ss[0] in ("BUY", "SELL") and all(x == ss[0] for x in ss)
        for tf in TFS:
            if sym in new[tf]: new[tf][sym]["strong"] = strong
    for tf in TFS:
        for sym, a in new[tf].items():
            t = STATE["tickers"].get(sym)
            if t: a["change24h"] = t
            key = (sym, tf); prev = STATE["prev_sig"].get(key)
            if prev != a["signal"]:
                STATE["prev_sig"][key] = a["signal"]
                if a["signal"] != "HOLD" and prev is not None:
                    with db() as c:
                        c.execute("INSERT INTO signals(ts,symbol,tf,signal,confidence,price,entry,tp,sl,strong) VALUES(?,?,?,?,?,?,?,?,?,?)",
                                  (time.time(), sym, tf, a["signal"], a["confidence"], a["price"], a["entry"], a["tp"], a["sl"], int(a["strong"])))
    STATE["signals"] = new; STATE["updated"] = time.time()
    # alerts on chosen timeframe
    if s["alerts"]:
        for sym, a in new.get(s["timeframe"], {}).items():
            if a["signal"] != "HOLD" and a["confidence"] >= s["alert_threshold"]:
                k = (sym, a["signal"]); last = STATE["last_alert"].get(sym)
                if not last or last[0] != a["signal"] or time.time() - last[1] > 900:
                    STATE["last_alert"][sym] = (a["signal"], time.time())
                    await notify(f"{'🟢' if a['signal']=='BUY' else '🔴'} {sym}/USDT {a['signal']} ({s['timeframe']}) conf {a['confidence']}%"
                                 f"{' ⚡ all TFs agree' if a['strong'] else ''}\nEntry {fmt_price(sym,a['entry'])}  TP {fmt_price(sym,a['tp'])}  SL {fmt_price(sym,a['sl'])}\n(Indicator analysis, not financial advice)")

async def tickers_loop():
    while True:
        try:
            j = await pub_get("/api/v3/ticker/24hr", {"symbols": json.dumps([s + "USDT" for s in SYMBOLS], separators=(",", ":")), "type": "MINI"})
            for t in j:
                o, c = float(t["openPrice"]), float(t["lastPrice"])
                STATE["tickers"][t["symbol"][:-4]] = round((c - o) / o * 100, 2) if o else 0
        except Exception as e: add_err(f"ticker: {e}")
        await asyncio.sleep(30)

async def engine_loop():
    while True:
        t0 = time.time()
        try:
            await refresh_signals()
            await manage_positions()
            await auto_trade()
        except Exception as e: add_err(f"engine: {type(e).__name__}: {e}")
        await asyncio.sleep(max(1, REFRESH_SEC - (time.time() - t0)))

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
    if o and not DISABLE_BOT: await tg("sendMessage", chat_id=o, text=text)

def webapp_url(): return STATE["webapp_url"]

def signals_text(tf):
    sigs = STATE["signals"].get(tf, {})
    if not sigs: return "Signals are loading, try again in a few seconds."
    lines = [f"📊 Scalping signals ({tf}) — Indicator analysis"]
    for sym in SYMBOLS:
        a = sigs.get(sym)
        if not a: continue
        ic = {"BUY": "🟢", "SELL": "🔴", "HOLD": "⚪"}[a["signal"]]
        ch = a.get("change24h"); chs = f" ({ch:+.2f}%)" if ch is not None else ""
        line = f"{ic} {sym:<5} {a['signal']:<4} {a['confidence']:>3}%  {fmt_price(sym, a['price'])}{chs}{' ⚡' if a['strong'] else ''}"
        if a["signal"] != "HOLD": line += f"\n     TP {fmt_price(sym, a['tp'])} | SL {fmt_price(sym, a['sl'])}"
        lines.append(line)
    lines.append("⚡ = 1m, 5m and 15m agree. Not financial advice.")
    return "\n".join(lines)

def signals_kb(tf):
    sigs = STATE["signals"].get(tf, {})
    rows = [[{"text": ("• " if t == tf else "") + t, "callback_data": f"tf:{t}"} for t in TFS]]
    act = [{"text": f"{'🟢' if a['signal']=='BUY' else '🔴'} {a['signal']} {sym}", "callback_data": f"ask:{a['signal']}:{sym}:{tf}"}
           for sym, a in sigs.items() if a["signal"] in ("BUY", "SELL")]
    rows += [act[i:i + 3] for i in range(0, len(act), 3)]
    rows.append([{"text": "🔄 Refresh", "callback_data": f"tf:{tf}"}])
    return {"inline_keyboard": rows}

async def handle_cb(cq):
    frm = cq.get("from", {}); data = cq.get("data", ""); m = cq.get("message", {})
    if frm.get("id") != owner_id():
        await tg("answerCallbackQuery", callback_query_id=cq["id"], text="Private bot"); return
    parts = data.split(":")
    if parts[0] == "tf" and parts[1] in TFS:
        await tg("editMessageText", chat_id=m["chat"]["id"], message_id=m["message_id"], text=f"<pre>{signals_text(parts[1])}</pre>",
                 parse_mode="HTML", reply_markup=signals_kb(parts[1]))
        await tg("answerCallbackQuery", callback_query_id=cq["id"]); return
    if parts[0] == "ask":
        side, sym, tf = parts[1], parts[2], parts[3]; a = STATE["signals"].get(tf, {}).get(sym, {}); s = get_settings()
        txt = (f"Confirm {side} {sym}/USDT at market?\n" + (f"Size {s['order_size_usdt']} USDT, TP {fmt_price(sym, a.get('tp', 0))} / SL {fmt_price(sym, a.get('sl', 0))} (OCO)" if side == "BUY" else "Closes the open position / sells free balance"))
        await tg("sendMessage", chat_id=m["chat"]["id"], text=txt, reply_markup={"inline_keyboard": [[
            {"text": f"✅ Yes, {side}", "callback_data": f"do:{side}:{sym}"}, {"text": "❌ Cancel", "callback_data": "cancel"}]]})
        await tg("answerCallbackQuery", callback_query_id=cq["id"]); return
    if parts[0] == "do" and parts[1] in ("BUY", "SELL") and parts[2] in SYMBOLS:
        await tg("answerCallbackQuery", callback_query_id=cq["id"], text="Sending order…")
        r = await execute(parts[2], parts[1], "manual")
        await tg("editMessageText", chat_id=m["chat"]["id"], message_id=m["message_id"], text=("✅ " + r["msg"]) if r["ok"] else ("❌ " + r["error"])); return
    if parts[0] == "cancel":
        await tg("editMessageText", chat_id=m["chat"]["id"], message_id=m["message_id"], text="Cancelled.")
    await tg("answerCallbackQuery", callback_query_id=cq["id"])

def open_btn():
    u = webapp_url()
    return {"inline_keyboard": [[{"text": "📈 Open Scalping App", "web_app": {"url": u}}]]} if u else None

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
    s = get_settings()
    if cmd == "/start":
        kb = open_btn()
        await tg("sendMessage", chat_id=chat["id"], text="⚡ Scalping Bot ready.\nSignals on 1m / 5m / 15m from indicator analysis (EMA 9/21, RSI, MACD, VWAP, Bollinger, volume).\n\n/signals [1m|5m|15m]\n/balance\n/auto on|off\n/status\n\nAuto Trade is OFF by default." + ("" if kb else "\n(Mini app is offline right now; use /signals buttons.)"),
                 **({"reply_markup": kb} if kb else {}))
    elif cmd == "/signals":
        tf = args[0] if args and args[0] in TFS else s["timeframe"]
        await tg("sendMessage", chat_id=chat["id"], text=f"<pre>{signals_text(tf)}</pre>", parse_mode="HTML", reply_markup=signals_kb(tf))
    elif cmd == "/balance":
        st, j = await signed("GET", "/api/v3/account", {"omitZeroBalances": "true"})
        if st != 200: txt = f"Binance error {st}: {j.get('msg')}"
        else:
            bals = [b for b in j.get("balances", []) if float(b["free"]) + float(b["locked"]) > 0]
            txt = "💰 Spot balances\n" + ("\n".join(f"{b['asset']}: {float(b['free']):.8g} (locked {float(b['locked']):.8g})" for b in bals) or "Empty")
        await tg("sendMessage", chat_id=chat["id"], text=txt)
    elif cmd == "/auto":
        if args and args[0].lower() in ("on", "off"):
            on = args[0].lower() == "on"
            kc = STATE.get("key_check") or {}
            if on and not kc.get("order_test_ok"):
                await tg("sendMessage", chat_id=chat["id"], text="⚠️ Cannot enable: the Binance API key has no Spot trading permission (order test failed). Enable 'Spot & Margin Trading' on the key, then /status.")
                return
            save_settings({"auto_trade": on})
            await tg("sendMessage", chat_id=chat["id"], text=f"Auto Trade {'ON ✅' if on else 'OFF'} (timeframe {s['timeframe']}, {s['order_size_usdt']} USDT/trade, min conf {s['auto_min_confidence']}%)")
        else: await tg("sendMessage", chat_id=chat["id"], text=f"Auto Trade is {'ON' if s['auto_trade'] else 'OFF'}. Use /auto on or /auto off")
    elif cmd == "/status":
        kc = await check_key()
        txt = (f"🤖 Status\nTimeframe: {s['timeframe']}\nAuto Trade: {'ON' if s['auto_trade'] else 'OFF'}\nOrder size: {s['order_size_usdt']} USDT\n"
               f"Max positions: {s['max_open_positions']} | Daily loss limit: {s['daily_loss_limit_usdt']} USDT\n"
               f"Open positions: {len(open_trades())} | Today PnL: {today_pnl():+.2f} USDT\n"
               f"Signals updated: {int(time.time() - STATE['updated'])}s ago\n\nBinance key:\n"
               f"  Account read: {'OK' if kc.get('account_ok') else 'FAIL'}\n"
               f"  Spot trading: {'ENABLED' if kc.get('spot_trading_enabled') else 'DISABLED'}\n"
               f"  Withdrawals: {'ENABLED ⚠️' if kc.get('withdrawals_enabled') else 'disabled ✅'}\n"
               f"  Order test: {'OK' if kc.get('order_test_ok') else 'FAIL ' + str((kc.get('order_test_error') or {}).get('msg', ''))}\n"
               f"Mini app: {webapp_url() or 'n/a'}")
        await tg("sendMessage", chat_id=chat["id"], text=txt)

async def bot_loop():
    offset = kv_get("tg_offset", 0)
    await tg("setMyCommands", commands=[{"command": "start", "description": "Open the scalping app"},
        {"command": "signals", "description": "Signals: /signals 1m|5m|15m"}, {"command": "balance", "description": "Spot balances"},
        {"command": "auto", "description": "Auto trade on|off"}, {"command": "status", "description": "Bot & key status"}])
    while True:
        try:
            j = await tg("getUpdates", offset=offset, timeout=25, allowed_updates=["message", "callback_query"])
            STATE["last_poll"] = time.time(); STATE["poll_ok"] = bool(j.get("ok"))
            if not j.get("ok"):
                add_err(f"getUpdates: {j.get('description')}"); await asyncio.sleep(5); continue
            for u in j["result"]:
                offset = u["update_id"] + 1; kv_set("tg_offset", offset)
                if "callback_query" in u:
                    try: await handle_cb(u["callback_query"])
                    except Exception as e: add_err(f"callback: {type(e).__name__}: {e}")
                if "message" in u:
                    try: await handle(u["message"])
                    except Exception as e: add_err(f"handler: {type(e).__name__}: {e}")
        except Exception as e:
            add_err(f"bot loop: {type(e).__name__}"); await asyncio.sleep(5)

async def set_menu_button():
    u = webapp_url()
    if DISABLE_BOT: return
    if not u:
        await tg("setChatMenuButton", menu_button={"type": "commands"}); return
    btn = {"type": "web_app", "text": "Signals", "web_app": {"url": u}}
    r = await tg("setChatMenuButton", menu_button=btn)            # default for all chats
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
    init_db(); client = httpx.AsyncClient(http2=False)
    STATE["webapp_url"] = os.environ.get("WEBAPP_URL") or (open(URL_FILE).read().strip() if os.path.exists(URL_FILE) else "") or None
    try: await load_filters()
    except Exception as e: add_err(f"exchangeInfo: {e}")
    tasks = [asyncio.create_task(engine_loop()), asyncio.create_task(tickers_loop())]
    try: await check_key()
    except Exception as e: add_err(f"key check: {e}")
    if not DISABLE_BOT:
        me = await tg("getMe"); STATE["bot_username"] = (me.get("result") or {}).get("username")
        await set_menu_button(); tasks.append(asyncio.create_task(bot_loop()))
    yield
    for t in tasks: t.cancel()
    await client.aclose()

J = JSONResponse

async def index(request): return FileResponse(os.path.join(BASE, "static", "index.html"), headers={"Cache-Control": "no-store"})

async def health(request): return J({"ok": True, "updated": STATE["updated"], "has_signals": bool(STATE["signals"]), "bot_poll_ok": STATE["poll_ok"]})

async def api_signals(request: Request):
    auth(request); tf = request.query_params.get("tf", "5m")
    if tf not in TFS: raise HTTPException(400, "tf must be 1m, 5m or 15m")
    sigs = STATE["signals"].get(tf, {})
    out = []
    for sym in SYMBOLS:
        a = sigs.get(sym)
        if not a: continue
        out.append({"symbol": sym, "name": a["name"], "signal": a["signal"], "confidence": a["confidence"], "strong": a["strong"],
                    "price": fmt_price(sym, a["price"]), "entry": fmt_price(sym, a["entry"]), "tp": fmt_price(sym, a["tp"]),
                    "sl": fmt_price(sym, a["sl"]), "change24h": a.get("change24h"), "rsi": a["rsi"], "indicators": a["indicators"],
                    "others": {t: STATE["signals"].get(t, {}).get(sym, {}).get("signal") for t in TFS}})
    return J({"tf": tf, "updated": STATE["updated"], "refresh_sec": REFRESH_SEC, "signals": out})

async def api_history(request: Request):
    auth(request)
    with db() as c:
        sig = [dict(r) for r in c.execute("SELECT * FROM signals ORDER BY id DESC LIMIT 100")]
        tr = [dict(r) for r in c.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 100")]
    return J({"signals": sig, "trades": tr})

async def api_settings(request: Request):
    auth(request)
    if request.method == "POST":
        body = await request.json()
        if body.get("auto_trade") and not (STATE.get("key_check") or {}).get("order_test_ok"):
            raise HTTPException(400, "Binance API key has no Spot trading permission; Auto Trade cannot be enabled.")
        return J({"settings": save_settings(body)})
    kc = STATE.get("key_check") or {}
    return J({"settings": get_settings(), "open_positions": open_trades(), "today_pnl": today_pnl(),
              "key": {k: kc.get(k) for k in ("account_ok", "spot_trading_enabled", "withdrawals_enabled", "order_test_ok", "ip_restricted")}})

async def api_trade(request: Request):
    auth(request); body = await request.json()
    sym, side = str(body.get("symbol", "")).upper(), str(body.get("side", "")).upper()
    if sym not in SYMBOLS or side not in ("BUY", "SELL") or body.get("confirm") is not True:
        raise HTTPException(400, "Bad request")
    if TEST_MODE: return J({"ok": False, "error": "Test mode: no orders placed"})
    return J(await execute(sym, side, "manual"))

async def http_error(request, exc): return J({"detail": exc.detail}, status_code=exc.status_code)

app = Starlette(routes=[
    Route("/", index), Route("/healthz", health), Route("/api/signals", api_signals),
    Route("/api/history", api_history), Route("/api/settings", api_settings, methods=["GET", "POST"]),
    Route("/api/trade", api_trade, methods=["POST"]),
    Mount("/static", StaticFiles(directory=os.path.join(BASE, "static")), name="static"),
], lifespan=lifespan, exception_handlers={HTTPException: http_error})
