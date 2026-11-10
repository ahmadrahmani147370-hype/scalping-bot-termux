"""Generate candidate signals with features + outcomes for one symbol, reusing the bot's analyze_tf/combine."""
import os, sys, time, numpy as np
import appimp
A = appimp.A
S = dict(A.DEFAULT_SETTINGS)
TFM = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}
TPM = np.array([0.5, 0.75, 1.0, 1.5, 2.0]); SLM = np.array([0.5, 1.0, 1.5, 2.0, 3.0])
H = 60
def agg(a, m):
    n = len(a) // m; a = a[: n * m]
    t = a[::m, 0]; o = a[::m, 1]; h = a[:, 2].reshape(n, m).max(1); l = a[:, 3].reshape(n, m).min(1)
    c = a[m - 1::m, 4]; v = a[:, 5].reshape(n, m).sum(1)
    return t, o, h, l, c, v
def load_btc():
    b = np.load("npy/BTCUSDT.npy"); out = {}
    for tf in ("15m", "1h"):
        t, o, h, l, c, v = agg(b, TFM[tf]); L = [list(x) for x in (o, h, l, c, v)]
        tr = np.full(len(t), np.nan)
        for i in range(98, len(t)):
            k = tuple(x[i - 98:i + 1] for x in L) + (list(t[i - 98:i + 1]),)
            tr[i] = A.analyze_tf(k, tf)["trend"]   # trend of CLOSED bar i (approx; open candle = last closed)
        out[tf] = (t, tr)
    return out
def run(sym):
    a = np.load(f"npy/{sym}.npy")
    btc = np.load("feat/btc.npz")
    N1 = len(a); T1 = a[:, 0]
    tfd = {tf: agg(a, TFM[tf]) for tf in TFM}
    lists = {tf: [list(x) for x in tfd[tf][1:]] for tf in TFM}
    tls = {tf: list(tfd[tf][0]) for tf in TFM}
    hi1, lo1, cl1 = a[:, 2], a[:, 3], a[:, 4]
    qv1 = a[:, 4] * a[:, 5]; cq = np.concatenate([[0], np.cumsum(qv1)])
    def window(tf, j):
        """Bot-style kline window at time T = end of 1m bar j: 98 closed bars + the open/partial bar."""
        m = TFM[tf]; nb = (j + 1) // m; rem = (j + 1) % m
        if nb < 98: return None
        o, h, l, c, v = lists[tf]; t = tls[tf]
        if rem == 0:
            px = cl1[j]
            k = (o[nb - 98:nb] + [px], h[nb - 98:nb] + [px], l[nb - 98:nb] + [px], c[nb - 98:nb] + [px], v[nb - 98:nb] + [0.0],
                 t[nb - 98:nb] + [T1[j] + 60])
        else:
            s0 = nb * m
            po, ph, pl, pc, pv = a[s0, 1], hi1[s0:j + 1].max(), lo1[s0:j + 1].min(), cl1[j], a[s0:j + 1, 5].sum()
            k = (o[nb - 98:nb] + [po], h[nb - 98:nb] + [ph], l[nb - 98:nb] + [pl], c[nb - 98:nb] + [pc], v[nb - 98:nb] + [pv],
                 t[nb - 98:nb] + [T1[s0]])
        return k
    rawcache = {}
    def raw_at(tf, j):
        key = (tf, j)
        if key not in rawcache:
            k = window(tf, j); rawcache[key] = A.analyze_tf(k, tf) if k else None
        return rawcache[key]
    rows = []; outs = []
    for tf in ("1m", "5m", "15m"):
        m = TFM[tf]; t, o, h, l, c, v = tfd[tf]; nb = len(t)
        prev_hot = {}
        for b in range(100, nb - H - 2):
            j = b * m + m - 1           # last 1m bar of tf bar b -> evaluation at its close
            rawcache.clear()
            r0 = raw_at(tf, j)
            if r0 is None: continue
            pre = A.combine({tf: r0}, tf, S)
            if pre["signal"] == "NEUTRAL" or pre["confidence"] + 3 * len(A.HTF[tf]) < 60:
                prev_hot.clear() if False else None
                prev_hot[b] = None; continue
            raw = {tf: r0}
            for t2 in ("1m", "5m", "15m", "1h"):
                if t2 not in raw:
                    rr = raw_at(t2, j)
                    if rr: raw[t2] = rr
            if len(raw) < 4: prev_hot[b] = None; continue
            cb = {x: A.combine(raw, x, S) for x in ("1m", "5m", "15m")}
            sig = cb[tf]
            if sig["signal"] == "NEUTRAL" or sig["confidence"] < 60: prev_hot[b] = None; continue
            d = 1 if sig["signal"] == "LONG" else -1
            sigs = [cb[x]["signal"] for x in ("1m", "5m", "15m")]
            strong = (all(x == sigs[0] for x in sigs) and sigs[0] != "NEUTRAL" and raw["1h"]["trend"] * (1 if sigs[0] == "LONG" else -1) > -0.25
                      and all(cb[x]["confidence"] >= S["strong_min_conf"] for x in ("1m", "5m", "15m")))
            hot = sig["confidence"] >= 80
            # previous bar hot same side?  (bot records only fresh hot transitions)
            ph = prev_hot.get(b - 1)
            prev_hot[b] = sig["signal"] if hot else None
            comps = {n: vt for n, vt, *_ in r0["comps"]}
            p = sig["price"]; at = sig["atr"]
            # future path: tf bars b+1..b+H
            fh = h[b + 1:b + 1 + H]; fl = l[b + 1:b + 1 + H]; fc = c[b + H]
            res = []
            for side in (1, -1):
                tpd = TPM * at; sld = SLM * at
                if side == 1:
                    tph = fh[None, :] >= (p + tpd)[:, None]; slh = fl[None, :] <= (p - sld)[:, None]
                else:
                    tph = fl[None, :] <= (p - tpd)[:, None]; slh = fh[None, :] >= (p + sld)[:, None]
                ti = np.where(tph.any(1), tph.argmax(1), 999); si = np.where(slh.any(1), slh.argmax(1), 999)
                for x in range(len(TPM)):
                    for y in range(len(SLM)):
                        if si[y] <= ti[x] and si[y] < 999: res += [-1, -sld[y] / p, si[y] + 1]
                        elif ti[x] < 999: res += [1, tpd[x] / p, ti[x] + 1]
                        else: res += [0, side * (fc - p) / p, H]
            # bot default cell (with floors), from bar b+1 (correct) and from b+2 (live tracker skips the entry candle)
            for st in (1, 2):
                tpd = max(1.5 * at, p * 0.0025); sld = max(1.0 * at, p * 0.0015)
                hh = h[b + st:b + 1 + H]; ll = l[b + st:b + 1 + H]
                if d == 1: tpx = np.nonzero(hh >= p + tpd)[0]; slx = np.nonzero(ll <= p - sld)[0]
                else: tpx = np.nonzero(ll <= p - tpd)[0]; slx = np.nonzero(hh >= p + sld)[0]
                ti = tpx[0] if len(tpx) else 999; si = slx[0] if len(slx) else 999
                if si <= ti and si < 999: res += [-1, -sld / p, si + st]
                elif ti < 999: res += [1, tpd / p, ti + st]
                else: res += [0, d * (fc - p) / p, H]
            # features
            T = T1[j] + 60
            bi15 = np.searchsorted(btc["t15"], T - 900, side="right") - 1; bi1h = np.searchsorted(btc["t1h"], T - 3600, side="right") - 1
            qv24 = cq[j + 1] - cq[max(0, j + 1 - 1440)]
            rh = r0
            vv = lists[tf][4]; avgv = (sum(vv[b - 20:b]) / 20) or 1e-12
            e50 = A.ema(lists[tf][3][b - 98:b + 1], 50)[-1]
            rows.append([TFM[tf], b, T, d, sig["confidence"], sig["score"], int(strong), int(hot), int(ph == sig["signal"]),
                         raw.get("5m", {}).get("trend", np.nan) if tf != "5m" else np.nan,
                         raw["15m"]["trend"] if "15m" in raw else np.nan, raw["1h"]["trend"] if "1h" in raw else np.nan,
                         r0["trend"], r0["rsi"], r0["atr_pct"], vv[b] / avgv, (T // 3600) % 24, (T // 86400 + 4) % 7,
                         comps["EMA 9/21 cross"], comps["Bollinger"], comps["Volume spike"], comps["Candles"], comps["RSI(9)"], comps["MACD hist"],
                         btc["tr15"][bi15] if bi15 >= 0 else np.nan, btc["tr1h"][bi1h] if bi1h >= 0 else np.nan, qv24 / 1e6,
                         (p - e50) / at if at else 0, at / p,
                         cb["1m"]["confidence"] * (1 if cb["1m"]["signal"] == sig["signal"] else -1 if cb["1m"]["signal"] != "NEUTRAL" else 0),
                         cb["5m"]["confidence"] * (1 if cb["5m"]["signal"] == sig["signal"] else -1 if cb["5m"]["signal"] != "NEUTRAL" else 0),
                         cb["15m"]["confidence"] * (1 if cb["15m"]["signal"] == sig["signal"] else -1 if cb["15m"]["signal"] != "NEUTRAL" else 0),
                         int("volatility very high" in sig["notes"])])
            outs.append(res)
    F = np.array(rows, dtype=np.float64); O = np.array(outs, dtype=np.float32)
    np.savez_compressed(f"feat/{sym}.npz", F=F, O=O)
    return sym, len(F)
COLS = ["tf", "b", "T", "d", "conf", "score", "strong", "hot", "prevhot", "tr5", "tr15", "tr1h", "trown", "rsi", "atrpct", "volr", "hour", "dow",
        "cross", "bb", "vspike", "candle", "rsiv", "macdv", "btc15", "btc1h", "qv24", "dist50", "atrp", "c1m", "c5m", "c15m", "hivol"]
if __name__ == "__main__":
    if sys.argv[1] == "btc":
        o = load_btc(); np.savez("feat/btc.npz", t15=o["15m"][0], tr15=o["15m"][1], t1h=o["1h"][0], tr1h=o["1h"][1]); print("btc ok")
    else:
        from multiprocessing import Pool
        syms = sys.argv[1:] if sys.argv[1] != "all" else open("syms.txt").read().split()
        syms = [s for s in syms if not os.path.exists(f"feat/{s}.npz")]
        t0 = time.time()
        with Pool(int(os.environ.get("NP", "6"))) as p:
            for r in p.imap_unordered(run, syms): print(r, round(time.time() - t0), flush=True)
