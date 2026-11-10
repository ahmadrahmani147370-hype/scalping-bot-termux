import numpy as np
from ana import *
from gen import TPM, SLM
from search2 import mask_of, F, O, S, T, oos, isA
DI = (SPLIT - T.min()) / 86400; DO = (T.max() - SPLIT) / 86400
d = F[:, C["d"]]
def score(G):
    dd = G[:, C["d"]]
    return (G[:, C["conf"]] + 10 * np.nan_to_num(G[:, C["tr1h"]] * dd) + 5 * np.nan_to_num(G[:, C["tr15"]] * dd)
            + 5 * np.nan_to_num(G[:, C["btc1h"]] * dd) + 3 * (G[:, C["vspike"]] * dd > 0) + 2 * (G[:, C["tf"]] == 15) + (G[:, C["tf"]] == 5))
def ultra_select(pool, off, cap=5):
    """Chronological: candidate fires if it is in the pool, the UTC day has < cap ULTRA signals, its score is >= the day's
    running threshold, and the symbol has no open ULTRA (any TF) and no ULTRA in the last 4h."""
    idx = np.nonzero(pool)[0]; o = np.lexsort((-score(F[idx]), T[idx])); idx = idx[o]
    keep = []; day_n = {}; sym_free = {}
    for i in idx:
        dy = int(T[i] // 86400)
        if day_n.get(dy, 0) >= cap: continue
        if T[i] < sym_free.get(S[i], -1): continue
        keep.append(i); day_n[dy] = day_n.get(dy, 0) + 1
        sym_free[S[i]] = max(T[i] + off[i] * 60 * F[i, C["tf"]], T[i] + 4 * 3600)
    return np.array(keep, int)
P = lambda **k: {**dict(conf=80, strong=0, h1=-9, h15=-9, btc=-9, atrp=0, vs=0, rsiok=0, sess=-1), **k}
pools = {"U1 90%+ STRONG 1h>=0.5 15m>=0.25 BTC>=0.25 ATR>=0.4%": P(conf=90, strong=1, h1=0.5, h15=0.25, btc=0.25, atrp=.004),
         "U2 90%+ STRONG 1h>=0.5 ATR>=0.4% vol-spike RSI 30-70": P(conf=90, strong=1, h1=0.5, atrp=.004, vs=1, rsiok=1),
         "U3 90%+ STRONG 1h>=0.5 15m>=0.25 BTC>=0.25 ATR>=0.6% vol-spike": P(conf=90, strong=1, h1=0.5, h15=0.25, btc=0.25, atrp=.006, vs=1)}
cells = [("base", None, None, "TP1.5/SL1.0 cur")] + [("with", x, y, f"TP{TPM[x]}/SL{SLM[y]}") for x in range(5) for y in range(5)]
for nm, p in pools.items():
    pool0 = mask_of(p, F)
    print("\n##", nm)
    rows = []
    for mode, x, y, lbl in cells:
        cd, rt, off = outcome(F, O, mode, x, y)
        mi = metrics(F, cd, rt, ultra_select(pool0 & ~oos, off), DI); mo = metrics(F, cd, rt, ultra_select(pool0 & oos, off), DO)
        rows.append((lbl, mi, mo))
    rows.sort(key=lambda r: -r[1].get("exp", -9))
    for lbl, mi, mo in rows[:8]: print(f"  {lbl:16s} IS ", fmt(mi), "\n  {:16s} OOS".format(""), fmt(mo))
    r = [x for x in rows if x[0].startswith("TP1.5/SL1.0 cur")][0]; print(f"  [current TP/SL] IS", fmt(r[1]), "\n                  OOS", fmt(r[2]))
