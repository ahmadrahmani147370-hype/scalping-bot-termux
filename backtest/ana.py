import numpy as np, glob, os, datetime as D
from gen import COLS, TPM, SLM, H
C = {c: i for i, c in enumerate(COLS)}
SPLIT = D.datetime(2026, 10, 16, tzinfo=D.UTC).timestamp()
COST = 0.0012     # round trip: 0.08% taker fees + ~0.04% slippage
def load(minconf=80):
    syms = open("syms.txt").read().split()
    Fs, Os, Ss = [], [], []
    for i, s in enumerate(syms):
        p = f"feat/{s}.npz"
        if not os.path.exists(p): continue
        z = np.load(p); F, O = z["F"], z["O"]
        m = F[:, C["conf"]] >= minconf
        Fs.append(F[m]); Os.append(O[m]); Ss.append(np.full(m.sum(), i))
    return np.concatenate(Fs), np.concatenate(Os), np.concatenate(Ss), syms
def cell(side, x, y):     # side 0 = with signal, 1 = fade ; returns column of code
    return None
def outcome(F, O, mode, x=None, y=None):
    """mode: 'base' (bot default TP/SL with floors, check from next candle), 'live' (skips the entry candle),
    'with'/'fade' with TPM[x], SLM[y]. Returns code, ret(gross), exit offset."""
    if mode == "base": k = 150
    elif mode == "live": k = 153
    else:
        d = F[:, C["d"]]; ci = x * len(SLM) + y
        if mode == "with": sidx = np.where(d == 1, 0, 1)
        else: sidx = np.where(d == 1, 1, 0)
        base = sidx * 75 + ci * 3
        r = np.arange(len(F))
        return O[r, base], O[r, base + 1], O[r, base + 2]
    return O[:, k], O[:, k + 1], O[:, k + 2]
def select(F, S, mask, off, fresh_only=False, per_side=False):
    """Sequential no-overlap: one open position per (sym, tf) [or (sym, tf, side) like the live tracker]."""
    idx = np.nonzero(mask)[0]
    if fresh_only: idx = idx[F[idx, C["prevhot"]] == 0]
    tf = F[idx, C["tf"]]; b = F[idx, C["b"]]; d = F[idx, C["d"]]
    key = S[idx] * 100 + tf + (d > 0) * 50 * per_side
    order = np.lexsort((b, key)); idx = idx[order]; key = key[order]; b = b[order]
    keep = np.zeros(len(idx), bool); last_key = None; free_at = -1
    offs = off[idx]
    for n in range(len(idx)):
        if key[n] != last_key: last_key = key[n]; free_at = -1
        if b[n] > free_at: keep[n] = True; free_at = b[n] + offs[n]
    return idx[keep]
def metrics(F, code, ret, sel, days):
    if len(sel) == 0: return dict(n=0)
    cd = code[sel]; net = ret[sel] - COST
    tp = (cd == 1).sum(); sl = (cd == -1).sum(); ex = (cd == 0).sum()
    T = F[sel, C["T"]]; o = np.argsort(T); eq = np.cumsum(net[o]); dd = (np.maximum.accumulate(np.maximum(eq, 0)) - eq).max()
    g = net[net > 0].sum(); l = -net[net < 0].sum()
    return dict(n=len(sel), wr=100 * tp / max(1, tp + sl), netwin=100 * (net > 0).mean(), exp=100 * net.mean(), gross=100 * ret[sel].mean(),
                pf=g / l if l else np.inf, perday=len(sel) / days, dd=100 * dd, exp_n=ex)
def fmt(m):
    if not m.get("n"): return "n=0"
    return (f"n={m['n']:5d} WR={m['wr']:5.1f}% net+={m['netwin']:5.1f}% exp={m['exp']:+.3f}% (gross {m['gross']:+.3f}%) PF={m['pf']:.2f} "
            f"/day={m['perday']:.1f} maxDD={m['dd']:.1f}% exp'd={m['exp_n']}")
