import numpy as np, itertools, pickle
from ana import *
F, O, S, syms = load(80)
T = F[:, C["T"]]; oos = T >= SPLIT; isA = S % 2 == 0
MID = T.min() + (SPLIT - T.min()) / 2
cells = [(m, x, y) for m in ("with", "fade") for x in range(len(TPM)) for y in range(len(SLM))] + [("base", None, None)]
CODE = np.zeros((len(F), len(cells)), np.float32); RET = np.zeros_like(CODE)
for k, (m, x, y) in enumerate(cells):
    cd, rt, _ = outcome(F, O, m, x, y); CODE[:, k] = cd; RET[:, k] = rt - COST
WIN = (CODE == 1).astype(np.float32); LOSS = (CODE == -1).astype(np.float32)
grid = dict(conf=[80, 85, 90], strong=[0, 1], h1=[-9, 0.25, 0.5], h15=[-9, 0.25], btc=[-9, 0, 0.25], atrp=[0, .002, .004, .006],
            vs=[0, 1], rsiok=[0, 1], sess=[-1, 0, 1, 2])
def mask_of(p, G):
    dd = G[:, C["d"]]
    m = (G[:, C["conf"]] >= p["conf"]) & ((G[:, C["strong"]] == 1) | (p["strong"] == 0))
    m &= np.nan_to_num(G[:, C["tr1h"]] * dd, nan=-9) >= p["h1"]
    m &= np.nan_to_num(G[:, C["tr15"]] * dd, nan=-9) >= p["h15"]
    m &= np.nan_to_num(G[:, C["btc1h"]] * dd, nan=-9) >= p["btc"]
    m &= G[:, C["atrp"]] >= p["atrp"]
    if p["vs"]: m &= G[:, C["vspike"]] * dd > 0
    if p["rsiok"]: m &= (G[:, C["rsi"]] > 30) & (G[:, C["rsi"]] < 70)
    if p["sess"] >= 0: m &= (G[:, C["hour"]] // 8) == p["sess"]
    return m
combos = [dict(zip(grid, v)) for v in itertools.product(*grid.values())]
groups = {"IS1A": (T < MID) & isA, "IS1B": (T < MID) & ~isA, "IS2A": (T >= MID) & ~oos & isA, "IS2B": (T >= MID) & ~oos & ~isA,
          "OOSA": oos & isA, "OOSB": oos & ~isA}
if __name__ == "__main__":
    out = {}
    for tf in (1, 5, 15, 0):
        tfm = (F[:, C["tf"]] == tf) | (tf == 0)
        for g, gm in groups.items():
            sub = np.nonzero(tfm & gm)[0]; G = F[sub]
            M = np.stack([mask_of(p, G) for p in combos]).astype(np.float32)
            out[(tf, g)] = (M.sum(1), M @ WIN[sub], M @ LOSS[sub], M @ RET[sub])
        print("tf", tf, flush=True)
    pickle.dump(dict(out=out, combos=combos, cells=cells, groups=list(groups)), open("search2.pkl", "wb"))
