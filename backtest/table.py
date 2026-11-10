import numpy as np
from ana import *
from search2 import mask_of, F, O, S, T, oos, isA
DI = (SPLIT - T.min()) / 86400; DO = (T.max() - SPLIT) / 86400
P = lambda **k: {**dict(conf=80, strong=0, h1=-9, h15=-9, btc=-9, atrp=0, vs=0, rsiok=0, sess=-1), **k}
cfgs = {"C0 current (80%+, all TF)": (0, P()), "C1 15m, 85%+, STRONG, 1h aligned": (15, P(conf=85, strong=1, h1=0.25)),
        "C2 C1 + ATR>=0.4%": (15, P(conf=85, strong=1, h1=0.25, atrp=.004)),
        "C3 all TF, 90%+, STRONG, 1h>=0.5, ATR>=0.4%, vol spike": (0, P(conf=90, strong=1, h1=0.5, atrp=.004, vs=1)),
        "C4 5m, 85%+, STRONG, 1h+15m aligned, BTC aligned": (5, P(conf=85, strong=1, h1=0.25, h15=0.25, btc=0.25))}
cellsx = [("base", None, None, "TP1.5/SL1.0 (current)"), ("with", 2, 1, "TP1.0/SL1.0"), ("with", 1, 4, "TP0.75/SL3.0"), ("with", 0, 4, "TP0.5/SL3.0"),
          ("fade", 0, 4, "FADE TP0.5/SL3.0")]
for name, (tf, p) in cfgs.items():
    m0 = mask_of(p, F) & ((F[:, C["tf"]] == tf) | (tf == 0))
    print("\n##", name)
    for mode, x, y, lbl in cellsx:
        cd, rt, off = outcome(F, O, mode, x, y)
        for part, nm, days in ((~oos, "IS (all coins)", DI), (oos, "OOS (all coins)", DO)):
            sel = select(F, S, m0 & part, off, fresh_only=(name.startswith("C0")), per_side=name.startswith("C0"))
            print(f"  {lbl:22s} {nm:16s}", fmt(metrics(F, cd, rt, sel, days)))
