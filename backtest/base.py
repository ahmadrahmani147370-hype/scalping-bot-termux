from ana import *
F, O, S, syms = load(80)
T = F[:, C["T"]]; oos = T >= SPLIT; isA = S % 2 == 0
DI = (SPLIT - T.min()) / 86400; DO = (T.max() - SPLIT) / 86400
print("rows", len(F), "IS days %.1f OOS days %.1f coins %d" % (DI, DO, len(set(S))))
for mode in ("base", "live"):
    cd, rt, off = outcome(F, O, mode)
    for tf in (1, 5, 15, 0):
        for nm, part, days in (("IS all", ~oos, DI), ("OOS all", oos, DO)):
            m = part & ((F[:, C["tf"]] == tf) | (tf == 0))
            sel = select(F, S, m, off, fresh_only=True, per_side=True)
            print(mode, tf or "all", nm, fmt(metrics(F, cd, rt, sel, days)))
