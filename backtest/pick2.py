import pickle, numpy as np
from gen import TPM, SLM
z = pickle.load(open("search2.pkl", "rb")); out, combos, cells = z["out"], z["combos"], z["cells"]
ISG = ["IS1A", "IS1B", "IS2A", "IS2B"]
def stats(tf, g):
    n, W, L, R = out[(tf, g)]
    n2 = n[:, None]
    return n2 + 0 * W, 100 * W / np.maximum(1, W + L), 100 * R / np.maximum(1, n2)
def desc(ci, k):
    m, x, y = cells[k]; p = {a: b for a, b in combos[ci].items() if b not in (-9, 0, -1) or a == "strong" and b}
    return f"{p} " + ("default" if m == "base" else f"{m} TP{TPM[x]}/SL{SLM[y]}")
summary = {}
for tf in (1, 5, 15, 0):
    S = {g: stats(tf, g) for g in ISG + ["OOSA", "OOSB"]}
    nmin = np.min([S[g][0] for g in ISG], 0); wrmin = np.min([S[g][1] for g in ISG], 0); exmin = np.min([S[g][2] for g in ISG], 0)
    ok = nmin >= 15
    pos = ok & (exmin > 0)
    oos_exp = (S["OOSA"][2] * S["OOSA"][0] + S["OOSB"][2] * S["OOSB"][0]) / np.maximum(1, S["OOSA"][0] + S["OOSB"][0])
    oos_wr = (S["OOSA"][1] * 0 + 0)
    print(f"\n#### tf={tf or 'all'}: configs w/ n>=15 per IS group: {ok.sum()}, robust-positive in all 4 IS groups: {pos.sum()}, "
          f"of those positive OOS(pooled): {(pos & (oos_exp > 0)).sum()}")
    for title, key, m in (("by min-IS expectancy", -exmin, pos), ("by min-IS WR (min-IS exp>0)", -wrmin, pos), ("by min-IS WR (any exp)", -wrmin, ok)):
        idx = np.argwhere(m); o = np.argsort(key[m])[:6]
        print(" --", title)
        for ci, k in idx[o]:
            print(f"   {desc(ci,k)} | IS min n={nmin[ci,k]:.0f} WR>={wrmin[ci,k]:.1f} exp>={exmin[ci,k]:+.3f} | OOS-A n={S['OOSA'][0][ci,k]:.0f} WR={S['OOSA'][1][ci,k]:.1f} exp={S['OOSA'][2][ci,k]:+.3f} | OOS-B n={S['OOSB'][0][ci,k]:.0f} WR={S['OOSB'][1][ci,k]:.1f} exp={S['OOSB'][2][ci,k]:+.3f}")
