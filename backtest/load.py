import zipfile, io, numpy as np, glob, os, sys
from concurrent.futures import ProcessPoolExecutor
def load(sym):
    rows = []
    for f in sorted(glob.glob(f"data/{sym}-1m-*.zip")):
        with zipfile.ZipFile(f) as z:
            txt = z.read(z.namelist()[0]).decode()
        for ln in txt.splitlines():
            if not ln or ln[0] not in "0123456789": continue
            p = ln.split(","); rows.append((int(p[0]) // 1000, float(p[1]), float(p[2]), float(p[3]), float(p[4]), float(p[5])))
    import json
    fp = f"fill/{sym}.json"
    if os.path.exists(fp):
        for x in json.load(open(fp)): rows.append((int(x[0]) // 1000, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])))
    d = {}
    for r in rows: d[r[0]] = r
    a = np.array([d[k] for k in sorted(d)], dtype=float)
    np.save(f"npy/{sym}.npy", a)
    t = a[:, 0]; gaps = int(np.sum(np.diff(t) != 60))
    return sym, len(a), gaps, t[0], t[-1]
if __name__ == "__main__":
    os.makedirs("npy", exist_ok=True)
    syms = open("syms.txt").read().split()
    with ProcessPoolExecutor(6) as ex:
        for r in ex.map(load, syms): print(r)
