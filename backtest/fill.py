import json, time, urllib.request, os
syms = open("syms.txt").read().split(); os.makedirs("fill", exist_ok=True)
st = 1793145600 * 1000  # 2026-10-28 00:00 UTC
for s in syms:
    u = f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=1m&startTime={st}&limit=1440"
    r = urllib.request.urlopen(u, timeout=20); wt = r.headers.get("x-mbx-used-weight-1m")
    d = json.loads(r.read()); json.dump(d, open(f"fill/{s}.json", "w"))
    if int(wt or 0) > 1200: time.sleep(20)
    time.sleep(0.5)
print("ok", wt, len(d))
