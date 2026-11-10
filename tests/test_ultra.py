"""ULTRA tier tests (no network, no Telegram, no Binance). Run: .venv/bin/python -m unittest discover -s tests -v"""
import asyncio, os, sys, tempfile, time, unittest
_TMP = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_TMP, "ultra.db"); os.environ["DISABLE_BOT"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import app  # noqa: E402

T0 = 1_700_000_100.0 - (1_700_000_100.0 % 900)

def usig(side="LONG", conf=92, price=100.0, atr=0.6, h1=0.75, **kw):
    d = 1 if side == "LONG" else -1
    a = {"signal": side, "confidence": conf, "entry": price, "price": price, "tp": price + d * 1.5 * atr, "sl": price - d * atr, "atr": atr,
         "atr_pct": round(100 * atr / price, 3), "rsi": 60 if d == 1 else 40, "strong": True, "filtered": False, "stale": False, "paused": False,
         "tr": {"1m": 0.5 * d, "5m": 0.5 * d, "15m": 0.75 * d, "1h": h1 * d},
         "indicators": [{"name": "Volume spike", "vote": d, "weight": 1.0, "detail": "2.5x"}]}
    a.update(kw); return a

def set_kl(sym, tf, ots, px=100.0):
    app.KL[(sym, tf)] = ([px] * len(ots), [px] * len(ots), [px] * len(ots), [px] * len(ots), [1.0] * len(ots), list(ots))

def urows():
    with app.db() as c: return [dict(r) for r in c.execute("SELECT * FROM paper WHERE tier='ULTRA' ORDER BY id")]

class UltraTests(unittest.TestCase):
    def setUp(self):
        if os.path.exists(app.DB_PATH): os.remove(app.DB_PATH)   # app may have been imported by another test module
        app.init_db(); app.KL.clear(); app.LIVE.clear()
        app.STATE.update(signals={tf: {} for tf in app.TFS}, paper_prev={}, hot_prev={}, info={}, tickers={}, ready=True,
                         ultra_ot={}, ultra_open=set(), mood={})
        self.s = app.get_settings()
        self.now = T0 + 905

    def candle(self, tf, n):
        """Advance the reference candle so ultra_scan sees a new close."""
        ots = [T0 + i * app.TF_SEC[tf] for i in range(n)]
        for sym in ["BTCUSDT"] + list(app.STATE["signals"][tf]): set_kl(sym, tf, ots)

    def scan(self, tf="15m", n=3, now=None):
        self.candle(tf, n); return app.ultra_scan(tf, self.s, now or self.now)

    def test_defaults_safe(self):
        self.assertFalse(self.s["auto_trade"]); self.assertFalse(self.s["paper_trade"])
        self.assertEqual(self.s["ultra_max_per_day"], 5); self.assertTrue(self.s["ultra_alerts"])

    def test_reasons(self):
        self.assertEqual(app.ultra_reasons(usig(), "15m"), [])
        for kw, why in (({"conf": 88}, "conf<90"), ({"strong": False}, "not STRONG"), ({"h1": 0.25}, "1h trend not aligned"),
                        ({"atr": 0.2}, "ATR too small vs fees"), ({"rsi": 75}, "RSI extreme"), ({"paused": True}, "BTC guard"),
                        ({"indicators": []}, "no volume spike")):
            self.assertIn(why, app.ultra_reasons(usig(**kw), "15m"), kw)
        self.assertTrue(app.ultra_reasons(usig(stale=True), "15m")); self.assertTrue(app.ultra_reasons(usig(filtered=True), "15m"))
        self.assertTrue(app.ultra_reasons(usig("SHORT", h1=-0.75), "15m"))   # short against a rising 1h trend

    def test_no_burst_then_fires_once_per_candle(self):
        app.STATE["signals"]["15m"]["SOLUSDT"] = usig()
        self.assertEqual(self.scan(n=3), [])            # first evaluation after start only primes
        out = self.scan(n=4); self.assertEqual(len(out), 1)
        self.assertEqual(self.scan(n=4), [])            # same candle again -> nothing
        r = urows()[0]
        self.assertEqual((r["symbol"], r["tf"], r["side"], r["status"], r["trade"]), ("SOLUSDT", "15m", "LONG", "OPEN", 0))
        self.assertAlmostEqual(r["tp"], 100.45); self.assertAlmostEqual(r["sl"], 98.2)
        self.assertIn(("SOLUSDT", "15m"), app.STATE["ultra_open"])

    def test_daily_cap_and_ranking(self):
        for i in range(8): app.STATE["signals"]["15m"][f"C{i}USDT"] = usig(conf=90 + i)
        self.scan(n=3); out = self.scan(n=4)
        self.assertEqual(len(out), 5)
        self.assertEqual([o["symbol"] for o in out], [f"C{i}USDT" for i in (7, 6, 5, 4, 3)])   # best score first
        self.assertEqual(self.scan(n=5, now=self.now + 900), [])                                  # cap reached for the UTC day
        nxt = app.ultra_day_start(self.now) + 86400 + 60
        self.assertEqual(len(self.scan(n=6, now=nxt)), 3)                                          # new day: remaining coins (others still OPEN)

    def test_symbol_cooldown(self):
        app.STATE["signals"]["15m"]["SOLUSDT"] = usig(); self.scan(n=3); self.scan(n=4)
        with app.db() as c: c.execute("UPDATE paper SET status='TP' WHERE tier='ULTRA'")
        self.assertEqual(self.scan(n=5, now=self.now + 3600), [])                                 # 4h cooldown
        self.assertEqual(len(self.scan(n=6, now=self.now + 5 * 3600)), 1)

    def test_separate_stats_and_tracking(self):
        app.STATE["signals"]["15m"]["SOLUSDT"] = usig(); self.scan(n=3); self.scan(n=4)
        r = urows()[0]
        # next candle after the signal candle hits TP (high 101) -> ULTRA TP
        app.KL[("SOLUSDT", "15m")] = ([100, 100, 100], [100, 100, 101.0], [100, 100, 99.9], [100, 100, 100.5], [1, 1, 1],
                                      [r["last_ot"] - 900, r["last_ot"], r["last_ot"] + 900])
        with app.db() as c: c.execute("UPDATE paper SET ts=? WHERE tier='ULTRA'", (time.time(),))   # not expired by wall clock
        app.update_paper(); self.assertEqual(urows()[0]["status"], "TP")
        st = app.paper_stats("ULTRA", (("all", 0),))["all"]["total"]; self.assertEqual((st["TP"], st["win_rate"]), (1, 100.0))
        self.assertEqual(app.paper_stats()["7d"]["total"]["TP"], 0)              # standard tracker untouched
        txt = app.extra_command("/ultra", []); self.assertIn("ULTRA", txt); self.assertIn("SOLUSDT", txt)
        self.assertIn("ULTRA", app.extra_command("/stats", []))

    def test_standard_tracker_not_blocked_by_ultra_row(self):
        app.STATE["signals"]["15m"]["SOLUSDT"] = usig(); self.scan(n=3); self.scan(n=4)
        app.STATE["hot_prev"].clear(); self.s = app.save_settings({"alerts": False})
        asyncio.run(app.record_and_alert(self.s))
        with app.db() as c:
            std = c.execute("SELECT COUNT(*) FROM paper WHERE COALESCE(tier,'')!='ULTRA' AND symbol='SOLUSDT'").fetchone()[0]
        self.assertEqual(std, 1)

    def test_ultra_command_toggle(self):
        app.extra_command("/ultra", ["off"]); self.assertFalse(app.get_settings()["ultra_alerts"])
        app.extra_command("/ultra", ["on"]); self.assertTrue(app.get_settings()["ultra_alerts"])

    def test_alert_text(self):
        t = app.ultra_alert_text([{"symbol": "SOLUSDT", "tf": "15m", "side": "LONG", "confidence": 93, "entry": 100, "tp": 101, "sl": 99,
                                   "score": 110, "n_today": 2}], self.s)
        self.assertIn("ULTRA", t); self.assertIn("PAPER", t)

if __name__ == "__main__": unittest.main()
