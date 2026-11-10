"""Paper-trading tests (no network, no Telegram, no Binance). Run: .venv/bin/python -m unittest discover -s tests -v"""
import asyncio, os, sys, tempfile, time, unittest
_TMP = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_TMP, "test.db"); os.environ["DISABLE_BOT"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import app  # noqa: E402

T0 = 1_700_000_100.0 - (1_700_000_100.0 % 300)

def sig(side="LONG", conf=86, entry=100.0, atr=1.0, **kw):
    d = 1 if side == "LONG" else -1
    a = {"signal": side, "confidence": conf, "entry": entry, "price": entry, "tp": entry + d * 1.5 * atr, "sl": entry - d * atr,
         "atr": atr, "strong": False, "filtered": False, "stale": False, "paused": False}
    a.update(kw); return a

def set_kl(sym, tf, candles):
    """candles: list of (open_time, high, low, close)"""
    app.KL[(sym, tf)] = ([c[3] for c in candles], [c[1] for c in candles], [c[2] for c in candles], [c[3] for c in candles],
                         [1.0] * len(candles), [c[0] for c in candles])

def rows(where="trade=1"):
    with app.db() as c: return [dict(r) for r in c.execute(f"SELECT * FROM paper WHERE {where} ORDER BY id")]

class PaperTests(unittest.TestCase):
    def setUp(self):
        if os.path.exists(app.DB_PATH): os.remove(app.DB_PATH)
        app.init_db(); app.KL.clear(); app.LIVE.clear()
        app.STATE.update(signals={tf: {} for tf in app.TFS}, paper_prev={}, hot_prev={}, info={}, tickers={}, ready=True)
        self.s = app.save_settings({"paper_trade": True}); self.sent = []   # $500 paper feature is OFF by default; tests switch it on
        async def fake_notify(text): self.sent.append(text)
        app.notify = fake_notify
        set_kl("BTCUSDT", "5m", [(T0, 100, 100, 100)])

    def put(self, sym, tf, a): app.STATE["signals"][tf][sym] = a

    def test_defaults(self):
        self.assertFalse(app.DEFAULT_SETTINGS["paper_trade"]); self.assertTrue(self.s["paper_trade"]); self.assertEqual(self.s["paper_min_conf"], 85); self.assertEqual(self.s["paper_size_usdt"], 500.0)
        self.assertFalse(self.s["auto_trade"])   # real auto-trade stays OFF

    def test_open_at_85_with_500_notional(self):
        self.put("BTCUSDT", "5m", sig(conf=85, entry=100.0))
        opened = app.open_paper_trades(self.s)
        self.assertEqual(len(opened), 1); r = rows()[0]
        self.assertEqual((r["side"], r["status"], r["notional"]), ("LONG", "OPEN", 500.0))
        self.assertAlmostEqual(r["qty"], 5.0); self.assertAlmostEqual(r["tp"], 101.5); self.assertAlmostEqual(r["sl"], 99.0)

    def test_short_opens(self):
        self.put("BTCUSDT", "5m", sig("SHORT", conf=92)); app.open_paper_trades(self.s)
        self.assertEqual(rows()[0]["side"], "SHORT")

    def test_skip_below_85(self):
        self.put("BTCUSDT", "5m", sig(conf=84)); self.assertEqual(app.open_paper_trades(self.s), []); self.assertEqual(rows(), [])

    def test_disabled(self):
        s = app.save_settings({"paper_trade": False}); self.put("BTCUSDT", "5m", sig(conf=95))
        self.assertEqual(app.open_paper_trades(s), [])

    def test_threshold_and_size_adjustable(self):
        s = app.save_settings({"paper_min_conf": 90, "paper_size_usdt": 250})
        self.put("BTCUSDT", "5m", sig(conf=88)); self.assertEqual(app.open_paper_trades(s), [])
        app.STATE["paper_prev"].clear(); self.put("BTCUSDT", "5m", sig(conf=91)); app.open_paper_trades(s)
        self.assertEqual(rows()[0]["notional"], 250.0)
        self.assertEqual(app.save_settings({"paper_min_conf": 50})["paper_min_conf"], 80)   # clamped to 80..99

    def test_no_duplicate_while_open(self):
        self.put("BTCUSDT", "5m", sig(conf=90)); app.open_paper_trades(self.s)
        app.STATE["paper_prev"].clear(); app.open_paper_trades(self.s)                       # re-qualifies (flicker)
        app.STATE["paper_prev"].clear(); self.put("BTCUSDT", "5m", sig("SHORT", conf=90)); app.open_paper_trades(self.s)  # opposite side
        self.assertEqual(len(rows()), 1)
        self.put("BTCUSDT", "1m", sig(conf=90)); set_kl("BTCUSDT", "1m", [(T0, 100, 100, 100)]); app.open_paper_trades(self.s)
        self.assertEqual(len(rows()), 2)                                                      # other timeframe is independent

    def test_quality_filters(self):
        for kw in ({"stale": True}, {"filtered": True}, {"paused": True}, {"signal": "NEUTRAL"}):
            app.STATE["paper_prev"].clear(); self.put("BTCUSDT", "5m", sig(conf=95, **kw))
            self.assertEqual(app.open_paper_trades(self.s), [], kw)
        self.assertEqual(rows(), [])

    def test_no_burst_on_startup(self):
        self.put("BTCUSDT", "5m", sig(conf=95)); app.prime_hot()                 # already >=85% when the bot starts
        self.assertEqual(app.open_paper_trades(self.s), [])
        self.put("ETHUSDT", "5m", sig(conf=88)); set_kl("ETHUSDT", "5m", [(T0, 100, 100, 100)])
        self.assertEqual(len(app.open_paper_trades(self.s)), 1)                  # a fresh crossing after startup opens

    def test_record_and_alert_opens_and_notifies_even_with_alerts_off(self):
        s = app.save_settings({"alerts": False}); self.put("BTCUSDT", "5m", sig(conf=87))
        asyncio.run(app.record_and_alert(s))
        self.assertEqual(len(rows()), 1); self.assertEqual(len(rows("COALESCE(trade,0)=0")), 1)   # win-rate row still recorded
        self.assertTrue(any("PAPER trade OPEN" in m and "BTCUSDT" in m for m in self.sent))

    def _open(self, side="LONG", conf=90):
        self.put("BTCUSDT", "5m", sig(side, conf=conf)); app.open_paper_trades(self.s)

    def test_close_tp_pnl(self):
        s = app.save_settings({"trailing": False}); self._open()
        set_kl("BTCUSDT", "5m", [(T0, 100, 100, 100), (T0 + 300, 101.6, 99.5, 101.0)])
        closed = []; app.update_paper(closed, s); r = rows()[0]
        self.assertEqual(r["status"], "TP"); self.assertAlmostEqual(r["pnl"], 7.5); self.assertAlmostEqual(r["pnl_pct"], 1.5)
        self.assertEqual(len(closed), 1); txt = app.paper_close_text(closed)
        self.assertIn("+7.50 USDT", txt); self.assertIn("+1.50%", txt)

    def test_close_sl_pnl_short(self):
        s = app.save_settings({"trailing": False}); self._open("SHORT")
        set_kl("BTCUSDT", "5m", [(T0, 100, 100, 100), (T0 + 300, 101.2, 99.9, 101.0)])
        app.update_paper([], s); r = rows()[0]
        self.assertEqual(r["status"], "SL"); self.assertAlmostEqual(r["pnl"], -5.0); self.assertAlmostEqual(r["pnl_pct"], -1.0)

    def test_same_candle_tp_and_sl_counts_as_sl(self):
        s = app.save_settings({"trailing": False}); self._open()
        set_kl("BTCUSDT", "5m", [(T0, 100, 100, 100), (T0 + 300, 102.0, 98.0, 100.0)])
        app.update_paper([], s); r = rows()[0]
        self.assertEqual(r["status"], "SL"); self.assertAlmostEqual(r["pnl"], -5.0)

    def test_trailing_breakeven_then_stop(self):
        s = app.save_settings({"trailing": True, "be_trigger_atr": 0.5, "trail_atr": 1.0}); self._open()
        # closed candle reaches +1.4 ATR (no TP at +1.5) -> SL to breakeven, then trails to 101.4 - 1.0 = 100.4
        set_kl("BTCUSDT", "5m", [(T0, 100, 100, 100), (T0 + 300, 101.4, 99.8, 101.2), (T0 + 600, 101.3, 101.0, 101.1)])
        app.update_paper([], s); r = rows()[0]
        self.assertEqual(r["status"], "OPEN"); self.assertEqual(r["be_done"], 1); self.assertAlmostEqual(r["sl"], 100.4)
        set_kl("BTCUSDT", "5m", [(T0, 100, 100, 100), (T0 + 300, 101.4, 99.8, 101.2), (T0 + 600, 101.3, 100.3, 100.5)])
        closed = []; app.update_paper(closed, s); r = rows()[0]
        self.assertEqual(r["status"], "TRAIL"); self.assertAlmostEqual(r["pnl"], 2.0); self.assertEqual(len(closed), 1)

    def test_expired(self):
        self._open(); 
        with app.db() as c: c.execute("UPDATE paper SET ts=?", (time.time() - 61 * 300,))
        set_kl("BTCUSDT", "5m", [(T0, 100, 100, 100), (T0 + 300, 100.5, 99.5, 100.2)])
        app.update_paper([], self.s); r = rows()[0]
        self.assertEqual(r["status"], "EXPIRED"); self.assertAlmostEqual(r["pnl"], 1.0)

    def test_new_trade_allowed_after_close(self):
        s = app.save_settings({"trailing": False}); self._open()
        set_kl("BTCUSDT", "5m", [(T0, 100, 100, 100), (T0 + 300, 101.6, 99.5, 101.0)]); app.update_paper([], s)
        app.STATE["paper_prev"].clear(); self.put("BTCUSDT", "5m", sig(conf=90, entry=101.0)); app.open_paper_trades(s)
        self.assertEqual([r["status"] for r in rows()], ["TP", "OPEN"])

    def test_winrate_stats_ignore_trade_rows(self):
        self._open(); st = app.paper_stats()["24h"]["total"]
        self.assertEqual(st["OPEN"], 0)   # only trade row exists; win-rate tracker counts signal rows only

    def test_paper_command_and_text(self):
        self._open()
        txt = app.extra_command("/paper", [])
        self.assertIn("Paper trading ON", txt); self.assertIn("$500", txt); self.assertIn("BTCUSDT", txt)
        app.extra_command("/paper", ["size", "300"]); app.extra_command("/paper", ["90"]); app.extra_command("/paper", ["off"])
        s = app.get_settings(); self.assertEqual((s["paper_size_usdt"], s["paper_min_conf"], s["paper_trade"]), (300.0, 90, False))
        self.assertIn("Paper trades", app.extra_command("/stats", []))
        self.assertFalse(app.get_settings()["auto_trade"])

    def test_no_binance_calls(self):
        called = []
        async def boom(*a, **k): called.append(a); raise AssertionError("no real orders")
        app.signed = app.fsigned = boom
        self.put("BTCUSDT", "5m", sig(conf=95)); asyncio.run(app.record_and_alert(self.s)); app.update_paper([], self.s)
        self.assertEqual(called, [])

if __name__ == "__main__": unittest.main()
