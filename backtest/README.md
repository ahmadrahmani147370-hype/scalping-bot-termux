# Offline backtester (signal quality)
Reuses the bot's own `analyze_tf` / `combine` from `../app.py` on Binance USDT-M 1m klines (public data.binance.vision zips),
aggregated to 5m / 15m / 1h. Signals evaluated at each candle close with a bot-style 99-candle window (98 closed + open candle;
higher TFs use their partially formed candle built from 1m). Outcome = first touch of TP / SL on later candles, same-candle both = SL,
expire after 60 candles; 0.12% round-trip cost (0.08% taker fees + 0.04% slippage).
Needs numpy (own venv): `python -m venv .venv && .venv/bin/pip install numpy -r ../requirements.txt`.
1. download zips into data/ (see urls in the report) -> `python load.py` (npy/)   2. `python gen.py btc && python gen.py all` (feat/, ~15 min, 6 cores)
3. `python base.py` (current logic) · `python search2.py && python pick2.py` (filter grid, IS halves x coin halves) · `python table.py` · `python ultra.py`
Run with APP_DIR=/path/to/bot if not /workspace/scalping-bot.
