# Scalping Bot for Termux (Telegram bot + Mini App)
Scans the top 200 Binance USDT-M perpetual futures (by 24h volume, refreshed hourly; spot fallback if futures are blocked)
on 1m / 5m / 15m with EMA 9/21/50, RSI, MACD histogram, VWAP, Bollinger, volume spikes, candle patterns (engulfing,
hammer/pin bar, shooting star, inside-bar breakout), ATR volatility filter and higher-timeframe confirmation.
LONG / SHORT / NEUTRAL with confidence 0-100 (= indicator agreement, NOT win probability); 80%+ setups sorted to the top.
ATR-based entry / TP / SL. Manual trades need confirmation. Futures (isolated, 1-10x, default 3x) or spot.
Auto Trade is OFF by default. Not financial advice.
Rate limits: weight-tracked REST (~1000 weight/min of the 2400 futures limit), 1m refreshed every ~15s.

    bash install.sh          # Termux packages + Python deps (pure Python, no compiling)
    nano ~/scalping-bot/.env # add TELEGRAM_BOT_TOKEN, BINANCE_API_KEY, BINANCE_API_SECRET
    bash ~/scalping-bot/start.sh
    bash ~/scalping-bot/stop.sh

## Extra features (v3)
- **Auto alerts** (default ON): Telegram alert when a coin newly crosses 80%+ (or 85/90) on 1m/5m/15m, with entry/TP/SL.
  Cooldown 15 min per coin+tf+side, max 10 alerts/min, optional favorites-only. `/alerts on|off|80|85|90|fav|all`
- **Paper win-rate tracker**: every 80%+ signal is tracked on candles (TP / SL / expired after 60 candles), per tf and side,
  last 24h / 7d. `/stats` and the Stats tab. Paper results only, not real trades.
- **Risk guard**: daily max loss 5 USDT, max 3 open trades, max 20 trades/day (UTC day). Blocks manual + auto. `/risk`
- **Trailing stop / breakeven** (default ON): SL to entry at +0.5×ATR (or 50% of TP distance), then trails 1×ATR.
  Futures: replaces the exchange stop order; otherwise bot-managed exits.
- **Funding / liquidity filter** (default ON): |funding| > 0.1% or 24h volume < 20M USDT is marked ⚠ and skipped by alerts/auto.
- **Favorites**: `/fav add SOL`, `/fav remove SOL`, `/fav list`, ☆ star in the Mini App; favorites are pinned to the top.

## v4 features
- **STRONG confluence**: 1m + 5m + 15m same direction (each >= 60%, configurable) and 1h not opposing. Sorted to the top, 🔥 STRONG badge, `/strong` or `/signals strong`, flagged in alerts.
- **Live prices** via Binance futures WebSocket `!miniTicker@arr` (`/market/stream` path; spot fallback `data-stream.binance.vision`), ~1 s updates,
  auto reconnect with backoff, REST `ticker/price` fallback when the stream is silent. Signals = closed candles + live price on the open candle.
- **Candle-aligned refresh**: 1m every 15 s, 5m every 60 s, 15m every 90 s, 1h every 300 s, all aligned to candle closes (+2 s), 3 retries per coin,
  retry when the new candle is not open yet. **Stale guard**: data older than 2x its interval is marked ⏱ stale and excluded from alerts / auto / manual orders.
- **PnL dashboard**: realized / unrealized, today / 7d / all, wins / losses; Binance futures income (realized, fees, funding) when the key allows. `/pnl`, PnL tab.
- **Market mood**: BTC trend 15m / 1h, LONG vs SHORT % of 200 coins; **BTC guard** (default ±1% in 5m pauses LONG / SHORT alerts + auto for 15 min). `/market`, header in the app.
- **Health monitor + self-healing**: watchdog for scanner loops, WebSocket, Telegram poller, DB; restarts crashed / silent tasks; one Telegram alert per incident
  (30 min cooldown) plus a recovery message. `/health`, Health tab.
- Indicators verified against TA-Lib on live Binance data (EMA / RSI / ATR / Bollinger / VWAP / MACD match to ~1e-11 relative).

Commands: /start /signals [1m|5m|15m|strong] /strong /market /pnl /health /alerts /stats /risk /fav /balance /auto /status /help

Only ONE copy of the bot may run per Telegram token.
