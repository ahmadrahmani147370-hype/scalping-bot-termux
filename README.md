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

Commands: /start /signals [1m|5m|15m] /alerts /stats /risk /fav /balance /auto /status /help

Only ONE copy of the bot may run per Telegram token.
