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

Only ONE copy of the bot may run per Telegram token.
