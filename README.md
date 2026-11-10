# Scalping Bot for Termux (Telegram bot + Mini App)
Binance spot scalping signals on 1m / 5m / 15m (EMA 9/21, RSI 7, MACD, VWAP, Bollinger, volume spike),
ATR-based entry / TP / SL, Telegram Mini App, manual BUY/SELL with confirm, optional Auto Trade (OFF by default) with OCO exits.
Indicator analysis only. Not financial advice.

    bash install.sh          # Termux packages + Python deps (pure Python, no compiling)
    nano ~/scalping-bot/.env # add TELEGRAM_BOT_TOKEN, BINANCE_API_KEY, BINANCE_API_SECRET
    bash ~/scalping-bot/start.sh
    bash ~/scalping-bot/stop.sh

Only ONE copy of the bot may run per Telegram token.
