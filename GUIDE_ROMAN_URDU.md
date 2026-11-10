# Scalping Bot ko Android phone (Termux) par chalane ka tareeqa

## 1. Termux install karein
- Termux **F-Droid** se install karein (https://f-droid.org/packages/com.termux/). Play Store wala Termux purana hai, woh mat lein.
- Termux kholein aur pehli dafa thora intezar karein jab tak `$` nazar aa jaye.

## 2. Bot install karein (ek command)
Termux mein ye command paste karke Enter dabayein:

```
curl -fsSL https://raw.githubusercontent.com/ahmadrahmani147370-hype/scalping-bot-termux/main/install.sh | bash
```

Ye python, cloudflared aur baqi cheezein khud install kar dega (5 se 10 minute lag sakte hain). Agar beech mein koi sawal aaye to `y` likh kar Enter dabayein.

**Agar link na chale** (zip file wala tareeqa):
1. `scalping-bot-termux.zip` phone ke **Download** folder mein save karein.
2. Termux mein: `termux-setup-storage` likhein aur storage ki permission **Allow** karein.
3. Phir ye likhein:
```
cd ~ && pkg install -y unzip && unzip -o ~/storage/downloads/scalping-bot-termux.zip && bash ~/scalping-bot-termux/install.sh
```

## 3. Apni keys daalein
```
nano ~/scalping-bot/.env
```
- `paste_your_...` ki jagah apna Telegram bot token, Binance API key aur Binance secret likhein.
- Save: **Ctrl + O**, phir **Enter**. Bahar niklein: **Ctrl + X**. (Ctrl button Termux ke neeche wali extra keys mein hai.)
- Ye file kisi ko na bhejein.

## 4. Bot start karein
```
bash ~/scalping-bot/start.sh
```
- Screen par "Mini app: https://....trycloudflare.com" aayega. Bot ka menu button khud set ho jayega.
- Ab Telegram mein apne bot ko **/start** bhejein. Jo pehle /start bhejega wohi bot ka malik (owner) banega, is liye foran bhejein.
- Bot band karna ho: `bash ~/scalping-bot/stop.sh`
- Har dafa phone restart ho ya Termux band ho, dobara `bash ~/scalping-bot/start.sh` chalayein. Har start par naya link banta hai aur menu button khud update ho jata hai.

## 5. Bot ko chalta rakhein
- Termux ko band (swipe) na karein. Notification mein "wake lock" nazar aana chahiye.
- Phone Settings > Apps > Termux > Battery > **Unrestricted / Don't optimize** kar dein.
- Internet on rakhein. Agar mini app ka link na bane to `cat ~/scalping-bot/logs/tunnel.log` dekhein. Mobile data aur WiFi badal kar dobara start karein.

## 6. Zaroori baatein
- Ek token par sirf **ek** bot chal sakta hai. Phone par chalane se pehle baaki jagah (doosre phone, purana Termux bot) band karein.
- Binance key mein **Withdrawals hamesha OFF** rakhein. Trade karna ho to sirf "Spot & Margin Trading" ON karein.
- Auto Trade shuru mein OFF hota hai. Pehle chhoti amount (10 USDT) se test karein.
- Signals sirf indicators (EMA, RSI, MACD, VWAP, Bollinger, volume) par hain. Ye koi guarantee nahi, nuqsan bhi ho sakta hai.

## 7. Naya version (update) kaise lein
Bot band karein, phir wohi install command dobara chalayein. Aapki `.env` (keys) aur `data.db` (history) safe rehti hain:
```
bash ~/scalping-bot/stop.sh
curl -fsSL https://raw.githubusercontent.com/ahmadrahmani147370-hype/scalping-bot-termux/main/install.sh | bash
bash ~/scalping-bot/start.sh
```
Naye version mein `websockets` package bhi install hota hai (pure Python, compiler ki zaroorat nahi). Live price har ~1 second update hoti hai.

## 8. Saari Telegram commands
| Command | Kaam |
|---|---|
| `/start` | Bot shuru, mini app ka button |
| `/signals` · `/signals 1m` · `/signals 5m` · `/signals 15m` | 80%+ LONG/SHORT signals, Entry / TP / SL ke saath |
| `/signals strong` ya `/strong` | 🔥 STRONG confluence: 1m + 5m + 15m teeno same direction (har TF 60%+), 1h opposite nahi |
| `/market` | Market mood: BTC trend (15m/1h), 200 coins mein LONG vs SHORT %, BTC guard status |
| `/pnl` | PnL: aaj / 7 din / all time, wins/losses, open trades ka unrealized PnL, Binance futures income (agar key allow kare) |
| `/health` | Har timeframe ke data ki age, coverage %, WebSocket status, rate-limit weight, errors, uptime |
| `/alerts on` · `/alerts off` | Auto alerts on/off (default ON) |
| `/alerts 80` · `/alerts 85` · `/alerts 90` | Alert threshold |
| `/alerts fav` · `/alerts all` | Sirf favorites ke alerts / sab coins |
| `/stats` | Paper win-rate (24h / 7d, har TF aur LONG/SHORT) — ye paper results hain, real trades nahi |
| `/ultra` · `/ultra on\|off` | ⚡ ULTRA STRONG: sab se sakht filter, din mein max 5 signals, alag paper win-rate. Backtest mein bhi fees ke baad nuqsan — guarantee nahi |
| `/paper` · `/paper on` · `/paper off` · `/paper 85` · `/paper size 500` | 🧪 Paper trades: open aur band hue trades, PnL USDT aur %, threshold aur size badlein |
| `/risk` | Risk guard: daily loss limit, open trades, trades per day |
| `/fav add SOL` · `/fav remove SOL` · `/fav list` | Favorites (mini app mein ☆ star bhi) |
| `/balance` | Spot aur futures balance |
| `/auto on` · `/auto off` | Auto trade (default OFF) |
| `/status` | Scanner aur API key status |
| `/help` | Ye list |

## 9. Features aur defaults
- **Auto alerts** (ON, 80%): naya 80%+ signal aate hi Telegram message. Ek coin+TF+side par 15 min cooldown, max 10 alerts/minute. STRONG coins par "🔥 STRONG" likha hota hai.
- **Paper win-rate**: har 80%+ signal ko TP / SL / expired (60 candles) tak track karta hai.
- **Risk guard**: daily max loss 5 USDT, max 3 open trades, max 20 trades/din (din UTC 00:00 = Pakistan 05:00 baje badalta hai). Manual aur auto dono rukte hain.
- **Trailing stop / breakeven** (ON): +0.5×ATR profit par SL entry par, phir 1×ATR peeche trail.
- **Funding / liquidity filter** (ON): funding 0.1% se zyada ya 24h volume 20M USDT se kam ho to ⚠ aur alerts/auto skip.
- **Favorites**: ☆ star, list mein upar pinned.
- **STRONG confluence**: 1m+5m+15m same direction + 1h opposite na ho → list mein sab se upar, mini app mein 🔥 STRONG badge, "🔥 Strong" filter.
- **Live price + stale guard**: Binance futures WebSocket se har ~1s price. Data apne refresh time ke 2 guna se purana ho to "⏱ stale" — us par alert/auto nahi hota. WebSocket band ho to bot khud REST se price leta hai aur reconnect karta hai.
- **Market mood + BTC guard** (ON): BTC 5 minute mein -1% gire to LONG alerts/auto 15 min pause, +1% chadhe to SHORT pause. Settings mein badal sakte hain.
- **PnL dashboard**: mini app ka PnL tab aur `/pnl`.
- **Health monitor**: scanner, WebSocket, Telegram poller, database par nazar. Koi part ruk jaye to bot khud restart karta hai, aur Telegram par ek dafa alert + theek hone par "Recovered" message bhejta hai (30 min cooldown). Mini app ka Health tab aur `/health`.

Signals sirf indicators hain — confidence ka matlab jeetne ka chance nahi. Pehle chhoti amount se test karein.

## 9. Paper trading (asal paise nahi)
- Bot khud **paper (naqli) trade** kholta hai jab kisi coin ka signal **85% ya us se upar** ho jaye. Har signal par **$500** ka paper position.
- **Binance par koi order nahi jata.** Asal Auto Trade alag hai aur OFF hi rehta hai.
- Wohi filters jo alerts mein hain: purana (stale) data nahi, ⚠ funding / kam volume wale coin nahi, BTC guard pause wali side nahi.
  Bot start hote hi purane 85% signals par trade nahi khulta, sirf naye signals par.
- Ek coin + ek timeframe par ek waqt mein sirf ek paper trade. TP/SL wohi ATR wale, breakeven/trailing bhi.
  Agar ek hi candle mein TP aur SL dono lagein to SL gina jata hai. 60 candles ke baad trade expire.
- Trade khulne aur band hone par Telegram message aata hai, PnL USDT aur % ke saath (fees shamil nahi).
- `/paper` se open aur band trades dekhein. Mini App mein **Stats** tab par "Paper trades", aur **Settings** mein on/off, % aur size.

