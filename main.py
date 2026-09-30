import os
import time
import requests
import numpy as np
import pandas as pd
from binance.client import Client

# ==========================================
# 1. CONFIGURATION & ENVIRONMENT VARIABLES
# ==========================================
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "YOUR_BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "YOUR_BINANCE_API_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

SCAN_INTERVAL = 15  # 15 seconds fast scanning
TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

def send_telegram_msg(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARNING] Telegram tokens missing.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        res = requests.post(url, json=payload, timeout=10)
        if res.status_code != 200:
            print(f"[ERROR] Telegram Failed: {res.text}")
    except Exception as e:
        print(f"[ERROR] Telegram Connection Exception: {e}")

# ==========================================
# 2. INDICATORS CALCULATION
# ==========================================
def calculate_indicators(df):
    # Exponential Moving Averages (EMA 9, 20, 200)
    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()

    # VWAP (Volume Weighted Average Price)
    tp = (df['high'] + df['low'] + df['close']) / 3
    df['vwap'] = (tp * df['volume']).cumsum() / df['volume'].cumsum()

    # RVOL (Relative Volume vs 50-period SMA Volume)
    df['vol_ma50'] = df['volume'].rolling(window=50).mean()
    df['rvol'] = df['volume'] / df['vol_ma50']

    # MACD (12, 26, 9)
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = ema12 - ema26
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()

    return df

# ==========================================
# 3. UPDATED STRATEGY LOGIC
# ==========================================
def check_strategy(df, symbol, category_name):
    if len(df) < 210:
        return False, ""

    curr = df.iloc[-1]       # Current active (live forming) candle
    prev = df.iloc[-2]       # Last completed 5m candle

    # 1. Instant Trigger: Current candle MUST start/be GREEN (Price > Open)
    if curr['close'] <= curr['open']:
        return False, ""

    # 2. RVOL Check: Minimum 2x
    rvol_val = max(prev['rvol'], curr['rvol'])
    if rvol_val < 2.0 or np.isnan(rvol_val):
        return False, ""

    # 3. Moving Averages & VWAP Stack Condition
    # EMA 9 > EMA 20 > VWAP > EMA 200
    cond_ma = (
        (prev['ema9'] > prev['ema20']) and
        (prev['ema9'] > prev['vwap']) and
        (prev['ema20'] > prev['vwap']) and
        (prev['ema9'] > prev['ema200']) and
        (prev['ema20'] > prev['ema200']) and
        (prev['vwap'] > prev['ema200'])
    )
    if not cond_ma:
        return False, ""

    # 4. MACD Line > Signal Line
    if prev['macd'] <= prev['macd_signal']:
        return False, ""

    # 5. Extract Pullback (Red) & Rally (Green) Candles
    candles = df.iloc[:-1].reset_index(drop=True)

    # Red Pullback Candles
    pullback_candles = []
    idx = len(candles) - 1
    while idx >= 0 and candles.loc[idx, 'close'] < candles.loc[idx, 'open']:
        pullback_candles.append(candles.loc[idx])
        idx -= 1

    if len(pullback_candles) == 0:
        return False, ""  # Pullback red candles required

    # Green Rally Candles (Directly before pullback)
    rally_candles = []
    while idx >= 0 and candles.loc[idx, 'close'] > candles.loc[idx, 'open']:
        rally_candles.append(candles.loc[idx])
        idx -= 1

    # Rule: Minimum 2 Green Candles in Rally (No Body % Limit)
    if len(rally_candles) < 2:
        return False, ""

    # Rule: Retracement MUST be 20% or LESS (Strict Limit)
    rally_low = min(c['low'] for c in rally_candles)
    rally_high = max(c['high'] for c in rally_candles)
    rally_move = rally_high - rally_low

    if rally_move <= 0:
        return False, ""

    pullback_low = min(c['low'] for c in pullback_candles)
    pullback_depth = rally_high - pullback_low
    retracement_pct = (pullback_depth / rally_move) * 100

    if retracement_pct > 20.0:  # Max 20% Retracement
        return False, ""

    # Rule: Pullback Volume MUST be at least 40% lower than Rally Volume
    # (i.e. Pullback Volume <= 60% of Rally Volume)
    avg_rally_vol = np.mean([c['volume'] for c in rally_candles])
    avg_pullback_vol = np.mean([c['volume'] for c in pullback_candles])

    if avg_rally_vol == 0:
        return False, ""

    vol_ratio = avg_pullback_vol / avg_rally_vol
    if vol_ratio > 0.60:  # Must be 40%+ smaller
        return False, ""

    # -------------------------------------------------------------
    # 100% STRATEGY MATCH -> INSTANT TELEGRAM SIGNAL
    # -------------------------------------------------------------
    msg = (
        f"🚨 *INSTANT BUY SIGNAL ({category_name})*\n"
        f"-----------------------------------\n"
        f"• *Coin:* `{symbol}`\n"
        f"• *Price:* `{curr['close']}` (Live Green Start)\n"
        f"• *Timeframe:* `5m`\n"
        f"• *RVOL:* `{rvol_val:.2f}x` (>= 2x)\n"
        f"• *Rally:* `{len(rally_candles)} Green Candles`\n"
        f"• *Retracement:* `{retracement_pct:.1f}%` (<= 20% Max)\n"
        f"• *Pullback Vol Drop:* `{(1 - vol_ratio)*100:.1f}%` (>= 40% Lower)\n"
        f"• *Trend Status:* EMA9 > EMA20 > VWAP > EMA200 (MACD Bullish)"
    )

    return True, msg

# ==========================================
# 4. PAIRS FETCHING (SPOT + ALPHA)
# ==========================================
def get_spot_pairs():
    try:
        info = client.get_exchange_info()
        return [s['symbol'] for s in info['symbols'] if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING']
    except Exception as e:
        print(f"[ERROR] Fetching Spot Pairs: {e}")
        return []

def get_alpha_pairs(spot_pairs):
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "category": "binance-alpha-spotlight",
            "per_page": 250,
            "page": 1
        }
        res = requests.get(url, params=params, timeout=10)
        if res.status_code == 200:
            data = res.json()
            alpha_list = [item.get("symbol", "").upper() + "USDT" for item in data]
            return [s for s in alpha_list if s in spot_pairs]
    except Exception as e:
        print(f"[ERROR] Fetching Alpha Pairs: {e}")
    
    fallback = ["KMNOUSDT", "ONDOUSDT", "VIRTUALUSDT", "HUMAUSDT", "SAGAUSDT", "BOMEUSDT"]
    return [s for s in fallback if s in spot_pairs]

def process_klines(klines_raw):
    df = pd.DataFrame(klines_raw, columns=[
        'time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'qav', 'num_trades', 'tb_base_vol', 'tb_quote_vol', 'ignore'
    ])
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].astype(float)
    return calculate_indicators(df)

def scan_markets(spot_symbols, alpha_symbols):
    # 1. Scan Alpha Category
    print("Scanning Binance Alpha Category Coins...")
    for symbol in alpha_symbols:
        try:
            klines = client.get_klines(symbol=symbol, interval=TIMEFRAME, limit=220)
            df = process_klines(klines)
            signal, msg = check_strategy(df, symbol, "BINANCE ALPHA")
            if signal:
                send_telegram_msg(msg)
                print(f"[ALPHA SIGNAL SENT]: {symbol}")
        except Exception:
            continue

    # 2. Scan Spot Category
    print("Scanning Binance Spot Pairs...")
    for symbol in spot_symbols:
        try:
            klines = client.get_klines(symbol=symbol, interval=TIMEFRAME, limit=220)
            df = process_klines(klines)
            signal, msg = check_strategy(df, symbol, "BINANCE SPOT")
            if signal:
                send_telegram_msg(msg)
                print(f"[SPOT SIGNAL SENT]: {symbol}")
        except Exception:
            continue

# ==========================================
# 5. MAIN EXECUTION LOOP
# ==========================================
if __name__ == "__main__":
    send_telegram_msg("🤖 *Strategy Bot Started!* Monitoring Spot & Alpha Category Pairs...")
    
    spot_pairs = get_spot_pairs()
    alpha_pairs = get_alpha_pairs(spot_pairs)

    print(f"Loaded {len(spot_pairs)} Spot Pairs and {len(alpha_pairs)} Alpha Category Pairs.")

    while True:
        try:
            scan_markets(spot_pairs, alpha_pairs)
            time.sleep(SCAN_INTERVAL)
        except KeyboardInterrupt:
            print("Bot stopped by user.")
            break
        except Exception as e:
            print(f"[LOOP ERROR]: {e}")
            time.sleep(10)
