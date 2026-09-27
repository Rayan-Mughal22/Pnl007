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

SCAN_INTERVAL = 30  # 30 seconds delay between scans
TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

def send_telegram_msg(message):
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
        print(f"[ERROR] Telegram Connection Error: {e}")

# ==========================================
# 2. TECHNICAL INDICATOR CALCULATIONS
# ==========================================
def calculate_indicators(df):
    # EMAs: 9, 20, 200
    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()

    # VWAP (Volume Weighted Average Price)
    tp = (df['high'] + df['low'] + df['close']) / 3
    df['vwap'] = (tp * df['volume']).cumsum() / df['volume'].cumsum()

    # RVOL (Relative Volume - 50 Period SMA Volume)
    df['vol_ma50'] = df['volume'].rolling(window=50).mean()
    df['rvol'] = df['volume'] / df['vol_ma50']

    # MACD (12, 26, 9)
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = ema12 - ema26
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()

    # Body Percentage: |Close - Open| / (High - Low)
    candle_range = df['high'] - df['low']
    candle_range = candle_range.replace(0, 0.00001)
    df['body_pct'] = (df['close'] - df['open']).abs() / candle_range

    return df

# ==========================================
# 3. STRATEGY RULE CHECK
# ==========================================
def check_strategy(df, symbol, category_type):
    if len(df) < 210:
        return False, ""

    curr = df.iloc[-1]       # Current active/opening candle
    prev = df.iloc[-2]       # Last completed 5m candle

    # 1. Current candle MUST be Green (bounce start point)
    if curr['close'] <= curr['open']:
        return False, ""

    # 2. RVOL Check (Minimum 2x)
    rvol_val = max(prev['rvol'], curr['rvol'])
    if rvol_val < 2.0 or np.isnan(rvol_val):
        return False, ""

    # 3. EMA & VWAP Alignment
    # EMA 9 > EMA 20
    # EMA 9 & EMA 20 > VWAP
    # EMA 9, EMA 20, VWAP > EMA 200
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

    # 4. MACD Condition: MACD Line > Signal Line
    if prev['macd'] <= prev['macd_signal']:
        return False, ""

    # 5. Green Rally & Red Pullback Detection
    candles = df.iloc[:-1].reset_index(drop=True)

    # Red Pullback Candles
    pullback_candles = []
    idx = len(candles) - 1
    while idx >= 0 and candles.loc[idx, 'close'] < candles.loc[idx, 'open']:
        pullback_candles.append(candles.loc[idx])
        idx -= 1

    if len(pullback_candles) == 0:
        return False, ""

    # Green Rally Candles (Directly preceding the pullback)
    rally_candles = []
    while idx >= 0 and candles.loc[idx, 'close'] > candles.loc[idx, 'open']:
        rally_candles.append(candles.loc[idx])
        idx -= 1

    # Minimum 2 green candles in rally
    if len(rally_candles) < 2:
        return False, ""

    # Body size >= 60% for each green candle in rally
    for rc in rally_candles:
        if rc['body_pct'] < 0.60:
            return False, ""

    # Retracement Check (<= 30%)
    rally_low = min(c['low'] for c in rally_candles)
    rally_high = max(c['high'] for c in rally_candles)
    rally_move = rally_high - rally_low

    if rally_move <= 0:
        return False, ""

    pullback_low = min(c['low'] for c in pullback_candles)
    pullback_depth = rally_high - pullback_low
    retracement_pct = (pullback_depth / rally_move) * 100

    if retracement_pct > 30.0:
        return False, ""

    # Volume Ratio Check (Rally Vol >= 1.2x Pullback Vol)
    avg_rally_vol = np.mean([c['volume'] for c in rally_candles])
    avg_pullback_vol = np.mean([c['volume'] for c in pullback_candles])
    if avg_pullback_vol == 0:
        avg_pullback_vol = 0.0001

    vol_ratio = avg_rally_vol / avg_pullback_vol
    if vol_ratio < 1.20:
        return False, ""

    # -------------------------------------------------------------
    # ALL CONDITIONS 100% MET -> ALERT
    # -------------------------------------------------------------
    msg = (
        f"🚨 *BUY SIGNAL TRIGGERED*\n"
        f"-----------------------------------\n"
        f"• *Coin:* `{symbol}`\n"
        f"• *Category:* `{category_type}`\n"
        f"• *Price:* `{curr['close']}`\n"
        f"• *Timeframe:* `5m`\n"
        f"• *RVOL:* `{rvol_val:.2f}x` (>= 2x)\n"
        f"• *Rally:* `{len(rally_candles)} Green Candles` (Body >= 60%)\n"
        f"• *Retracement:* `{retracement_pct:.1f}%` (<= 30%)\n"
        f"• *Vol Surge Ratio:* `{vol_ratio:.2f}x` (>= 1.2x)\n"
        f"• *Trend Status:* EMA9 > EMA20 > VWAP > EMA200 (MACD Bullish)"
    )

    return True, msg

# ==========================================
# 4. FETCH SPOT & ALPHA CATEGORY COINS
# ==========================================
def get_spot_pairs():
    """Fetch standard Binance USDT Spot Pairs"""
    try:
        info = client.get_exchange_info()
        return [s['symbol'] for s in info['symbols'] if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING']
    except Exception as e:
        print(f"[ERROR] Fetching Spot Pairs: {e}")
        return []

def get_binance_alpha_pairs(spot_pairs):
    """
    Fetch Binance Alpha / Alpha Spotlight Category Tokens.
    Pulls Alpha token list via public API and maps them to active Binance USDT Spot pairs.
    """
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "category": "binance-alpha-spotlight",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1
        }
        res = requests.get(url, params=params, timeout=10)
        if res.status_code == 200:
            data = res.json()
            alpha_symbols = []
            for item in data:
                symbol = item.get("symbol", "").upper() + "USDT"
                if symbol in spot_pairs:
                    alpha_symbols.append(symbol)
            return list(set(alpha_symbols))
    except Exception as e:
        print(f"[ERROR] Fetching Alpha Category Coins: {e}")
    
    # Fallback list of known Binance Alpha tokens if API call is throttled
    fallback_alpha = ["KMNOUSDT", "ONDOUSDT", "VIRTUALUSDT", "HUMAUSDT", "SAGAUSDT", "BOMEUSDT"]
    return [s for s in fallback_alpha if s in spot_pairs]

def process_klines(klines_raw):
    df = pd.DataFrame(klines_raw, columns=[
        'time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'qav', 'num_trades', 'tb_base_vol', 'tb_quote_vol', 'ignore'
    ])
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)
    return calculate_indicators(df)

def scan_markets(spot_symbols, alpha_symbols):
    # 1. Scan Alpha Category Coins
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

    # 2. Scan General Spot Pairs
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
# 5. MAIN LOOP
# ==========================================
if __name__ == "__main__":
    send_telegram_msg("🤖 *Crypto Scanner Bot Started!* Scanning Spot & Binance Alpha Tokens...")
    
    spot_pairs = get_spot_pairs()
    alpha_pairs = get_binance_alpha_pairs(spot_pairs)

    print(f"Loaded {len(spot_pairs)} Spot Pairs and {len(alpha_pairs)} Binance Alpha Category Tokens.")

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
