"""
Binance Wave Strategy Bot -- Optimized & Crash-Proof Edition
-------------------------------------------------------------
Spot + Alpha scanning with real-time signal generation & testnet trading.
"""

import os
import time
import math
import json
import hmac
import hashlib
import logging
import requests
import pandas as pd
import numpy as np
from binance.client import Client

# ----------------------------- CONFIG -----------------------------------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "YOUR_BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "YOUR_BINANCE_API_SECRET")

ENABLE_TRADING = os.environ.get("ENABLE_TRADING", "true").lower() == "true"
TESTNET_API_KEY = os.environ.get("TESTNET_API_KEY", "")
TESTNET_API_SECRET = os.environ.get("TESTNET_API_SECRET", "")
TESTNET_BASE = "https://testnet.binance.vision"

ACCOUNT_BUDGET_USDT = 500
TRADE_SIZE_USDT = 100
MAX_CONCURRENT_TRADES = int(ACCOUNT_BUDGET_USDT / TRADE_SIZE_USDT)  # 5
FEE_RATE = 0.001  # 0.1% per side

BINANCE_BASE = "https://api.binance.com"
ALPHA_BASE = "https://www.binance.com"
KLINE_INTERVAL = Client.KLINE_INTERVAL_5MINUTE
KLINE_LIMIT = 220
SCAN_INTERVAL = 15  # Fast 15-second scan interval

VOL_MA_PERIODS = [10, 20, 30, 50]
VOL_MULTIPLIER = 2.0  # RVOL >= 2.0x

RALLY_MIN_BODY_PCT = 0.60  # Minimum 60% candle body
INVALIDATION_RETRACE_PCT = 0.30  # Max 30% pullback retracement

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("wavebot")

client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

# State Storage
_open_positions = {}
_stats = {"realized_pnl_total": 0.0, "trades_closed": 0, "wins": 0, "losses": 0}

# ----------------------------- TELEGRAM ----------------------------------
def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured, skipping send.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            log.error("Telegram send failed: %s %s", r.status_code, r.text)
    except Exception as e:
        log.error("Telegram Exception: %s", e)

# ----------------------------- PAIR FETCHING -----------------------------
def get_spot_symbols():
    try:
        info = client.get_exchange_info()
        return [
            s["symbol"] for s in info["symbols"]
            if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
            and s.get("isSpotTradingAllowed", True)
        ]
    except Exception as e:
        log.error("Error fetching spot symbols: %s", e)
        return []

def get_alpha_tokens(spot_symbols):
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
            alpha_symbols = [item.get("symbol", "").upper() + "USDT" for item in data]
            return [s for s in alpha_symbols if s in spot_symbols]
    except Exception as e:
        log.warning("Alpha category fetch error: %s", e)
    
    fallback_alpha = ["KMNOUSDT", "ONDOUSDT", "VIRTUALUSDT", "HUMAUSDT", "SAGAUSDT", "BOMEUSDT"]
    return [s for s in fallback_alpha if s in spot_symbols]

# ----------------------------- INDICATORS --------------------------------
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()

    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    df["cum_vol"] = df["volume"].cumsum()
    df["cum_vol_price"] = (typical_price * df["volume"]).cumsum()
    df["vwap"] = df["cum_vol_price"] / df["cum_vol"]

    # 50-period SMA Volume for RVOL
    df["vol_ma50"] = df["volume"].rolling(50).mean()
    df["rvol"] = df["volume"] / df["vol_ma50"]

    for p in VOL_MA_PERIODS:
        df[f"vol_ma_{p}"] = df["volume"].rolling(p).mean()

    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()

    candle_range = df["high"] - df["low"]
    candle_range = candle_range.replace(0, 0.00001)
    df["body_pct"] = (df["close"] - df["open"]).abs() / candle_range
    return df

# ----------------------------- STRATEGY LOGIC -----------------------------
def check_strategy(df: pd.DataFrame, symbol: str, category_name: str):
    if len(df) < 210:
        return False, ""

    curr = df.iloc[-1]       # Current active candle
    prev = df.iloc[-2]       # Last closed candle

    # 1. Active Candle must be Green
    if curr["close"] <= curr["open"]:
        return False, ""

    # 2. RVOL Check: Minimum 2x
    rvol_val = max(prev["rvol"], curr["rvol"])
    if rvol_val < VOL_MULTIPLIER or np.isnan(rvol_val):
        return False, ""

    # 3. Moving Averages & VWAP Stack Condition
    # EMA 9 > EMA 20 > VWAP > EMA 200
    cond_ma = (
        (prev["ema9"] > prev["ema20"]) and
        (prev["ema9"] > prev["vwap"]) and
        (prev["ema20"] > prev["vwap"]) and
        (prev["ema9"] > prev["ema200"]) and
        (prev["ema20"] > prev["ema200"]) and
        (prev["vwap"] > prev["ema200"])
    )
    if not cond_ma:
        return False, ""

    # 4. MACD Line > Signal Line
    if prev["macd"] <= prev["macd_signal"]:
        return False, ""

    # 5. Extract Rally & Pullback Phases
    candles = df.iloc[:-1].reset_index(drop=True)

    # Red Pullback Candles
    pullback_candles = []
    idx = len(candles) - 1
    while idx >= 0 and candles.loc[idx, "close"] < candles.loc[idx, "open"]:
        pullback_candles.append(candles.loc[idx])
        idx -= 1

    if len(pullback_candles) == 0:
        return False, ""

    # Green Rally Candles
    rally_candles = []
    while idx >= 0 and candles.loc[idx, "close"] > candles.loc[idx, "open"]:
        rally_candles.append(candles.loc[idx])
        idx -= 1

    if len(rally_candles) < 2:
        return False, ""

    # Body Size >= 60% for all rally candles
    for rc in rally_candles:
        if rc["body_pct"] < RALLY_MIN_BODY_PCT:
            return False, ""

    # Retracement Check (<= 30%)
    rally_low = min(c["low"] for c in rally_candles)
    rally_high = max(c["high"] for c in rally_candles)
    rally_move = rally_high - rally_low

    if rally_move <= 0:
        return False, ""

    pullback_low = min(c["low"] for c in pullback_candles)
    pullback_depth = rally_high - pullback_low
    retracement_pct = (pullback_depth / rally_move) * 100

    if retracement_pct > (INVALIDATION_RETRACE_PCT * 100):
        return False, ""

    # Volume Comparison (Rally Volume >= 1.2x Pullback Volume)
    avg_rally_vol = np.mean([c["volume"] for c in rally_candles])
    avg_pullback_vol = np.mean([c["volume"] for c in pullback_candles])
    if avg_pullback_vol == 0:
        avg_pullback_vol = 0.0001

    vol_ratio = avg_rally_vol / avg_pullback_vol
    if vol_ratio < 1.20:
        return False, ""

    # Check if Breakout occurs (High crosses 1st red candle high)
    pullback_first_high = pullback_candles[0]["high"]
    if curr["high"] <= pullback_first_high:
        return False, ""

    # -------------------------------------------------------------
    # 100% STRATEGY MATCH -> TRIGGER ALERT & TRADING
    # -------------------------------------------------------------
    msg = (
        f"🚨 *STRONG BUY SIGNAL ({category_name})*\n"
        f"-----------------------------------\n"
        f"• *Coin:* `{symbol}`\n"
        f"• *Price:* `{curr['close']}`\n"
        f"• *Timeframe:* `5m`\n"
        f"• *RVOL:* `{rvol_val:.2f}x` (>= 2x)\n"
        f"• *Rally Candles:* `{len(rally_candles)} Green` (Body >= 60%)\n"
        f"• *Retracement:* `{retracement_pct:.1f}%` (<= 30%)\n"
        f"• *Vol Surge Ratio:* `{vol_ratio:.2f}x` (>= 1.2x)\n"
        f"• *Trend Status:* EMA9 > EMA20 > VWAP > EMA200 (MACD Bullish)"
    )

    return True, msg

# ----------------------------- TESTNET TRADING ----------------------------
def _signed_request(method: str, path: str, params: dict):
    if not TESTNET_API_KEY or not TESTNET_API_SECRET:
        return None
    params = dict(params)
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 10000
    query = "&".join(f"{k}={v}" for k, v in params.items())
    signature = hmac.new(TESTNET_API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    query += f"&signature={signature}"
    url = f"{TESTNET_BASE}{path}?{query}"
    headers = {"X-MBX-APIKEY": TESTNET_API_KEY}
    try:
        r = requests.request(method, url, headers=headers, timeout=10)
        if r.status_code != 200:
            log.error("Testnet error [%s]: %s", r.status_code, r.text)
            return None
        return r.json()
    except Exception as e:
        log.error("Testnet exception: %s", e)
        return None

def total_open_trades() -> int:
    return sum(len(v) for v in _open_positions.values())

def open_long(symbol: str):
    if not ENABLE_TRADING:
        return
    if total_open_trades() >= MAX_CONCURRENT_TRADES:
        log.info("Max trades reached (%d/%d), skipping %s", total_open_trades(), MAX_CONCURRENT_TRADES, symbol)
        return

    resp = _signed_request("POST", "/api/v3/order", {
        "symbol": symbol, "side": "BUY", "type": "MARKET",
        "quoteOrderQty": TRADE_SIZE_USDT,
    })
    if resp and "executedQty" in resp:
        qty = float(resp["executedQty"])
        quote_spent = float(resp.get("cummulativeQuoteQty", TRADE_SIZE_USDT))
        _open_positions.setdefault(symbol, []).append({
            "qty": qty, "entry_price": quote_spent / qty,
            "entry_time": time.time()
        })
        log.info("TRADE OPENED on %s | qty: %.4f", symbol, qty)

def close_all_for_symbol(symbol: str):
    if symbol not in _open_positions:
        return
    positions = _open_positions.pop(symbol, [])
    for pos in positions:
        _signed_request("POST", "/api/v3/order", {
            "symbol": symbol, "side": "SELL", "type": "MARKET",
            "quantity": f"{pos['qty']:.8f}".rstrip("0").rstrip(".")
        })
        log.info("TRADE CLOSED on %s", symbol)

# ----------------------------- PROCESS & SCAN -----------------------------
def process_klines(klines_raw):
    df = pd.DataFrame(klines_raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "num_trades", "tb_base_vol", "tb_quote_vol", "ignore"
    ])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return add_indicators(df)

def scan_markets(spot_symbols, alpha_symbols):
    # Scan Alpha Category
    log.info("Scanning Binance Alpha Category...")
    for symbol in alpha_symbols:
        try:
            klines = client.get_klines(symbol=symbol, interval=KLINE_INTERVAL, limit=KLINE_LIMIT)
            df = process_klines(klines)
            signal, msg = check_strategy(df, symbol, "BINANCE ALPHA")
            if signal:
                send_telegram(msg)
                log.info("[ALPHA SIGNAL SENT]: %s", symbol)
        except Exception:
            continue

    # Scan Spot Category
    log.info("Scanning Binance Spot Pairs...")
    for symbol in spot_symbols:
        try:
            klines = client.get_klines(symbol=symbol, interval=KLINE_INTERVAL, limit=KLINE_LIMIT)
            df = process_klines(klines)
            signal, msg = check_strategy(df, symbol, "BINANCE SPOT")
            if signal:
                send_telegram(msg)
                open_long(symbol)
                log.info("[SPOT SIGNAL SENT]: %s", symbol)
        except Exception:
            continue

# ----------------------------- MAIN LOOP ----------------------------------
if __name__ == "__main__":
    send_telegram("🤖 *Crypto Wave Strategy Bot Started Successfully!* Scanning Spot & Alpha...")
    
    spot_pairs = get_spot_symbols()
    alpha_pairs = get_alpha_tokens(spot_pairs)

    log.info("Loaded %d Spot pairs and %d Alpha pairs.", len(spot_pairs), len(alpha_pairs))

    while True:
        try:
            scan_markets(spot_pairs, alpha_pairs)
            time.sleep(SCAN_INTERVAL)
        except KeyboardInterrupt:
            log.info("Bot stopped manually.")
            break
        except Exception as e:
            log.error("[LOOP ERROR]: %s", e)
            time.sleep(10)
