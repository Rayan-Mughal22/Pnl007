"""
Binance Wave Strategy Bot -- REAL-TIME (WebSocket) edition
-------------------------------------------------------------
Spot + Alpha scanning, Spot demo trading on Binance Testnet.

WHY THIS VERSION EXISTS
The earlier version re-fetched every coin's candles over REST every ~1
minute, which meant a full cycle took ~7 minutes across ~1150+ symbols --
entries could land minutes after the real breakout, at a much worse price.
This version instead opens a live WebSocket connection to Binance (one for
Spot, one for Alpha) and reacts the instant a price update arrives -- no
polling delay. Historical candles are still fetched once at startup (and
once an hour on refresh) over REST, just to seed enough history for
EMA200/VWAP/volume-MA to be meaningful; after that, everything is event-driven.

STRATEGY (unchanged from the polling version)
Trend filter: EMA9 > EMA20 > VWAP > EMA200
Volume (rally1 formation only): RVOL >= 2x vs. ANY ONE of the 10/20/30/50
                                 period volume moving averages
MACD: MACD line above its signal line

Rally 1: 2+ consecutive green candles, real body (>= RALLY_MIN_BODY_PCT),
         rising volume, with trend + RVOL + MACD holding on the latest candle.
Pullback: 1-3+ red candles after rally 1. Invalidated if a pullback candle's
          volume gets too close to/exceeds the rally's own average volume,
          OR if retracement reaches INVALIDATION_RETRACE_PCT (30%) of the
          rally's range.
Entry (rally 2, then rally 3 once more): the INSTANT any green candle's high
         crosses the pullback's first red candle's high (wick included),
         with volume showing at least some increase vs. the immediately
         preceding candle -- checked live, not just after candle close.
Exit: the moment the first red candle appears right after the rally you
      entered on -- take whatever profit built up.

MONEY MANAGEMENT
$500 demo budget, $50/trade, max 10 concurrent trades across all coins.
Fees: Binance's standard 0.1%-per-side fee is subtracted from PnL even
though the testnet itself charges 0%, so numbers reflect real-world cost.

Alpha tokens get alerts only (no demo trading -- Alpha doesn't use the
classic order book Testnet supports), but now also get real-time WebSocket
detection, same as Spot.
"""

import os
import time
import math
import json
import hmac
import hashlib
import logging
import asyncio
import resource
import threading
import requests
import pandas as pd
import numpy as np
import websockets
from concurrent.futures import ThreadPoolExecutor

# ----------------------------- CONFIG -----------------------------------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

ENABLE_TRADING = os.environ.get("ENABLE_TRADING", "true").lower() == "true"
TESTNET_API_KEY = os.environ.get("TESTNET_API_KEY", "")
TESTNET_API_SECRET = os.environ.get("TESTNET_API_SECRET", "")
TESTNET_BASE = "https://testnet.binance.vision"

ACCOUNT_BUDGET_USDT = 500
TRADE_SIZE_USDT = 50
MAX_CONCURRENT_TRADES = int(ACCOUNT_BUDGET_USDT / TRADE_SIZE_USDT)  # 10
FEE_RATE = 0.001  # Binance standard spot trading fee: 0.1% per side (entry + exit)

BINANCE_BASE = "https://api.binance.com"
ALPHA_BASE = "https://www.binance.com"
SPOT_WS_BASE = "wss://stream.binance.com:9443/stream"
ALPHA_WS_BASE = "wss://nbstream.binance.com/w3w/wsa/stream/stream"
KLINE_INTERVAL = "5m"
KLINE_LIMIT = 260
QUOTE_ASSET = "USDT"
MAX_HISTORY_ROWS = 300           # trimmed window kept per symbol after seeding
SEED_WORKERS = 24                # parallel REST calls during startup seeding
SYMBOL_REFRESH_SECONDS = 3600    # re-seed + reconnect hourly to pick up new/delisted symbols
STREAMS_PER_SUBSCRIBE = 200      # batch size for SUBSCRIBE messages

VOL_MA_PERIODS = [10, 20, 30, 50]
VOL_MULTIPLIER = 2.0             # RVOL threshold vs. ANY ONE of the MA periods above

RALLY_MIN_BODY_PCT = 0.4         # min body % for a green rally candle
PULLBACK_MAX_VOL_RATIO = 0.6     # pullback candle volume must be <= 60% of avg rally candle volume
INVALIDATION_RETRACE_PCT = 0.30  # pullback retrace vs. rally's range must stay under 30%
MAX_RALLY_WATCH_CANDLES = 3      # breakout must happen within this many new green candles

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("wavebot")

# symbol -> list of open positions: [{"qty":.., "entry_price":.., "entry_fee":.., "entry_time":.., "stage":..}]
_open_positions = {}
_entries_taken = {}  # f"{symbol}-{stage}-{candle_open_time}" -> timestamp added (dedup guard)
_trade_history = []  # capped list of closed-trade records, for the performance breakdown
TRADE_HISTORY_MAX = 200
BREAKDOWN_EVERY_N_TRADES = 50  # send a performance breakdown after every N closed trades

# ----------------------------- PERSISTED STATE -----------------------------
# Everything above is in-memory only, which is wiped on every restart --
# including the watchdog's own restarts, and every time new code is
# deployed (Railway's disk is NOT persisted across redeploys without a paid
# Volume). That includes _open_positions itself: if a position was open
# when a redeploy happened, the bot would completely forget it existed --
# no exit, no PnL, nothing, even though the position was still live on the
# exchange. So _stats and _open_positions are persisted together, in a
# pinned Telegram message (using infrastructure that's already set up, no
# Railway Volume needed) -- kept deliberately small so it fits Telegram's
# message size limit. _trade_history is bulkier (needed for the
# performance breakdown) and only financially-inert analytics, so it's only
# persisted to the local JSON file backup -- fine for same-deployment
# restarts, may reset on a full redeploy, which doesn't matter financially.
PNL_STATE_PATH = os.environ.get("PNL_STATE_PATH", "pnl_state.json")
_stats = {"realized_pnl_total": 0.0, "trades_closed": 0, "wins": 0, "losses": 0}
_stats_message_id = None  # the pinned Telegram message we keep editing
STATS_MARKER = "STATS_JSON:"


def _persisted_payload_compact() -> dict:
    """Small payload synced to the pinned Telegram message -- stats + open
    positions only, kept under Telegram's message size limit."""
    return {"stats": _stats, "open_positions": _open_positions}


def _persisted_payload_full() -> dict:
    """Everything, including trade history -- written to the local file
    backup only (never sent to Telegram, could get large)."""
    payload = _persisted_payload_compact()
    payload["trade_history"] = _trade_history
    return payload


def _apply_persisted_payload(data: dict):
    if "stats" in data:
        _stats.update(data["stats"])
    if "open_positions" in data and isinstance(data["open_positions"], dict):
        _open_positions.clear()
        _open_positions.update(data["open_positions"])
    if "trade_history" in data and isinstance(data["trade_history"], list):
        _trade_history.clear()
        _trade_history.extend(data["trade_history"])


def _save_state_local():
    try:
        dirpath = os.path.dirname(PNL_STATE_PATH)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)
        with open(PNL_STATE_PATH, "w") as f:
            json.dump(_persisted_payload_full(), f)
    except Exception as e:
        log.warning("Could not save local state backup: %s", e)


def _sync_state_to_telegram():
    global _stats_message_id
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    open_count = sum(len(v) for v in _open_positions.values())
    text = (
        "Ã°ÂŸÂ“ÂŒ Bot state (auto-updated -- please don't delete or unpin this message)\n"
        f"Lifetime PnL: {_stats['realized_pnl_total']:.2f} USDT\n"
        f"Trades: {_stats['trades_closed']} (W:{_stats['wins']} L:{_stats['losses']})\n"
        f"Open positions right now: {open_count}\n"
        f"{STATS_MARKER}{json.dumps(_persisted_payload_compact())}"
    )
    base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    try:
        if _stats_message_id is None:
            r = requests.post(f"{base}/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
            result = r.json().get("result", {})
            _stats_message_id = result.get("message_id")
            if _stats_message_id:
                requests.post(f"{base}/pinChatMessage", data={
                    "chat_id": TELEGRAM_CHAT_ID, "message_id": _stats_message_id, "disable_notification": True
                }, timeout=10)
        else:
            requests.post(f"{base}/editMessageText", data={
                "chat_id": TELEGRAM_CHAT_ID, "message_id": _stats_message_id, "text": text
            }, timeout=10)
    except Exception as e:
        log.warning("Could not sync state to Telegram: %s", e)


def _load_state_from_telegram() -> bool:
    """Recover _stats + _open_positions (and which message to keep editing)
    from the pinned Telegram message. Returns True if it found and loaded one."""
    global _stats_message_id
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getChat",
                          params={"chat_id": TELEGRAM_CHAT_ID}, timeout=10)
        pinned = r.json().get("result", {}).get("pinned_message")
        if not pinned or STATS_MARKER not in pinned.get("text", ""):
            return False
        json_str = pinned["text"].split(STATS_MARKER, 1)[1].strip()
        loaded = json.loads(json_str)
        _apply_persisted_payload(loaded)
        _stats_message_id = pinned.get("message_id")
        return True
    except Exception as e:
        log.warning("Could not load state from pinned Telegram message: %s", e)
        return False


def _load_stats():
    if _load_state_from_telegram():
        log.info("State recovered from pinned Telegram message: stats=%s | open_positions=%s", _stats, _open_positions)
        return
    try:
        if os.path.exists(PNL_STATE_PATH):
            with open(PNL_STATE_PATH, "r") as f:
                _apply_persisted_payload(json.load(f))
            log.info("State recovered from local backup file: stats=%s | open_positions=%s", _stats, _open_positions)
            return
    except Exception as e:
        log.warning("Could not load local state backup either: %s", e)
    log.info("No prior state found anywhere -- starting fresh: %s", _stats)


def _save_stats():
    _save_state_local()
    _sync_state_to_telegram()


def _entry_already_taken(symbol: str, stage: str, candle_key) -> bool:
    return f"{symbol}-{stage}-{candle_key}" in _entries_taken


def _mark_entry_taken(symbol: str, stage: str, candle_key):
    _entries_taken[f"{symbol}-{stage}-{candle_key}"] = time.time()


def _prune_old_entries():
    cutoff = time.time() - 24 * 3600
    for k in [k for k, ts in _entries_taken.items() if ts < cutoff]:
        del _entries_taken[k]


# ----------------------------- TELEGRAM ----------------------------------
def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured, message suppressed: %s", message)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
        if r.status_code != 200:
            log.error("Telegram send failed: %s %s", r.status_code, r.text)
    except Exception as e:
        log.error("Telegram send exception: %s", e)


# ----------------------------- REST: SYMBOL DISCOVERY + SEEDING ------------
def get_usdt_symbols():
    r = requests.get(f"{BINANCE_BASE}/api/v3/exchangeInfo", timeout=15)
    r.raise_for_status()
    data = r.json()
    return [
        s["symbol"] for s in data["symbols"]
        if s["quoteAsset"] == QUOTE_ASSET and s["status"] == "TRADING"
        and s.get("isSpotTradingAllowed", True)
    ]


def get_alpha_tokens():
    """Returns list of dicts: {"alpha_symbol": "ALPHA_175USDT", "name": "TOKEN"}"""
    url = f"{ALPHA_BASE}/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return []
        data = r.json().get("data", [])
        out = []
        for t in data:
            alpha_id = t.get("alphaId")
            symbol = t.get("symbol")
            if alpha_id is None or not symbol:
                continue
            out.append({"alpha_symbol": f"ALPHA_{alpha_id}USDT", "name": symbol})
        return out
    except Exception as e:
        log.warning("Failed to fetch Alpha token list: %s", e)
        return []


def _parse_klines(raw):
    if not raw or len(raw) < 60:
        return None
    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df


def get_spot_klines(symbol: str):
    params = {"symbol": symbol, "interval": KLINE_INTERVAL, "limit": KLINE_LIMIT}
    r = requests.get(f"{BINANCE_BASE}/api/v3/klines", params=params, timeout=10)
    if r.status_code != 200:
        return None
    return _parse_klines(r.json())


def get_alpha_klines(alpha_symbol: str):
    url = f"{ALPHA_BASE}/bapi/defi/v1/public/alpha-trade/klines"
    params = {"symbol": alpha_symbol, "interval": KLINE_INTERVAL, "limit": KLINE_LIMIT}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get("success"):
            return None
        return _parse_klines(data.get("data", []))
    except Exception as e:
        log.warning("Failed to fetch Alpha klines for %s: %s", alpha_symbol, e)
        return None


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

    for p in VOL_MA_PERIODS:
        df[f"vol_ma_{p}"] = df["volume"].rolling(p).mean()

    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()

    df["body_pct"] = (df["close"] - df["open"]) / df["open"] * 100
    return df


def _best_rvol_ratio(row):
    best_period, best_ratio = None, 0.0
    for p in VOL_MA_PERIODS:
        ma = row.get(f"vol_ma_{p}")
        if ma is not None and not math.isnan(ma) and ma > 0:
            ratio = row["volume"] / ma
            if ratio > best_ratio:
                best_ratio = ratio
                best_period = p
    return best_period, best_ratio


def _rvol_ok(row) -> bool:
    _, best_ratio = _best_rvol_ratio(row)
    return best_ratio >= VOL_MULTIPLIER


def _trend_ok(row) -> bool:
    return row["ema9"] > row["ema20"] > row["vwap"] > row["ema200"]


def _macd_ok(row) -> bool:
    return row["macd"] > row["macd_signal"]


def _avg_vol(cands):
    return sum(c["volume"] for c in cands) / len(cands) if cands else 0


def _row_vol_ma_lookup(row) -> dict:
    return {p: row.get(f"vol_ma_{p}") for p in VOL_MA_PERIODS}


def _rvol_ok_against(volume, vol_ma_lookup) -> bool:
    """Same 'RVOL >= 2x vs ANY ONE of the 10/20/30/50-period MAs' check as
    _rvol_ok, but works off plain values so it's usable on live ticks too
    (where we don't have a fresh pandas row, just the latest closed
    candle's already-computed MA values as the reference)."""
    if not vol_ma_lookup:
        return False
    for p in VOL_MA_PERIODS:
        ma = vol_ma_lookup.get(p)
        if ma and ma > 0 and volume / ma >= VOL_MULTIPLIER:
            return True
    return False


def _breakout_confirmed(high, volume, pullback_first_high, prev_volume, vol_ma_lookup=None) -> bool:
    if high <= pullback_first_high:
        return False
    if prev_volume is None or volume <= prev_volume:
        return False
    if not _rvol_ok_against(volume, vol_ma_lookup):
        return False
    return True

def _entry_detail(row) -> dict:
    period, ratio = _best_rvol_ratio(row)
    return {
        "rvol_ratio": ratio,
        "rvol_period": period,
        "macd": row["macd"],
        "macd_signal": row["macd_signal"],
        "macd_above_signal": row["macd"] > row["macd_signal"],
        "ema9": row["ema9"],
        "ema20": row["ema20"],
        "vwap": row["vwap"],
        "ema200": row["ema200"],
    }


def _detail_line(detail: dict) -> str:
    """One consistent, full-detail line used everywhere a signal/trade is
    logged -- RVOL, MACD, the EMA9>EMA20>VWAP>EMA200 trend stack, and the
    pullback's actual retracement % (vs. the 30% ceiling -- lower is
    better), each value shown explicitly."""
    retrace = detail.get("pullback_retrace_pct")
    retrace_str = f" | Pullback retrace: {retrace:.1f}% of rally (max allowed {INVALIDATION_RETRACE_PCT*100:.0f}%)" if retrace is not None else ""
    return (
        f"RVOL={detail['rvol_ratio']:.2f}x (vs {detail['rvol_period']}-period MA) | "
        f"MACD {'above' if detail['macd_above_signal'] else 'below'} signal "
        f"(macd={detail['macd']:.6f}, signal={detail['macd_signal']:.6f}) | "
        f"Trend: EMA9={detail['ema9']:.6f} > EMA20={detail['ema20']:.6f} > "
        f"VWAP={detail['vwap']:.6f} > EMA200={detail['ema200']:.6f}"
        f"{retrace_str}"
    )



# ----------------------------- LIVE INDICATORS -------------------------------
def _best_rvol_ratio_from_values(volume, vol_ma_lookup):
    best_period, best_ratio = None, 0.0
    for p in VOL_MA_PERIODS:
        ma = vol_ma_lookup.get(p) if vol_ma_lookup else None
        if ma and ma > 0:
            ratio = float(volume) / float(ma)
            if ratio > best_ratio:
                best_period, best_ratio = p, ratio
    return best_period, best_ratio


def _live_indicator_row(state, o, h, l, c, v):
    df = state['df']
    if df is None or len(df) < 210:
        return None
    closes = pd.concat([df['close'], pd.Series([float(c)])], ignore_index=True)
    ema9 = closes.ewm(span=9, adjust=False).mean().iloc[-1]
    ema20 = closes.ewm(span=20, adjust=False).mean().iloc[-1]
    ema200 = closes.ewm(span=200, adjust=False).mean().iloc[-1]
    typical = (df['high'] + df['low'] + df['close']) / 3.0
    base_vol = float(df['volume'].sum())
    base_vp = float((typical * df['volume']).sum())
    live_typical = (float(h) + float(l) + float(c)) / 3.0
    total_vol = base_vol + float(v)
    vwap = (base_vp + live_typical * float(v)) / total_vol if total_vol > 0 else float(df.iloc[-1]['vwap'])
    ema12 = closes.ewm(span=12, adjust=False).mean()
    ema26 = closes.ewm(span=26, adjust=False).mean()
    macd_series = ema12 - ema26
    macd = float(macd_series.iloc[-1])
    macd_signal = float(macd_series.ewm(span=9, adjust=False).mean().iloc[-1])
    vol_ma_lookup = {p: (float(df['volume'].tail(p).mean()) if not pd.isna(df['volume'].tail(p).mean()) else None) for p in VOL_MA_PERIODS}
    return {
        'ema9': float(ema9), 'ema20': float(ema20), 'ema200': float(ema200), 'vwap': float(vwap),
        'macd': macd, 'macd_signal': macd_signal, 'macd_above_signal': macd > macd_signal,
        'trend_ok': ema9 > ema20 > vwap > ema200, 'vol_ma_lookup': vol_ma_lookup,
    }

# ----------------------------- WAVE STATE MACHINE --------------------------
def run_state_machine(df: pd.DataFrame):
    """Build a future live-breakout watch from CLOSED candles only."""
    n = len(df)
    if n < 210:
        return None, None
    last_idx = n - 1
    start_idx = max(0, n - 260)
    phase = 'WAIT_RALLY1'
    rally_candles = []
    rally_range = None
    pullback_candles = []
    pullback_first_high = None
    pullback_max_retrace_pct = 0.0
    watch_candles = []
    stage = None
    for i in range(start_idx, last_idx + 1):
        row = df.iloc[i]
        green = row['close'] > row['open']
        red = row['close'] < row['open']
        body = abs(row['body_pct'])
        if phase == 'WAIT_RALLY1':
            if green and body >= RALLY_MIN_BODY_PCT:
                if rally_candles and row['volume'] <= rally_candles[-1]['volume']:
                    rally_candles = [row]
                else:
                    rally_candles.append(row)
                if len(rally_candles) >= 2 and _trend_ok(row) and _rvol_ok(row) and _macd_ok(row):
                    rally_range = (min(x['low'] for x in rally_candles), max(x['high'] for x in rally_candles))
                    phase = 'PULLBACK'
                    pullback_candles = []
                    pullback_max_retrace_pct = 0.0
                    stage = 'rally2'
            else:
                rally_candles = []
        elif phase == 'PULLBACK':
            if red:
                pullback_candles.append(row)
                size = rally_range[1] - rally_range[0]
                retrace = (rally_range[1] - row['low']) / size if size > 0 else 0
                pullback_max_retrace_pct = max(pullback_max_retrace_pct, retrace)
                avg_rally_vol = _avg_vol(rally_candles)
                if retrace >= INVALIDATION_RETRACE_PCT or (avg_rally_vol > 0 and row['volume'] >= PULLBACK_MAX_VOL_RATIO * avg_rally_vol):
                    phase = 'WAIT_RALLY1'; rally_candles = []
            elif green:
                if not pullback_candles:
                    rally_candles.append(row)
                else:
                    pullback_first_high = pullback_candles[0]['high']
                    watch_candles = [row]
                    phase = 'WATCH_BREAKOUT'
            else:
                phase = 'WAIT_RALLY1'; rally_candles = []
        elif phase == 'WATCH_BREAKOUT':
            if green:
                watch_candles.append(row)
                if row['high'] > pullback_first_high:
                    phase = 'WAIT_RALLY1'; rally_candles = []
            elif red:
                phase = 'WAIT_RALLY1'; rally_candles = []
    if phase == 'WATCH_BREAKOUT':
        last = watch_candles[-1] if watch_candles else pullback_candles[-1]
        return None, {
            'pullback_first_high': float(pullback_first_high),
            'prev_volume': float(last['volume']),
            'stage': stage,
            'vol_ma_lookup': _row_vol_ma_lookup(last),
            'pullback_retrace_pct': pullback_max_retrace_pct * 100,
        }
    return None, None

def check_exit_row(row) -> bool:
    """Exit rule: get out the moment the first red candle appears right
    after the rally you entered on. `row` is the newest CLOSED candle."""
    return row["close"] < row["open"]


# ----------------------------- TESTNET TRADING ----------------------------
def _signed_request(method: str, path: str, params: dict):
    if not TESTNET_API_KEY or not TESTNET_API_SECRET:
        log.warning("Testnet API keys not set, skipping trade call.")
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
            log.error("Testnet order error [%s]: %s", r.status_code, r.text)
            return None
        return r.json()
    except Exception as e:
        log.error("Testnet request exception: %s", e)
        return None


def total_open_trades() -> int:
    return sum(len(v) for v in _open_positions.values())


def open_long(symbol: str, stage: str, detail: dict = None, entry_candle_open_time=None):
    if total_open_trades() >= MAX_CONCURRENT_TRADES:
        return
    resp = _signed_request('POST', '/api/v3/order', {
        'symbol': symbol, 'side': 'BUY', 'type': 'MARKET', 'quoteOrderQty': TRADE_SIZE_USDT,
    })
    if not resp or 'executedQty' not in resp:
        log.error('ENTRY FAILED %s [%s]: %s', symbol, stage, resp)
        return
    qty = float(resp['executedQty'])
    quote_spent = float(resp.get('cummulativeQuoteQty', TRADE_SIZE_USDT))
    if qty <= 0:
        return
    entry_price = quote_spent / qty
    entry_fee = quote_spent * FEE_RATE
    _open_positions.setdefault(symbol, []).append({
        'qty': qty, 'entry_price': entry_price, 'entry_fee': entry_fee,
        'entry_time': time.time(), 'entry_candle_open_time': entry_candle_open_time, 'stage': stage,
        'breakout_level': detail.get('breakout_level') if detail else None,
        'entry_rvol_ratio': detail.get('rvol_ratio') if detail else None,
        'entry_rvol_period': detail.get('rvol_period') if detail else None,
        'breakout_rvol': detail.get('breakout_rvol') if detail else None,
        'entry_retrace_pct': detail.get('pullback_retrace_pct') if detail else None,
    })
    _save_stats()
    log.info('TRADE ENTRY %s [%s] | Entry Price=%.8f | RVOL MA=%s | Entry RVOL=%.2fx | Breakout RVOL=%.2fx | Retracement=%.1f%%',
             symbol, stage, entry_price, detail.get('rvol_period') if detail else None,
             detail.get('rvol_ratio', 0) if detail else 0, detail.get('breakout_rvol', 0) if detail else 0,
             detail.get('pullback_retrace_pct', 0) if detail else 0)

def _bucket_rvol(ratio):
    if ratio is None:
        return None
    if ratio < 3:
        return "2-3x"
    if ratio < 5:
        return "3-5x"
    return "5x+"


def _bucket_retrace(pct):
    if pct is None:
        return None
    if pct < 10:
        return "0-10%"
    if pct < 20:
        return "10-20%"
    return "20-30%"


def _summarize_bucket(records, bucket_fn, label) -> str:
    buckets = {}
    for r in records:
        key = bucket_fn(r)
        if key is None:
            continue
        b = buckets.setdefault(key, {"count": 0, "wins": 0, "pnl": 0.0})
        b["count"] += 1
        if r["win"]:
            b["wins"] += 1
        b["pnl"] += r["pnl"]
    lines = [f"By {label}:"]
    if not buckets:
        lines.append("  (no data)")
    for key in sorted(buckets.keys()):
        b = buckets[key]
        win_rate = b["wins"] / b["count"] * 100 if b["count"] else 0
        avg_pnl = b["pnl"] / b["count"] if b["count"] else 0
        lines.append(f"  {key}: {b['count']} trades, {win_rate:.1f}% win, avg pnl {avg_pnl:+.2f} USDT")
    return "\n".join(lines)


def _compute_breakdown() -> str:
    """Breaks the trade history down by RVOL magnitude, which MA period the
    RVOL matched against, and pullback retracement % -- to see which
    conditions are actually producing the wins, so thresholds can later be
    tightened toward whatever's working best."""
    records = _trade_history
    if not records:
        return "Ã°ÂŸÂ“ÂŠ PERFORMANCE BREAKDOWN: no trade history yet."

    rvol_section = _summarize_bucket(records, lambda r: _bucket_rvol(r.get("rvol_ratio")), "RVOL magnitude")
    period_section = _summarize_bucket(
        records, lambda r: f"{r['rvol_period']}-period" if r.get("rvol_period") else None, "RVOL period matched"
    )
    retrace_section = _summarize_bucket(records, lambda r: _bucket_retrace(r.get("retrace_pct")), "pullback retracement")
    breakout_rvol_section = _summarize_bucket(records, lambda r: _bucket_rvol(r.get("breakout_rvol")), "breakout RVOL")

    total = len(records)
    wins = sum(1 for r in records if r["win"])
    win_rate = wins / total * 100 if total else 0
    total_pnl = sum(r["pnl"] for r in records)

    return (
        f"Ã°ÂŸÂ“ÂŠ PERFORMANCE BREAKDOWN (last {total} trades)\n\n"
        f"{rvol_section}\n\n{period_section}\n\n{breakout_rvol_section}\n\n{retrace_section}\n\n"
        f"Overall: {total} trades, {win_rate:.1f}% win rate, total pnl {total_pnl:+.2f} USDT"
    )


def close_all_for_symbol(symbol: str, trigger_price=None, trigger_time=None):
    positions = _open_positions.get(symbol)
    if not positions:
        return
    for pos in positions:
        resp = _signed_request('POST', '/api/v3/order', {
            'symbol': symbol, 'side': 'SELL', 'type': 'MARKET',
            'quantity': f"{pos['qty']:.8f}".rstrip('0').rstrip('.'),
        })
        if not resp or 'executedQty' not in resp:
            log.error('EXIT FAILED %s: %s', symbol, resp)
            continue
        qty_sold = float(resp['executedQty'])
        quote_received = float(resp.get('cummulativeQuoteQty', 0))
        exit_price = quote_received / qty_sold if qty_sold else (trigger_price or 0)
        exit_fee = quote_received * FEE_RATE
        cost_basis = pos['qty'] * pos['entry_price']
        gross_pnl = quote_received - cost_basis
        total_fees = pos.get('entry_fee', 0) + exit_fee
        pnl = gross_pnl - total_fees
        _stats['realized_pnl_total'] += pnl
        _stats['trades_closed'] += 1
        _stats['wins' if pnl >= 0 else 'losses'] += 1
        _trade_history.append({
            'symbol': symbol, 'stage': pos['stage'], 'pnl': pnl, 'win': pnl >= 0,
            'rvol_ratio': pos.get('entry_rvol_ratio'), 'rvol_period': pos.get('entry_rvol_period'),
            'breakout_rvol': pos.get('breakout_rvol'), 'retrace_pct': pos.get('entry_retrace_pct'),
        })
        if len(_trade_history) > TRADE_HISTORY_MAX:
            del _trade_history[:len(_trade_history)-TRADE_HISTORY_MAX]
        _save_stats()
        wr = _stats['wins'] / _stats['trades_closed'] * 100 if _stats['trades_closed'] else 0
        msg = (f"{'ðŸŸ¢' if pnl >= 0 else 'ðŸ”´'} TRADE EXIT {symbol} [{pos['stage']}]\n"
               f"Entry Price: {pos['entry_price']:.8f}\nExit Price: {exit_price:.8f}\n"
               f"Gross Profit/Loss: {gross_pnl:+.4f} USDT\nBinance Fees: {total_fees:.4f} USDT\n"
               f"Net Profit/Loss: {pnl:+.4f} USDT\nOverall Profit/Loss: {_stats['realized_pnl_total']:+.4f} USDT\n"
               f"RVOL MA: {pos.get('entry_rvol_period')} | Entry RVOL: {pos.get('entry_rvol_ratio',0):.2f}x | "
               f"Breakout RVOL: {pos.get('breakout_rvol',0):.2f}x | Retracement: {pos.get('entry_retrace_pct',0):.1f}%\n"
               f"Trades: {_stats['trades_closed']} | W:{_stats['wins']} L:{_stats['losses']} | Win rate: {wr:.1f}%")
        log.info(msg.replace(chr(10), ' | '))
        send_telegram(msg)
        if _stats['trades_closed'] % BREAKDOWN_EVERY_N_TRADES == 0:
            breakdown = _compute_breakdown()
            log.info(breakdown.replace(chr(10), ' | '))
            send_telegram(breakdown)
    del _open_positions[symbol]

def log_portfolio_summary(also_telegram: bool = False):
    win_rate = (_stats["wins"] / _stats["trades_closed"] * 100) if _stats["trades_closed"] else 0
    trailer = (f"LIFETIME: total_pnl={_stats['realized_pnl_total']:.2f} USDT | "
               f"trades={_stats['trades_closed']} (win={_stats['wins']} loss={_stats['losses']}, "
               f"{win_rate:.1f}% win rate)")
    if not _open_positions:
        msg = f"PORTFOLIO: no open positions | 0/{MAX_CONCURRENT_TRADES} slots used\n{trailer}"
    else:
        lines = []
        for s, positions in _open_positions.items():
            for p in positions:
                lines.append(f"{s} [{p['stage']}]: qty={p['qty']:.4f} entry={p['entry_price']:.6f}")
        msg = (f"PORTFOLIO: {total_open_trades()}/{MAX_CONCURRENT_TRADES} slots used\n  " +
               "\n  ".join(lines) + f"\n{trailer}")
    log.info(msg.replace(chr(10), "\n"))
    if also_telegram:
        send_telegram(f"Ã°ÂŸÂ“ÂŠ Status update\n{msg}")


# ----------------------------- PER-SYMBOL STATE ----------------------------
# symbol -> {"df": DataFrame (with indicators), "tradeable": bool, "watch": dict or None}
_symbol_state = {}
_alpha_symbol_to_name = {}  # "ALPHA_116USDT" -> "TOKEN" (human name for logs/telegram)


def _run_bg(fn, *args):
    """Fire-and-forget a blocking call (Telegram, Testnet order) on a
    background thread so it can NEVER block the WebSocket event loop.
    Without this, a single slow network call freezes ALL scanning --
    both WebSocket connections and the heartbeat -- until it returns."""
    try:
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, fn, *args)
    except RuntimeError:
        fn(*args)  # no running loop (shouldn't happen) -- fall back to direct call


# All 5m candles across every symbol close at the same wall-clock moment, so
# on every candle-close boundary, hundreds of on_candle_close() calls arrive
# in a burst. Each does real CPU work (pandas concat + full indicator
# recompute), so running them one-by-one on the event loop thread would
# stall it (and, given enough symbols, cause a growing backlog that never
# catches up -- this is what caused the earlier multi-hour hangs). A
# dedicated thread pool lets these run concurrently instead.
_CANDLE_EXECUTOR = ThreadPoolExecutor(max_workers=32)


def _submit_candle_close(*args):
    future = _CANDLE_EXECUTOR.submit(on_candle_close, *args)

    def _log_if_failed(f):
        exc = f.exception()
        if exc:
            log.error("on_candle_close crashed for args=%s: %r", args[:1], exc)

    future.add_done_callback(_log_if_failed)


def _signal_and_maybe_trade(symbol: str, tradeable: bool, entry_result):
    stage, price, candle_key, detail = entry_result
    if _entry_already_taken(symbol, stage, candle_key):
        return
    _mark_entry_taken(symbol, stage, candle_key)
    tag = "Ã°ÂŸÂŸÂ¢ SPOT" if tradeable else "Ã°ÂŸÂŸÂ¡ ALPHA (manual only)"
    msg = (f"{tag} {symbol}\nEntry signal: {stage.upper()} breakout\nPrice: {price:.6f}\nTimeframe: 5m\n"
           f"{_detail_line(detail)}")
    log.info(msg.replace(chr(10), " | "))
    send_telegram(msg)
    if tradeable and ENABLE_TRADING:
        open_long(symbol, stage, detail)


def seed_symbol(key: str, tradeable: bool, fetch_symbol: str = None):
    fetch_symbol = fetch_symbol or key
    df = get_spot_klines(fetch_symbol) if tradeable else get_alpha_klines(fetch_symbol)
    if df is None:
        return
    df = add_indicators(df)
    # Compute initial state silently -- don't act on whatever the state
    # machine finds in old history, only store it so live ticks going
    # forward can react to genuinely NEW breakouts.
    _, watch_state = run_state_machine(df)
    _symbol_state[key] = {"df": df, "tradeable": tradeable, "watch": watch_state}


def seed_all_symbols(spot_symbols, alpha_tokens):
    log.info("Seeding history for %d spot pairs + %d Alpha tokens (parallelized)...",
              len(spot_symbols), len(alpha_tokens))
    _alpha_symbol_to_name.clear()
    for t in alpha_tokens:
        _alpha_symbol_to_name[t["alpha_symbol"].upper()] = t["name"]

    with ThreadPoolExecutor(max_workers=SEED_WORKERS) as ex:
        futures = [ex.submit(seed_symbol, s, True, s) for s in spot_symbols]
        futures += [ex.submit(seed_symbol, t["name"], False, t["alpha_symbol"]) for t in alpha_tokens]
        for f in futures:
            try:
                f.result(timeout=30)
            except Exception as e:
                log.warning("Seed error: %s", e)


def on_candle_close(symbol: str, o, h, l, c, v, open_time, close_time):
    state = _symbol_state.get(symbol)
    if state is None:
        return
    new_row = pd.DataFrame([{
        'open_time': open_time, 'open': o, 'high': h, 'low': l, 'close': c, 'volume': v,
        'close_time': close_time, 'quote_asset_volume': 0, 'num_trades': 0,
        'taker_buy_base': 0, 'taker_buy_quote': 0, 'ignore': 0,
    }])
    df = pd.concat([state['df'][['open_time','open','high','low','close','volume','close_time','quote_asset_volume','num_trades','taker_buy_base','taker_buy_quote','ignore']], new_row], ignore_index=True)
    if len(df) > MAX_HISTORY_ROWS:
        df = df.iloc[-MAX_HISTORY_ROWS:].reset_index(drop=True)
    state['df'] = add_indicators(df)
    # Never execute an old breakout at candle close. This only prepares the next live watch.
    _, state['watch'] = run_state_machine(state['df'])

def _live_rvol_detail(volume, vol_ma_lookup, live):
    period, ratio = _best_rvol_ratio_from_values(volume, vol_ma_lookup)
    return {
        'rvol_ratio': ratio, 'rvol_period': period,
        'macd': live['macd'], 'macd_signal': live['macd_signal'],
        'macd_above_signal': live['macd_above_signal'], 'ema9': live['ema9'],
        'ema20': live['ema20'], 'vwap': live['vwap'], 'ema200': live['ema200'],
    }

def on_live_tick(symbol: str, o: float, high: float, low: float, close: float, volume: float, open_time):
    state = _symbol_state.get(symbol)
    if state is None:
        return

    # Keep a small live state for the wave that was just traded.  It allows the
    # next pullback/breakout to become RALLY 3 without waiting for a full
    # historical reconstruction that would otherwise lose the live entry.
    after = state.get('after_entry')

    # EXIT: first NEW red candle after the entry candle starts -> immediate sell.
    if state['tradeable'] and symbol in _open_positions:
        for pos in list(_open_positions.get(symbol, [])):
            if open_time > pos.get('entry_candle_open_time', -1) and close < o:
                # The red candle is also the first pullback candle for Rally 3.
                if after and after.get('stage') == 'rally2':
                    rally_hi = max(after.get('high', high), high)
                    rally_lo = after.get('low', low)
                    rally_size = rally_hi - rally_lo
                    retrace = (rally_hi - low) / rally_size if rally_size > 0 else 0
                    avg_vol = after.get('volume', volume)
                    if retrace < INVALIDATION_RETRACE_PCT and volume < PULLBACK_MAX_VOL_RATIO * avg_vol:
                        state['rally3_pullback'] = {
                            'first_high': high,
                            'retrace_pct': retrace * 100,
                            'rally_high': rally_hi,
                            'rally_low': rally_lo,
                            'rally_avg_vol': avg_vol,
                            'prev_volume': volume,
                        }
                    else:
                        state['rally3_pullback'] = None
                    state['after_entry'] = None
                _run_bg(close_all_for_symbol, symbol, close, open_time)
                return

    # Continue building the post-Rally-2 wave while no position is open.
    pull3 = state.get('rally3_pullback')
    if pull3 and open_time >= state.get('rally3_watch_candle', open_time):
        # Additional red pullback candles deepen the retracement; invalidate if
        # the 30% ceiling or 60%-of-rally-volume ceiling is reached.
        if close < o:
            size = pull3['rally_high'] - pull3['rally_low']
            retrace = (pull3['rally_high'] - low) / size if size > 0 else 0
            pull3['retrace_pct'] = max(pull3['retrace_pct'], retrace * 100)
            if retrace >= INVALIDATION_RETRACE_PCT or volume >= PULLBACK_MAX_VOL_RATIO * pull3['rally_avg_vol']:
                state['rally3_pullback'] = None
            else:
                pull3['prev_volume'] = volume
            return
        if close > o:
            state['watch'] = {
                'pullback_first_high': pull3['first_high'],
                'prev_volume': pull3['prev_volume'],
                'stage': 'rally3',
                'vol_ma_lookup': _row_vol_ma_lookup(state['df'].iloc[-1]),
                'pullback_retrace_pct': pull3['retrace_pct'],
            }
            state['rally3_pullback'] = None
            # Continue below so this very first Rally-3 green candle can itself
            # be the one-shot breakout candle if it crosses the first red high.
            pull3 = None

    watch = state.get('watch')
    if not watch or high <= watch['pullback_first_high']:
        # If an entry just happened, keep the current candle as Rally 2.
        if state.get('after_entry') and open_time == state['after_entry'].get('open_time'):
            state['after_entry']['high'] = max(state['after_entry']['high'], high)
            state['after_entry']['low'] = min(state['after_entry']['low'], low)
            state['after_entry']['volume'] = volume
        return

    # Exact one-shot breakout moment: evaluate every condition NOW.
    live = _live_indicator_row(state, o, high, low, close, volume)
    if not live:
        return
    if not (_breakout_confirmed(high, volume, watch['pullback_first_high'], watch['prev_volume'], live['vol_ma_lookup'])
            and live['trend_ok'] and live['macd_above_signal']):
        state['watch'] = None
        state['rally3_pullback'] = None
        return

    stage = watch['stage']
    candle_key = open_time
    if _entry_already_taken(symbol, stage, candle_key):
        return
    detail = _live_rvol_detail(volume, live['vol_ma_lookup'], live)
    detail['pullback_retrace_pct'] = watch.get('pullback_retrace_pct', 0.0)
    detail['breakout_level'] = watch['pullback_first_high']
    detail['breakout_rvol'] = detail['rvol_ratio']
    _mark_entry_taken(symbol, stage, candle_key)
    state['watch'] = None
    state['rally3_pullback'] = None
    state['after_entry'] = {
        'stage': stage,
        'open_time': open_time,
        'high': high,
        'low': low,
        'volume': volume,
    } if stage == 'rally2' else None

    tag = 'ðŸŸ¢ SPOT' if state['tradeable'] else 'ðŸŸ¡ ALPHA'
    msg = (f'{tag} {symbol}\nTRADE ENTRY {stage.upper()} â€” LIVE BREAKOUT\n'
           f'Breakout Level: {watch["pullback_first_high"]:.8f}\n'
           f'Trigger Price: {close:.8f}\nTimeframe: 5m\n{_detail_line(detail)} | Breakout RVOL={detail["breakout_rvol"]:.2f}x')
    log.info(msg.replace(chr(10), ' | '))
    _run_bg(send_telegram, msg)
    if state['tradeable'] and ENABLE_TRADING:
        _run_bg(open_long, symbol, stage, detail, candle_key)

async def _subscribe_all(ws, stream_names):
    for i in range(0, len(stream_names), STREAMS_PER_SUBSCRIBE):
        batch = stream_names[i:i + STREAMS_PER_SUBSCRIBE]
        await ws.send(json.dumps({"method": "SUBSCRIBE", "params": batch, "id": i + 1}))
        await asyncio.sleep(0.2)


async def spot_ws_loop(spot_symbols):
    streams = [f"{s.lower()}@kline_{KLINE_INTERVAL}" for s in spot_symbols]
    async with websockets.connect("wss://stream.binance.com:9443/stream", ping_interval=20, ping_timeout=60, max_size=None) as ws:
        await _subscribe_all(ws, streams)
        log.info("Spot WebSocket connected and subscribed to %d streams.", len(streams))
        async for raw in ws:
            try:
                msg = json.loads(raw)
                data = msg.get("data", msg)
                if data.get("e") != "kline":
                    continue
                symbol = data["s"]
                k = data["k"]
                o, h, l, c, v = float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"])
                if k["x"]:
                    # All 5m candles across every symbol close at the SAME
                    # wall-clock moment, so hundreds of these can arrive at
                    # once. The pandas/indicator recompute per symbol is real
                    # CPU work -- doing it directly here would stall the
                    # event loop (and eventually the whole connection) under
                    # that burst. Offload to the candle-close thread pool.
                    _submit_candle_close(symbol, o, h, l, c, v, k["t"], k["T"])
                else:
                    on_live_tick(symbol, o, h, float(k["l"]), c, v, k["t"])
            except Exception as e:
                log.warning("Spot WS message error: %s", e)


async def alpha_ws_loop(alpha_tokens):
    streams = [f"{t['alpha_symbol'].lower()}@kline_{KLINE_INTERVAL}" for t in alpha_tokens]
    async with websockets.connect(ALPHA_WS_BASE, ping_interval=20, ping_timeout=60, max_size=None) as ws:
        await _subscribe_all(ws, streams)
        log.info("Alpha WebSocket connected and subscribed to %d streams.", len(streams))
        async for raw in ws:
            try:
                msg = json.loads(raw)
                data = msg.get("data", msg)
                if data.get("e") != "kline":
                    continue
                alpha_symbol = data["s"].upper()
                name = _alpha_symbol_to_name.get(alpha_symbol, alpha_symbol)
                k = data["k"]
                o, h, l, c, v = float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"])
                if k["x"]:
                    _submit_candle_close(name, o, h, l, c, v, k["t"], k["T"])
                else:
                    on_live_tick(name, o, h, float(k["l"]), c, v, k["t"])
            except Exception as e:
                log.warning("Alpha WS message error: %s", e)


async def periodic_tasks(duration_seconds):
    global _last_heartbeat
    elapsed = 0
    while elapsed < duration_seconds:
        await asyncio.sleep(60)
        elapsed += 60
        _last_heartbeat = time.time()
        _prune_old_entries()

def _watchdog_loop():
    while True:
        time.sleep(30)
        if time.time() - _last_heartbeat > WATCHDOG_TIMEOUT_SECONDS:
            log.critical(
                "WATCHDOG: no heartbeat in over %ds -- bot appears stuck. "
                "Forcing a hard process exit so Railway restarts it fresh.",
                WATCHDOG_TIMEOUT_SECONDS
            )
            try:
                send_telegram("Ã¢ÂšÂ Ã¯Â¸Â Bot got stuck and is force-restarting itself now (watchdog triggered). Back online shortly.")
            except Exception:
                pass
            os._exit(1)


def _start_watchdog():
    global _last_heartbeat
    _last_heartbeat = time.time()
    threading.Thread(target=_watchdog_loop, daemon=True).start()


async def run_bot_cycle():
    loop = asyncio.get_event_loop()
    spot_symbols = await loop.run_in_executor(None, get_usdt_symbols)
    alpha_tokens = await loop.run_in_executor(None, get_alpha_tokens)
    await loop.run_in_executor(None, seed_all_symbols, spot_symbols, alpha_tokens)

    log.info(
        "Wave bot (real-time) starting. Telegram: %s | Trading: %s | Budget: $%d ($%d/trade, max %d concurrent)",
        bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID), ENABLE_TRADING,
        ACCOUNT_BUDGET_USDT, TRADE_SIZE_USDT, MAX_CONCURRENT_TRADES
    )
    send_telegram("Ã¢ÂœÂ… Wave strategy bot is online (real-time WebSocket, Spot + Alpha).")

    await asyncio.gather(
        spot_ws_loop(spot_symbols),
        alpha_ws_loop(alpha_tokens),
        periodic_tasks(SYMBOL_REFRESH_SECONDS),
    )


def main():
    _load_stats()
    _start_watchdog()
    while True:
        try:
            asyncio.run(run_bot_cycle())
        except Exception as e:
            log.error("Bot cycle ended/crashed: %s -- doing a full fresh reseed+reconnect in 10s", e)
        time.sleep(10)


if __name__ == "__main__":
    main()
