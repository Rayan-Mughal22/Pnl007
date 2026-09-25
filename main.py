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
         with current volume strictly greater than the immediately preceding candle,
         RVOL >= 2x, trend and MACD valid at the exact live crossing.
Exit: the moment the first red candle starts AFTER the entry candle --
      exit immediately, without waiting for that candle to close.

MONEY MANAGEMENT
$500 demo budget, $100/trade, max 5 concurrent trades across all coins.
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
TRADE_SIZE_USDT = 100
MAX_CONCURRENT_TRADES = int(ACCOUNT_BUDGET_USDT / TRADE_SIZE_USDT)  # 5
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
MAX_RALLY_WATCH_CANDLES = None    # no artificial candle-count limit; first crossing wins

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
STATE_VERSION = 4  # fresh strategy state; V3 and older state is intentionally ignored
STATS_MARKER = "STATS_JSON_V4:"


def _persisted_payload_compact() -> dict:
    """Small payload synced to the pinned Telegram message -- stats + open
    positions only, kept under Telegram's message size limit."""
    return {"state_version": STATE_VERSION, "stats": _stats, "open_positions": _open_positions}


def _persisted_payload_full() -> dict:
    """Everything, including trade history -- written to the local file
    backup only (never sent to Telegram, could get large)."""
    payload = _persisted_payload_compact()
    payload["trade_history"] = _trade_history
    return payload


def _apply_persisted_payload(data: dict):
    if data.get("state_version") != STATE_VERSION:
        return False
    if "stats" in data:
        _stats.update(data["stats"])
    if "open_positions" in data and isinstance(data["open_positions"], dict):
        _open_positions.clear()
        _open_positions.update(data["open_positions"])
    if "trade_history" in data and isinstance(data["trade_history"], list):
        _trade_history.clear()
        _trade_history.extend(data["trade_history"])
    return True


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
        if not _apply_persisted_payload(loaded):
            return False
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
                loaded = json.load(f)
            if _apply_persisted_payload(loaded):
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


def _breakout_confirmed(breakout_high, current_close, open_price, volume, pullback_first_high, prev_volume,
                        vol_ma_lookup=None, trend_ok=True, macd_ok=True) -> bool:
    """Exact one-shot breakout gate.

    The pullback's FIRST red candle high is the level. The first live/high
    tick that crosses that level is the decision point. The wick/high performs
    the crossing; the candle must simultaneously be green (current price >
    actual candle open), volume must be rising, RVOL must be >= 2x against at
    least one allowed MA, and the live trend/MACD filters must pass.
    """
    if breakout_high <= pullback_first_high:
        return False
    if current_close <= open_price:  # breakout candle must be green at that moment
        return False
    if prev_volume is not None and volume <= prev_volume:
        return False
    if not _rvol_ok_against(volume, vol_ma_lookup):
        return False
    if not trend_ok or not macd_ok:
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


# ----------------------------- WAVE STATE MACHINE --------------------------
def run_state_machine(df: pd.DataFrame):
    """
    Rebuilds the whole rally/pullback/breakout sequence from CLOSED candles
    only (the newest row in df is assumed already closed -- this is called
    right after a candle-close event, never on a live/forming candle).

    Returns (entry_or_None, watch_state_or_None):
      entry: (stage, price, candle_key, detail) if a breakout completed
              exactly on the newest closed candle.
      watch_state: {"pullback_first_high":, "prev_volume":, "stage":} if the
              state machine ended in WATCH_BREAKOUT (waiting for a breakout),
              so the caller can cheaply check future live ticks against it.
    """
    n = len(df)
    if n < 210:
        return None, None

    last_idx = n - 1
    start_idx = max(0, n - 260)

    phase = "WAIT_RALLY1"
    rally_candles = []
    rally_range = None
    pullback_candles = []
    pullback_first_high = None
    pullback_max_retrace_pct = 0.0
    watch_candles = []
    stage_after_pullback = None
    entry_index = None
    entry_stage = None
    entry_price = None
    entry_detail = None

    for i in range(start_idx, last_idx + 1):
        row = df.iloc[i]
        is_green = row["close"] > row["open"]
        is_red = row["close"] < row["open"]
        body_pct = abs(row["body_pct"])

        if phase == "WAIT_RALLY1":
            if is_green and body_pct >= RALLY_MIN_BODY_PCT:
                if rally_candles and row["volume"] < rally_candles[-1]["volume"]:
                    rally_candles = [row]
                else:
                    rally_candles.append(row)
                if len(rally_candles) >= 2 and _trend_ok(row) and _rvol_ok(row) and _macd_ok(row):
                    rally_range = (min(c["low"] for c in rally_candles), max(c["high"] for c in rally_candles))
                    phase = "PULLBACK"
                    pullback_candles = []
                    pullback_max_retrace_pct = 0.0
                    stage_after_pullback = "rally2"
            else:
                rally_candles = []

        elif phase == "PULLBACK":
            if is_red:
                pullback_candles.append(row)
                r_high, r_low = rally_range[1], rally_range[0]
                r_size = r_high - r_low
                retrace = r_high - row["low"]
                retrace_pct = (retrace / r_size) if r_size > 0 else 0
                pullback_max_retrace_pct = max(pullback_max_retrace_pct, retrace_pct)
                rally_avg_vol = _avg_vol(rally_candles)
                too_deep = retrace_pct >= INVALIDATION_RETRACE_PCT
                too_heavy = rally_avg_vol > 0 and row["volume"] >= PULLBACK_MAX_VOL_RATIO * rally_avg_vol
                if too_deep or too_heavy:
                    phase = "WAIT_RALLY1"
                    rally_candles = []
            elif is_green:
                if not pullback_candles:
                    if body_pct >= RALLY_MIN_BODY_PCT and (not rally_candles or row["volume"] > rally_candles[-1]["volume"]):
                        rally_candles.append(row)
                        rally_range = (min(c["low"] for c in rally_candles), max(c["high"] for c in rally_candles))
                    else:
                        phase = "WAIT_RALLY1"
                        rally_candles = []
                else:
                    pullback_first_high = pullback_candles[0]["high"]
                    watch_candles = [row]
                    if row["high"] > pullback_first_high:
                        # Price crosses the level for the very first time on
                        # THIS candle -- this is the one-and-only breakout
                        # judgment moment. If conditions (volume-rising,
                        # RVOL) aren't ALSO met right here, the setup is
                        # dead -- do NOT keep re-checking later candles
                        # against this same already-broken level (that would
                        # mean entering well after the real breakout, at a
                        # worse price than the actual crossing).
                        if _breakout_confirmed(row["high"], row["close"], row["open"], row["volume"], pullback_first_high, pullback_candles[-1]["volume"], _row_vol_ma_lookup(row), _trend_ok(row), _macd_ok(row)):
                            entry_index = i
                            entry_stage = stage_after_pullback
                            entry_price = row["close"]
                            entry_detail = _entry_detail(row)
                            entry_detail["pullback_retrace_pct"] = pullback_max_retrace_pct * 100
                            if stage_after_pullback == "rally2":
                                rally_range = (min(c["low"] for c in watch_candles), max(c["high"] for c in watch_candles))
                                rally_candles = list(watch_candles)
                                phase = "PULLBACK"
                                pullback_candles = []
                                pullback_max_retrace_pct = 0.0
                                stage_after_pullback = "rally3"
                            else:
                                phase = "WAIT_RALLY1"
                                rally_candles = []
                        else:
                            phase = "WAIT_RALLY1"
                            rally_candles = []
                    else:
                        # Hasn't reached the level yet -- keep watching for
                        # the candle that actually crosses it.
                        phase = "WATCH_BREAKOUT"

        elif phase == "WATCH_BREAKOUT":
            if is_green:
                watch_candles.append(row)
                if row["high"] > pullback_first_high:
                    # First candle to cross the level -- one-shot judgment,
                    # same rule as above: pass now or the setup is dead.
                    prev_vol = watch_candles[-2]["volume"] if len(watch_candles) >= 2 else pullback_candles[-1]["volume"]
                    if _breakout_confirmed(row["high"], row["close"], row["open"], row["volume"], pullback_first_high, prev_vol, _row_vol_ma_lookup(row), _trend_ok(row), _macd_ok(row)):
                        entry_index = i
                        entry_stage = stage_after_pullback
                        entry_price = row["close"]
                        entry_detail = _entry_detail(row)
                        entry_detail["pullback_retrace_pct"] = pullback_max_retrace_pct * 100
                        if stage_after_pullback == "rally2":
                            rally_range = (min(c["low"] for c in watch_candles), max(c["high"] for c in watch_candles))
                            rally_candles = list(watch_candles)
                            phase = "PULLBACK"
                            pullback_candles = []
                            pullback_max_retrace_pct = 0.0
                            stage_after_pullback = "rally3"
                        else:
                            phase = "WAIT_RALLY1"
                            rally_candles = []
                    else:
                        phase = "WAIT_RALLY1"
                        rally_candles = []
            elif is_red:
                phase = "WAIT_RALLY1"
                rally_candles = []

    entry_result = None
    if entry_index == last_idx:
        candle_key = df.iloc[last_idx]["open_time"]
        entry_result = (entry_stage, entry_price, candle_key, entry_detail)

    watch_state = None
    # IMPORTANT: arm the live breakout watcher immediately after a valid
    # pullback candle closes. The NEXT candle is the first opportunity to
    # cross the first red candle's high, and we must not wait for that green
    # candle to close. This is what makes entry truly real-time.
    if pullback_candles and phase in ("PULLBACK", "WATCH_BREAKOUT"):
        last_row = pullback_candles[-1]
        watch_state = {
            "pullback_first_high": pullback_candles[0]["high"],
            "prev_volume": last_row["volume"],
            "stage": stage_after_pullback,
            "vol_ma_lookup": _row_vol_ma_lookup(last_row),
            "pullback_retrace_pct": pullback_max_retrace_pct * 100,
        }
    elif phase == "WATCH_BREAKOUT":
        last_row = watch_candles[-1] if watch_candles else df.iloc[last_idx]
        watch_state = {
            "pullback_first_high": pullback_first_high,
            "prev_volume": last_row["volume"],
            "stage": stage_after_pullback,
            "vol_ma_lookup": _row_vol_ma_lookup(last_row),
            "pullback_retrace_pct": pullback_max_retrace_pct * 100,
        }

    return entry_result, watch_state


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


def open_long(symbol: str, stage: str, detail: dict = None):
    if total_open_trades() >= MAX_CONCURRENT_TRADES:
        log.info("Budget full (%d/%d trades) -- skipping %s entry on %s",
                  total_open_trades(), MAX_CONCURRENT_TRADES, stage, symbol)
        return
    resp = _signed_request("POST", "/api/v3/order", {
        "symbol": symbol, "side": "BUY", "type": "MARKET",
        "quoteOrderQty": TRADE_SIZE_USDT,
    })
    if not resp or "executedQty" not in resp:
        log.error("Failed to open long on %s: %s", symbol, resp)
        return
    qty = float(resp["executedQty"])
    quote_spent = float(resp.get("cummulativeQuoteQty", TRADE_SIZE_USDT))
    if qty <= 0:
        return
    entry_price = quote_spent / qty
    entry_fee = quote_spent * FEE_RATE
    if symbol in _symbol_state:
        _symbol_state[symbol]["exit_in_progress"] = False
    _open_positions.setdefault(symbol, []).append({
        "qty": qty, "entry_price": entry_price, "entry_fee": entry_fee,
        "entry_time": time.time(), "entry_candle_open_time": detail.get("entry_candle_open_time") if detail else None, "stage": stage,
        "entry_rvol_ratio": detail.get("rvol_ratio") if detail else None,
        "entry_rvol_period": detail.get("rvol_period") if detail else None,
        "breakout_rvol_ratio": detail.get("breakout_rvol_ratio") if detail else None,
        "breakout_rvol_period": detail.get("breakout_rvol_period") if detail else None,
        "entry_retrace_pct": detail.get("pullback_retrace_pct") if detail else None,
    })
    _save_stats()  # persist the newly-opened position immediately -- don't wait for it to close
    detail_str = f" | {_detail_line(detail)}" if detail else ""
    log.info("TRADE OPEN  %s [%s] | qty=%.6f entry=%.6f cost=%.2f USDT (fee ~%.3f) | open trades=%d/%d%s",
              symbol, stage, qty, entry_price, quote_spent, entry_fee, total_open_trades(), MAX_CONCURRENT_TRADES, detail_str)


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

    rvol_section = _summarize_bucket(records, lambda r: _bucket_rvol(r.get("rvol_ratio")), "Entry RVOL magnitude")
    breakout_rvol_section = _summarize_bucket(records, lambda r: _bucket_rvol(r.get("breakout_rvol")), "Breakout RVOL magnitude")
    period_section = _summarize_bucket(
        records, lambda r: f"{r['rvol_period']}-period" if r.get("rvol_period") else None, "RVOL period matched"
    )
    retrace_section = _summarize_bucket(records, lambda r: _bucket_retrace(r.get("retrace_pct")), "pullback retracement")

    total = len(records)
    wins = sum(1 for r in records if r["win"])
    win_rate = wins / total * 100 if total else 0
    total_pnl = sum(r["pnl"] for r in records)

    return (
        f"Ã°ÂŸÂ“ÂŠ PERFORMANCE BREAKDOWN (last {total} trades)\n\n"
        f"{rvol_section}\n\n{breakout_rvol_section}\n\n{period_section}\n\n{retrace_section}\n\n"
        f"Overall: {total} trades, {win_rate:.1f}% win rate, total pnl {total_pnl:+.2f} USDT"
    )


def close_all_for_symbol(symbol: str):
    positions = _open_positions.get(symbol)
    if not positions:
        return
    for pos in positions:
        resp = _signed_request("POST", "/api/v3/order", {
            "symbol": symbol, "side": "SELL", "type": "MARKET",
            "quantity": f"{pos['qty']:.8f}".rstrip("0").rstrip("."),
        })
        if not resp or "executedQty" not in resp:
            log.error("Failed to close long on %s: %s", symbol, resp)
            continue
        qty_sold = float(resp["executedQty"])
        quote_received = float(resp.get("cummulativeQuoteQty", 0))
        exit_price = quote_received / qty_sold if qty_sold else 0
        exit_fee = quote_received * FEE_RATE
        cost_basis = pos["qty"] * pos["entry_price"]
        gross_pnl = quote_received - cost_basis
        total_fees = pos.get("entry_fee", 0) + exit_fee
        pnl = gross_pnl - total_fees
        _stats["realized_pnl_total"] += pnl
        _stats["trades_closed"] += 1
        win = pnl >= 0
        if win:
            _stats["wins"] += 1
        else:
            _stats["losses"] += 1

        _trade_history.append({
            "symbol": symbol, "stage": pos["stage"], "pnl": pnl, "win": win,
            "rvol_ratio": pos.get("entry_rvol_ratio"),
            "rvol_period": pos.get("entry_rvol_period"),
            "breakout_rvol": pos.get("breakout_rvol_ratio", pos.get("entry_rvol_ratio")),
            "retrace_pct": pos.get("entry_retrace_pct"),
        })
        if len(_trade_history) > TRADE_HISTORY_MAX:
            del _trade_history[:len(_trade_history) - TRADE_HISTORY_MAX]

        _save_stats()
        hold_minutes = (time.time() - pos["entry_time"]) / 60
        win_rate = (_stats["wins"] / _stats["trades_closed"] * 100) if _stats["trades_closed"] else 0
        close_msg = (
            f"{'Ã°ÂŸÂŸÂ¢' if pnl >= 0 else 'Ã°ÂŸÂ”Â´'} TRADE CLOSE {symbol} [{pos['stage']}]\n"
            f"Entry: {pos['entry_price']:.6f} | Exit: {exit_price:.6f}\n"
            f"This trade: gross={gross_pnl:.2f} fees={total_fees:.2f} net={pnl:.2f} USDT ({hold_minutes:.1f} min held)\n"
            f"Lifetime: total PnL={_stats['realized_pnl_total']:.2f} USDT | "
            f"{_stats['trades_closed']} trades (W:{_stats['wins']} L:{_stats['losses']}, {win_rate:.1f}% win rate)"
        )
        log.info(close_msg.replace(chr(10), " | "))
        send_telegram(close_msg)

        if _stats["trades_closed"] % BREAKDOWN_EVERY_N_TRADES == 0:
            breakdown_msg = _compute_breakdown()
            log.info(breakdown_msg.replace(chr(10), " | "))
            send_telegram(breakdown_msg)
    del _open_positions[symbol]
    if symbol in _symbol_state:
        _symbol_state[symbol]["exit_in_progress"] = False


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
    _symbol_state[key] = {"df": df, "tradeable": tradeable, "watch": watch_state, "exit_in_progress": False}


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
    log.info("Seeding complete. %d symbols loaded.", len(_symbol_state))


def on_candle_close(symbol: str, o, h, l, c, v, open_time, close_time):
    state = _symbol_state.get(symbol)
    if state is None:
        return  # not seeded (shouldn't normally happen)
    df = state["df"]
    new_row = pd.DataFrame([{
        "open_time": open_time, "open": o, "high": h, "low": l, "close": c,
        "volume": v, "close_time": close_time, "quote_asset_volume": 0,
        "num_trades": 0, "taker_buy_base": 0, "taker_buy_quote": 0, "ignore": 0,
    }])
    # Replace the last row if it's the SAME candle (shouldn't be, since this
    # is only called on closed candles), otherwise append and trim.
    df = pd.concat([df[["open_time", "open", "high", "low", "close", "volume",
                        "close_time", "quote_asset_volume", "num_trades",
                        "taker_buy_base", "taker_buy_quote", "ignore"]], new_row], ignore_index=True)
    if len(df) > MAX_HISTORY_ROWS:
        df = df.iloc[-MAX_HISTORY_ROWS:].reset_index(drop=True)
    df = add_indicators(df)
    state["df"] = df

    entry_result, watch_state = run_state_machine(df)
    state["watch"] = watch_state
    if entry_result:
        _run_bg(_signal_and_maybe_trade, symbol, state["tradeable"], entry_result)

    if state["tradeable"] and symbol in _open_positions:
        last_row = df.iloc[-1]
        if check_exit_row(last_row):
            _run_bg(close_all_for_symbol, symbol)


def _live_indicator_snapshot(state: dict, open_price: float, high: float, close: float, volume: float) -> dict:
    """Build indicators for the current forming 5m candle from the live
    candle OHLCV, without waiting for candle close."""
    df = state["df"]
    last = df.iloc[-1]
    row = {
        "open_time": last["open_time"],
        "open": open_price,
        "high": max(high, open_price, close),
        "low": min(open_price, close),
        "close": close,
        "volume": volume,
        "close_time": last["close_time"],
        "quote_asset_volume": 0, "num_trades": 0,
        "taker_buy_base": 0, "taker_buy_quote": 0, "ignore": 0,
    }
    tmp = pd.concat([df.iloc[-MAX_HISTORY_ROWS + 1:].copy(), pd.DataFrame([row])], ignore_index=True)
    tmp = add_indicators(tmp)
    return tmp.iloc[-1]


def _live_rvol_detail(volume, vol_ma_lookup, live_row) -> dict:
    """RVOL from the live forming candle volume against the stored closed-candle
    volume-MA references; trend/MACD come from the live-price indicator row."""
    best_period, best_ratio = None, 0.0
    if vol_ma_lookup:
        for p in VOL_MA_PERIODS:
            ma = vol_ma_lookup.get(p)
            if ma is not None and not math.isnan(ma) and ma > 0:
                ratio = volume / ma
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_period = p
    return {
        "rvol_ratio": best_ratio,
        "rvol_period": best_period,
        "macd": live_row["macd"],
        "macd_signal": live_row["macd_signal"],
        "macd_above_signal": live_row["macd"] > live_row["macd_signal"],
        "ema9": live_row["ema9"],
        "ema20": live_row["ema20"],
        "vwap": live_row["vwap"],
        "ema200": live_row["ema200"],
    }


def _entry_gate_detail(state: dict, watch: dict, open_price: float, high: float, close: float, volume: float):
    live_row = _live_indicator_snapshot(state, open_price, high, close, volume)
    detail = _live_rvol_detail(volume, watch.get("vol_ma_lookup"), live_row)
    detail["pullback_retrace_pct"] = watch.get("pullback_retrace_pct", 0.0)
    trend_ok = _trend_ok(live_row)
    macd_ok = _macd_ok(live_row)
    confirmed = _breakout_confirmed(
        high, close, open_price, volume, watch["pullback_first_high"],
        watch.get("prev_volume"), watch.get("vol_ma_lookup"), trend_ok, macd_ok
    )
    detail["breakout_rvol_ratio"] = detail["rvol_ratio"]
    detail["breakout_rvol_period"] = detail["rvol_period"]
    detail["trend_ok_at_breakout"] = trend_ok
    detail["macd_ok_at_breakout"] = macd_ok
    detail["breakout_high"] = high
    detail["breakout_price"] = close
    return confirmed, detail


def on_live_tick(symbol: str, open_price: float, high: float, close: float, volume: float, open_time):
    state = _symbol_state.get(symbol)
    if state is None:
        return

    # EXIT is checked first. The moment the currently forming candle turns
    # red, close the live Spot position -- do NOT wait for candle close.
    if state["tradeable"] and symbol in _open_positions and close < open_price:
        positions = _open_positions.get(symbol, [])
        # Exit only on the FIRST red candle AFTER the candle on which entry
        # occurred. Never exit merely because the entry candle later turns red.
        eligible = any(p.get("entry_candle_open_time") is not None and
                       p.get("entry_candle_open_time") != open_time for p in positions)
        if eligible and not state.get("exit_in_progress"):
            state["exit_in_progress"] = True
            _run_bg(close_all_for_symbol, symbol)
        return

    watch = state.get("watch")
    if not watch:
        return

    # Detect the EXACT LIVE crossing tick. The breakout level is crossed by
    # the candle HIGH/WICK, not by requiring the candle to CLOSE above it.
    # We remember the highest live price seen so far in this candle so a
    # later tick cannot falsely become the "breakout" decision point.
    breakout_level = watch["pullback_first_high"]
    previous_live_high = watch.get("last_live_high", breakout_level)
    crossed_now = previous_live_high <= breakout_level and high > breakout_level
    if high > previous_live_high:
        watch["last_live_high"] = high
    if not crossed_now:
        return

    candle_key = open_time
    stage = watch["stage"]
    if _entry_already_taken(symbol, stage, candle_key):
        return

    confirmed, detail = _entry_gate_detail(state, watch, open_price, high, close, volume)
    detail["entry_candle_open_time"] = candle_key
    _mark_entry_taken(symbol, stage, candle_key)
    # Crossing happened, so this setup is dead regardless of pass/fail.
    state["watch"] = None

    if not confirmed:
        # Deliberately silent: user requested no noisy scanning/rejection logs.
        return

    tag = "ðŸŸ¢ SPOT" if state["tradeable"] else "ðŸŸ¡ ALPHA (manual only)"
    msg = (f"{tag} {symbol}\nEntry signal: {stage.upper()} breakout (LIVE)\n"
           f"Breakout Price: {close:.6f}\nTimeframe: 5m\n"
           f"{_detail_line(detail)} | Breakout RVOL={detail['breakout_rvol_ratio']:.2f}x")
    log.info(msg.replace(chr(10), " | "))
    _run_bg(send_telegram, msg)
    if state["tradeable"] and ENABLE_TRADING:
        _run_bg(open_long, symbol, stage, detail)


# ----------------------------- WEBSOCKET LOOPS -----------------------------
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
                    on_live_tick(symbol, o, h, c, v, k["t"])
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
                    on_live_tick(name, o, h, c, v, k["t"])
            except Exception as e:
                log.warning("Alpha WS message error: %s", e)


async def periodic_tasks(duration_seconds):
    """Updates the heartbeat every minute (silently, for the watchdog) but
    only PRINTS a status line every HEALTH_LOG_EVERY_SECONDS, so routine
    logs don't drown out the entry/exit lines that actually matter. Nothing
    here ever goes to Telegram -- Telegram only gets the actual trade
    signal/open/close messages, never periodic status noise. Returns
    (letting the caller trigger a full reseed+reconnect) after
    `duration_seconds`."""
    global _last_heartbeat
    elapsed = 0
    HEALTH_LOG_EVERY_SECONDS = 30 * 60          # print a routine status line only this often
    since_health_log = 0
    while elapsed < duration_seconds:
        await asyncio.sleep(60)
        elapsed += 60
        since_health_log += 60
        _last_heartbeat = time.time()  # keeps the watchdog satisfied every cycle, no log line needed for this alone
        _prune_old_entries()

        if since_health_log >= HEALTH_LOG_EVERY_SECONDS:
            since_health_log = 0
            log_portfolio_summary(also_telegram=False)
            mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
            pending = _CANDLE_EXECUTOR._work_queue.qsize()
            log.info("HEALTH: memory=%.1fMB | symbols_tracked=%d | candle-close queue backlog=%d | dedup_cache=%d",
                      mem_mb, len(_symbol_state), pending, len(_entries_taken))


# ----------------------------- WATCHDOG ------------------------------------
# Even after fixing every hang cause found so far, there could always be a
# not-yet-seen one. Rather than keep chasing causes one at a time, this is
# an unconditional safety net: if the heartbeat above hasn't updated in
# WATCHDOG_TIMEOUT_SECONDS, something is stuck badly enough that nothing in
# this process can be trusted to recover on its own -- so it hard-exits the
# whole process (os._exit, not a normal exception) and lets Railway restart
# it completely fresh. This runs on a plain OS thread, independent of
# asyncio, so it keeps working even if the event loop itself is wedged.
_last_heartbeat = time.time()
WATCHDOG_TIMEOUT_SECONDS = 300  # 5 min -- heartbeat is expected every 60s


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
    # Publish the current V4 state message at startup. Older pinned/local
    # state is intentionally ignored, so the new strategy starts at zero.
    _sync_state_to_telegram()
    _start_watchdog()
    while True:
        try:
            asyncio.run(run_bot_cycle())
        except Exception as e:
            log.error("Bot cycle ended/crashed: %s -- doing a full fresh reseed+reconnect in 10s", e)
        time.sleep(10)


if __name__ == "__main__":
    main()
