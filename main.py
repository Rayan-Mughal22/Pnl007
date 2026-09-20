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
Volume (rally1 formation only): RVOL >= 1x vs. ANY ONE of the 10/20/30/50
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
VOL_MULTIPLIER = 1.0             # RVOL threshold vs. ANY ONE of the MA periods above

RALLY_MIN_BODY_PCT = 0.4         # min body % for a green rally candle
PULLBACK_MAX_VOL_RATIO = 0.6     # pullback candle volume must be <= 60% of avg rally candle volume
INVALIDATION_RETRACE_PCT = 0.30  # pullback retrace vs. rally's range must stay under 30%
MAX_RALLY_WATCH_CANDLES = 3      # breakout must happen within this many new green candles

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("wavebot")

# symbol -> list of open positions: [{"qty":.., "entry_price":.., "entry_fee":.., "entry_time":.., "stage":..}]
_open_positions = {}
_realized_pnl_total = 0.0
_entries_taken = {}  # f"{symbol}-{stage}-{candle_open_time}" -> timestamp added (dedup guard)


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
    """Same 'RVOL >= 1x vs ANY ONE of the 10/20/30/50-period MAs' check as
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
    """Fires the instant price crosses the pullback's first red candle's
    high (wick included), with volume showing at least some increase vs.
    the immediately preceding candle, AND RVOL >= 1x (same floor as rally1
    formation, no ceiling -- higher is always fine) at the breakout itself.
    Takes plain values (not a pandas row) so it can be called cheaply on
    every live WebSocket tick."""
    if high <= pullback_first_high:
        return False
    if prev_volume is not None and volume < prev_volume:
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
    }


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
                    stage_after_pullback = "rally2"
            else:
                rally_candles = []

        elif phase == "PULLBACK":
            if is_red:
                pullback_candles.append(row)
                r_high, r_low = rally_range[1], rally_range[0]
                r_size = r_high - r_low
                retrace = r_high - row["low"]
                rally_avg_vol = _avg_vol(rally_candles)
                too_deep = r_size > 0 and (retrace / r_size) >= INVALIDATION_RETRACE_PCT
                too_heavy = rally_avg_vol > 0 and row["volume"] > PULLBACK_MAX_VOL_RATIO * rally_avg_vol
                if too_deep or too_heavy:
                    phase = "WAIT_RALLY1"
                    rally_candles = []
            elif is_green:
                if not pullback_candles:
                    rally_candles.append(row)
                else:
                    pullback_first_high = pullback_candles[0]["high"]
                    watch_candles = [row]
                    if _breakout_confirmed(row["high"], row["volume"], pullback_first_high, pullback_candles[-1]["volume"], _row_vol_ma_lookup(row)):
                        entry_index = i
                        entry_stage = stage_after_pullback
                        entry_price = row["close"]
                        entry_detail = _entry_detail(row)
                        if stage_after_pullback == "rally2":
                            rally_range = (min(c["low"] for c in watch_candles), max(c["high"] for c in watch_candles))
                            rally_candles = list(watch_candles)
                            phase = "PULLBACK"
                            pullback_candles = []
                            stage_after_pullback = "rally3"
                        else:
                            phase = "WAIT_RALLY1"
                            rally_candles = []
                    else:
                        phase = "WATCH_BREAKOUT"

        elif phase == "WATCH_BREAKOUT":
            if is_green:
                prev_vol = watch_candles[-1]["volume"] if watch_candles else pullback_candles[-1]["volume"]
                watch_candles.append(row)
                if _breakout_confirmed(row["high"], row["volume"], pullback_first_high, prev_vol, _row_vol_ma_lookup(row)):
                    entry_index = i
                    entry_stage = stage_after_pullback
                    entry_price = row["close"]
                    entry_detail = _entry_detail(row)
                    if stage_after_pullback == "rally2":
                        rally_range = (min(c["low"] for c in watch_candles), max(c["high"] for c in watch_candles))
                        rally_candles = list(watch_candles)
                        phase = "PULLBACK"
                        pullback_candles = []
                        stage_after_pullback = "rally3"
                    else:
                        phase = "WAIT_RALLY1"
                        rally_candles = []
                elif len(watch_candles) > MAX_RALLY_WATCH_CANDLES:
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
    if phase == "WATCH_BREAKOUT":
        prev_vol = watch_candles[-1]["volume"] if watch_candles else pullback_candles[-1]["volume"]
        last_row = watch_candles[-1] if watch_candles else pullback_candles[-1]
        watch_state = {
            "pullback_first_high": pullback_first_high,
            "prev_volume": prev_vol,
            "stage": stage_after_pullback,
            "vol_ma_lookup": _row_vol_ma_lookup(last_row),
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
    _open_positions.setdefault(symbol, []).append({
        "qty": qty, "entry_price": entry_price, "entry_fee": entry_fee,
        "entry_time": time.time(), "stage": stage
    })
    detail_str = ""
    if detail:
        detail_str = (
            f" | RVOL={detail['rvol_ratio']:.2f}x (vs {detail['rvol_period']}-period MA) | "
            f"MACD {'above' if detail['macd_above_signal'] else 'below'} signal "
            f"(macd={detail['macd']:.6f}, signal={detail['macd_signal']:.6f})"
        )
    log.info("TRADE OPEN  %s [%s] | qty=%.6f entry=%.6f cost=%.2f USDT (fee ~%.3f) | open trades=%d/%d%s",
              symbol, stage, qty, entry_price, quote_spent, entry_fee, total_open_trades(), MAX_CONCURRENT_TRADES, detail_str)


def close_all_for_symbol(symbol: str):
    global _realized_pnl_total
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
        _realized_pnl_total += pnl
        hold_minutes = (time.time() - pos["entry_time"]) / 60
        log.info(
            "TRADE CLOSE %s [%s] | entry=%.6f exit=%.6f gross=%.2f fees=%.2f net_pnl=%.2f USDT "
            "(%.1f min held) | running total net pnl=%.2f",
            symbol, pos["stage"], pos["entry_price"], exit_price, gross_pnl, total_fees, pnl,
            hold_minutes, _realized_pnl_total
        )
    del _open_positions[symbol]


def log_portfolio_summary(also_telegram: bool = False):
    if not _open_positions:
        msg = (f"PORTFOLIO: no open positions | 0/{MAX_CONCURRENT_TRADES} slots used | "
               f"realized net PnL (fees included): {_realized_pnl_total:.2f} USDT")
    else:
        lines = []
        for s, positions in _open_positions.items():
            for p in positions:
                lines.append(f"{s} [{p['stage']}]: qty={p['qty']:.4f} entry={p['entry_price']:.6f}")
        msg = (f"PORTFOLIO: {total_open_trades()}/{MAX_CONCURRENT_TRADES} slots used | "
               f"realized net PnL (fees included): {_realized_pnl_total:.2f} USDT\n  " + "\n  ".join(lines))
    log.info(msg.replace(chr(10), "\n"))
    if also_telegram:
        send_telegram(f"ðŸ“Š Status update\n{msg}")


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
    tag = "ðŸŸ¢ SPOT" if tradeable else "ðŸŸ¡ ALPHA (manual only)"
    msg = f"{tag} {symbol}\nEntry signal: {stage.upper()} breakout\nPrice: {price:.6f}\nTimeframe: 5m"
    log.info(
        "SIGNAL: %s | RVOL=%.2fx (vs %s-period MA) | MACD %s signal (macd=%.6f, signal=%.6f)",
        msg.replace(chr(10), " | "), detail["rvol_ratio"], detail["rvol_period"],
        "above" if detail["macd_above_signal"] else "below", detail["macd"], detail["macd_signal"]
    )
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


def on_live_tick(symbol: str, high: float, close: float, volume: float, open_time):
    state = _symbol_state.get(symbol)
    if state is None:
        return
    watch = state.get("watch")
    if not watch:
        return
    if _breakout_confirmed(high, volume, watch["pullback_first_high"], watch["prev_volume"], watch.get("vol_ma_lookup")):
        candle_key = open_time
        stage = watch["stage"]
        if _entry_already_taken(symbol, stage, candle_key):
            return
        # Build a lightweight detail dict from the last known closed-candle
        # indicators (good enough for logging -- indicators don't move much
        # within one 5m candle).
        last_closed = state["df"].iloc[-1]
        detail = _entry_detail(last_closed)
        _mark_entry_taken(symbol, stage, candle_key)
        tag = "ðŸŸ¢ SPOT" if state["tradeable"] else "ðŸŸ¡ ALPHA (manual only)"
        msg = f"{tag} {symbol}\nEntry signal: {stage.upper()} breakout (live)\nPrice: {close:.6f}\nTimeframe: 5m"
        log.info(
            "SIGNAL (live): %s | RVOL=%.2fx (vs %s-period MA) | MACD %s signal (macd=%.6f, signal=%.6f)",
            msg.replace(chr(10), " | "), detail["rvol_ratio"], detail["rvol_period"],
            "above" if detail["macd_above_signal"] else "below", detail["macd"], detail["macd_signal"]
        )
        _run_bg(send_telegram, msg)
        if state["tradeable"] and ENABLE_TRADING:
            _run_bg(open_long, symbol, stage, detail)
        # Clear the watch so we don't refire every tick until the candle
        # closes and the state machine naturally rebuilds (possibly into

        # rally3 pullback-tracking).
        state["watch"] = None


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
                    on_live_tick(symbol, h, c, v, k["t"])
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
                    on_live_tick(name, h, c, v, k["t"])
            except Exception as e:
                log.warning("Alpha WS message error: %s", e)


async def periodic_tasks(duration_seconds):
    """Logs a heartbeat/portfolio summary every minute; returns (letting the
    caller trigger a full reseed+reconnect) after `duration_seconds`."""
    global _last_heartbeat
    elapsed = 0
    TELEGRAM_SUMMARY_EVERY_SECONDS = 6 * 3600  # also post the summary to Telegram every 6h
    since_telegram_summary = 0
    while elapsed < duration_seconds:
        await asyncio.sleep(60)
        elapsed += 60
        since_telegram_summary += 60
        _last_heartbeat = time.time()
        _prune_old_entries()
        send_to_telegram_too = since_telegram_summary >= TELEGRAM_SUMMARY_EVERY_SECONDS
        if send_to_telegram_too:
            since_telegram_summary = 0
        log_portfolio_summary(also_telegram=send_to_telegram_too)
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
                send_telegram("âš ï¸ Bot got stuck and is force-restarting itself now (watchdog triggered). Back online shortly.")
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
    send_telegram("âœ… Wave strategy bot is online (real-time WebSocket, Spot + Alpha).")

    await asyncio.gather(
        spot_ws_loop(spot_symbols),
        alpha_ws_loop(alpha_tokens),
        periodic_tasks(SYMBOL_REFRESH_SECONDS),
    )


def main():
    _start_watchdog()
    while True:
        try:
            asyncio.run(run_bot_cycle())
        except Exception as e:
            log.error("Bot cycle ended/crashed: %s -- doing a full fresh reseed+reconnect in 10s", e)
        time.sleep(10)


if __name__ == "__main__":
    main()
