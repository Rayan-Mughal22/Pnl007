"""
Binance Wave Strategy Bot (Spot + Alpha scanning, Spot demo trading)
---------------------------------------------------------------------
5-minute chart, fresh build. Scans:
  - All Binance Spot USDT pairs (alerts + auto demo-trades on Testnet)
  - All Binance Alpha tokens (alerts only -- Alpha can't be demo-traded,
    it uses a different swap mechanism, not the classic order book)

STRATEGY (as described)
Trend filter (must hold): EMA9 > EMA20 > VWAP > EMA200
Volume: current candle volume >= 2x AT LEAST ONE of the 10/20/30/50-period
        volume moving averages (not all four at once)
MACD: MACD line above its signal line (covers "already above" and the
      moment just after crossing up)

Rally 1: 2+ consecutive green candles, each with a real body (>= RALLY_MIN_BODY_PCT),
         with each candle's volume >= the previous one's (rising volume),
         while trend + volume(RVOL) + MACD hold on the latest candle.

Pullback 1: 1-3 small red candles after rally 1 -- body and volume both
            clearly smaller than the rally's own candles (buyers stepping
            back, not sellers taking over).
Invalidation: if a pullback candle has a BIG body (as big as the rally's
              candles) and price retraces >=50% of rally 1's range -> this
              setup is scrapped, back to looking for a fresh rally 1.

Entry (rally 2): once green candles resume, watch up to 3 of them. The
                  moment one closes above pullback 1's FIRST red candle's
                  high, with rising green volume -> ENTRY.

Rally 3: same mechanism can repeat once more after rally 2 (pullback 2,
         breakout of pullback 2's first red candle's high) for a second,
         separate entry on the same coin. No further entries after rally 3
         in the same wave -- the state resets and looks for a brand new
         rally 1 from scratch.

EXIT: ride the green rally you entered on; exit the moment the FIRST red
(pullback) candle appears right after it, taking whatever profit built up
during that green run. Applies the same way to both the rally-2 entry and
the rally-3 entry (each rides its own green run and exits on its own first
red candle).

MONEY MANAGEMENT
Demo account budget: $500 total. Each trade: $50. So at most 10 trades can
be open across all coins at once (500 / 50). Both rally-2 and rally-3
entries on the same coin count separately toward this cap.

All the "how big is a good rally/pullback candle" numbers below are my own
reasonable defaults (you asked me to pick these) -- tune them in CONFIG.
"""

import os
import time
import math
import hmac
import hashlib
import logging
import requests
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

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
KLINE_INTERVAL = "5m"
KLINE_LIMIT = 260
QUOTE_ASSET = "USDT"
SCAN_INTERVAL_SECONDS = 60  # check every 1 minute; candles are still 5m
SCAN_CYCLE_TIMEOUT_SECONDS = 900  # 15 min safety net -- a normal cycle takes ~7-8 min

VOL_MA_PERIODS = [10, 20, 30, 50]
VOL_MULTIPLIER = 1.0             # RVOL threshold vs. ANY ONE of the MA periods above

RALLY_MIN_BODY_PCT = 0.4         # min body % for a green rally / red invalidation candle
PULLBACK_MAX_BODY_RATIO = 0.5    # pullback candle body must be <= 50% of avg rally candle body
PULLBACK_MAX_VOL_RATIO = 0.6     # pullback candle volume must be <= 60% of avg rally candle volume
INVALIDATION_RETRACE_PCT = 0.30  # pullback retrace vs. rally's range must stay under 30%
# No candle-count cap on the pullback -- it stays valid for as long as the
# retracement stays under INVALIDATION_RETRACE_PCT, however many candles that takes.
MAX_RALLY_WATCH_CANDLES = 3      # breakout must happen within this many new green candles

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("wavebot")

# symbol -> list of open positions: [{"qty":.., "entry_price":.., "entry_time":.., "stage":..}]
_open_positions = {}
_realized_pnl_total = 0.0
_entries_taken = {}  # f"{symbol}-{stage}-{candle_open_time}" -> timestamp added (dedup guard)


def _entry_already_taken(symbol: str, stage: str, candle_key) -> bool:
    key = f"{symbol}-{stage}-{candle_key}"
    return key in _entries_taken


def _mark_entry_taken(symbol: str, stage: str, candle_key):
    key = f"{symbol}-{stage}-{candle_key}"
    _entries_taken[key] = time.time()


def _prune_old_entries():
    cutoff = time.time() - 24 * 3600
    stale = [k for k, ts in _entries_taken.items() if ts < cutoff]
    for k in stale:
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


# ----------------------------- SPOT DATA ----------------------------------
def get_usdt_symbols():
    r = requests.get(f"{BINANCE_BASE}/api/v3/exchangeInfo", timeout=15)
    r.raise_for_status()
    data = r.json()
    return [
        s["symbol"] for s in data["symbols"]
        if s["quoteAsset"] == QUOTE_ASSET and s["status"] == "TRADING"
        and s.get("isSpotTradingAllowed", True)
    ]


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


# ----------------------------- ALPHA DATA ----------------------------------
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


def _rvol_ok(row) -> bool:
    for p in VOL_MA_PERIODS:
        ma = row.get(f"vol_ma_{p}")
        if ma is not None and not math.isnan(ma) and ma > 0 and row["volume"] >= VOL_MULTIPLIER * ma:
            return True
    return False


def _trend_ok(row) -> bool:
    return row["ema9"] > row["ema20"] > row["vwap"] > row["ema200"]


def _macd_ok(row) -> bool:
    return row["macd"] > row["macd_signal"]


# ----------------------------- WAVE STATE MACHINE --------------------------
def analyze_symbol(df: pd.DataFrame):
    """
    Rebuilds the whole rally/pullback/breakout sequence from the fetched
    candle window every time (stateless across restarts -- safe for
    redeploys). Returns ("rally2"|"rally3", entry_price) if a fresh ENTRY
    happened on the most recently CLOSED candle, else None.
    """
    n = len(df)
    if n < 210:
        return None

    last_closed_idx = n - 2  # -1 may be a live/incomplete candle
    start_idx = 200          # need EMA200 to be meaningful

    phase = "WAIT_RALLY1"
    rally_candles = []          # current rally's candles (rally1 or rally2, reused)
    rally_range = None
    pullback_candles = []
    pullback_first_high = None
    watch_candles = []          # green candles being watched for breakout
    stage_after_pullback = None  # "rally2" or "rally3" -- which breakout we're watching for
    entry_index = None
    entry_stage = None
    entry_price = None

    def avg_body(cands):
        return sum(abs(c["body_pct"]) for c in cands) / len(cands) if cands else 0

    def avg_vol(cands):
        return sum(c["volume"] for c in cands) / len(cands) if cands else 0

    for i in range(start_idx, last_closed_idx + 1):
        row = df.iloc[i]
        is_green = row["close"] > row["open"]
        is_red = row["close"] < row["open"]
        body_pct = abs(row["body_pct"])

        if phase == "WAIT_RALLY1":
            if is_green and body_pct >= RALLY_MIN_BODY_PCT:
                if rally_candles and row["volume"] < rally_candles[-1]["volume"]:
                    rally_candles = [row]  # volume dipped, restart the count
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
                if r_size > 0 and (retrace / r_size) >= INVALIDATION_RETRACE_PCT:
                    phase = "WAIT_RALLY1"
                    rally_candles = []
            elif is_green:
                if not pullback_candles:
                    rally_candles.append(row)  # rally just extended, no pullback yet
                else:
                    pullback_first_high = pullback_candles[0]["high"]
                    watch_candles = [row]
                    # Check THIS candle for breakout too -- it may already cross
                    # the pullback high (previously only later candles were
                    # checked, which meant an immediate breakout was missed).
                    if row["high"] > pullback_first_high:
                        entry_index = i
                        entry_stage = stage_after_pullback
                        entry_price = row["close"]
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
                watch_candles.append(row)
                vol_rising = len(watch_candles) < 2 or row["volume"] >= watch_candles[-2]["volume"]
                if row["high"] > pullback_first_high and vol_rising:
                    entry_index = i
                    entry_stage = stage_after_pullback
                    entry_price = row["close"]
                    if stage_after_pullback == "rally2":
                        # allow one more cycle (rally 3) before fully resetting
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

    if entry_index == last_closed_idx:
        candle_key = df.iloc[last_closed_idx]["open_time"]
        return entry_stage, entry_price, candle_key

    # No breakout found in fully-closed candles -- also check the LIVE
    # (still-forming) candle. Waiting for a candle to fully close before
    # reacting means entries land several minutes late, often at a much
    # worse price than the actual breakout. If we're currently watching
    # for a breakout and the live candle has already crossed the level,
    # enter now instead of waiting up to 5 more minutes.
    if phase == "WATCH_BREAKOUT" and n >= 1:
        live = df.iloc[-1]
        if live["high"] > pullback_first_high:
            candle_key = live["open_time"]
            return stage_after_pullback, live["close"], candle_key

    return None


def check_exit(df: pd.DataFrame) -> bool:
    """
    Exit rule: ride the green rally you entered on, and get out the moment
    the FIRST red (pullback) candle appears after it -- take whatever
    profit built up during the green run. Applies the same way whether the
    open position came from a rally2 or rally3 entry.
    """
    if len(df) < 3:
        return False
    row = df.iloc[-2]  # last CLOSED candle
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


def open_long(symbol: str, stage: str):
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
    log.info("TRADE OPEN  %s [%s] | qty=%.6f entry=%.6f cost=%.2f USDT (fee ~%.3f) | open trades=%d/%d",
              symbol, stage, qty, entry_price, quote_spent, entry_fee, total_open_trades(), MAX_CONCURRENT_TRADES)


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
        pnl = gross_pnl - total_fees  # net of Binance's standard 0.1%-per-side fee
        _realized_pnl_total += pnl
        hold_minutes = (time.time() - pos["entry_time"]) / 60
        log.info(
            "TRADE CLOSE %s [%s] | entry=%.6f exit=%.6f gross=%.2f fees=%.2f net_pnl=%.2f USDT "
            "(%.1f min held) | running total net pnl=%.2f",
            symbol, pos["stage"], pos["entry_price"], exit_price, gross_pnl, total_fees, pnl,
            hold_minutes, _realized_pnl_total
        )
    del _open_positions[symbol]


def log_portfolio_summary():
    if not _open_positions:
        log.info("PORTFOLIO: no open positions | %d/%d slots used | realized net PnL (fees included): %.2f USDT",
                  0, MAX_CONCURRENT_TRADES, _realized_pnl_total)
        return
    lines = []
    for s, positions in _open_positions.items():
        for p in positions:
            lines.append(f"{s} [{p['stage']}]: qty={p['qty']:.4f} entry={p['entry_price']:.6f}")
    log.info(
        "PORTFOLIO: %d/%d slots used | realized net PnL (fees included): %.2f USDT\n  %s",
        total_open_trades(), MAX_CONCURRENT_TRADES, _realized_pnl_total, "\n  ".join(lines)
    )


# ----------------------------- MAIN LOOP ----------------------------------
def process_symbol(symbol: str, df, tradeable: bool):
    df = add_indicators(df)

    result = analyze_symbol(df)
    if result:
        stage, price, candle_key = result
        if not _entry_already_taken(symbol, stage, candle_key):
            _mark_entry_taken(symbol, stage, candle_key)
            tag = "ðŸŸ¢ SPOT" if tradeable else "ðŸŸ¡ ALPHA (manual only)"
            msg = f"{tag} {symbol}\nEntry signal: {stage.upper()} breakout\nPrice: {price:.6f}\nTimeframe: 5m"
            log.info("SIGNAL: %s", msg.replace(chr(10), " | "))
            send_telegram(msg)
            if tradeable and ENABLE_TRADING:
                open_long(symbol, stage)

    if tradeable and symbol in _open_positions and check_exit(df):
        close_all_for_symbol(symbol)


def scan_once():
    _prune_old_entries()
    spot_symbols = get_usdt_symbols()
    alpha_tokens = get_alpha_tokens()
    log.info("Scanning %d spot pairs + %d Alpha tokens...", len(spot_symbols), len(alpha_tokens))

    for symbol in spot_symbols:
        try:
            df = get_spot_klines(symbol)
            if df is not None:
                process_symbol(symbol, df, tradeable=True)
        except Exception as e:
            log.warning("Error processing spot %s: %s", symbol, e)
        time.sleep(0.12)

    for token in alpha_tokens:
        try:
            df = get_alpha_klines(token["alpha_symbol"])
            if df is not None:
                process_symbol(token["name"], df, tradeable=False)
        except Exception as e:
            log.warning("Error processing alpha %s: %s", token["alpha_symbol"], e)
        time.sleep(0.12)


def main():
    log.info(
        "Wave bot starting. Telegram: %s | Trading: %s | Budget: $%d ($%d/trade, max %d concurrent)",
        bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID), ENABLE_TRADING,
        ACCOUNT_BUDGET_USDT, TRADE_SIZE_USDT, MAX_CONCURRENT_TRADES
    )
    send_telegram("âœ… Wave strategy bot is online (Spot + Alpha scanning).")
    executor = ThreadPoolExecutor(max_workers=1)
    while True:
        start = time.time()
        try:
            future = executor.submit(scan_once)
            future.result(timeout=SCAN_CYCLE_TIMEOUT_SECONDS)
        except FutureTimeoutError:
            log.error(
                "Scan cycle exceeded %ds and appears stuck -- abandoning it and "
                "starting a fresh cycle (the stuck one is left to die in the background).",
                SCAN_CYCLE_TIMEOUT_SECONDS
            )
            executor = ThreadPoolExecutor(max_workers=1)  # fresh worker, old one abandoned
        except Exception as e:
            log.error("Scan cycle failed: %s", e)
        log_portfolio_summary()
        elapsed = time.time() - start
        sleep_for = max(5, SCAN_INTERVAL_SECONDS - elapsed)
        log.info("Cycle done in %.1fs, sleeping %.1fs", elapsed, sleep_for)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()                            
