"""
Binance Spot USDT + Binance Alpha  Rally-Pullback  DEMO TRADING BOT  (virtual money, Telegram statements)

SETUP (5m chart) - ALL must be true:
  1. RVOL = average 5m-candle volume of the rally / normal 5m-candle volume from the 50-DAY average >= 5x
     (normal 5m volume = average daily volume of the last 50 days / 288)
  2. EMA9 > EMA20 > VWAP > EMA200   (EMA9 & EMA20 above VWAP, all three above EMA200)
  3. MACD line > MACD signal line
  4. Rally = 2 or more consecutive green candles
  5. Pullback = 1+ consecutive red candles right after the rally
  6. Pullback retracement < 10% of the rally range
  7. Rally volume bars clearly bigger than pullback red volume bars
  8. Price between 0.01 and 20 USD (no coins like 0.00xx / 0.000xx)

ENTRY (virtual buy):
  The instant price breaks ABOVE the high of the pullback's FIRST red candle (on a green candle),
  with the trend filters (2, 3) still true at that moment.
  Only on the 2nd and 3rd rally of a chain:
     rally1 + pullback1  -> breakout = rally2 entry
     rally2 ends (exit), pullback2 forms -> breakout = rally3 entry
  After a rally3 entry no more entries on that coin until the chain resets (1 hour).

EXIT (no stop-loss order, two rules, both on LIVE price):
  a) price falls back below the breakout level -> sell immediately
  b) a new red candle starts after the entry candle (price < that candle's open) -> sell immediately

MONEY (demo): pool = 1100 USD (+ net PnL), 500 USD per trade, fees 0.1% per side.
  Two trades can run together while the free pool is >= 500 USD for each.

Telegram / logs: NO scanning messages. Only a detailed statement when a trade opens and when it closes.
Open trades + totals are saved in an edited-in-place pinned Telegram message (survives restarts / redeploys).

Environment variables (Railway -> Variables):
  TELEGRAM_BOT_TOKEN   (required)
  TELEGRAM_CHAT_ID     (required)
Optional tuning:
  START_CAPITAL (1100), TRADE_SIZE (500), FEE_PCT (0.1), MAX_WAIT_CANDLES (3),
  RVOL_MIN (5), RVOL_DAYS (50), MIN_RALLY_CANDLES (2), MAX_RETRACE (0.10), VOL_DOMINANCE (1.5),
  MIN_PRICE (0.01), MAX_PRICE (20), SCAN_ALPHA (1 = on, 0 = off)
"""

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

import aiohttp
import websockets

# ----------------------------- CONFIG ---------------------------------------
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

TF_MS = 5 * 60 * 1000
DAY_MS = 24 * 60 * 60 * 1000
HIST = 500                                   # closed candles kept per symbol

RVOL_DAYS = int(os.getenv("RVOL_DAYS", "50"))
MIN_BASE_DAYS = 7                            # new coins: need at least this many closed days of history
REST_SEM = None                              # set in main()
RVOL_MIN = float(os.getenv("RVOL_MIN", "5"))
MIN_RALLY_CANDLES = int(os.getenv("MIN_RALLY_CANDLES", "2"))
MAX_RETRACE = float(os.getenv("MAX_RETRACE", "0.10"))
# rally avg volume must be at least this many times the pullback avg volume
# (and every pullback red candle must also be below the rally avg volume)
VOL_DOMINANCE = float(os.getenv("VOL_DOMINANCE", "1.5"))

# price filter: only coins priced from 0.01 USD (at most ONE zero after the decimal point,
# e.g. 0.05 ok, 0.005 / 0.0005 not ok) up to 20 USD
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.01"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "20"))

# demo account
START_CAPITAL = float(os.getenv("START_CAPITAL", "1100"))
TRADE_SIZE = float(os.getenv("TRADE_SIZE", "500"))
FEE_PCT = float(os.getenv("FEE_PCT", "0.1"))                 # per side
MAX_WAIT_CANDLES = int(os.getenv("MAX_WAIT_CANDLES", "3"))   # how long to wait for the breakout after the pullback
CHAIN_RESET_MS = 12 * TF_MS                                  # rally2/rally3 chain resets after 1 hour
STATE_PATH = os.getenv("STATE_PATH", "/tmp/demo_state.json")
STATE_MARK = "DEMO-STATE"

REST_HOSTS = ["api.binance.com", "data-api.binance.vision"]
WS_HOSTS = ["stream.binance.com:9443", "data-stream.binance.vision"]
STREAMS_PER_CONN = 150
SCAN_ALPHA = os.getenv("SCAN_ALPHA", "1") == "1"
ALPHA_REST = "https://www.binance.com/bapi/defi/v1/public"
ALPHA_WS = "wss://nbstream.binance.com/w3w/wsa/stream"
ALPHA_STREAMS_PER_CONN = 100
WATCHDOG_SECONDS = 180                       # no WS data for this long -> exit (Railway restarts)

EXCLUDE_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "USDD", "AEUR", "EUR", "EURI",
    "GBP", "TRY", "BRL", "UST", "USTC", "XUSD", "USD1", "PAXG", "WBTC", "WBETH",
}
BAD_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scanner")

_last_beat = time.time()


class Sym:
    __slots__ = ("candles", "arm", "reseeding", "base", "base_day", "chain_n", "chain_t", "chain_entry_t")

    def __init__(self):
        self.candles = []        # closed candles: (t, o, h, l, c, v)
        self.arm = None          # valid setup waiting for the breakout
        self.reseeding = False
        self.base = None         # normal volume of ONE 5m candle = 50-day avg daily volume / 288
        self.base_day = 0        # UTC day number the baseline was computed on
        self.chain_n = 0         # entries taken in the current rally2/rally3 chain
        self.chain_t = 0         # ms time of the last exit in the chain
        self.chain_entry_t = 0   # open time of the last entry candle


STATE = {}
META = {}   # key -> {"kind": "spot"|"alpha", "name": str, "chain": str, "contract": str}


# ----------------------------- INDICATORS -----------------------------------
def ema_series(vals, period):
    a = 2.0 / (period + 1)
    out = []
    e = vals[0]
    for i, v in enumerate(vals):
        e = v if i == 0 else e + a * (v - e)
        out.append(e)
    return out


def trend_check(closed, live):
    """Trend filters evaluated with the live (forming) candle as the last point."""
    seq = closed[-HIST:] + [live]
    if len(seq) < 210:
        return None
    closes = [x[4] for x in seq]
    e9 = ema_series(closes, 9)[-1]
    e20 = ema_series(closes, 20)[-1]
    e200 = ema_series(closes, 200)[-1]
    e12 = ema_series(closes, 12)
    e26 = ema_series(closes, 26)
    macd = [a - b for a, b in zip(e12, e26)]
    sig = ema_series(macd, 9)[-1]
    m = macd[-1]

    # VWAP resets at 00:00 UTC
    day = live[0] // DAY_MS
    pv = vol = 0.0
    for x in reversed(seq):
        if x[0] // DAY_MS != day:
            break
        tp = (x[2] + x[3] + x[4]) / 3.0
        pv += tp * x[5]
        vol += x[5]
    if vol <= 0:
        return None
    vwap = pv / vol

    ok = (e9 > e20 > vwap > e200) and (m > sig)
    return {"ok": ok, "e9": e9, "e20": e20, "vwap": vwap, "e200": e200, "macd": m, "sig": sig}


def eval_setup(cs, base):
    """Rally (2+ green) -> pullback (red candles) check on CLOSED candles only."""
    n = len(cs)
    if not base or base <= 0 or n < MIN_RALLY_CANDLES + 2:
        return None

    # pullback = trailing run of red candles
    i = n - 1
    while i >= 0 and cs[i][4] < cs[i][1]:
        i -= 1
    pb_start = i + 1
    if pb_start == n:                       # last closed candle is not red
        return None

    # rally = run of green candles right before the pullback
    j = i
    while j >= 0 and cs[j][4] > cs[j][1]:
        j -= 1
    rally_start = j + 1
    rally = cs[rally_start:pb_start]
    pb = cs[pb_start:]
    if len(rally) < MIN_RALLY_CANDLES:
        return None

    # retracement must be below MAX_RETRACE (10%) of the rally range
    r_high = max(x[2] for x in rally)
    r_low = min(x[3] for x in rally)
    rng = r_high - r_low
    if rng <= 0:
        return None
    pb_low = min(x[3] for x in pb)
    retrace = (r_high - pb_low) / rng
    if retrace >= MAX_RETRACE:
        return None

    # rally volume clearly bigger than pullback volume
    avg_r = sum(x[5] for x in rally) / len(rally)
    avg_p = sum(x[5] for x in pb) / len(pb)
    if avg_p <= 0 or avg_r < VOL_DOMINANCE * avg_p:
        return None
    if max(x[5] for x in pb) >= avg_r:
        return None

    # RVOL (overall): average 5m volume of the whole rally vs normal 5m volume from the 50-day average
    best_rvol = avg_r / base
    if best_rvol < RVOL_MIN:
        return None

    return {
        "last_t": cs[-1][0],
        "pb_start_t": pb[0][0],
        "level": pb[0][2],                 # high of the pullback's FIRST red candle = breakout level
        "r_high": r_high,
        "rng": rng,
        "pb_low": pb_low,
        "rally_n": len(rally),
        "pb_n": len(pb),
        "retrace": retrace * 100,
        "rvol": best_rvol,
        "vol_ratio": avg_r / avg_p,
    }


# ----------------------------- TELEGRAM -------------------------------------
SESSION = None


async def tg_api(method, payload):
    if not TOKEN or not CHAT_ID:
        return None
    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{TOKEN}/{method}", json=payload,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            return await r.json()
    except Exception as e:
        log.warning("Telegram %s error: %r", method, e)
        return None


async def tg_send(text):
    for attempt in range(3):
        r = await tg_api("sendMessage", {"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True})
        if r and r.get("ok"):
            return
        if r:
            log.warning("Telegram send failed: %s", str(r)[:200])
        await asyncio.sleep(2 * (attempt + 1))


def fmt(x):
    return f"{x:.8g}"


def money(x, sign=False):
    return f"{x:+,.2f}" if sign else f"{x:,.2f}"


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def emit(text):
    """One statement -> Railway logs + Telegram (identical content)."""
    log.info("\n%s", text)
    asyncio.ensure_future(tg_send(text))


# ----------------------------- DEMO ACCOUNT ---------------------------------
STATS = {"pnl": 0.0, "trades": 0, "wins": 0, "losses": 0}
POSITIONS = {}          # sym -> position dict (JSON-serialisable)
_state_msg_id = None
_save_dirty = False
_save_running = False


def pool_free():
    pool = START_CAPITAL + STATS["pnl"]
    return pool, pool - TRADE_SIZE * len(POSITIONS)


def name_block(sym):
    meta = META.get(sym, {})
    if meta.get("kind") == "alpha":
        return (f"{meta.get('name', sym)}/USDT  [BINANCE ALPHA]\n"
                f"Alpha ID: {sym}\nChain: {meta.get('chain', '?')}\nContract: {meta.get('contract', '?')}")
    return f"{sym[:-4]}/USDT  [SPOT]"


def account_lines():
    pool, free = pool_free()
    n = STATS["trades"]
    wr = (STATS["wins"] / n * 100) if n else 0.0
    return (f"Pool: {money(pool)} USDT (start {money(START_CAPITAL)}) | Free: {money(free)}\n"
            f"Total net PnL: {money(STATS['pnl'], True)} USDT | Trades: {n} | "
            f"Wins: {STATS['wins']} | Losses: {STATS['losses']} | Win rate: {wr:.1f}%\n"
            f"Open trades: {len(POSITIONS)}")


def open_position(sym, st, candle, arm, ind):
    price = candle[4]
    retrace = (arm["r_high"] - arm["pb_low"]) / arm["rng"] * 100
    pos = {
        "sym": sym, "head": name_block(sym), "stage": arm["stage"],
        "entry_price": price, "qty": TRADE_SIZE / price, "size": TRADE_SIZE,
        "entry_fee": TRADE_SIZE * FEE_PCT / 100, "entry_ms": int(time.time() * 1000),
        "entry_t": candle[0], "level": arm["level"],
        "rally_n": arm["rally_n"], "pb_n": arm["pb_n"], "retrace": retrace,
        "rvol": arm["rvol"], "vol_ratio": arm["vol_ratio"],
        "ind": {k: ind[k] for k in ("e9", "e20", "vwap", "e200", "macd", "sig")},
    }
    POSITIONS[sym] = pos
    st.chain_n += 1
    st.chain_entry_t = candle[0]
    text = (
        f"🟢 DEMO TRADE OPENED - RALLY{pos['stage']} ENTRY\n"
        f"{pos['head']}\n"
        f"Time: {utc_now()}\n\n"
        f"Entry price: {fmt(price)}\n"
        f"Quantity: {fmt(pos['qty'])}\n"
        f"Trade size: {money(TRADE_SIZE)} USDT | Entry fee: {money(pos['entry_fee'])} USDT ({FEE_PCT:g}%)\n"
        f"Breakout level (high of pullback's 1st red candle): {fmt(pos['level'])}\n"
        f"Exit rules: price back below {fmt(pos['level'])}, or the next red candle starts\n\n"
        f"Setup:\n"
        f"Rally: {pos['rally_n']} green | Pullback: {pos['pb_n']} red\n"
        f"Pullback retracement: {retrace:.1f}% (max {MAX_RETRACE * 100:.0f}%)\n"
        f"RVOL (rally vs {RVOL_DAYS}-day avg): {pos['rvol']:.1f}x (min {RVOL_MIN:g}x)\n"
        f"Rally vol / pullback vol: {pos['vol_ratio']:.1f}x\n"
        f"EMA9 {fmt(ind['e9'])} > EMA20 {fmt(ind['e20'])} > VWAP {fmt(ind['vwap'])} > EMA200 {fmt(ind['e200'])}\n"
        f"MACD {fmt(ind['macd'])} > Signal {fmt(ind['sig'])}\n\n"
        f"{account_lines()}"
    )
    emit(text)
    schedule_save()


def close_position(sym, st, price, reason):
    pos = POSITIONS.pop(sym, None)
    if not pos:
        return
    value = pos["qty"] * price
    gross = value - pos["size"]
    exit_fee = value * FEE_PCT / 100
    net = gross - pos["entry_fee"] - exit_fee
    STATS["pnl"] += net
    STATS["trades"] += 1
    STATS["wins" if net > 0 else "losses"] += 1
    if st is not None:
        st.chain_t = int(time.time() * 1000)
    held = max(0, (int(time.time() * 1000) - pos["entry_ms"]) // 1000)
    move = (price / pos["entry_price"] - 1) * 100
    icon = "✅" if net > 0 else "❌"
    text = (
        f"{icon} DEMO TRADE CLOSED - RALLY{pos['stage']} TRADE\n"
        f"{pos['head']}\n"
        f"Time: {utc_now()}\n"
        f"Reason: {reason}\n\n"
        f"Entry: {fmt(pos['entry_price'])} -> Exit: {fmt(price)} ({move:+.2f}%)\n"
        f"Held: {held // 60}m {held % 60}s\n"
        f"Size: {money(pos['size'])} USDT\n"
        f"Gross PnL: {money(gross, True)} USDT\n"
        f"Fees (entry + exit): {money(pos['entry_fee'] + exit_fee)} USDT\n"
        f"NET PnL: {money(net, True)} USDT ({net / pos['size'] * 100:+.2f}%)\n\n"
        f"Setup was: retracement {pos['retrace']:.1f}% | RVOL {pos['rvol']:.1f}x | "
        f"rally vol/pullback vol {pos['vol_ratio']:.1f}x\n\n"
        f"{account_lines()}"
    )
    emit(text)
    schedule_save()


# ----------------------------- STATE PERSISTENCE ----------------------------
def state_text():
    names = ", ".join(p["sym"] for p in POSITIONS.values()) or "none"
    return (f"📌 {STATE_MARK} (do not delete - bot memory)\n"
            f"{account_lines()}\nOpen: {names}\n"
            f"STATE:" + json.dumps({"stats": STATS, "positions": list(POSITIONS.values())},
                                   separators=(",", ":")))


async def tg_state_write(text):
    global _state_msg_id
    if not TOKEN or not CHAT_ID:
        return
    if _state_msg_id:
        r = await tg_api("editMessageText", {"chat_id": CHAT_ID, "message_id": _state_msg_id, "text": text})
        if r is None:
            return                                   # network problem, retry on next save
        if r.get("ok") or "not modified" in str(r.get("description", "")):
            return
        _state_msg_id = None                         # message gone -> create a new one
    r = await tg_api("sendMessage", {"chat_id": CHAT_ID, "text": text, "disable_notification": True})
    if r and r.get("ok"):
        _state_msg_id = r["result"]["message_id"]
        await tg_api("pinChatMessage",
                     {"chat_id": CHAT_ID, "message_id": _state_msg_id, "disable_notification": True})


async def _saver():
    global _save_dirty, _save_running
    try:
        while _save_dirty:
            _save_dirty = False
            text = state_text()
            try:
                with open(STATE_PATH, "w") as f:
                    f.write(text)
            except Exception as e:
                log.warning("State file write failed: %r", e)
            await tg_state_write(text)
            await asyncio.sleep(1)
    finally:
        _save_running = False


def schedule_save():
    global _save_dirty, _save_running
    _save_dirty = True
    if not _save_running:
        _save_running = True
        asyncio.ensure_future(_saver())


async def load_state():
    """Restore pool / stats / open trades from the pinned Telegram message (or the local backup file)."""
    global _state_msg_id
    text = None
    r = await tg_api("getChat", {"chat_id": CHAT_ID})
    pm = r["result"].get("pinned_message") if (r and r.get("ok")) else None
    if pm and STATE_MARK in (pm.get("text") or ""):
        _state_msg_id = pm["message_id"]
        text = pm["text"]
    if not text:
        try:
            with open(STATE_PATH) as f:
                text = f.read()
        except Exception:
            text = None
    if not text or "STATE:" not in text:
        return
    try:
        data = json.loads(text.split("STATE:", 1)[1].strip())
        STATS.update({k: data["stats"][k] for k in STATS})
        for p in data.get("positions", []):
            POSITIONS[p["sym"]] = p
        log.info("State restored: pnl=%s trades=%s open=%d", money(STATS["pnl"], True), STATS["trades"], len(POSITIONS))
    except Exception as e:
        log.warning("Could not parse saved state: %r", e)


# ----------------------------- ENTRY / EXIT LOGIC ---------------------------
def make_arm(st, res):
    """Decide whether a fresh rally+pullback may become an entry (rally2 / rally3 chain)."""
    if st.chain_n and res["pb_start_t"] - max(st.chain_t, st.chain_entry_t) > CHAIN_RESET_MS:
        st.chain_n = 0                                   # chain expired -> start over
    if st.chain_n >= 2:                                  # rally2 and rally3 already traded
        return None
    if st.chain_n == 1 and res["pb_start_t"] <= st.chain_entry_t:
        return None                                      # pullback must come AFTER the traded rally
    arm = dict(res)
    arm["stage"] = st.chain_n + 2
    return arm


def on_close(sym, st):
    """Called after a 5m candle closed (and was appended to st.candles)."""
    if sym in POSITIONS:
        st.arm = None
        return
    cs = st.candles
    res = eval_setup(cs, st.base)
    if res:
        st.arm = make_arm(st, res)
        return
    arm = st.arm
    if not arm:
        return
    last = cs[-1]
    if last[4] < last[1] or last[0] <= arm["last_t"]:     # red candle that failed the checks -> setup dead
        st.arm = None
        return
    if last[3] < arm["pb_low"]:
        arm["pb_low"] = last[3]
    if ((arm["r_high"] - arm["pb_low"]) / arm["rng"] >= MAX_RETRACE
            or last[0] - arm["last_t"] >= MAX_WAIT_CANDLES * TF_MS):
        st.arm = None


def try_entry(sym, st, candle):
    """Live tick: enter the moment price breaks the pullback's first red candle high."""
    arm = st.arm
    t, o, h, l, c = candle[:5]
    if t <= arm["last_t"]:
        return
    if t - arm["last_t"] > MAX_WAIT_CANDLES * TF_MS:
        st.arm = None
        return
    if l < arm["pb_low"]:                                  # pullback got deeper while waiting
        arm["pb_low"] = l
        if (arm["r_high"] - l) / arm["rng"] >= MAX_RETRACE:
            st.arm = None
            return
    if c <= arm["level"] or c <= o:                        # no breakout yet / candle not green
        return

    st.arm = None                                          # breakout moment: judged once
    if sym in POSITIONS or not (MIN_PRICE <= c <= MAX_PRICE):
        return
    if pool_free()[1] < TRADE_SIZE:                        # no free capital
        return
    ind = trend_check(st.candles, candle)
    if not ind or not ind["ok"]:
        return
    open_position(sym, st, candle, arm, ind)


def check_exit(sym, st, pos, candle):
    t, o, h, l, c = candle[:5]
    if c < pos["level"]:
        close_position(sym, st, c, "PRICE FELL BACK BELOW THE BREAKOUT LEVEL")
    elif t > pos["entry_t"] and c < o:
        close_position(sym, st, c, "RED CANDLE STARTED (end of the rally)")


# ----------------------------- BINANCE REST ---------------------------------
async def rest_get(session, path, params=None):
    for host in REST_HOSTS:
        try:
            async with session.get(
                f"https://{host}{path}", params=params, timeout=aiohttp.ClientTimeout(total=20)
            ) as r:
                if r.status == 200:
                    return await r.json()
                if r.status in (418, 429):
                    await asyncio.sleep(5)
                else:
                    log.warning("REST %s%s -> HTTP %s", host, path, r.status)
        except Exception as e:
            log.warning("REST %s%s error: %r", host, path, e)
    return None


async def get_symbols(session):
    info = await rest_get(session, "/api/v3/exchangeInfo")
    if not info:
        return [], set()
    out = []
    bases = set()
    for s in info["symbols"]:
        if s.get("status") != "TRADING" or s.get("quoteAsset") != "USDT":
            continue
        if not s.get("isSpotTradingAllowed", True):
            continue
        base = s["baseAsset"]
        if base in EXCLUDE_BASES or base.endswith(BAD_SUFFIXES):
            continue
        out.append(s["symbol"])
        bases.add(base.upper())
        META[s["symbol"]] = {"kind": "spot", "name": s["symbol"]}
    return sorted(out), bases


async def get_alpha_tokens(session, spot_bases):
    """Binance Alpha tokens -> keys like ALPHA_175USDT (skips tokens already scanned on spot)."""
    try:
        async with session.get(
            f"{ALPHA_REST}/wallet-direct/buw/wallet/cex/alpha/all/token/list",
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            if r.status != 200:
                log.warning("Alpha token list HTTP %s", r.status)
                return []
            data = (await r.json()).get("data") or []
    except Exception as e:
        log.warning("Alpha token list error: %r", e)
        return []
    out = []
    for t in data:
        aid = t.get("alphaId")
        if not aid:
            continue
        cex = (t.get("cexCoinName") or "").upper()
        if cex and cex in spot_bases:      # already covered by the spot scan
            continue
        if t.get("offline"):
            continue
        key = f"{aid}USDT".upper()
        if key in META:
            continue
        META[key] = {
            "kind": "alpha",
            "name": str(t.get("symbol") or t.get("name") or aid).upper(),
            "chain": str(t.get("chainName") or t.get("chainId") or "?"),
            "contract": t.get("contractAddress", "?"),
        }
        out.append(key)
    return sorted(out)


async def fetch_rows(session, sym, interval, limit):
    if META.get(sym, {}).get("kind") == "alpha":
        rows = None
        try:
            async with session.get(
                f"{ALPHA_REST}/alpha-trade/klines",
                params={"symbol": sym, "interval": interval, "limit": limit},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                if r.status == 200:
                    rows = (await r.json()).get("data")
                elif r.status in (418, 429):
                    await asyncio.sleep(5)
        except Exception as e:
            log.warning("Alpha klines %s error: %r", sym, e)
        await asyncio.sleep(0.15)
        return rows
    rows = await rest_get(session, "/api/v3/klines", {"symbol": sym, "interval": interval, "limit": limit})
    await asyncio.sleep(0.05)
    return rows


async def load_base(session, sym):
    """50-day average daily volume -> normal volume of one 5m candle. Returns True on success."""
    rows = await fetch_rows(session, sym, "1d", RVOL_DAYS + 1)
    if not rows:
        return False
    now_ms = int(time.time() * 1000)
    vols = [
        float(r[5]) for r in rows
        if (int(r[6]) if len(r) > 6 else int(r[0]) + DAY_MS - 1) < now_ms    # closed days only
    ][-RVOL_DAYS:]
    if len(vols) < MIN_BASE_DAYS:
        return False
    avg_day = sum(vols) / len(vols)
    st = STATE.setdefault(sym, Sym())
    st.base = avg_day / (DAY_MS // TF_MS) if avg_day > 0 else None
    st.base_day = int(time.time() * 1000) // DAY_MS
    return st.base is not None


async def seed_symbol(session, sym, sem):
    async with sem:
        rows = await fetch_rows(session, sym, "5m", HIST + 1)
        st0 = STATE.get(sym)
        today = int(time.time() * 1000) // DAY_MS
        if rows and (st0 is None or st0.base is None or st0.base_day != today):
            await load_base(session, sym)
    if not rows:
        return False
    now_ms = int(time.time() * 1000)
    cs = [
        (int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))
        for r in rows
        if (int(r[6]) if len(r) > 6 else int(r[0]) + TF_MS - 1) < now_ms   # drop the still-forming candle
    ]
    st = STATE.setdefault(sym, Sym())
    st.candles = cs[-HIST:]
    res = eval_setup(st.candles, st.base)
    if res and sym not in POSITIONS and now_ms - res["last_t"] <= (MAX_WAIT_CANDLES + 1) * TF_MS:
        st.arm = make_arm(st, res)
    return True


async def refresh_base(session, sym):
    """New UTC day -> recompute the 50-day baseline."""
    st = STATE.get(sym)
    if not st:
        return
    st.base_day = int(time.time() * 1000) // DAY_MS      # mark first so we don't spawn duplicates
    async with REST_SEM:
        await load_base(session, sym)


async def reseed(session, sym):
    st = STATE.get(sym)
    if not st or st.reseeding:
        return
    st.reseeding = True
    try:
        await seed_symbol(session, sym, asyncio.Semaphore(1))
    finally:
        st.reseeding = False


# ----------------------------- STREAM HANDLING ------------------------------
def handle_message(raw, session, loop_tasks):
    global _last_beat
    _last_beat = time.time()
    d = json.loads(raw)
    payload = d.get("data", d) if isinstance(d, dict) else None
    k = payload.get("k") if isinstance(payload, dict) else None
    if not k:
        return
    sym = (k.get("s") or payload.get("s") or str(d.get("stream", "")).split("@")[0]).upper()
    st = STATE.get(sym)
    if st is None:
        return

    t = int(k["t"])
    candle = (t, float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"]))

    pos = POSITIONS.get(sym)
    if pos:                                   # open trade: exit rules run on every tick
        check_exit(sym, st, pos, candle)

    if k["x"]:  # candle closed
        cs = st.candles
        if cs and cs[-1][0] == t:
            cs[-1] = candle
        elif not cs or t == cs[-1][0] + TF_MS:
            cs.append(candle)
            if len(cs) > HIST + 50:
                del cs[:-HIST]
        elif META.get(sym, {}).get("kind") == "alpha":
            # Alpha: no trades in a 5m window = no candle, so a gap is normal
            cs.append(candle)
        else:
            # spot gap -> missed candles, reload history
            st.arm = None
            loop_tasks.append(asyncio.ensure_future(reseed(session, sym)))
            return
        on_close(sym, st)
        if t // DAY_MS != st.base_day and (int(time.time() * 1000) // DAY_MS) != st.base_day:
            loop_tasks.append(asyncio.ensure_future(refresh_base(session, sym)))
        return

    # live tick of the forming candle
    if st.arm and sym not in POSITIONS:
        try_entry(sym, st, candle)


async def ws_worker(idx, syms, session):
    first = True
    attempt = 0
    pending = []
    while True:
        host = WS_HOSTS[attempt % len(WS_HOSTS)]
        streams = "/".join(f"{s.lower()}@kline_5m" for s in syms)
        url = f"wss://{host}/stream?streams={streams}"
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=2048) as ws:
                log.info("WS-%d connected (%s) %d streams", idx, host, len(syms))
                if not first:
                    for s in syms:
                        pending.append(asyncio.ensure_future(reseed(session, s)))
                first = False
                attempt = 0
                async for raw in ws:
                    handle_message(raw, session, pending)
                    if len(pending) > 200:
                        pending[:] = [p for p in pending if not p.done()]
        except Exception as e:
            log.warning("WS-%d error: %r -> reconnecting", idx, e)
            attempt += 1
            await asyncio.sleep(min(5 * attempt, 30))


async def alpha_ws_worker(idx, syms, session):
    first = True
    attempt = 0
    pending = []
    while True:
        try:
            async with websockets.connect(ALPHA_WS, ping_interval=20, ping_timeout=20, max_queue=2048) as ws:
                for i in range(0, len(syms), 50):
                    batch = [f"{s.lower()}@kline_5m" for s in syms[i:i + 50]]
                    await ws.send(json.dumps({"method": "SUBSCRIBE", "params": batch, "id": i + 1}))
                    await asyncio.sleep(0.3)
                log.info("ALPHA-WS-%d connected, %d streams", idx, len(syms))
                if not first:
                    for s in syms:
                        pending.append(asyncio.ensure_future(reseed(session, s)))
                first = False
                attempt = 0
                async for raw in ws:
                    handle_message(raw, session, pending)
                    if len(pending) > 200:
                        pending[:] = [p for p in pending if not p.done()]
        except Exception as e:
            log.warning("ALPHA-WS-%d error: %r -> reconnecting", idx, e)
            attempt += 1
            await asyncio.sleep(min(5 * attempt, 30))


# ----------------------------- WATCHDOG -------------------------------------
def watchdog():
    while True:
        time.sleep(30)
        if time.time() - _last_beat > WATCHDOG_SECONDS:
            log.error("Watchdog: no stream data for %ss -> exiting for restart", WATCHDOG_SECONDS)
            os._exit(1)


# ----------------------------- MAIN -----------------------------------------
async def main():
    if not TOKEN or not CHAT_ID:
        log.error("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in Railway variables")
    threading.Thread(target=watchdog, daemon=True).start()

    global SESSION
    async with aiohttp.ClientSession() as session:
        SESSION = session
        await load_state()
        symbols, spot_bases = await get_symbols(session)
        if not symbols:
            log.error("Could not load symbols (Binance blocked? Set Railway region to EU West) -> exit")
            os._exit(1)
        alpha = await get_alpha_tokens(session, spot_bases) if SCAN_ALPHA else []
        log.info("Loaded %d spot USDT symbols + %d Alpha tokens, seeding history...", len(symbols), len(alpha))

        global REST_SEM
        REST_SEM = asyncio.Semaphore(4)
        sem = asyncio.Semaphore(6)
        results = await asyncio.gather(*(seed_symbol(session, s, sem) for s in symbols + alpha))
        log.info("Seeded %d/%d symbols", sum(1 for r in results if r), len(results))
        symbols = [s for s in symbols if s in STATE and STATE[s].candles and STATE[s].base]
        alpha = [s for s in alpha if s in STATE and STATE[s].candles and STATE[s].base]

        # open trades whose coin is no longer scanned can never exit -> close them flat (fees only)
        scanned = set(symbols) | set(alpha)
        for sym in list(POSITIONS):
            if sym not in scanned:
                close_position(sym, STATE.get(sym), POSITIONS[sym]["entry_price"], "COIN NO LONGER AVAILABLE AFTER RESTART")

        global _last_beat
        _last_beat = time.time()
        log.info("DEMO BOT running | pool %s | open trades %d | scanning %d spot + %d alpha (no scan messages)",
                 money(pool_free()[0]), len(POSITIONS), len(symbols), len(alpha))
        schedule_save()

        chunks = [symbols[i:i + STREAMS_PER_CONN] for i in range(0, len(symbols), STREAMS_PER_CONN)]
        tasks = [asyncio.create_task(ws_worker(i + 1, c, session)) for i, c in enumerate(chunks)]
        achunks = [alpha[i:i + ALPHA_STREAMS_PER_CONN] for i in range(0, len(alpha), ALPHA_STREAMS_PER_CONN)]
        tasks += [asyncio.create_task(alpha_ws_worker(i + 1, c, session)) for i, c in enumerate(achunks)]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
