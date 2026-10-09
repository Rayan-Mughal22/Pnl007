"""
Binance Spot USDT + Binance Alpha Rally-Pullback Scanner  ->  Telegram instant alerts

Strategy (5m chart), ALL must be true:
  1. RVOL = average volume of the rally / average volume of the 50 candles before it >= 5x
  2. EMA9 > EMA20 > VWAP > EMA200   (EMA9 & EMA20 above VWAP, all three above EMA200)
  3. MACD line > MACD signal line
  4. Rally = 2 or more consecutive green candles
  5. Pullback = 1+ consecutive red candles right after the rally
  6. Pullback retracement < 20% of the rally range
  7. Rally volume bars clearly bigger than pullback red volume bars
  8. Price between 0.01 and 20 USD (no coins like 0.00xx / 0.000xx)
  -> The moment the next candle turns green (live tick, no waiting for 5m close) => Telegram alert

Environment variables (Railway -> Variables):
  TELEGRAM_BOT_TOKEN   (required)
  TELEGRAM_CHAT_ID     (required)
Optional tuning:
  RVOL_MIN (5), RVOL_PERIOD (50), MIN_RALLY_CANDLES (2), MAX_RETRACE (0.20), SCAN_ALPHA (1 = on, 0 = off), MIN_PRICE (0.01), MAX_PRICE (20), VOL_DOMINANCE (1.5)
"""

import asyncio
import json
import logging
import os
import threading
import time

import aiohttp
import websockets

# ----------------------------- CONFIG ---------------------------------------
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

TF_MS = 5 * 60 * 1000
DAY_MS = 24 * 60 * 60 * 1000
HIST = 500                                   # closed candles kept per symbol

RVOL_PERIOD = int(os.getenv("RVOL_PERIOD", "50"))
RVOL_MIN = float(os.getenv("RVOL_MIN", "5"))
MIN_RALLY_CANDLES = int(os.getenv("MIN_RALLY_CANDLES", "2"))
MAX_RETRACE = float(os.getenv("MAX_RETRACE", "0.20"))
# rally avg volume must be at least this many times the pullback avg volume
# (and every pullback red candle must also be below the rally avg volume)
VOL_DOMINANCE = float(os.getenv("VOL_DOMINANCE", "1.5"))

# price filter: only coins priced from 0.01 USD (at most ONE zero after the decimal point,
# e.g. 0.05 ok, 0.005 / 0.0005 not ok) up to 20 USD
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.01"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "20"))

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
_alerts_sent = 0


class Sym:
    __slots__ = ("candles", "setup", "alerted_t", "reseeding")

    def __init__(self):
        self.candles = []        # closed candles: (t, o, h, l, c, v)
        self.setup = None        # valid rally+pullback setup computed on last closed candle
        self.alerted_t = 0       # open time of the candle we already alerted on
        self.reseeding = False


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


def eval_setup(cs):
    """Rally (2+ green) -> pullback (red candles) check on CLOSED candles only."""
    n = len(cs)
    if n < RVOL_PERIOD + MIN_RALLY_CANDLES + 2:
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
    if len(rally) < MIN_RALLY_CANDLES or rally_start < RVOL_PERIOD:
        return None

    # retracement must be < 30% of rally range
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

    # RVOL (overall): average volume of the whole rally vs the 50-candle average volume before the rally
    base = cs[rally_start - RVOL_PERIOD:rally_start]
    ma = sum(x[5] for x in base) / RVOL_PERIOD
    if ma <= 0:
        return None
    best_rvol = avg_r / ma
    if best_rvol < RVOL_MIN:
        return None

    return {
        "last_t": cs[-1][0],
        "rally_n": len(rally),
        "pb_n": len(pb),
        "retrace": retrace * 100,
        "rvol": best_rvol,
        "vol_ratio": avg_r / avg_p,
    }


# ----------------------------- TELEGRAM -------------------------------------
async def tg_send(session, text):
    if not TOKEN or not CHAT_ID:
        log.error("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing")
        return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True}
    for attempt in range(3):
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    return
                log.warning("Telegram HTTP %s: %s", r.status, (await r.text())[:200])
        except Exception as e:
            log.warning("Telegram error: %r", e)
        await asyncio.sleep(2 * (attempt + 1))


def fmt(x):
    return f"{x:.8g}"


def build_alert(sym, live, setup, ind):
    meta = META.get(sym, {})
    if meta.get("kind") == "alpha":
        head = (f"ðŸš€ RALLY-PULLBACK ALERT (5m) - BINANCE ALPHA\n"
                f"{meta.get('name', sym)} ({sym})\n"
                f"Chain ID: {meta.get('chain', '?')}\n"
                f"Contract: {meta.get('contract', '?')}\n")
        link = ""
    else:
        head = f"ðŸš€ RALLY-PULLBACK ALERT (5m)\n{sym}\n"
        link = f"\n\nhttps://www.binance.com/en/trade/{sym[:-4]}_USDT?type=spot"
    return (
        f"{head}"
        f"Price: {fmt(live[4])}  (green candle open: {fmt(live[1])})\n\n"
        f"Rally: {setup['rally_n']} green | Pullback: {setup['pb_n']} red\n"
        f"Retracement: {setup['retrace']:.1f}% (max {MAX_RETRACE * 100:.0f}%)\n"
        f"RVOL (rally avg vs {RVOL_PERIOD}p avg): {setup['rvol']:.1f}x (min {RVOL_MIN:g}x)\n"
        f"Rally vol / pullback vol: {setup['vol_ratio']:.1f}x\n\n"
        f"EMA9 {fmt(ind['e9'])} > EMA20 {fmt(ind['e20'])} > VWAP {fmt(ind['vwap'])} > EMA200 {fmt(ind['e200'])}\n"
        f"MACD {fmt(ind['macd'])} > Signal {fmt(ind['sig'])}"
        f"{link}"
    )


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
        key = f"{aid}USDT".upper()
        if key in META:
            continue
        META[key] = {
            "kind": "alpha",
            "name": t.get("symbol") or str(aid),
            "chain": str(t.get("chainId", "?")),
            "contract": t.get("contractAddress", "?"),
        }
        out.append(key)
    return sorted(out)


async def seed_symbol(session, sym, sem):
    async with sem:
        if META.get(sym, {}).get("kind") == "alpha":
            rows = None
            try:
                async with session.get(
                    f"{ALPHA_REST}/alpha-trade/klines",
                    params={"symbol": sym, "interval": "5m", "limit": HIST + 1},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as r:
                    if r.status == 200:
                        rows = (await r.json()).get("data")
                    elif r.status in (418, 429):
                        await asyncio.sleep(5)
            except Exception as e:
                log.warning("Alpha klines %s error: %r", sym, e)
            await asyncio.sleep(0.15)
        else:
            rows = await rest_get(session, "/api/v3/klines",
                                  {"symbol": sym, "interval": "5m", "limit": HIST + 1})
            await asyncio.sleep(0.05)
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
    st.setup = eval_setup(st.candles)
    return True


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
    global _last_beat, _alerts_sent
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
            st.setup = None
            loop_tasks.append(asyncio.ensure_future(reseed(session, sym)))
            return
        st.setup = eval_setup(cs)
        return

    # live tick of forming candle
    setup = st.setup
    if not setup or st.alerted_t == t:
        return
    if t != setup["last_t"] + TF_MS:      # must be the candle right after the pullback
        return
    if candle[4] <= candle[1]:            # not green (yet)
        return
    if not (MIN_PRICE <= candle[4] <= MAX_PRICE):   # price filter
        return

    ind = trend_check(st.candles, candle)
    if not ind or not ind["ok"]:
        return

    st.alerted_t = t
    _alerts_sent += 1
    log.info("ALERT %s price=%s rvol=%.1fx retrace=%.1f%%", sym, fmt(candle[4]), setup["rvol"], setup["retrace"])
    loop_tasks.append(asyncio.ensure_future(tg_send(session, build_alert(sym, candle, setup, ind))))


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


async def health_logger():
    while True:
        await asyncio.sleep(1800)
        armed = sum(1 for s in STATE.values() if s.setup)
        log.info("HEALTH symbols=%d armed_setups=%d alerts_sent=%d", len(STATE), armed, _alerts_sent)


# ----------------------------- MAIN -----------------------------------------
async def main():
    if not TOKEN or not CHAT_ID:
        log.error("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in Railway variables")
    threading.Thread(target=watchdog, daemon=True).start()

    async with aiohttp.ClientSession() as session:
        symbols, spot_bases = await get_symbols(session)
        if not symbols:
            log.error("Could not load symbols (Binance blocked? Set Railway region to EU West) -> exit")
            os._exit(1)
        alpha = await get_alpha_tokens(session, spot_bases) if SCAN_ALPHA else []
        log.info("Loaded %d spot USDT symbols + %d Alpha tokens, seeding history...", len(symbols), len(alpha))

        sem = asyncio.Semaphore(6)
        results = await asyncio.gather(*(seed_symbol(session, s, sem) for s in symbols + alpha))
        log.info("Seeded %d/%d symbols", sum(1 for r in results if r), len(results))
        symbols = [s for s in symbols if s in STATE and STATE[s].candles]
        alpha = [s for s in alpha if s in STATE and STATE[s].candles]

        global _last_beat
        _last_beat = time.time()

        chunks = [symbols[i:i + STREAMS_PER_CONN] for i in range(0, len(symbols), STREAMS_PER_CONN)]
        tasks = [asyncio.create_task(ws_worker(i + 1, c, session)) for i, c in enumerate(chunks)]
        achunks = [alpha[i:i + ALPHA_STREAMS_PER_CONN] for i in range(0, len(alpha), ALPHA_STREAMS_PER_CONN)]
        tasks += [asyncio.create_task(alpha_ws_worker(i + 1, c, session)) for i, c in enumerate(achunks)]
        tasks.append(asyncio.create_task(health_logger()))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
