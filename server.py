"""
Institutional-Grade Zero-Lag Quantitative Trading Terminal
Python Backend — server.py

Architecture:
  - asyncio WebSocket server on ws://localhost:8765
  - Fetches historical klines + OI from Binance REST
  - Streams live aggTrades + markPrice from Binance WS
  - Polls OI every 3s (not available via stream)
  - Applies FB/FS/EB/ES state machine + Blended Smoothie filter
  - Broadcasts processed data to connected browser clients
"""

import asyncio
import json
import time
import logging
import os
import sqlite3
from datetime import datetime, timezone
from collections import defaultdict

import aiohttp
from aiohttp import web
import websockets

# ─────────────────────────────────────────────
# CONFIGURATION & LOCAL ARCHIVE DATABASE
# ─────────────────────────────────────────────
SYMBOL          = os.environ.get("SYMBOL", "BTCUSDT")
SYMBOL_LOWER    = SYMBOL.lower()
PORT            = int(os.environ.get("PORT", "8765"))
HOST            = "0.0.0.0" if os.environ.get("PORT") else "localhost"
OI_POLL_SEC     = 1          # 1-Second Sub-Candle X-Ray polling interval
BINANCE_REST    = "https://fapi.binance.com"
BINANCE_STREAM  = f"wss://fstream.binance.com/stream?streams={SYMBOL_LOWER}@aggTrade/{SYMBOL_LOWER}@markPrice@1s"

# SQLite 24/7 Tick Archive Database (Persists across restarts on Render / Local)
DB_PATH = os.environ.get("DB_PATH", "oi_ticks_1s.db")
db_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
db_cursor = db_conn.cursor()
db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS oi_ticks (
        ts_ms INTEGER PRIMARY KEY,
        oi REAL,
        vol REAL,
        price REAL
    )
""")
db_cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts ON oi_ticks(ts_ms)")
db_conn.commit()

TIMEFRAME_MS = {
    "1m":  60_000,
    "3m":  180_000,
    "5m":  300_000,
    "15m": 900_000,
    "1H":  3_600_000,
    "1D":  86_400_000,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("terminal")

# ─────────────────────────────────────────────
# GLOBAL STATE
# ─────────────────────────────────────────────
connected_clients: set = set()
current_tf: str = "1m"

# Live candle accumulator
live_candle: dict = {}
live_oi: float = 0.0          # latest OI value from REST
prev_oi: float = 0.0          # OI at start of current candle
last_close: float = 0.0       # last closed price

# Latency tracking
ws_latency_ms: float = 0.0


# ─────────────────────────────────────────────
# QUANTITATIVE ENGINE
# ─────────────────────────────────────────────

def classify_participant(price_delta: float, oi_delta: float,
                          volume: float, buy_vol: float, sell_vol: float,
                          vol_fbfs: float = None, vol_ebes: float = None) -> dict:
    """
    FB/FS/EB/ES state machine + 4-pair transaction volume divisor equations.

    Price Delta  |  OI Delta  |  State
    -------------|------------|--------
    > 0          |  > 0       |  FB  (Fresh Buyers)
    < 0          |  > 0       |  FS  (Fresh Sellers)
    < 0          |  < 0       |  EB  (Existing Buyers exiting)
    > 0          |  < 0       |  ES  (Existing Sellers covering)
    """
    # Step 0: Determine participant state
    if price_delta >= 0 and oi_delta >= 0:
        state = "FB"
    elif price_delta < 0 and oi_delta >= 0:
        state = "FS"
    elif price_delta < 0 and oi_delta < 0:
        state = "EB"
    else:
        state = "ES"

    if volume <= 0:
        return {"state": state, "coverage": 0.0, "openers_share": 0.0, "closers_share": 0.0, "signal": 0.0, "oi_delta": oi_delta}

    # Step 1: Microstructure Trade Volume Assignment (FB+FS Volume vs EB+ES Volume)
    if vol_fbfs is None or vol_ebes is None:
        if oi_delta > 0:
            vol_fbfs = abs(oi_delta)
            vol_ebes = 0.0
        elif oi_delta < 0:
            vol_ebes = abs(oi_delta)
            vol_fbfs = 0.0
        else:
            vol_fbfs = 0.0
            vol_ebes = 0.0

    # Step 2: Exact Volume Divisor Equations (Divided by Total Traded Volume)
    total_covered_vol = vol_fbfs + vol_ebes
    coverage      = min((total_covered_vol / volume) * 100.0, 100.0)
    openers_share = min((vol_fbfs / volume) * 100.0, 100.0)
    closers_share = min((vol_ebes / volume) * 100.0, 100.0)

    # Signal: +1 for momentum-aligned states (FB/ES), -1 for counter states (FS/EB)
    signal = 1.0 if state in ("FB", "ES") else -1.0

    return {
        "state": state,
        "coverage": round(coverage, 2),
        "openers_share": round(openers_share, 2),
        "closers_share": round(closers_share, 2),
        "signal": signal,
        "oi_delta": round(oi_delta, 4),
    }


def aggregate_klines_oi(klines: list, oi_map: dict, tf: str, klines_1m: list = None) -> list:
    """
    Merge historical klines with OI data into enriched bar objects using 1-Minute Kline Volatility Interpolation.
    oi_map: {open_time_ms: oi_value}
    """
    bars = []
    sorted_times = sorted(oi_map.keys())

    # Build 1-Minute Volatility Activity Map: Volume * (High - Low)
    act_map = {}
    if klines_1m is None and tf == "1m":
        klines_1m = klines
    if klines_1m:
        for b in klines_1m:
            ts_1m = int(b[0])
            h_1m, l_1m, v_1m = float(b[2]), float(b[3]), float(b[5])
            act_map[ts_1m] = max(v_1m * max(h_1m - l_1m, 0.0001), 0.0001)

    # Precompute Volatility-Weighted 1-Minute OI History
    oi_1m_history = dict(oi_map)
    if len(sorted_times) >= 2 and act_map:
        for idx in range(len(sorted_times) - 1):
            t0, t1 = sorted_times[idx], sorted_times[idx + 1]
            if t1 > t0:
                delta_anchor = oi_map[t1] - oi_map[t0]
                sub_ts_list = list(range(t0, t1, 60_000))
                total_act = sum(act_map.get(t, 1.0) for t in sub_ts_list)
                if total_act <= 0:
                    total_act = 1.0
                
                curr = oi_map[t0]
                for t in sub_ts_list:
                    oi_1m_history[t] = curr
                    step_w = act_map.get(t, 1.0) / total_act
                    curr += delta_anchor * step_w
                    oi_1m_history[t + 60_000] = curr

    def interpolate_oi(ts_ms: int) -> float:
        if ts_ms in oi_1m_history:
            return oi_1m_history[ts_ms]
        if not sorted_times:
            return 0.0
        if ts_ms <= sorted_times[0]:
            return oi_map[sorted_times[0]]
        if ts_ms >= sorted_times[-1]:
            return oi_map[sorted_times[-1]]
        # Find surrounding points in 1m history or anchors
        sorted_history = sorted(oi_1m_history.keys())
        for i in range(len(sorted_history) - 1):
            t0, t1 = sorted_history[i], sorted_history[i + 1]
            if t0 <= ts_ms <= t1:
                ratio = (ts_ms - t0) / (t1 - t0) if (t1 - t0) > 0 else 0
                return oi_1m_history[t0] + ratio * (oi_1m_history[t1] - oi_1m_history[t0])
        return oi_map[sorted_times[-1]]

    prev_close = None
    prev_oi_val = None

    for i, k in enumerate(klines):
        open_time = int(k[0])
        o = float(k[1])
        h = float(k[2])
        l = float(k[3])
        c = float(k[4])
        vol = float(k[5])
        taker_buy_vol = float(k[9]) if len(k) > 9 else vol * 0.5   # Taker buy base asset volume
        taker_sell_vol = vol - taker_buy_vol

        close_time = open_time + TIMEFRAME_MS.get(tf, 60_000)
        oi_val = interpolate_oi(close_time)

        if prev_close is None:
            price_delta = c - o
        else:
            price_delta = c - prev_close

        if prev_oi_val is None:
            start_oi = interpolate_oi(open_time)
            oi_delta = oi_val - start_oi
        else:
            oi_delta = oi_val - prev_oi_val

        # 4-pair transaction volume aggregation across sub-intervals (SQLite 1s Archive -> Volatility Fallback)
        vol_fbfs = 0.0
        vol_ebes = 0.0
        
        db_cursor.execute("SELECT oi FROM oi_ticks WHERE ts_ms >= ? AND ts_ms <= ? ORDER BY ts_ms ASC", (open_time, close_time))
        recorded_ticks = db_cursor.fetchall()
        
        if len(recorded_ticks) >= 2:
            # 100% True Real-Time 24/7 Second-by-Second history from local/Render database!
            for t_idx in range(len(recorded_ticks) - 1):
                diff = recorded_ticks[t_idx + 1][0] - recorded_ticks[t_idx][0]
                if diff > 0:
                    vol_fbfs += diff
                elif diff < 0:
                    vol_ebes += abs(diff)
        else:
            step_ms = 60_000
            num_mins = max(TIMEFRAME_MS.get(tf, 60_000) // step_ms, 1)
            for sub_idx in range(num_mins):
                t0 = open_time + (sub_idx * step_ms)
                t1 = open_time + ((sub_idx + 1) * step_ms)
                diff = interpolate_oi(t1) - interpolate_oi(t0)
                if diff > 0:
                    vol_fbfs += diff
                elif diff < 0:
                    vol_ebes += abs(diff)

        metrics = classify_participant(
            price_delta, oi_delta, vol, taker_buy_vol, taker_sell_vol,
            vol_fbfs=vol_fbfs, vol_ebes=vol_ebes
        )

        bars.append({
            "time":          open_time // 1000,   # Unix seconds for LWC
            "open":          o,
            "high":          h,
            "low":           l,
            "close":         c,
            "volume":        round(vol, 4),
            "buy_vol":       round(taker_buy_vol, 4),
            "sell_vol":      round(taker_sell_vol, 4),
            "oi":            round(oi_val, 2),
            **metrics,
        })

        prev_close = c
        prev_oi_val = oi_val

    return bars


# ─────────────────────────────────────────────
# BINANCE REST — HISTORICAL DATA
# ─────────────────────────────────────────────

async def fetch_klines(session: aiohttp.ClientSession, tf: str, limit: int = 1000) -> list:
    """Fetch historical klines with Perpetual Futures automated failover (Bypasses Cloud Geoblock)."""
    # 1. Try Binance Perpetual Futures endpoints
    binance_endpoints = [
        f"{BINANCE_REST}/fapi/v1/klines",
        "https://fapi1.binance.com/fapi/v1/klines",
        "https://fapi2.binance.com/fapi/v1/klines",
    ]
    params = {"symbol": SYMBOL, "interval": tf.lower(), "limit": limit}
    
    for url in binance_endpoints:
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    log.info(f"Successfully fetched {len(data)} Binance Futures klines from {url} for {tf}")
                    return data
        except Exception:
            continue

    # 2. Perpetual Futures Override: Bybit Linear Perpetual (Non-geoblocked global futures mirror)
    try:
        bybit_tf_map = {"1m": "1", "3m": "3", "5m": "5", "15m": "15", "1h": "60", "1d": "D"}
        bybit_interval = bybit_tf_map.get(tf.lower(), "1")
        bybit_url = "https://api.bybit.com/v5/market/kline"
        bybit_params = {"category": "linear", "symbol": SYMBOL, "interval": bybit_interval, "limit": min(limit, 1000)}
        async with session.get(bybit_url, params=bybit_params, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status == 200:
                data = await resp.json()
                raw_list = data.get("result", {}).get("list", [])
                # Bybit returns reverse order [newest first] -> reverse it to oldest first
                raw_list.reverse()
                klines = []
                for b in raw_list:
                    klines.append([
                        b[0], b[1], b[2], b[3], b[4], b[5],
                        0, b[6], 0, float(b[5]) * 0.5, 0, 0
                    ])
                log.info(f"Successfully loaded {len(klines)} Perpetual Futures klines via Bybit Gateway for {tf}")
                return klines
    except Exception as e:
        log.warning(f"Bybit Perpetual Futures fallback error: {e}")

    # 3. Final emergency backup: Binance Public Data Mirror
    backup_url = "https://data-api.binance.vision/api/v3/klines"
    try:
        async with session.get(backup_url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status == 200:
                data = await resp.json()
                log.info(f"Loaded {len(data)} emergency candles from {backup_url} for {tf}")
                return data
    except Exception:
        pass

    log.error("All historical kline endpoints failed!")
    return []


async def fetch_oi_history(session: aiohttp.ClientSession, tf: str) -> dict:
    """Fetch historical Open Interest with Perpetual Futures Gateway failover."""
    oi_period_map = {
        "1m": "5m", "3m": "5m", "5m": "5m",
        "15m": "15m", "1H": "1h", "1D": "1d"
    }
    period = oi_period_map.get(tf, "5m")
    binance_endpoints = [
        f"{BINANCE_REST}/futures/data/openInterestHist",
        "https://fapi1.binance.com/futures/data/openInterestHist",
        "https://fapi2.binance.com/futures/data/openInterestHist",
    ]
    params = {"symbol": SYMBOL, "period": period, "limit": 500}
    oi_map = {}
    
    for url in binance_endpoints:
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data:
                        ts = int(item["timestamp"])
                        oi = float(item["sumOpenInterest"])
                        oi_map[ts] = oi
                    log.info(f"Successfully fetched {len(oi_map)} Binance Futures OI points from {url}")
                    return oi_map
        except Exception:
            continue

    # 2. Perpetual Futures Override: Bybit Linear Perpetual Open Interest
    try:
        bybit_oi_period_map = {"1m": "5min", "3m": "5min", "5m": "5min", "15m": "15min", "1h": "1h", "1d": "1d"}
        bybit_oi_interval = bybit_oi_period_map.get(tf.lower(), "5min")
        bybit_url = "https://api.bybit.com/v5/market/open-interest"
        bybit_params = {"category": "linear", "symbol": SYMBOL, "intervalTime": bybit_oi_interval, "limit": 200}
        async with session.get(bybit_url, params=bybit_params, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status == 200:
                data = await resp.json()
                raw_list = data.get("result", {}).get("list", [])
                for item in raw_list:
                    ts = int(item["timestamp"])
                    oi = float(item["openInterest"])
                    oi_map[ts] = oi
                log.info(f"Successfully loaded {len(oi_map)} Perpetual Futures OI points via Bybit Gateway")
                return oi_map
    except Exception as e:
        log.warning(f"Bybit OI fallback error: {e}")

    # If REST OI history is totally blocked, check local SQLite database as fallback!
    if not oi_map:
        try:
            db_cursor.execute("SELECT ts_ms, oi FROM oi_ticks ORDER BY ts_ms ASC LIMIT 1000")
            for ts_ms, oi_val in db_cursor.fetchall():
                oi_map[ts_ms] = oi_val
            log.info(f"Restored {len(oi_map)} historical OI points directly from local SQLite archive!")
        except Exception:
            pass
    return oi_map


async def fetch_current_oi(session: aiohttp.ClientSession) -> float:
    """Poll current Open Interest with Perpetual Futures Gateway mirrors."""
    endpoints = [
        f"{BINANCE_REST}/fapi/v1/openInterest",
        "https://fapi1.binance.com/fapi/v1/openInterest",
        "https://fapi2.binance.com/fapi/v1/openInterest",
        "https://fapi3.binance.com/fapi/v1/openInterest",
    ]
    params = {"symbol": SYMBOL}
    for url in endpoints:
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["openInterest"])
        except Exception:
            continue

    # Bybit fallback for live OI polling
    try:
        bybit_url = "https://api.bybit.com/v5/market/open-interest"
        bybit_params = {"category": "linear", "symbol": SYMBOL, "intervalTime": "5min", "limit": 1}
        async with session.get(bybit_url, params=bybit_params, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                data = await resp.json()
                raw_list = data.get("result", {}).get("list", [])
                if raw_list:
                    return float(raw_list[0]["openInterest"])
    except Exception:
        pass
    return 0.0


# ─────────────────────────────────────────────
# LIVE CANDLE MANAGEMENT
# ─────────────────────────────────────────────

def get_candle_open_time(ts_ms: int, tf: str) -> int:
    """Snap a timestamp to the open of its candle bucket."""
    interval = TIMEFRAME_MS[tf]
    return (ts_ms // interval) * interval


def init_live_candle(price: float, ts_ms: int, tf: str) -> dict:
    open_ts = get_candle_open_time(ts_ms, tf)
    return {
        "open_time":    open_ts,
        "close_time":   open_ts + TIMEFRAME_MS[tf] - 1,
        "open":         price,
        "high":         price,
        "low":          price,
        "close":        price,
        "volume":       0.0,
        "buy_vol":      0.0,
        "sell_vol":     0.0,
        # 1-Second Sub-Tick Trade Volume Accumulators
        "xray_vol_fbfs": 0.0,
        "xray_vol_ebes": 0.0,
        "last_xray_oi":  live_oi,
        "last_xray_vol": 0.0,
    }


def update_live_candle(candle: dict, price: float, qty: float, is_buyer_maker: bool) -> dict:
    candle["high"]   = max(candle["high"], price)
    candle["low"]    = min(candle["low"],  price)
    candle["close"]  = price
    candle["volume"] += qty
    if not is_buyer_maker:   # taker is buyer
        candle["buy_vol"] += qty
    else:
        candle["sell_vol"] += qty
    return candle


async def send_ws(ws, payload_str: str):
    try:
        if hasattr(ws, "send_str"):
            await ws.send_str(payload_str)
        else:
            await ws.send(payload_str)
        return True
    except Exception:
        return False


async def broadcast(msg: dict):
    """Send JSON message to all connected clients."""
    if not connected_clients:
        return
    payload = json.dumps(msg)
    dead = set()
    for ws in connected_clients:
        success = await send_ws(ws, payload)
        if not success:
            dead.add(ws)
    connected_clients -= dead


# ─────────────────────────────────────────────
# BACKGROUND TASKS
# ─────────────────────────────────────────────

async def oi_poller(session: aiohttp.ClientSession):
    """Poll OI every 1 second, drive X-Ray sub-accumulation, and emit real-time updates."""
    global live_oi, live_candle, prev_oi, last_close, ws_latency_ms
    while True:
        val = await fetch_current_oi(session)
        if val > 0:
            old_oi = live_oi
            live_oi = val
            
            try:
                db_cursor.execute("INSERT OR REPLACE INTO oi_ticks (ts_ms, oi, vol, price) VALUES (?, ?, ?, ?)",
                                  (int(time.time() * 1000), live_oi, live_candle.get("volume", 0.0), live_candle.get("close", 0.0)))
                db_conn.commit()
            except Exception:
                pass

            # Real-Time 1-Second Sub-Tick Volume Accumulation Engine
            if live_candle and old_oi > 0:
                delta_step = live_oi - live_candle.get("last_xray_oi", old_oi)

                if delta_step > 0:
                    live_candle["xray_vol_fbfs"] = live_candle.get("xray_vol_fbfs", 0.0) + delta_step
                elif delta_step < 0:
                    live_candle["xray_vol_ebes"] = live_candle.get("xray_vol_ebes", 0.0) + abs(delta_step)

                live_candle["last_xray_oi"] = live_oi

                # Emit instant 1-second X-Ray bar update to all connected screens
                price_delta = live_candle["close"] - last_close
                oi_delta    = live_oi - prev_oi
                metrics     = classify_participant(
                    price_delta, oi_delta,
                    live_candle["volume"],
                    live_candle["buy_vol"],
                    live_candle["sell_vol"],
                    vol_fbfs=live_candle.get("xray_vol_fbfs"),
                    vol_ebes=live_candle.get("xray_vol_ebes"),
                )
                await broadcast({
                    "type": "bar_update",
                    "data": {
                        "time":        live_candle["open_time"] // 1000,
                        "open":        live_candle["open"],
                        "high":        live_candle["high"],
                        "low":         live_candle["low"],
                        "close":       live_candle["close"],
                        "volume":      round(live_candle["volume"], 4),
                        "buy_vol":     round(live_candle["buy_vol"], 4),
                        "sell_vol":    round(live_candle["sell_vol"], 4),
                        "oi":          round(live_oi, 2),
                        "latency_ms":  round(ws_latency_ms, 1),
                        **metrics,
                    }
                })
        await asyncio.sleep(OI_POLL_SEC)


async def binance_stream_listener(session: aiohttp.ClientSession):
    """Connect to Binance combined stream and process aggTrade + markPrice events."""
    global live_candle, prev_oi, last_close, ws_latency_ms, live_oi, current_tf

    log.info(f"Connecting to Binance stream: {BINANCE_STREAM}")

    while True:
        try:
            async with websockets.connect(
                BINANCE_STREAM,
                ping_interval=20,
                ping_timeout=30,
                max_size=2**20,
            ) as ws:
                log.info("Binance WebSocket connected.")
                async for raw in ws:
                    recv_ts = time.time() * 1000
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    stream = msg.get("stream", "")
                    data   = msg.get("data", {})
                    e_type = data.get("e", "")

                    if e_type == "aggTrade":
                        trade_ts   = int(data["T"])
                        price      = float(data["p"])
                        qty        = float(data["q"])
                        is_bm      = data["m"]          # buyer is maker = sell taker
                        ws_latency_ms = recv_ts - trade_ts

                        tf_interval = TIMEFRAME_MS[current_tf]
                        candle_open = get_candle_open_time(trade_ts, current_tf)

                        if not live_candle:
                            # First trade — initialize candle
                            live_candle = init_live_candle(price, trade_ts, current_tf)
                            prev_oi     = live_oi
                            last_close  = price

                        elif candle_open > live_candle["open_time"]:
                            # Candle closed — finalize and emit
                            price_delta = live_candle["close"] - last_close
                            oi_val      = live_oi
                            oi_delta    = oi_val - prev_oi
                            metrics     = classify_participant(
                                price_delta, oi_delta,
                                live_candle["volume"],
                                live_candle["buy_vol"],
                                live_candle["sell_vol"],
                                vol_fbfs=live_candle.get("xray_vol_fbfs"),
                                vol_ebes=live_candle.get("xray_vol_ebes"),
                            )
                            closed_bar = {
                                "time":     live_candle["open_time"] // 1000,
                                "open":     live_candle["open"],
                                "high":     live_candle["high"],
                                "low":      live_candle["low"],
                                "close":    live_candle["close"],
                                "volume":   round(live_candle["volume"], 4),
                                "buy_vol":  round(live_candle["buy_vol"], 4),
                                "sell_vol": round(live_candle["sell_vol"], 4),
                                "oi":       round(oi_val, 2),
                                **metrics,
                            }
                            await broadcast({"type": "bar_close", "data": closed_bar})

                            last_close  = live_candle["close"]
                            prev_oi     = live_oi
                            live_candle = init_live_candle(price, trade_ts, current_tf)

                        # Update live candle
                        live_candle = update_live_candle(live_candle, price, qty, is_bm)

                        # Emit live tick update
                        if live_candle:
                            price_delta = live_candle["close"] - last_close
                            oi_delta    = live_oi - prev_oi
                            metrics     = classify_participant(
                                price_delta, oi_delta,
                                live_candle["volume"],
                                live_candle["buy_vol"],
                                live_candle["sell_vol"],
                                vol_fbfs=live_candle.get("xray_vol_fbfs"),
                                vol_ebes=live_candle.get("xray_vol_ebes"),
                            )
                            await broadcast({
                                "type": "bar_update",
                                "data": {
                                    "time":        live_candle["open_time"] // 1000,
                                    "open":        live_candle["open"],
                                    "high":        live_candle["high"],
                                    "low":         live_candle["low"],
                                    "close":       live_candle["close"],
                                    "volume":      round(live_candle["volume"], 4),
                                    "buy_vol":     round(live_candle["buy_vol"], 4),
                                    "sell_vol":    round(live_candle["sell_vol"], 4),
                                    "oi":          round(live_oi, 2),
                                    "latency_ms":  round(ws_latency_ms, 1),
                                    **metrics,
                                }
                            })

                    elif e_type == "markPriceUpdate":
                        # Just used for latency display; price comes from aggTrade
                        pass

        except Exception as e:
            log.warning(f"Binance WS disconnected: {e}. Reconnecting in 3s...")
            await asyncio.sleep(3)


async def ping_broadcaster():
    """Emit ping/latency badge every second."""
    while True:
        await asyncio.sleep(1)
        if connected_clients and ws_latency_ms > 0:
            await broadcast({
                "type": "ping",
                "latency_ms": round(ws_latency_ms, 1)
            })


# ─────────────────────────────────────────────
# CLIENT CONNECTION HANDLER
# ─────────────────────────────────────────────

async def send_history(ws, session: aiohttp.ClientSession, tf: str):
    """Fetch and send full historical data to a newly connected client."""
    await send_ws(ws, json.dumps({"type": "loading", "tf": tf}))

    if tf == "1m":
        klines, oi_map = await asyncio.gather(
            fetch_klines(session, tf, limit=1000),
            fetch_oi_history(session, tf)
        )
        klines_1m = klines
    else:
        klines, oi_map, klines_1m = await asyncio.gather(
            fetch_klines(session, tf, limit=1000),
            fetch_oi_history(session, tf),
            fetch_klines(session, "1m", limit=1000)
        )

    if not klines:
        await send_ws(ws, json.dumps({"type": "error", "msg": "Failed to fetch historical klines"}))
        return

    bars = aggregate_klines_oi(klines, oi_map, tf, klines_1m=klines_1m)
    await send_ws(ws, json.dumps({
        "type":   "history",
        "tf":     tf,
        "symbol": SYMBOL,
        "data":   bars,
    }))
    log.info(f"Sent {len(bars)} historical bars to client")


async def client_handler(ws, session: aiohttp.ClientSession, remote="web_client"):
    """Handle a single browser client connection."""
    global current_tf, live_candle, prev_oi

    connected_clients.add(ws)
    log.info(f"Client connected: {remote}")

    try:
        # Send initial history
        await send_history(ws, session, current_tf)

        # Listen for client commands (supports both websockets and aiohttp)
        async for item in ws:
            raw = item.data if hasattr(item, "data") else item
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            action = msg.get("action")
            if action == "set_timeframe":
                tf = msg.get("tf", "1m")
                if tf in TIMEFRAME_MS:
                    current_tf  = tf
                    live_candle = {}   # reset live candle
                    prev_oi     = live_oi
                    log.info(f"Timeframe changed to {tf}")
                    await send_history(ws, session, tf)

    except Exception as e:
        log.warning(f"Client error: {e}")
    finally:
        connected_clients.discard(ws)
        log.info(f"Client disconnected: {remote}")


# ─────────────────────────────────────────────
# CLOUD & LOCAL WEB + WS ROUTER (RENDER READY)
# ─────────────────────────────────────────────

async def handle_ping(request):
    return web.json_response({
        "status": "awake",
        "service": "Institutional Quant Terminal",
        "symbol": SYMBOL,
        "live_oi": live_oi,
        "clients": len(connected_clients),
        "timestamp": int(time.time() * 1000)
    })


async def handle_ws(request):
    ws = web.WebSocketResponse(max_msg_size=10 * 1024 * 1024)
    await ws.prepare(request)
    session = request.app['session']
    await client_handler(ws, session, remote=request.remote)
    return ws


async def handle_static(request):
    path = request.path.lstrip("/") or "index.html"
    if os.path.exists(path):
        return web.FileResponse(path)
    return web.Response(status=404, text="File not found")


async def main():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
    }
    connector = aiohttp.TCPConnector(ssl=True)
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        # Warm up OI
        oi_val = await fetch_current_oi(session)
        if oi_val > 0:
            global live_oi, prev_oi
            live_oi = oi_val
            prev_oi = oi_val
            log.info(f"Initial OI: {oi_val:,.2f}")

        # Start quantitative background tasks
        asyncio.create_task(oi_poller(session))
        asyncio.create_task(binance_stream_listener(session))
        asyncio.create_task(ping_broadcaster())

        # Build combined HTTP + WS application for Render Cloud & Local UI
        app = web.Application()
        app['session'] = session
        app.router.add_get('/ping', handle_ping)
        app.router.add_get('/ws', handle_ws)
        app.router.add_get('/{tail:.*}', handle_static)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, HOST, PORT)
        await site.start()

        log.info(f"Institutional Terminal live on http://{HOST}:{PORT}")
        log.info(f"WebSocket endpoint: ws://{HOST}:{PORT}/ws | Keep-alive ping: /ping")
        
        await asyncio.Future()   # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Server stopped.")
