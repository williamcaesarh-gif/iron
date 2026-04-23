#!/usr/bin/env python3
"""
Iron Dome v10 — Book-Driven Taker Bot (Polymarket Crypto Up/Down)

Architecture:
  Layer 1: Direction model — Binance WS + Chainlink RTDS → P_win per market
  Layer 2: Book trigger   — CLOB WS price_change → fire FAK when EV > 0
  Layer 3: Position lifecycle — fill tracking, auto-exit, reversal, settlement

This file is Part 1: the three live data feeds + display.
  - Binance WebSocket (BTC/ETH/SOL 1s trades)
  - Chainlink RTDS WebSocket (oracle prices — what Polymarket actually settles on)
  - Polymarket CLOB order book WebSocket (real-time best bid/ask per token)
  - Market discovery (find current 5m/15m window token IDs from Gamma)
  - Main loop: stream all three + show best ask per market
"""

# ===========================================═══════════════════
# IMPORTS
# ===========================================═══════════════════

import os, sys, json, time, math, random, threading, asyncio, requests, csv
from collections import deque
from datetime import datetime, timezone, timedelta

# Force UTF-8 stdout on Windows to avoid cp932/cp1252 encoding errors
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
from pathlib import Path
from collections import deque

try:
    import websockets
    WS_OK = True
except ImportError:
    WS_OK = False
    print("FATAL: pip install websockets")
    sys.exit(1)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
    CLOB_SDK_OK = True
except ImportError:
    CLOB_SDK_OK = False

# ===========================================═══════════════════
# CONFIG — every tunable in one place
# ===========================================═══════════════════
# Group: what it controls
# Each entry: name, value, comment with sane range + tradeoff

CONFIG = {
    # ── Assets & Timeframes ──
    "assets": [
        {"name": "BTC", "slug_pfx": "btc", "binance_sym": "btcusdt", "rtds_sym": "btc/usd"},
        {"name": "ETH", "slug_pfx": "eth", "binance_sym": "ethusdt", "rtds_sym": "eth/usd"},
        {"name": "SOL", "slug_pfx": "sol", "binance_sym": "solusdt", "rtds_sym": "sol/usd"},
    ],
    "timeframes": [
        {"label": "5m",  "secs": 300},
    ],  # Only 5m — removed 15m

    # ── Sniper ──
    "sniper_start_s":     120,      # Start scanning 30s into window.
    "max_entry_price":    0.69,    # Never buy above 75c. Hard cap. Need ~76% WR to profit.
    "fee_rate":           0.0156,  # Polymarket taker fee on crypto Up/Down = 1.56% (156 bps).
    "require_both_agree": True,    # Require Binance+Chainlink to agree on direction before firing.
    # Conviction thresholds (|delta_pct| = how far price moved from PTB)
    "conv_low":           0.0002,  # 0.02% — too weak, skip
    "conv_high":          0.0003,  # 0.03% — tradeable
    "conv_mega":          0.0012,  # 0.12% — strong signal
    # Edge model: model probability vs CLOB implied probability
    "min_edge_up":        0.02,    # 2c min edge for UP (tighter — MMs price UP aggressively, less margin)
    "min_edge_dn":        0.03,    # 3c min edge for DN (normal — MMs underprice pullbacks, edge to spare)
    "vol_5m":             0.0006,  # assumed 5m price volatility for probability model (entry)
    "vol_5m_stop":        0.0012,  # higher vol for stop-loss — gives positions room to breathe
    # Book pressure: bid/ask imbalance near top of book
    "book_imbalance_min": 0.3,     # min bid_size / (bid_size + ask_size) ratio within depth range
    "book_depth_cents":   0.05,    # look at orders within 5c of best bid/ask
    # VPIN: Volume-synchronized Probability of Informed Trading (from Binance aggTrades)
    "vpin_min":           0.2,     # minimum VPIN to confirm entry (0-1, higher = more informed flow)
    "vpin_window_s":      30,      # seconds of Binance trades to compute VPIN over
    # Freshness: reject entries where the move is already priced in (stale)
    "freshness_lookback": 3,       # seconds to look back for prior delta
    "freshness_ratio":    0.6,     # if delta 3s ago was already ≥60% of current delta, move is stale
    "limit_tolerance":    0.03,    # +3c above best ask for live limit orders

    # ── Sizing ──
    "balance":            30.00,   # Starting balance for dry-run paper trading.
    "size_pct":           0.08,    # 10% of available balance per trade.
    "max_position_usd":   25.00,    # Hard cap per single position.
    "min_order_usd":      1.00,    # Polymarket minimum order value.
    # Streak-based sizing: halve after 3 losses, quarter after 6 losses.
    # Scaled size is floored at min_order_usd ($1) so the bot keeps trading
    # through streaks at minimum size instead of silently skipping. Resets on any win.
    "streak_halve_at":    3,       # consecutive losses before cutting size 50%
    "streak_quarter_at":  6,       # consecutive losses before cutting size 75%

    # ── Lifecycle ──
    "stop_prob_floor":    0.35,    # Stop-loss when model probability drops below 35% (was 0.25 — exits earlier to avoid 5-15c asymmetric stops).

    # ── Trend filter (15min regime) ──
    # Block counter-trend entries when both 5min AND 15min Binance slopes agree.
    # Prevents the $100→$190→$67 giveback from trending sessions.
    "trend_short_s":      300,     # 5min lookback
    "trend_long_s":       900,     # 15min lookback
    "trend_short_thr":    0.0005,  # 0.05% over 5min to call it a trend
    "trend_long_thr":     0.0010,  # 0.10% over 15min to call it a regime
    "trend_override_book": 0.90,   # book imbalance that overrides trend filter (reversal signal)
    "cl_lead_max_age":    2.0,     # max Chainlink age (s) for Chainlink-led UP override
    "cl_lead_enabled":    True,    # enable C>PTB>B → UP override path
    "cl_lead_min_cl":     0.0001,  # min Chainlink delta above PTB (0.01% — filter sub-signal noise, data-backed threshold)
    "cl_lead_min_gap":    0.0002,  # min total feed divergence |cl_delta| + |bnc_delta| (0.02%)
    "cl_exit_floor":      0.0002,  # cl_led exit: Chainlink must stay within this buffer below PTB (0.02% — caps loss before full-stake settlement bleed)
    "flip_book_min":      0.60,    # flip requires opp-side book ≥60% (stricter than entry to avoid whipsaw flips)
    "up_cl_max_secs_remaining": 240,  # UP-CL only fires in last 2min of window (late-window filter, 60% WR)
    "path_c_max_entry":         0.68, # Path C (B>C>PTB) ask cap — tighter than global max_entry_price
    "min_book_entry_up_cl":     0.51, # UP-CL requires UP-side book ≥51% (cuts <50% bleed zone)
    "stop_min_secs_remaining": 15,    # disable stop-loss in final 15s (let settlement resolve, avoid pointless stop-penalty)
    "entry_min_secs_remaining": 10,   # block new entries in final 10s (last-instant dust-price lottery, oracle-flip risk)
    "chainsaw_block":           True, # block further entries/flips on an asset after it was stopped in the same window
    # Delta persistence: require delta to stay on the signal's side for N recent ticks
    # before entering. Addresses "flash signal → reversal" losses seen in chop regimes.
    "persistence_ticks":        3,    # ticks required on correct side (including current)
    "persistence_window_s":     3.5,  # look back this many seconds (scan_interval_s=1.0, so ~3 ticks)
    "persistence_edge_bypass":  0.07, # skip persistence when edge ≥ this (fat-edge mispricings are the signal)

    # ── Infrastructure ──
    "dry_run":            True,    # True=paper trading, False=real CLOB orders.
    "scan_interval_s":    1.0,     # Main loop tick rate for refreshing display + direction model.
    "ws_stale_s":         2.0,     # Max age for WS data before considering it stale.
    "min_balance_halt":   1.50,    # Halt bot if balance drops below this.

    # ── Endpoints ──
    "gamma_url":          "https://gamma-api.polymarket.com",
    "clob_url":           "https://clob.polymarket.com",
    "clob_ws_url":        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "rtds_ws_url":        "wss://ws-live-data.polymarket.com",

    # ── Auth (live mode only, from env) ──
    "pk":                 os.getenv("POLYMARKET_PK", ""),
    "funder":             os.getenv("POLYMARKET_FUNDER", ""),
    "chain_id":           137,
}

C = CONFIG  # shorthand used everywhere


# ===========================================═══════════════════
# LOGGING — structured, greppable, colored
# ===========================================═══════════════════
# Every log line: [HH:MM:SS.mmm]   [TAG] message
# Tags: BNCE=Binance, CHNL=Chainlink, CLOB=orderbook, MKT=market discovery,
#       LIVE=display, SYS=system, WARN=warning

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init()
    COLORS = {
        "bnce": Fore.CYAN,
        "chnl": Fore.YELLOW,
        "clob": Fore.GREEN,
        "mkt":  Fore.MAGENTA,
        "live": Fore.WHITE + Style.BRIGHT,
        "sys":  Fore.BLUE,
        "warn": Fore.RED,
        "fire": Fore.GREEN + Style.BRIGHT,
    }
except ImportError:
    COLORS = {}

# Eastern Time (UTC-4 EDT / UTC-5 EST). Python handles DST via fixed offset
# for simplicity — Polymarket is a US-Eastern platform.
ET = timezone(timedelta(hours=-4))  # EDT (Apr-Nov). Change to -5 for EST (Nov-Mar).

def log(msg, tag="sys"):
    ts = datetime.now(ET).strftime("%H:%M:%S.%f")[:-3]
    color = COLORS.get(tag, "")
    reset = Style.RESET_ALL if color else ""
    print(f"{color}[{ts}]   {msg}{reset}", flush=True)


# ===========================================═══════════════════
# CSV TRADE LOG — one line per event (ENTRY/STOP/FLIP/SETTLED)
# ===========================================═══════════════════
# Appends to trades.csv in the same directory as the script.
# File size: ~200 bytes per trade → 10,000 trades ≈ 2 MB. Negligible.

_CSV_PATH = Path(__file__).parent / "trades.csv"
_CSV_LOCK = threading.Lock()
_CSV_FIELDS = [
    "timestamp", "event", "asset", "timeframe", "side",
    "entry_price", "exit_price", "size", "shares", "pnl",
    "result", "model_prob", "edge", "delta_pct", "balance",
]

def _csv_init():
    """Write header row if file doesn't exist yet."""
    if not _CSV_PATH.exists():
        with open(_CSV_PATH, "w", newline="") as f:
            csv.writer(f).writerow(_CSV_FIELDS)

def csv_log(event, pos, extra=None):
    """Append one trade row. Called from ENTRY/STOP/FLIP/SETTLED handlers."""
    extra = extra or {}
    with _CSV_LOCK:
        _csv_init()
        row = {
            "timestamp": datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S"),
            "event":       event,
            "asset":       pos.get("asset", ""),
            "timeframe":   pos.get("timeframe", ""),
            "side":        pos.get("side", ""),
            "entry_price": f"{pos.get('entry_price', 0):.4f}",
            "exit_price":  f"{extra.get('exit_price', 0):.4f}",
            "size":        f"{pos.get('size', 0):.4f}",
            "shares":      f"{pos.get('shares', 0):.4f}",
            "pnl":         f"{pos.get('pnl', 0):.4f}",
            "result":      pos.get("result", extra.get("result", "")),
            "model_prob":  f"{extra.get('model_prob', 0):.4f}",
            "edge":        f"{extra.get('edge', 0):.4f}",
            "delta_pct":   f"{extra.get('delta_pct', 0):.6f}",
            "balance":     f"{extra.get('balance', 0):.4f}",
        }
        with open(_CSV_PATH, "a", newline="") as f:
            csv.DictWriter(f, _CSV_FIELDS).writerow(row)


# ===========================================═══════════════════
# PART 1-A: BINANCE WEBSOCKET — real-time asset prices
# ===========================================═══════════════════
# Connects to Binance combined stream for BTC/ETH/SOL mini-tickers.
# Why Binance: fastest public crypto price feed (~100ms updates).
# Chainlink lags Binance by 200-500ms during volatility — Binance
# gives us the earliest read on direction so we can fire before
# slower bots see the move on Chainlink.
#
# Data stored: {symbol: {"price": float, "ts": float}}
# Thread-safe via lock. One persistent WS connection, auto-reconnect.

class BinanceFeed:
    def __init__(self):
        self.prices = {}   # "BTCUSDT" → {"price": float, "ts": float}
        self._lock = threading.Lock()
        # VPIN: per-symbol 1-second buckets [epoch_sec, buy_usd, sell_usd]
        self._buckets = {}  # "BTCUSDT" → deque of [sec, buy_usd, sell_usd]
        # Price history for trend filter: per-symbol deque of (epoch_sec, price).
        # Holds ~20min of 1s samples so get_price_at() can look back 15min.
        self._history = {}
        for a in C["assets"]:
            self._history[a["binance_sym"].upper()] = deque(maxlen=1300)

    def get(self, symbol):
        """Returns (price, age_seconds) or (None, None)."""
        with self._lock:
            d = self.prices.get(symbol.upper())
            if not d:
                return None, None
            return d["price"], time.time() - d["ts"]

    def get_price_at(self, symbol, epoch_sec):
        """Look up the Binance price near a specific epoch second.
        Returns the price closest to epoch_sec within 5s, else None.
        Used by the 5m/15m trend filter in the entry loop."""
        with self._lock:
            hist = self._history.get(symbol.upper())
            if not hist:
                return None
            best_price, best_diff = None, 999
            for ts, px in hist:
                diff = abs(ts - epoch_sec)
                if diff < best_diff:
                    best_diff = diff
                    best_price = px
        return best_price if best_diff <= 5 else None

    def get_vpin(self, symbol, window_s=None):
        """Compute VPIN over the last window_s seconds of Binance aggTrades.

        Returns (vpin, direction) or (None, None).
          vpin: 0.0-1.0 — |buy - sell| / total.  Higher = more one-sided flow.
          direction: "UP" if net buying, "DN" if net selling.
        """
        if window_s is None:
            window_s = C["vpin_window_s"]
        with self._lock:
            bkts = self._buckets.get(symbol.upper())
            if not bkts:
                return None, None
            cutoff = int(time.time()) - window_s
            buy = sell = 0.0
            for b in bkts:
                if b[0] >= cutoff:
                    buy += b[1]
                    sell += b[2]
        total = buy + sell
        if total < 500:  # less than $500 volume — too thin
            return None, None
        vpin = abs(buy - sell) / total
        direction = "UP" if buy > sell else "DN"
        return vpin, direction

    def start(self):
        threading.Thread(target=self._thread, daemon=True).start()

    def _thread(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._run())

    async def _run(self):
        # Combined stream: miniTicker (price) + aggTrade (VPIN) per asset
        streams = []
        for a in C["assets"]:
            sym = a["binance_sym"]
            streams.append(f"{sym}@miniTicker")
            streams.append(f"{sym}@aggTrade")
        url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

        while True:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    log("[BNCE] Binance WebSocket connected", "bnce")
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        try:
                            msg = json.loads(raw)
                            data = msg.get("data", {})
                            event = data.get("e", "")

                            if event == "24hrMiniTicker":
                                sym = data.get("s", "").upper()
                                close = data.get("c")
                                if sym and close:
                                    px = float(close)
                                    now_s = int(time.time())
                                    with self._lock:
                                        self.prices[sym] = {
                                            "price": px,
                                            "ts": time.time(),
                                        }
                                        # Record into history for trend lookback.
                                        # Only append one sample per second to keep deque lean.
                                        hist = self._history.get(sym)
                                        if hist is not None and (not hist or hist[-1][0] != now_s):
                                            hist.append((now_s, px))

                            elif event == "aggTrade":
                                self._handle_trade(data)

                        except (json.JSONDecodeError, ValueError, TypeError):
                            pass
            except asyncio.TimeoutError:
                log("[BNCE] timeout — reconnect 3s", "warn")
                await asyncio.sleep(3)
            except Exception as e:
                log(f"[BNCE] error: {type(e).__name__}: {e} — reconnect 5s", "warn")
                await asyncio.sleep(5)

    def _handle_trade(self, data):
        """Aggregate an aggTrade into 1-second VPIN buckets.

        aggTrade fields:
          s: symbol, p: price, q: quantity,
          m: true if buyer is maker (= seller is aggressor = SELL trade)
        """
        try:
            sym = data.get("s", "").upper()
            price = float(data.get("p", 0))
            qty = float(data.get("q", 0))
            is_sell = data.get("m", False)  # m=true → buyer is maker → sell aggressor
        except (ValueError, TypeError):
            return
        if not sym or price <= 0 or qty <= 0:
            return

        vol_usd = price * qty
        now_s = int(time.time())

        with self._lock:
            if sym not in self._buckets:
                self._buckets[sym] = deque(maxlen=120)  # 2 min of 1s buckets
            bkts = self._buckets[sym]

            # Append to current second or start new bucket
            if bkts and bkts[-1][0] == now_s:
                b = bkts[-1]
                if is_sell:
                    b[2] += vol_usd
                else:
                    b[1] += vol_usd
            else:
                if is_sell:
                    bkts.append([now_s, 0.0, vol_usd])
                else:
                    bkts.append([now_s, vol_usd, 0.0])

            # Prune old buckets (keep 2 min)
            cutoff = now_s - 120
            while bkts and bkts[0][0] < cutoff:
                bkts.popleft()


# ===========================================═══════════════════
# PART 1-B: CHAINLINK RTDS WEBSOCKET — oracle prices
# ===========================================═══════════════════
# Polymarket settles crypto Up/Down markets using Chainlink price
# feeds on Polygon (BTC/USD, ETH/USD, SOL/USD). The RTDS WebSocket
# at wss://ws-live-data.polymarket.com streams these oracle prices
# in real-time. This is the GROUND TRUTH for settlement.
#
# Why both Binance AND Chainlink:
#   - Binance is faster (leads by 200-500ms) → early directional signal
#   - Chainlink is what Polymarket actually settles on → confirm direction
#   - When both agree, confidence in direction is very high
#
# Data stored: {symbol: {"price": float, "ts": float}}
# symbol mapping: "btc/usd" → "BTCUSDT" (our canonical key)

class ChainlinkFeed:
    def __init__(self):
        self.prices = {}   # "BTCUSDT" → {"price": 71234.56, "ts": 1713000000.0}
        self._lock = threading.Lock()
        # Map RTDS symbols to our canonical keys
        self._sym_map = {}
        for a in C["assets"]:
            self._sym_map[a["rtds_sym"]] = a["binance_sym"].upper()
        # History buffer: canonical_sym → deque of (server_epoch_sec, price)
        # Keeps last 90s of Chainlink server-side timestamps so we can look up
        # the exact price at any recent epoch second (used for PTB).
        self._history = {}
        for a in C["assets"]:
            self._history[a["binance_sym"].upper()] = deque(maxlen=120)

    def get(self, symbol):
        """Returns (price, age_seconds) or (None, None)."""
        with self._lock:
            d = self.prices.get(symbol.upper())
            if not d:
                return None, None
            return d["price"], time.time() - d["ts"]

    def get_price_at(self, symbol, epoch_sec):
        """Look up the Chainlink price at a specific epoch second.
        Uses the server-side timestamp from RTDS messages, NOT our local clock.
        Returns the price closest to epoch_sec, or None if nothing within 2s."""
        with self._lock:
            hist = self._history.get(symbol.upper())
            if not hist:
                return None
            best_price, best_diff = None, 999
            for ts, px in hist:
                diff = abs(ts - epoch_sec)
                if diff < best_diff:
                    best_diff = diff
                    best_price = px
        return best_price if best_diff <= 2 else None

    def start(self):
        threading.Thread(target=self._thread, daemon=True).start()

    def _thread(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._run())

    async def _run(self):
        sub_msg = json.dumps({
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}
            ]
        })

        while True:
            try:
                async with websockets.connect(C["rtds_ws_url"], ping_interval=5) as ws:
                    await ws.send(sub_msg)
                    log("[CHNL] Chainlink RTDS WebSocket connected", "chnl")
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=15)
                        if not raw or not isinstance(raw, str):
                            continue
                        try:
                            msg = json.loads(raw)
                            payload = msg.get("payload", {})
                            sym_raw = payload.get("symbol", "")  # "btc/usd"
                            val = payload.get("value")
                            # Server-side timestamp in ms from RTDS
                            srv_ts_ms = payload.get("timestamp")
                            canonical = self._sym_map.get(sym_raw)
                            if canonical and val:
                                price = float(val)
                                with self._lock:
                                    self.prices[canonical] = {
                                        "price": price,
                                        "ts": time.time(),
                                    }
                                    # Buffer with server-side epoch seconds for PTB lookup
                                    if srv_ts_ms:
                                        srv_sec = int(srv_ts_ms) // 1000
                                        self._history[canonical].append((srv_sec, price))
                        except (json.JSONDecodeError, ValueError, TypeError):
                            pass
            except asyncio.TimeoutError:
                log("[CHNL] timeout — reconnect 3s", "warn")
                await asyncio.sleep(3)
            except Exception as e:
                log(f"[CHNL] error: {type(e).__name__}: {e} — reconnect 5s", "warn")
                await asyncio.sleep(5)


# ===========================================═══════════════════
# PART 1-C: POLYMARKET CLOB BOOK WEBSOCKET — real-time orderbook
# ===========================================═══════════════════
# Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market.
# Subscribes to YES+NO token IDs of all active 5m/15m markets.
#
# Protocol:
#   Subscribe: {"type":"market","assets_ids":["<token_id>",...]}}
#   Initial:   [{event_type:"book", asset_id, bids:[{price,size}], asks:[{price,size}]}, ...]
#   Updates:   {market, price_changes:[{asset_id, price, size, side:"BUY"|"SELL"}]}
#
# Why this matters: REST /book snapshots show 1c/99c dust, but real
# fills happen at 30-70c. The asks live for milliseconds — only the
# WS feed can see them. This is the difference between "book is dead"
# and "we can actually take liquidity."
#
# Data stored: {token_id: {"bids":[(px,sz)...], "asks":[(px,sz)...], "ts":float}}
# Sorted: bids descending, asks ascending. Best bid = bids[0], best ask = asks[0].

class CLOBBookFeed:
    def __init__(self):
        self.books = {}        # token_id → {bids, asks, ts}
        self._lock = threading.Lock()
        self._desired = set()  # token IDs we want subscribed
        self._last_sub = set()
        self._connected = False
        self._ws = None           # live WS reference so main thread can force-close it
        self._loop = None         # reference to the async event loop
        self._reconnect_event = None  # asyncio.Event — signals "close + reconnect now"

    def set_active_tokens(self, token_ids):
        """Called each scan loop. Pass YES+NO token IDs of all active markets.
        If token set changed, force-closes the WS so the async loop reconnects
        with a fresh subscription and gets new book snapshots."""
        with self._lock:
            new_desired = {str(t) for t in token_ids if t}
            if new_desired == self._desired:
                return  # no change
            old = self._desired
            self._desired = new_desired
        added = new_desired - old
        removed = old - new_desired
        if not added and not removed:
            return
        log(f"[CLOB] token set changed: +{len(added)} -{len(removed)} — forcing reconnect", "clob")
        # Signal the async loop to break out and reconnect.
        # The reconnect will subscribe to _desired and get fresh book snapshots.
        if self._loop and self._reconnect_event:
            try:
                self._loop.call_soon_threadsafe(self._reconnect_event.set)
            except RuntimeError:
                pass

    def get_best(self, token_id):
        """Returns (best_bid_px, best_ask_px, age_secs) or (None, None, None)."""
        with self._lock:
            b = self.books.get(str(token_id))
            if not b:
                return None, None, None
            age = time.time() - b["ts"]
            bid = b["bids"][0][0] if b["bids"] else None
            ask = b["asks"][0][0] if b["asks"] else None
            return bid, ask, age

    def get_book(self, token_id):
        """Returns full {bids, asks, ts} or None."""
        with self._lock:
            b = self.books.get(str(token_id))
            if not b:
                return None
            return {"bids": list(b["bids"]), "asks": list(b["asks"]), "ts": b["ts"]}

    def is_connected(self):
        return self._connected

    def start(self):
        threading.Thread(target=self._thread, daemon=True).start()

    def _thread(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._run())

    async def _run(self):
        self._loop = asyncio.get_event_loop()
        self._reconnect_event = asyncio.Event()

        while True:
            try:
                # Clear stale books and reconnect signal before each connection
                self._reconnect_event.clear()
                with self._lock:
                    self.books.clear()
                    self._last_sub = set()

                async with websockets.connect(
                    C["clob_ws_url"], ping_interval=10, ping_timeout=20
                ) as ws:
                    self._connected = True
                    self._ws = ws
                    log("[CLOB] CLOB book WebSocket connected", "clob")

                    # Subscribe to whatever tokens are desired right now
                    await self._subscribe_desired(ws)

                    while True:
                        # Wait for EITHER a WS message OR a reconnect signal
                        recv_task = asyncio.ensure_future(ws.recv())
                        reconn_task = asyncio.ensure_future(self._reconnect_event.wait())

                        done, pending = await asyncio.wait(
                            {recv_task, reconn_task},
                            timeout=5.0,
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                        for t in pending:
                            t.cancel()

                        # Reconnect signal from main thread (window rotation)
                        # Break inner loop → closes WS → outer loop reconnects fresh
                        if reconn_task in done:
                            log("[CLOB] reconnect signal received — closing WS for fresh connect", "clob")
                            break

                        # Handle incoming WS message
                        if recv_task in done:
                            try:
                                raw = recv_task.result()
                            except Exception:
                                raw = None
                            if raw:
                                try:
                                    msg = json.loads(raw) if isinstance(raw, str) else None
                                except (json.JSONDecodeError, ValueError):
                                    msg = None
                                if msg:
                                    self._handle(msg)

                    # Inner loop broke (reconnect signal) — connection closes via
                    # async with, then outer loop immediately reconnects
                    self._connected = False
                    continue

            except asyncio.TimeoutError:
                self._connected = False
                log("[CLOB] timeout — reconnect 3s", "warn")
                await asyncio.sleep(3)
            except Exception as e:
                self._connected = False
                log(f"[CLOB] error: {type(e).__name__}: {e} — reconnect 5s", "warn")
                await asyncio.sleep(5)

    async def _subscribe_desired(self, ws):
        """Send subscription for all desired tokens. Called once per fresh connection."""
        with self._lock:
            desired = set(self._desired)
        if not desired:
            return
        sub_msg = json.dumps({"type": "market", "assets_ids": list(desired)})
        try:
            await ws.send(sub_msg)
            self._last_sub = desired
            short_ids = [f"...{t[-12:]}" for t in sorted(desired)]
            log(f"[CLOB] subscribed: {len(desired)} tokens  ids: {short_ids}", "clob")
        except Exception as e:
            log(f"[CLOB] subscribe failed: {e}", "warn")

    def _handle(self, msg):
        if isinstance(msg, list):
            for item in msg:
                if isinstance(item, dict) and item.get("event_type") == "book":
                    self._apply_book(item)
        elif isinstance(msg, dict):
            if msg.get("event_type") == "book":
                self._apply_book(msg)
            elif "price_changes" in msg:
                for pc in msg.get("price_changes", []):
                    self._apply_delta(pc)

    def book_count(self):
        """How many tokens have book data loaded."""
        with self._lock:
            return len(self.books)

    def _apply_book(self, item):
        """Full book snapshot — replaces entire ladder for this token."""
        aid = str(item.get("asset_id") or "")
        if not aid:
            return
        # Ignore data for tokens we're no longer subscribed to (post-rotation ghosts)
        with self._lock:
            if aid not in self._desired:
                return
        try:
            bids = [(float(b["price"]), float(b["size"]))
                    for b in item.get("bids", []) if float(b.get("size", 0)) > 0]
            asks = [(float(a["price"]), float(a["size"]))
                    for a in item.get("asks", []) if float(a.get("size", 0)) > 0]
        except (ValueError, TypeError, KeyError):
            return
        bids.sort(key=lambda x: -x[0])
        asks.sort(key=lambda x: x[0])
        with self._lock:
            is_new = aid not in self.books
            self.books[aid] = {"bids": bids, "asks": asks, "ts": time.time()}
        if is_new:
            best_bid = f"{bids[0][0]*100:.0f}c" if bids else "--"
            best_ask = f"{asks[0][0]*100:.0f}c" if asks else "--"
            log(f"[CLOB] book loaded: ...{aid[-12:]}  bid:{best_bid} ask:{best_ask}  "
                f"({len(bids)} bids, {len(asks)} asks)", "clob")

    def _apply_delta(self, pc):
        """Single price level change — add/update/remove one level."""
        aid = str(pc.get("asset_id") or "")
        if not aid:
            return
        # Ignore deltas for tokens we're no longer tracking (stale messages
        # that arrive after rotation before the server processes our unsubscribe)
        with self._lock:
            if aid not in self._desired:
                return
        try:
            price = float(pc.get("price"))
            size = float(pc.get("size"))
            side = str(pc.get("side", "")).upper()
        except (ValueError, TypeError):
            return
        with self._lock:
            book = self.books.get(aid)
            if book is None:
                book = {"bids": [], "asks": [], "ts": time.time()}
                self.books[aid] = book
            if side == "BUY":
                book["bids"] = [(p, s) for p, s in book["bids"] if abs(p - price) > 1e-9]
                if size > 0:
                    book["bids"].append((price, size))
                book["bids"].sort(key=lambda x: -x[0])
            elif side == "SELL":
                book["asks"] = [(p, s) for p, s in book["asks"] if abs(p - price) > 1e-9]
                if size > 0:
                    book["asks"].append((price, size))
                book["asks"].sort(key=lambda x: x[0])
            book["ts"] = time.time()


# ===========================================═══════════════════
# PART 1-C2: POLYMARKET USER FEED WEBSOCKET — real-time fill events
# ===========================================═══════════════════
# Connects to wss://ws-subscriptions-clob.polymarket.com/ws/user with
# API credentials. Receives live TRADE events for every fill our keys
# touch — the authoritative source of fill data.
#
# Why this matters: REST CLOB may return status="cancelled" on a FAK
# while partial fills were still propagating. The cancellation message
# arrives before the fills settle. Polling get_order once at T+2s can
# miss these late fills entirely, leaving orphan shares on-chain that
# the bot's position tracker doesn't know about.
#
# Design: maintain a rolling buffer of recent trades indexed by the
# taker_order_id. When live_buy/live_sell posts an order and gets its
# ID back, we wait a fixed window (5s) on the buffer, collecting all
# matching fills regardless of what REST says. The full window is
# always observed — never trust an early cancel message.

class UserFeedWS:
    def __init__(self, api_key, api_secret, api_passphrase):
        self._creds = {
            "apiKey": api_key,
            "secret": api_secret,
            "passphrase": api_passphrase,
        }
        # Rolling buffer of trade events. Each: {"order_id","price","size","ts"}
        self._trades = []
        self._lock = threading.Lock()
        self._connected = False
        self._loop = None
        self._ws = None

    def is_connected(self):
        return self._connected

    def start(self):
        threading.Thread(target=self._thread, daemon=True).start()

    def _thread(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        loop.run_until_complete(self._run())

    async def _run(self):
        url = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
        while True:
            try:
                async with websockets.connect(url, ping_interval=10, ping_timeout=20) as ws:
                    self._ws = ws
                    # Subscribe to user channel — empty markets = all our trades
                    await ws.send(json.dumps({
                        "type": "user",
                        "auth": self._creds,
                        "markets": [],
                    }))
                    self._connected = True
                    log("[USER] User feed WebSocket connected", "clob")
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60)
                        self._handle(raw)
            except asyncio.TimeoutError:
                # 60s silence — ping should keep it alive, but reconnect just in case
                self._connected = False
                log("[USER] User feed idle timeout — reconnecting", "warn")
                await asyncio.sleep(1)
            except Exception as e:
                self._connected = False
                log(f"[USER] User feed error: {e} — reconnecting in 3s", "warn")
                await asyncio.sleep(3)

    def _handle(self, raw):
        try:
            data = json.loads(raw)
        except Exception:
            return
        events = data if isinstance(data, list) else [data]
        for ev in events:
            etype = (ev.get("event_type") or ev.get("type") or "").lower()
            if etype == "trade" or ("price" in ev and "size" in ev and "taker_order_id" in ev):
                self._record(ev)

    def _record(self, ev):
        try:
            price = float(ev.get("price", 0) or 0)
            size = float(ev.get("size", 0) or 0)
            # Polymarket trade events expose our FAK order as taker_order_id
            oid = (ev.get("taker_order_id") or ev.get("order_id")
                   or ev.get("orderID") or "")
            if not oid or price <= 0 or size <= 0:
                return
            with self._lock:
                self._trades.append({
                    "order_id": str(oid),
                    "price": price,
                    "size": size,
                    "ts": time.time(),
                })
                # Prune anything older than 60s
                cutoff = time.time() - 60
                self._trades = [t for t in self._trades if t["ts"] >= cutoff]
        except Exception:
            pass

    def wait_for_fills(self, order_id, wait_s=5.0, lookback_s=3.0):
        """Block `wait_s` seconds, then return all trades matching order_id
        seen within the last (wait_s + lookback_s) seconds. Always waits the
        full window — never trusts an early cancel message from REST."""
        oid = str(order_id)
        start = time.time()
        lookback_cutoff = start - lookback_s
        time.sleep(wait_s)
        with self._lock:
            return [t for t in self._trades
                    if t["order_id"] == oid and t["ts"] >= lookback_cutoff]


# ===========================================═══════════════════
# PART 1-D: MARKET DISCOVERY — find active 5m/15m token IDs
# ===========================================═══════════════════
# Queries Gamma API for the current window of each (asset, timeframe).
# Returns a list of market dicts with:
#   asset, timeframe, secs_left, up_token, dn_token,
#   price_to_beat, window_end
#
# Refreshes on each scan tick. On window rotation (new token IDs),
# the CLOB WS will be re-subscribed automatically via set_active_tokens.

class MarketDiscovery:
    def __init__(self):
        self.markets = []  # list of market dicts
        self._prev = {}    # key → last good market dict (survives failed fetches)
        self._lock = threading.Lock()

    def refresh(self):
        """Fetch current markets from Gamma. Called each scan tick.
        If a fetch fails for one asset, keep the previous result so it
        doesn't disappear from the display."""
        now = int(time.time())
        results = {}
        for asset in C["assets"]:
            for tf in C["timeframes"]:
                key = f"{asset['name']}_{tf['label']}"
                try:
                    mkt = self._fetch_one(asset, tf, now)
                    if mkt:
                        results[key] = mkt
                    elif key in self._prev:
                        # Gamma returned empty — keep previous until window expires
                        results[key] = self._prev[key]
                except Exception as e:
                    log(f"[MKT] {asset['name']} {tf['label']} error: {e}", "warn")
                    if key in self._prev:
                        results[key] = self._prev[key]
        self._prev = results
        with self._lock:
            self.markets = list(results.values())

    def get_all(self):
        with self._lock:
            return list(self.markets)

    def get_all_token_ids(self):
        """Returns flat list of all YES token IDs (UP + DN) for CLOB WS subscription."""
        with self._lock:
            ids = []
            for m in self.markets:
                if m.get("up_token"):
                    ids.append(m["up_token"])
                if m.get("dn_token"):
                    ids.append(m["dn_token"])
            return ids

    def _fetch_one(self, asset, tf, now):
        """Fetch a single (asset, timeframe) market from Gamma."""
        secs = tf["secs"]
        window_start = now - (now % secs)
        window_end = window_start + secs
        secs_left = window_end - now

        slug = f"{asset['slug_pfx']}-updown-{tf['label']}-{window_start}"
        url = f"{C['gamma_url']}/events?slug={slug}"
        r = requests.get(url, timeout=5)
        if r.status_code != 200 or not r.json():
            return None

        ev = r.json()[0]
        raw_markets = ev.get("markets", [])
        if not raw_markets:
            return None

        # Parse PTB (Price To Beat).
        # Gamma only populates eventMetadata.priceToBeat AFTER the window closes.
        # During the live window, we get PTB from the PREVIOUS window's finalPrice.
        # prev_finalPrice == current_PTB because both are the Chainlink price at
        # the same boundary second (the end of prev = start of current).
        em = ev.get("eventMetadata") or {}
        ptb = None
        if em.get("priceToBeat"):
            try:
                ptb = float(em["priceToBeat"])
            except (ValueError, TypeError):
                pass

        if ptb is None:
            # Fetch previous window's finalPrice as current PTB
            ts_prev = window_start - secs
            prev_slug = f"{asset['slug_pfx']}-updown-{tf['label']}-{ts_prev}"
            try:
                rp = requests.get(f"{C['gamma_url']}/events?slug={prev_slug}", timeout=5)
                if rp.ok:
                    prev_data = rp.json()
                    if isinstance(prev_data, list) and prev_data:
                        prev_meta = prev_data[0].get("eventMetadata") or {}
                        fp = prev_meta.get("finalPrice")
                        if fp is not None:
                            ptb = float(fp)
            except Exception:
                pass

        # Find UP and DOWN token IDs.
        # Gamma returns either:
        #   (a) Two markets with groupItemTitle "Up" / "Down", each with 1 clobTokenId
        #   (b) One market with outcomes=["Up","Down"] and 2 clobTokenIds (tid[0]=Up, tid[1]=Down)
        up_token, dn_token = None, None
        if len(raw_markets) >= 2:
            # Format (a): separate Up/Down markets
            for m in raw_markets:
                title = (m.get("groupItemTitle") or "").lower()
                tids = m.get("clobTokenIds")
                if isinstance(tids, str):
                    tids = json.loads(tids)
                if not tids:
                    continue
                if "up" in title:
                    up_token = tids[0]
                elif "down" in title:
                    dn_token = tids[0]
        else:
            # Format (b): single market, outcomes=["Up","Down"], 2 token IDs
            m = raw_markets[0]
            tids = m.get("clobTokenIds")
            if isinstance(tids, str):
                tids = json.loads(tids)
            outcomes = m.get("outcomes")
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if tids and len(tids) >= 2:
                # Map by outcomes order: ["Up","Down"] → tid[0]=Up, tid[1]=Down
                if outcomes and len(outcomes) >= 2:
                    for i, out in enumerate(outcomes):
                        if out.lower() == "up":
                            up_token = tids[i]
                        elif out.lower() == "down":
                            dn_token = tids[i]
                else:
                    # No outcomes label — assume index 0=Up, 1=Down (Gamma convention)
                    up_token, dn_token = tids[0], tids[1]

        if not up_token and not dn_token:
            return None

        return {
            "asset":       asset["name"],
            "sym":         asset["binance_sym"].upper(),
            "slug_pfx":    asset["slug_pfx"],
            "timeframe":   tf["label"],
            "tf_secs":     secs,
            "window_start": window_start,
            "window_end":  window_end,
            "secs_left":   secs_left,
            "up_token":    up_token,
            "dn_token":    dn_token,
            "ptb":         ptb,
        }


# ===========================================═══════════════════
# PART 1-E: MAIN LOOP — stream live, show best ask
# ===========================================═══════════════════
# Ties all three feeds together:
#   1. Refresh market discovery (find current token IDs)
#   2. Feed token IDs to CLOB WS
#   3. Display per-market: asset price (Binance+Chainlink), PTB,
#      direction delta, best bid/ask from CLOB WS, time left
#
# This is the skeleton that v10 Layer 2 (book trigger) will hook into.

def dynamic_fee(price):
    """Polymarket fee curve: peaks at 50c, drops at extremes.
    fee = rate * 2 * p * (1 - p)"""
    p = max(0.01, min(0.99, price))
    return C["fee_rate"] * 2.0 * p * (1.0 - p)


def model_probability(abs_delta, secs_in, tf_secs, vol_override=None):
    """Estimate probability that price stays on the same side of PTB at settlement.

    Uses a random-walk model: the remaining price movement follows a normal
    distribution with std = vol_5m * sqrt(fraction_remaining).  The z-score
    of the current delta against that remaining vol gives us a CDF probability.

    Returns a float 0.0-1.0 (e.g. 0.73 = 73% chance the current side wins).
    vol_override: use a different volatility (e.g. vol_5m_stop for stop-loss).
    """
    time_frac = min(secs_in / tf_secs, 0.999)  # clamp to avoid div-by-zero
    vol = vol_override if vol_override is not None else C["vol_5m"]
    remaining_vol = vol * math.sqrt(1.0 - time_frac)
    if remaining_vol < 1e-9:
        return 0.99 if abs_delta > 0 else 0.50
    z = abs_delta / remaining_vol
    # Normal CDF via math.erf
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def book_imbalance(clob, token_id):
    """Compute bid/ask depth imbalance near top of book for a token.

    Returns bid_size / (bid_size + ask_size) within book_depth_cents of
    the best bid/ask.  > 0.5 means buyers dominate, < 0.5 means sellers.
    Returns None if book data unavailable.
    """
    book = clob.get_book(token_id)
    if not book:
        return None
    depth = C["book_depth_cents"]
    bids, asks = book["bids"], book["asks"]

    bid_vol = 0.0
    if bids:
        top_bid = bids[0][0]
        for px, sz in bids:
            if top_bid - px > depth:
                break
            bid_vol += sz

    ask_vol = 0.0
    if asks:
        top_ask = asks[0][0]
        for px, sz in asks:
            if px - top_ask > depth:
                break
            ask_vol += sz

    total = bid_vol + ask_vol
    if total < 1.0:  # less than $1 of liquidity — unreliable
        return None
    return bid_vol / total


# ===========================================═══════════════════
# LIVE TRADING — order execution via py-clob-client
# ===========================================═══════════════════
# Phase 1: CLOB API for fills. Place FAK limit at best_ask + tolerance.
# Wait 2s, confirm fill via get_order. No on-chain check yet.

live_client = None  # initialized in main() when dry_run=False
userfeed = None     # UserFeedWS instance for authoritative fill events

def init_live_client():
    """Authenticate with Polymarket CLOB for live order placement."""
    global live_client, userfeed
    if not CLOB_SDK_OK:
        log("[SYS] py-clob-client not installed — cannot go live", "warn")
        return False
    if not C["pk"]:
        log("[SYS] POLYMARKET_PK not set — cannot go live", "warn")
        return False
    try:
        cl = ClobClient(C["clob_url"], key=C["pk"],
                        chain_id=C["chain_id"],
                        signature_type=1, funder=C["funder"])
        creds = cl.create_or_derive_api_creds()
        cl.set_api_creds(creds)
        live_client = cl
        # Start user feed WS for authoritative fill tracking
        try:
            api_key = getattr(creds, "api_key", None) or getattr(creds, "apiKey", None)
            api_secret = getattr(creds, "api_secret", None) or getattr(creds, "secret", None)
            api_pass = getattr(creds, "api_passphrase", None) or getattr(creds, "passphrase", None)
            if api_key and api_secret and api_pass:
                userfeed = UserFeedWS(api_key, api_secret, api_pass)
                userfeed.start()
                log("[SYS] User feed starting for fill monitoring", "sys")
            else:
                log("[SYS] Could not extract API creds — fill monitoring will use REST fallback", "warn")
        except Exception as e:
            log(f"[SYS] User feed init failed: {e} — REST fallback active", "warn")
        log("[SYS] CLOB authenticated — LIVE TRADING ENABLED", "warn")
        return True
    except Exception as e:
        log(f"[SYS] CLOB auth failed: {e}", "warn")
        return False


def live_buy(token_id, size_usd, best_ask):
    """Place a BUY FAK limit order at best_ask + tolerance.
    Returns (filled: bool, fill_price: float, shares: float)."""
    tolerance = C["limit_tolerance"]
    limit_price = round(min(best_ask + tolerance, C["max_entry_price"]), 2)

    # Floor shares to 0.01 tick
    shares = math.floor((size_usd / limit_price) / 0.01) * 0.01
    shares = round(shares, 2)
    if shares < 0.01 or shares * limit_price < C["min_order_usd"]:
        log(f"[LIVE] Below min order: {shares:.2f}sh @ {limit_price*100:.0f}c", "warn")
        return False, 0, 0

    try:
        args = OrderArgs(
            token_id=token_id,
            price=limit_price,
            size=shares,
            side=BUY,
            fee_rate_bps=156,
        )
        signed = live_client.create_order(args)
        resp = live_client.post_order(signed, OrderType.FAK)

        order_id = resp.get("orderID", "?")
        status = str(resp.get("status", "unknown")).lower()

        # Primary path: userfeed WS gives authoritative fill data.
        # Always wait the full window — do NOT trust an early cancelled status.
        if userfeed and userfeed.is_connected() and order_id != "?":
            fills = userfeed.wait_for_fills(order_id, wait_s=5.0, lookback_s=3.0)
            if fills:
                total_size = sum(f["size"] for f in fills)
                total_cost = sum(f["size"] * f["price"] for f in fills)
                fill_price = total_cost / total_size if total_size > 0 else limit_price
                log(f"[LIVE] FILL CONFIRMED (WS) | {total_size:.2f}sh @ {fill_price*100:.1f}c "
                    f"(limit:{limit_price*100:.0f}c) | orderID:{str(order_id)[:14]}", "fire")
                return True, fill_price, total_size
            # No fills seen on userfeed after full window
            log(f"[LIVE] NO FILL (WS) | ask:{best_ask*100:.0f}c limit:{limit_price*100:.0f}c "
                f"| orderID:{str(order_id)[:14]} status:{status}", "warn")
            return False, 0, 0

        # Fallback: REST poll (userfeed unavailable)
        if status in ("cancelled", "canceled", "unmatched"):
            log(f"[LIVE] NO FILL | ask:{best_ask*100:.0f}c limit:{limit_price*100:.0f}c "
                f"| orderID:{str(order_id)[:14]} status:{status}", "warn")
            return False, 0, 0

        fill_price = limit_price
        time.sleep(2)
        try:
            order_info = live_client.get_order(order_id)
            actual_size = float(order_info.get("size_matched", 0) or 0)
            avg_price = float(order_info.get("associate_trades", [{}])[0].get("price", 0) or 0)
            if actual_size > 0:
                shares = actual_size
            if avg_price > 0:
                fill_price = avg_price
            log(f"[LIVE] FILL CONFIRMED (REST) | {shares:.2f}sh @ {fill_price*100:.1f}c "
                f"(limit:{limit_price*100:.0f}c) | orderID:{str(order_id)[:14]}", "fire")
        except Exception as e:
            log(f"[LIVE] Fill check failed ({e}), using limit price {limit_price*100:.0f}c", "warn")

        return True, fill_price, shares

    except Exception as e:
        log(f"[LIVE] BUY ORDER FAILED: {e}", "warn")
        return False, 0, 0


def live_sell(token_id, shares, best_bid=None):
    """Place a SELL FAK order to exit a position (stop-loss).
    Sells at best_bid - tolerance to ensure fill. Returns (filled, sell_price)."""
    if best_bid is None or best_bid <= 0:
        best_bid = 0.50  # fallback — just dump it
    tolerance = C["limit_tolerance"]
    limit_price = round(max(best_bid - tolerance, 0.01), 2)

    shares = round(shares, 2)
    if shares < 0.01:
        return False, 0

    try:
        args = OrderArgs(
            token_id=token_id,
            price=limit_price,
            size=shares,
            side=SELL,
            fee_rate_bps=156,
        )
        signed = live_client.create_order(args)
        resp = live_client.post_order(signed, OrderType.FAK)

        order_id = resp.get("orderID", "?")
        status = str(resp.get("status", "unknown")).lower()

        # Primary path: userfeed WS — authoritative, ignores early cancel msgs
        if userfeed and userfeed.is_connected() and order_id != "?":
            fills = userfeed.wait_for_fills(order_id, wait_s=5.0, lookback_s=3.0)
            if fills:
                total_size = sum(f["size"] for f in fills)
                total_cost = sum(f["size"] * f["price"] for f in fills)
                fill_price = total_cost / total_size if total_size > 0 else limit_price
                log(f"[LIVE] SELL CONFIRMED (WS) | {total_size:.2f}sh @ {fill_price*100:.1f}c "
                    f"(limit:{limit_price*100:.0f}c) | orderID:{str(order_id)[:14]}", "fire")
                return True, fill_price
            log(f"[LIVE] SELL NO FILL (WS) | limit:{limit_price*100:.0f}c "
                f"| orderID:{str(order_id)[:14]} status:{status}", "warn")
            return False, 0

        # Fallback: REST poll
        if status in ("cancelled", "canceled", "unmatched"):
            log(f"[LIVE] SELL NO FILL | bid:{best_bid*100:.0f}c limit:{limit_price*100:.0f}c "
                f"| orderID:{str(order_id)[:14]}", "warn")
            return False, 0

        fill_price = limit_price
        time.sleep(2)
        try:
            order_info = live_client.get_order(order_id)
            actual_size = float(order_info.get("size_matched", 0) or 0)
            avg_price = float(order_info.get("associate_trades", [{}])[0].get("price", 0) or 0)
            if avg_price > 0:
                fill_price = avg_price
            log(f"[LIVE] SELL CONFIRMED (REST) | {actual_size or shares:.2f}sh @ {fill_price*100:.1f}c "
                f"(limit:{limit_price*100:.0f}c) | orderID:{str(order_id)[:14]}", "fire")
        except Exception as e:
            log(f"[LIVE] Sell fill check failed ({e}), using limit price {limit_price*100:.0f}c", "warn")

        return True, fill_price

    except Exception as e:
        log(f"[LIVE] SELL ORDER FAILED: {e}", "warn")
        return False, 0


def sync_balance(bal):
    """Query on-chain USDC balance and sync internal tracker.
    Called at window rotation and after stop-loss in live mode."""
    if C["dry_run"] or not live_client:
        return
    try:
        resp = requests.get(
            f"{C['clob_url']}/balance?address={C['funder']}",
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            # Polymarket returns balance in USDC (6 decimals)
            chain_bal = float(data.get("balance", 0)) / 1e6
            with bal["lock"]:
                old = bal["bal"]
                bal["bal"] = chain_bal + bal["committed"]  # committed funds are still "ours"
            log(f"[LIVE] Balance sync: on-chain ${chain_bal:.2f} "
                f"(was ${old:.2f}, committed ${bal['committed']:.2f})", "sys")
    except Exception as e:
        log(f"[LIVE] Balance sync failed: {e}", "warn")


# ===========================================═══════════════════
# REVERSAL MONITOR — stop-loss + flip logic
# ===========================================═══════════════════
# Daemon thread per position. Watches token value via CLOB best_bid.
# If value drops below floor (35c) → stop-loss → check flip.

def reversal_monitor(pos, chainlink, binance, clob, open_positions, pos_lock, bal, stopped_windows, flipped_windows):
    """Watch a dry-run position. Stop-loss based on Chainlink price vs PTB.

    Instead of watching noisy CLOB token prices, we monitor the actual
    settlement oracle (Chainlink).  If the model probability for our side
    drops below stop_prob_floor (delta flipped or shrank too much), we exit.
    This avoids false stop-losses from market-maker repricing noise.
    """
    tf_secs = pos["window_end"] - pos["window_start"]

    while not pos["resolved"]:
        time.sleep(0.5)
        now = time.time()
        secs_left = pos["window_end"] - now
        if secs_left <= C["stop_min_secs_remaining"]:
            break  # final 15s — let settlement decide, avoid pointless stop-penalty

        # Get Chainlink price (settlement oracle)
        cl_px, cl_age = chainlink.get(pos["sym"])
        if not cl_px or cl_age is None or cl_age > 5.0:
            continue  # no fresh oracle data

        # Current delta from PTB
        delta_pct = (cl_px - pos["ptb"]) / pos["ptb"]

        # Is Chainlink still on our side?
        if pos["side"] == "UP":
            on_our_side = delta_pct > 0
            our_delta = delta_pct
        else:
            on_our_side = delta_pct < 0
            our_delta = -delta_pct  # make positive when DN is winning

        # cl_led UP positions use ordering-based exit, not stop_prob_floor.
        # Thesis: Binance catching up to Chainlink. Hold while Chainlink stays
        # above (or within buffer below) PTB. Exit when Chainlink drops clearly.
        if pos.get("cl_led"):
            if delta_pct >= -C["cl_exit_floor"]:
                continue  # Chainlink still above PTB or within noise buffer — hold
            # Chainlink fell meaningfully below PTB → thesis broken, exit
            m_prob = 0.0  # force exit path below
        else:
            # Model probability for our side right now (use higher vol for stop-loss patience)
            secs_in = now - pos["window_start"]
            m_prob = model_probability(abs(delta_pct), secs_in, tf_secs,
                                       vol_override=C["vol_5m_stop"])
            # If delta is against us, invert: prob of OTHER side winning
            if not on_our_side:
                m_prob = 1.0 - m_prob

            if m_prob >= C["stop_prob_floor"]:
                continue  # model still favors our side, hold

        # ── STOP LOSS — model probability dropped below floor ──
        # Estimate sell price from CLOB for PnL calculation
        our_bid, _, _ = clob.get_best(pos["token_id"])  # bid on our token = what we can sell at
        _, opp_ask, opp_age = clob.get_best(pos["opp_token_id"])
        if opp_ask and opp_age and opp_age <= C["ws_stale_s"]:
            sell_price = 1.0 - opp_ask
        else:
            if our_bid and our_bid > 0:
                sell_price = our_bid
            else:
                sell_price = m_prob  # use model prob as price estimate

        # ── Live: dump tokens at any price (1c limit = market sell) ──
        if not C["dry_run"] and live_client:
            sold, actual_sell = live_sell(pos["token_id"], pos["shares"], 0.01)
            if sold:
                sell_price = actual_sell

        buy_fee = dynamic_fee(pos["entry_price"]) * pos["size"]
        sell_fee = dynamic_fee(sell_price) * sell_price * pos["shares"]
        pos["pnl"] = (sell_price * pos["shares"]) - pos["size"] - buy_fee - sell_fee
        pos["result"] = "stop_loss"
        pos["resolved"] = True
        # Chainsaw guard: mark this (asset, window) as stopped so no more entries/flips fire here.
        stopped_windows.add((pos["asset"], int(pos["window_end"])))

        with pos_lock:
            if pos in open_positions:
                open_positions.remove(pos)
        with bal["lock"]:
            bal["bal"] += pos["pnl"]
            bal["committed"] = max(0, bal["committed"] - pos["size"])
            bal["losses"] += 1
            bal["trades"] += 1
            bal["loss_streak"] += 1
            if pos["side"] == "UP":
                bal["up_losses"] += 1
            else:
                bal["dn_losses"] += 1

        # Sync real balance after stop-loss
        sync_balance(bal)

        sl = max(0, int(secs_left))
        exp_str = time.strftime("%H:%M:%S", time.localtime(pos["window_end"]))
        log(f"[STOP] {pos['asset']} {pos['timeframe']} {pos['side']} "
            f"| P:{m_prob*100:.0f}% delta:{delta_pct*100:+.3f}% "
            f"| {pos['entry_price']*100:.0f}c→{sell_price*100:.0f}c "
            f"| pnl:${pos['pnl']:+.4f} "
            f"| exp:{exp_str} ({sl // 60}m{sl % 60:02d}s)", "warn")
        csv_log("STOP", pos, {"exit_price": sell_price, "model_prob": m_prob,
                               "delta_pct": delta_pct, "balance": bal["bal"]})

        # ── FLIP CHECK: enter opposite side if HIGH+ conviction ──
        # cl_px and delta_pct already computed above in stop-loss check
        abs_delta = abs(delta_pct)

        # One-flip budget per (asset, window). First stop → flip allowed.
        # Second stop (of the flipped position) → no further flip.
        flip_key = (pos["asset"], int(pos["window_end"]))
        if flip_key in flipped_windows:
            break  # already flipped once in this window — no double-flip

        if abs_delta < C["conv_high"]:
            break  # not enough conviction to flip

        opp_side = "DN" if pos["side"] == "UP" else "UP"
        opp_token = pos["opp_token_id"]

        # Agreement check
        if C["require_both_agree"]:
            bnc_px, _ = binance.get(pos["sym"])
            if bnc_px:
                bnc_delta = (bnc_px - pos["ptb"]) / pos["ptb"]
                if (delta_pct > 0 and bnc_delta <= 0) or (delta_pct < 0 and bnc_delta >= 0):
                    break  # disagree, no flip

        # Check ask on opposite token
        _, opp_ask, opp_age = clob.get_best(opp_token)
        if not opp_ask or opp_ask > C["max_entry_price"]:
            break
        if opp_age and opp_age > C["ws_stale_s"]:
            break

        # Edge check on the flip side
        secs_in = time.time() - pos["window_start"]
        flip_fee = dynamic_fee(opp_ask)
        flip_prob = model_probability(abs_delta, secs_in, pos["window_end"] - pos["window_start"])
        flip_edge = flip_prob - opp_ask - flip_fee
        flip_min_edge = C["min_edge_up"] if opp_side == "UP" else C["min_edge_dn"]
        if flip_edge < flip_min_edge:
            break  # no edge on the flip

        # Book pressure on the flip token (stricter than entry — flips are higher-risk)
        flip_imb = book_imbalance(clob, opp_token)
        if flip_imb is not None and flip_imb < C["flip_book_min"]:
            break  # opp-side book not strong enough to justify flip

        # VPIN must support the flip direction
        flip_vpin, flip_vpin_dir = binance.get_vpin(pos["sym"])
        if flip_vpin is not None:
            if flip_vpin < C["vpin_min"] or flip_vpin_dir != opp_side:
                break  # informed flow doesn't support the flip

        # Sizing (streak-aware — same scaling as fresh entries)
        with bal["lock"]:
            avail = bal["bal"] - bal["committed"]
            streak = bal["loss_streak"]
        if avail < C["min_balance_halt"]:
            break
        if streak >= C["streak_quarter_at"]:
            streak_mult = 0.25
        elif streak >= C["streak_halve_at"]:
            streak_mult = 0.5
        else:
            streak_mult = 1.0
        flip_size = min(avail * C["size_pct"] * streak_mult, C["max_position_usd"])
        # Floor scaled size at $1 so flips keep firing during streaks
        if flip_size < C["min_order_usd"] and avail >= C["min_order_usd"]:
            flip_size = C["min_order_usd"]
        if flip_size < C["min_order_usd"]:
            break

        flip_conv = "MEGA" if abs_delta >= C["conv_mega"] else "HIGH"

        # ── Execute: dry-run simulated fill vs live CLOB order ──
        if C["dry_run"] or not live_client:
            slip = random.uniform(0.001, 0.009)
            fill_price = min(opp_ask + slip, C["max_entry_price"])
            flip_shares = flip_size / fill_price
        else:
            filled, fill_price, flip_shares = live_buy(opp_token, flip_size, opp_ask)
            if not filled:
                break
            flip_size = flip_shares * fill_price

        flip_pos = {
            "asset": pos["asset"], "sym": pos["sym"],
            "timeframe": pos["timeframe"], "side": opp_side,
            "token_id": opp_token, "opp_token_id": pos["token_id"],
            "entry_price": fill_price, "size": flip_size,
            "shares": flip_shares,
            "window_start": pos["window_start"],
            "window_end": pos["window_end"],
            "ptb": pos["ptb"], "entry_time": time.time(),
            "resolved": False, "result": None, "pnl": 0.0,
        }

        with pos_lock:
            open_positions.append(flip_pos)
        with bal["lock"]:
            bal["committed"] += flip_size
            if opp_side == "UP":
                bal["ups"] += 1
            else:
                bal["dns"] += 1
            bal["entry_px_sum"] += fill_price
            bal["entry_px_n"] += 1

        # Burn the flip budget for this (asset, window)
        flipped_windows.add(flip_key)

        fl = max(0, int(pos["window_end"] - time.time()))
        exp_str = time.strftime("%H:%M:%S", time.localtime(pos["window_end"]))
        log(f"[ENTRY] FLIP {pos['asset']} {pos['timeframe']} {opp_side} "
            f"| conv:{flip_conv} delta:{abs_delta*100:.3f}% "
            f"| fill:{fill_price*100:.1f}c size:${flip_size:.2f} "
            f"| exp:{exp_str} ({fl // 60}m{fl % 60:02d}s)", "fire")
        csv_log("FLIP", flip_pos, {"model_prob": flip_prob, "edge": flip_edge,
                                    "delta_pct": abs_delta, "balance": bal["bal"]})

        # Monitor the flipped position too
        threading.Thread(
            target=reversal_monitor,
            args=(flip_pos, chainlink, binance, clob,
                  open_positions, pos_lock, bal, stopped_windows, flipped_windows),
            daemon=True,
        ).start()
        break


def main():
    log("===========================================", "sys")
    log("  Iron Dome v10 — Book-Driven Taker", "sys")
    log("  Part 1: Live Data Feeds + Display", "sys")
    log("===========================================", "sys")

    # ── Initialize live trading if enabled ──
    if not C["dry_run"]:
        if not init_live_client():
            log("[SYS] Falling back to dry-run mode", "warn")
            C["dry_run"] = True
    mode = "DRY-RUN" if C["dry_run"] else "LIVE"
    log(f"[SYS] Mode: {mode}", "warn" if not C["dry_run"] else "sys")

    # ── Initialize feeds ──
    binance  = BinanceFeed()
    chainlink = ChainlinkFeed()
    clob     = CLOBBookFeed()
    discovery = MarketDiscovery()

    # ── Start all WebSockets ──
    binance.start()
    chainlink.start()
    clob.start()

    log("[SYS] Waiting for feeds to connect...", "sys")
    time.sleep(4)

    # ── Initial market discovery ──
    discovery.refresh()
    token_ids = discovery.get_all_token_ids()
    if token_ids:
        clob.set_active_tokens(token_ids)
        log(f"[SYS] Discovered {len(token_ids)} tokens across "
            f"{len(discovery.get_all())} markets", "sys")
    else:
        log("[SYS] No markets found yet — will retry", "warn")

    # Give CLOB WS time to subscribe + receive initial book snapshots
    time.sleep(3)
    log("[SYS] Streaming live. Ctrl+C to stop.", "sys")

    # ── Main display loop ──
    last_discovery = 0
    last_display = 0
    prev_token_ids = set(token_ids) if token_ids else set()
    # PTB cache: (asset_name, window_start) → price
    # 2-tier resolution: Gamma → RTDS history
    ptb_cache = {}
    sniper_logged = set()  # (asset, window_start, side) — dedup snipe logs
    stopped_windows = set()  # (asset, window_end_int) — chainsaw guard: asset was stopped in this window
    flipped_windows = set()  # (asset, window_end_int) — flip budget: one flip max per (asset, window)

    # ── Dry-run state ──
    open_positions = []
    pos_lock = threading.Lock()
    bal = {
        "lock": threading.Lock(),
        "bal": C["balance"],
        "committed": 0.0,
        "wins": 0,
        "losses": 0,
        "trades": 0,
        "ups": 0,            # total UP entries (for REPORT)
        "dns": 0,            # total DN entries (for REPORT)
        "up_wins": 0,        # UP winners (for per-side WR in REPORT)
        "up_losses": 0,      # UP losses + stops
        "dn_wins": 0,        # DN winners
        "dn_losses": 0,      # DN losses + stops
        "entry_px_sum": 0.0, # sum of fill prices (for avg entry price)
        "entry_px_n": 0,     # count of entries used in the sum
        "trend_skips": 0,    # total trend_filter blocks (for REPORT — direct counter since SKIP_STAGES hides it)
        "loss_streak": 0,    # consecutive losses for streak-based sizing (reset on any win)
        "start_time": time.time(),
    }
    last_stats = 0  # timestamp of last 10-min stats log
    # Skip tracker: {(asset, window_start): {"reason": str, "count": int, "side": str, "best_ask": float, "best_edge": float}}
    skip_tracker = {}
    delta_history = {}  # asset → deque of (timestamp, abs_delta_pct)
    # Stage priority for "furthest reached" (higher = closer to entry)
    SKIP_STAGES = {"no_ptb": 0, "no_feed": 1, "conv_low": 2, "stale_move": 3,
                   "trend_filter": 4, "chainlink_disagree": 5, "no_book": 6, "max_price": 7,
                   "no_edge": 8, "book_pressure": 9,
                   "vpin_low": 10, "vpin_disagree": 11, "has_pos": 12, "chainsaw_block": 13,
                   "no_persistence": 14}
    log(f"[DRY] Paper trading — balance: ${C['balance']:.2f}", "sys")
    try:
        while True:
            now = time.time()

            # Check for window rotation: if any market's window_end has passed,
            # force an immediate discovery refresh (don't wait for the 10s timer).
            force_refresh = False
            for mkt in discovery.get_all():
                if now >= mkt["window_end"]:
                    force_refresh = True
                    break

            # Refresh discovery every 10s, or immediately on window rotation
            if force_refresh or now - last_discovery > 10:
                if force_refresh:
                    log("[MKT] Window rotation detected — refreshing discovery", "mkt")
                    # Dump skip summaries for the ending window
                    for (sk_asset, sk_ws), sk in skip_tracker.items():
                        log(f"[SKIP] {sk_asset} 5m {sk.get('side','?')} | "
                            f"blocked by: {sk['reason']} ({sk['count']}x) | "
                            f"best ask:{sk.get('best_ask', 0)*100:.0f}c edge:{sk.get('best_edge', 0)*100:.1f}c",
                            "warn")
                    skip_tracker.clear()
                    delta_history.clear()
                    sync_balance(bal)  # sync real balance at window rotation
                discovery.refresh()
                new_ids = discovery.get_all_token_ids()
                new_set = set(new_ids) if new_ids else set()
                if new_ids:
                    clob.set_active_tokens(new_ids)
                    if new_set != prev_token_ids:
                        log(f"[MKT] New tokens: {len(new_ids)} across "
                            f"{len(discovery.get_all())} markets", "mkt")
                    prev_token_ids = new_set
                last_discovery = now

            markets = discovery.get_all()
            if not markets:
                log("No active markets -- waiting...", "warn")
                time.sleep(1)
                continue

            # ── PTB resolution (2 tiers) ──
            # 1. Gamma priceToBeat or prev-window finalPrice (from discovery)
            # 2. Chainlink RTDS history at exact window_start epoch
            for mkt in markets:
                key = (mkt["asset"], mkt["window_start"])
                if key in ptb_cache:
                    continue
                # Tier 1: Gamma (prev-window finalPrice or direct priceToBeat)
                if mkt.get("ptb"):
                    ptb_cache[key] = mkt["ptb"]
                    log(f"[PTB] {mkt['asset']}: ${mkt['ptb']:,.2f} (Gamma)", "chnl")
                    continue
                # Tier 2: RTDS history buffer at exact window_start second
                rtds_ptb = chainlink.get_price_at(mkt["sym"], mkt["window_start"])
                if rtds_ptb:
                    ptb_cache[key] = rtds_ptb
                    log(f"[PTB] {mkt['asset']}: ${rtds_ptb:,.2f} (RTDS history)", "chnl")
                    continue

            # Cleanup old PTB entries (keep last 10 minutes)
            cutoff = int(now) - 600
            for k in [k for k in ptb_cache if k[1] < cutoff]:
                del ptb_cache[k]

            # ── SNIPER: scan for edge on each tick ──
            for mkt in markets:
                sym = mkt["sym"]
                asset = mkt["asset"]
                secs_in = now - mkt["window_start"]
                secs_left = max(0, int(mkt["window_end"] - now))
                sk_key = (asset, mkt["window_start"])

                def track_skip(reason, side="?", ask=0.0, edge=0.0):
                    """Update skip tracker — keeps the furthest stage reached."""
                    prev = skip_tracker.get(sk_key)
                    stage = SKIP_STAGES.get(reason, -1)
                    if prev is None or stage > SKIP_STAGES.get(prev["reason"], -1):
                        skip_tracker[sk_key] = {"reason": reason, "count": 1,
                                                "side": side, "best_ask": ask, "best_edge": edge}
                    else:
                        prev["count"] += 1
                        if ask > 0:
                            prev["best_ask"] = ask
                        if edge != 0:
                            prev["best_edge"] = edge

                # Only scan after sniper_start_s into the window
                if secs_in < C["sniper_start_s"]:
                    continue

                # Chainsaw guard: asset was stopped in this window — no re-entry
                if C["chainsaw_block"] and (asset, int(mkt["window_end"])) in stopped_windows:
                    track_skip("chainsaw_block")
                    continue

                # Too-late guard: block last-instant entries (dust ask + oracle-flip risk)
                if secs_left < C["entry_min_secs_remaining"]:
                    continue

                # Need PTB to compute delta
                ptb = ptb_cache.get((asset, mkt["window_start"]))
                if not ptb:
                    track_skip("no_ptb")
                    continue

                # Get Binance price (fastest feed — drives conviction)
                bnc_price, bnc_age = binance.get(sym)
                if not bnc_price or bnc_age is None or bnc_age > 3.0:
                    track_skip("no_feed")
                    continue

                # Get Chainlink price (settlement oracle — for agreement)
                cl_price, cl_age = chainlink.get(sym)

                # Delta from Binance (faster, catches moves 1-2s before Chainlink)
                delta_pct = (bnc_price - ptb) / ptb  # positive = UP winning

                # Direction based on Binance
                if delta_pct > 0:
                    side = "UP"
                    token_id = mkt.get("up_token")
                else:
                    side = "DN"
                    token_id = mkt.get("dn_token")

                # Chainlink-led UP entry — three valid orderings, all require CL above PTB:
                #   Path A: C > PTB > B  (CL leads, Binance lagging catch-up)
                #   Path B: C > B > PTB  (CL confirms above Binance, both above PTB)
                #   Path C: B > C > PTB  (Binance leads, CL lagging but confirming above PTB; tighter ask cap)
                cl_led = False
                path_c = False
                if (C.get("cl_lead_enabled") and cl_price
                    and cl_age is not None and cl_age <= C["cl_lead_max_age"]):
                    cl_delta_lead = (cl_price - ptb) / ptb

                    # Path A: C > PTB > B (override DN → UP)
                    if side == "DN" and cl_delta_lead > 0 and delta_pct < 0:
                        total_gap = cl_delta_lead + abs(delta_pct)
                        if (cl_delta_lead >= C["cl_lead_min_cl"]
                            and total_gap >= C["cl_lead_min_gap"]
                            and secs_left <= C["up_cl_max_secs_remaining"]):
                            up_tok = mkt.get("up_token")
                            up_book = book_imbalance(clob, up_tok)
                            if up_book is not None and up_book >= C["min_book_entry_up_cl"]:
                                side = "UP"
                                token_id = up_tok
                                cl_led = True
                                # Use |bnc_delta| for model prob: Binance's distance-to-PTB
                                # is the catch-up room the UP thesis depends on.
                                delta_pct = abs(delta_pct)

                    # Path B: C > B > PTB (confirm existing UP)
                    elif side == "UP" and cl_delta_lead > 0 and cl_price > bnc_price:
                        total_gap = cl_delta_lead + delta_pct  # both positive
                        if (cl_delta_lead >= C["cl_lead_min_cl"]
                            and total_gap >= C["cl_lead_min_gap"]
                            and secs_left <= C["up_cl_max_secs_remaining"]):
                            up_book = book_imbalance(clob, token_id)
                            if up_book is not None and up_book >= C["min_book_entry_up_cl"]:
                                cl_led = True

                    # Path C: B > C > PTB (Binance leads, CL still confirming above PTB)
                    elif side == "UP" and cl_delta_lead > 0 and cl_price < bnc_price:
                        total_gap = cl_delta_lead + delta_pct  # both positive
                        if (cl_delta_lead >= C["cl_lead_min_cl"]
                            and total_gap >= C["cl_lead_min_gap"]
                            and secs_left <= C["up_cl_max_secs_remaining"]):
                            up_book = book_imbalance(clob, token_id)
                            if up_book is not None and up_book >= C["min_book_entry_up_cl"]:
                                cl_led = True
                                path_c = True

                # UP entries only fire via CL-led override. Standard B>PTB UP disabled.
                if side == "UP" and not cl_led:
                    track_skip("up_non_cl", side)
                    continue

                abs_delta = abs(delta_pct)

                # ── 15min trend filter ──
                # Block counter-trend entries. Both 5min AND 15min slopes must
                # agree on a strong regime before we refuse an entry — this
                # avoids whipsaw in chop but prevents 5hr trending sessions
                # from bleeding the balance via against-trend stops.
                bnc_5m_ago = binance.get_price_at(sym, int(now) - C["trend_short_s"])
                bnc_15m_ago = binance.get_price_at(sym, int(now) - C["trend_long_s"])
                if bnc_5m_ago and bnc_15m_ago:
                    short_trend = (bnc_price - bnc_5m_ago) / bnc_5m_ago
                    long_trend = (bnc_price - bnc_15m_ago) / bnc_15m_ago
                    st_thr = C["trend_short_thr"]
                    lt_thr = C["trend_long_thr"]
                    if side == "DN" and short_trend > st_thr and long_trend > lt_thr:
                        # Override: strong DN book imbalance = confirmed reversal, trust book over trend
                        imb_ovr = book_imbalance(clob, token_id)
                        if imb_ovr is None or imb_ovr < C["trend_override_book"]:
                            track_skip("trend_filter", side)
                            with bal["lock"]:
                                bal["trend_skips"] += 1
                            continue
                    if side == "UP" and short_trend < -st_thr and long_trend < -lt_thr:
                        imb_ovr = book_imbalance(clob, token_id)
                        if imb_ovr is None or imb_ovr < C["trend_override_book"]:
                            track_skip("trend_filter", side)
                            with bal["lock"]:
                                bal["trend_skips"] += 1
                            continue

                # ── Record signed delta for freshness + persistence checks ──
                if asset not in delta_history:
                    delta_history[asset] = deque(maxlen=10)
                delta_history[asset].append((now, delta_pct))

                # Conviction level (from Binance delta)
                if abs_delta >= C["conv_mega"]:
                    conv = "MEGA"
                elif abs_delta >= C["conv_high"]:
                    conv = "HIGH"
                elif abs_delta >= C["conv_low"]:
                    conv = "LOW"
                else:
                    conv = "SKIP"

                # Chainlink-led UP: ordering is the signal, not magnitude. Bypass conv gate.
                if cl_led and conv not in ("HIGH", "MEGA"):
                    conv = "HIGH"

                # Only act on HIGH or MEGA
                if conv not in ("HIGH", "MEGA"):
                    track_skip("conv_low", side)
                    continue

                # ── Freshness check: is this a NEW move or already priced in? ──
                lookback = C["freshness_lookback"]
                ratio = C["freshness_ratio"]
                hist = delta_history.get(asset, deque())
                old_delta = None
                for ts, d in hist:
                    if now - ts >= lookback - 0.5 and now - ts <= lookback + 0.5:
                        old_delta = abs(d)  # history now stores signed; freshness compares magnitude
                        break
                if old_delta is not None and abs_delta > 0:
                    if old_delta >= abs_delta * ratio:
                        track_skip("stale_move", side)
                        continue

                # Agreement check: Chainlink must confirm same direction
                if C["require_both_agree"] and cl_price and cl_age is not None and cl_age <= 3.0:
                    cl_delta = (cl_price - ptb) / ptb
                    if (delta_pct > 0 and cl_delta <= 0) or (delta_pct < 0 and cl_delta >= 0):
                        track_skip("chainlink_disagree", side)
                        continue

                # Check best ask on the target token
                if not token_id:
                    continue
                _, best_ask, ask_age = clob.get_best(token_id)
                if not best_ask or (ask_age and ask_age > C["ws_stale_s"]):
                    track_skip("no_book", side)
                    continue

                # Max entry price cap
                if best_ask > C["max_entry_price"]:
                    track_skip("max_price", side, ask=best_ask)
                    continue
                if path_c and best_ask > C["path_c_max_entry"]:
                    track_skip("max_price", side, ask=best_ask)
                    continue

                # ── Edge check: model probability vs CLOB implied price ──
                fee = dynamic_fee(best_ask)
                m_prob = model_probability(abs_delta, secs_in, mkt["tf_secs"])
                edge = m_prob - best_ask - fee
                min_edge = C["min_edge_up"] if side == "UP" else C["min_edge_dn"]
                if edge < min_edge:
                    track_skip("no_edge", side, ask=best_ask, edge=edge)
                    continue

                # ── Persistence check: signal must stay on-side for N recent ticks ──
                # Bypassed for cl_led (independent CL+book confirmation) and for
                # fat-edge entries (mispricing strong enough to trust without confirmation).
                if not cl_led and edge < C["persistence_edge_bypass"]:
                    need_ticks = C["persistence_ticks"]
                    window = C["persistence_window_s"]
                    sign_need = 1.0 if side == "UP" else -1.0
                    on_side = 0
                    for ts, d in hist:
                        if now - ts <= window and (d * sign_need) > 0:
                            on_side += 1
                    if on_side < need_ticks:
                        track_skip("no_persistence", side, ask=best_ask, edge=edge)
                        continue

                # ── Book pressure: is the book supporting our direction? ──
                imb = book_imbalance(clob, token_id)
                if imb is not None and imb < C["book_imbalance_min"]:
                    track_skip("book_pressure", side, ask=best_ask, edge=edge)
                    continue

                # ── VPIN: is Binance informed flow supporting our direction? ──
                vpin, vpin_dir = binance.get_vpin(sym)
                if vpin is not None:
                    if vpin < C["vpin_min"]:
                        track_skip("vpin_low", side, ask=best_ask, edge=edge)
                        continue
                    if vpin_dir != side:
                        track_skip("vpin_disagree", side, ask=best_ask, edge=edge)
                        continue

                # Already positioned in this asset?
                with pos_lock:
                    has_pos = any(p["asset"] == asset and not p["resolved"]
                                 for p in open_positions)
                if has_pos:
                    track_skip("has_pos", side, ask=best_ask, edge=edge)
                    continue

                # Only enter once per (asset, window, side)
                sniper_key = (asset, mkt["window_start"], side)
                if sniper_key in sniper_logged:
                    continue
                sniper_logged.add(sniper_key)

                # Sizing (fee already computed above)
                with bal["lock"]:
                    avail = bal["bal"] - bal["committed"]
                    streak = bal["loss_streak"]
                if avail < C["min_balance_halt"]:
                    continue
                # Streak-based size scaling. Halves at N, quarters at M losses.
                # Reset to 1.0 on any win (handled at win sites).
                if streak >= C["streak_quarter_at"]:
                    streak_mult = 0.25
                elif streak >= C["streak_halve_at"]:
                    streak_mult = 0.5
                else:
                    streak_mult = 1.0
                trade_size = min(avail * C["size_pct"] * streak_mult, C["max_position_usd"])
                # Floor scaled size at $1 so the bot keeps firing during streaks
                # instead of silently skipping. Only skip if balance can't even cover $1.
                if trade_size < C["min_order_usd"] and avail >= C["min_order_usd"]:
                    trade_size = C["min_order_usd"]
                if trade_size < C["min_order_usd"]:
                    continue

                opp_token = mkt.get("dn_token") if side == "UP" else mkt.get("up_token")

                # ── Execute: dry-run simulated fill vs live CLOB order ──
                if C["dry_run"] or not live_client:
                    slip = random.uniform(0.001, 0.009)
                    fill_price = min(best_ask + slip, C["max_entry_price"])
                    shares = trade_size / fill_price
                else:
                    filled, fill_price, shares = live_buy(token_id, trade_size, best_ask)
                    if not filled:
                        exp_str = time.strftime("%H:%M:%S", time.localtime(mkt["window_end"]))
                        log(
                            f"[MISS] {asset} {mkt['timeframe']} {side} "
                            f"| conv:{conv} delta:{abs_delta*100:.3f}% "
                            f"| ask:{best_ask*100:.0f}c edge:{edge*100:.1f}c "
                            f"| size:${trade_size:.2f} "
                            f"| exp:{exp_str} ({secs_left // 60}m{secs_left % 60:02d}s)",
                            "warn"
                        )
                        continue
                    trade_size = shares * fill_price  # actual filled amount

                # UP-CL counterfactual: record DN-side best ask at entry so we
                # can evaluate after settlement whether the DN→UP swap was good.
                dn_cf_ask = None
                if cl_led:
                    _, _ask, _ = clob.get_best(mkt.get("dn_token"))
                    dn_cf_ask = _ask

                pos = {
                    "asset": asset, "sym": sym,
                    "timeframe": mkt["timeframe"], "side": side,
                    "token_id": token_id, "opp_token_id": opp_token,
                    "entry_price": fill_price, "size": trade_size,
                    "shares": shares,
                    "window_start": mkt["window_start"],
                    "window_end": mkt["window_end"],
                    "ptb": ptb, "entry_time": time.time(),
                    "resolved": False, "result": None, "pnl": 0.0,
                    "cl_led": cl_led,
                    "dn_cf_ask": dn_cf_ask,
                }

                with pos_lock:
                    open_positions.append(pos)
                with bal["lock"]:
                    bal["committed"] += trade_size
                    if side == "UP":
                        bal["ups"] += 1
                    else:
                        bal["dns"] += 1
                    bal["entry_px_sum"] += fill_price
                    bal["entry_px_n"] += 1

                imb_str = f" bk:{imb:.0%}" if imb is not None else ""
                vpin_str = f" V:{vpin:.0%}{vpin_dir}" if vpin is not None else ""
                exp_str = time.strftime("%H:%M:%S", time.localtime(mkt["window_end"]))
                price_str = (
                    f"B:${bnc_price:,.2f} C:${cl_price:,.2f} PTB:${ptb:,.2f}"
                    if cl_price else
                    f"B:${bnc_price:,.2f} PTB:${ptb:,.2f}"
                )
                side_tag = f"{side}-CL" if cl_led else side
                lottery_tag = "LOTTERY " if fill_price < 0.20 else ""
                cf_str = f" cf_dn:{dn_cf_ask*100:.0f}c" if cl_led and dn_cf_ask else ""
                log(
                    f"[ENTRY] {lottery_tag}{asset} {mkt['timeframe']} {side_tag} "
                    f"| conv:{conv} delta:{abs_delta*100:.3f}% "
                    f"| P:{m_prob*100:.0f}% ask:{best_ask*100:.0f}c edge:{edge*100:.1f}c{imb_str}{vpin_str}{cf_str} "
                    f"| fill:{fill_price*100:.1f}c size:${trade_size:.2f} "
                    f"| {price_str} "
                    f"| exp:{exp_str} ({secs_left // 60}m{secs_left % 60:02d}s)",
                    "fire"
                )
                csv_log("ENTRY", pos, {"model_prob": m_prob, "edge": edge,
                                       "delta_pct": abs_delta, "balance": bal["bal"]})

                # Spawn reversal monitor for this position
                threading.Thread(
                    target=reversal_monitor,
                    args=(pos, chainlink, binance, clob,
                          open_positions, pos_lock, bal, stopped_windows, flipped_windows),
                    daemon=True,
                ).start()

            # ── Settle expired positions (next-window PTB = current finalPrice) ──
            with pos_lock:
                expired = [p for p in open_positions
                           if not p["resolved"] and now > p["window_end"] + 10]
            for ep in expired:
                # Next window's PTB = this window's finalPrice (Polymarket identity)
                # Tier 1: ptb_cache — already resolved via RTDS or Gamma at window rotation
                final_price = ptb_cache.get((ep["asset"], int(ep["window_end"])))

                # Tier 2: Gamma API — fetch next-window event's priceToBeat
                if final_price is None:
                    asset_cfg = next((a for a in C["assets"] if a["name"] == ep["asset"]), None)
                    if not asset_cfg:
                        continue
                    next_slug = f"{asset_cfg['slug_pfx']}-updown-{ep['timeframe']}-{int(ep['window_end'])}"
                    try:
                        r = requests.get(f"{C['gamma_url']}/events",
                                         params={"slug": next_slug}, timeout=5)
                        if r.ok:
                            evts = r.json()
                            if isinstance(evts, list) and evts:
                                meta = evts[0].get("eventMetadata") or {}
                                ptb_val = meta.get("priceToBeat")
                                if ptb_val is not None:
                                    final_price = float(ptb_val)
                    except Exception:
                        pass

                # Tier 3: Chainlink RTDS live price as last resort
                if final_price is None:
                    sym = ep.get("sym")
                    if sym:
                        cl_px, cl_age = chainlink.get(sym)
                        if cl_px and cl_age is not None and cl_age < 5.0:
                            final_price = cl_px

                if final_price is None:
                    if now < ep["window_end"] + 300:
                        continue  # wait up to 5min
                    won = False
                    log(f"[SETTLE] {ep['asset']} {ep['timeframe']} {ep['side']} "
                        f"| finalPrice unavailable after 5min — LOSS", "warn")
                else:
                    # finalPrice >= PTB → UP wins (ties go UP)
                    if ep["side"] == "UP":
                        won = final_price >= ep["ptb"]
                    else:  # DN wins when finalPrice < PTB
                        won = final_price < ep["ptb"]

                # ── Live: sell tokens immediately instead of waiting for redemption ──
                sell_price_settle = 1.0 if won else 0.0
                if not C["dry_run"] and live_client and won:
                    sold, actual_sell = live_sell(ep["token_id"], ep["shares"], 0.01)
                    if sold:
                        sell_price_settle = actual_sell
                        log(f"[LIVE] AUTO-SELL {ep['asset']} {ep['side']} "
                            f"| {ep['shares']:.2f}sh @ {actual_sell*100:.0f}c "
                            f"| instant USDC return", "fire")

                buy_fee = dynamic_fee(ep["entry_price"]) * ep["size"]
                if won:
                    ep["pnl"] = (sell_price_settle * ep["shares"]) - ep["size"] - buy_fee
                    ep["result"] = "win"
                else:
                    ep["pnl"] = -ep["size"] - buy_fee
                    ep["result"] = "loss"
                ep["resolved"] = True
                with pos_lock:
                    if ep in open_positions:
                        open_positions.remove(ep)
                with bal["lock"]:
                    bal["bal"] += ep["pnl"]
                    bal["committed"] = max(0, bal["committed"] - ep["size"])
                    if won:
                        bal["wins"] += 1
                        bal["loss_streak"] = 0  # reset streak on any win
                        if ep["side"] == "UP":
                            bal["up_wins"] += 1
                        else:
                            bal["dn_wins"] += 1
                    else:
                        bal["losses"] += 1
                        bal["loss_streak"] += 1
                        if ep["side"] == "UP":
                            bal["up_losses"] += 1
                        else:
                            bal["dn_losses"] += 1
                    bal["trades"] += 1
                tag = "fire" if won else "warn"
                fp_str = f" final:${final_price:,.2f}" if final_price else ""
                # UP-CL counterfactual: DN wins iff UP loses (same market).
                # cf_pnl ≈ what the DN trade would have returned at entry cf_ask.
                cf_str = ""
                if ep.get("cl_led") and ep.get("dn_cf_ask"):
                    cf_ask = ep["dn_cf_ask"]
                    cf_won = not won
                    cf_pnl = ep["size"] * (1 - cf_ask) / cf_ask if cf_won else -ep["size"]
                    cf_str = (f" | cf_dn:{cf_ask*100:.0f}c "
                              f"cf:{'WIN' if cf_won else 'LOSS'} "
                              f"cf_pnl:${cf_pnl:+.4f}")
                log(f"[SETTLED] {ep['asset']} {ep['timeframe']} {ep['side']} "
                    f"| {'WIN' if won else 'LOSS'} "
                    f"| entry:{ep['entry_price']*100:.0f}c"
                    f" ptb:${ep['ptb']:,.2f}{fp_str} "
                    f"pnl:${ep['pnl']:+.4f}{cf_str}", tag)
                csv_log("SETTLED", ep, {"exit_price": sell_price_settle if won else 0.0,
                                        "result": "win" if won else "loss",
                                        "balance": bal["bal"]})
                sync_balance(bal)  # sync real balance after settlement

            # ── Display every 10s (feeds update live in background) ──
            if now - last_display >= 10:
                last_display = now
                for mkt in markets:
                    sym = mkt["sym"]
                    secs_left = max(0, int(mkt["window_end"] - time.time()))
                    mins, secs_r = divmod(secs_left, 60)
                    exp_str = f"{mins}m {secs_r:02d}s"

                    bnc_price, _ = binance.get(sym)
                    bnc_str = f"${bnc_price:,.2f}" if bnc_price else "---"

                    cl_price, _ = chainlink.get(sym)
                    cl_str = f"${cl_price:,.2f}" if cl_price else "---"

                    # PTB from cache (Gamma or RTDS history)
                    ptb = ptb_cache.get((mkt["asset"], mkt["window_start"]))
                    ptb_str = f"${ptb:,.2f}" if ptb else "---"

                    _, up_ask, _ = clob.get_best(mkt.get("up_token"))
                    _, dn_ask, _ = clob.get_best(mkt.get("dn_token"))
                    up_str = f"{up_ask*100:.0f}c" if up_ask else "--"
                    dn_str = f"{dn_ask*100:.0f}c" if dn_ask else "--"

                    log(
                        f"[{mkt['asset']} {mkt['timeframe']}] "
                        f"B: {bnc_str} | C: {cl_str} | "
                        f"PTB: {ptb_str} | "
                        f"UP: {up_str}  DN: {dn_str} | "
                        f"Exp {exp_str}",
                        "live"
                    )

                # Balance + positions summary
                with bal["lock"]:
                    wr = round(bal["wins"] / bal["trades"] * 100) if bal["trades"] else 0
                    pnl = bal["bal"] - C["balance"]
                with pos_lock:
                    n_open = len(open_positions)
                log(f"[DRY] Bal:${bal['bal']:.2f} "
                    f"PnL:{'+'if pnl>=0 else ''}${pnl:.2f} | "
                    f"Open:{n_open} | "
                    f"W:{bal['wins']} L:{bal['losses']} WR:{wr}%", "sys")

            # ── 10-minute stats summary ──
            if now - last_stats >= 600:
                last_stats = now
                with bal["lock"]:
                    pnl = bal["bal"] - C["balance"]
                    trades = bal["trades"]
                    wr = round(bal["wins"] / trades * 100) if trades else 0
                    elapsed = int(now - bal.get("start_time", now))
                    hrs, mins = divmod(elapsed // 60, 60)
                    # EV per trade (realized)
                    ev = pnl / trades if trades else 0.0
                    # Trades per hour
                    tph = trades / (elapsed / 3600) if elapsed > 0 else 0.0
                    # Average entry price (in cents)
                    avg_px = (bal["entry_px_sum"] / bal["entry_px_n"] * 100) if bal["entry_px_n"] else 0.0
                    ups = bal["ups"]
                    dns = bal["dns"]
                    uw, ul = bal["up_wins"], bal["up_losses"]
                    dw, dl = bal["dn_wins"], bal["dn_losses"]
                    up_wr = round(uw / (uw + ul) * 100) if (uw + ul) else 0
                    dn_wr = round(dw / (dw + dl) * 100) if (dw + dl) else 0
                    log(f"═══ [REPORT] {hrs}h{mins:02d}m | "
                        f"Bal:${bal['bal']:.2f} "
                        f"PnL:{'+'if pnl>=0 else ''}${pnl:.2f} | "
                        f"W:{bal['wins']} L:{bal['losses']} "
                        f"WR:{wr}% | "
                        f"Trades:{trades} (UP:{ups} DN:{dns}) | "
                        f"EV:${ev:+.3f} | "
                        f"TPH:{tph:.1f} | "
                        f"AvgPx:{avg_px:.1f}c | "
                        f"UP_WR:{up_wr}% DN_WR:{dn_wr}% ═══", "live")

            time.sleep(0.5)  # 500ms tick — reads local memory only

    except KeyboardInterrupt:
        log("\n[SYS] Stopped.", "sys")
        # Final stats
        with bal["lock"]:
            pnl = bal["bal"] - C["balance"]
            wr = round(bal["wins"] / bal["trades"] * 100) if bal["trades"] else 0
            log(f"[FINAL] Bal:${bal['bal']:.2f} "
                f"PnL:{'+'if pnl>=0 else ''}${pnl:.2f} | "
                f"W:{bal['wins']} L:{bal['losses']} WR:{wr}% | "
                f"Trades:{bal['trades']}", "live")


# ===========================================═══════════════════
if __name__ == "__main__":
    main()
