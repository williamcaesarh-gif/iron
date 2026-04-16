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

import os, sys, json, time, math, random, threading, asyncio, requests
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
    "sniper_start_s":     30,      # Start scanning 30s into window.
    "max_entry_price":    0.67,    # Never buy above 75c. Hard cap. Need ~76% WR to profit.
    "fee_rate":           0.0156,  # Polymarket taker fee on crypto Up/Down = 1.56% (156 bps).
    "require_both_agree": True,    # Require Binance+Chainlink to agree on direction before firing.
    # Conviction thresholds (|delta_pct| = how far price moved from PTB)
    "conv_low":           0.0002,  # 0.02% — too weak, skip
    "conv_high":          0.0003,  # 0.03% — tradeable
    "conv_mega":          0.0012,  # 0.12% — strong signal
    # Edge model: model probability vs CLOB implied probability
    "min_edge":           0.03,    # 3c minimum edge (model_prob - ask - fee) to enter
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

    # ── Sizing ──
    "balance":            20.00,   # Starting balance for dry-run paper trading.
    "size_pct":           0.10,    # 10% of available balance per trade.
    "max_position_usd":   5.00,    # Hard cap per single position.
    "min_order_usd":      1.00,    # Polymarket minimum order value.

    # ── Lifecycle ──
    "auto_exit_price":    0.95,    # GTC sell at 95c for early win exit.
    "stop_prob_floor":    0.25,    # Stop-loss when model probability drops below 25%.

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

    def get(self, symbol):
        """Returns (price, age_seconds) or (None, None)."""
        with self._lock:
            d = self.prices.get(symbol.upper())
            if not d:
                return None, None
            return d["price"], time.time() - d["ts"]

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
                                    with self._lock:
                                        self.prices[sym] = {
                                            "price": float(close),
                                            "ts": time.time(),
                                        }

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
# DRY-RUN: REVERSAL MONITOR
# ===========================================═══════════════════
# Daemon thread per position. Watches token value via CLOB best_bid.
# If value drops below floor (35c) → stop-loss → check flip.

def reversal_monitor(pos, chainlink, binance, clob, open_positions, pos_lock, bal):
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
        if secs_left <= 15:
            break  # under 15s left — CLOB spread too wide, let settlement decide

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
        _, opp_ask, opp_age = clob.get_best(pos["opp_token_id"])
        if opp_ask and opp_age and opp_age <= C["ws_stale_s"]:
            sell_price = 1.0 - opp_ask
        else:
            _, our_ask, our_age = clob.get_best(pos["token_id"])
            if our_ask and our_age and our_age <= C["ws_stale_s"]:
                sell_price = our_ask
            else:
                sell_price = m_prob  # use model prob as price estimate

        buy_fee = dynamic_fee(pos["entry_price"]) * pos["size"]
        sell_fee = dynamic_fee(sell_price) * sell_price * pos["shares"]
        pos["pnl"] = (sell_price * pos["shares"]) - pos["size"] - buy_fee - sell_fee
        pos["result"] = "stop_loss"
        pos["resolved"] = True

        with pos_lock:
            if pos in open_positions:
                open_positions.remove(pos)
        with bal["lock"]:
            bal["bal"] += pos["pnl"]
            bal["committed"] = max(0, bal["committed"] - pos["size"])
            bal["losses"] += 1
            bal["trades"] += 1

        sl = max(0, int(secs_left))
        log(f"[STOP] {pos['asset']} {pos['timeframe']} {pos['side']} "
            f"| P:{m_prob*100:.0f}% delta:{delta_pct*100:+.3f}% "
            f"| {pos['entry_price']*100:.0f}c→{sell_price*100:.0f}c "
            f"| pnl:${pos['pnl']:+.4f} "
            f"| {sl // 60}m {sl % 60:02d}s left", "warn")

        # ── FLIP CHECK: enter opposite side if HIGH+ conviction ──
        # cl_px and delta_pct already computed above in stop-loss check
        abs_delta = abs(delta_pct)

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
        if flip_edge < C["min_edge"]:
            break  # no edge on the flip

        # Book pressure on the flip token
        flip_imb = book_imbalance(clob, opp_token)
        if flip_imb is not None and flip_imb < C["book_imbalance_min"]:
            break  # MMs selling hard on the flip side

        # VPIN must support the flip direction
        flip_vpin, flip_vpin_dir = binance.get_vpin(pos["sym"])
        if flip_vpin is not None:
            if flip_vpin < C["vpin_min"] or flip_vpin_dir != opp_side:
                break  # informed flow doesn't support the flip

        # Sizing
        with bal["lock"]:
            avail = bal["bal"] - bal["committed"]
        if avail < C["min_balance_halt"]:
            break
        flip_size = min(avail * C["size_pct"], C["max_position_usd"])
        if flip_size < C["min_order_usd"]:
            break

        # Fill with slippage (0.1c-0.9c above ask)
        slip = random.uniform(0.001, 0.009)
        fill_price = min(opp_ask + slip, C["max_entry_price"])
        flip_shares = flip_size / fill_price
        flip_conv = "MEGA" if abs_delta >= C["conv_mega"] else "HIGH"

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

        fl = max(0, int(pos["window_end"] - time.time()))
        log(f"[FLIP] {pos['asset']} {pos['timeframe']} {opp_side} "
            f"| conv:{flip_conv} delta:{abs_delta*100:.3f}% "
            f"| fill:{fill_price*100:.1f}c size:${flip_size:.2f} "
            f"| {fl // 60}m {fl % 60:02d}s left", "fire")

        # Monitor the flipped position too
        threading.Thread(
            target=reversal_monitor,
            args=(flip_pos, chainlink, binance, clob,
                  open_positions, pos_lock, bal),
            daemon=True,
        ).start()
        break


def main():
    log("===========================================", "sys")
    log("  Iron Dome v10 — Book-Driven Taker", "sys")
    log("  Part 1: Live Data Feeds + Display", "sys")
    log("===========================================", "sys")

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
        "start_time": time.time(),
    }
    last_stats = 0  # timestamp of last 5-min stats log
    # Skip tracker: {(asset, window_start): {"reason": str, "count": int, "side": str, "best_ask": float, "best_edge": float}}
    skip_tracker = {}
    delta_history = {}  # asset → deque of (timestamp, abs_delta_pct)
    # Stage priority for "furthest reached" (higher = closer to entry)
    SKIP_STAGES = {"no_ptb": 0, "no_feed": 1, "conv_low": 2, "stale_move": 3,
                   "chainlink_disagree": 4, "no_book": 5, "max_price": 6,
                   "no_edge": 7, "book_pressure": 8,
                   "vpin_low": 9, "vpin_disagree": 10, "has_pos": 11}
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

                abs_delta = abs(delta_pct)

                # ── Record delta for freshness check ──
                if asset not in delta_history:
                    delta_history[asset] = deque(maxlen=10)
                delta_history[asset].append((now, abs_delta))

                # Conviction level (from Binance delta)
                if abs_delta >= C["conv_mega"]:
                    conv = "MEGA"
                elif abs_delta >= C["conv_high"]:
                    conv = "HIGH"
                elif abs_delta >= C["conv_low"]:
                    conv = "LOW"
                else:
                    conv = "SKIP"

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
                        old_delta = d
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

                # ── Edge check: model probability vs CLOB implied price ──
                fee = dynamic_fee(best_ask)
                m_prob = model_probability(abs_delta, secs_in, mkt["tf_secs"])
                edge = m_prob - best_ask - fee
                if edge < C["min_edge"]:
                    track_skip("no_edge", side, ask=best_ask, edge=edge)
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
                if avail < C["min_balance_halt"]:
                    continue
                trade_size = min(avail * C["size_pct"], C["max_position_usd"])
                if trade_size < C["min_order_usd"]:
                    continue

                # 100% fill with 0.1c-0.9c slippage above best ask
                slip = random.uniform(0.001, 0.009)
                fill_price = min(best_ask + slip, C["max_entry_price"])
                shares = trade_size / fill_price
                opp_token = mkt.get("dn_token") if side == "UP" else mkt.get("up_token")

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
                }

                with pos_lock:
                    open_positions.append(pos)
                with bal["lock"]:
                    bal["committed"] += trade_size

                imb_str = f" bk:{imb:.0%}" if imb is not None else ""
                vpin_str = f" V:{vpin:.0%}{vpin_dir}" if vpin is not None else ""
                log(
                    f"[ENTRY] {asset} {mkt['timeframe']} {side} "
                    f"| conv:{conv} delta:{abs_delta*100:.3f}% "
                    f"| P:{m_prob*100:.0f}% ask:{best_ask*100:.0f}c edge:{edge*100:.1f}c{imb_str}{vpin_str} "
                    f"| fill:{fill_price*100:.1f}c size:${trade_size:.2f} "
                    f"| B:${bnc_price:,.2f} C:${cl_price:,.2f} PTB:${ptb:,.2f} " if cl_price else
                    f"| B:${bnc_price:,.2f} PTB:${ptb:,.2f} "
                    f"| {secs_left // 60}m {secs_left % 60:02d}s left",
                    "fire"
                )

                # Spawn reversal monitor for this position
                threading.Thread(
                    target=reversal_monitor,
                    args=(pos, chainlink, binance, clob,
                          open_positions, pos_lock, bal),
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

                buy_fee = dynamic_fee(ep["entry_price"]) * ep["size"]
                if won:
                    ep["pnl"] = (1.0 * ep["shares"]) - ep["size"] - buy_fee
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
                    else:
                        bal["losses"] += 1
                    bal["trades"] += 1
                tag = "fire" if won else "warn"
                fp_str = f" final:${final_price:,.2f}" if final_price else ""
                log(f"[SETTLED] {ep['asset']} {ep['timeframe']} {ep['side']} "
                    f"| {'WIN' if won else 'LOSS'} "
                    f"| entry:{ep['entry_price']*100:.0f}c"
                    f" ptb:${ep['ptb']:,.2f}{fp_str} "
                    f"pnl:${ep['pnl']:+.4f}", tag)

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

            # ── 5-minute stats summary ──
            if now - last_stats >= 300:
                last_stats = now
                with bal["lock"]:
                    pnl = bal["bal"] - C["balance"]
                    wr = round(bal["wins"] / bal["trades"] * 100) if bal["trades"] else 0
                    elapsed = int(now - bal.get("start_time", now))
                    mins_run = elapsed // 60
                    log(f"═══ [REPORT] {mins_run}min | "
                        f"Bal:${bal['bal']:.2f} "
                        f"PnL:{'+'if pnl>=0 else ''}${pnl:.2f} | "
                        f"W:{bal['wins']} L:{bal['losses']} "
                        f"WR:{wr}% | "
                        f"Trades:{bal['trades']} ═══", "live")

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
