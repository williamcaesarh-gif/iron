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

import os, sys, json, time, math, threading, asyncio, requests
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

    # ── Direction model ──
    "min_delta_pct":      0.0005,  # 0.05% PTB move to have a directional thesis. Lower=more signals, more noise.
    "require_both_agree": True,    # Require Binance+Chainlink to agree on direction before firing.
    "thesis_max_age_s":   2.0,     # Thesis older than this is stale when a book event arrives.

    # ── Trigger / EV ──
    "min_ev":             0.10,    # Min expected return per dollar to fire. 0.10 = need 10% edge after fees.
    "max_entry_price":    0.85,    # Never pay above 85c regardless of EV. Caps tail risk on wrong calls.
    "fee_rate":           0.0156,  # Polymarket taker fee on crypto Up/Down = 1.56% (156 bps).

    # ── Sizing ──
    "balance":            10.00,   # Starting balance (overridden by wallet sync in live mode).
    "size_pct":           0.20,    # 20% of available balance per trade.
    "max_position_usd":   5.00,    # Hard cap per single position.
    "min_order_usd":      1.00,    # Polymarket minimum order value.

    # ── Lifecycle ──
    "auto_exit_price":    0.95,    # GTC sell at 95c for early win exit.
    "reversal_drop":      0.15,    # Stop-loss triggers if price drops this much from entry.
    "reversal_floor":     0.40,    # Absolute floor — never stop-loss above this.

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
        self.prices = {}   # "BTCUSDT" → {"price": 71234.56, "ts": 1713000000.0}
        self._lock = threading.Lock()

    def get(self, symbol):
        """Returns (price, age_seconds) or (None, None)."""
        with self._lock:
            d = self.prices.get(symbol.upper())
            if not d:
                return None, None
            return d["price"], time.time() - d["ts"]

    def start(self):
        threading.Thread(target=self._thread, daemon=True).start()

    def _thread(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._run())

    async def _run(self):
        # Build combined stream URL: btcusdt@miniTicker/ethusdt@miniTicker/...
        streams = "/".join(
            f"{a['binance_sym']}@miniTicker" for a in C["assets"]
        )
        url = f"wss://stream.binance.com:9443/stream?streams={streams}"

        while True:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    log("[BNCE] Binance WebSocket connected", "bnce")
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        try:
                            msg = json.loads(raw)
                            data = msg.get("data", {})
                            sym = data.get("s", "").upper()     # "BTCUSDT"
                            close = data.get("c")               # last price string
                            if sym and close:
                                with self._lock:
                                    self.prices[sym] = {
                                        "price": float(close),
                                        "ts": time.time(),
                                    }
                        except (json.JSONDecodeError, ValueError, TypeError):
                            pass
            except asyncio.TimeoutError:
                log("[BNCE] timeout — reconnect 3s", "warn")
                await asyncio.sleep(3)
            except Exception as e:
                log(f"[BNCE] error: {type(e).__name__}: {e} — reconnect 5s", "warn")
                await asyncio.sleep(5)


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

    def get(self, symbol):
        """Returns (price, age_seconds) or (None, None)."""
        with self._lock:
            d = self.prices.get(symbol.upper())
            if not d:
                return None, None
            return d["price"], time.time() - d["ts"]

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
                            canonical = self._sym_map.get(sym_raw)
                            if canonical and val:
                                with self._lock:
                                    self.prices[canonical] = {
                                        "price": float(val),
                                        "ts": time.time(),
                                    }
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

        # Parse event metadata (PTB, finalPrice)
        em = ev.get("eventMetadata") or {}
        ptb = None
        if em.get("priceToBeat"):
            try:
                ptb = float(em["priceToBeat"])
            except (ValueError, TypeError):
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
    # Source: Gamma eventMetadata.priceToBeat (the only authoritative source).
    # Our own Chainlink feed diverges because we can't snapshot at the exact
    # epoch second that Polymarket's server does.
    ptb_cache = {}
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

            # PTB not yet known for any market? Poll Gamma more aggressively
            # in the first 60s of a window to pick up priceToBeat sooner.
            ptb_missing = False
            for mkt in discovery.get_all():
                key = (mkt["asset"], mkt["window_start"])
                secs_in = now - mkt["window_start"]
                if key not in ptb_cache and secs_in < 60:
                    ptb_missing = True
                    break

            # Refresh discovery: every 10s normally, every 3s if waiting for PTB,
            # or immediately on window rotation
            refresh_interval = 3 if ptb_missing else 10
            if force_refresh or now - last_discovery > refresh_interval:
                if force_refresh:
                    log("[MKT] Window rotation detected — refreshing discovery", "mkt")
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

            # ── PTB: pick up from Gamma eventMetadata ──
            for mkt in markets:
                key = (mkt["asset"], mkt["window_start"])
                if key not in ptb_cache and mkt.get("ptb"):
                    ptb_cache[key] = mkt["ptb"]
                    log(f"[PTB] {mkt['asset']}: ${mkt['ptb']:,.2f}", "chnl")

            # Cleanup old PTB entries (keep last 10 minutes)
            cutoff = int(now) - 600
            for k in [k for k in ptb_cache if k[1] < cutoff]:
                del ptb_cache[k]

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

                    # PTB: prefer our Chainlink snapshot, fall back to Gamma
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

            time.sleep(1)

    except KeyboardInterrupt:
        log("\n[SYS] Stopped.", "sys")


# ===========================================═══════════════════
if __name__ == "__main__":
    main()
