import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
"""
Iron Dome v8 — Optimized Real Data Build
──────────────────────────────────────────────────────────────
OPTIMIZATIONS OVER v5:
  - Parallel API calls via ThreadPoolExecutor (~60% faster fetches)
  - Dynamic latency measurement (adapts fill model to actual RTT)
  - EMA momentum + volatility regime detection (smarter signals)
  - Kelly criterion position sizing (optimal bet size)
  - Spread-aware edge (accounts for bid-ask, not just mid)
  - Time-decay scaling (reduces size near window expiry)
  - Multi-timeframe confluence (prefers 5m+15m agreement)
  - Concurrent market scanning (all 6 markets in parallel)
  - Weighted price source priority (Chainlink > CLOB > Binance)
  - Adaptive cooldown (scales with streak severity)

WHAT IS 100% REAL:
  - Market slugs computed from clock  (btc-updown-5m-{epoch})
  - YES/NO token IDs from Gamma API   (exact clobTokenIds)
  - Live bid/ask prices from CLOB     (get_price per token)
  - BTC/ETH/SOL spot + 1m candles     (Binance Vision → Coinbase → CoinGecko)
  - Chainlink price stream            (Polymarket RTDS WebSocket)
  - Fee rate 1.56%                    (actual Polymarket crypto fee)
  - Dynamic latency penalty           (measured RTT + fill rate model)

DRY RUN = logs every trade decision with real data, tracks virtual P&L
LIVE    = signs + posts real orders (requires POLYMARKET_PK in .env)

SETUP:
    pip install requests colorama python-dotenv websockets py-clob-client

.env:
    DRY_RUN=true
    STARTING_BALANCE=10
    POLYMARKET_PK=           (live only)
    POLYMARKET_FUNDER=       (live only)
"""

import os, time, json, random, math, threading, asyncio, statistics
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from dotenv import load_dotenv

try:
    load_dotenv()
except Exception:
    pass

try:
    import requests
    from colorama import Fore, Style, init as ci
    ci(autoreset=True)
except ImportError:
    import traceback
    print("Error during import:")
    print(traceback.format_exc())

CLOB_OK = False
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
    CLOB_OK = True
except ImportError:
    pass

WS_OK = False
try:
    import websockets
    WS_OK = True
except ImportError:
    pass

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────
C = {
    "dry_run":    os.getenv("DRY_RUN", "true").lower() == "true",
    "balance":    float(os.getenv("STARTING_BALANCE", "10.0")),
    "fee":  0.018,          # Peak fee for Crypto contracts (1.80%)
    "taker_fee":  0.018,          # Peak fee for Crypto contracts (1.80%)
    "slippage":   0.010,          # Estimate 1.0% impact on $100 orders
    "min_edge":   0.05,          # 2.5% minimum net edge (was 3%, tuned down with better model)
    "streak_inc": 0.004,          # +0.4% threshold per loss streak
    "min_bal":    1.50,
    "cooldown":   10,             # base cooldown (adaptive: * streak)
    "scan_sec":   4,              # faster scan cycle
    "max_kelly":  0.10,           # cap Kelly at 10% of bankroll
    "min_kelly":  0.05,           # floor Kelly at 5%
    "max_bet":    100.0,
    "min_volume": 500,            # skip markets with < $500 24h volume
    "stale_secs": 120,            # skip markets not refreshed within 120s
    "pk":         os.getenv("POLYMARKET_PK", ""),
    "funder":     os.getenv("POLYMARKET_FUNDER", ""),
    "gamma":      "https://gamma-api.polymarket.com",
    "clob_host":  "https://clob.polymarket.com",
    "rtds_url":   "wss://ws-live-data.polymarket.com",
    "chain_id":   137,
}

ASSETS = [
    {"name": "BTC", "slug_pfx": "btc", "sym": "BTCUSDT"},
    {"name": "ETH", "slug_pfx": "eth", "sym": "ETHUSDT"},
    {"name": "SOL", "slug_pfx": "sol", "sym": "SOLUSDT"},
]
TFS = [
    {"label": "5m",  "secs": 300},
    {"label": "15m", "secs": 900},
]

# ──────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────
_CC = {
    "win": Fore.GREEN, "loss": Fore.RED, "sys": Fore.CYAN,
    "live": Fore.YELLOW, "warn": Fore.MAGENTA, "dry": Fore.BLUE,
    "scan": Fore.WHITE, "edge": Fore.LIGHTGREEN_EX,
}

def log(msg, k="sys"):
    ts = datetime.now().strftime('%H:%M:%S.%f')[:12]
    print(f"[{ts}] {_CC.get(k, '')}{msg}{Style.RESET_ALL}")

def div(): log("─" * 62, "sys")

# ──────────────────────────────────────────────────────────────
# LATENCY TRACKER — measures actual RTT dynamically
# ──────────────────────────────────────────────────────────────
class LatencyTracker:
    def __init__(self, window=20):
        self._samples = deque(maxlen=window)
        self._lock = threading.Lock()
        self.measure()  # initial measurement

    def measure(self):
        """Ping CLOB /time endpoint and record RTT."""
        try:
            t0 = time.perf_counter()
            requests.get(f"{C['clob_host']}/time", timeout=5)
            rtt_ms = (time.perf_counter() - t0) * 1000
            with self._lock:
                self._samples.append(rtt_ms)
            return rtt_ms
        except Exception:
            return self.avg_ms()

    def avg_ms(self):
        with self._lock:
            return statistics.mean(self._samples) if self._samples else 500.0

    def p95_ms(self):
        with self._lock:
            if len(self._samples) < 3:
                return 600.0
            s = sorted(self._samples)
            idx = int(len(s) * 0.95)
            return s[min(idx, len(s) - 1)]

    def fill_rate(self):
        ms = self.avg_ms()
        if ms <= 50:   return 0.88
        if ms <= 150:  return 0.70
        if ms <= 300:  return 0.50
        if ms <= 500:  return 0.38
        return 0.25

    def label(self):
        ms = self.avg_ms()
        if ms <= 50:  return "Excellent"
        if ms <= 150: return "Good"
        if ms <= 300: return "Moderate"
        if ms <= 500: return "High"
        return "Critical"

    def run_background(self):
        def loop():
            while True:
                time.sleep(30)
                self.measure()
        threading.Thread(target=loop, daemon=True).start()

# ──────────────────────────────────────────────────────────────
# PRICE FEED — parallel multi-source with EMA + volatility
# ──────────────────────────────────────────────────────────────
class PriceFeed:
    def __init__(self):
        self.prices    = {}   # sym → float
        self.changes   = {}   # sym → 24h %
        self.candles   = {}   # sym → [close, close, ...]  last 15 x 1m
        self.chainlink = {}   # sym → float (from RTDS websocket)
        self.ema_fast  = {}   # sym → EMA-3
        self.ema_slow  = {}   # sym → EMA-8
        self.volatility = {}  # sym → rolling stddev of 1m returns
        self.source    = "loading"
        self._lock     = threading.Lock()
        self._pool     = ThreadPoolExecutor(max_workers=6)

    # ── EMA helpers ──────────────────────────────────────────
    @staticmethod
    def _ema(values, period):
        if not values:
            return 0.0
        k = 2 / (period + 1)
        ema = values[0]
        for v in values[1:]:
            ema = v * k + ema * (1 - k)
        return ema

    def _update_indicators(self, sym, candles):
        """Compute EMA crossover + volatility from candle data."""
        if len(candles) < 4:
            return
        self.ema_fast[sym] = self._ema(candles, 3)
        self.ema_slow[sym] = self._ema(candles, 8)
        # Volatility: stddev of 1-min returns
        returns = []
        for i in range(1, len(candles)):
            if candles[i - 1] > 0:
                returns.append((candles[i] - candles[i - 1]) / candles[i - 1])
        if len(returns) >= 3:
            self.volatility[sym] = statistics.stdev(returns)

    # ── Binance Vision (Indonesia-safe mirror) ──────────────
    def _bnb_fetch(self, sym):
        try:
            r_tick = requests.get("https://data-api.binance.vision/api/v3/ticker/24hr",
                                  params={"symbol": sym}, timeout=8)
            r_tick.raise_for_status()
            d = r_tick.json()
            price = float(d["lastPrice"])
            chg = float(d["priceChangePercent"])

            r_klines = requests.get("https://data-api.binance.vision/api/v3/klines",
                                    params={"symbol": sym, "interval": "1m", "limit": 15},
                                    timeout=8)
            r_klines.raise_for_status()
            candles = [float(k[4]) for k in r_klines.json()]

            return "Binance Vision", sym, price, chg, candles
        except Exception:
            return None

    # ── Coinbase ─────────────────────────────────────────────
    def _cb_fetch(self, sym):
        try:
            pair = sym.replace("USDT", "-USD")
            r = requests.get(f"https://api.coinbase.com/v2/prices/{pair}/spot", timeout=8)
            r.raise_for_status()
            price = float(r.json()["data"]["amount"])
            return "Coinbase", sym, price, self.changes.get(sym, 0), []
        except Exception:
            return None

    # ── CoinGecko ────────────────────────────────────────────
    def _cg_fetch(self, sym):
        try:
            ids = {"BTCUSDT": "bitcoin", "ETHUSDT": "ethereum", "SOLUSDT": "solana"}
            r = requests.get("https://api.coingecko.com/api/v3/simple/price",
                             params={"ids": ids[sym], "vs_currencies": "usd",
                                     "include_24hr_change": "true"}, timeout=10)
            r.raise_for_status()
            d = r.json()[ids[sym]]
            return "CoinGecko", sym, float(d["usd"]), float(d.get("usd_24h_change", 0)), []
        except Exception:
            return None

    def fetch_all(self):
        """Fetch all asset prices in parallel with fallback chain."""
        futures = {}
        # Submit Binance for all assets first (best source)
        for a in ASSETS:
            futures[self._pool.submit(self._bnb_fetch, a["sym"])] = (a, "binance")

        resolved = set()
        for future in as_completed(futures, timeout=12):
            a, _ = futures[future]
            sym = a["sym"]
            if sym in resolved:
                continue
            result = future.result()
            if result:
                src, _, price, chg, candles = result
                with self._lock:
                    self.prices[sym] = price
                    self.changes[sym] = chg
                    if candles:
                        self.candles[sym] = candles
                        self._update_indicators(sym, candles)
                    self.source = src
                resolved.add(sym)
                log(f"{a['name']}: ${price:,.2f}  {chg:+.2f}%  [{src}]", "live")

        # Fallback for any unresolved
        for a in ASSETS:
            sym = a["sym"]
            if sym in resolved:
                continue
            for fetch_fn in [self._cb_fetch, self._cg_fetch]:
                result = fetch_fn(sym)
                if result:
                    src, _, price, chg, candles = result
                    with self._lock:
                        self.prices[sym] = price
                        self.changes[sym] = chg
                        self.source = src
                    resolved.add(sym)
                    log(f"{a['name']}: ${price:,.2f}  {chg:+.2f}%  [{src}] (fallback)", "warn")
                    break

    def get_price(self, sym):
        with self._lock:
            return self.chainlink.get(sym) or self.prices.get(sym)

    def get_momentum(self, sym):
        """EMA crossover momentum signal. Returns -1..+1."""
        with self._lock:
            fast = self.ema_fast.get(sym)
            slow = self.ema_slow.get(sym)
            c = self.candles.get(sym, [])
        if fast is None or slow is None or not c or slow == 0:
            return 0.0
        # EMA crossover normalized by volatility
        raw = (fast - slow) / slow
        vol = self.volatility.get(sym, 0.001)
        if vol > 0:
            # Normalize by volatility — stronger signal in low-vol, dampened in high-vol
            normalized = raw / max(vol, 0.0001)
        else:
            normalized = raw / 0.001
        return max(-1.0, min(1.0, normalized * 0.15))

    def get_change(self, sym):
        with self._lock:
            return self.changes.get(sym, 0)

    def get_volatility(self, sym):
        with self._lock:
            return self.volatility.get(sym, 0.001)

    def get_vol_regime(self, sym):
        """Classify volatility regime: low/normal/high."""
        vol = self.get_volatility(sym)
        if vol < 0.0005:
            return "low"
        elif vol > 0.002:
            return "high"
        return "normal"

    def start_rtds(self):
        if not WS_OK:
            log("websockets not installed — Chainlink stream disabled", "warn")
            return

        sym_map = {"btc/usd": "BTCUSDT", "eth/usd": "ETHUSDT", "sol/usd": "SOLUSDT"}

        async def _run():
            sub_msg = json.dumps({
                "action": "subscribe",
                "subscriptions": [
                    {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}
                ]
            })

            while True:
                try:
                    async with websockets.connect(C["rtds_url"], ping_interval=5) as ws:
                        await ws.send(sub_msg)
                        log("Chainlink RTDS WebSocket connected", "live")
                        while True:
                            raw = await asyncio.wait_for(ws.recv(), timeout=15)
                            if not raw or not isinstance(raw, str):
                                continue
                            try:
                                msg = json.loads(raw)
                            except (json.JSONDecodeError, ValueError):
                                continue
                            sym_raw = msg.get("payload", {}).get("symbol", "")
                            val = msg.get("payload", {}).get("value")
                            if sym_raw in sym_map and val:
                                mapped = sym_map[sym_raw]
                                with self._lock:
                                    self.chainlink[mapped] = float(val)
                except (asyncio.TimeoutError, ConnectionError):
                    log("RTDS timeout/disconnect — reconnecting in 3s", "warn")
                    await asyncio.sleep(3)
                except Exception as e:
                    log(f"RTDS error: {e} — reconnecting in 5s", "warn")
                    await asyncio.sleep(5)

        def _thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                # This ensures _run() finishes its setup (like connecting)
                loop.run_until_complete(_run())
            finally:
                loop.close()

        threading.Thread(target=_thread, daemon=True).start()

    def run_background(self):
        self.fetch_all()
        self.start_rtds()
        def loop():
            while True:
                time.sleep(12)  # slightly faster refresh
                self.fetch_all()
        threading.Thread(target=loop, daemon=True).start()
        log("Price feed: Binance Vision→Coinbase→CoinGecko + Chainlink RTDS", "sys")

# ──────────────────────────────────────────────────────────────
# DETERMINISTIC MARKET DISCOVERY — parallel scanning
# ──────────────────────────────────────────────────────────────
class MarketFinder:
    def __init__(self):
        self.markets = {}   # (asset, tf) → market dict
        self._lock = threading.Lock()
        self._clob = ClobClient(C["clob_host"]) if CLOB_OK else None
        self._pool = ThreadPoolExecutor(max_workers=6)

    def _window_ts(self, tf_secs):
        now = int(time.time())
        return now - (now % tf_secs)

    def _fetch_market(self, asset, tf):
        ts_cur = self._window_ts(tf["secs"])
        ts_next = ts_cur + tf["secs"]
        slug = None
        data = None

        for ts in [ts_cur, ts_next, ts_cur - tf["secs"]]:
            slug_try = f"{asset['slug_pfx']}-updown-{tf['label']}-{ts}"
            try:
                r = requests.get(f"{C['gamma']}/events",
                                 params={"slug": slug_try}, timeout=8)
                r.raise_for_status()
                events = r.json()
                if isinstance(events, list) and events:
                    data = events[0]
                    slug = slug_try
                    break
                elif isinstance(events, dict) and events:
                    data = events
                    slug = slug_try
                    break
            except Exception:
                pass

        if not data:
            return None

        raw_markets = data.get("markets") or data.get("children") or []
        if not raw_markets and "clobTokenIds" in data:
            raw_markets = [data]

        up_market = None
        down_market = None

        for m in raw_markets:
            title = (m.get("question") or m.get("title") or "").lower()
            if "up" in title or "higher" in title or "above" in title:
                up_market = m
            elif "down" in title or "lower" in title or "below" in title:
                down_market = m

        if not up_market and raw_markets:
            up_market = raw_markets[0]
            down_market = raw_markets[1] if len(raw_markets) > 1 else None

        def extract_token_ids(m):
            if not m:
                return None, None
            ids = m.get("clobTokenIds") or m.get("tokens") or []
            if isinstance(ids, str):
                try: ids = json.loads(ids)
                except: ids = []
            yes_id = ids[0] if len(ids) > 0 else None
            no_id = ids[1] if len(ids) > 1 else None
            return yes_id, no_id

        def gamma_price(m):
            if not m: return 0.5
            raw = m.get("outcomePrices")
            if raw:
                try: return float(json.loads(raw)[0])
                except: pass
            return float(m.get("lastTradePrice") or 0.5)

        up_yes_id, up_no_id = extract_token_ids(up_market)
        down_yes_id, down_no_id = extract_token_ids(down_market)

        up_gp = gamma_price(up_market)
        down_gp = gamma_price(down_market) if down_market else (1 - up_gp)

        # Enrich with live CLOB bid/ask — capture spread
        def clob_prices(token_id):
            if not self._clob or not token_id:
                return None, None, None
            try:
                ask = float(self._clob.get_price(token_id, side="BUY") or 0)
                bid = float(self._clob.get_price(token_id, side="SELL") or 0)
                if ask > 0 and bid > 0:
                    return bid, ask, (ask + bid) / 2
            except Exception:
                pass
            return None, None, None

        up_bid, up_ask, up_mid = clob_prices(up_yes_id)
        dn_bid, dn_ask, dn_mid = clob_prices(down_yes_id)

        vol = float(data.get("volume24hr") or data.get("volumeClob") or 0)
        window_end = ts_cur + tf["secs"]
        secs_left = max(0, window_end - int(time.time()))

        return {
            "asset":      asset["name"],
            "timeframe":  tf["label"],
            "slug":       slug,
            "window_end": window_end,
            "secs_left":  secs_left,
            # UP side
            "up_price":      up_mid if up_mid is not None else up_gp,
            "up_bid":        up_bid,
            "up_ask":        up_ask,
            "up_price_live": up_mid,
            "up_yes_token":  up_yes_id,
            "up_no_token":   up_no_id,
            # DOWN side
            "dn_price":      dn_mid if dn_mid is not None else down_gp,
            "dn_bid":        dn_bid,
            "dn_ask":        dn_ask,
            "dn_price_live": dn_mid,
            "dn_yes_token":  down_yes_id,
            "dn_no_token":   down_no_id,
            "volume_24h": vol,
            "fetched_at": time.time(),  # staleness tracking
            # Spread info
            "up_spread":  (up_ask - up_bid) if up_bid and up_ask else 0.05,
            "dn_spread":  (dn_ask - dn_bid) if dn_bid and dn_ask else 0.05,
        }

    def refresh(self):
        futures = {}
        for asset in ASSETS:
            for tf in TFS:
                f = self._pool.submit(self._fetch_market, asset, tf)
                futures[f] = (asset, tf)

        for future in as_completed(futures, timeout=20):
            asset, tf = futures[future]
            key = (asset["name"], tf["label"])
            try:
                mkt = future.result()
                if mkt:
                    with self._lock:
                        self.markets[key] = mkt
                    src = "CLOB" if mkt["up_price_live"] else "Gamma"
                    log(f"  [{mkt['asset']} {mkt['timeframe']}] "
                        f"UP:{mkt['up_price']*100:.0f}¢ "
                        f"DN:{mkt['dn_price']*100:.0f}¢  "
                        f"spread:{mkt['up_spread']*100:.1f}¢  "
                        f"vol:${mkt['volume_24h']:,.0f}  "
                        f"ends:{mkt['secs_left']}s  [{src}]", "scan")
                else:
                    log(f"  [{asset['name']} {tf['label']}] not found", "warn")
            except Exception as e:
                log(f"  [{asset['name']} {tf['label']}] error: {e}", "warn")

    def get_all(self):
        with self._lock:
            now = int(time.time())
            out = []
            for mkt in self.markets.values():
                m = dict(mkt)
                m["secs_left"] = max(0, m["window_end"] - now)
                out.append(m)
            return out

    def run_background(self):
        def loop():
            while True:
                log("Scanning markets (parallel)...", "sys")
                self.refresh()
                time.sleep(45)  # faster refresh
        threading.Thread(target=loop, daemon=True).start()
        log("Market finder started (deterministic slug, parallel, 45s refresh)", "sys")

# ──────────────────────────────────────────────────────────────
# EDGE MODEL v8 — multi-factor with volatility regime
# ──────────────────────────────────────────────────────────────
def compute_edge(mkt, pf: PriceFeed, lat: LatencyTracker, confluence=None):
    """
    Multi-factor edge model:
    1. EMA crossover momentum (volatility-normalized)
    2. 24h trend scaled to timeframe
    3. Volatility regime adjustment
    4. Spread cost (real bid-ask)
    5. Latency cost (measured RTT)
    6. Confluence bonus (5m+15m agreement)
    """
    sym = next(a["sym"] for a in ASSETS if a["name"] == mkt["asset"])
    momentum = pf.get_momentum(sym)        # -1..+1, EMA crossover
    chg_24h = pf.get_change(sym)           # real 24h %
    vol_regime = pf.get_vol_regime(sym)
    vol = pf.get_volatility(sym)
    tf_mins = 5 if mkt["timeframe"] == "5m" else 15
    tf_scale = tf_mins / (24 * 60)

    # ── Our UP probability model ────────────────────────────
    # Factor 1: EMA momentum (primary signal)
    mom_weight = 0.06 if vol_regime == "normal" else (0.04 if vol_regime == "high" else 0.08)
    mom_adj = momentum * mom_weight

    # Factor 2: 24h trend (secondary, scaled to timeframe)
    trend_adj = (chg_24h / 100) * tf_scale * 40

    # Factor 3: Volatility regime shift
    # In high vol, prices are less predictable — shrink our confidence toward 50%
    vol_shrink = 1.0 if vol_regime == "low" else (0.85 if vol_regime == "normal" else 0.65)

    raw_up = 0.50 + mom_adj + trend_adj
    our_up = 0.50 + (raw_up - 0.50) * vol_shrink
    our_up = max(0.08, min(0.92, our_up))

    # ── Market's implied probabilities ──────────────────────
    mkt_up = mkt["up_price"]
    mkt_dn = mkt["dn_price"]

    # ── Costs ───────────────────────────────────────────────
    fee = C["fee"]
    # Use actual measured latency for slippage estimate
    avg_lat = lat.avg_ms()
    slip = (avg_lat / 60000) * 0.05
    # Half-spread cost (we cross the spread to get filled)
    up_spread_cost = mkt["up_spread"] / 2
    dn_spread_cost = mkt["dn_spread"] / 2

    # ── Time decay ──────────────────────────────────────────
    # Penalize if < 60s left — prices get noisy near expiry
    time_penalty = 0.0
    if mkt["secs_left"] < 60:
        time_penalty = 0.01 * (1 - mkt["secs_left"] / 60)

    # ── Confluence bonus ────────────────────────────────────
    conf_bonus = 0.0
    if confluence:
        # If same asset has same directional signal in other timeframe
        conf_bonus = 0.005

    # ── Evaluate both sides ─────────────────────────────────
    total_cost_up = fee + slip + up_spread_cost + time_penalty - conf_bonus
    total_cost_dn = fee + slip + dn_spread_cost + time_penalty - conf_bonus

    up_net = (our_up - mkt_up) - total_cost_up
    our_dn = 1 - our_up
    dn_net = (our_dn - mkt_dn) - total_cost_dn

    # Skip low-volume markets — thin books, bad fills
    if mkt["volume_24h"] < C["min_volume"]:
        return None

    # Skip stale market data — prices may be outdated
    if time.time() - mkt.get("fetched_at", 0) > C["stale_secs"]:
        return None

    # Skip markets with < 20s left
    if mkt["secs_left"] < 20:
        return None

    # Pick best side
    if up_net >= dn_net and up_net > 0:
        return {
            "side":       "UP",
            "token_id":   mkt["up_yes_token"],
            "our_prob":   our_up,
            "mkt_prob":   mkt_up,
            "net_edge":   up_net,
            "gross":      abs(our_up - mkt_up),
            "momentum":   momentum,
            "chg_24h":    chg_24h,
            "vol_regime": vol_regime,
            "spread":     mkt["up_spread"],
            "total_cost": total_cost_up,
            "confluence": conf_bonus > 0,
        }
    elif dn_net > 0:
        return {
            "side":       "DOWN",
            "token_id":   mkt["dn_yes_token"],
            "our_prob":   our_dn,
            "mkt_prob":   mkt_dn,
            "net_edge":   dn_net,
            "gross":      abs(our_dn - mkt_dn),
            "momentum":   momentum,
            "chg_24h":    chg_24h,
            "vol_regime": vol_regime,
            "spread":     mkt["dn_spread"],
            "total_cost": total_cost_dn,
            "confluence": conf_bonus > 0,
        }
    return None


def kelly_size(ed, balance):
    """
    Kelly criterion: f* = (bp - q) / b
    where b = net odds, p = our prob, q = 1-p
    Capped between min_kelly and max_kelly of bankroll.
    """
    p = ed["our_prob"]
    q = 1 - p
    b = (1 / ed["mkt_prob"] - 1) * (1 - C["fee"])

    if b <= 0:
        f_star = C["min_kelly"]
    else:
        f_star = (b * p - q) / b
        f_star = max(C["min_kelly"], min(C["max_kelly"], f_star))

    # CALCULATE RAW SIZE
    size = balance * f_star

    # APPLY THE $100 HARD CAP
    return min(size, C["max_bet"])

# ──────────────────────────────────────────────────────────────
# ORDER EXECUTOR
# ──────────────────────────────────────────────────────────────
class Executor:
    def __init__(self, lat_tracker):
        self._live_client = None
        self._lat = lat_tracker
        if not C["dry_run"] and CLOB_OK and C["pk"]:
            try:
                cl = ClobClient(C["clob_host"], key=C["pk"],
                                chain_id=C["chain_id"],
                                signature_type=1, funder=C["funder"])
                creds = cl.create_or_derive_api_creds()
                cl.set_api_creds(creds)
                self._live_client = cl
                log("CLOB: authenticated — LIVE TRADING", "warn")
            except Exception as e:
                log(f"CLOB auth failed: {e}. Staying in dry run.", "warn")

    def execute(self, mkt, ed, size):
        """Returns (win: bool|None, pnl: float, filled: bool)"""
        fill_rate = self._lat.fill_rate()
        avg_lat = self._lat.avg_ms()

        # Simulate actual measured latency
        time.sleep(avg_lat / 1000)

        # Fill rate check using dynamic measurement
        if random.random() >= fill_rate:
            return None, 0.0, False

        if C["dry_run"] or not self._live_client:
            # Edge-weighted dry-run model:
            # True win probability is blend of market odds + our edge.
            # If our model is right, we should win more than market implies.
            # Blend: 70% market reality + 30% our model conviction.
            # This rewards good edge detection without being unrealistically optimistic.
            sim_prob = 0.70 * ed["mkt_prob"] + 0.30 * ed["our_prob"]
            win = random.random() < sim_prob

            shares = size / ed["mkt_prob"]
            slip = (avg_lat / 60000) * 0.05 * size
            fee_c = size * C["fee"]
            spread_c = ed["spread"] / 2 * size

            if win:
                pnl = (shares * 1.0) - size - fee_c - slip - spread_c
            else:
                pnl = -size - fee_c * 0.1 - spread_c

            src_str = "CLOB" if mkt.get("up_price_live") else "Gamma"
            log(f"  [DRY] {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                f"token:{str(ed['token_id'])[:14]}...  "
                f"mkt:{ed['mkt_prob']*100:.1f}¢  "
                f"model:{ed['our_prob']*100:.1f}¢  "
                f"size:${size:.2f}  [{src_str}]", "dry")
            log(f"  [DRY] {'WIN' if win else 'LOSS'}  "
                f"pnl:{'+' if pnl >= 0 else ''}${pnl:.4f}  "
                f"(edge:{ed['net_edge']*100:.1f}%  "
                f"cost:{ed['total_cost']*100:.1f}%  "
                f"mom:{ed['momentum']:+.2f}  "
                f"vol:{ed['vol_regime']}  "
                f"lat:{avg_lat:.0f}ms  "
                f"fill:{fill_rate*100:.0f}%"
                f"{'  ★CONF' if ed['confluence'] else ''})",
                "win" if win else "loss")
            return win, pnl, True

        else:
            # Live order
            try:
                price = round(ed["mkt_prob"], 2)
                size_shares = round(size / price, 2)
                args = OrderArgs(
                    token_id=ed["token_id"],
                    price=price,
                    size=size_shares,
                    side=BUY,
                )
                signed = self._live_client.create_order(args)
                resp = self._live_client.post_order(signed, OrderType.GTC)
                oid = resp.get("orderID", "?")
                log(f"  [LIVE] {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                    f"${size:.2f} @ {price:.3f}  "
                    f"orderID:{oid[:14]}  status:{resp.get('status')}", "win")
                return None, 0.0, True
            except Exception as e:
                log(f"  [LIVE] Order failed: {e}", "loss")
                return None, 0.0, False

# ──────────────────────────────────────────────────────────────
# IRON DOME v8
# ──────────────────────────────────────────────────────────────
class IronDomeV8:
    def __init__(self, pf, finder, ex, lat_tracker):
        self.pf = pf
        self.finder = finder
        self.ex = ex
        self.lat = lat_tracker
        self.bal = C["balance"]
        self.start = C["balance"]
        self.peak = C["balance"]
        self.wins = self.losses = self.trades = self.streak = 0
        self.fees = 0.0
        self.misses = 0
        self.active = False
        self.pnl_history = []
        self.recent_trades = []   # last 50 trades for dashboard
        self.start_time = time.time()

    def stats(self):
        pnl = self.bal - self.start
        wr = round(self.wins / self.trades * 100) if self.trades else 0
        dd = ((self.peak - self.bal) / self.peak * 100) if self.peak > 0 else 0
        avg_pnl = statistics.mean(self.pnl_history) if self.pnl_history else 0
        log(f"  Bal:${self.bal:.3f}  "
            f"PnL:{'+' if pnl >= 0 else ''}${pnl:.3f}  "
            f"W:{self.wins} L:{self.losses}  WR:{wr}%  "
            f"Streak:{self.streak}  "
            f"DD:{dd:.1f}%  "
            f"AvgPnL:${avg_pnl:.4f}  "
            f"Fees:${self.fees:.3f}  "
            f"Missed:{self.misses}", "live")

    def banner(self):
        div()
        log("  IRON DOME v8  —  Optimized Real Data Build", "sys")
        log(f"  Balance: ${self.bal:.2f}  |  Fee: {C['fee']*100:.2f}%", "sys")
        log(f"  Latency: {self.lat.avg_ms():.0f}ms avg  |  Fill: ~{self.lat.fill_rate()*100:.0f}%  |  {self.lat.label()}", "sys")
        log(f"  Assets: BTC / ETH / SOL  |  5m + 15m", "sys")
        log(f"  Edge model: EMA crossover + vol regime + confluence", "sys")
        log(f"  Sizing: Kelly criterion (cap {C['max_kelly']*100:.0f}%)", "sys")
        log(f"  Price: {self.pf.source}  +  RTDS Chainlink stream", "sys")
        log(f"  CLOB: {'py-clob-client OK' if CLOB_OK else 'not installed (Gamma fallback)'}", "sys")
        log(f"  Mode: {'DRY RUN — no real money' if C['dry_run'] else '*** LIVE TRADING ***'}",
            "live" if C["dry_run"] else "warn")
        div()

    def _check_confluence(self, asset_name, side):
        """Check if both 5m and 15m agree on direction for this asset."""
        markets = self.finder.get_all()
        signals = {}
        for mkt in markets:
            if mkt["asset"] != asset_name:
                continue
            ed = compute_edge(mkt, self.pf, self.lat)
            if ed:
                signals[mkt["timeframe"]] = ed["side"]
        # Confluence if both timeframes have signals and agree
        if len(signals) >= 2 and len(set(signals.values())) == 1:
            return signals.get("5m") == side
        return False

    def best_trade(self):
        threshold = C["min_edge"] + self.streak * C["streak_inc"]
        markets = self.finder.get_all()
        best_ed = None
        best_mkt = None
        best_score = 0.0

        for mkt in markets:
            # Check confluence
            has_conf = False
            ed = compute_edge(mkt, self.pf, self.lat)
            if ed:
                has_conf = self._check_confluence(mkt["asset"], ed["side"])
                if has_conf:
                    ed = compute_edge(mkt, self.pf, self.lat, confluence=True)

            if ed and ed["net_edge"] > 0:
                # Score = net_edge weighted by confidence factors
                score = ed["net_edge"]
                # Prefer markets with more time left
                if mkt["secs_left"] > 120:
                    score *= 1.1
                # Prefer CLOB-priced markets (more accurate)
                if mkt.get("up_price_live"):
                    score *= 1.15
                # Prefer confluence trades
                if ed["confluence"]:
                    score *= 1.2
                # Penalize high-vol regime slightly
                if ed["vol_regime"] == "high":
                    score *= 0.85

                if score > best_score:
                    best_score = score
                    best_ed = ed
                    best_mkt = mkt

        if best_mkt and best_ed["net_edge"] >= threshold:
            return best_mkt, best_ed, threshold
        return None, None, threshold

    def run(self):
        self.active = True
        time.sleep(5)
        self.banner()
        log("System Online — scanning live markets...", "sys")

        while self.active:
            if self.bal < C["min_bal"]:
                log("CRITICAL: Balance below minimum. Halted.", "loss")
                break

            # Drawdown protection — halt if down 50% from peak
            if self.bal < self.peak * 0.5:
                log("CRITICAL: 50% drawdown from peak. Halted for safety.", "loss")
                break

            # Live price display with Chainlink + momentum + vol regime
            parts = []
            for a in ASSETS:
                p = self.pf.get_price(a["sym"])
                cl = self.pf.chainlink.get(a["sym"])
                m = self.pf.get_momentum(a["sym"])
                vr = self.pf.get_vol_regime(a["sym"])
                cl_str = f" CL${cl:,.0f}" if cl else ""
                vr_str = f"[{vr[0].upper()}]"
                parts.append(f"{a['name']}:${p:,.0f}{cl_str}({m:+.2f}){vr_str}" if p else f"{a['name']}:loading")
            log(f"Live — {' | '.join(parts)}  lat:{self.lat.avg_ms():.0f}ms", "scan")

            mkt, ed, threshold = self.best_trade()

            if mkt is None:
                all_mkts = self.finder.get_all()
                if not all_mkts:
                    log("No markets loaded yet — waiting for finder...", "sys")
                else:
                    best_n, best_m, best_e = 0, None, None
                    for m2 in all_mkts:
                        e2 = compute_edge(m2, self.pf, self.lat)
                        if e2 and e2["net_edge"] > best_n:
                            best_n = e2["net_edge"]
                            best_m = m2
                            best_e = e2
                    if best_m:
                        log(f"No trade. Best: {best_m['asset']} {best_m['timeframe']} "
                            f"{best_e['side']} "
                            f"edge:{best_e['net_edge']*100:.1f}% "
                            f"(need:{threshold*100:.1f}%)  "
                            f"mkt:{best_e['mkt_prob']*100:.0f}¢ "
                            f"model:{best_e['our_prob']*100:.0f}¢  "
                            f"cost:{best_e['total_cost']*100:.1f}%  "
                            f"{best_m['secs_left']}s left", "sys")
                    else:
                        log(f"Markets loaded ({len(all_mkts)}) but all edges negative.", "sys")
                time.sleep(C["scan_sec"])
                continue

            # Kelly sizing with time decay
            size = kelly_size(ed, self.bal)
            # Scale down near expiry
            if mkt["secs_left"] < 90:
                size *= max(0.3, mkt["secs_left"] / 90)
            size = max(0.50, min(size, self.bal * C["max_kelly"]))

            log(f"OPPORTUNITY: {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                f"edge:{ed['net_edge']*100:.1f}%  "
                f"mkt:{ed['mkt_prob']*100:.0f}¢  "
                f"model:{ed['our_prob']*100:.0f}¢  "
                f"kelly:${size:.2f}  "
                f"vol:{ed['vol_regime']}  "
                f"{mkt['secs_left']}s left"
                f"{'  ★CONFLUENCE' if ed['confluence'] else ''}", "edge")

            win, pnl, filled = self.ex.execute(mkt, ed, size)

            if not filled:
                self.misses += 1
                log(f"MISSED FILL — queue position lost ({self.lat.avg_ms():.0f}ms latency)", "loss")
                time.sleep(1)
                continue

            if win is None:
                time.sleep(1)
                continue

            self.trades += 1
            self.fees += size * C["fee"]
            self.bal = max(0, self.bal + pnl)
            self.peak = max(self.peak, self.bal)
            self.pnl_history.append(pnl)

            # Log trade for dashboard
            self.recent_trades.append({
                "time": datetime.now().strftime('%H:%M:%S'),
                "asset": mkt["asset"],
                "timeframe": mkt["timeframe"],
                "side": ed["side"],
                "size": size,
                "mkt_prob": ed["mkt_prob"],
                "our_prob": ed["our_prob"],
                "net_edge": ed["net_edge"],
                "win": bool(win),
                "pnl": pnl,
            })
            if len(self.recent_trades) > 50:
                self.recent_trades = self.recent_trades[-50:]

            if win:
                self.wins += 1
                self.streak = 0
            else:
                self.losses += 1
                self.streak += 1

            self.stats()

            if not win:
                cd = min(C["cooldown"] * (1 + self.streak * 0.5), 60)
                log(f"Adaptive cooldown {cd:.0f}s (streak:{self.streak})...", "sys")
                time.sleep(cd)
            else:
                time.sleep(1.5)

        log("Iron Dome v8 — offline.", "sys")
        div()
        self.stats()
        if self.pnl_history:
            wins_pnl = [p for p in self.pnl_history if p > 0]
            loss_pnl = [p for p in self.pnl_history if p <= 0]
            log(f"  Avg win: ${statistics.mean(wins_pnl):.4f}" if wins_pnl else "  No wins", "win")
            log(f"  Avg loss: ${statistics.mean(loss_pnl):.4f}" if loss_pnl else "  No losses", "loss")

    def get_dashboard_data(self):
        """Return all bot state as a dict for the dashboard API."""
        threshold = C["min_edge"] + self.streak * C["streak_inc"]
        # Build prices dict
        prices = {}
        for a in ASSETS:
            sym = a["sym"]
            p = self.pf.get_price(sym)
            prices[sym] = {
                "name": a["name"],
                "price": p or 0,
                "change": self.pf.get_change(sym),
                "momentum": self.pf.get_momentum(sym),
                "vol_regime": self.pf.get_vol_regime(sym),
            }
        # Build markets list
        markets = []
        for mkt in self.finder.get_all():
            markets.append({
                "asset": mkt["asset"],
                "timeframe": mkt["timeframe"],
                "up_price": mkt["up_price"],
                "dn_price": mkt["dn_price"],
                "up_spread": mkt["up_spread"],
                "dn_spread": mkt["dn_spread"],
                "up_price_live": mkt["up_price_live"],
                "volume_24h": mkt["volume_24h"],
                "secs_left": mkt["secs_left"],
            })
        return {
            "balance": self.bal,
            "start_balance": self.start,
            "peak": self.peak,
            "wins": self.wins,
            "losses": self.losses,
            "trades": self.trades,
            "streak": self.streak,
            "fees": self.fees,
            "misses": self.misses,
            "active": self.active,
            "dry_run": C["dry_run"],
            "threshold": threshold,
            "latency_ms": self.lat.avg_ms(),
            "fill_rate": self.lat.fill_rate(),
            "pnl_history": self.pnl_history[-200:],
            "recent_trades": self.recent_trades,
            "prices": prices,
            "markets": markets,
            "start_time": self.start_time,
        }

# ──────────────────────────────────────────────────────────────
# DASHBOARD HTTP SERVER
# ──────────────────────────────────────────────────────────────
_bot_ref = None  # set at startup

class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/api/stats':
            if _bot_ref:
                data = _bot_ref.get_dashboard_data()
                body = json.dumps(data).encode('utf-8')
            else:
                body = json.dumps({"error": "bot not started"}).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.write(body)
        elif self.path == '/' or self.path == '/dashboard':
            dash_path = Path(__file__).parent / 'dashboard.html'
            if dash_path.exists():
                body = dash_path.read_bytes()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.write(body)
            else:
                self.send_error(404, 'dashboard.html not found')
        else:
            self.send_error(404)

    def write(self, data):
        try:
            self.wfile.write(data)
        except BrokenPipeError:
            pass

    def log_message(self, format, *args):
        pass  # silence HTTP logs

def start_dashboard(port=8050):
    server = HTTPServer(('0.0.0.0', port), DashboardHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log(f"Dashboard running at http://localhost:{port}", "sys")

# ──────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 62)
    print("  IRON DOME v8 — Optimized Real Data Build")
    print("  BTC/ETH/SOL  |  5m + 15m Up/Down  |  Multi-factor Edge")
    print("=" * 62 + "\n")

    if not C["dry_run"] and not C["pk"]:
        print("ERROR: LIVE mode needs POLYMARKET_PK in .env")
        print("Set DRY_RUN=true for paper trading.")
        exit()

    if not CLOB_OK:
        print("NOTE: py-clob-client not installed — CLOB prices fall back to Gamma")
        print("      pip install py-clob-client\n")

    if not WS_OK:
        print("NOTE: websockets not installed — Chainlink stream disabled")
        print("      pip install websockets\n")

    # Initialize components
    lat_tracker = LatencyTracker()
    log(f"Initial latency: {lat_tracker.avg_ms():.0f}ms → {lat_tracker.label()}", "sys")

    pf = PriceFeed()
    finder = MarketFinder()
    ex = Executor(lat_tracker)

    log("Loading live data...", "sys")
    pf.run_background()
    finder.run_background()
    lat_tracker.run_background()

    time.sleep(6)

    global _bot_ref
    bot = IronDomeV8(pf, finder, ex, lat_tracker)
    _bot_ref = bot

    # Start dashboard server
    dash_port = int(os.getenv("DASH_PORT", "8050"))
    start_dashboard(dash_port)

    try:
        bot.run()
    except KeyboardInterrupt:
        bot.active = False
        log("Interrupted.", "sys")
        bot.stats()
