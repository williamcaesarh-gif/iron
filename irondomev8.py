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
    "fee_rate":   0.018,          # Base fee rate for crypto (1.80%)
    "min_edge":   0.005,          # 0.5% minimum net edge (costs already subtracted)
    "streak_inc": 0.002,          # +0.2% threshold per loss streak
    "min_bal":    1.50,
    "cooldown":   10,             # base cooldown (adaptive: * streak)
    "scan_sec":   2,              # fast scan cycle (was 4s, need speed for frontrunning)
    "max_kelly":  0.10,           # cap Kelly at 10% of bankroll
    "min_kelly":  0.05,           # floor Kelly at 5%
    "max_bet":    35.0,
    "min_volume": 50,             # skip markets with < $50 24h volume
    "stale_secs": 120,            # skip markets not refreshed within 120s
    "stop_loss":  0.06,           # sell early if asset moves 0.06% against us
    "take_profit": 0.04,          # sell early if token value rises enough to cover 2x fees
    "exit_check_interval": 2,     # check positions every 2 seconds
    "ob_depth":   20,             # order book depth to fetch for fill sim
    "competitor_speed_ms": 15,    # fast bots operate at ~15ms
    "competitor_count":    8,     # ~8 serious bots competing on crypto markets
    "max_trades_per_hour": 6,    # realistic cap: ~6 fills/hour at our latency
    "pk":         os.getenv("POLYMARKET_PK", ""),
    "funder":     os.getenv("POLYMARKET_FUNDER", ""),
    "gamma":      "https://gamma-api.polymarket.com",
    "clob_host":  "https://clob.polymarket.com",
    "rtds_url":   "wss://ws-live-data.polymarket.com",
    "chain_id":   137,
}

STATE_FILE = Path(__file__).parent / "bot_state.json"

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
# DYNAMIC FEE CURVE — matches Polymarket's actual fee schedule
# Fee peaks at p=0.50 (50c), drops to near-zero at extremes
# Formula: fee = rate * 2 * p * (1 - p)
# At 50c: fee = 0.018 * 2 * 0.5 * 0.5 = 0.9%
# At 80c: fee = 0.018 * 2 * 0.8 * 0.2 = 0.576%
# At 10c: fee = 0.018 * 2 * 0.1 * 0.9 = 0.324%
# ──────────────────────────────────────────────────────────────
def dynamic_fee(price):
    """Calculate actual Polymarket fee for a given token price."""
    p = max(0.01, min(0.99, price))
    return C["fee_rate"] * 2 * p * (1 - p)

# ──────────────────────────────────────────────────────────────
# ADAPTIVE LEARNING — tracks per-segment win rates and adjusts edge
# ──────────────────────────────────────────────────────────────
class AdaptiveLearner:
    """Learns from trade outcomes per asset/timeframe/side.
    Segments that lose more get higher edge requirements.
    Segments that win get slightly lower requirements (reward)."""

    def __init__(self):
        self._history = {}   # key: "BTC_5m_UP" -> list of (win: bool, net_edge: float)
        self._lock = threading.Lock()

    def _key(self, asset, tf, side):
        return f"{asset}_{tf}_{side}"

    def record(self, asset, tf, side, won, net_edge, momentum, vol_regime):
        k = self._key(asset, tf, side)
        with self._lock:
            if k not in self._history:
                self._history[k] = []
            self._history[k].append({
                "won": won, "edge": net_edge, "mom": momentum,
                "vol": vol_regime, "ts": time.time()
            })
            # Keep last 100 trades per segment
            if len(self._history[k]) > 100:
                self._history[k] = self._history[k][-100:]

    def edge_adjustment(self, asset, tf, side):
        """Returns extra edge required (positive) or discount (negative)
        based on historical performance of this segment."""
        k = self._key(asset, tf, side)
        with self._lock:
            trades = self._history.get(k, [])
        if len(trades) < 3:
            return 0.0  # not enough data
        # Use recent trades (exponential weighting toward recent)
        recent = trades[-20:]  # last 20 trades in this segment
        wins = sum(1 for t in recent if t["won"])
        wr = wins / len(recent)
        # Neutral at 55% win rate (our target)
        # Below 45% → penalize up to +2%
        # Above 60% → reward up to -0.5%
        if wr < 0.45:
            return 0.02 * (0.45 - wr) / 0.45  # max +2% penalty at 0% WR
        elif wr > 0.60:
            return -0.005  # small reward for proven segments
        return 0.0

    def get_stats(self):
        """Return learning stats for dashboard."""
        with self._lock:
            stats = {}
            for k, trades in self._history.items():
                recent = trades[-20:]
                wins = sum(1 for t in recent if t["won"])
                stats[k] = {
                    "total": len(trades),
                    "recent": len(recent),
                    "win_rate": round(wins / len(recent) * 100) if recent else 0,
                    "adj": round(self.edge_adjustment(*k.split("_")) * 100, 2),
                }
            return stats

LEARNER = AdaptiveLearner()

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
        self.prices    = {}   # sym → float (primary: Binance)
        self.changes   = {}   # sym → 24h %
        self.candles   = {}   # sym → [close, close, ...]  last 15 x 1m
        self.chainlink = {}   # sym → float (from RTDS websocket)
        self.index     = {}   # sym → weighted multi-exchange price (settlement proxy)
        self.exchange_prices = {}  # sym → {exchange: price} for index calculation
        self.ema_fast  = {}   # sym → EMA-3
        self.ema_slow  = {}   # sym → EMA-8
        self.volatility = {}  # sym → rolling stddev of 1m returns
        self.source    = "loading"
        self._lock     = threading.Lock()
        self._pool     = ThreadPoolExecutor(max_workers=9)  # more workers for parallel exchange fetches

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
        """Fetch ALL exchanges in parallel, build settlement index."""
        futures = {}
        # Submit ALL sources for ALL assets simultaneously
        for a in ASSETS:
            sym = a["sym"]
            futures[self._pool.submit(self._bnb_fetch, sym)] = (a, "binance")
            futures[self._pool.submit(self._cb_fetch, sym)] = (a, "coinbase")
            futures[self._pool.submit(self._cg_fetch, sym)] = (a, "coingecko")

        # Collect all exchange prices
        new_exchange_prices = {a["sym"]: {} for a in ASSETS}
        primary_set = set()

        for future in as_completed(futures, timeout=15):
            a, exchange = futures[future]
            sym = a["sym"]
            try:
                result = future.result()
                if result:
                    src, _, price, chg, candles = result
                    new_exchange_prices[sym][exchange] = price

                    # Use Binance as primary (has candles + 24h change)
                    if exchange == "binance" and sym not in primary_set:
                        with self._lock:
                            self.prices[sym] = price
                            self.changes[sym] = chg
                            if candles:
                                self.candles[sym] = candles
                                self._update_indicators(sym, candles)
                            self.source = src
                        primary_set.add(sym)
            except Exception:
                pass

        # Fallback: if Binance failed, use any available source as primary
        for a in ASSETS:
            sym = a["sym"]
            if sym not in primary_set:
                for ex_name in ["coinbase", "coingecko"]:
                    if ex_name in new_exchange_prices[sym]:
                        with self._lock:
                            self.prices[sym] = new_exchange_prices[sym][ex_name]
                            self.source = ex_name.title()
                        primary_set.add(sym)
                        break

        # Build weighted settlement index (mimics Polymarket's multi-exchange oracle)
        # Weights: Binance 40%, Coinbase 35%, CoinGecko 25%
        INDEX_WEIGHTS = {"binance": 0.40, "coinbase": 0.35, "coingecko": 0.25}
        with self._lock:
            self.exchange_prices = new_exchange_prices
            for a in ASSETS:
                sym = a["sym"]
                ep = new_exchange_prices[sym]
                if ep:
                    total_weight = sum(INDEX_WEIGHTS.get(k, 0) for k in ep)
                    if total_weight > 0:
                        weighted_price = sum(ep[k] * INDEX_WEIGHTS.get(k, 0) for k in ep) / total_weight
                        self.index[sym] = weighted_price

        # Log summary
        for a in ASSETS:
            sym = a["sym"]
            p = self.prices.get(sym)
            idx = self.index.get(sym)
            ep = new_exchange_prices[sym]
            if p:
                sources = "/".join(ep.keys())
                idx_str = f"  idx:${idx:,.2f}" if idx else ""
                log(f"{a['name']}: ${p:,.2f}{idx_str}  {self.changes.get(sym, 0):+.2f}%  [{sources}]", "live")

    def get_price(self, sym):
        """Primary price: Chainlink > Binance."""
        with self._lock:
            return self.chainlink.get(sym) or self.prices.get(sym)

    def get_index_price(self, sym):
        """Settlement index: weighted multi-exchange price (closest to Polymarket oracle)."""
        with self._lock:
            return self.index.get(sym) or self.chainlink.get(sym) or self.prices.get(sym)

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

    def start_binance_ws(self):
        """Binance WebSocket for real-time BTC/ETH/SOL prices (tick-by-tick).
        Replaces 12s REST polling with sub-second updates — critical for the
        sniper since the edge depends on detecting moves before Polymarket's
        oracle catches up."""
        if not WS_OK:
            log("websockets not installed — Binance WS disabled (REST fallback)", "warn")
            return

        streams = "/".join(f"{s.lower()}@miniTicker" for s in ["btcusdt", "ethusdt", "solusdt"])
        ws_url = f"wss://stream.binance.com:9443/stream?streams={streams}"

        async def _run():
            while True:
                try:
                    async with websockets.connect(ws_url, ping_interval=20) as ws:
                        log("Binance WebSocket connected (real-time prices)", "live")
                        while True:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                            if not raw:
                                continue
                            try:
                                msg = json.loads(raw)
                            except (json.JSONDecodeError, ValueError):
                                continue
                            data = msg.get("data", {})
                            sym = data.get("s")  # e.g. "BTCUSDT"
                            close = data.get("c")  # current close price
                            if sym and close:
                                with self._lock:
                                    self.prices[sym] = float(close)
                except (asyncio.TimeoutError, ConnectionError):
                    log("Binance WS timeout/disconnect — reconnecting in 3s", "warn")
                    await asyncio.sleep(3)
                except Exception as e:
                    log(f"Binance WS error: {e} — reconnecting in 5s", "warn")
                    await asyncio.sleep(5)

        def _thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(_run())
            finally:
                loop.close()

        threading.Thread(target=_thread, daemon=True).start()

    def run_background(self):
        self.fetch_all()
        self.start_rtds()
        self.start_binance_ws()
        # PTB snapshots: capture Chainlink price at exact window boundaries.
        # Polymarket's PTB = Chainlink price at the second the window starts.
        # We pre-schedule captures at every 5m and 15m boundary so our PTB
        # matches what the oracle will publish later.
        self.ptb_snapshots = {}  # (sym, tf_secs, window_start) → price
        def _ptb_capture_loop():
            while True:
                now = time.time()
                # Next 5m boundary (also covers 15m since 900 is multiple of 300)
                next_5m = (int(now) // 300 + 1) * 300
                wait = next_5m - now
                if wait > 0:
                    time.sleep(wait)
                snap_time = int(time.time())
                # Retry briefly in case Chainlink WS hasn't ticked the boundary yet
                for _ in range(4):
                    captured = False
                    with self._lock:
                        for asset in ASSETS:
                            sym = asset["sym"]
                            cl = self.chainlink.get(sym)
                            if cl:
                                for tf in TFS:
                                    if snap_time % tf["secs"] == 0:
                                        key = (sym, tf["secs"], snap_time)
                                        if key not in self.ptb_snapshots:
                                            self.ptb_snapshots[key] = cl
                                            log(f"  [PTB] Boundary snapshot {asset['name']} {tf['label']} "
                                                f"→ ${cl:,.2f}", "sys")
                                        captured = True
                    if captured:
                        break
                    time.sleep(0.25)
                # Purge >30 min old snapshots
                cutoff = snap_time - 1800
                stale = [k for k in self.ptb_snapshots if k[2] < cutoff]
                for k in stale:
                    del self.ptb_snapshots[k]
        threading.Thread(target=_ptb_capture_loop, daemon=True).start()
        def loop():
            while True:
                time.sleep(12)  # slightly faster refresh
                self.fetch_all()
        threading.Thread(target=loop, daemon=True).start()
        log("Price feed: Binance WS (real-time) + Chainlink RTDS WS + REST fallback + PTB boundary capture", "sys")

# ──────────────────────────────────────────────────────────────
# DETERMINISTIC MARKET DISCOVERY — parallel scanning
# ──────────────────────────────────────────────────────────────
class MarketFinder:
    def __init__(self, pf=None):
        self.markets = {}   # (asset, tf) → market dict
        self._lock = threading.Lock()
        self._clob = ClobClient(C["clob_host"]) if CLOB_OK else None
        self._pool = ThreadPoolExecutor(max_workers=6)
        self._pf = pf  # PriceFeed reference for recording window start prices
        self._price_to_beat = {}  # (asset, tf, window_start) → asset price at window start
        self._last_window = {}    # (asset_name, tf_label) → last seen window_start timestamp

    def _window_ts(self, tf_secs):
        now = int(time.time())
        return now - (now % tf_secs)

    def _fetch_market(self, asset, tf):
        ts_cur = self._window_ts(tf["secs"])
        slug = f"{asset['slug_pfx']}-updown-{tf['label']}-{ts_cur}"
        data = None

        # ONLY fetch the current window's slug. The previous version had a
        # fallback chain [ts_cur, ts_next, ts_prev] which silently corrupted
        # PTB and clobTokenIds whenever Gamma was momentarily slow on the
        # current slug — we'd end up trading the wrong window's tokens with
        # a stale PTB. Better to skip this refresh and try again next cycle.
        try:
            r = requests.get(f"{C['gamma']}/events",
                             params={"slug": slug}, timeout=8)
            r.raise_for_status()
            events = r.json()
            if isinstance(events, list) and events:
                data = events[0]
            elif isinstance(events, dict) and events:
                data = events
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

        # Enrich with live CLOB bid/ask + order book depth
        # Uses direct REST API — no auth needed for reading prices/books
        def clob_data(token_id):
            """Fetch bid/ask + order book for realistic fill simulation."""
            info = {"bid": None, "ask": None, "mid": None, "book_asks": [], "book_bids": []}
            if not token_id:
                return info
            try:
                book_url = f"{C['clob_host']}/book?token_id={token_id}"
                resp = requests.get(book_url, timeout=5)
                if resp.status_code == 200:
                    ob = resp.json()
                    raw_asks = ob.get("asks", [])
                    raw_bids = ob.get("bids", [])
                    if raw_asks:
                        info["book_asks"] = [{"price": float(a["price"]), "size": float(a["size"])}
                                             for a in raw_asks[:C["ob_depth"]]]
                    if raw_bids:
                        info["book_bids"] = [{"price": float(b["price"]), "size": float(b["size"])}
                                             for b in raw_bids[:C["ob_depth"]]]
                    if raw_bids and raw_asks:
                        info["bid"] = float(raw_bids[0]["price"])
                        info["ask"] = float(raw_asks[0]["price"])
                        info["mid"] = (info["bid"] + info["ask"]) / 2
            except Exception as e:
                log(f"  CLOB book fetch failed for {str(token_id)[:14]}...: {e}", "warn")
            return info

        up_clob = clob_data(up_yes_id)
        dn_clob = clob_data(down_yes_id)

        vol = float(data.get("volume24hr") or data.get("volumeClob") or 0)
        window_start = ts_cur
        window_end = ts_cur + tf["secs"]
        secs_left = max(0, window_end - int(time.time()))

        # ── PTB resolution from Polymarket's oracle (Chainlink boundary price) ──
        # Verified against Gamma 2026-04-07: finalPrice[N] == priceToBeat[N+1]
        # for every BTC/ETH/SOL window. Two valid sources during an active window:
        #   1. THIS event's eventMetadata.priceToBeat (usually populated immediately
        #      at window start — most reliable).
        #   2. The PREVIOUS window's eventMetadata.finalPrice (definitionally equal
        #      to our priceToBeat, but only available once the prior window settled).
        # If neither is available we leave ptb_oracle=None and let refresh() fall
        # back to our Chainlink boundary snapshot.
        ptb_oracle = None
        meta = data.get("eventMetadata") or {}
        if meta.get("priceToBeat") is not None:
            try:
                ptb_oracle = float(meta["priceToBeat"])
            except (ValueError, TypeError):
                pass

        if ptb_oracle is None:
            ts_prev = ts_cur - tf["secs"]
            prev_slug = f"{asset['slug_pfx']}-updown-{tf['label']}-{ts_prev}"
            try:
                rp = requests.get(f"{C['gamma']}/events",
                                  params={"slug": prev_slug}, timeout=5)
                if rp.ok:
                    prev_events = rp.json()
                    if isinstance(prev_events, list) and prev_events:
                        prev_meta = prev_events[0].get("eventMetadata") or {}
                        fp = prev_meta.get("finalPrice")
                        if fp is not None:
                            ptb_oracle = float(fp)
            except Exception:
                pass

        return {
            "asset":      asset["name"],
            "timeframe":  tf["label"],
            "slug":       slug,
            "window_start": window_start,
            "window_end": window_end,
            "secs_left":  secs_left,
            # UP side
            "up_price":      up_clob["mid"] if up_clob["mid"] is not None else up_gp,
            "up_bid":        up_clob["bid"],
            "up_ask":        up_clob["ask"],
            "up_price_live": up_clob["mid"],
            "up_yes_token":  up_yes_id,
            "up_no_token":   up_no_id,
            "up_book_asks":  up_clob["book_asks"],
            "up_book_bids":  up_clob["book_bids"],
            # DOWN side
            "dn_price":      dn_clob["mid"] if dn_clob["mid"] is not None else down_gp,
            "dn_bid":        dn_clob["bid"],
            "dn_ask":        dn_clob["ask"],
            "dn_price_live": dn_clob["mid"],
            "dn_yes_token":  down_yes_id,
            "dn_no_token":   down_no_id,
            "dn_book_asks":  dn_clob["book_asks"],
            "dn_book_bids":  dn_clob["book_bids"],
            "volume_24h": vol,
            "fetched_at": time.time(),
            # Spread info
            "up_spread":  (up_clob["ask"] - up_clob["bid"]) if up_clob["bid"] and up_clob["ask"] else 1.0,
            "dn_spread":  (dn_clob["ask"] - dn_clob["bid"]) if dn_clob["bid"] and dn_clob["ask"] else 1.0,
            # Gamma prices (from recent trades, useful near expiry when CLOB is dust)
            "up_gamma": up_gp,
            "dn_gamma": down_gp,
            # Oracle price_to_beat from Polymarket (Chainlink snapshot at window start)
            "price_to_beat": ptb_oracle,
        }

    def refresh(self):
        # Purge stale PTB entries (older than 30 min)
        cutoff = int(time.time()) - 1800
        stale = [k for k in self._price_to_beat if k[2] < cutoff]
        for k in stale:
            del self._price_to_beat[k]

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
                    # ── PTB resolution priorities (verified against Gamma 2026-04-07) ──
                    # A. Gamma eventMetadata.priceToBeat — GROUND TRUTH. Usually
                    #    populated immediately at window start. Always wins.
                    # B. Previous window's eventMetadata.finalPrice — definitionally
                    #    equal to current PTB (verified: finalPrice[N] == priceToBeat[N+1]).
                    #    Both A and B are read inside _fetch_market.
                    # C. Our Chainlink boundary snapshot — captured by the PTB loop
                    #    at the exact window-start second. Used only if Gamma is silent.
                    # D. Current Chainlink WS price — last-resort estimate (slightly
                    #    late, used only if the boundary capture missed).
                    ptb_key = (mkt["asset"], mkt["timeframe"], mkt["window_start"])
                    wnd_key = (mkt["asset"], mkt["timeframe"])
                    prev_ws = self._last_window.get(wnd_key)
                    self._last_window[wnd_key] = mkt["window_start"]

                    # Detect new window (or first time seeing this window) → snapshot Chainlink price as PTB
                    if (prev_ws is None or mkt["window_start"] != prev_ws) and ptb_key not in self._price_to_beat:
                        _sym = next((a["sym"] for a in ASSETS if a["name"] == mkt["asset"]), None)
                        _tf_secs = next((t["secs"] for t in TFS if t["label"] == mkt["timeframe"]), None)
                        if _sym and self._pf and _tf_secs:
                            # PRIORITY 1: pre-captured boundary snapshot (matches oracle exactly)
                            boundary_key = (_sym, _tf_secs, mkt["window_start"])
                            boundary_price = getattr(self._pf, 'ptb_snapshots', {}).get(boundary_key)
                            if boundary_price:
                                self._price_to_beat[ptb_key] = boundary_price
                                log(f"  [PTB] Boundary {mkt['asset']} {mkt['timeframe']} "
                                    f"→ ${boundary_price:,.2f} (exact window start)", "sys")
                            else:
                                # PRIORITY 2: current Chainlink price (close-but-not-exact, for late starts)
                                snap_price = self._pf.chainlink.get(_sym) or self._pf.prices.get(_sym)
                                snap_src = "chainlink" if self._pf.chainlink.get(_sym) else "binance"
                                if snap_price:
                                    self._price_to_beat[ptb_key] = snap_price
                                    log(f"  [PTB] Snapshot {mkt['asset']} {mkt['timeframe']} "
                                        f"new window → {snap_src} ${snap_price:,.2f} (late, no boundary)", "sys")

                    ptb_src = "none"
                    if mkt.get("price_to_beat"):
                        # Oracle PTB from Gamma — always overrides our snapshot
                        self._price_to_beat[ptb_key] = mkt["price_to_beat"]
                        ptb_src = "oracle"
                    elif ptb_key in self._price_to_beat:
                        mkt["price_to_beat"] = self._price_to_beat[ptb_key]
                        ptb_src = "chainlink"
                    up_src = "CLOB" if mkt["up_price_live"] else "Gamma"
                    dn_src = "CLOB" if mkt.get("dn_price_live") else "Gamma"
                    ptb_val = mkt.get("price_to_beat")
                    ptb_info = f"  PTB:${ptb_val:,.2f}[{ptb_src}]" if ptb_val else "  PTB:none"

                    # Compute implied price from asset price vs PTB for display
                    up_show = mkt['up_price']
                    dn_show = mkt['dn_price']
                    impl_src = None
                    if ptb_val and self._pf:
                        _sym = next((a["sym"] for a in ASSETS if a["name"] == mkt["asset"]), None)
                        if _sym:
                            _cur = self._pf.prices.get(_sym) or self._pf.chainlink.get(_sym)
                            if _cur and ptb_val > 0:
                                _delta = (_cur - ptb_val) / ptb_val
                                _abs_d = abs(_delta)
                                _sl = max(1, mkt['secs_left'])
                                _tr = max(0.0, min(1.0, (145 - _sl) / 110.0)) if _sl <= 145 else 0.0
                                _sens = 40.0 * math.exp(2.5 * _tr)
                                _prob = max(0.05, min(0.97, 0.50 + _abs_d * _sens))
                                if _delta > 0:
                                    up_show, dn_show = _prob, 1.0 - _prob
                                elif _delta < 0:
                                    up_show, dn_show = 1.0 - _prob, _prob
                                else:
                                    up_show, dn_show = 0.50, 0.50
                                impl_src = "impl"

                    show_up_src = impl_src or up_src
                    show_dn_src = impl_src or dn_src
                    log(f"  [{mkt['asset']} {mkt['timeframe']}] "
                        f"UP:{up_show*100:.0f}¢[{show_up_src}] "
                        f"DN:{dn_show*100:.0f}¢[{show_dn_src}]  "
                        f"vol:${mkt['volume_24h']:,.0f}  "
                        f"ends:{mkt['secs_left']}s{ptb_info}", "scan")
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
                # Attach price_to_beat if we have it
                ptb_key = (m["asset"], m["timeframe"], m["window_start"])
                if ptb_key in self._price_to_beat:
                    m["price_to_beat"] = self._price_to_beat[ptb_key]
                out.append(m)
            return out

    def run_background(self):
        def loop():
            while True:
                log("Scanning markets (parallel)...", "sys")
                self.refresh()
                time.sleep(10)  # refresh every 10s — books move fast on 5m markets
        threading.Thread(target=loop, daemon=True).start()
        log("Market finder started (deterministic slug, parallel, 45s refresh)", "sys")

# ──────────────────────────────────────────────────────────────
# ORDER BOOK FILL SIMULATOR
# Walks the order book to determine realistic fill price + whether
# enough liquidity exists for our order size.
# ──────────────────────────────────────────────────────────────
def simulate_fill(book_asks, size_usd):
    """
    Simulate buying `size_usd` worth of shares by walking the ask book.
    Returns (avg_fill_price, filled_usd, slippage_pct) or None if no liquidity.
    book_asks = [{"price": 0.52, "size": 100}, ...] sorted price asc
    """
    if not book_asks:
        return None

    total_spent = 0.0
    total_shares = 0.0
    remaining = size_usd

    for level in book_asks:
        price = level["price"]
        available_usd = level["size"] * price
        if remaining <= available_usd:
            shares_here = remaining / price
            total_shares += shares_here
            total_spent += remaining
            remaining = 0
            break
        else:
            total_shares += level["size"]
            total_spent += available_usd
            remaining -= available_usd

    if total_shares == 0:
        return None

    avg_price = total_spent / total_shares
    best_ask = book_asks[0]["price"]
    slippage = (avg_price - best_ask) / best_ask if best_ask > 0 else 0
    filled = total_spent

    return {
        "avg_price": avg_price,
        "filled_usd": filled,
        "unfilled_usd": remaining,
        "shares": total_shares,
        "slippage_pct": slippage,
        "levels_consumed": sum(1 for _ in book_asks if total_spent > 0),
    }


def check_oracle_settlement(token_id, side=None):
    """
    Check if a token has been settled by Polymarket's oracle.
    Uses Gamma API: resolved/winner, outcomePrices, AND PTB vs finalPrice.
    Returns: (won: bool|None, ptb: float|None)
      - won: True (won), False (lost), or None (not settled yet)
      - ptb: price_to_beat from oracle (if available)
    """
    if not token_id:
        return None, None
    try:
        r = requests.get(f"{C['gamma']}/markets",
                        params={"clob_token_id": token_id}, timeout=5)
        if r.ok:
            markets = r.json()
            if isinstance(markets, list) and markets:
                m = markets[0]

                # Extract PTB and finalPrice from event metadata
                ptb = None
                final_price = None
                event = m.get("eventMetadata") or {}
                if event.get("priceToBeat") is not None:
                    try:
                        ptb = float(event["priceToBeat"])
                    except (ValueError, TypeError):
                        pass
                if event.get("finalPrice") is not None:
                    try:
                        final_price = float(event["finalPrice"])
                    except (ValueError, TypeError):
                        pass

                # Method 1: Explicit resolved + winner
                if m.get("resolved"):
                    winner = m.get("winner")
                    if winner is not None:
                        return str(winner) == str(token_id), ptb

                # Method 2: outcomePrices near 0/1
                raw = m.get("outcomePrices")
                if raw:
                    prices = json.loads(raw) if isinstance(raw, str) else raw
                    if len(prices) >= 2:
                        yes_price = float(prices[0])
                        no_price = float(prices[1])
                        if yes_price >= 0.95 and no_price <= 0.05:
                            return True, ptb
                        elif yes_price <= 0.05 and no_price >= 0.95:
                            return False, ptb

                # Method 3: PTB vs finalPrice (Gamma has both but resolved=None)
                # If we know the side (UP/DOWN), we can determine winner directly
                if ptb is not None and final_price is not None and side:
                    price_went_up = final_price > ptb
                    if side == "UP":
                        return price_went_up, ptb
                    else:  # DOWN
                        return not price_went_up, ptb
    except Exception:
        pass

    return None, None  # Can't determine yet


def get_live_token_price(clob_client, token_id):
    """Fetch current live bid/ask for a token from CLOB."""
    if not clob_client or not token_id:
        return None, None
    try:
        ask = float(clob_client.get_price(token_id, side="BUY") or 0)
        bid = float(clob_client.get_price(token_id, side="SELL") or 0)
        if ask > 0 and bid > 0:
            return bid, ask
    except Exception:
        pass
    return None, None


def competitor_check(our_latency_ms, edge_size):
    """
    Simulate whether faster bots grabbed the opportunity before us.

    Reality: ~8 institutional bots run at 5-20ms on AWS Virginia.
    They see the same price feeds. The bigger and more obvious the edge,
    the MORE likely a fast bot already took it.

    Returns: (survived: bool, reason: str)
    """
    n_competitors = C["competitor_count"]
    fast_ms = C["competitor_speed_ms"]

    # Time advantage the fast bots have over us (in seconds)
    speed_gap = (our_latency_ms - fast_ms) / 1000

    # Base probability a fast bot grabs the edge before us
    # At 100ms vs 15ms, they have ~85ms head start
    # More competitors = higher chance one of them takes it
    # P(at least one bot beats us) = 1 - P(none beat us)^n
    per_bot_grab = min(0.85, speed_gap * 3)  # ~25% per bot at 85ms gap
    prob_beaten = 1 - (1 - per_bot_grab) ** n_competitors

    # Edge visibility factor: obvious edges (>10%) are seen by everyone
    # Subtle edges (<5%) might be missed by simpler bot strategies
    if edge_size > 0.10:
        visibility = 1.0    # everyone sees it
    elif edge_size > 0.06:
        visibility = 0.7    # most bots see it
    else:
        visibility = 0.4    # only sophisticated bots catch it

    final_prob_beaten = prob_beaten * visibility

    # Roll the dice
    if random.random() < final_prob_beaten:
        return False, f"competitor (prob:{final_prob_beaten*100:.0f}%)"
    return True, "passed"

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

    # Sanity check: market prices should be roughly complementary (sum ~1.0)
    # If they don't add up, the data is stale (Gamma cache vs real CLOB)
    price_sum = mkt_up + mkt_dn
    if price_sum < 0.80 or price_sum > 1.20:
        return None

    # ── Per-side eligibility: CLOB data + spread check ───────
    up_eligible = bool(mkt.get("up_price_live")) and mkt["up_spread"] <= 0.10
    dn_eligible = bool(mkt.get("dn_price_live")) and mkt["dn_spread"] <= 0.10

    # Need at least one tradeable side
    if not up_eligible and not dn_eligible:
        return None

    # ── Costs (dynamic fee based on token price) ─────────────
    up_fee = dynamic_fee(mkt_up)
    dn_fee = dynamic_fee(mkt_dn)
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
    total_cost_up = up_fee + slip + up_spread_cost + time_penalty - conf_bonus
    total_cost_dn = dn_fee + slip + dn_spread_cost + time_penalty - conf_bonus

    up_net = (our_up - mkt_up) - total_cost_up
    our_dn = 1 - our_up
    dn_net = (our_dn - mkt_dn) - total_cost_dn

    # Zero out ineligible sides so we never pick them
    if not up_eligible:
        up_net = -1.0
    if not dn_eligible:
        dn_net = -1.0

    # Skip low-volume markets — thin books, bad fills
    if mkt["volume_24h"] < C["min_volume"]:
        return None

    # Skip stale market data — prices may be outdated
    if time.time() - mkt.get("fetched_at", 0) > C["stale_secs"]:
        return None

    # Skip markets with < 20s left
    if mkt["secs_left"] < 20:
        return None

    # Cap max edge — real edges on 5m/15m binary markets never exceed ~8%
    # If our model says 20%+ edge, the model is wrong, not the market
    MAX_EDGE = 0.08
    up_net = min(up_net, MAX_EDGE)
    dn_net = min(dn_net, MAX_EDGE)

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
    b = (1 / ed["mkt_prob"] - 1) * (1 - dynamic_fee(ed["mkt_prob"]))

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
# POSITION TRACKER — holds open positions, resolves with real prices
# ──────────────────────────────────────────────���───────────────
class Position:
    """A single open position in a binary market."""
    def __init__(self, asset, sym, timeframe, side, token_id, entry_price,
                 size, shares, window_end, entry_asset_price, ed,
                 price_to_beat=None):
        self.asset = asset
        self.sym = sym
        self.timeframe = timeframe
        self.side = side               # "UP" or "DOWN"
        self.token_id = token_id
        self.entry_price = entry_price  # what we paid per share (e.g. 0.50)
        self.size = size                # total $ spent
        self.shares = shares            # size / entry_price
        self.window_end = window_end
        self.entry_asset_price = entry_asset_price  # BTC/ETH/SOL price at entry
        self.price_to_beat = price_to_beat  # asset price at window START (Polymarket's reference)
        self.entry_time = time.time()
        self.ed = ed                    # edge data at entry
        self.resolved = False
        self.result = None              # "win", "loss", "stop_loss", "take_profit"
        self.pnl = 0.0
        self.exit_price = None          # token price at exit
        self.exit_asset_price = None    # asset price at exit
        self.exit_order_id = None       # CLOB sell order ID (live mode auto-exit at 99c)

    def to_dict(self):
        return {
            "asset": self.asset, "sym": self.sym, "timeframe": self.timeframe,
            "side": self.side, "token_id": self.token_id,
            "entry_price": self.entry_price, "size": self.size,
            "shares": self.shares, "window_end": self.window_end,
            "entry_asset_price": self.entry_asset_price,
            "price_to_beat": self.price_to_beat,
            "entry_time": self.entry_time,
            "ed": self.ed,
        }

    @classmethod
    def from_dict(cls, d):
        pos = cls(
            asset=d["asset"], sym=d["sym"], timeframe=d["timeframe"],
            side=d["side"], token_id=d["token_id"],
            entry_price=d["entry_price"], size=d["size"],
            shares=d["shares"], window_end=d["window_end"],
            entry_asset_price=d["entry_asset_price"],
            ed=d.get("ed", {}),
            price_to_beat=d.get("price_to_beat"),
        )
        pos.entry_time = d.get("entry_time", time.time())
        return pos


class PositionTracker:
    """Tracks open positions. Resolves via real CLOB token prices + settlement index."""
    def __init__(self, pf: PriceFeed, lat: LatencyTracker):
        self.pf = pf
        self.lat = lat
        self._clob = ClobClient(C["clob_host"]) if CLOB_OK else None
        self._live_client = None  # set by Executor after auth for cancel/sell
        self.open_positions = []
        self.closed_positions = []
        self._lock = threading.Lock()

    def cancel_exit_order(self, pos):
        """Cancel a position's standing exit order on the CLOB."""
        if not pos.exit_order_id or not self._live_client:
            return
        try:
            self._live_client.cancel(pos.exit_order_id)
            log(f"  [LIVE] Cancelled exit order {pos.exit_order_id[:14]} "
                f"for {pos.asset} {pos.timeframe} {pos.side}", "sys")
        except Exception as e:
            log(f"  [LIVE] Cancel exit order failed: {e}", "warn")

    def add(self, pos: Position):
        with self._lock:
            self.open_positions.append(pos)
        log(f"  [POS] OPENED {pos.asset} {pos.timeframe} {pos.side}  "
            f"entry:{pos.entry_price*100:.1f}c  "
            f"shares:{pos.shares:.2f}  "
            f"size:${pos.size:.2f}  "
            f"asset_price:${pos.entry_asset_price:,.2f}  "
            f"expires:{int(pos.window_end - time.time())}s", "dry")

    def check_positions(self):
        """
        Check all open positions for exit conditions using:
        - Real CLOB token bid/ask for stop-loss/take-profit pricing
        - Multi-exchange settlement index for expiry resolution
        - Dynamic fee curve for accurate cost calculation

        Lock strategy: snapshot positions under lock, do all HTTP work
        outside the lock, then update the list under lock. This prevents
        the dashboard from blocking on slow CLOB/oracle API calls.
        """
        now = time.time()

        # 1. Snapshot current positions (fast, under lock)
        with self._lock:
            positions_to_check = [p for p in self.open_positions if not p.resolved]

        # 2. Process each position WITHOUT holding the lock
        #    (HTTP calls to CLOB/oracle happen here)
        still_open = []
        newly_closed = []

        for pos in positions_to_check:
            if pos.resolved:  # may have been resolved by reversal monitor thread
                newly_closed.append(pos)
                continue

            # Get real asset price — prefer Chainlink (matches Polymarket oracle)
            index_price = self.pf.chainlink.get(pos.sym) or self.pf.get_index_price(pos.sym)
            if not index_price:
                still_open.append(pos)
                continue

            secs_left = pos.window_end - now
            price_change_pct = (index_price - pos.entry_asset_price) / pos.entry_asset_price

            # Determine direction
            if pos.side == "UP":
                favorable = price_change_pct > 0
                move_magnitude = price_change_pct
            else:
                favorable = price_change_pct < 0
                move_magnitude = -price_change_pct

            # Fetch REAL token price from CLOB for exit pricing
            token_bid, token_ask = get_live_token_price(self._clob, pos.token_id)

            # ── STOP LOSS: asset moved against us ──
            if move_magnitude < -C["stop_loss"]:
                # Use real CLOB bid (what we'd actually get selling)
                if token_bid and token_bid > 0.01:
                    sell_price = token_bid
                else:
                    # Fallback: estimate from asset move
                    sell_price = max(0.02, pos.entry_price + move_magnitude * 2)

                buy_fee = dynamic_fee(pos.entry_price) * pos.size
                sell_fee = dynamic_fee(sell_price) * sell_price * pos.shares
                pos.exit_price = sell_price
                pos.exit_asset_price = index_price
                pos.pnl = (sell_price * pos.shares) - pos.size - buy_fee - sell_fee
                pos.result = "stop_loss"
                pos.resolved = True
                self.cancel_exit_order(pos)  # cancel standing 99c sell
                newly_closed.append(pos)
                src = "CLOB" if token_bid else "model"
                log(f"  [POS] STOP LOSS {pos.asset} {pos.timeframe} {pos.side}  "
                    f"asset:{price_change_pct*100:+.3f}%  "
                    f"token:{pos.entry_price*100:.0f}c->{sell_price*100:.0f}c [{src}]  "
                    f"pnl:${pos.pnl:.4f}", "loss")
                continue

            # ── SMART EXIT: momentum-aware take-profit ──
            # If we're in profit, check momentum to decide: sell now or hold to expiry
            if favorable and move_magnitude > C["take_profit"]:
                # Use real CLOB bid for exit pricing
                if token_bid and token_bid > pos.entry_price:
                    sell_price = token_bid
                else:
                    sell_price = min(0.98, pos.entry_price + move_magnitude * 2)

                buy_fee = dynamic_fee(pos.entry_price) * pos.size
                sell_fee = dynamic_fee(sell_price) * sell_price * pos.shares
                potential_pnl = (sell_price * pos.shares) - pos.size - buy_fee - sell_fee

                if potential_pnl > 0:
                    # Read current momentum to decide hold vs sell
                    mom = self.pf.get_momentum(pos.sym)
                    vol_regime = self.pf.get_vol_regime(pos.sym)
                    unrealized_gain = sell_price - pos.entry_price  # how much token has risen

                    # Decision factors:
                    # 1. Is momentum still pushing in our favor?
                    mom_favorable = (mom > 0.1 and pos.side == "UP") or (mom < -0.1 and pos.side == "DOWN")
                    # 2. How much time is left? (more time = more risk of reversal)
                    time_ratio = secs_left / (pos.window_end - pos.entry_time) if (pos.window_end - pos.entry_time) > 0 else 0
                    # 3. How big is the gain already? (bigger gain = more to protect)
                    gain_pct = unrealized_gain / pos.entry_price

                    # SELL NOW if:
                    # - Momentum is fading/reversing (no longer pushing our way)
                    # - OR gain is very large (>30c rise) — protect the profit
                    # - OR high volatility regime — price could snap back
                    # - OR very little time left (<30s) — take the bird in hand
                    should_sell = False
                    sell_reason = ""

                    if not mom_favorable:
                        should_sell = True
                        sell_reason = "momentum fading"
                    elif gain_pct > 0.30:
                        # Huge gain — lock it in regardless
                        should_sell = True
                        sell_reason = f"large gain ({gain_pct*100:.0f}%)"
                    elif vol_regime == "high" and gain_pct > 0.10:
                        # High vol + decent gain — don't risk reversal
                        should_sell = True
                        sell_reason = "high vol + profit"
                    elif secs_left < 30 and gain_pct > 0.05:
                        # Near expiry, small gain — sell to avoid last-second reversal
                        should_sell = True
                        sell_reason = "near expiry"

                    if should_sell:
                        pos.exit_price = sell_price
                        pos.exit_asset_price = index_price
                        pos.pnl = potential_pnl
                        pos.result = "take_profit"
                        pos.resolved = True
                        self.cancel_exit_order(pos)  # cancel standing 99c sell
                        newly_closed.append(pos)
                        src = "CLOB" if token_bid else "model"
                        log(f"  [POS] SMART TP {pos.asset} {pos.timeframe} {pos.side}  "
                            f"SELL ({sell_reason})  "
                            f"token:{pos.entry_price*100:.0f}c->{sell_price*100:.0f}c [{src}]  "
                            f"mom:{mom:+.2f}  vol:{vol_regime}  "
                            f"pnl:${pos.pnl:.4f}", "win")
                        continue
                    else:
                        # HOLD — momentum still strong, let it ride
                        log(f"  [POS] SMART HOLD {pos.asset} {pos.timeframe} {pos.side}  "
                            f"up {gain_pct*100:.1f}% but mom:{mom:+.2f} still strong  "
                            f"{int(secs_left)}s left — riding to expiry", "edge")
                        # Don't exit, fall through to expiry check

            # ── AUTO-EXIT / SELL HIGH ──
            # Polymarket books lock ~2-5s before epoch ends — liquidity vanishes.
            # Strategy: tiered exit based on time remaining.
            #   >5s left:  99c sell order sits in book, fills if outcome clear early
            #   5-8s left: lower exit to 95c — must exit BEFORE lockout
            #   <5s left:  book is likely locked, don't bother — fall to oracle settlement
            LOCKOUT_SECS = 5  # book goes dead this many seconds before end

            if token_bid and token_bid > pos.entry_price:
                sell_price = token_bid
                buy_fee = dynamic_fee(pos.entry_price) * pos.size
                sell_fee = dynamic_fee(sell_price) * sell_price * pos.shares
                potential_pnl = (sell_price * pos.shares) - pos.size - buy_fee - sell_fee

                should_exit = False
                exit_reason = ""

                if token_bid >= 0.99:
                    # 99c hit — auto-exit order likely filled (or simulate it)
                    should_exit = potential_pnl > 0
                    exit_reason = "AUTO-EXIT 99c" if pos.exit_order_id else "SELL 99c"
                    sell_price = 0.99
                elif token_bid >= 0.95 and secs_left < 15 and secs_left > LOCKOUT_SECS:
                    # 95c+ with 5-15s left — exit NOW before book locks
                    should_exit = potential_pnl > 0
                    exit_reason = f"SELL {sell_price*100:.0f}c pre-lockout"
                elif token_bid >= 0.90 and secs_left <= LOCKOUT_SECS + 3 and secs_left > LOCKOUT_SECS:
                    # 90c+ in the danger zone (5-8s left) — take what we can get
                    should_exit = potential_pnl > 0
                    exit_reason = f"SELL {sell_price*100:.0f}c last-chance"

                if should_exit and potential_pnl > 0:
                    # Recalc fees with actual sell_price
                    sell_fee = dynamic_fee(sell_price) * sell_price * pos.shares
                    potential_pnl = (sell_price * pos.shares) - pos.size - buy_fee - sell_fee
                    pos.exit_price = sell_price
                    pos.exit_asset_price = index_price
                    pos.pnl = potential_pnl
                    pos.result = "take_profit"
                    pos.resolved = True
                    self.cancel_exit_order(pos)
                    newly_closed.append(pos)
                    log(f"  [POS] {exit_reason} {pos.asset} {pos.timeframe} {pos.side}  "
                        f"token:{pos.entry_price*100:.0f}c->{sell_price*100:.0f}c [CLOB]  "
                        f"pnl:${pos.pnl:.4f}  {int(secs_left)}s left", "win")
                    continue

            # ── EXPIRY: settle using Polymarket's ORACLE (ground truth) ──
            # CRITICAL: Only trust Polymarket's own oracle for settlement.
            # Our price feeds and PTB can diverge from the oracle — using them
            # caused wrong W/L results (e.g., bot says WIN but oracle says LOSS).
            if secs_left <= 0:
                # Wait a bit after expiry for oracle to settle (don't check instantly)
                time_past_expiry = now - pos.window_end
                if time_past_expiry < 10:
                    # Too soon — oracle probably hasn't settled yet, keep waiting
                    still_open.append(pos)
                    continue

                # Try to get ACTUAL settlement from Polymarket oracle
                oracle_win, oracle_ptb = check_oracle_settlement(pos.token_id, side=pos.side)
                settle_source = "oracle"

                # Update PTB if oracle returned one (ground truth)
                if oracle_ptb is not None:
                    pos.price_to_beat = oracle_ptb

                if oracle_win is None and time_past_expiry < 60:
                    # Oracle not ready yet — keep waiting (up to 60s past expiry)
                    # Then fall back to index price vs PTB which is reliable enough
                    if time_past_expiry > 20 and int(time_past_expiry) % 15 < 3:
                        log(f"  [POS] Waiting for oracle: {pos.asset} {pos.timeframe} {pos.side}  "
                            f"{int(time_past_expiry)}s past expiry", "sys")
                    still_open.append(pos)
                    continue

                if oracle_win is None:
                    # Oracle API didn't return settlement — fall back to OUR own
                    # Chainlink boundary snapshot at window_end (== finalPrice that
                    # Polymarket WILL publish). Verified 2026-04-07: finalPrice[N]
                    # equals next window's priceToBeat[N+1], i.e. the Chainlink
                    # tick at the exact `window_end` second. Our boundary capture
                    # loop reads from the same RTDS feed at the same instant.
                    ref = pos.price_to_beat or pos.entry_asset_price
                    tf_secs = 300 if pos.timeframe == "5m" else 900
                    snap_key = (pos.sym, tf_secs, pos.window_end)
                    boundary_fp = getattr(self.pf, 'ptb_snapshots', {}).get(snap_key)
                    fp_for_settle = boundary_fp or index_price

                    if ref and fp_for_settle and fp_for_settle > 0:
                        # Polymarket rule: finalPrice > PTB → UP wins; tie → DOWN wins
                        price_went_up = fp_for_settle > ref
                        oracle_win = price_went_up if pos.side == "UP" else not price_went_up
                        settle_source = "boundary_fp" if boundary_fp else "index_fallback"
                        log(f"  [POS] Oracle silent after {int(time_past_expiry)}s, "
                            f"using {settle_source}: {pos.asset} {pos.timeframe} {pos.side}  "
                            f"ptb:${ref:,.2f} fp:${fp_for_settle:,.2f} → {'WIN' if oracle_win else 'LOSS'}",
                            "win" if oracle_win else "loss")
                    else:
                        # No PTB and no index — truly can't determine
                        oracle_win = False
                        settle_source = "no_data_loss"
                        log(f"  [POS] No PTB or index after {int(time_past_expiry)}s: "
                            f"{pos.asset} {pos.timeframe} {pos.side} — counting as LOSS", "warn")

                win = oracle_win
                ref_price = pos.price_to_beat or pos.entry_asset_price
                ref_change_pct = (index_price - ref_price) / ref_price if ref_price > 0 else 0

                buy_fee = dynamic_fee(pos.entry_price) * pos.size
                if win:
                    pos.pnl = (1.0 * pos.shares) - pos.size - buy_fee
                    pos.result = "win"
                else:
                    pos.pnl = -pos.size - buy_fee
                    pos.result = "loss"

                pos.exit_price = 1.0 if win else 0.0
                pos.exit_asset_price = index_price
                pos.resolved = True
                self.cancel_exit_order(pos)  # cancel standing 99c sell if still open
                newly_closed.append(pos)
                ptb_str = f"ptb:${ref_price:,.2f}" if pos.price_to_beat else f"entry:${pos.entry_asset_price:,.2f}(no ptb)"
                log(f"  [POS] SETTLED {pos.asset} {pos.timeframe} {pos.side}  "
                    f"{'WIN' if win else 'LOSS'}  [{settle_source}]  "
                    f"index:{ref_change_pct*100:+.4f}%  "
                    f"{ptb_str}->idx:${index_price:,.2f}  "
                    f"fee:${buy_fee:.4f}  "
                    f"pnl:${pos.pnl:.4f}", "win" if win else "loss")
                continue

            # Still open
            still_open.append(pos)

        # 3. Update position lists under lock (fast, no HTTP)
        with self._lock:
            self.open_positions = still_open
            self.closed_positions.extend(newly_closed)

    def get_resolved(self):
        """Pop all resolved positions for the main loop to process."""
        with self._lock:
            resolved = self.closed_positions[:]
            self.closed_positions = []
            return resolved

    def has_open(self, asset=None):
        """Check if there are open positions (optionally for a specific asset)."""
        with self._lock:
            if asset:
                return any(p.asset == asset and not p.resolved for p in self.open_positions)
            return len(self.open_positions) > 0

    def open_count(self):
        with self._lock:
            return len(self.open_positions)

    def open_list(self):
        """Return open positions info for dashboard."""
        with self._lock:
            out = []
            now = time.time()
            for p in self.open_positions:
                current = self.pf.get_price(p.sym) or p.entry_asset_price
                pct = (current - p.entry_asset_price) / p.entry_asset_price * 100
                out.append({
                    "asset": p.asset,
                    "timeframe": p.timeframe,
                    "side": p.side,
                    "size": p.size,
                    "entry_price": p.entry_price,
                    "entry_asset_price": p.entry_asset_price,
                    "current_asset_price": current,
                    "change_pct": pct,
                    "secs_left": max(0, int(p.window_end - now)),
                })
            return out

    def run_background(self):
        def loop():
            while True:
                time.sleep(C["exit_check_interval"])
                self.check_positions()
        threading.Thread(target=loop, daemon=True).start()
        log(f"Position tracker: stop_loss={C['stop_loss']*100:.1f}%  "
            f"take_profit={C['take_profit']*100:.1f}%  "
            f"check every {C['exit_check_interval']}s", "sys")


# ──────────────────────────────────────────────────────────────
# ORDER EXECUTOR — opens positions, doesn't resolve them
# ──────────────────────────────────────────────────────────────
class Executor:
    def __init__(self, lat_tracker, pf, pos_tracker):
        self._live_client = None
        self._lat = lat_tracker
        self._pf = pf
        self._pos = pos_tracker
        self._trade_times = deque(maxlen=100)  # timestamps of recent trades for rate limiting
        if not C["dry_run"] and CLOB_OK and C["pk"]:
            try:
                cl = ClobClient(C["clob_host"], key=C["pk"],
                                chain_id=C["chain_id"],
                                signature_type=1, funder=C["funder"])
                creds = cl.create_or_derive_api_creds()
                cl.set_api_creds(creds)
                self._live_client = cl
                pos_tracker._live_client = cl  # share auth'd client for exit order cancels
                log("CLOB: authenticated — LIVE TRADING", "warn")
            except Exception as e:
                log(f"CLOB auth failed: {e}. Staying in dry run.", "warn")

    def _trades_last_hour(self):
        """Count how many trades were filled in the last hour."""
        cutoff = time.time() - 3600
        return sum(1 for t in self._trade_times if t > cutoff)

    def execute(self, mkt, ed, size):
        """
        Opens a position. Returns (filled: bool, size: float).
        Uses order book + competitor simulation for realistic fills.
        Resolution happens asynchronously via PositionTracker.
        """
        avg_lat = self._lat.avg_ms()

        # ── Competitor simulation: faster bots may grab edge first ──
        survived, comp_reason = competitor_check(avg_lat, ed["net_edge"])
        if not survived:
            log(f"  SNIPED by {comp_reason} — {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                f"edge:{ed['net_edge']*100:.1f}%  lat:{avg_lat:.0f}ms", "warn")
            return False, 0.0

        # Simulate actual measured latency
        time.sleep(avg_lat / 1000)

        sym = next(a["sym"] for a in ASSETS if a["name"] == mkt["asset"])
        current_asset_price = self._pf.get_index_price(sym)
        if not current_asset_price:
            return False, 0.0

        if C["dry_run"] or not self._live_client:
            # Simulate fill against real order book
            MAX_ENTRY_PRICE = 0.85  # Don't enter above 85c (margin too thin)
            book_key = "up_book_asks" if ed["side"] == "UP" else "dn_book_asks"
            book_asks = mkt.get(book_key, [])

            # Check book staleness - refetch if data is >8s old
            book_age = time.time() - mkt.get("fetched_at", 0)
            if book_asks and book_age > 8:
                log(f"  [DRY] STALE BOOK {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                    f"age:{book_age:.0f}s - refetching", "warn")
                try:
                    book_resp = requests.get(
                        f"{C['clob_host']}/book?token_id={ed['token_id']}", timeout=3)
                    if book_resp.status_code == 200:
                        ob = book_resp.json()
                        book_asks = [{"price": float(a["price"]), "size": float(a["size"])}
                                     for a in ob.get("asks", [])]
                except Exception:
                    pass

            if book_asks:
                # Reject if best ask already too high
                if book_asks[0]["price"] > MAX_ENTRY_PRICE:
                    log(f"  [DRY] PRICE TOO HIGH {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                        f"ask:{book_asks[0]['price']*100:.0f}c > cap:{MAX_ENTRY_PRICE*100:.0f}c", "warn")
                    return False, 0.0
                fill = simulate_fill(book_asks, size)
                if fill is None or fill["unfilled_usd"] > size * 0.5:
                    # Not enough liquidity — rejected
                    log(f"  [DRY] NO LIQUIDITY {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                        f"wanted:${size:.2f}  book_depth:{len(book_asks)} levels", "warn")
                    return False, 0.0
                entry_price = fill["avg_price"]
                shares = fill["shares"]
                actual_size = fill["filled_usd"]
                ob_slippage = fill["slippage_pct"]
            else:
                # No order book data �� fall back to latency-based fill rate
                fill_rate = self._lat.fill_rate()
                if random.random() >= fill_rate:
                    return False, 0.0
                entry_price = ed["mkt_prob"]
                if entry_price > MAX_ENTRY_PRICE:
                    log(f"  [DRY] MODEL PRICE TOO HIGH {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                        f"model:{entry_price*100:.0f}c > cap:{MAX_ENTRY_PRICE*100:.0f}c", "warn")
                    return False, 0.0
                shares = size / entry_price
                actual_size = size
                ob_slippage = 0.0

            entry_fee = dynamic_fee(entry_price)

            pos = Position(
                asset=mkt["asset"],
                sym=sym,
                timeframe=mkt["timeframe"],
                side=ed["side"],
                token_id=ed["token_id"],
                entry_price=entry_price,
                size=actual_size,
                shares=shares,
                window_end=mkt["window_end"],
                entry_asset_price=current_asset_price,
                ed=ed,
                price_to_beat=mkt.get("price_to_beat"),
            )
            self._pos.add(pos)
            self._trade_times.append(time.time())

            src_str = "CLOB" if mkt.get("up_price_live") else "Gamma"
            ob_str = f"OB-fill:{entry_price*100:.1f}c slip:{ob_slippage*100:.2f}%" if book_asks else "no-OB"
            log(f"  [DRY] ENTRY {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                f"token:{str(ed['token_id'])[:14]}...  "
                f"fill:{entry_price*100:.1f}c  "
                f"model:{ed['our_prob']*100:.1f}c  "
                f"size:${actual_size:.2f}  shares:{shares:.2f}  "
                f"fee:{entry_fee*100:.2f}%  "
                f"idx:${current_asset_price:,.2f}  [{src_str}]  "
                f"({ob_str}  edge:{ed['net_edge']*100:.1f}%  "
                f"mom:{ed['momentum']:+.2f}  vol:{ed['vol_regime']}"
                f"{'  CONF' if ed['confluence'] else ''})", "dry")
            return True, actual_size

        else:
            # Live order — fetch FRESH order book, use real ask price
            MAX_ENTRY_PRICE = 0.85  # Don't enter above 85c (margin too thin)
            SLIPPAGE_CENTS = 0.02   # Bid 2c above best ask to handle ~110ms movement
            try:
                # Fresh book fetch (not stale 10s cache)
                token_id = ed["token_id"]
                book_url = f"{C['clob_host']}/book?token_id={token_id}"
                book_resp = requests.get(book_url, timeout=3)
                fresh_asks = []
                if book_resp.status_code == 200:
                    ob = book_resp.json()
                    fresh_asks = [{"price": float(a["price"]), "size": float(a["size"])}
                                  for a in ob.get("asks", [])]

                if not fresh_asks:
                    log(f"  [LIVE] NO BOOK {mkt['asset']} {mkt['timeframe']} {ed['side']} — skipping", "warn")
                    return False, 0.0

                best_ask = fresh_asks[0]["price"]

                # Reject if price already too high (thin margin, reversal risk)
                if best_ask > MAX_ENTRY_PRICE:
                    log(f"  [LIVE] PRICE TOO HIGH {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                        f"ask:{best_ask*100:.0f}c > cap:{MAX_ENTRY_PRICE*100:.0f}c — skipping", "warn")
                    return False, 0.0

                # Limit price = best ask + slippage buffer (covers ~110ms price movement)
                limit_price = round(min(best_ask + SLIPPAGE_CENTS, MAX_ENTRY_PRICE), 2)
                size_shares = round(size / limit_price, 2)

                args = OrderArgs(
                    token_id=token_id,
                    price=limit_price,
                    size=size_shares,
                    side=BUY,
                    fee_rate_bps=156,  # 1.56% taker fee for crypto updown markets
                )
                signed = self._live_client.create_order(args)
                resp = self._live_client.post_order(signed, OrderType.GTC)
                oid = resp.get("orderID", "?")
                self._trade_times.append(time.time())
                log(f"  [LIVE] {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                    f"${size:.2f} @ {limit_price:.3f} (ask:{best_ask:.3f}+{SLIPPAGE_CENTS*100:.0f}c)  "
                    f"orderID:{oid[:14]}  status:{resp.get('status')}", "win")

                # Use actual limit price as entry (not model probability)
                entry_price = limit_price
                pos = Position(
                    asset=mkt["asset"], sym=sym, timeframe=mkt["timeframe"],
                    side=ed["side"], token_id=token_id,
                    entry_price=entry_price, size=size,
                    shares=size_shares, window_end=mkt["window_end"],
                    entry_asset_price=current_asset_price,
                    ed=ed, price_to_beat=mkt.get("price_to_beat"),
                )

                # ── AUTO-EXIT: immediately place sell limit at 95c ──
                # When we win, token rises toward $1. Selling at 95c locks in
                # profit BEFORE the ~5s liquidity lockout near epoch end.
                # 95c is aggressive enough to fill while books are still active,
                # yet captures most of the upside (e.g. buy@76c sell@95c = 25% return).
                try:
                    exit_price = 0.95
                    exit_args = OrderArgs(
                        token_id=token_id,
                        price=exit_price,
                        size=size_shares,
                        side=SELL,
                        fee_rate_bps=156,
                    )
                    exit_signed = self._live_client.create_order(exit_args)
                    exit_resp = self._live_client.post_order(exit_signed, OrderType.GTC)
                    pos.exit_order_id = exit_resp.get("orderID")
                    log(f"  [LIVE] AUTO-EXIT placed {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                        f"SELL {size_shares:.2f}sh @ 95c  orderID:{pos.exit_order_id[:14] if pos.exit_order_id else '?'}",
                        "sys")
                except Exception as e:
                    log(f"  [LIVE] Auto-exit order failed (will rely on polling): {e}", "warn")

                self._pos.add(pos)
                return True, size
            except Exception as e:
                log(f"  [LIVE] Order failed: {e}", "loss")
                return False, 0.0

    def get_wallet_balance(self):
        """Fetch real USDC balance from Polymarket wallet (live mode only)."""
        if not self._live_client:
            return None
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            resp = self._live_client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            bal = float(resp.get("balance", 0)) / 1e6  # USDC has 6 decimals
            return bal
        except Exception as e:
            log(f"Wallet balance fetch failed: {e}", "warn")
            return None

# ──────────────────────────────────────────────────────────────
# IRON DOME v8 — async position-based trading
# ──────────────────────────────────────────────────────────────
class IronDomeV8:
    def __init__(self, pf, finder, ex, lat_tracker, pos_tracker):
        self.pf = pf
        self.finder = finder
        self.ex = ex
        self.lat = lat_tracker
        self.pos = pos_tracker
        self.bal = C["balance"]
        self.start = C["balance"]
        self.peak = C["balance"]
        self.wins = self.losses = self.trades = self.streak = 0
        self.fees = 0.0
        self.misses = 0
        self.active = False
        self.pnl_history = []
        self.recent_trades = []
        self.start_time = time.time()
        self.committed = 0.0          # $ currently locked in open positions
        self._last_wallet_sync = 0    # timestamp of last wallet balance fetch
        self._load_state()            # restore positions + stats from previous run
        self._sync_wallet_balance()   # in live mode, use real wallet balance

    def _sync_wallet_balance(self):
        """In live mode, sync balance from real Polymarket wallet."""
        if C["dry_run"]:
            return
        now = time.time()
        if now - self._last_wallet_sync < 30:  # don't spam API, sync every 30s
            return
        wallet_bal = self.ex.get_wallet_balance()
        if wallet_bal is not None:
            old = self.bal
            self.bal = wallet_bal
            self.peak = max(self.peak, self.bal)
            self._last_wallet_sync = now
            if abs(old - wallet_bal) > 0.01:
                log(f"  [WALLET] Balance synced: ${wallet_bal:.2f} (was ${old:.2f})", "sys")

    def _save_state(self):
        """Persist open positions + bot stats to disk so restarts don't lose trades."""
        try:
            with self.pos._lock:
                positions = [p.to_dict() for p in self.pos.open_positions if not p.resolved]
            state = {
                "bal": self.bal, "peak": self.peak,
                "wins": self.wins, "losses": self.losses,
                "trades": self.trades, "streak": self.streak,
                "fees": self.fees, "misses": self.misses,
                "committed": self.committed,
                "pnl_history": self.pnl_history[-200:],
                "recent_trades": self.recent_trades[-50:],
                "positions": positions,
                "saved_at": time.time(),
            }
            tmp = STATE_FILE.with_suffix('.tmp')
            with open(tmp, 'w') as f:
                json.dump(state, f, default=str)
            tmp.replace(STATE_FILE)
        except Exception as e:
            log(f"State save failed: {e}", "warn")

    def _load_state(self):
        """Restore positions + stats from previous run if state file exists."""
        if not STATE_FILE.exists():
            return
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)

            # Restore bot stats
            self.bal = state.get("bal", self.bal)
            self.peak = state.get("peak", self.peak)
            self.wins = state.get("wins", 0)
            self.losses = state.get("losses", 0)
            self.trades = state.get("trades", 0)
            self.streak = state.get("streak", 0)
            self.fees = state.get("fees", 0.0)
            self.misses = state.get("misses", 0)
            self.committed = state.get("committed", 0.0)
            self.pnl_history = state.get("pnl_history", [])
            self.recent_trades = state.get("recent_trades", [])

            # Restore open positions (skip expired ones)
            now = time.time()
            restored = 0
            expired_committed = 0.0
            for pd in state.get("positions", []):
                if pd["window_end"] > now - 90:  # still within settlement window
                    pos = Position.from_dict(pd)
                    with self.pos._lock:
                        self.pos.open_positions.append(pos)
                    restored += 1
                else:
                    # Position expired while bot was down — count as loss
                    expired_committed += pd.get("size", 0)
                    self.trades += 1
                    self.losses += 1
                    self.streak += 1
                    self.pnl_history.append(-pd.get("size", 0))
                    self.bal = max(0, self.bal - pd.get("size", 0))
                    self.committed = max(0, self.committed - pd.get("size", 0))
                    log(f"  [STATE] Expired while offline: {pd['asset']} {pd['timeframe']} {pd['side']} "
                        f"-> LOSS (missed settlement) pnl:-${pd.get('size', 0):.2f}", "loss")

            saved_at = state.get("saved_at", 0)
            age = int(now - saved_at) if saved_at else 0
            log(f"  [STATE] Restored: bal=${self.bal:.2f}  "
                f"{self.trades}trades  {restored}open  "
                f"age:{age}s", "sys")
            if expired_committed > 0:
                log(f"  [STATE] {len(state.get('positions', [])) - restored} positions expired while offline", "warn")

        except Exception as e:
            log(f"State load failed (starting fresh): {e}", "warn")

    def stats(self):
        pnl = self.bal - self.start
        wr = round(self.wins / self.trades * 100) if self.trades else 0
        dd = ((self.peak - self.bal) / self.peak * 100) if self.peak > 0 else 0
        avg_pnl = statistics.mean(self.pnl_history) if self.pnl_history else 0
        open_n = self.pos.open_count()
        log(f"  Bal:${self.bal:.3f}  "
            f"Committed:${self.committed:.2f}  "
            f"PnL:{'+' if pnl >= 0 else ''}${pnl:.3f}  "
            f"W:{self.wins} L:{self.losses}  WR:{wr}%  "
            f"Streak:{self.streak}  "
            f"DD:{dd:.1f}%  "
            f"Open:{open_n}  "
            f"AvgPnL:${avg_pnl:.4f}  "
            f"Fees:${self.fees:.3f}  "
            f"Missed:{self.misses}", "live")

    def banner(self):
        div()
        log("  IRON DOME v8  —  Real Position Tracking", "sys")
        log(f"  Balance: ${self.bal:.2f}  |  Fee: dynamic (peak {C['fee_rate']*100:.1f}% at 50c)", "sys")
        log(f"  Latency: {self.lat.avg_ms():.0f}ms avg  |  Fill: ~{self.lat.fill_rate()*100:.0f}%  |  {self.lat.label()}", "sys")
        log(f"  Assets: BTC / ETH / SOL  |  5m + 15m", "sys")
        log(f"  Edge model: EMA crossover + vol regime + confluence", "sys")
        log(f"  Sizing: Kelly criterion (cap {C['max_kelly']*100:.0f}%)", "sys")
        log(f"  Exits: stop_loss={C['stop_loss']*100:.1f}%  take_profit={C['take_profit']*100:.1f}%  hold-to-expiry", "sys")
        log(f"  Price: {self.pf.source}  +  RTDS Chainlink stream", "sys")
        log(f"  CLOB: {'py-clob-client OK' if CLOB_OK else 'not installed (Gamma fallback)'}", "sys")
        log(f"  Mode: {'DRY RUN — no real money' if C['dry_run'] else '*** LIVE TRADING ***'}",
            "live" if C["dry_run"] else "warn")
        div()

    def _check_confluence(self, asset_name, side):
        markets = self.finder.get_all()
        signals = {}
        for mkt in markets:
            if mkt["asset"] != asset_name:
                continue
            ed = compute_edge(mkt, self.pf, self.lat)
            if ed:
                signals[mkt["timeframe"]] = ed["side"]
        if len(signals) >= 2 and len(set(signals.values())) == 1:
            return signals.get("5m") == side
        return False

    def best_trade(self):
        base_threshold = C["min_edge"] + self.streak * C["streak_inc"]
        markets = self.finder.get_all()
        best_ed = None
        best_mkt = None
        best_score = 0.0
        best_threshold = base_threshold

        for mkt in markets:
            # Don't open multiple positions on same asset
            if self.pos.has_open(mkt["asset"]):
                continue

            has_conf = False
            ed = compute_edge(mkt, self.pf, self.lat)
            if ed:
                has_conf = self._check_confluence(mkt["asset"], ed["side"])
                if has_conf:
                    ed = compute_edge(mkt, self.pf, self.lat, confluence=True)

            if ed and ed["net_edge"] > 0:
                # Adaptive learning: adjust threshold per segment
                learn_adj = LEARNER.edge_adjustment(mkt["asset"], mkt["timeframe"], ed["side"])
                seg_threshold = max(0.003, base_threshold + learn_adj)  # floor 0.3%

                score = ed["net_edge"]
                if mkt["secs_left"] > 120:
                    score *= 1.1
                if mkt.get("up_price_live") or mkt.get("dn_price_live"):
                    score *= 1.15
                if ed["confluence"]:
                    score *= 1.2
                if ed["vol_regime"] == "high":
                    score *= 0.85

                if score > best_score and ed["net_edge"] >= seg_threshold:
                    best_score = score
                    best_ed = ed
                    best_mkt = mkt
                    best_threshold = seg_threshold

        if best_mkt:
            return best_mkt, best_ed, best_threshold
        return None, None, base_threshold

    def _process_resolved(self):
        """Process positions that have been resolved by the tracker."""
        resolved = self.pos.get_resolved()
        for pos in resolved:
            self.trades += 1
            self.fees += pos.size * dynamic_fee(pos.entry_price)
            # Return committed capital + PnL
            self.committed = max(0, self.committed - pos.size)
            self.bal = max(0, self.bal + pos.pnl)
            self.peak = max(self.peak, self.bal)
            self.pnl_history.append(pos.pnl)

            win = pos.result in ("win", "take_profit")

            self.recent_trades.append({
                "time": datetime.now().strftime('%H:%M:%S'),
                "asset": pos.asset,
                "timeframe": pos.timeframe,
                "side": pos.side,
                "size": pos.size,
                "mkt_prob": pos.entry_price,
                "our_prob": pos.ed["our_prob"],
                "net_edge": pos.ed["net_edge"],
                "win": win,
                "pnl": pos.pnl,
                "result": pos.result,
                "exit_price": pos.exit_price,
                "hold_time": int(time.time() - pos.entry_time),
            })
            if len(self.recent_trades) > 50:
                self.recent_trades = self.recent_trades[-50:]

            if win:
                self.wins += 1
                self.streak = 0
            else:
                self.losses += 1
                self.streak += 1

            # Record outcome for adaptive learning
            LEARNER.record(
                pos.asset, pos.timeframe, pos.side, win,
                pos.ed.get("net_edge", 0),
                pos.ed.get("momentum", 0),
                pos.ed.get("vol_regime", "normal"),
            )

            result_tag = pos.result.upper().replace("_", " ")
            log(f"  RESOLVED: {pos.asset} {pos.timeframe} {pos.side} -> {result_tag}  "
                f"pnl:${pos.pnl:+.4f}  "
                f"held:{int(time.time() - pos.entry_time)}s", "win" if win else "loss")
            self.stats()
        if resolved:
            self._save_state()

    def available_balance(self):
        """Balance minus capital locked in open positions."""
        return max(0, self.bal - self.committed)

    # ── LATE-GAME SNIPER ────────────────────────────────────────
    # Enters 8-15s before close when one side is nearly decided (90c+)
    # Monitors for reversal and flips if price collapses
    def _fresh_clob_mid(self, token_id):
        """Fetch a fresh CLOB mid-price for a single token."""
        if not token_id:
            return None
        try:
            resp = requests.get(f"{C['clob_host']}/book?token_id={token_id}", timeout=1.5)
            if resp.status_code == 200:
                ob = resp.json()
                bids = ob.get("bids", [])
                asks = ob.get("asks", [])
                if bids and asks:
                    bid = float(bids[0]["price"])
                    ask = float(asks[0]["price"])
                    if ask - bid <= 0.10:  # only trust tight books
                        return (bid + ask) / 2
        except Exception:
            pass
        return None

    def _fresh_gamma_price(self, token_id):
        """Fetch fresh price from Gamma API for a token (lastTradePrice or outcomePrices).
        This is what Polymarket.com actually displays — much more accurate than
        stale cached prices, especially near expiry when markets move fast."""
        if not token_id:
            return None
        try:
            r = requests.get(f"{C['gamma']}/markets",
                            params={"clob_token_id": token_id}, timeout=1.5)
            if r.ok:
                markets = r.json()
                if isinstance(markets, list) and markets:
                    m = markets[0]
                    # Try outcomePrices first (most accurate post-trade)
                    raw = m.get("outcomePrices")
                    if raw:
                        prices = json.loads(raw) if isinstance(raw, str) else raw
                        if prices:
                            p = float(prices[0])
                            if 0.01 < p < 0.99:  # only if not stuck at 0/1
                                return p
                    # Fall back to lastTradePrice
                    ltp = m.get("lastTradePrice")
                    if ltp:
                        p = float(ltp)
                        if 0.01 < p < 0.99:
                            return p
        except Exception:
            pass
        return None

    def snipe_late_game(self):
        """Check for nearly-decided markets close to expiry and snipe them."""
        avail = self.available_balance()
        if avail < C["min_bal"]:
            return

        markets = self.finder.get_all()
        for mkt in markets:
            # Compute fresh secs_left (cached value may be stale by up to 10s)
            secs_left = max(0, int(mkt["window_end"] - time.time()))
            # Sniper window: enter from 145s before close, stop at 35s before close.
            # Last 35s is dead-zone (avoid getting stuck on dust books at expiry).
            if secs_left < 35 or secs_left > 145:
                continue
            # Don't double up on same asset
            if self.pos.has_open(mkt["asset"]):
                continue

            # ── PRICE DISCOVERY ──
            # Strategy: Use Binance price (fastest) vs oracle PTB to FRONTRUN
            # Polymarket. Binance updates 1-3s before Chainlink/CLOB reacts.
            # We detect the move on Binance and enter before Polymarket catches up.
            sym = next((a["sym"] for a in ASSETS if a["name"] == mkt["asset"]), None)
            ptb = mkt.get("price_to_beat")

            # Get fastest price source (Binance > Coinbase > CoinGecko)
            # These update every ~13s via REST, but are AHEAD of Chainlink RTDS
            binance_price = self.pf.prices.get(sym) if sym else None
            chainlink_price = self.pf.chainlink.get(sym) if sym else None

            # Use Binance as primary (fastest), Chainlink as confirmation
            fast_price = binance_price or chainlink_price
            if not fast_price or not ptb or ptb <= 0:
                continue

            delta_pct = (fast_price - ptb) / ptb  # positive = UP winning
            abs_delta = abs(delta_pct)

            # Map price delta to implied probability using exponential time scaling.
            # Window is now 35-145s, so we measure how far through the active
            # entry range we are. Closer to 35s = more confident the move is locked.
            time_ratio = max(0.0, min(1.0, (145 - secs_left) / 110.0))  # 0.0 at 145s, 1.0 at 35s
            sensitivity = 40.0 * math.exp(2.5 * time_ratio)  # 40 at 145s, ~485 at 35s
            raw_prob = 0.50 + abs_delta * sensitivity
            implied = max(0.05, min(0.97, raw_prob))

            if delta_pct > 0:  # UP winning
                up_mid = implied
                dn_mid = 1.0 - implied
            elif delta_pct < 0:  # DOWN winning
                dn_mid = implied
                up_mid = 1.0 - implied
            else:
                up_mid = 0.50
                dn_mid = 0.50

            # Cross-validate with Chainlink if available (confirm direction)
            src_tag = "bnc" if binance_price else "cex"
            both_agree = False
            if binance_price and chainlink_price and ptb > 0:
                cl_delta = (chainlink_price - ptb) / ptb
                # If Binance and Chainlink agree on direction, stronger signal
                if (delta_pct > 0 and cl_delta > 0) or (delta_pct < 0 and cl_delta < 0):
                    src_tag = "bnc+cl"
                    both_agree = True
                # If they disagree, Binance is leading — still trust it but note
                elif abs(delta_pct) > 0.001 and ((delta_pct > 0) != (cl_delta > 0)):
                    src_tag = "bnc>cl"  # Binance leading, Chainlink hasn't caught up
            up_src = src_tag
            dn_src = src_tag

            # ── CONVICTION-BASED THRESHOLDS ──
            # Old approach: wait for model to hit 75c+ → by then CLOB is 99c.
            # New approach: enter EARLY when we're convinced on direction.
            # Conviction = Binance + Chainlink agree + meaningful price delta.
            # The CLOB ask (checked later) becomes the real entry price.
            #
            # With both_agree + abs_delta > 0.05%: direction is ~90% locked.
            # We don't need model to say 75c — we just need the CLOB to still
            # have asks at a price where we profit if we're right.
            high_conviction = both_agree and abs_delta > 0.0005  # >0.05% move from PTB
            mega_conviction = both_agree and abs_delta > 0.001   # >0.1% move

            # Three time tiers for the new 35-145s window:
            #   LATE  : 35-60s   — closest to close, move most locked in
            #   MID   : 60-100s  — direction firming up
            #   EARLY : 100-145s — fresh window, more uncertainty
            if mega_conviction:
                if secs_left <= 60:
                    SNIPE_THRESHOLD = 0.40
                elif secs_left <= 120:
                    SNIPE_THRESHOLD = 0.45
                else:
                    SNIPE_THRESHOLD = 0.50
            elif high_conviction:
                if secs_left <= 60:
                    SNIPE_THRESHOLD = 0.45
                elif secs_left <= 120:
                    SNIPE_THRESHOLD = 0.50
                else:
                    SNIPE_THRESHOLD = 0.55
            else:
                # Single source or tiny move — need higher model confidence
                if secs_left <= 60:
                    SNIPE_THRESHOLD = 0.60
                elif secs_left <= 100:
                    SNIPE_THRESHOLD = 0.65
                else:
                    SNIPE_THRESHOLD = 0.75

            best_price = max(up_mid or 0, dn_mid or 0)
            best_side = "UP" if (up_mid or 0) >= (dn_mid or 0) else "DN"
            conv_tag = "MEGA" if mega_conviction else "HIGH" if high_conviction else "low"
            log(f"  [SNIPER] Checking {mkt['asset']} {mkt['timeframe']}  "
                f"UP:{up_mid*100:.0f}c[{up_src}] DN:{dn_mid*100:.0f}c[{dn_src}]  "
                f"best:{best_side}@{best_price*100:.0f}c  {secs_left}s left  "
                f"delta:{abs_delta*100:.3f}%  conv:{conv_tag}  "
                f"thresh:{SNIPE_THRESHOLD*100:.0f}c"
                f"{'  -> QUALIFY' if best_price >= SNIPE_THRESHOLD else '  -> below thresh'}", "scan")

            # Determine if one side is nearly decided
            snipe_side = None
            snipe_price = None
            snipe_token = None
            opp_token = None  # opposite side for potential flip
            snipe_src = None

            if up_mid and up_mid >= SNIPE_THRESHOLD:
                snipe_side = "UP"
                snipe_price = up_mid
                snipe_token = mkt.get("up_yes_token")
                opp_token = mkt.get("dn_yes_token")
                snipe_src = up_src
            elif dn_mid and dn_mid >= SNIPE_THRESHOLD:
                snipe_side = "DOWN"
                snipe_price = dn_mid
                snipe_token = mkt.get("dn_yes_token")
                opp_token = mkt.get("up_yes_token")
                snipe_src = dn_src

            if not snipe_side:
                continue

            # ── CLOB REALITY CHECK ──
            # The CLOB on 5m markets is either dust (50c) or decided (99c).
            # Real price discovery doesn't happen on the book — it jumps instantly.
            # Our edge is KNOWING the direction from Binance+Chainlink BEFORE
            # the CLOB reacts. We place a limit buy at our model-implied price.
            #
            # We only block entry if the CLOB confirms outcome is FULLY decided:
            # both sides show extreme prices (ask >= 99c) AND the model agrees
            # (model > 90c). This catches the case where we're just too late.
            # But if model says 60c and CLOB says 99c, CLOB is probably showing
            # stale/dust prices — our Binance-derived signal is more current.
            if snipe_price > 0.90:
                # Model itself says >90c — outcome is very decided.
                # Check CLOB to confirm we can't actually get a fill.
                clob_token = snipe_token
                if clob_token:
                    try:
                        book_resp = requests.get(
                            f"{C['clob_host']}/book?token_id={clob_token}", timeout=1.5)
                        if book_resp.status_code == 200:
                            ob = book_resp.json()
                            asks = ob.get("asks", [])
                            if not asks:
                                log(f"  [SNIPER] SKIP {mkt['asset']} {mkt['timeframe']} {snipe_side}  "
                                    f"model:{snipe_price*100:.0f}c  NO ASKS — outcome decided", "warn")
                                continue
                            best_ask = float(asks[0]["price"])
                            if best_ask >= 0.95:
                                log(f"  [SNIPER] SKIP {mkt['asset']} {mkt['timeframe']} {snipe_side}  "
                                    f"model:{snipe_price*100:.0f}c  ask:{best_ask*100:.0f}c — too late", "warn")
                                continue
                            # CLOB has asks below 95c — use CLOB ask as entry
                            snipe_price = best_ask
                    except Exception:
                        pass

            # Expected profit: buy at model price, win → $1
            expected_return = (1.0 - snipe_price) / snipe_price
            fee_cost = dynamic_fee(snipe_price)
            net_return = expected_return - fee_cost
            if net_return < 0.05:  # need at least 5% net return after fees
                continue

            # ── CONVICTION-SCALED SIZING ──
            # Bet size scales with how confident we are in the call:
            #   - Move size from PTB (delta) → continuous 0..1 score
            #   - Both feeds agreeing roughly doubles effective size
            #   - Closer to expiry increases scale (move locked in)
            # Capped at C["max_bet"] ($35 default), with a $1 floor.
            cap = C["max_bet"]
            # Delta strength: 0.0 at delta=0, 1.0 at delta>=0.20%
            delta_score = min(1.0, abs_delta / 0.002)
            # Time-of-window boost: 0.6 at 145s left → 1.0 at 35s left
            time_boost = 0.6 + 0.4 * time_ratio
            # Agreement multiplier: solo source gets 35%, both-agree gets 100%
            agree_mult = 1.0 if both_agree else 0.35
            conviction = delta_score * time_boost * agree_mult  # 0..1
            target_size = cap * conviction
            # Floor: anything that passes the threshold gets at least $1.50
            target_size = max(1.50, target_size)
            # Hard cap + balance constraint
            size = min(target_size, cap, avail - C["min_bal"])
            if size < 1.0:
                continue

            window_tier = "LATE" if secs_left <= 60 else "MID" if secs_left <= 100 else "EARLY"
            log(f"  SNIPER: {mkt['asset']} {mkt['timeframe']} {snipe_side}  "
                f"price:{snipe_price*100:.1f}c [{snipe_src}]  "
                f"delta:{abs_delta*100:.3f}%  conv:{conv_tag}  "
                f"net_return:{net_return*100:.1f}%  "
                f"size:${size:.2f}  {secs_left}s left  "
                f"[{window_tier} thresh:{SNIPE_THRESHOLD*100:.0f}c]", "edge")

            # Skip competitor check for sniper — we're not competing for a
            # mispriced edge, we're timing an entry near expiry. Different strategy.
            # Competitors don't block this because the opportunity is time-based,
            # not information-based.

            # Execute entry
            shares = size / snipe_price
            entry_fee = dynamic_fee(snipe_price)
            ed = {
                "side": snipe_side, "token_id": snipe_token,
                "our_prob": snipe_price, "mkt_prob": snipe_price,
                "net_edge": net_return, "gross": net_return,
                "momentum": 0, "vol_regime": "normal",
                "spread": 0.02, "total_cost": fee_cost,
                "confluence": False,
            }

            entry_asset_price = fast_price
            if not entry_asset_price:
                continue

            pos = Position(
                asset=mkt["asset"], sym=sym, timeframe=mkt["timeframe"],
                side=snipe_side, token_id=snipe_token,
                entry_price=snipe_price, size=size, shares=shares,
                window_end=mkt["window_end"],
                entry_asset_price=entry_asset_price, ed=ed,
                price_to_beat=mkt.get("price_to_beat"),
            )
            self.pos.add(pos)
            self.committed += size
            self._trade_times_sniper = getattr(self, '_trade_times_sniper', [])
            self._trade_times_sniper.append(time.time())

            log(f"  [SNIPE] ENTRY {mkt['asset']} {mkt['timeframe']} {snipe_side}  "
                f"at {snipe_price*100:.1f}c  shares:{shares:.2f}  "
                f"size:${size:.2f}  {secs_left}s to close", "dry")
            self._save_state()

            # ── REVERSAL MONITOR (non-blocking) ──
            # Runs in a daemon thread so the main loop keeps serving the dashboard
            def _monitor_reversal(executor, pos, snipe_side, snipe_price, snipe_token,
                                  opp_token, shares, size, entry_fee, entry_asset_price,
                                  sym, mkt):
                REVERSAL_THRESHOLD = max(0.40, snipe_price - 0.15)
                FLIP_THRESHOLD = 0.65
                monitor_secs = max(2, int(mkt["window_end"] - time.time()) - 2)

                for _check in range(monitor_secs):
                    time.sleep(1)
                    if pos.resolved:
                        return
                    # Compute implied price from fastest exchange price (Binance)
                    # Don't waste time on CLOB fetch (slow, usually dust books)
                    fresh = None
                    if True:
                        idx = (executor.pf.prices.get(sym) or
                               executor.pf.chainlink.get(sym))
                        ptb = mkt.get("price_to_beat")
                        if idx and ptb and ptb > 0:
                            secs_rem = max(1, int(mkt["window_end"] - time.time()))
                            delta = (idx - ptb) / ptb
                            abs_d = abs(delta)
                            tr = max(0.0, min(1.0, (145 - secs_rem) / 110.0))
                            sens = 40.0 * math.exp(2.5 * tr)
                            prob = max(0.05, min(0.97, 0.50 + abs_d * sens))
                            # Compute price for the side we're holding
                            if snipe_side == "UP":
                                fresh = prob if delta > 0 else (1.0 - prob)
                            else:
                                fresh = prob if delta < 0 else (1.0 - prob)
                        if fresh is None:
                            continue

                    if fresh < REVERSAL_THRESHOLD:
                        sell_price = fresh
                        sell_pnl = (sell_price * shares) - size - (entry_fee * size) - (dynamic_fee(sell_price) * sell_price * shares)
                        pos.exit_price = sell_price
                        pos.exit_asset_price = executor.pf.get_index_price(sym) or entry_asset_price
                        pos.pnl = sell_pnl
                        pos.result = "stop_loss"
                        pos.resolved = True
                        executor.committed = max(0, executor.committed - size)

                        log(f"  [SNIPE] REVERSAL! {mkt['asset']} {snipe_side}  "
                            f"{snipe_price*100:.0f}c->{fresh*100:.0f}c  "
                            f"SOLD pnl:${sell_pnl:+.4f}", "loss")

                        # Use the opposite side implied price (1 - fresh was our side)
                        opp_mid = 1.0 - fresh if fresh else None
                        opp_side = "DOWN" if snipe_side == "UP" else "UP"
                        remaining_avail = executor.available_balance()

                        if (opp_mid and opp_mid >= FLIP_THRESHOLD
                                and remaining_avail >= 0.50
                                and (mkt["window_end"] - time.time()) > 3):
                            flip_size = min(remaining_avail * 0.15, remaining_avail - C["min_bal"])
                            if flip_size >= 0.50:
                                flip_shares = flip_size / opp_mid
                                flip_ed = {
                                    "side": opp_side, "token_id": opp_token,
                                    "our_prob": opp_mid, "mkt_prob": opp_mid,
                                    "net_edge": (1.0 - opp_mid) / opp_mid - dynamic_fee(opp_mid),
                                    "gross": (1.0 - opp_mid) / opp_mid,
                                    "momentum": 0, "vol_regime": "normal",
                                    "spread": 0.02, "total_cost": dynamic_fee(opp_mid),
                                    "confluence": False,
                                }
                                flip_pos = Position(
                                    asset=mkt["asset"], sym=sym, timeframe=mkt["timeframe"],
                                    side=opp_side, token_id=opp_token,
                                    entry_price=opp_mid, size=flip_size, shares=flip_shares,
                                    window_end=mkt["window_end"],
                                    entry_asset_price=pos.exit_asset_price, ed=flip_ed,
                                    price_to_beat=mkt.get("price_to_beat"),
                                )
                                executor.pos.add(flip_pos)
                                executor.committed += flip_size
                                log(f"  [SNIPE] FLIP to {opp_side}!  "
                                    f"at {opp_mid*100:.1f}c  size:${flip_size:.2f}  "
                                    f"{int(mkt['window_end'] - time.time())}s left", "edge")
                        return

            t = threading.Thread(
                target=_monitor_reversal,
                args=(self, pos, snipe_side, snipe_price, snipe_token,
                      opp_token, shares, size, entry_fee, entry_asset_price,
                      sym, mkt),
                daemon=True,
            )
            t.start()

            # Only snipe one market per cycle
            return

    def run(self):
        self.active = True
        time.sleep(5)
        self.banner()
        log("System Online — scanning live markets...", "sys")

        while self.active:
            # ── Process any resolved positions first ──
            self._process_resolved()
            self._sync_wallet_balance()

            if self.bal < C["min_bal"]:
                log("CRITICAL: Balance below minimum. Halted.", "loss")
                break

            if self.bal < self.peak * 0.5:
                log("CRITICAL: 50% drawdown from peak. Halted for safety.", "loss")
                break

            # ── Late-game sniper: catch nearly-decided markets ──
            self.snipe_late_game()

            # Process any positions resolved while sniper was running
            self._process_resolved()

            # Live price display
            parts = []
            for a in ASSETS:
                p = self.pf.get_price(a["sym"])
                cl = self.pf.chainlink.get(a["sym"])
                m = self.pf.get_momentum(a["sym"])
                vr = self.pf.get_vol_regime(a["sym"])
                cl_str = f" CL${cl:,.0f}" if cl else ""
                vr_str = f"[{vr[0].upper()}]"
                parts.append(f"{a['name']}:${p:,.0f}{cl_str}({m:+.2f}){vr_str}" if p else f"{a['name']}:loading")
            open_n = self.pos.open_count()
            avail = self.available_balance()
            log(f"Live — {' | '.join(parts)}  lat:{self.lat.avg_ms():.0f}ms  "
                f"open:{open_n}  avail:${avail:.2f}", "scan")

            # Don't open new positions if too much capital committed
            if avail < C["min_bal"]:
                log(f"Capital committed (${self.committed:.2f}), waiting for positions to resolve...", "sys")
                time.sleep(C["scan_sec"])
                continue

            mkt, ed, threshold = self.best_trade()

            if mkt is None:
                all_mkts = self.finder.get_all()
                if not all_mkts:
                    log("No markets loaded yet — waiting for finder...", "sys")
                else:
                    best_n, best_m, best_e = 0, None, None
                    for m2 in all_mkts:
                        if self.pos.has_open(m2["asset"]):
                            continue
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
                            f"mkt:{best_e['mkt_prob']*100:.0f}c "
                            f"model:{best_e['our_prob']*100:.0f}c  "
                            f"cost:{best_e['total_cost']*100:.1f}%  "
                            f"{best_m['secs_left']}s left", "sys")
                    else:
                        log(f"Markets loaded ({len(all_mkts)}) but no edges qualify.", "sys")
                time.sleep(C["scan_sec"])
                continue

            # Kelly sizing with time decay
            size = kelly_size(ed, avail)
            if mkt["secs_left"] < 90:
                size *= max(0.3, mkt["secs_left"] / 90)
            size = max(0.50, min(size, avail * C["max_kelly"]))

            if size > avail:
                log(f"Size ${size:.2f} > available ${avail:.2f}, skipping.", "warn")
                time.sleep(C["scan_sec"])
                continue

            log(f"OPPORTUNITY: {mkt['asset']} {mkt['timeframe']} {ed['side']}  "
                f"edge:{ed['net_edge']*100:.1f}%  "
                f"mkt:{ed['mkt_prob']*100:.0f}c  "
                f"model:{ed['our_prob']*100:.0f}c  "
                f"kelly:${size:.2f}  "
                f"vol:{ed['vol_regime']}  "
                f"{mkt['secs_left']}s left"
                f"{'  CONFLUENCE' if ed['confluence'] else ''}", "edge")

            filled, committed_size = self.ex.execute(mkt, ed, size)

            if not filled:
                self.misses += 1
                log(f"MISSED FILL — queue position lost ({self.lat.avg_ms():.0f}ms latency)", "loss")
                time.sleep(1)
                continue

            # Lock capital for open position
            self.committed += committed_size
            log(f"  Position opened. Committed: ${self.committed:.2f}  Available: ${self.available_balance():.2f}", "sys")
            self._save_state()

            time.sleep(C["scan_sec"])

        # Wait for remaining positions to resolve
        if self.pos.open_count() > 0:
            log(f"Waiting for {self.pos.open_count()} open positions to resolve...", "sys")
            while self.pos.open_count() > 0:
                self._process_resolved()
                time.sleep(2)

        log("Iron Dome v8 — offline.", "sys")
        div()
        self.stats()
        if self.pnl_history:
            wins_pnl = [p for p in self.pnl_history if p > 0]
            loss_pnl = [p for p in self.pnl_history if p <= 0]
            log(f"  Avg win: ${statistics.mean(wins_pnl):.4f}" if wins_pnl else "  No wins", "win")
            log(f"  Avg loss: ${statistics.mean(loss_pnl):.4f}" if loss_pnl else "  No losses", "loss")
            # Show exit type breakdown
            tp = sum(1 for t in self.recent_trades if t.get("result") == "take_profit")
            sl = sum(1 for t in self.recent_trades if t.get("result") == "stop_loss")
            exp_w = sum(1 for t in self.recent_trades if t.get("result") == "win")
            exp_l = sum(1 for t in self.recent_trades if t.get("result") == "loss")
            log(f"  Exits: TP:{tp}  SL:{sl}  Expiry-Win:{exp_w}  Expiry-Loss:{exp_l}", "sys")

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
            "committed": self.committed,
            "available": self.available_balance(),
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
            "open_positions": self.pos.open_list(),
            "prices": prices,
            "markets": markets,
            "start_time": self.start_time,
            "learning": LEARNER.get_stats(),
        }

# ──────────────────────────────────────────────────────────────
# DASHBOARD HTTP SERVER
# ──────────────────────────────────────────────────────────────
_bot_ref = None  # set at startup

class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split('?')[0]  # strip query params
        if path == '/api/stats':
            try:
                if _bot_ref:
                    data = _bot_ref.get_dashboard_data()
                    body = json.dumps(data, default=str).encode('utf-8')
                else:
                    body = json.dumps({"error": "bot not started"}).encode('utf-8')
            except Exception as e:
                body = json.dumps({"error": str(e)}).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.write(body)
        elif path in ('/', '/dashboard', '/health'):
            # Try multiple paths for dashboard.html
            candidates = [
                Path(__file__).parent / 'dashboard.html',
                Path.cwd() / 'dashboard.html',
                Path('/app/dashboard.html'),  # Railway default
            ]
            body = None
            for p in candidates:
                if p.exists():
                    body = p.read_bytes()
                    break
            if path == '/health':
                # Health check endpoint for Railway
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.write(json.dumps({"status": "ok"}).encode('utf-8'))
            elif body:
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.write(body)
            else:
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.write(b'<html><body><h2>Iron Dome v8 Running</h2><p>dashboard.html not found on disk</p><p><a href="/api/stats">API Stats (JSON)</a></p></body></html>')
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
    finder = MarketFinder(pf=pf)
    pos_tracker = PositionTracker(pf, lat_tracker)
    ex = Executor(lat_tracker, pf, pos_tracker)

    log("Loading live data...", "sys")
    pf.run_background()
    finder.run_background()
    lat_tracker.run_background()
    pos_tracker.run_background()

    time.sleep(6)

    bot = IronDomeV8(pf, finder, ex, lat_tracker, pos_tracker)

    # Start dashboard server
    # Railway sets PORT env var — must use it or external URL won't route
    import __main__ as _main_module
    _main_module._bot_ref = bot
    dash_port = int(os.getenv("PORT", os.getenv("DASH_PORT", "8050")))
    start_dashboard(dash_port)

    try:
        bot.run()
    except KeyboardInterrupt:
        bot.active = False
        log("Interrupted.", "sys")
        bot.stats()
