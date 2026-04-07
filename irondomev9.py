import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
"""
Iron Dome v9 — Pure Market Maker
──────────────────────────────────────────────────────────────
Pure maker-only strategy. No taker sniper, no edge model.

Posts ladder limit bids across the window at escalating prices.
0% maker fee. VPIN (Volume-synchronized Probability of Informed Trading)
detects smart-money flow from CLOB book depth to confirm direction.

4-tier ladder: SCOUT → CONFIRM → SCALE → LOAD
  Tier | Secs into 5m | Bid   | Size% | Conviction needed
  ─────┼──────────────┼───────┼───────┼──────────────────
   1   | 5-60s        | 22-30c| 5%    | both_agree + >0.05%
   2   | 60-150s      | 35-42c| 10%   | both_agree + >0.08%
   3   | 150-240s     | 43-50c| 15%   | both_agree + >0.10%
   4   | 240-290s     | 55-65c| 20%   | both_agree + >0.15%

WHAT IS 100% REAL:
  - Market slugs computed from clock  (btc-updown-5m-{epoch})
  - YES/NO token IDs from Gamma API   (exact clobTokenIds)
  - Live bid/ask prices from CLOB     (get_price per token)
  - BTC/ETH/SOL spot + 1m candles     (Binance Vision → Coinbase → CoinGecko)
  - Chainlink price stream            (Polymarket RTDS WebSocket)
  - Fee rate 0% (maker)               (Polymarket maker rebate)
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

import os, time, json, random, math, threading, asyncio, statistics, hashlib
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
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
    "min_edge":   0.008,          # 0.8% minimum net edge (costs already subtracted)
    "streak_inc": 0.002,          # +0.2% threshold per loss streak
    "min_bal":    1.50,
    "cooldown":   10,             # base cooldown (adaptive: * streak)
    "scan_sec":   2,              # fast scan cycle (was 4s, need speed for frontrunning)
    "max_kelly":  0.10,           # cap Kelly at 10% of bankroll
    "min_kelly":  0.05,           # floor Kelly at 5%
    "max_bet":    float(os.getenv("MAX_BET", "35.0")),
    "min_volume": 50,             # skip markets with < $50 24h volume
    "stale_secs": 130,            # skip markets not refreshed within 120s
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

STATE_FILE = Path(__file__).parent / "bot_state_v9.json"

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

def pos_buy_fee(pos):
    """Buy fee for a position: 0% for maker orders, dynamic_fee for taker."""
    if getattr(pos, 'order_type', 'taker') == 'maker':
        return 0.0
    return dynamic_fee(pos.entry_price) * pos.size

# ──────────────────────────────────────────────────────────────
# VPIN — Volume-synchronized Probability of Informed Trading
# ──────────────────────────────────────────────────────────────
# Detects smart-money positioning from CLOB order book depth.
# When informed traders (like woshfq19) enter, they place limit bids
# on one side. This shows up as asymmetric $ volume in the books.
#
# UP_pressure  = $ sitting in UP bids  + $ sitting in DN asks
#   (both indicate someone expects price to go UP)
# DN_pressure  = $ sitting in DN bids  + $ sitting in UP asks
#   (both indicate someone expects price to go DOWN)
# VPIN = |UP_pressure - DN_pressure| / (UP_pressure + DN_pressure)
#
# Returns (score, direction, detail_str):
#   score:     0.0-1.0 (0 = balanced, 1 = fully one-sided)
#   direction: "UP" or "DOWN" (which side has more informed flow)
#   detail:    human-readable summary for logging

def compute_vpin(mkt):
    """Compute VPIN from cached CLOB book depth on both UP and DOWN tokens."""
    up_bids = mkt.get("up_book_bids", [])
    up_asks = mkt.get("up_book_asks", [])
    dn_bids = mkt.get("dn_book_bids", [])
    dn_asks = mkt.get("dn_book_asks", [])

    # Filter out dust orders (1c bids and 99c asks are market-maker noise)
    def real_volume(orders, is_bid=True):
        """Sum $ volume of non-dust orders.
        Only count orders in the 'informed' range: 15-85c.
        Below 15c = speculative dust, above 85c = near-certain noise."""
        total = 0.0
        for o in orders:
            p, s = o["price"], o["size"]
            if p < 0.15 or p > 0.85:
                continue
            total += p * s  # dollar volume
        return total

    # Also check if the book has a meaningful spread before trusting it.
    # If only one side has orders (all bids, no asks or vice versa),
    # that side's volume is unreliable — it's just parked orders.
    def book_has_two_sides(bids, asks):
        """Check if both bid and ask exist with reasonable prices."""
        has_bid = any(0.15 <= o["price"] <= 0.85 for o in bids)
        has_ask = any(0.15 <= o["price"] <= 0.85 for o in asks)
        return has_bid and has_ask

    # VPIN requires BOTH token books to be two-sided (informed).
    # If either book is one-sided or empty, we can't distinguish
    # informed flow from market-maker spread noise.
    up_two_sided = book_has_two_sides(up_bids, up_asks)
    dn_two_sided = book_has_two_sides(dn_bids, dn_asks)

    if not (up_two_sided and dn_two_sided):
        return 0.0, None, "no vol"

    up_bid_vol = real_volume(up_bids)
    up_ask_vol = real_volume(up_asks, False)
    dn_bid_vol = real_volume(dn_bids)
    dn_ask_vol = real_volume(dn_asks, False)

    # UP pressure: people buying UP tokens + people selling DN tokens
    # (both = bullish bets)
    up_pressure = up_bid_vol + dn_ask_vol
    # DN pressure: people buying DN tokens + people selling UP tokens
    # (both = bearish bets)
    dn_pressure = dn_bid_vol + up_ask_vol

    total = up_pressure + dn_pressure
    if total < 5.0:  # need at least $5 of informed volume
        return 0.0, None, "no vol"

    imbalance = abs(up_pressure - dn_pressure)
    vpin_score = imbalance / total

    if up_pressure > dn_pressure:
        direction = "UP"
    elif dn_pressure > up_pressure:
        direction = "DOWN"
    else:
        direction = None

    detail = (f"UP${up_pressure:.0f}(b${up_bid_vol:.0f}+da${dn_ask_vol:.0f}) "
              f"DN${dn_pressure:.0f}(b${dn_bid_vol:.0f}+ua${up_ask_vol:.0f})")

    return vpin_score, direction, detail


def compute_vpin_fresh(up_token, dn_token):
    """Fetch fresh CLOB books and compute VPIN (for time-critical decisions)."""
    def fetch_book(token_id):
        if not token_id:
            return [], []
        try:
            resp = requests.get(f"{C['clob_host']}/book?token_id={token_id}", timeout=2)
            if resp.ok:
                ob = resp.json()
                bids = [{"price": float(b["price"]), "size": float(b["size"])}
                        for b in ob.get("bids", [])[:C["ob_depth"]]]
                asks = [{"price": float(a["price"]), "size": float(a["size"])}
                        for a in ob.get("asks", [])[:C["ob_depth"]]]
                return bids, asks
        except Exception:
            pass
        return [], []

    up_bids, up_asks = fetch_book(up_token)
    dn_bids, dn_asks = fetch_book(dn_token)

    fake_mkt = {
        "up_book_bids": up_bids, "up_book_asks": up_asks,
        "dn_book_bids": dn_bids, "dn_book_asks": dn_asks,
    }
    return compute_vpin(fake_mkt)


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
        """Settlement price: Chainlink first (matches Polymarket oracle), then index."""
        with self._lock:
            return self.chainlink.get(sym) or self.index.get(sym) or self.prices.get(sym)

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
                loop.run_until_complete(_run())
            finally:
                loop.close()

        threading.Thread(target=_thread, daemon=True).start()

    def start_binance_ws(self):
        """Binance WebSocket for real-time BTC/ETH/SOL prices (tick-by-tick)."""
        if not WS_OK:
            log("websockets not installed — Binance WS disabled", "warn")
            return

        # Combined stream: miniTicker for all 3 assets
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
        # PTB snapshots: capture Chainlink price at exact window boundaries
        # Polymarket's PTB = Chainlink price at the exact second the window starts.
        # We pre-schedule captures at every 5m and 15m boundary so our PTB matches.
        self.ptb_snapshots = {}  # (sym, tf_secs, window_start) → price
        def _ptb_capture_loop():
            while True:
                now = time.time()
                # Find the NEXT 5m boundary (also covers 15m since 900 is multiple of 300)
                next_5m = (int(now) // 300 + 1) * 300
                wait = next_5m - now
                if wait > 0:
                    time.sleep(wait)
                # Capture Chainlink price at this exact boundary
                snap_time = int(time.time())
                # Small retry loop — Chainlink WS might be a fraction behind
                for _ in range(3):
                    with self._lock:
                        for asset in ASSETS:
                            sym = asset["sym"]
                            cl = self.chainlink.get(sym)
                            if cl:
                                for tf in TFS:
                                    if snap_time % tf["secs"] == 0:
                                        # This boundary is a window start for this timeframe
                                        key = (sym, tf["secs"], snap_time)
                                        self.ptb_snapshots[key] = cl
                                        log(f"  [PTB] Boundary snapshot {asset['name']} {tf['label']} "
                                            f"→ ${cl:,.2f}", "sys")
                    if any(self.ptb_snapshots.get((a["sym"], t["secs"], snap_time))
                           for a in ASSETS for t in TFS if snap_time % t["secs"] == 0):
                        break
                    time.sleep(0.3)  # retry if Chainlink hadn't updated yet
                # Purge old snapshots (>30 min)
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
        log("Price feed: Binance WS (real-time) + Chainlink RTDS WS + REST fallback", "sys")

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
                    if raw_bids:
                        info["bid"] = float(raw_bids[0]["price"])
                    if raw_asks:
                        info["ask"] = float(raw_asks[0]["price"])
                    if info["bid"] is not None and info["ask"] is not None:
                        spread = info["ask"] - info["bid"]
                        if spread <= 0.40:
                            # Tight enough spread — mid is meaningful
                            info["mid"] = (info["bid"] + info["ask"]) / 2
                        else:
                            # Dust book (e.g. 1c/99c) — mid is meaningless
                            info["mid"] = None
            except Exception as e:
                log(f"  CLOB book fetch failed for {str(token_id)[:14]}...: {e}", "warn")
            return info

        up_clob = clob_data(up_yes_id)
        dn_clob = clob_data(down_yes_id)

        vol = float(data.get("volume24hr") or data.get("volumeClob") or 0)
        window_start = ts_cur
        window_end = ts_cur + tf["secs"]
        secs_left = max(0, window_end - int(time.time()))

        # ── PTB from Gamma API ONLY (no external price feeds) ──
        # Tier 1: current event's eventMetadata.priceToBeat (appears after window closes)
        # Tier 2: previous window's finalPrice via Gamma (= current window's PTB)
        # Tier 3: cached from a previous scan this window (Gamma-sourced only)
        ptb_oracle = None
        meta = data.get("eventMetadata") or {}
        if meta.get("priceToBeat") is not None:
            try:
                ptb_oracle = float(meta["priceToBeat"])
            except (ValueError, TypeError):
                pass

        if ptb_oracle is None:
            # Previous window's finalPrice = current window's PTB
            # This is the ONLY valid Gamma source during the current window.
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
            # Tier 3: check our Gamma-sourced cache from earlier this window
            if ptb_oracle is None:
                ptb_cache_key = (asset["name"], tf["label"], ts_cur)
                if ptb_cache_key in self._price_to_beat:
                    ptb_oracle = self._price_to_beat[ptb_cache_key]

        # Each side has its own CLOB book — use real prices, fall back to Gamma
        up_price = up_clob["mid"] if up_clob["mid"] is not None else up_gp
        dn_price = dn_clob["mid"] if dn_clob["mid"] is not None else down_gp

        return {
            "asset":      asset["name"],
            "timeframe":  tf["label"],
            "slug":       slug,
            "window_start": window_start,
            "window_end": window_end,
            "secs_left":  secs_left,
            # UP side
            "up_price":      up_price,
            "up_bid":        up_clob["bid"],
            "up_ask":        up_clob["ask"],
            "up_price_live": up_clob["mid"],
            "up_yes_token":  up_yes_id,
            "up_no_token":   up_no_id,
            "up_book_asks":  up_clob["book_asks"],
            "up_book_bids":  up_clob["book_bids"],
            # DOWN side
            "dn_price":      dn_price,
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
                    # ── PTB resolution ──
                    # Priority: Gamma API (ground truth) > Chainlink snapshot (fast estimate)
                    # Chainlink gets us trading immediately; Gamma overrides once available.
                    ptb_key = (mkt["asset"], mkt["timeframe"], mkt["window_start"])
                    wnd_key = (mkt["asset"], mkt["timeframe"])
                    prev_ws = self._last_window.get(wnd_key)
                    self._last_window[wnd_key] = mkt["window_start"]

                    # On new window: use pre-captured boundary snapshot (exact second)
                    # Falls back to current Chainlink price if boundary wasn't captured
                    if (prev_ws is None or mkt["window_start"] != prev_ws) and ptb_key not in self._price_to_beat:
                        _sym = next((a["sym"] for a in ASSETS if a["name"] == mkt["asset"]), None)
                        _tf_secs = next((t["secs"] for t in TFS if t["label"] == mkt["timeframe"]), None)
                        if _sym and self._pf and _tf_secs:
                            # Prefer pre-captured boundary price (matches Polymarket exactly)
                            boundary_key = (_sym, _tf_secs, mkt["window_start"])
                            boundary_price = getattr(self._pf, 'ptb_snapshots', {}).get(boundary_key)
                            if boundary_price:
                                self._price_to_beat[ptb_key] = boundary_price
                                log(f"  [PTB] Boundary {mkt['asset']} {mkt['timeframe']} "
                                    f"→ ${boundary_price:,.2f} (exact window start)", "sys")
                            else:
                                # Fallback: current Chainlink price (close but not exact)
                                snap_price = self._pf.chainlink.get(_sym) or self._pf.prices.get(_sym)
                                if snap_price:
                                    self._price_to_beat[ptb_key] = snap_price
                                    log(f"  [PTB] Snapshot {mkt['asset']} {mkt['timeframe']} "
                                        f"→ ${snap_price:,.2f} (late, waiting for Gamma)", "sys")

                    ptb_src = "none"
                    if mkt.get("price_to_beat"):
                        # Gamma API PTB — always overrides Chainlink snapshot
                        prev_ptb = self._price_to_beat.get(ptb_key)
                        self._price_to_beat[ptb_key] = mkt["price_to_beat"]
                        ptb_src = "gamma"
                        if prev_ptb and abs(prev_ptb - mkt["price_to_beat"]) > 0.01:
                            log(f"  [PTB] Gamma override {mkt['asset']} {mkt['timeframe']} "
                                f"${prev_ptb:,.2f} → ${mkt['price_to_beat']:,.2f}", "sys")
                    elif ptb_key in self._price_to_beat:
                        mkt["price_to_beat"] = self._price_to_beat[ptb_key]
                        ptb_src = "chainlink"
                    up_src = "CLOB" if mkt["up_price_live"] else "Gamma"
                    dn_src = "CLOB" if mkt.get("dn_price_live") else "Gamma"
                    ptb_val = mkt.get("price_to_beat")
                    ptb_info = f"  PTB:${ptb_val:,.2f}[{ptb_src}]" if ptb_val else "  PTB:none"

                    # Always show real CLOB prices — no model override
                    up_show = mkt['up_price']
                    dn_show = mkt['dn_price']

                    # VPIN from cached book data
                    _vs, _vd, _ = compute_vpin(mkt)
                    vpin_str = f"  VPIN:{_vs:.0%}->{_vd}" if _vd else ""

                    log(f"  [{mkt['asset']} {mkt['timeframe']}] "
                        f"UP:{up_show*100:.0f}¢[{up_src}] "
                        f"DN:{dn_show*100:.0f}¢[{dn_src}]  "
                        f"vol:${mkt['volume_24h']:,.0f}  "
                        f"ends:{mkt['secs_left']}s{ptb_info}{vpin_str}", "scan")
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
        log("Market finder started (deterministic slug, parallel, 90s refresh)", "sys")


def check_oracle_settlement(token_id, side=None, asset=None, timeframe=None, window_end=None):
    """
    Check if a market has been settled by Polymarket's oracle.
    Uses slug-based Gamma API query (NOT token_id — that returns wrong markets).
    Returns: (won: bool|None, ptb: float|None, final_price: float|None)
      - won: True (won), False (lost), or None (not settled yet)
      - ptb: price_to_beat from Gamma (if available)
      - final_price: settlement price from Gamma (if available)
    """
    if not asset or not timeframe or not window_end:
        return None, None, None

    # Reconstruct the slug for this market's window
    asset_cfg = next((a for a in ASSETS if a["name"] == asset), None)
    tf_cfg = next((t for t in TFS if t["label"] == timeframe), None)
    if not asset_cfg or not tf_cfg:
        return None, None, None

    window_start = int(window_end) - tf_cfg["secs"]
    slug = f"{asset_cfg['slug_pfx']}-updown-{tf_cfg['label']}-{window_start}"

    try:
        r = requests.get(f"{C['gamma']}/events",
                        params={"slug": slug}, timeout=5)
        if r.ok:
            events = r.json()
            if isinstance(events, list) and events:
                ev = events[0]
                meta = ev.get("eventMetadata") or {}
                markets = ev.get("markets") or []

                # Extract PTB and finalPrice from event metadata
                ptb = None
                final_price = None
                if meta.get("priceToBeat") is not None:
                    try:
                        ptb = float(meta["priceToBeat"])
                    except (ValueError, TypeError):
                        pass
                if meta.get("finalPrice") is not None:
                    try:
                        final_price = float(meta["finalPrice"])
                    except (ValueError, TypeError):
                        pass

                # The event has ONE market with outcomes: ["Up", "Down"]
                # outcomePrices[0] = Up price, outcomePrices[1] = Down price
                # We must check the price for OUR side's outcome index.
                m = markets[0] if markets else None
                if m:
                    # Determine which index corresponds to our side
                    outcomes = m.get("outcomes") or []
                    if isinstance(outcomes, str):
                        outcomes = json.loads(outcomes)
                    our_idx = None
                    for i, oc in enumerate(outcomes):
                        if side == "UP" and oc.lower() in ("up", "yes", "higher", "above"):
                            our_idx = i
                            break
                        elif side == "DOWN" and oc.lower() in ("down", "no", "lower", "below"):
                            our_idx = i
                            break
                    if our_idx is None:
                        # Default: UP=0, DOWN=1
                        our_idx = 0 if side == "UP" else 1

                    # Method 1: umaResolutionStatus + outcomePrices
                    resolved = m.get("umaResolutionStatus") == "resolved" or m.get("closed")
                    if resolved:
                        raw = m.get("outcomePrices")
                        if raw:
                            prices = json.loads(raw) if isinstance(raw, str) else raw
                            if len(prices) > our_idx:
                                our_price = float(prices[our_idx])
                                if our_price >= 0.95:
                                    return True, ptb, final_price
                                elif our_price <= 0.05:
                                    return False, ptb, final_price

                    # Method 2: PTB vs finalPrice from eventMetadata
                    if ptb is not None and final_price is not None:
                        price_went_up = final_price >= ptb
                        if side == "UP":
                            return price_went_up, ptb, final_price
                        else:
                            return not price_went_up, ptb, final_price
    except Exception:
        pass

    return None, None, None  # Can't determine yet


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


# ──────────────────────────────────────────────────────────────
# POSITION TRACKER — holds open positions, resolves with real prices
# ──────────────────────────────────────────────────────────────
class Position:
    """A single open position in a binary market."""
    def __init__(self, asset, sym, timeframe, side, token_id, entry_price,
                 size, shares, window_end, entry_asset_price, ed,
                 price_to_beat=None, order_type="taker"):
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
        self.order_type = order_type    # "taker" (hit asks, 1.56% fee) or "maker" (post bids, 0% fee)

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
            "order_type": self.order_type,
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
            order_type=d.get("order_type", "taker"),
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
        ptb_str = f"  PTB:${pos.price_to_beat:,.2f}" if pos.price_to_beat else ""
        otype = f"[{pos.order_type}]" if getattr(pos, 'order_type', 'taker') == 'maker' else ""
        log(f"  [POS] OPENED {pos.asset} {pos.timeframe} {pos.side} {otype}  "
            f"entry:{pos.entry_price*100:.1f}c  "
            f"shares:{pos.shares:.2f}  "
            f"size:${pos.size:.2f}  "
            f"asset_price:${pos.entry_asset_price:,.2f}{ptb_str}  "
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

                buy_fee = pos_buy_fee(pos)
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

                buy_fee = pos_buy_fee(pos)
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
                buy_fee = pos_buy_fee(pos)
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

            # ── EXPIRY: settle using Gamma API (ground truth) ──
            # CRITICAL: Only trust Polymarket's own Gamma API for settlement.
            # Uses slug-based query → umaResolutionStatus, outcomePrices, eventMetadata.
            # No external price feeds — no index_fallback.
            if secs_left <= 0:
                time_past_expiry = now - pos.window_end
                if time_past_expiry < 10:
                    still_open.append(pos)
                    continue

                # Query Gamma API by slug (NOT by token_id — that returns wrong markets)
                oracle_win, oracle_ptb, oracle_fp = check_oracle_settlement(
                    pos.token_id, side=pos.side,
                    asset=pos.asset, timeframe=pos.timeframe,
                    window_end=pos.window_end)
                settle_source = "gamma"

                # Update PTB and final price if Gamma returned them
                if oracle_ptb is not None:
                    pos.price_to_beat = oracle_ptb

                if oracle_win is None and time_past_expiry < 300:
                    # Fast settlement: use Chainlink boundary snapshot after 30s
                    # Polymarket settles using Chainlink — our PTB snapshot at window_end
                    # IS the finalPrice. No need to wait for Gamma to reflect it.
                    if time_past_expiry >= 30 and pos.price_to_beat:
                        asset_cfg = next((a for a in ASSETS if a["name"] == pos.asset), None)
                        tf_cfg = next((t for t in TFS if t["label"] == pos.timeframe), None)
                        if asset_cfg and tf_cfg:
                            # The Chainlink price at the window END = finalPrice
                            # window_end IS the next window's start boundary
                            snap_key = (asset_cfg["sym"], tf_cfg["secs"], int(pos.window_end))
                            cl_fp = self.pf.ptb_snapshots.get(snap_key)
                            if cl_fp is not None:
                                price_went_up = cl_fp >= pos.price_to_beat
                                oracle_win = price_went_up if pos.side == "UP" else not price_went_up
                                oracle_fp = cl_fp
                                settle_source = "chainlink"
                                log(f"  [POS] Fast settle via Chainlink: {pos.asset} {pos.timeframe} {pos.side}  "
                                    f"ptb:${pos.price_to_beat:,.2f} fp:${cl_fp:,.2f} -> {'WIN' if oracle_win else 'LOSS'}", "sys")

                if oracle_win is None and time_past_expiry < 300:
                    # Gamma hasn't resolved yet — keep polling (up to 5 min)
                    if time_past_expiry > 20 and int(time_past_expiry) % 15 < 3:
                        log(f"  [POS] Waiting for Gamma resolution: {pos.asset} {pos.timeframe} {pos.side}  "
                            f"{int(time_past_expiry)}s past expiry", "sys")
                    still_open.append(pos)
                    continue

                if oracle_win is None:
                    # Gamma still not resolved after 5 min — force loss (extremely rare)
                    oracle_win = False
                    settle_source = "gamma_timeout"
                    log(f"  [POS] Gamma unresolved after {int(time_past_expiry)}s: "
                        f"{pos.asset} {pos.timeframe} {pos.side} — counting as LOSS", "warn")

                # outcomePrices often populates before finalPrice in Gamma.
                # Fast path: next window's PTB = current window's finalPrice.
                # This is available immediately when the next window opens.
                if oracle_win is not None and oracle_fp is None:
                    asset_cfg = next((a for a in ASSETS if a["name"] == pos.asset), None)
                    tf_cfg = next((t for t in TFS if t["label"] == pos.timeframe), None)
                    if asset_cfg and tf_cfg:
                        next_ws = int(pos.window_end)  # next window starts when this one ends
                        next_slug = f"{asset_cfg['slug_pfx']}-updown-{tf_cfg['label']}-{next_ws}"
                        try:
                            nr = requests.get(f"{C['gamma']}/events",
                                            params={"slug": next_slug}, timeout=5)
                            if nr.ok:
                                nev = nr.json()
                                if isinstance(nev, list) and nev:
                                    nmeta = nev[0].get("eventMetadata") or {}
                                    nptb = nmeta.get("priceToBeat")
                                    if nptb is not None:
                                        oracle_fp = float(nptb)
                        except Exception:
                            pass

                # If still no finalPrice, wait up to 60s for Gamma
                if oracle_win is not None and oracle_fp is None and time_past_expiry < 60:
                    still_open.append(pos)
                    continue

                win = oracle_win
                ref_price = pos.price_to_beat or pos.entry_asset_price
                fp_str = f"${oracle_fp:,.2f}" if oracle_fp else f"${index_price:,.2f}(no fp)"

                buy_fee = pos_buy_fee(pos)
                if win:
                    pos.pnl = (1.0 * pos.shares) - pos.size - buy_fee
                else:
                    pos.pnl = -pos.size - buy_fee

                pos.exit_price = 1.0 if win else 0.0
                pos.exit_asset_price = oracle_fp or index_price
                pos.resolved = True
                self.cancel_exit_order(pos)
                newly_closed.append(pos)
                ptb_str = f"ptb:${ref_price:,.2f}" if pos.price_to_beat else f"entry:${pos.entry_asset_price:,.2f}(no ptb)"
                log(f"  [POS] SETTLED {pos.asset} {pos.timeframe} {pos.side}  "
                    f"{'WIN' if win else 'LOSS'}  [{settle_source}]  "
                    f"{ptb_str}->fp:{fp_str}  "
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
# ORDER EXECUTOR — CLOB auth + wallet balance (maker posts from IronDomeV9)
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
# IRON DOME v9 — Pure Market Maker
# ──────────────────────────────────────────────────────────────
class IronDomeV9:
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
        self._live_client = ex._live_client  # share auth'd client for live maker orders
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
        log("  IRON DOME v9  —  Pure Market Maker", "sys")
        log(f"  Balance: ${self.bal:.2f}  |  Fee: 0% (maker)", "sys")
        log(f"  Latency: {self.lat.avg_ms():.0f}ms avg  |  Fill: ~{self.lat.fill_rate()*100:.0f}%  |  {self.lat.label()}", "sys")
        log(f"  Assets: BTC / ETH / SOL  |  5m + 15m", "sys")
        log(f"  Strategy: 4-tier maker ladder (SCOUT/CONFIRM/SCALE/LOAD)", "sys")
        log(f"  VPIN: smart-money flow detection from CLOB book depth", "sys")
        log(f"  Exits: stop_loss={C['stop_loss']*100:.1f}%  take_profit={C['take_profit']*100:.1f}%  hold-to-expiry", "sys")
        log(f"  Price: Binance WS + Chainlink RTDS WS (both real-time)", "sys")
        log(f"  CLOB: {'py-clob-client OK' if CLOB_OK else 'not installed (Gamma fallback)'}", "sys")
        log(f"  Mode: {'DRY RUN — no real money' if C['dry_run'] else '*** LIVE TRADING ***'}",
            "live" if C["dry_run"] else "warn")
        div()

    def _process_resolved(self):
        """Process positions that have been resolved by the tracker."""
        resolved = self.pos.get_resolved()
        for pos in resolved:
            self.trades += 1
            self.fees += pos_buy_fee(pos)
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
                "our_prob": pos.ed.get("our_prob", pos.entry_price),
                "net_edge": pos.ed.get("net_edge", 0),
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

            result_tag = (pos.result or "EXPIRED").upper().replace("_", " ")
            log(f"  RESOLVED: {pos.asset} {pos.timeframe} {pos.side} -> {result_tag}  "
                f"pnl:${pos.pnl:+.4f}  "
                f"held:{int(time.time() - pos.entry_time)}s", "win" if win else "loss")
            self.stats()
        if resolved:
            self._save_state()

    def available_balance(self):
        """Balance minus capital locked in open positions."""
        return max(0, self.bal - self.committed)

    # ── MAKER SNIPER (LADDER) ─────────────────────────────────
    # Posts limit bid orders across the window at escalating prices.
    # 4 tiers matching woshfq19 pattern: scout → confirm → scale → load.
    # 0% maker fee. Each tier fires once per (asset, tf, window, tier).
    #
    # Tier | Secs into 5m | Bid   | Size% | Conviction needed
    # ─────┼──────────────┼───────┼───────┼──────────────────
    #  1   | 5-60s        | 22-30c| 5%    | both_agree + >0.05%
    #  2   | 60-150s      | 35-42c| 10%   | both_agree + >0.08%
    #  3   | 150-240s     | 43-50c| 15%   | both_agree + >0.10%
    #  4   | 240-290s     | 55-65c| 20%   | both_agree + >0.15%
    MAKER_TIERS = [
        # (tier, secs_into_min, secs_into_max, bid_lo, bid_hi, size_pct, min_delta)
        (1,   5,  60, 0.212, 0.287, 0.05, 0.0005),   # scout
        (2,  60, 150, 0.318, 0.394, 0.10, 0.0008),   # confirm
        (3, 150, 240, 0.413, 0.476, 0.15, 0.0010),   # scale
        (4, 240, 290, 0.518, 0.637, 0.20, 0.0015),   # load
    ]

    def _maker_tier_tracker(self):
        """Track which (asset, tf, window_end, tier) already fired."""
        if not hasattr(self, '_maker_tiers_done'):
            self._maker_tiers_done = set()
        return self._maker_tiers_done

    def snipe_maker(self):
        """Ladder limit bids across the window at escalating prices."""
        avail = self.available_balance()
        if avail < C["min_bal"]:
            return

        markets = self.finder.get_all()
        for mkt in markets:
            secs_left = max(0, int(mkt["window_end"] - time.time()))
            tf_secs = 300 if mkt["timeframe"] == "5m" else 900
            secs_in = tf_secs - secs_left

            # Only active during the window (5s in to 290s for 5m)
            if secs_in < 5 or secs_left < 10:
                continue

            # ── DIRECTION from price feeds ──
            sym = next((a["sym"] for a in ASSETS if a["name"] == mkt["asset"]), None)
            ptb = mkt.get("price_to_beat")
            chainlink_price = self.pf.chainlink.get(sym) if sym else None
            binance_price = self.pf.prices.get(sym) if sym else None
            fast_price = chainlink_price or binance_price
            if not fast_price or not ptb or ptb <= 0:
                continue

            delta_pct = (fast_price - ptb) / ptb
            abs_delta = abs(delta_pct)

            both_agree = False
            if binance_price and chainlink_price and ptb > 0:
                bnc_delta = (binance_price - ptb) / ptb
                if (delta_pct > 0 and bnc_delta > 0) or (delta_pct < 0 and bnc_delta < 0):
                    both_agree = True

            if not both_agree:
                continue

            # Determine side from current price direction
            if delta_pct > 0:
                maker_side = "UP"
                maker_token = mkt.get("up_yes_token")
            else:
                maker_side = "DOWN"
                maker_token = mkt.get("dn_yes_token")

            if not maker_token:
                continue

            # ── VPIN: detect smart-money flow from CLOB book depth ──
            vpin_score, vpin_dir, vpin_detail = compute_vpin(mkt)

            # VPIN confirms direction: price feeds + order flow agree
            vpin_confirms = (vpin_dir == maker_side) and vpin_score > 0.15
            # VPIN contradicts: smart money positioning opposite to price
            vpin_contradicts = (vpin_dir is not None and vpin_dir != maker_side
                                and vpin_score > 0.30)

            # Block entry if VPIN strongly contradicts price direction
            if vpin_contradicts:
                log(f"  [MAKER] VPIN BLOCK {mkt['asset']} {mkt['timeframe']} {maker_side}  "
                    f"vpin:{vpin_score:.0%} -> {vpin_dir}  price -> {maker_side}  "
                    f"({vpin_detail})", "sys")
                continue

            # Scale secs_in for 15m windows (tier boundaries are for 5m)
            scaled_secs_in = secs_in if tf_secs == 300 else secs_in * 300 / 900

            # ── CHECK EACH TIER ──
            tracker = self._maker_tier_tracker()
            for tier, t_min, t_max, bid_lo, bid_hi, size_pct, min_delta in self.MAKER_TIERS:
                tier_key = (mkt["asset"], mkt["timeframe"], mkt["window_end"], tier)
                if tier_key in tracker:
                    continue

                # Check if we're in this tier's time window
                if scaled_secs_in < t_min or scaled_secs_in >= t_max:
                    continue

                # VPIN confirmation lowers delta threshold by 30%
                effective_delta = min_delta * 0.70 if vpin_confirms else min_delta

                # Check conviction threshold for this tier
                if abs_delta < effective_delta:
                    continue

                # Interpolate bid price within the tier's range
                t_frac = (scaled_secs_in - t_min) / max(1, t_max - t_min)
                bid_price = round(bid_lo + (bid_hi - bid_lo) * t_frac, 2)

                # ── KELLY SIZING per tier ──
                # Win probability: base from tier + boost from delta strength + VPIN
                #   T1: 50% base (early, uncertain)
                #   T2: 58% (direction confirming)
                #   T3: 65% (strong move)
                #   T4: 72% (near-certain)
                tier_win_base = {1: 0.50, 2: 0.58, 3: 0.65, 4: 0.72}
                win_prob = tier_win_base[tier]
                # Delta strength boost: stronger move = higher win prob (up to +10%)
                delta_boost = min(0.10, abs_delta * 50)  # 0.1% delta → +5%, 0.2% → +10%
                win_prob = min(0.85, win_prob + delta_boost)
                # VPIN boost
                if vpin_confirms:
                    win_prob = min(0.88, win_prob + 0.05)

                # Kelly: f* = (p * b - q) / b  where b = payoff ratio, p = win prob
                # Payoff: buy at bid_price, win → $1, 0% fee
                payoff = (1.0 - bid_price) / bid_price  # e.g. 30c → 2.33x
                loss_frac = 1.0  # lose entire stake
                kelly_raw = (win_prob * payoff - (1 - win_prob) * loss_frac) / payoff
                kelly_frac = max(0.0, kelly_raw * 0.5)  # half-Kelly: reduces variance

                # Apply Kelly to balance with tier floors and caps
                # Polymarket minimum order = $1
                #   T1: min $1   T2: min $2   T3: min $3   T4: min $5
                #   All tiers: max $50
                tier_min = {1: 1.0, 2: 2.0, 3: 3.0, 4: 5.0}
                # NOTE: Polymarket enforces $1 minimum per order
                size = avail * kelly_frac
                # VPIN confirmation boosts size by 25%
                if vpin_confirms:
                    size *= 1.25
                # Clamp: tier minimum → $50 max → don't exceed available
                size = max(tier_min[tier], size)
                size = min(size, 50.0, avail - C["min_bal"])
                if size < tier_min[tier] or avail - size < C["min_bal"]:
                    continue
                shares = size / bid_price

                expected_return = (1.0 - bid_price) / bid_price
                tier_names = {1: "SCOUT", 2: "CONFIRM", 3: "SCALE", 4: "LOAD"}
                tier_name = tier_names[tier]
                vpin_tag = f"  vpin:{vpin_score:.0%}->{vpin_dir}" if vpin_dir else ""

                log(f"  [MAKER T{tier}] {tier_name} {mkt['asset']} {mkt['timeframe']} {maker_side}  "
                    f"bid:{bid_price*100:.1f}c  ${size:.2f}  "
                    f"kelly:{kelly_frac*100:.1f}%(wp:{win_prob*100:.0f}%)  "
                    f"delta:{abs_delta*100:.3f}%{vpin_tag}  {secs_left}s left", "edge")

                # Mark tier as done
                tracker.add(tier_key)

                if C["dry_run"]:
                    # Fill probability: higher tier + higher bid = more likely
                    # T1@25c→15%, T2@40c→45%, T3@48c→60%, T4@60c→80%
                    fill_prob = min(0.90, max(0.10, (bid_price - 0.10) * 1.5 + tier * 0.05))

                    fill_hash = hashlib.md5(f"{tier_key}".encode()).hexdigest()
                    fill_roll = int(fill_hash[:8], 16) / 0xFFFFFFFF

                    if fill_roll > fill_prob:
                        log(f"  [MAKER T{tier}] NOT FILLED {mkt['asset']} {mkt['timeframe']} {maker_side}  "
                            f"bid:{bid_price*100:.1f}c  prob:{fill_prob*100:.0f}%  (sim)", "sys")
                        continue

                    log(f"  [MAKER T{tier}] FILLED {mkt['asset']} {mkt['timeframe']} {maker_side}  "
                        f"at {bid_price*100:.1f}c  shares:{shares:.1f}  ${size:.2f}  "
                        f"({fill_prob*100:.0f}% prob)", "dry")

                    entry_asset_price = chainlink_price or fast_price
                    ed = {
                        "side": maker_side, "token_id": maker_token,
                        "our_prob": bid_price, "mkt_prob": bid_price,
                        "net_edge": expected_return, "gross": expected_return,
                        "momentum": 0, "vol_regime": "normal",
                        "spread": 0.02, "total_cost": 0.0,
                        "confluence": False,
                    }
                    pos = Position(
                        asset=mkt["asset"], sym=sym, timeframe=mkt["timeframe"],
                        side=maker_side, token_id=maker_token,
                        entry_price=bid_price, size=size, shares=shares,
                        window_end=mkt["window_end"],
                        entry_asset_price=entry_asset_price, ed=ed,
                        price_to_beat=mkt.get("price_to_beat"),
                        order_type="maker",
                    )
                    self.pos.add(pos)
                    self.committed += size
                    self._save_state()

                    ptb_str = f"  PTB:${ptb:,.2f}" if ptb else ""
                    log(f"  [MAKER T{tier}] FILLED {mkt['asset']} {mkt['timeframe']} {maker_side}  "
                        f"at {bid_price*100:.1f}c  ${size:.2f}  "
                        f"ret:{expected_return*100:.0f}%{ptb_str}", "dry")

                else:
                    # ── LIVE: post limit BID ──
                    if not self._live_client:
                        log("  [MAKER] No live client — skipping", "warn")
                        continue
                    try:
                        args = OrderArgs(
                            token_id=maker_token,
                            price=bid_price,
                            size=round(shares, 2),
                            side=BUY,
                            fee_rate_bps=0,
                        )
                        signed = self._live_client.create_order(args)
                        resp = self._live_client.post_order(signed, OrderType.GTC)
                        oid = resp.get("orderID", "?")
                        log(f"  [MAKER T{tier}] LIVE BID {mkt['asset']} {mkt['timeframe']} {maker_side}  "
                            f"bid:{bid_price*100:.1f}c  shares:{shares:.1f}  ${size:.2f}  "
                            f"orderID:{oid[:14]}", "win")

                        entry_asset_price = chainlink_price or fast_price
                        ed = {
                            "side": maker_side, "token_id": maker_token,
                            "our_prob": bid_price, "mkt_prob": bid_price,
                            "net_edge": expected_return, "gross": expected_return,
                            "momentum": 0, "vol_regime": "normal",
                            "spread": 0.02, "total_cost": 0.0,
                            "confluence": False,
                        }
                        pos = Position(
                            asset=mkt["asset"], sym=sym, timeframe=mkt["timeframe"],
                            side=maker_side, token_id=maker_token,
                            entry_price=bid_price, size=size, shares=shares,
                            window_end=mkt["window_end"],
                            entry_asset_price=entry_asset_price, ed=ed,
                            price_to_beat=mkt.get("price_to_beat"),
                            order_type="maker",
                        )
                        pos.exit_order_id = oid
                        self.pos.add(pos)
                        self.committed += size
                        self._save_state()
                    except Exception as e:
                        log(f"  [MAKER T{tier}] Order failed: {e}", "loss")

                # Update available balance for next tier
                avail = self.available_balance()
                if avail < C["min_bal"]:
                    return

            # Purge old tier tracking (windows that ended > 5 min ago)
            cutoff = time.time() - 300
            stale = {k for k in tracker if k[2] < cutoff}
            tracker -= stale

    def run(self):
        self.active = True
        time.sleep(5)
        self.banner()
        log("System Online — scanning live markets (maker only)...", "sys")

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

            # ── Maker sniper: post ladder limit bids ──
            self.snipe_maker()

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

            time.sleep(C["scan_sec"])

        # Wait for remaining positions to resolve
        if self.pos.open_count() > 0:
            log(f"Waiting for {self.pos.open_count()} open positions to resolve...", "sys")
            while self.pos.open_count() > 0:
                self._process_resolved()
                time.sleep(2)

        log("Iron Dome v9 — offline.", "sys")
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


# ──────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 62)
    print("  IRON DOME v9 — Pure Market Maker")
    print("  BTC/ETH/SOL  |  5m + 15m Up/Down  |  Maker Ladder Only")
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

    bot = IronDomeV9(pf, finder, ex, lat_tracker, pos_tracker)

    try:
        bot.run()
    except KeyboardInterrupt:
        bot.active = False
        log("Interrupted.", "sys")
        bot.stats()
