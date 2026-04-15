#!/usr/bin/env python3
"""
Amsterdam VPS Endpoint Test — checks all endpoints Iron Dome v10 uses.
Deploy to your VPS and run: python amsterdamtest.py
"""

import time, sys

try:
    import requests
except ImportError:
    print("FATAL: pip install requests")
    sys.exit(1)

try:
    import websockets
    import asyncio
    WS_OK = True
except ImportError:
    WS_OK = False
    print("WARN: pip install websockets — WS tests will be skipped\n")

ENDPOINTS = [
    ("Gamma API", "https://gamma-api.polymarket.com/markets?limit=1"),
    ("CLOB REST", "https://clob.polymarket.com/time"),
    ("Binance Vision", "https://data-api.binance.vision/api/v3/ticker/price?symbol=BTCUSDT"),
]

WS_ENDPOINTS = [
    ("CLOB WebSocket", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
    ("Chainlink RTDS", "wss://ws-live-data.polymarket.com"),
    ("Binance WS", "wss://stream.binance.com:9443/ws/btcusdt@miniTicker"),
]

def test_rest():
    print("=" * 55)
    print("  REST ENDPOINTS")
    print("=" * 55)
    for name, url in ENDPOINTS:
        try:
            start = time.time()
            r = requests.get(url, timeout=10)
            ms = (time.time() - start) * 1000
            status = "OK" if r.status_code == 200 else "BLOCKED"
            print(f"  {name:20s} | HTTP {r.status_code} | {ms:6.0f}ms | {status}")
        except requests.exceptions.Timeout:
            print(f"  {name:20s} | TIMEOUT          | BLOCKED")
        except Exception as e:
            print(f"  {name:20s} | ERROR: {e}")

async def test_ws():
    print()
    print("=" * 55)
    print("  WEBSOCKET ENDPOINTS")
    print("=" * 55)
    for name, url in WS_ENDPOINTS:
        try:
            start = time.time()
            async with websockets.connect(url, open_timeout=10) as ws:
                ms = (time.time() - start) * 1000
                # Try to receive one message (2s timeout)
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=3)
                    data_preview = msg[:80] if isinstance(msg, str) else str(msg)[:80]
                    print(f"  {name:20s} | CONNECTED | {ms:6.0f}ms | OK")
                    print(f"  {'':20s} | data: {data_preview}")
                except asyncio.TimeoutError:
                    print(f"  {name:20s} | CONNECTED | {ms:6.0f}ms | OK (no data in 3s)")
        except Exception as e:
            err = str(e)[:60]
            print(f"  {name:20s} | FAILED: {err}")

def main():
    print()
    print("  Iron Dome v10 — Amsterdam Endpoint Test")
    print()

    test_rest()

    if WS_OK:
        asyncio.run(test_ws())

    print()
    print("=" * 55)
    print("  DONE — all OK means no geoblock from this VPS")
    print("=" * 55)
    print()

if __name__ == "__main__":
    main()
