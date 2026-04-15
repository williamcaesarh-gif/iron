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

def test_geoblock():
    print()
    print("=" * 55)
    print("  GEOBLOCK CHECK")
    print("=" * 55)
    try:
        start = time.time()
        r = requests.get("https://polymarket.com/api/geoblock", timeout=10)
        ms = (time.time() - start) * 1000
        print(f"  Geoblock API         | HTTP {r.status_code} | {ms:6.0f}ms")
        print(f"  {'':20s} | response: {r.text[:200]}")
        try:
            data = r.json()
            blocked = data.get("blocked", data.get("restricted", "unknown"))
            print(f"  {'':20s} | blocked: {blocked}")
        except Exception:
            pass
    except Exception as e:
        print(f"  Geoblock API         | ERROR: {e}")


def test_order_endpoint():
    """Send a dummy POST to the CLOB order endpoint (no valid signature).
    We expect HTTP 400 (bad request) or 401 (unauthorized) = endpoint reachable.
    HTTP 403/451 or timeout = geoblocked."""
    print()
    print("=" * 55)
    print("  ORDER EXECUTION ENDPOINT")
    print("=" * 55)

    # POST /order — the actual endpoint py-clob-client hits
    url = "https://clob.polymarket.com/order"
    try:
        start = time.time()
        r = requests.post(url, json={"dummy": True}, timeout=10,
                          headers={"Content-Type": "application/json"})
        ms = (time.time() - start) * 1000

        if r.status_code in (400, 401, 422):
            status = "OK (reachable, rejected bad payload as expected)"
        elif r.status_code in (403, 451):
            status = "BLOCKED (geofenced)"
        elif r.status_code == 200:
            status = "OK (unexpected success)"
        else:
            status = f"UNKNOWN (check manually)"

        print(f"  POST /order          | HTTP {r.status_code} | {ms:6.0f}ms | {status}")
        print(f"  {'':20s} | body: {r.text[:100]}")
    except requests.exceptions.Timeout:
        print(f"  POST /order          | TIMEOUT          | LIKELY BLOCKED")
    except Exception as e:
        print(f"  POST /order          | ERROR: {e}")

    # Also test the cancel endpoint
    url2 = "https://clob.polymarket.com/cancel"
    try:
        start = time.time()
        r2 = requests.post(url2, json={"dummy": True}, timeout=10,
                           headers={"Content-Type": "application/json"})
        ms2 = (time.time() - start) * 1000

        if r2.status_code in (400, 401, 422):
            status2 = "OK (reachable)"
        elif r2.status_code in (403, 451):
            status2 = "BLOCKED"
        else:
            status2 = f"check manually"

        print(f"  POST /cancel         | HTTP {r2.status_code} | {ms2:6.0f}ms | {status2}")
    except Exception as e:
        print(f"  POST /cancel         | ERROR: {e}")


def main():
    print()
    print("  Iron Dome v10 — Amsterdam Endpoint Test")
    print()

    test_rest()

    if WS_OK:
        asyncio.run(test_ws())

    test_geoblock()
    test_order_endpoint()

    print()
    print("=" * 55)
    print("  DONE — all OK means no geoblock from this VPS")
    print("=" * 55)
    print()

if __name__ == "__main__":
    main()
