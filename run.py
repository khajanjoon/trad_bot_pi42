import hashlib
import hmac
import json
import os
import time
import traceback
import random
from datetime import datetime
from typing import Dict, Optional, List, Tuple
import requests
from dotenv import load_dotenv
import signal
import sys

# =========================
# SYMBOL CONFIG
# =========================
SYMBOL_CONFIG = {
    "XAGINR": {"qty": 1, "price_precision": 0, "qty_precision": 3, "avg_percent": 2, "target_percent": 1.0},
    "XAUINR": {"qty": 0.005, "price_precision": 0, "qty_precision": 3, "avg_percent": 2, "target_percent": 1.0},
                 }

# =========================
# SIGNATURE
# =========================
def generate_signature(secret: str, message: str) -> str:
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()

# =========================
# RATE LIMITER
# =========================
class RateLimiter:
    def __init__(self, calls_per_second=2.0):
        self.min_interval = 1.0 / calls_per_second
        self.last = 0

    def wait(self):
        now = time.time()
        diff = now - self.last
        if diff < self.min_interval:
            time.sleep(self.min_interval - diff)
        self.last = time.time()

# =========================
# ERROR HANDLER
# =========================
class APIErrorHandler:
    def __init__(self, retries=3, backoff=1.5):
        self.retries = retries
        self.backoff = backoff

    def retry_delay(self, attempt):
        return min(10, self.backoff ** attempt + random.uniform(0, 0.5))

# =========================
# BOT
# =========================
class AveragingBot:
    def __init__(self):
        load_dotenv()
        self.base_url = "https://fapi.pi42.com"
        self.market_url = "https://api.pi42.com"

        self.api_key = os.getenv("PI42_API_KEY")
        self.secret_key = os.getenv("PI42_API_SECRET")
        if not self.api_key or not self.secret_key:
            raise Exception("API keys missing")

        self.api_limiter = RateLimiter(1.5)
        self.order_limiter = RateLimiter(0.5)
        self.error_handler = APIErrorHandler()

        self.is_running = True
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        print("\n🚀 PI42 Averaging Bot Started\n")

    def stop(self, *args):
        print("Shutting down...")
        self.is_running = False
        sys.exit(0)

    # =========================
    # API WRAPPER
    # =========================
    def api_call(self, func, name):
        for i in range(self.error_handler.retries):
            try:
                self.api_limiter.wait()
                return func()
            except Exception as e:
                print(f"{name} failed: {e}")
                time.sleep(self.error_handler.retry_delay(i))
        return None

    # =========================
    # MARKET DATA
    # =========================
    def fetch_ltp(self, symbol):
        def f():
            r = requests.post(f"{self.market_url}/v1/market/klines",
                              json={"pair": symbol, "interval": "1m", "limit": 1}, timeout=10)
            r.raise_for_status()
            return float(r.json()[-1]["close"])
        return self.api_call(f, "LTP")

    # =========================
    # POSITION
    # =========================
    def get_position(self, symbol):
        def f():
            ts = str(int(time.time() * 1000))
            q = f"symbol={symbol}&timestamp={ts}&pageSize=10"
            sig = generate_signature(self.secret_key, q)
            h = {"api-key": self.api_key, "signature": sig}
            r = requests.get(f"{self.base_url}/v1/positions/OPEN", headers=h,
                             params={"symbol": symbol, "timestamp": ts, "pageSize": 10})
            r.raise_for_status()
            for p in r.json():
                if p["contractPair"] == symbol and float(p["quantity"]) > 0:
                    return float(p["quantity"]), float(p["entryPrice"])
            return None
        return self.api_call(f, "Position")

    # =========================
    # ORDER
    # =========================
    def format_price(self, price, symbol):
        return round(price, SYMBOL_CONFIG[symbol]["price_precision"])

    def place_market(self, symbol, qty):
        def f():
            ts = str(int(time.time() * 1000))
            payload = {
                "timestamp": ts,
                "placeType": "ORDER_FORM",
                "quantity": qty,
                "side": "BUY",
                "symbol": symbol,
                "type": "MARKET",
                "marginAsset": "INR",
                "deviceType": "WEB",
                "timeInForce": "GTC",
            }
            sig = generate_signature(self.secret_key, json.dumps(payload, separators=(",", ":")))
            h = {"api-key": self.api_key, "signature": sig, "Content-Type": "application/json"}
            r = requests.post(f"{self.base_url}/v1/order/place-order", json=payload, headers=h)
            r.raise_for_status()
            return r.json()
        return self.api_call(f, "Market Order")

    def place_tp(self, symbol, qty, entry):
        def f():
            ts = str(int(time.time() * 1000))
            tp = self.format_price(entry * 1.01, symbol)
            payload = {
                "timestamp": ts,
                "placeType": "ORDER_FORM",
                "quantity": qty,
                "side": "SELL",
                "symbol": symbol,
                "type": "STOP_LIMIT",
                "stopPrice": tp,
                "price": tp,
                "marginAsset": "INR",
                "deviceType": "WEB",
                "timeInForce": "GTC",
                "reduceOnly": True,
            }
            sig = generate_signature(self.secret_key, json.dumps(payload, separators=(",", ":")))
            h = {"api-key": self.api_key, "signature": sig, "Content-Type": "application/json"}
            r = requests.post(f"{self.base_url}/v1/order/place-order", json=payload, headers=h)
            r.raise_for_status()
            return True
        return self.api_call(f, "TP Order")

    # =========================
    # MAIN LOOP
    # =========================
    def run(self):
        while self.is_running:
            for symbol, cfg in SYMBOL_CONFIG.items():
                try:
                    ltp = self.fetch_ltp(symbol)
                    pos = self.get_position(symbol)

                    print(f"\n{symbol} LTP: {ltp}")

                    if pos:
                        qty, entry = pos
                        print(f"Position {qty} @ {entry}")
                        self.place_tp(symbol, qty, entry)
                    else:
                        print("No position, placing buy")
                        self.place_market(symbol, cfg["qty"])

                    time.sleep(1)

                except Exception as e:
                    print(f"{symbol} error:", e)

            print("---- Next Cycle ----")
            time.sleep(5)


if __name__ == "__main__":
    AveragingBot().run()
