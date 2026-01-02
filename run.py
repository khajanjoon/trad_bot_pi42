import hashlib
import hmac
import json
import os
import time
import requests
from dotenv import load_dotenv

# =========================
# SYMBOL CONFIG (MUST BE ABOVE CLASS)
# =========================
SYMBOL_CONFIG = {
    "ETHINR": {
        "qty": 0.02,
        "leverage": 150,
    },
    "BTCINR": {
        "qty": 0.002,
        "leverage": 150,
    },
    "SOLINR": {
        "qty": 0.05,
        "leverage": 100,
    },
}

# =========================
# SIGNATURE
# =========================
def generate_signature(secret, message):
    return hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


# =========================
# TRADING BOT
# =========================
class TradingBot:
    def __init__(self):
        load_dotenv()

        self.base_url = "https://fapi.pi42.com"
        self.market_url = "https://api.pi42.com"

        self.api_key = os.getenv("PI42_API_KEY")
        self.secret_key = os.getenv("PI42_API_SECRET")

        if not self.api_key or not self.secret_key:
            raise Exception("❌ API KEYS NOT FOUND")

        # ===== GLOBAL SETTINGS =====
        self.drop_percent = 0.75
        self.target_percent = 0.5
        self.max_buys = 8
        self.max_margin_usage = 0.6
        self.min_liq_buffer = 0.2

        self.balance = self.get_user_balance()
        self.next_avg_price = {}

        print("\n🚀 MULTI-SYMBOL BOT STARTED")
        print(f"Balance: {self.balance:.2f} INR\n")

    # =========================
    # FETCH BALANCE
    # =========================
    def get_user_balance(self):
        timestamp = str(int(time.time() * 1000))
        query = f"marginAsset=INR&timestamp={timestamp}"
        signature = generate_signature(self.secret_key, query)

        res = requests.get(
            f"{self.base_url}/v1/wallet/futures-wallet/details",
            headers={"api-key": self.api_key, "signature": signature},
            params={"marginAsset": "INR", "timestamp": timestamp},
        )
        res.raise_for_status()
        return float(res.json()["inrBalance"])

    # =========================
    # FETCH POSITION
    # =========================
    def get_position(self, symbol):
        timestamp = str(int(time.time() * 1000))
        params = {
            "symbol": symbol,
            "timestamp": timestamp,
            "pageSize": 50,
            "sortOrder": "desc",
        }

        query = "&".join(f"{k}={v}" for k, v in params.items())
        signature = generate_signature(self.secret_key, query)

        res = requests.get(
            f"{self.base_url}/v1/positions/OPEN",
            headers={"api-key": self.api_key, "signature": signature},
            params=params,
        )
        res.raise_for_status()

        for pos in res.json():
            if pos["contractPair"] == symbol and float(pos["quantity"]) > 0:
                return {
                    "qty": float(pos["quantity"]),
                    "avg": float(pos["entryPrice"]),
                    "liq": float(pos["liquidationPrice"]),
                }
        return None

    # =========================
    # PLACE ORDER
    # =========================
    def place_order(self, symbol, side, qty):
        timestamp = str(int(time.time() * 1000))

        payload = {
            "timestamp": timestamp,
            "placeType": "ORDER_FORM",
            "quantity": qty,
            "side": side,
            "symbol": symbol,
            "type": "MARKET",
            "marginAsset": "INR",
            "deviceType": "WEB",
            "userCategory": "EXTERNAL",
            "reduceOnly": False,
        }

        signature = generate_signature(
            self.secret_key, json.dumps(payload, separators=(",", ":"))
        )

        res = requests.post(
            f"{self.base_url}/v1/order/place-order",
            json=payload,
            headers={"api-key": self.api_key, "signature": signature},
        )

        print(f"\n📨 {symbol} {side} RESPONSE")
        print(json.dumps(res.json(), indent=2))

    # =========================
    # CLOSE SYMBOL POSITION
    # =========================
    def close_symbol(self, symbol):
        timestamp = str(int(time.time() * 1000))
        payload = {"timestamp": timestamp, "symbol": symbol}

        signature = generate_signature(
            self.secret_key, json.dumps(payload, separators=(",", ":"))
        )

        res = requests.delete(
            f"{self.base_url}/v1/positions/close-position",
            json=payload,
            headers={"api-key": self.api_key, "signature": signature},
        )

        print(f"\n❌ CLOSED {symbol}")
        print(json.dumps(res.json(), indent=2))

    # =========================
    # FETCH PRICE
    # =========================
    def fetch_price(self, symbol):
        res = requests.post(
            f"{self.market_url}/v1/market/klines",
            json={"pair": symbol, "interval": "1m", "limit": 1},
        )
        res.raise_for_status()
        return float(res.json()[-1]["close"])

    # =========================
    # STRATEGY
    # =========================
    def get_signal(self, symbol, price, position, qty):
        if not position:
            return "buy"

        buy_count = int(position["qty"] / qty)

        if buy_count >= self.max_buys:
            return "hold"

        liq_gap = (price - position["liq"]) / price * 100
        if liq_gap < self.min_liq_buffer:
            return "hold"

        next_price = position["avg"] * (1 - (self.drop_percent / 100) * buy_count)
        self.next_avg_price[symbol] = next_price

        if price < next_price:
            return "buy"

        if price > position["avg"] * (1 + self.target_percent / 100):
            return "sell"

        return "hold"

    # =========================
    # MAIN LOOP
    # =========================
    def run(self):
        while True:
            try:
                self.balance = self.get_user_balance()

                for symbol, cfg in SYMBOL_CONFIG.items():
                    qty = cfg["qty"]
                    leverage = cfg["leverage"]

                    price = self.fetch_price(symbol)
                    position = self.get_position(symbol)
                    signal = self.get_signal(symbol, price, position, qty)

                    if signal == "buy":
                        margin_needed = (qty * price) / leverage
                        if margin_needed < self.balance * self.max_margin_usage:
                            self.place_order(symbol, "BUY", qty)

                    elif signal == "sell":
                        self.close_symbol(symbol)

                    print(
                        f"{symbol} | Price={price:.2f} | "
                        f"Qty={position['qty'] if position else 0} | "
                        f"Avg={position['avg'] if position else 0} | "
                        f"Liq={position['liq'] if position else 0} | "
                        f"Signal={signal} | "
                        f"Next Avg={self.next_avg_price.get(symbol, 'N/A')}"
                    )

                time.sleep(10)

            except Exception as e:
                print("❌ ERROR:", e)
                time.sleep(5)


# =========================
# START
# =========================
if __name__ == "__main__":
    TradingBot().run()
