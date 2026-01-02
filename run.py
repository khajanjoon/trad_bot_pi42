import hashlib
import hmac
import json
import os
import time
import requests
from dotenv import load_dotenv


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

        # ===== SETTINGS =====
        self.symbol = "ETHINR"
        self.qty = 0.02
        self.leverage = 150
        self.drop_percent = 0.75
        self.target_percent = 0.5
        self.max_buys = 8
        self.max_margin_usage = 0.6
        self.min_liq_buffer = .002

        self.next_avg_price = 0.0
        self.balance = self.get_user_balance()

        print("\n🚀 BOT STARTED")
        print(f"Balance: {self.balance:.2f} INR")
        print("⚠️ MODE: 150× LEVERAGE (EXTREME RISK)\n")

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
    # FETCH POSITION (API ONLY)
    # =========================
    def get_position(self):
        timestamp = str(int(time.time() * 1000))
        params = {
            "symbol": self.symbol,
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
            if pos["contractPair"] == self.symbol and float(pos["quantity"]) > 0:
                return {
                    "qty": float(pos["quantity"]),
                    "avg": float(pos["entryPrice"]),
                    "liq": float(pos["liquidationPrice"]),
                }

        return None

    # =========================
    # PLACE ORDER (PRINT RESPONSE)
    # =========================
    def place_order(self, side):
        timestamp = str(int(time.time() * 1000))

        payload = {
            "timestamp": timestamp,
            "placeType": "ORDER_FORM",
            "quantity": self.qty,
            "side": side,
            "symbol": self.symbol,
            "type": "MARKET",
            "marginAsset": "INR",
            "deviceType": "WEB",
            "userCategory": "EXTERNAL",
            'reduceOnly': False, 
        }

        signature = generate_signature(
            self.secret_key, json.dumps(payload, separators=(",", ":"))
        )

        response = requests.post(
            f"{self.base_url}/v1/order/place-order",
            json=payload,
            headers={"api-key": self.api_key, "signature": signature},
        )

        try:
            data = response.json()
        except Exception:
            print("❌ NON-JSON RESPONSE:", response.text)
            return None

        print("\n📨 ORDER RESPONSE")
        print(json.dumps(data, indent=2))
        return data

    # =========================
    # CLOSE ALL POSITIONS
    # =========================
    def close_all(self):
        timestamp = str(int(time.time() * 1000))
        payload = {"timestamp": timestamp}

        signature = generate_signature(
            self.secret_key, json.dumps(payload, separators=(",", ":"))
        )

        response = requests.delete(
            f"{self.base_url}/v1/positions/close-all-positions",
            json=payload,
            headers={"api-key": self.api_key, "signature": signature},
        )

        print("\n📨 CLOSE RESPONSE")
        print(json.dumps(response.json(), indent=2))
        return response.json()

    # =========================
    # FETCH LIVE PRICE
    # =========================
    def fetch_price(self):
        res = requests.post(
            f"{self.market_url}/v1/market/klines",
            json={"pair": self.symbol, "interval": "1m", "limit": 1},
        )
        res.raise_for_status()
        return float(res.json()[-1]["close"])

    # =========================
    # STRATEGY
    # =========================
    def get_signal(self, price, position):
        if not position:
            return "buy"

        qty = position["qty"]
        avg = position["avg"]
        liq = position["liq"]

        buy_count = int(qty / self.qty)

        if buy_count >= self.max_buys:
            return "hold"

        liq_gap = (price - liq) / price * 100
        if liq_gap < self.min_liq_buffer:
            print("🚨 LIQUIDATION TOO CLOSE")
            return "hold"

        self.next_avg_price = avg * (1 - (self.drop_percent / 100) * buy_count)

        if price < self.next_avg_price:
            return "buy"

        if price > avg * (1 + self.target_percent / 100):
            return "sell"

        return "hold"

    # =========================
    # EXECUTE TRADE
    # =========================
    def execute_trade(self, signal, price):
        if signal == "buy":
            margin_needed = (self.qty * price) / self.leverage
            if margin_needed > self.balance * self.max_margin_usage:
                print("⚠️ Margin limit reached")
                return
            self.place_order("BUY")

        elif signal == "sell":
            self.close_all()

    # =========================
    # MAIN LOOP
    # =========================
    def run(self):
        while True:
            try:
                price = self.fetch_price()
                self.balance = self.get_user_balance()
                position = self.get_position()

                signal = self.get_signal(price, position)
                self.execute_trade(signal, price)

                print(
                    f"Price={price:.2f} | "
                    f"Qty={position['qty'] if position else 0} | "
                    f"Avg={position['avg'] if position else 0} | "
                    f"Liq={position['liq'] if position else None} | "
                    f"Balance={self.balance:.2f} | "
                    f"Signal={signal} | "
                    f"NextAvg={self.next_avg_price}"
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
