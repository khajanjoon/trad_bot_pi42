import hashlib
import hmac
import json
import os
import time
import requests
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

# =========================
# MQTT CONFIG
# =========================
MQTT_BROKER = "45.120.136.157"          # change if needed
MQTT_PORT = 1883
MQTT_TOPIC = "pi42/account"
MQTT_CLIENT_ID = "pi42_trading_bot"

# =========================
# SYMBOL CONFIG
# =========================
SYMBOL_CONFIG = {
    "ETHINR": {"qty": 0.02, "leverage": 150},
    "BTCINR": {"qty": 0.002, "leverage": 150},
    "SOLINR": {"qty": 0.5, "leverage": 100},
    "LTCINR": {"qty": 0.7, "leverage": 50},
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

        # ===== STRATEGY SETTINGS =====
        self.drop_percent = 0.75
        self.target_percent = 0.75
        self.max_buys = 8
        self.max_margin_usage = 0.6
        self.min_liq_buffer = 0.2

        self.next_avg_price = {}

        # ===== MQTT SETUP (paho-mqtt v2.x FIXED) =====
        self.mqtt_client = mqtt.Client(
            client_id=MQTT_CLIENT_ID,
            protocol=mqtt.MQTTv311,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
        )
        self.mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.mqtt_client.loop_start()

        # ===== INITIAL BALANCE =====
        bal = self.get_user_balance()
        self.balance = bal["inr_balance"]
        self.pnl_isolated = bal["pnl_isolated"]
        self.pnl_cross = bal["pnl_cross"]

        print("\n🚀 MULTI-SYMBOL BOT STARTED")
        print(f"Wallet Balance: {self.balance:.2f} INR")
        print(f"PnL Isolated: {self.pnl_isolated:.2f} INR\n")

        self.publish_mqtt_balance()

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
            timeout=10,
        )
        res.raise_for_status()
        data = res.json()

        return {
            "inr_balance": float(data.get("inrBalance", 0)),
            "wallet_balance": float(data.get("walletBalance", 0)),
            "withdrawable_balance": float(data.get("withdrawableBalance", 0)),
            "margin_balance": float(data.get("marginBalance", 0)),
            "locked_balance": float(data.get("lockedBalance", 0)),
            "pnl_cross": float(data.get("unrealisedPnlCross", 0)),
            "pnl_isolated": float(data.get("unrealisedPnlIsolated", 0)),
        }

    # =========================
    # MQTT PUBLISH
    # =========================
    def publish_mqtt_balance(self):
        payload = {
            "wallet_balance": round(self.balance, 2),
            "pnl_isolated": round(self.pnl_isolated, 2),
            "pnl_cross": round(self.pnl_cross, 2),
            "timestamp": int(time.time()),
        }

        self.mqtt_client.publish(
            MQTT_TOPIC,
            json.dumps(payload),
            qos=1,
            retain=False,
        )

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
    def place_order(self, symbol, side, qty, price):
        timestamp = str(int(time.time() * 1000))

        payload = {
            "timestamp": timestamp,
            "placeType": "ORDER_FORM",
            "quantity": qty,
            "side": side,
            "symbol": symbol,
            "type": "MARKET",
            "takeProfitPrice": int(price * (1 + self.target_percent / 100)),
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

        print(f"\n📨 {symbol} BUY RESPONSE")
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

        return "buy" if price < next_price else "hold"

    # =========================
    # MAIN LOOP
    # =========================
    def run(self):
        while True:
            try:
                bal = self.get_user_balance()
                self.balance = bal["inr_balance"]
                self.pnl_isolated = bal["pnl_isolated"]
                self.pnl_cross = bal["pnl_cross"]

                self.publish_mqtt_balance()

                for symbol, cfg in SYMBOL_CONFIG.items():
                    qty = cfg["qty"]
                    leverage = cfg["leverage"]

                    price = self.fetch_price(symbol)
                    position = self.get_position(symbol)
                    signal = self.get_signal(symbol, price, position, qty)

                    if signal == "buy":
                        margin_needed = (qty * price) / leverage
                        if margin_needed < self.balance * self.max_margin_usage:
                            self.place_order(symbol, "BUY", qty, price)

                    print(
                        f"Wallet={self.balance:.2f} | "
                        f"{symbol} | Price={price:.2f} | "
                        f"Qty={position['qty'] if position else 0} | "
                        f"Avg={position['avg'] if position else 0} | "
                        f"Liq={position['liq'] if position else 0} | "
                        f"Signal={signal}"
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
