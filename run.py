import hashlib
import hmac
import json
import os
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple, Any
import requests
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import signal
import sys

# =========================
# CONFIGURATION
# =========================
MQTT_BROKER = "45.120.136.157"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "pi42_trading_bot"

HA_PREFIX = "homeassistant"
DEVICE_ID = "pi42_trading_bot"
STATE_TOPIC = "pi42/account/state"
ERROR_TOPIC = "pi42/account/errors"

# Symbol configuration
SYMBOL_CONFIG = {
    "ETHINR": {
        "qty": 0.02, 
        "leverage": 150, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 100,
        "tick_size": 10,
        "maker_spread": 0.07,
        "target_spread": 0.75,
        "stop_loss_spread": 2.0,
    },
    "BTCINR": {
        "qty": 0.002, 
        "leverage": 150, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 1000,
        "tick_size": 100,
        "maker_spread": 0.15,
        "target_spread": 0.75,
        "stop_loss_spread": 2.0,
    },
    "SOLINR": {
        "qty": 0.5, 
        "leverage": 100, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 10,
        "tick_size": 1,
        "maker_spread": 0.15,
        "target_spread": 0.75,
        "stop_loss_spread": 2.0,
    },
    "LTCINR": {
        "qty": 0.7, 
        "leverage": 50, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 10,
        "tick_size": 1,
        "maker_spread": 0.15,
        "target_spread": 0.75,
        "stop_loss_spread": 2.0,
    },
    "BNBINR": {
        "qty": 0.05, 
        "leverage": 75, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 10,
        "tick_size": 1,
        "maker_spread": 0.15,
        "target_spread": 0.75,
        "stop_loss_spread": 2.0,
    },
    "BCHINR": {
        "qty": 0.07, 
        "leverage": 50, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 10,
        "tick_size": 1,
        "maker_spread": 0.15,
        "target_spread": 0.75,
        "stop_loss_spread": 2.0,
    },
    "ZECINR": {
        "qty": 0.1, 
        "leverage": 40, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 10,
        "tick_size": 1,
        "maker_spread": 0.15,
        "target_spread": 0.75,
        "stop_loss_spread": 2.0,
    },
}

# =========================
# SIGNATURE
# =========================
def generate_signature(secret: str, message: str) -> str:
    """Generate HMAC SHA256 signature."""
    return hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


# =========================
# API RATE LIMITER
# =========================
class RateLimiter:
    def __init__(self, calls_per_second: float = 2.0):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self.last_call_time = 0
    
    def wait(self):
        """Wait if necessary to maintain rate limit."""
        current_time = time.time()
        elapsed = current_time - self.last_call_time
        
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            time.sleep(sleep_time)
        
        self.last_call_time = time.time()


# =========================
# SIMPLE MAKER ORDER BOT WITH CORRECT CANCEL ORDER API
# =========================
class SimpleMakerBot:
    def __init__(self):
        load_dotenv()
        
        self.base_url = "https://fapi.pi42.com"
        self.market_url = "https://api.pi42.com"
        
        self.api_key = os.getenv("PI42_API_KEY")
        self.secret_key = os.getenv("PI42_API_SECRET")
        
        if not self.api_key or not self.secret_key:
            raise Exception("❌ API KEYS NOT FOUND - Please check your .env file")
        
        # ===== STRATEGY SETTINGS =====
        self.drop_percent = 0.75
        self.max_buys = 8
        self.max_margin_usage = 0.6
        self.min_liq_buffer = 0.2
        self.maker_order_timeout = 120
        
        # ===== STATE MANAGEMENT =====
        self.next_avg_price = {}
        self.last_update_time = datetime.now()
        self.is_running = True
        
        # Order tracking
        self.pending_orders = {}  # Track pending maker orders
        self.last_order_check = 0
        self.order_check_interval = 30
        
        # Statistics
        self.orders_placed = 0
        self.orders_filled = 0
        self.orders_cancelled = 0
        
        # Simple tracking
        self.api_error_count = 0
        self.max_api_errors = 5
        
        # ===== RATE LIMITERS =====
        self.api_limiter = RateLimiter(calls_per_second=1.0)
        self.order_limiter = RateLimiter(calls_per_second=0.3)
        
        # ===== MQTT SETUP =====
        self.mqtt_client = mqtt.Client(
            client_id=MQTT_CLIENT_ID,
            protocol=mqtt.MQTTv311,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
        )
        
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
        self._connect_mqtt()
        
        # ===== HOME ASSISTANT DISCOVERY =====
        self.publish_ha_discovery()
        
        # ===== SIGNAL HANDLERS =====
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # ===== INITIALIZE =====
        self._initialize_balance_and_positions()
        
        print("\n" + "="*60)
        print("🚀 PI42 MAKER BOT WITH CORRECT CANCEL ORDER API")
        print(f"📅 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"💰 Wallet Balance: {self.balance:.2f} INR")
        print(f"📈 PnL Isolated: {self.pnl_isolated:.2f} INR")
        print(f"📊 Trading {len(SYMBOL_CONFIG)} symbols")
        print("="*60 + "\n")
    
    # =========================
    # INITIALIZATION
    # =========================
    def _initialize_balance_and_positions(self):
        """Initialize balance and fetch all positions with retry."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"Initializing (attempt {attempt + 1})...")
                self._update_balance()
                
                # Fetch all positions at startup
                print("Fetching initial positions...")
                self._fetch_all_positions()
                
                # Fetch and cancel any existing open orders
                self.fetch_and_clean_open_orders()
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"⚠️ Initialization failed (attempt {attempt + 1}): {e}")
                    time.sleep(3)
                else:
                    raise
    
    def _fetch_all_positions(self):
        """Fetch all open positions at once to avoid multiple API calls."""
        try:
            self.api_limiter.wait()
            timestamp = str(int(time.time() * 1000))
            params = {
                "timestamp": timestamp,
                "pageSize": 50,
            }
            
            query = "&".join(f"{k}={v}" for k, v in params.items())
            signature = generate_signature(self.secret_key, query)
            
            headers = {
                "api-key": self.api_key,
                "signature": signature,
                "Content-Type": "application/json"
            }
            
            res = requests.get(
                f"{self.base_url}/v1/positions/OPEN",
                headers=headers,
                params=params,
                timeout=10
            )
            
            if res.status_code == 200:
                positions = res.json()
                print(f"Loaded {len([p for p in positions if float(p.get('quantity', 0)) > 0])} positions")
                return True
            else:
                print(f"⚠️ Failed to fetch positions: {res.status_code}")
                return False
                
        except Exception as e:
            print(f"⚠️ Error fetching all positions: {e}")
            return False
    
    # =========================
    # OPEN ORDERS API MANAGEMENT
    # =========================
    def fetch_open_orders(self, symbol: str = None) -> List[Dict]:
        """
        Fetch open orders from PI42 API using /v1/order/open-orders endpoint.
        """
        try:
            self.api_limiter.wait()
            timestamp = str(int(time.time() * 1000))
            
            params = {"timestamp": timestamp}
            signature = generate_signature(self.secret_key, f"timestamp={timestamp}")
            
            headers = {
                'api-key': self.api_key,
                'signature': signature,
            }
            
            open_orders_url = f"{self.base_url}/v1/order/open-orders"
            response = requests.get(open_orders_url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                response_data = response.json()
                
                if symbol:
                    filtered_orders = []
                    for order in response_data:
                        if order.get("symbol") == symbol:
                            filtered_orders.append(order)
                    return filtered_orders
                else:
                    return response_data
                    
            elif response.status_code == 404:
                return []
            else:
                print(f"⚠️ Failed to fetch open orders: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            print(f"⚠️ Error fetching open orders: {str(e)}")
            return []
    
    def fetch_and_clean_open_orders(self):
        """Fetch all open orders and clean up old ones."""
        print("🔍 Fetching open orders from API...")
        
        open_orders = self.fetch_open_orders()
        
        if not open_orders:
            print("✅ No open orders found")
            return
        
        print(f"📋 Found {len(open_orders)} open orders")
        
        self.pending_orders.clear()
        
        for order in open_orders:
            try:
                symbol = order.get("symbol")
                order_id = order.get("clientOrderId") or order.get("linkId")
                order_type = order.get("type")
                side = order.get("side")
                price = float(order.get("price", 0))
                order_amount = float(order.get("orderAmount", 0))
                filled_amount = float(order.get("filledAmount", 0))
                time_str = order.get("time", "")
                
                # Parse time
                try:
                    if time_str:
                        time_str_clean = time_str.split('.')[0] if '.' in time_str else time_str
                        time_str_clean = time_str_clean.replace('Z', '')
                        created_time = datetime.strptime(time_str_clean, "%Y-%m-%dT%H:%M:%S")
                        created_seconds = created_time.timestamp()
                    else:
                        created_seconds = time.time()
                except:
                    created_seconds = time.time()
                
                # Determine status
                if filled_amount == 0:
                    status = "NEW"
                elif filled_amount < order_amount:
                    status = "PARTIALLY_FILLED"
                else:
                    status = "FILLED"
                
                fill_percentage = (filled_amount / order_amount * 100) if order_amount > 0 else 0
                
                print(f"   {symbol} {side} {order_type} - ID: {order_id}, "
                      f"Price: {price}, Filled: {fill_percentage:.1f}%")
                
                # Track pending BUY LIMIT orders
                if side == "BUY" and order_type == "LIMIT" and status in ["NEW", "PARTIALLY_FILLED"]:
                    current_time = time.time()
                    age_seconds = current_time - created_seconds
                    
                    if age_seconds > self.maker_order_timeout:
                        print(f"⏰ {symbol}: Order {order_id} is {age_seconds:.0f}s old, cancelling...")
                        self.cancel_order(symbol, order_id)
                    else:
                        self.pending_orders[symbol] = {
                            "order_id": order_id,
                            "client_order_id": order_id,  # Store as client_order_id for cancel
                            "placed_time": created_seconds,
                            "price": price,
                            "quantity": order_amount,
                            "filled": filled_amount,
                            "fill_percentage": fill_percentage,
                            "status": status,
                            "order_type": order_type,
                            "attempts": 1,
                        }
                        print(f"✅ Tracking {symbol} order {order_id} ({fill_percentage:.1f}% filled)")
                
            except Exception as e:
                print(f"⚠️ Error processing order: {e}")
                continue
        
        print(f"📊 Now tracking {len(self.pending_orders)} pending orders")
    
    def get_open_order_by_id(self, symbol: str, order_id: str) -> Optional[Dict]:
        """Get a specific open order by ID."""
        try:
            open_orders = self.fetch_open_orders(symbol)
            
            if not open_orders:
                return None
            
            for order in open_orders:
                current_order_id = order.get("clientOrderId") or order.get("linkId")
                if current_order_id == order_id:
                    return order
            
            return None
            
        except Exception as e:
            print(f"⚠️ Error getting order by ID: {e}")
            return None
    
    def update_pending_orders_from_api(self):
        """Update pending orders status from API."""
        current_time = time.time()
        
        if current_time - self.last_order_check < self.order_check_interval:
            return
        
        self.last_order_check = current_time
        print("🔍 Checking pending orders status from API...")
        
        symbols_to_remove = []
        
        for symbol, order_info in list(self.pending_orders.items()):
            order_id = order_info.get("order_id")
            
            if not order_id:
                continue
            
            order_data = self.get_open_order_by_id(symbol, order_id)
            
            if order_data:
                order_amount = float(order_data.get("orderAmount", 0))
                filled_amount = float(order_data.get("filledAmount", 0))
                fill_percentage = (filled_amount / order_amount * 100) if order_amount > 0 else 0
                
                if filled_amount == 0:
                    status = "NEW"
                elif filled_amount < order_amount:
                    status = "PARTIALLY_FILLED"
                else:
                    status = "FILLED"
                
                order_info["filled"] = filled_amount
                order_info["fill_percentage"] = fill_percentage
                order_info["status"] = status
                
                if status in ["FILLED"]:
                    print(f"✅ {symbol}: Order {order_id} is {status} ({fill_percentage:.1f}% filled)")
                    
                    if status == "FILLED":
                        self.orders_filled += 1
                        print(f"🎉 Order filled! Total filled: {self.orders_filled}")
                    
                    symbols_to_remove.append(symbol)
                elif status in ["PARTIALLY_FILLED"]:
                    print(f"⏳ {symbol}: Order {order_id} is {status} ({fill_percentage:.1f}% filled)")
                else:
                    print(f"⏳ {symbol}: Order {order_id} still {status}")
            else:
                print(f"⚠️ {symbol}: Order {order_id} not found in open orders")
                symbols_to_remove.append(symbol)
        
        for symbol in symbols_to_remove:
            self.pending_orders.pop(symbol, None)
            print(f"🗑️ Removed {symbol} from pending orders")
    
    # =========================
    # CANCEL ORDER API (CORRECTED)
    # =========================
    def cancel_order(self, symbol: str, order_id: str) -> bool:
        """
        Cancel an order using the correct DELETE /v1/order/delete-order endpoint.
        Uses clientOrderId parameter.
        """
        try:
            self.order_limiter.wait()
            
            timestamp = str(int(time.time() * 1000))
            
            # Prepare parameters - NOTE: Uses clientOrderId not orderId
            params = {
                'clientOrderId': order_id,
                'timestamp': timestamp
            }
            
            # Generate signature from JSON string
            data_to_sign = json.dumps(params, separators=(',', ':'))
            signature = generate_signature(self.secret_key, data_to_sign)
            
            headers = {
                'api-key': self.api_key,
                'Content-Type': 'application/json',
                'signature': signature,
            }
            
            delete_order_url = f"{self.base_url}/v1/order/delete-order"
            
            # Use DELETE method with json parameter
            response = requests.delete(delete_order_url, json=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                response_data = response.json()
                success = response_data.get("success", False)
                status = response_data.get("status", "")
                
                if success:
                    print(f"✅ Successfully cancelled order {order_id} for {symbol}")
                    self.orders_cancelled += 1
                    return True
                else:
                    print(f"⚠️ Cancel request failed for order {order_id}: {status}")
                    return False
            else:
                print(f"❌ Failed to cancel order {order_id} for {symbol}: {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except requests.exceptions.HTTPError as err:
            print(f"❌ HTTP Error cancelling order {order_id}: {err}")
            return False
        except Exception as e:
            print(f"⚠️ Error cancelling order: {e}")
            return False
    
    def cancel_all_pending_orders(self):
        """Cancel all pending orders from API."""
        print("\n🔄 Cancelling all open orders...")
        
        open_orders = self.fetch_open_orders()
        
        if not open_orders:
            print("✅ No open orders to cancel")
            return
        
        cancelled_count = 0
        for order in open_orders:
            try:
                symbol = order.get("symbol")
                order_id = order.get("clientOrderId") or order.get("linkId")
                order_type = order.get("type")
                side = order.get("side")
                
                # Only cancel BUY LIMIT orders
                if side == "BUY" and order_type == "LIMIT":
                    if self.cancel_order(symbol, order_id):
                        cancelled_count += 1
                    time.sleep(0.5)  # Rate limiting
            except Exception as e:
                print(f"⚠️ Error cancelling order: {e}")
                continue
        
        print(f"✅ Cancelled {cancelled_count} orders")
        self.pending_orders.clear()
    
    def check_and_clean_old_orders(self):
        """Check for old orders and clean them up."""
        current_time = time.time()
        orders_to_cancel = []
        
        for symbol, order_info in list(self.pending_orders.items()):
            placed_time = order_info.get("placed_time", 0)
            order_id = order_info.get("order_id")
            fill_percentage = order_info.get("fill_percentage", 0)
            
            if not order_id or placed_time == 0:
                continue
            
            age_seconds = current_time - placed_time
            
            # Only cancel if order is old AND not partially filled
            if age_seconds > self.maker_order_timeout and fill_percentage < 50:
                print(f"⏰ {symbol}: Order {order_id} is {age_seconds:.0f}s old and "
                      f"{fill_percentage:.1f}% filled, marking for cancellation")
                orders_to_cancel.append((symbol, order_id))
        
        for symbol, order_id in orders_to_cancel:
            if self.cancel_order(symbol, order_id):
                if symbol in self.pending_orders:
                    self.pending_orders.pop(symbol, None)
    
    # =========================
    # MQTT HANDLERS
    # =========================
    def _connect_mqtt(self):
        """Connect to MQTT broker with retry."""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                self.mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
                self.mqtt_client.loop_start()
                print(f"✅ Connected to MQTT broker")
                return
            except Exception as e:
                print(f"❌ MQTT connection failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    print("⚠️ Continuing without MQTT...")
    
    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("✅ MQTT connection established")
        else:
            print(f"❌ MQTT connection failed with code {rc}")
    
    def _on_mqtt_disconnect(self, client, userdata, rc):
        print(f"⚠️ MQTT disconnected (rc={rc})")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"\n⚠️ Received signal {signum}, shutting down gracefully...")
        self.is_running = False
        self.cancel_all_pending_orders()
        time.sleep(1)
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        sys.exit(0)
    
    # =========================
    # HOME ASSISTANT DISCOVERY
    # =========================
    def publish_ha_discovery(self):
        """Publish Home Assistant MQTT discovery topics."""
        device = {
            "identifiers": [DEVICE_ID],
            "name": "PI42 Maker Bot",
            "manufacturer": "PI42",
            "model": "Maker Bot v3",
            "sw_version": "3.0",
        }
        
        sensors = [
            ("wallet_balance", "PI42 Wallet Balance", "INR", "mdi:wallet"),
            ("pnl_isolated", "PI42 Isolated PnL", "INR", "mdi:chart-line"),
            ("pnl_cross", "PI42 Cross PnL", "INR", "mdi:chart-areaspline"),
            ("total_pnl", "PI42 Total PnL", "INR", "mdi:chart-arc"),
            ("available_balance", "PI42 Available Balance", "INR", "mdi:cash"),
            ("pending_orders", "PI42 Pending Orders", "count", "mdi:timer-sand"),
            ("orders_placed", "PI42 Orders Placed", "count", "mdi:order-bool-ascending"),
            ("orders_filled", "PI42 Orders Filled", "count", "mdi:order-bool-descending"),
            ("orders_cancelled", "PI42 Orders Cancelled", "count", "mdi:close-circle"),
            ("total_fill_percentage", "PI42 Total Fill %", "%", "mdi:percent"),
        ]
        
        for key, name, unit, icon in sensors:
            topic = f"{HA_PREFIX}/sensor/{DEVICE_ID}_{key}/config"
            payload = {
                "name": name,
                "state_topic": STATE_TOPIC,
                "value_template": f"{{{{ value_json.{key} }}}}",
                "unit_of_measurement": unit,
                "unique_id": f"{DEVICE_ID}_{key}",
                "icon": icon,
                "device": device,
            }
            
            try:
                self.mqtt_client.publish(topic, json.dumps(payload), qos=1, retain=True)
                time.sleep(0.1)
            except Exception as e:
                print(f"⚠️ Failed to publish HA discovery for {key}: {e}")
    
    # =========================
    # BALANCE MANAGEMENT
    # =========================
    def _update_balance(self):
        """Fetch and update balance information."""
        try:
            self.api_limiter.wait()
            bal = self.get_user_balance()
            
            self.balance = bal["inr_balance"]
            self.pnl_isolated = bal["pnl_isolated"]
            self.pnl_cross = bal["pnl_cross"]
            self.total_pnl = self.pnl_isolated + self.pnl_cross
            self.available_balance = max(0, self.balance - abs(self.pnl_isolated))
            self.last_update_time = datetime.now()
            
        except Exception as e:
            print(f"❌ Failed to update balance: {e}")
            raise
    
    def get_user_balance(self) -> Dict:
        """Fetch user balance from PI42 API."""
        timestamp = str(int(time.time() * 1000))
        query = f"marginAsset=INR&timestamp={timestamp}"
        signature = generate_signature(self.secret_key, query)
        
        headers = {
            "api-key": self.api_key,
            "signature": signature,
            "Content-Type": "application/json"
        }
        
        try:
            res = requests.get(
                f"{self.base_url}/v1/wallet/futures-wallet/details",
                headers=headers,
                params={"marginAsset": "INR", "timestamp": timestamp},
                timeout=10
            )
            
            if res.status_code == 429:
                print("⚠️ Rate limited on balance request, waiting...")
                time.sleep(5)
                return {"inr_balance": self.balance, "pnl_isolated": self.pnl_isolated, "pnl_cross": self.pnl_cross}
            
            res.raise_for_status()
            data = res.json()
            
            return {
                "inr_balance": float(data.get("inrBalance", 0)),
                "pnl_isolated": float(data.get("unrealisedPnlIsolated", 0)),
                "pnl_cross": float(data.get("unrealisedPnlCross", 0)),
            }
            
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Balance API error: {e}")
            if hasattr(self, 'balance'):
                return {"inr_balance": self.balance, "pnl_isolated": self.pnl_isolated, "pnl_cross": self.pnl_cross}
            raise
    
    # =========================
    # MQTT PUBLISHING
    # =========================
    def publish_mqtt_balance(self):
        """Publish balance and status to MQTT."""
        total_fill_percentage = 0
        if self.pending_orders:
            total_fill = sum(order.get("fill_percentage", 0) for order in self.pending_orders.values())
            total_fill_percentage = total_fill / len(self.pending_orders)
        
        payload = {
            "wallet_balance": round(self.balance, 2),
            "pnl_isolated": round(self.pnl_isolated, 2),
            "pnl_cross": round(self.pnl_cross, 2),
            "total_pnl": round(self.total_pnl, 2),
            "available_balance": round(self.available_balance, 2),
            "pending_orders": len(self.pending_orders),
            "orders_placed": self.orders_placed,
            "orders_filled": self.orders_filled,
            "orders_cancelled": self.orders_cancelled,
            "total_fill_percentage": round(total_fill_percentage, 1),
            "timestamp": int(time.time()),
            "last_update": self.last_update_time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        
        try:
            self.mqtt_client.publish(
                STATE_TOPIC,
                json.dumps(payload),
                qos=1,
                retain=False,
            )
        except Exception as e:
            print(f"⚠️ MQTT publish failed: {e}")
    
    # =========================
    # POSITION MANAGEMENT
    # =========================
    def get_position(self, symbol: str) -> Optional[Dict]:
        """Fetch position directly from API."""
        try:
            self.api_limiter.wait()
            timestamp = str(int(time.time() * 1000))
            
            params = {"symbol": symbol, "timestamp": timestamp, "pageSize": 10}
            query = "&".join(f"{k}={v}" for k, v in params.items())
            signature = generate_signature(self.secret_key, query)
            
            headers = {
                "api-key": self.api_key,
                "signature": signature,
                "Content-Type": "application/json"
            }
            
            res = requests.get(
                f"{self.base_url}/v1/positions/OPEN",
                headers=headers,
                params=params,
                timeout=8
            )
            
            if res.status_code == 200:
                positions = res.json()
                for pos in positions:
                    if pos.get("contractPair") == symbol and float(pos.get("quantity", 0)) > 0:
                        return {
                            "qty": float(pos.get("quantity", 0)),
                            "avg": float(pos.get("entryPrice", 0)),
                            "liq": float(pos.get("liquidationPrice", 0)),
                            "leverage": float(pos.get("leverage", 1)),
                            "unrealized_pnl": float(pos.get("unrealisedPnl", 0)),
                        }
                return None
                
            elif res.status_code == 429:
                print(f"⚠️ Rate limited on position request for {symbol}")
                return None
                
            else:
                print(f"⚠️ Failed to fetch position for {symbol}: {res.status_code}")
                self.api_error_count += 1
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Position API error for {symbol}: {e}")
            self.api_error_count += 1
            return None
    
    # =========================
    # MARKET DATA - LTP BASED
    # =========================
    def fetch_ltp(self, symbol: str) -> float:
        """Fetch Last Traded Price (LTP) for a symbol."""
        try:
            self.api_limiter.wait()
            res = requests.post(
                f"{self.market_url}/v1/market/klines",
                json={"pair": symbol, "interval": "1m", "limit": 1},
                timeout=5
            )
            
            if res.status_code == 429:
                print(f"⚠️ Rate limited on LTP request for {symbol}")
                raise Exception("Rate limited")
            
            res.raise_for_status()
            data = res.json()
            
            if not data:
                raise ValueError(f"No LTP data for {symbol}")
            
            ltp = float(data[-1]["close"])
            return ltp
            
        except requests.exceptions.RequestException as e:
            print(f"⚠️ LTP fetch error for {symbol}: {e}")
            raise
    
    # =========================
    # TRADING STRATEGY
    # =========================
    def get_signal(self, symbol: str, ltp: float, position: Optional[Dict], qty: float) -> str:
        """Generate trading signal based on position and LTP."""
        if symbol in self.pending_orders:
            order_info = self.pending_orders[symbol]
            order_id = order_info.get("order_id")
            
            if order_id:
                order_data = self.get_open_order_by_id(symbol, order_id)
                if order_data:
                    order_amount = float(order_data.get("orderAmount", 0))
                    filled_amount = float(order_data.get("filledAmount", 0))
                    
                    if filled_amount == 0:
                        status = "NEW"
                    elif filled_amount < order_amount:
                        status = "PARTIALLY_FILLED"
                    else:
                        status = "FILLED"
                    
                    if status in ["NEW", "PARTIALLY_FILLED"]:
                        fill_percentage = (filled_amount / order_amount * 100) if order_amount > 0 else 0
                        print(f"⏳ {symbol}: Open order exists (ID: {order_id}, Status: {status}, "
                              f"Filled: {fill_percentage:.1f}%)")
                        return "pending"
                    else:
                        print(f"⚠️ {symbol}: Order {order_id} is {status}, removing from pending")
                        if symbol in self.pending_orders:
                            if status == "FILLED":
                                self.orders_filled += 1
                                print(f"🎉 Order filled! Total filled: {self.orders_filled}")
                            self.pending_orders.pop(symbol, None)
                else:
                    print(f"⚠️ {symbol}: Order {order_id} not found in open orders")
                    if symbol in self.pending_orders:
                        self.pending_orders.pop(symbol, None)
        
        if not position:
            return "buy"
        
        buy_count = int(position["qty"] / qty)
        if buy_count >= self.max_buys:
            return "hold"
        
        liq_gap = (ltp - position["liq"]) / ltp * 100
        if liq_gap < self.min_liq_buffer:
            print(f"⚠️ {symbol}: Liquidation buffer too low ({liq_gap:.2f}%), holding")
            return "hold"
        
        price_multiplier = 1 - (self.drop_percent / 100) * buy_count
        next_price = position["avg"] * price_multiplier
        
        self.next_avg_price[symbol] = round(next_price, 2)
        
        if ltp < next_price:
            drop_percent = (position["avg"] - ltp) / position["avg"] * 100
            print(f"📉 {symbol}: LTP dropped {drop_percent:.2f}% from average")
            return "buy"
        
        return "hold"
    
    # =========================
    # ORDER MANAGEMENT
    # =========================
    def format_order_price(self, price: float, symbol: str) -> int:
        """Format price as integer for API, respecting tick size."""
        cfg = SYMBOL_CONFIG.get(symbol, {})
        min_price = cfg.get("min_price", 1)
        tick_size = cfg.get("tick_size", 1)
        
        if min_price > 1:
            rounded = round(price / min_price) * min_price
        else:
            rounded = round(price)
        
        if tick_size > 1:
            rounded = round(rounded / tick_size) * tick_size
        
        return int(rounded)
    
    def calculate_maker_price(self, symbol: str, ltp: float) -> int:
        """Calculate maker order price based on LTP."""
        cfg = SYMBOL_CONFIG.get(symbol, {})
        maker_spread = cfg.get("maker_spread", 0.15)
        tick_size = cfg.get("tick_size", 1)
        
        target_price = ltp * (1 - maker_spread / 100)
        formatted_price = self.format_order_price(target_price, symbol)
        
        ltp_formatted = self.format_order_price(ltp, symbol)
        if formatted_price >= ltp_formatted:
            formatted_price = ltp_formatted - tick_size
        
        if formatted_price < 1:
            formatted_price = 1
        
        return formatted_price
    
    def calculate_order_prices(self, symbol: str, entry_price: float) -> Tuple[int, int]:
        """Calculate take profit and stop loss prices."""
        cfg = SYMBOL_CONFIG.get(symbol, {})
        target_spread = cfg.get("target_spread", 0.75)
        stop_loss_spread = cfg.get("stop_loss_spread", 2.0)
        
        tp_price = entry_price * (1 + target_spread / 100)
        sl_price = entry_price * (1 - stop_loss_spread / 100)
        
        tp_price_int = self.format_order_price(tp_price, symbol)
        sl_price_int = self.format_order_price(sl_price, symbol)
        
        if sl_price_int <= 0 or sl_price_int >= entry_price:
            sl_price_int = max(1, int(entry_price * 0.95))
        
        return tp_price_int, sl_price_int
    
    def place_maker_order(self, symbol: str, qty: float, ltp: float) -> Optional[str]:
        """Place a LIMIT (maker) buy order."""
        self.order_limiter.wait()
        
        cfg = SYMBOL_CONFIG.get(symbol, {})
        qty_precision = cfg.get("qty_precision", 3)
        formatted_qty = round(qty, qty_precision)
        
        maker_price = self.calculate_maker_price(symbol, ltp)
        tp_price, sl_price = self.calculate_order_prices(symbol, maker_price)
        
        timestamp = str(int(time.time() * 1000))
        
        payload = {
            "timestamp": timestamp,
            "placeType": "ORDER_FORM",
            "quantity": formatted_qty,
            "side": "BUY",
            "symbol": symbol,
            "type": "LIMIT",
            "price": maker_price,
            "takeProfitPrice": tp_price,
            "marginAsset": "INR",
            "deviceType": "WEB",
            "timeInForce": "GTC",
            "reduceOnly": False,
        }
        
        ltp_formatted = self.format_order_price(ltp, symbol)
        spread_pct = ((ltp_formatted - maker_price) / ltp_formatted) * 100
        
        print(f"\n📤 Placing MAKER order for {symbol}:")
        print(f"   Type: LIMIT BUY")
        print(f"   Qty: {formatted_qty}")
        print(f"   LTP: {ltp_formatted}")
        print(f"   Maker Price: {maker_price} ({spread_pct:.2f}% below LTP)")
        print(f"   TP: {tp_price} (+{cfg.get('target_spread', 0.75):.2f}%)")
        print(f"   SL: {sl_price} (-{cfg.get('stop_loss_spread', 2.0):.2f}%)")
        
        signature = generate_signature(
            self.secret_key, json.dumps(payload, separators=(",", ":"))
        )
        
        headers = {
            "api-key": self.api_key,
            "signature": signature,
            "Content-Type": "application/json"
        }
        
        try:
            res = requests.post(
                f"{self.base_url}/v1/order/place-order",
                json=payload,
                headers=headers,
                timeout=10
            )
            
            if res.status_code == 201:
                data = res.json()
                order_id = data.get("orderId") or data.get("clientOrderId")
                print(f"✅ {symbol} MAKER ORDER PLACED")
                print(f"   Order ID: {order_id}")
                
                self.pending_orders[symbol] = {
                    "order_id": order_id,
                    "client_order_id": order_id,
                    "placed_time": time.time(),
                    "price": maker_price,
                    "quantity": formatted_qty,
                    "filled": 0,
                    "fill_percentage": 0,
                    "status": "NEW",
                    "order_type": "LIMIT",
                    "ltp_at_order": ltp,
                    "attempts": 1,
                }
                
                self.orders_placed += 1
                return order_id
            else:
                error_data = res.json() if res.content else {}
                print(f"❌ {symbol} Maker order failed: {res.status_code}") 
                print(f"   Error: {error_data}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ {symbol} Maker order error: {e}")
            return None
    
    # =========================
    # RISK MANAGEMENT
    # =========================
    def check_risk_limits(self, symbol: str, qty: float, price: float) -> bool:
        """Check if trade meets risk management criteria."""
        cfg = SYMBOL_CONFIG.get(symbol, {})
        leverage = cfg.get("leverage", 1)
        
        margin_needed = (qty * price) / leverage
        available_margin = self.available_balance * self.max_margin_usage
        
        if margin_needed > available_margin:
            print(f"⚠️ {symbol}: Insufficient margin. Needed: {margin_needed:.2f}, Available: {available_margin:.2f}")
            return False
        
        return True
    
    # =========================
    # API HEALTH CHECK
    # =========================
    def check_api_health(self) -> bool:
        """Check if API is responding properly."""
        if self.api_error_count > self.max_api_errors:
            print(f"⚠️ Too many API errors ({self.api_error_count}), pausing trading...")
            return False
        
        try:
            self.api_limiter.wait()
            timestamp = str(int(time.time() * 1000))
            signature = generate_signature(self.secret_key, f"timestamp={timestamp}")
            
            headers = {
                'api-key': self.api_key,
                'signature': signature,
            }
            
            res = requests.get(
                f"{self.base_url}/v1/order/open-orders",
                headers=headers,
                params={'timestamp': timestamp},
                timeout=5
            )
            
            if res.status_code in [200, 404]:
                self.api_error_count = max(0, self.api_error_count - 1)
                return True
            else:
                self.api_error_count += 1
                return False
                
        except:
            self.api_error_count += 1
            return False
    
    # =========================
    # MAIN LOOP
    # =========================
    def run(self):
        """Main trading loop."""
        iteration = 0
        
        print("="*60)
        print("Starting main loop - WITH CORRECT CANCEL ORDER API")
        print("="*60 + "\n")
        
        while self.is_running:
            try:
                iteration += 1
                current_time = datetime.now()
                
                if not self.check_api_health():
                    print(f"⚠️ API unhealthy, waiting 60 seconds...")
                    time.sleep(60)
                    continue
                
                print(f"\n{'='*60}")
                print(f"🔄 Iteration {iteration} - {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                self.update_pending_orders_from_api()
                
                print(f"📊 Pending Orders: {len(self.pending_orders)}")
                print(f"📈 Orders Placed: {self.orders_placed}, "
                      f"Filled: {self.orders_filled}, "
                      f"Cancelled: {self.orders_cancelled}")
                
                if self.pending_orders:
                    print("📋 Pending Orders:")
                    for symbol, order_info in self.pending_orders.items():
                        fill_pct = order_info.get("fill_percentage", 0)
                        price = order_info.get("price", 0)
                        order_id = order_info.get("order_id", "N/A")[:8]
                        print(f"   {symbol}: ID {order_id}... @{price} ({fill_pct:.1f}% filled)")
                
                print(f"{'='*60}")
                
                self.check_and_clean_old_orders()
                
                try:
                    self._update_balance()
                    self.publish_mqtt_balance()
                    
                    print(f"💰 Balance: {self.balance:.2f} INR | "
                          f"Available: {self.available_balance:.2f} INR | "
                          f"Total PnL: {self.total_pnl:+.2f} INR")
                    print(f"📊 API Errors: {self.api_error_count}/{self.max_api_errors}")
                    
                except Exception as e:
                    print(f"⚠️ Balance update failed: {e}")
                    time.sleep(5)
                    continue
                
                for symbol, cfg in SYMBOL_CONFIG.items():
                    try:
                        qty = cfg["qty"]
                        
                        ltp = self.fetch_ltp(symbol)
                        position = self.get_position(symbol)
                        
                        signal = self.get_signal(symbol, ltp, position, qty)
                        
                        display_avg = position["avg"] if position else 0
                        display_qty = position["qty"] if position else 0
                        
                        pending_info = self.pending_orders.get(symbol, {})
                        pending_price = pending_info.get("price", 0)
                        fill_pct = pending_info.get("fill_percentage", 0)
                        order_status = pending_info.get("status", "N/A")
                        
                        status_line = (
                            f"{symbol:<8} | "
                            f"LTP: {ltp:>9.2f} | "
                            f"Qty: {display_qty:>6.3f} | "
                            f"Avg: {display_avg:>9.2f} | "
                            f"Signal: {signal:<7} | "
                        )
                        
                        if pending_price > 0:
                            ltp_at_order = pending_info.get("ltp_at_order", 0)
                            price_diff_pct = ((ltp - ltp_at_order) / ltp_at_order * 100) if ltp_at_order > 0 else 0
                            status_line += f"Pending@{pending_price} ({order_status}, {fill_pct:.1f}% filled)"
                        else:
                            status_line += f"Next: {self.next_avg_price.get(symbol, 'N/A'):>9}"
                        
                        print(status_line)
                        
                        if signal == "buy" and self.check_risk_limits(symbol, qty, ltp):
                            print(f"🎯 {symbol}: Placing maker order...")
                            order_id = self.place_maker_order(symbol, qty, ltp)
                            if order_id:
                                time.sleep(2)
                        
                        time.sleep(1)
                        
                    except Exception as e:
                        print(f"❌ Error processing {symbol}: {e}")
                        continue
                
                wait_time = 15
                print(f"\n⏳ Waiting {wait_time} seconds until next update...")
                for i in range(wait_time):
                    if not self.is_running:
                        break
                    time.sleep(1)
                    
            except KeyboardInterrupt:
                print("\n⚠️ Bot stopped by user")
                self.is_running = False
                break
                
            except Exception as e:
                print(f"\n❌ Critical error in iteration {iteration}: {e}")
                print(traceback.format_exc())
                time.sleep(30)


# =========================
# START
# =========================
if __name__ == "__main__":
    try:
        bot = SimpleMakerBot()
        bot.run()
    except Exception as e:
        print(f"❌ Failed to start bot: {e}")
        print(traceback.format_exc())
        sys.exit(1)
