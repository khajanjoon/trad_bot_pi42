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
    },
    "BTCINR": {
        "qty": 0.002, 
        "leverage": 150, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 1000,
    },
    "SOLINR": {
        "qty": 0.5, 
        "leverage": 100, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 10,
    },
    "LTCINR": {
        "qty": 0.7, 
        "leverage": 50, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 10,
    },
    "BNBINR": {
        "qty": 0.05, 
        "leverage": 75, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 10,
    },
    "BCHINR": {
        "qty": 0.07, 
        "leverage": 50, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 10,
    },
    "ZECINR": {
        "qty": 0.1, 
        "leverage": 40, 
        "price_precision": 0,
        "qty_precision": 3,
        "min_price": 10,
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
            raise Exception("❌ API KEYS NOT FOUND - Please check your .env file")
        
        # ===== STRATEGY SETTINGS =====
        self.drop_percent = 0.75
        self.target_percent = 0.75
        self.stop_loss_percent = 2.0
        self.max_buys = 8
        self.max_margin_usage = 0.6
        self.min_liq_buffer = 0.2
        
        # ===== STATE MANAGEMENT =====
        self.next_avg_price = {}
        self.last_update_time = datetime.now()
        self.is_running = True
        
        # Enhanced caching with safety
        self.positions_cache = {}
        self.prices_cache = {}
        self.known_positions = {}  # Track positions we know exist
        self.api_error_count = 0
        self.max_api_errors = 5
        
        # Cache timeouts
        self.position_cache_timeout = 10  # Increased from 5
        self.price_cache_timeout = 3
        
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
        print("🚀 PI42 MULTI-SYMBOL BOT STARTED")
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
                current_time = time.time()
                
                # Cache positions for all symbols
                for symbol in SYMBOL_CONFIG.keys():
                    position_found = None
                    for pos in positions:
                        if pos.get("contractPair") == symbol and float(pos.get("quantity", 0)) > 0:
                            position_found = {
                                "qty": float(pos.get("quantity", 0)),
                                "avg": float(pos.get("entryPrice", 0)),
                                "liq": float(pos.get("liquidationPrice", 0)),
                                "leverage": float(pos.get("leverage", 1)),
                                "unrealized_pnl": float(pos.get("unrealisedPnl", 0)),
                            }
                            # Store in known positions
                            self.known_positions[symbol] = position_found
                            break
                    
                    self.positions_cache[symbol] = (position_found, current_time)
                    
                print(f"Loaded {len([p for p in positions if float(p.get('quantity', 0)) > 0])} positions")
                return True
            else:
                print(f"⚠️ Failed to fetch positions: {res.status_code}")
                return False
                
        except Exception as e:
            print(f"⚠️ Error fetching all positions: {e}")
            return False
    
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
            "name": "PI42 Trading Bot",
            "manufacturer": "PI42",
            "model": "Futures Averaging Bot",
            "sw_version": "2.3",
        }
        
        sensors = [
            ("wallet_balance", "PI42 Wallet Balance", "INR", "mdi:wallet"),
            ("pnl_isolated", "PI42 Isolated PnL", "INR", "mdi:chart-line"),
            ("pnl_cross", "PI42 Cross PnL", "INR", "mdi:chart-areaspline"),
            ("total_pnl", "PI42 Total PnL", "INR", "mdi:chart-arc"),
            ("available_balance", "PI42 Available Balance", "INR", "mdi:cash"),
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
            # Return cached values if available
            if hasattr(self, 'balance'):
                return {"inr_balance": self.balance, "pnl_isolated": self.pnl_isolated, "pnl_cross": self.pnl_cross}
            raise
    
    # =========================
    # MQTT PUBLISHING
    # =========================
    def publish_mqtt_balance(self):
        """Publish balance and status to MQTT."""
        payload = {
            "wallet_balance": round(self.balance, 2),
            "pnl_isolated": round(self.pnl_isolated, 2),
            "pnl_cross": round(self.pnl_cross, 2),
            "total_pnl": round(self.total_pnl, 2),
            "available_balance": round(self.available_balance, 2),
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
    
    def publish_error(self, error_msg: str):
        """Publish error to MQTT."""
        payload = {
            "error": error_msg,
            "timestamp": int(time.time()),
            "level": "ERROR",
        }
        try:
            self.mqtt_client.publish(ERROR_TOPIC, json.dumps(payload), qos=1)
        except:
            pass
    
    # =========================
    # POSITION MANAGEMENT - SAFE VERSION
    # =========================
    def get_position(self, symbol: str, force_refresh: bool = False) -> Optional[Dict]:
        """
        Safe position fetching with fallback logic.
        Returns None only if we're sure there's no position.
        """
        current_time = time.time()
        
        # Check if we know this symbol has a position
        if symbol in self.known_positions and not force_refresh:
            # Return known position from cache if recent
            if symbol in self.positions_cache:
                cached_data, cache_time = self.positions_cache[symbol]
                if current_time - cache_time < self.position_cache_timeout:
                    return cached_data
        
        # Try to fetch from API with retry
        position_data = self._fetch_position_with_retry(symbol)
        
        if position_data:
            # Update caches
            self.positions_cache[symbol] = (position_data, current_time)
            self.known_positions[symbol] = position_data
            return position_data
        else:
            # API failed, check if we previously knew about a position
            if symbol in self.known_positions:
                print(f"⚠️ {symbol}: API failed, using last known position")
                return self.known_positions[symbol]
            
            # No known position and API failed
            self.positions_cache[symbol] = (None, current_time)
            return None
    
    def _fetch_position_with_retry(self, symbol: str, max_retries: int = 2) -> Optional[Dict]:
        """Fetch position with retry logic."""
        for attempt in range(max_retries):
            try:
                self.api_limiter.wait()
                timestamp = str(int(time.time() * 1000))
                
                # Try to get ALL positions first (more efficient)
                if attempt == 0:
                    params = {"timestamp": timestamp, "pageSize": 50}
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
                        return None  # No position found
                
                # Fallback to specific symbol query
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
                    return None  # No position found
                
                elif res.status_code == 429:
                    wait_time = 2 * (attempt + 1)
                    print(f"⚠️ Rate limited on position request for {symbol}, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 1 * (attempt + 1)
                    print(f"⚠️ Position API error for {symbol} (attempt {attempt + 1}): {e}")
                    time.sleep(wait_time)
                else:
                    print(f"❌ Failed to fetch position for {symbol} after {max_retries} attempts")
                    self.api_error_count += 1
                    return None
        
        return None
    
    # =========================
    # MARKET DATA
    # =========================
    def fetch_price(self, symbol: str) -> float:
        """Fetch current price for a symbol with caching."""
        current_time = time.time()
        
        # Check cache first
        if symbol in self.prices_cache:
            cached_price, cache_time = self.prices_cache[symbol]
            if current_time - cache_time < 2:
                return cached_price
        
        # Fetch from API
        try:
            self.api_limiter.wait()
            res = requests.post(
                f"{self.market_url}/v1/market/klines",
                json={"pair": symbol, "interval": "1m", "limit": 1},
                timeout=5
            )
            
            if res.status_code == 429:
                print(f"⚠️ Rate limited on price request for {symbol}")
                if symbol in self.prices_cache:
                    return self.prices_cache[symbol][0]
                raise Exception("Rate limited")
            
            res.raise_for_status()
            data = res.json()
            
            if not data:
                raise ValueError(f"No price data for {symbol}")
            
            price = float(data[-1]["close"])
            
            # Update cache
            self.prices_cache[symbol] = (price, current_time)
            return price
            
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Price fetch error for {symbol}: {e}")
            if symbol in self.prices_cache:
                return self.prices_cache[symbol][0]
            raise
    
    # =========================
    # TRADING STRATEGY - SAFE VERSION
    # =========================
    def get_signal(self, symbol: str, price: float, position: Optional[Dict], qty: float) -> str:
        """
        Safe signal generation.
        Returns 'hold' if position data is unreliable.
        """
        # If position is None but we previously knew about a position, be cautious
        if position is None and symbol in self.known_positions:
            print(f"⚠️ {symbol}: Position data unavailable, holding to prevent duplicate buys")
            return "hold"
        
        if not position:
            return "buy"
        
        # Calculate current buy count
        buy_count = int(position["qty"] / qty)
        if buy_count >= self.max_buys:
            return "hold"
        
        # Check liquidation buffer
        liq_gap = (price - position["liq"]) / price * 100
        if liq_gap < self.min_liq_buffer:
            print(f"⚠️ {symbol}: Liquidation buffer too low ({liq_gap:.2f}%), holding")
            return "hold"
        
        # Calculate next average price
        price_multiplier = 1 - (self.drop_percent / 100) * buy_count
        next_price = position["avg"] * price_multiplier
        
        # Store for display
        self.next_avg_price[symbol] = round(next_price, 2)
        
        # Check if price has dropped enough
        if price < next_price:
            drop_percent = (position["avg"] - price) / position["avg"] * 100
            print(f"📉 {symbol}: Price dropped {drop_percent:.2f}% from average")
            return "buy"
        
        return "hold"
    
    # =========================
    # ORDER MANAGEMENT
    # =========================
    def format_order_price(self, price: float, symbol: str) -> int:
        """Format price as integer for API."""
        cfg = SYMBOL_CONFIG.get(symbol, {})
        min_price = cfg.get("min_price", 1)
        
        rounded = round(price)
        if min_price > 1:
            rounded = round(rounded / min_price) * min_price
        
        return int(rounded)
    
    def calculate_order_prices(self, symbol: str, current_price: float) -> Tuple[int, int]:
        """Calculate take profit and stop loss prices."""
        tp_price = current_price * (1 + self.target_percent / 100)
        sl_price = current_price * (1 - self.stop_loss_percent / 100)
        
        tp_price_int = self.format_order_price(tp_price, symbol)
        sl_price_int = self.format_order_price(sl_price, symbol)
        
        # Ensure stop loss is reasonable
        current_price_int = self.format_order_price(current_price, symbol)
        if sl_price_int <= 0 or sl_price_int >= current_price_int:
            sl_price_int = max(1, int(current_price_int * 0.95))
        
        return tp_price_int, sl_price_int
    
    def place_order(self, symbol: str, qty: float, price: float) -> bool:
        """Place a market buy order."""
        self.order_limiter.wait()
        
        cfg = SYMBOL_CONFIG.get(symbol, {})
        qty_precision = cfg.get("qty_precision", 3)
        formatted_qty = round(qty, qty_precision)
        
        current_price_int = self.format_order_price(price, symbol)
        tp_price, sl_price = self.calculate_order_prices(symbol, price)
        
        timestamp = str(int(time.time() * 1000))
        
        # Create order payload - try without stop loss first if previous attempts failed
        payload = {
            "timestamp": timestamp,
            "placeType": "ORDER_FORM",
            "quantity": formatted_qty,
            "side": "BUY",
            "symbol": symbol,
            "type": "MARKET",
            "takeProfitPrice": tp_price,
            # Start without stop loss to avoid precision issues
            # "stopLossPrice": sl_price,
            "marginAsset": "INR",
            "deviceType": "WEB",
            "reduceOnly": False,
        }
        
        print(f"\n📤 Placing order for {symbol}:")
        print(f"   Qty: {formatted_qty}, Market Price: ~{current_price_int}")
        print(f"   TP: {tp_price}")
        
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
                print(f"✅ {symbol} BUY ORDER PLACED")
                print(f"   Order ID: {data.get('orderId', 'N/A')}")
                
                # Update known positions
                self.known_positions[symbol] = self.get_position(symbol, force_refresh=True)
                
                return True
            else:
                error_data = res.json() if res.content else {}
                print(f"❌ {symbol} Order failed: {res.status_code}") 
                print(f"   Error: {error_data}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"❌ {symbol} Order error: {e}")
            return False
    
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
        
        # Try a simple balance check
        try:
            self.api_limiter.wait()
            timestamp = str(int(time.time() * 1000))
            query = f"marginAsset=INR&timestamp={timestamp}"
            signature = generate_signature(self.secret_key, query)
            
            headers = {
                "api-key": self.api_key,
                "signature": signature,
                "Content-Type": "application/json"
            }
            
            res = requests.get(
                f"{self.base_url}/v1/wallet/futures-wallet/details",
                headers=headers,
                params={"marginAsset": "INR", "timestamp": timestamp},
                timeout=5
            )
            
            if res.status_code == 200:
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
        print("Starting main loop in MONITORING MODE")
        print("Trading will be cautious when API is unstable")
        print("="*60 + "\n")
        
        while self.is_running:
            try:
                iteration += 1
                current_time = datetime.now()
                
                # Check API health
                if not self.check_api_health():
                    print(f"⚠️ API unhealthy, waiting 60 seconds...")
                    time.sleep(60)
                    continue
                
                print(f"\n{'='*60}")
                print(f"🔄 Iteration {iteration} - {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'='*60}")
                
                # Update balance
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
                
                # Process each symbol
                for symbol, cfg in SYMBOL_CONFIG.items():
                    try:
                        qty = cfg["qty"]
                        
                        # Get price and position
                        price = self.fetch_price(symbol)
                        position = self.get_position(symbol)
                        
                        # Generate signal
                        signal = self.get_signal(symbol, price, position, qty)
                        
                        # Display status
                        display_avg = position["avg"] if position else 0
                        display_liq = position["liq"] if position else 0
                        display_qty = position["qty"] if position else 0
                        
                        status_line = (
                            f"{symbol:<8} | "
                            f"Price: {price:>9.2f} | "
                            f"Qty: {display_qty:>6.3f} | "
                            f"Avg: {display_avg:>9.2f} | "
                            f"Liq: {display_liq:>9.2f} | "
                            f"Signal: {signal:<4} | "
                            f"Next: {self.next_avg_price.get(symbol, 'N/A'):>9}"
                        )
                        
                        # Add warning if using cached position
                        if position is None and symbol in self.known_positions:
                            status_line += " (cached)"
                        
                        print(status_line)
                        
                        # Execute trade only if conditions are good
                        if signal == "buy" and self.check_risk_limits(symbol, qty, price):
                            # Extra safety check
                            if self.api_error_count < 5:  # Only trade if API is healthy
                                print(f"🎯 {symbol}: Executing buy order...")
                                if self.place_order(symbol, qty, price):
                                    time.sleep(3)  # Delay after order
                            else:
                                print(f"⚠️ {symbol}: Skipping trade due to API issues")
                        
                        # Delay between symbols
                        time.sleep(1)
                        
                    except Exception as e:
                        print(f"❌ Error processing {symbol}: {e}")
                        continue
                
                # Wait for next iteration
                wait_time = 5
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
        bot = TradingBot()
        bot.run()
    except Exception as e:
        print(f"❌ Failed to start bot: {e}")
        print(traceback.format_exc())
        sys.exit(1)
