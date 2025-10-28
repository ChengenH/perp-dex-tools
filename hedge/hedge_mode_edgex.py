import asyncio
import json
import signal
import logging
import os
import sys
import time
import requests
import argparse
import traceback
import csv
from decimal import Decimal
from typing import Tuple, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum
import threading
from collections import defaultdict

from lighter.signer_client import SignerClient
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.edgex import EdgeXClient
import websockets
from datetime import datetime
import pytz
import dotenv

dotenv.load_dotenv()


class OrderStatus(Enum):
    """Order status enumeration."""
    NEW = "NEW"
    OPEN = "OPEN"
    PENDING = "PENDING"
    CANCELING = "CANCELING"
    CANCELED = "CANCELED"
    CANCELLED = "CANCELLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"


class OrderSide(Enum):
    """Order side enumeration."""
    BUY = "buy"
    SELL = "sell"


@dataclass
class TradingConfig:
    """Trading configuration parameters."""
    ticker: str
    order_quantity: Decimal
    fill_timeout: int = 5
    iterations: int = 20
    edgex_ttl_seconds: int = 30
    price_adjustment_threshold: int = 5  # ticks
    max_price_adjustments: int = 2
    rate_limit_interval: float = 6.0
    waiting_log_interval: float = 10.0


@dataclass
class PerformanceMetrics:
    """Performance tracking metrics."""
    orders_placed: int = 0
    orders_filled: int = 0
    orders_canceled: int = 0
    total_latency_ms: float = 0.0
    edgex_api_calls: int = 0
    lighter_api_calls: int = 0
    websocket_reconnects: int = 0
    errors: int = 0
    price_adjustments: int = 0
    start_time: float = 0.0
    
    def __post_init__(self):
        self.start_time = time.time()
    
    def get_runtime_seconds(self) -> float:
        return time.time() - self.start_time
    
    def get_orders_per_minute(self) -> float:
        runtime = self.get_runtime_seconds()
        return (self.orders_placed / runtime * 60) if runtime > 0 else 0.0


class Config:
    """Simple config class to wrap dictionary for EdgeX client."""
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class TradingError(Exception):
    """Base exception for trading-related errors."""
    pass


class OrderError(TradingError):
    """Exception raised for order-related errors."""
    pass


class NetworkError(TradingError):
    """Exception raised for network-related errors."""
    pass


class RateLimitError(TradingError):
    """Exception raised for rate limit errors."""
    pass


class ErrorHandler:
    """Centralized error handling and recovery."""
    
    def __init__(self, logger: logging.Logger, metrics: PerformanceMetrics):
        self.logger = logger
        self.metrics = metrics
    
    def handle_error(self, error: Exception, context: str = "") -> bool:
        """Handle an error and return whether to retry."""
        self.metrics.errors += 1
        
        if isinstance(error, RateLimitError):
            self.logger.warning(f"Rate limit hit: {error}")
            return True
        elif isinstance(error, NetworkError):
            self.logger.warning(f"Network error: {error}")
            return True
        elif isinstance(error, OrderError):
            self.logger.error(f"Order error: {error}")
            return False
        else:
            self.logger.error(f"Unexpected error in {context}: {error}")
            return False


class HedgeBot:
    """Trading bot that places post-only orders on EdgeX and hedges with market orders on Lighter."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self.ticker = config.ticker
        self.order_quantity = config.order_quantity
        self.fill_timeout = config.fill_timeout
        self.lighter_order_filled = False
        self.iterations = config.iterations
        self.edgex_position = Decimal('0')
        self.lighter_position = Decimal('0')
        self.current_order = {}
        
        # Performance tracking
        self.metrics = PerformanceMetrics()
        
        # Initialize logging to file
        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/edgex_{self.ticker}_hedge_mode_log.txt"
        self.csv_filename = f"logs/edgex_{self.ticker}_hedge_mode_trades.csv"
        self.original_stdout = sys.stdout

        # Initialize CSV file with headers if it doesn't exist
        self._initialize_csv_file()

        # Setup logger
        self.logger = logging.getLogger(f"hedge_bot_{self.ticker}")
        self.logger.setLevel(logging.INFO)
        
        # Initialize error handler
        self.error_handler = ErrorHandler(self.logger, self.metrics)

        # Clear any existing handlers to avoid duplicates
        self.logger.handlers.clear()

        # Disable verbose logging from external libraries
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)

        # Create file handler
        file_handler = logging.FileHandler(self.log_filename)
        file_handler.setLevel(logging.INFO)

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        # Create different formatters for file and console
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)

        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # Prevent propagation to root logger to avoid duplicate messages
        self.logger.propagate = False

        # State management
        self.stop_flag = False
        self.order_counter = 0

        # EdgeX state
        self.edgex_client = None
        self.edgex_contract_id = None
        self.edgex_tick_size = None
        self.edgex_order_status = None

        # EdgeX order book state for websocket-based BBO (we may not have a public WS here, fallback to REST)
        self.edgex_order_book_ready = False
        self.edgex_best_bid = None
        self.edgex_best_ask = None

        # Lighter order book state
        self.lighter_client = None
        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid = None
        self.lighter_best_ask = None
        self.lighter_order_book_ready = False
        self.lighter_order_book_offset = 0
        self.lighter_order_book_sequence_gap = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_lock = asyncio.Lock()
        # EdgeX simple rate limit guard (2 ops / 2s -> ~1.1s min interval)
        self.edgex_rate_limit_lock = asyncio.Lock()
        self.edgex_last_op_time = 0.0

        # EdgeX single-flight control
        self.edgex_order_lock = asyncio.Lock()
        self.edgex_active_order_id = None
        self.edgex_active_side = None

        # Lighter WebSocket state
        self.lighter_ws_task = None
        self.lighter_order_result = None

        # Lighter order management
        self.lighter_order_status = None
        self.lighter_order_price = None
        self.lighter_order_side = None
        self.lighter_order_size = None
        self.lighter_order_start_time = None

        # Strategy state
        self.waiting_for_lighter_fill = False
        self.wait_start_time = None

        # Order execution tracking
        self.order_execution_complete = False

        # Current order details for immediate execution
        self.current_lighter_side = None
        self.current_lighter_quantity = None
        self.current_lighter_price = None
        self.lighter_order_info = None

        # Lighter API configuration
        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX'))

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown handler."""
        self.stop_flag = True
        self.logger.info("\n🛑 Stopping...")
        
        # Print performance report
        self.print_performance_report()

        # Cancel Lighter WebSocket task
        if self.lighter_ws_task and not self.lighter_ws_task.done():
            try:
                self.lighter_ws_task.cancel()
                self.logger.info("🔌 Lighter WebSocket task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling Lighter WebSocket task: {e}")

        # Close logging handlers properly
        for handler in self.logger.handlers[:]:
            try:
                handler.close()
                self.logger.removeHandler(handler)
            except Exception:
                pass

    def print_performance_report(self):
        """Print performance metrics report."""
        runtime = self.metrics.get_runtime_seconds()
        orders_per_min = self.metrics.get_orders_per_minute()
        
        self.logger.info("=" * 60)
        self.logger.info("📊 PERFORMANCE REPORT")
        self.logger.info("=" * 60)
        self.logger.info(f"Runtime: {runtime:.1f} seconds")
        self.logger.info(f"Orders Placed: {self.metrics.orders_placed}")
        self.logger.info(f"Orders Filled: {self.metrics.orders_filled}")
        self.logger.info(f"Orders Canceled: {self.metrics.orders_canceled}")
        self.logger.info(f"Fill Rate: {(self.metrics.orders_filled/max(1, self.metrics.orders_placed)*100):.1f}%")
        self.logger.info(f"Orders/Min: {orders_per_min:.1f}")
        self.logger.info(f"EdgeX API Calls: {self.metrics.edgex_api_calls}")
        self.logger.info(f"Lighter API Calls: {self.metrics.lighter_api_calls}")
        self.logger.info(f"Price Adjustments: {self.metrics.price_adjustments}")
        self.logger.info(f"Errors: {self.metrics.errors}")
        self.logger.info(f"WebSocket Reconnects: {self.metrics.websocket_reconnects}")
        self.logger.info("=" * 60)

    def _initialize_csv_file(self):
        """Initialize CSV file with headers if it doesn't exist."""
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['exchange', 'timestamp', 'side', 'price', 'quantity'])

    def log_trade_to_csv(self, exchange: str, side: str, price: str, quantity: str):
        """Log trade details to CSV file."""
        timestamp = datetime.now(pytz.UTC).isoformat()

        with open(self.csv_filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                exchange,
                timestamp,
                side,
                price,
                quantity
            ])

        self.logger.info(f"📊 Trade logged to CSV: {exchange} {side} {quantity} @ {price}")

    def handle_lighter_order_result(self, order_data):
        """Handle Lighter order result from WebSocket."""
        try:
            order_data["avg_filled_price"] = (Decimal(order_data["filled_quote_amount"]) /
                                              Decimal(order_data["filled_base_amount"]))
            if order_data["is_ask"]:
                order_data["side"] = "SHORT"
                self.lighter_position -= Decimal(order_data["filled_base_amount"]) 
            else:
                order_data["side"] = "LONG"
                self.lighter_position += Decimal(order_data["filled_base_amount"]) 

            self.logger.info(f"📊 Lighter order filled: {order_data['side']} "
                             f"{order_data['filled_base_amount']} @ {order_data['avg_filled_price']}")

            # Log Lighter trade to CSV
            self.log_trade_to_csv(
                exchange='Lighter',
                side=order_data['side'],
                price=str(order_data['avg_filled_price']),
                quantity=str(order_data['filled_base_amount'])
            )

            # Mark execution as complete
            self.lighter_order_filled = True  # Mark order as filled
            self.order_execution_complete = True

        except Exception as e:
            self.logger.error(f"Error handling Lighter order result: {e}")

    async def reset_lighter_order_book(self):
        """Reset Lighter order book state."""
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
            self.lighter_order_book_sequence_gap = False
            self.lighter_snapshot_loaded = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None

    def update_lighter_order_book(self, side: str, levels: list):
        """Update Lighter order book with new levels."""
        for level in levels:
            if isinstance(level, list) and len(level) >= 2:
                price = Decimal(level[0])
                size = Decimal(level[1])
            elif isinstance(level, dict):
                price = Decimal(level.get("price", 0))
                size = Decimal(level.get("size", 0))
            else:
                self.logger.warning(f"⚠️ Unexpected level format: {level}")
                continue

            if size > 0:
                self.lighter_order_book[side][price] = size
            else:
                self.lighter_order_book[side].pop(price, None)

    def validate_order_book_offset(self, new_offset: int) -> bool:
        """Validate order book offset sequence."""
        if new_offset <= self.lighter_order_book_offset:
            self.logger.warning(
                f"⚠️ Out-of-order update: new_offset={new_offset}, current_offset={self.lighter_order_book_offset}")
            return False
        return True

    def validate_order_book_integrity(self) -> bool:
        """Validate order book integrity."""
        for side in ["bids", "asks"]:
            for price, size in self.lighter_order_book[side].items():
                if price <= 0 or size <= 0:
                    self.logger.error(f"❌ Invalid order book data: {side} price={price}, size={size}")
                    return False
        return True

    def get_lighter_best_levels(self) -> Tuple[Tuple[Decimal, Decimal], Tuple[Decimal, Decimal]]:
        """Get best bid and ask levels from Lighter order book."""
        best_bid = None
        best_ask = None

        if self.lighter_order_book["bids"]:
            best_bid_price = max(self.lighter_order_book["bids"].keys())
            best_bid_size = self.lighter_order_book["bids"][best_bid_price]
            best_bid = (best_bid_price, best_bid_size)

        if self.lighter_order_book["asks"]:
            best_ask_price = min(self.lighter_order_book["asks"].keys())
            best_ask_size = self.lighter_order_book["asks"][best_ask_price]
            best_ask = (best_ask_price, best_ask_size)

        return best_bid, best_ask

    def get_lighter_mid_price(self) -> Decimal:
        """Get mid price from Lighter order book."""
        best_bid, best_ask = self.get_lighter_best_levels()

        if best_bid is None or best_ask is None:
            raise Exception("Cannot calculate mid price - missing order book data")

        mid_price = (best_bid[0] + best_ask[0]) / Decimal('2')
        return mid_price

    def get_lighter_order_price(self, is_ask: bool) -> Decimal:
        """Get order price from Lighter order book."""
        best_bid, best_ask = self.get_lighter_best_levels()

        if best_bid is None or best_ask is None:
            raise Exception("Cannot calculate order price - missing order book data")

        if is_ask:
            order_price = best_bid[0] + Decimal('0.1')
        else:
            order_price = best_ask[0] - Decimal('0.1')

        return order_price

    async def request_fresh_snapshot(self, ws):
        """Request fresh order book snapshot."""
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

    async def handle_lighter_ws(self):
        """Handle Lighter WebSocket connection and messages."""
        url = "wss://mainnet.zklighter.elliot.ai/stream"
        cleanup_counter = 0

        while not self.stop_flag:
            timeout_count = 0
            try:
                # Reset order book state before connecting
                await self.reset_lighter_order_book()

                async with websockets.connect(url) as ws:
                    # Subscribe to order book updates
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

                    # Subscribe to account orders updates
                    account_orders_channel = f"account_orders/{self.lighter_market_index}/{self.account_index}"

                    # Get auth token for the subscription
                    try:
                        ten_minutes_deadline = int(time.time() + 10 * 60)
                        auth_token, err = self.lighter_client.create_auth_token_with_expiry(ten_minutes_deadline)
                        if err is not None:
                            self.logger.warning(f"⚠️ Failed to create auth token for account orders subscription: {err}")
                        else:
                            auth_message = {
                                "type": "subscribe",
                                "channel": account_orders_channel,
                                "auth": auth_token
                            }
                            await ws.send(json.dumps(auth_message))
                            self.logger.info("✅ Subscribed to account orders with auth token (expires in 10 minutes)")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error creating auth token for account orders subscription: {e}")

                    while not self.stop_flag:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError as e:
                                self.logger.warning(f"⚠️ JSON parsing error in Lighter websocket: {e}")
                                continue

                            # Reset timeout counter on successful message
                            timeout_count = 0

                            async with self.lighter_order_book_lock:
                                if data.get("type") == "subscribed/order_book":
                                    # Initial snapshot - clear and populate the order book
                                    self.lighter_order_book["bids"].clear()
                                    self.lighter_order_book["asks"].clear()

                                    order_book = data.get("order_book", {})
                                    if order_book and "offset" in order_book:
                                        self.lighter_order_book_offset = order_book["offset"]
                                        self.logger.info(f"✅ Initial order book offset set to: {self.lighter_order_book_offset}")

                                    bids = order_book.get("bids", [])
                                    asks = order_book.get("asks", [])
                                    if bids:
                                        self.logger.debug(f"📊 Sample bid structure: {bids[0] if bids else 'None'}")
                                    if asks:
                                        self.logger.debug(f"📊 Sample ask structure: {asks[0] if asks else 'None'}")

                                    self.update_lighter_order_book("bids", bids)
                                    self.update_lighter_order_book("asks", asks)
                                    self.lighter_snapshot_loaded = True
                                    self.lighter_order_book_ready = True

                                    self.logger.info(f"✅ Lighter order book snapshot loaded with "
                                                     f"{len(self.lighter_order_book['bids'])} bids and "
                                                     f"{len(self.lighter_order_book['asks'])} asks")

                                elif data.get("type") == "update/order_book" and self.lighter_snapshot_loaded:
                                    order_book = data.get("order_book", {})
                                    if not order_book or "offset" not in order_book:
                                        self.logger.warning("⚠️ Order book update missing offset, skipping")
                                        continue

                                    new_offset = order_book["offset"]

                                    if not self.validate_order_book_offset(new_offset):
                                        self.lighter_order_book_sequence_gap = True
                                        break

                                    self.update_lighter_order_book("bids", order_book.get("bids", []))
                                    self.update_lighter_order_book("asks", order_book.get("asks", []))

                                    if not self.validate_order_book_integrity():
                                        self.logger.warning("🔄 Order book integrity check failed, requesting fresh snapshot...")
                                        break

                                    best_bid, best_ask = self.get_lighter_best_levels()
                                    if best_bid is not None:
                                        self.lighter_best_bid = best_bid[0]
                                    if best_ask is not None:
                                        self.lighter_best_ask = best_ask[0]

                                elif data.get("type") == "ping":
                                    await ws.send(json.dumps({"type": "pong"}))
                                elif data.get("type") == "update/account_orders":
                                    orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                                    if len(orders) == 1:
                                        order_data = orders[0]
                                        if order_data.get("status") == "filled":
                                            self.handle_lighter_order_result(order_data)
                                elif data.get("type") == "update/order_book" and not self.lighter_snapshot_loaded:
                                    continue

                            # Periodic cleanup outside the lock
                            cleanup_counter += 1
                            if cleanup_counter >= 1000:
                                cleanup_counter = 0

                            if self.lighter_order_book_sequence_gap:
                                try:
                                    await self.request_fresh_snapshot(ws)
                                    self.lighter_order_book_sequence_gap = False
                                except Exception as e:
                                    self.logger.error(f"⚠️ Failed to request fresh snapshot: {e}")
                                    break

                        except asyncio.TimeoutError:
                            timeout_count += 1
                            if timeout_count % 3 == 0:
                                self.logger.warning(f"⏰ No message from Lighter websocket for {timeout_count} seconds")
                            continue
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.warning(f"⚠️ Lighter websocket connection closed: {e}")
                            break
                        except websockets.exceptions.WebSocketException as e:
                            self.logger.warning(f"⚠️ Lighter websocket error: {e}")
                            break
                        except Exception as e:
                            self.logger.error(f"⚠️ Error in Lighter websocket: {e}")
                            self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                            break
            except Exception as e:
                self.logger.error(f"⚠️ Failed to connect to Lighter websocket: {e}")

            await asyncio.sleep(2)

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def initialize_lighter_client(self):
        """Initialize the Lighter client."""
        if self.lighter_client is None:
            api_key_private_key = os.getenv('API_KEY_PRIVATE_KEY')
            if not api_key_private_key:
                raise Exception("API_KEY_PRIVATE_KEY environment variable not set")

            self.lighter_client = SignerClient(
                url=self.lighter_base_url,
                private_key=api_key_private_key,
                account_index=self.account_index,
                api_key_index=self.api_key_index,
            )

            err = self.lighter_client.check_client()
            if err is not None:
                raise Exception(f"CheckClient error: {err}")

            self.logger.info("✅ Lighter client initialized successfully")
        return self.lighter_client

    def initialize_edgex_client(self):
        """Initialize the EdgeX client."""
        # Create config for EdgeX client
        config_dict = {
            'ticker': self.ticker,
            'contract_id': '',
            'quantity': self.order_quantity,
            'tick_size': Decimal('0.01'),
            'close_order_side': 'sell'
        }

        config = Config(config_dict)
        self.edgex_client = EdgeXClient(config)
        self.logger.info("✅ EdgeX client initialized successfully")
        return self.edgex_client

    def get_lighter_market_config(self) -> Tuple[int, int, int]:
        """Get Lighter market configuration."""
        url = f"{self.lighter_base_url}/api/v1/orderBooks"
        headers = {"accept": "application/json"}

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            if not response.text.strip():
                raise Exception("Empty response from Lighter API")

            data = response.json()

            if "order_books" not in data:
                raise Exception("Unexpected response format")

            for market in data["order_books"]:
                if market["symbol"] == self.ticker:
                    return (market["market_id"],
                            pow(10, market["supported_size_decimals"]),
                            pow(10, market["supported_price_decimals"]))

            raise Exception(f"Ticker {self.ticker} not found")

        except Exception as e:
            self.logger.error(f"⚠️ Error getting market config: {e}")
            raise

    async def get_edgex_contract_info(self) -> Tuple[str, Decimal]:
        """Get EdgeX contract ID and tick size."""
        if not self.edgex_client:
            raise Exception("EdgeX client not initialized")

        contract_id, tick_size = await self.edgex_client.get_contract_attributes()

        if self.order_quantity < self.edgex_client.config.quantity:
            raise ValueError(
                f"Order quantity is less than min quantity: {self.order_quantity} < {self.edgex_client.config.quantity}")

        return contract_id, tick_size

    async def fetch_edgex_bbo_prices(self) -> Tuple[Decimal, Decimal]:
        """Fetch best bid/ask prices from EdgeX via REST (SDK)."""
        if not self.edgex_client:
            raise Exception("EdgeX client not initialized")

        best_bid, best_ask = await self.edgex_client.fetch_bbo_prices(self.edgex_contract_id)
        return best_bid, best_ask

    def round_to_tick(self, price: Decimal) -> Decimal:
        """Round price to tick size."""
        if self.edgex_tick_size is None:
            return price
        return (price / self.edgex_tick_size).quantize(Decimal('1')) * self.edgex_tick_size

    async def place_bbo_order(self, side: str, quantity: Decimal):
        """Place an EdgeX post-only order with resilient fallback pricing.

        Strategy:
        1) Try SDK's place_open_order (which already retries internally).
        2) If rejected, compute safer maker prices with increasing tick offsets and
           place via place_close_order (also post-only) using explicit price.
        """
        best_bid, best_ask = await self.fetch_edgex_bbo_prices()

        # First attempt: EdgeX SDK helper (has its own retry)
        # rate-limit: ensure spacing for EdgeX ops
        await self._edgex_rate_limit_wait()
        order_result = await self.edgex_client.place_open_order(
            contract_id=self.edgex_contract_id,
            quantity=quantity,
            direction=side.lower()
        )
        if order_result.success:
            self.edgex_active_order_id = order_result.order_id
            self.metrics.orders_placed += 1
            return order_result.order_id, order_result.price

        # Fallback: place a single conservative maker price once (avoid rapid spam)
        tick = self.edgex_tick_size or Decimal('0')
        offset_ticks = 5

        def compute_price(offset_ticks: int) -> Decimal:
            if side == 'buy':
                return self.round_to_tick(best_ask - tick * Decimal(offset_ticks))
            else:
                return self.round_to_tick(best_bid + tick * Decimal(offset_ticks))

        price = compute_price(offset_ticks)
        if side == 'buy' and best_ask and price >= best_ask:
            price = best_ask - tick
        if side == 'sell' and best_bid and price <= best_bid:
            price = best_bid + tick

        await self._edgex_rate_limit_wait()
        fallback_result = await self.edgex_client.place_close_order(
            contract_id=self.edgex_contract_id,
            quantity=quantity,
            price=price,
            side=side
        )
        if fallback_result.success:
            self.logger.info(f"[Fallback] EdgeX order accepted at {price} (single conservative attempt)")
            self.edgex_active_order_id = fallback_result.order_id
            self.edgex_active_side = side
            self.edgex_order_status = OrderStatus.OPEN.value
            self.metrics.orders_placed += 1
            return fallback_result.order_id, price

        last_error = fallback_result.error_message if hasattr(fallback_result, 'error_message') else 'Unknown error'
        
        # Classify and handle different error types
        if isinstance(last_error, str):
            if 'rateLimit' in last_error.lower():
                self.logger.info("Hit EdgeX rate limit; waiting 6s and retrying fallback once")
                await asyncio.sleep(6.0)
                await self._edgex_rate_limit_wait()
                retry_result = await self.edgex_client.place_close_order(
                    contract_id=self.edgex_contract_id,
                    quantity=quantity,
                    price=price,
                    side=side
                )
                if retry_result.success:
                    self.edgex_active_order_id = retry_result.order_id
                    self.edgex_active_side = side
                    self.edgex_order_status = OrderStatus.OPEN.value
                    self.metrics.orders_placed += 1
                    return retry_result.order_id, price
                last_error = retry_result.error_message if hasattr(retry_result, 'error_message') else last_error
            elif 'insufficient' in last_error.lower() or 'balance' in last_error.lower():
                raise OrderError(f"Insufficient balance for EdgeX order: {last_error}")
            elif 'invalid' in last_error.lower() or 'bad' in last_error.lower():
                raise OrderError(f"Invalid EdgeX order parameters: {last_error}")
            elif 'network' in last_error.lower() or 'timeout' in last_error.lower():
                raise NetworkError(f"Network error placing EdgeX order: {last_error}")
        
        raise OrderError(f"Failed to place EdgeX order after fallback: {last_error}")

    async def _wait_edgex_order_inactive(self, order_id: str, timeout: float = 10.0):
        """Wait until the given order_id is no longer active (best-effort)."""
        try:
            start = time.time()
            while time.time() - start < timeout:
                await asyncio.sleep(0.2)
                try:
                    active = await self.edgex_client.get_active_orders(self.edgex_contract_id)
                    if not any(o.order_id == order_id for o in active):
                        return
                except Exception:
                    return
        except Exception:
            return

    async def _edgex_rate_limit_wait(self):
        """Ensure EdgeX API call spacing to respect rate limits."""
        async with self.edgex_rate_limit_lock:
            now = time.time()
            min_interval = self.config.rate_limit_interval
            delta = now - self.edgex_last_op_time
            if delta < min_interval:
                await asyncio.sleep(min_interval - delta)
            self.edgex_last_op_time = time.time()
            self.metrics.edgex_api_calls += 1

    async def place_edgex_post_only_order(self, side: str, quantity: Decimal):
        """Place a post-only order on EdgeX."""
        if not self.edgex_client:
            raise Exception("EdgeX client not initialized")

        if self.edgex_active_order_id is not None and (self.edgex_active_side == side):
            await self._wait_edgex_order_inactive(self.edgex_active_order_id)

        self.edgex_order_status = None
        self.logger.info(f"[OPEN] [EdgeX] [{side}] Placing EdgeX POST-ONLY order")
        async with self.edgex_order_lock:
            order_id, order_price = await self.place_bbo_order(side, quantity)
            # Update order_id if fallback was used
            if self.edgex_active_order_id and self.edgex_active_order_id != order_id:
                order_id = self.edgex_active_order_id

        start_time = time.time()
        last_cancel_time = 0
        last_waiting_log_time = 0
        price_adjustment_count = 0
        effective_ttl_seconds = self.config.edgex_ttl_seconds

        while not self.stop_flag:
            if self.edgex_order_status in ['CANCELED', 'CANCELLED']:
                self.logger.info(f"Order {order_id} was canceled, will re-place after 5s")
                self.edgex_order_status = None
                await asyncio.sleep(5)
                # ensure previous order fully inactive
                await self._wait_edgex_order_inactive(order_id)
                # if position已满足目标方向，则不再重下
                try:
                    pos_amt = await self.edgex_client.get_account_positions()
                except Exception:
                    pos_amt = abs(self.edgex_position)
                if (side == 'sell' and self.edgex_position <= 0) or (side == 'buy' and self.edgex_position >= 0):
                    self.logger.info("Skip re-place: position already satisfied after cancel")
                    break
                async with self.edgex_order_lock:
                    order_id, order_price = await self.place_bbo_order(side, quantity)
                start_time = time.time()
                last_cancel_time = 0
                await asyncio.sleep(0.5)
            elif self.edgex_order_status in ['NEW', 'OPEN', 'PENDING', 'CANCELING', 'PARTIALLY_FILLED']:
                await asyncio.sleep(0.5)

                # Optional: refine cancellation using latest BBO
                try:
                    best_bid, best_ask = await self.fetch_edgex_bbo_prices()
                except Exception:
                    best_bid = None
                    best_ask = None

                current_time = time.time()
                should_cancel = False
                if best_bid is not None and best_ask is not None:
                    # Cancel only when our limit would cross the book and变为吃单
                    if side == 'buy' and order_price >= best_ask:
                        should_cancel = True
                    elif side == 'sell' and order_price <= best_bid:
                        should_cancel = True
                    # Also cancel if price is too close to BBO and waiting too long (15s)
                    elif current_time - start_time > 15:
                        if side == 'buy' and order_price >= best_ask - (self.edgex_tick_size or Decimal('0.1')):
                            should_cancel = True
                        elif side == 'sell' and order_price <= best_bid + (self.edgex_tick_size or Decimal('0.1')):
                            should_cancel = True

                # Hard validity window: cancel and re-place after TTL regardless of price
                if current_time - start_time >= effective_ttl_seconds and current_time - last_cancel_time > 1:
                    try:
                        self.logger.info(f"Canceling order {order_id} due to TTL {effective_ttl_seconds}s expiry")
                        cancel_result = await self.edgex_client.cancel_order(order_id)
                        if cancel_result.success:
                            last_cancel_time = current_time
                            await self._wait_edgex_order_inactive(order_id)
                            await asyncio.sleep(5)
                            # 二次检查是否还需要同向重下
                            try:
                                pos_amt = await self.edgex_client.get_account_positions()
                            except Exception:
                                pos_amt = abs(self.edgex_position)
                            if (side == 'sell' and self.edgex_position <= 0) or (side == 'buy' and self.edgex_position >= 0):
                                self.logger.info("Skip re-place after TTL: position already satisfied")
                                break
                            async with self.edgex_order_lock:
                                order_id, order_price = await self.place_bbo_order(side, quantity)
                            start_time = time.time()
                            continue
                        else:
                            self.logger.error(f"❌ Error canceling EdgeX order (TTL): {cancel_result.error_message}")
                    except Exception as e:
                        self.logger.error(f"❌ Error canceling EdgeX order (TTL): {e}")

                # Smart price adjustment: only if waiting very long and price is far from BBO
                # Limit to maximum price adjustments per order to avoid infinite loops
                if (current_time - start_time > 25 and not should_cancel and 
                    best_bid is not None and best_ask is not None and 
                    price_adjustment_count < self.config.max_price_adjustments):
                    # Only adjust if price is significantly far from BBO
                    tick = self.edgex_tick_size or Decimal('0.1')
                    threshold_ticks = self.config.price_adjustment_threshold
                    if side == 'buy' and order_price < best_ask - tick * threshold_ticks:
                        self.logger.info(f"Adjusting buy price from {order_price} to {best_ask - tick * 2} for faster fill (adjustment {price_adjustment_count + 1}/{self.config.max_price_adjustments})")
                        try:
                            # Cancel current order and place new one with better price
                            cancel_result = await self.edgex_client.cancel_order(order_id)
                            if cancel_result.success:
                                await self._wait_edgex_order_inactive(order_id)
                                await asyncio.sleep(2)  # Longer wait to avoid rapid adjustments
                                async with self.edgex_order_lock:
                                    order_id, order_price = await self.place_bbo_order(side, quantity)
                                start_time = time.time()
                                price_adjustment_count += 1
                                continue
                        except Exception as e:
                            self.logger.warning(f"Failed to adjust price: {e}")
                    elif side == 'sell' and order_price > best_bid + tick * threshold_ticks:
                        self.logger.info(f"Adjusting sell price from {order_price} to {best_bid + tick * 2} for faster fill (adjustment {price_adjustment_count + 1}/{self.config.max_price_adjustments})")
                        try:
                            # Cancel current order and place new one with better price
                            cancel_result = await self.edgex_client.cancel_order(order_id)
                            if cancel_result.success:
                                await self._wait_edgex_order_inactive(order_id)
                                await asyncio.sleep(2)  # Longer wait to avoid rapid adjustments
                                async with self.edgex_order_lock:
                                    order_id, order_price = await self.place_bbo_order(side, quantity)
                                start_time = time.time()
                                price_adjustment_count += 1
                                continue
                        except Exception as e:
                            self.logger.warning(f"Failed to adjust price: {e}")

                if current_time - start_time > 10:
                    if should_cancel and current_time - last_cancel_time > 5:
                        try:
                            self.logger.info(f"Canceling order {order_id} due to timeout/price mismatch")
                            cancel_result = await self.edgex_client.cancel_order(order_id)
                            if cancel_result.success:
                                last_cancel_time = current_time
                                await self._wait_edgex_order_inactive(order_id)
                                # wait 5s before re-placing a fresh post-only order at latest price
                                await asyncio.sleep(5)
                                # 二次检查是否还需要同向重下
                                try:
                                    pos_amt = await self.edgex_client.get_account_positions()
                                except Exception:
                                    pos_amt = abs(self.edgex_position)
                                if (side == 'sell' and self.edgex_position <= 0) or (side == 'buy' and self.edgex_position >= 0):
                                    self.logger.info("Skip re-place after price-mismatch: position already satisfied")
                                    break
                                async with self.edgex_order_lock:
                                    order_id, order_price = await self.place_bbo_order(side, quantity)
                                # reset timer window after new order
                                start_time = time.time()
                            else:
                                self.logger.error(f"❌ Error canceling EdgeX order: {cancel_result.error_message}")
                        except Exception as e:
                            self.logger.error(f"❌ Error canceling EdgeX order: {e}")
                    elif not should_cancel:
                        # Throttle waiting log based on config
                        if current_time - last_waiting_log_time >= self.config.waiting_log_interval:
                            self.logger.info(f"Waiting for EdgeX order to be filled (order price is near BBO)")
                            last_waiting_log_time = current_time
            elif self.edgex_order_status == 'FILLED':
                self.logger.info(f"Order {order_id} filled successfully")
                break
            else:
                if self.edgex_order_status is not None:
                    self.logger.error(f"❌ Unknown EdgeX order status: {self.edgex_order_status}")
                    break
                else:
                    await asyncio.sleep(0.5)

    def handle_edgex_order_update(self, order_data):
        """Handle EdgeX order updates from WebSocket (via SDK -> our client hook)."""
        side = order_data.get('side', '').lower()
        filled_size = Decimal(order_data.get('filled_size', '0'))
        price = Decimal(order_data.get('price', '0'))

        if side == 'buy':
            self.edgex_position += filled_size
            lighter_side = 'sell'
        else:
            self.edgex_position -= filled_size
            lighter_side = 'buy'

        self.current_lighter_side = lighter_side
        self.current_lighter_quantity = filled_size
        self.current_lighter_price = price

        self.lighter_order_info = {
            'lighter_side': lighter_side,
            'quantity': filled_size,
            'price': price
        }

        self.waiting_for_lighter_fill = True
        self.logger.info(f"📋 Ready to place Lighter order: {lighter_side} {filled_size} @ {price}")

    async def place_lighter_market_order(self, lighter_side: str, quantity: Decimal, price: Decimal):
        if not self.lighter_client:
            await self.initialize_lighter_client()

        best_bid, best_ask = self.get_lighter_best_levels()

        if lighter_side.lower() == 'buy':
            is_ask = False
            price = best_ask[0] * Decimal('1.002')
        else:
            is_ask = True
            price = best_bid[0] * Decimal('0.998')

        self.logger.info(f"Placing Lighter market order: {lighter_side} {quantity} | is_ask: {is_ask}")

        self.lighter_order_filled = False
        self.lighter_order_price = price
        self.lighter_order_side = lighter_side
        self.lighter_order_size = quantity

        try:
            client_order_index = int(time.time() * 1000)
            tx_info, error = self.lighter_client.sign_create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(quantity * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
            )
            if error is not None:
                raise Exception(f"Sign error: {error}")

            tx_hash = await self.lighter_client.send_tx(
                tx_type=self.lighter_client.TX_TYPE_CREATE_ORDER,
                tx_info=tx_info
            )
            self.logger.info(f"🚀 Lighter limit order sent: {lighter_side} {quantity}")
            await self.monitor_lighter_order(client_order_index)

            return tx_hash
        except Exception as e:
            self.logger.error(f"❌ Error placing Lighter order: {e}")
            return None

    async def monitor_lighter_order(self, client_order_index: int):
        """Monitor Lighter order and adjust price if needed."""
        self.logger.info(f"🔍 Starting to monitor Lighter order - Order ID: {client_order_index}")

        start_time = time.time()
        while not self.lighter_order_filled and not self.stop_flag:
            if time.time() - start_time > 30:
                self.logger.error(f"❌ Timeout waiting for Lighter order fill after {time.time() - start_time:.1f}s")
                self.logger.warning("⚠️ Using fallback - marking order as filled to continue trading")
                self.lighter_order_filled = True
                self.waiting_for_lighter_fill = False
                self.order_execution_complete = True
                break

            await asyncio.sleep(0.1)

    async def setup_edgex_websocket(self):
        """Setup EdgeX websocket for order updates via SDK private WS."""
        if not self.edgex_client:
            raise Exception("EdgeX client not initialized")

        def order_update_handler(order_data):
            if order_data.get('contract_id') != self.edgex_contract_id:
                return
            try:
                order_id = order_data.get('order_id')
                status = order_data.get('status')
                side = order_data.get('side', '').lower()
                filled_size = Decimal(order_data.get('filled_size', '0'))
                size = Decimal(order_data.get('size', '0'))
                price = order_data.get('price', '0')

                if side == 'buy':
                    order_type = "OPEN"
                else:
                    order_type = "CLOSE"

                if status == 'FILLED':
                    if side == 'buy':
                        self.edgex_position += filled_size
                    else:
                        self.edgex_position -= filled_size
                    self.logger.info(f"[{order_id}] [{order_type}] [EdgeX] [{status}]: {filled_size} @ {price}")
                    self.edgex_order_status = status
                    if self.edgex_active_order_id == order_id:
                        self.edgex_active_order_id = None

                    self.log_trade_to_csv(
                        exchange='EdgeX',
                        side=side,
                        price=str(price),
                        quantity=str(filled_size)
                    )

                    self.handle_edgex_order_update({
                        'order_id': order_id,
                        'side': side,
                        'status': status,
                        'size': size,
                        'price': price,
                        'contract_id': self.edgex_contract_id,
                        'filled_size': filled_size
                    })
                else:
                    if status == 'OPEN':
                        self.logger.info(f"[{order_id}] [{order_type}] [EdgeX] [{status}]: {size} @ {price}")
                    else:
                        self.logger.info(f"[{order_id}] [{order_type}] [EdgeX] [{status}]: {filled_size} @ {price}")

                    if status == 'PARTIALLY_FILLED':
                        self.edgex_order_status = "OPEN"
                    elif status in ['CANCELED', 'CANCELLED']:
                        self.edgex_order_status = status
                        if self.edgex_active_order_id == order_id:
                            self.edgex_active_order_id = None
                            self.edgex_active_side = None
                    elif status in ['NEW', 'OPEN', 'PENDING', 'CANCELING']:
                        self.edgex_order_status = status
                    else:
                        self.logger.warning(f"Unknown order status: {status}")
                        self.edgex_order_status = status

            except Exception as e:
                self.logger.error(f"Error handling EdgeX order update: {e}")

        try:
            self.edgex_client.setup_order_update_handler(order_update_handler)
            self.logger.info("✅ EdgeX WebSocket order update handler set up")

            await self.edgex_client.connect()
            self.logger.info("✅ EdgeX WebSocket connection established")

        except Exception as e:
            self.logger.error(f"Could not setup EdgeX WebSocket handlers: {e}")

    async def trading_loop(self):
        """Main trading loop implementing the strategy."""
        self.logger.info(f"🚀 Starting hedge bot for {self.ticker}")

        try:
            self.initialize_lighter_client()
            self.initialize_edgex_client()

            self.edgex_contract_id, self.edgex_tick_size = await self.get_edgex_contract_info()
            self.lighter_market_index, self.base_amount_multiplier, self.price_multiplier = self.get_lighter_market_config()

            self.logger.info(f"Contract info loaded - EdgeX: {self.edgex_contract_id}, "
                             f"Lighter: {self.lighter_market_index}")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize: {e}")
            return

        try:
            await self.setup_edgex_websocket()
            self.logger.info("✅ EdgeX WebSocket connection established")
        except Exception as e:
            self.logger.error(f"❌ Failed to setup EdgeX websocket: {e}")
            return

        try:
            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            self.logger.info("✅ Lighter WebSocket task started")

            self.logger.info("⏳ Waiting for initial Lighter order book data...")
            timeout = 10
            start_time = time.time()
            while not self.lighter_order_book_ready and not self.stop_flag:
                if time.time() - start_time > timeout:
                    self.logger.warning(f"⚠️ Timeout waiting for Lighter WebSocket order book data after {timeout}s")
                    break
                await asyncio.sleep(0.5)

            if self.lighter_order_book_ready:
                self.logger.info("✅ Lighter WebSocket order book data received")
            else:
                self.logger.warning("⚠️ Lighter WebSocket order book not ready")

        except Exception as e:
            self.logger.error(f"❌ Failed to setup Lighter websocket: {e}")
            return

        await asyncio.sleep(5)

        iterations = 0
        while iterations < self.iterations and not self.stop_flag:
            iterations += 1
            self.logger.info("-----------------------------------------------")
            self.logger.info(f"🔄 Trading loop iteration {iterations}")
            self.logger.info("-----------------------------------------------")

            self.logger.info(f"[STEP 1] EdgeX position: {self.edgex_position} | Lighter position: {self.lighter_position}")

            if abs(self.edgex_position + self.lighter_position) > 0.2:
                self.logger.error(f"❌ Position diff is too large: {self.edgex_position + self.lighter_position}")
                break

            self.order_execution_complete = False
            self.waiting_for_lighter_fill = False
            try:
                side = 'buy'
                await self.place_edgex_post_only_order(side, self.order_quantity)
            except Exception as e:
                self.logger.error(f"⚠️ Error in trading loop: {e}")
                self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                break

            start_time = time.time()
            while not self.order_execution_complete and not self.stop_flag:
                if self.waiting_for_lighter_fill:
                    await self.place_lighter_market_order(
                        self.current_lighter_side,
                        self.current_lighter_quantity,
                        self.current_lighter_price
                    )
                    break

                await asyncio.sleep(0.01)
                if time.time() - start_time > 180:
                    self.logger.error("❌ Timeout waiting for trade completion")
                    break

            if self.stop_flag:
                break

            self.logger.info(f"[STEP 2] EdgeX position: {self.edgex_position} | Lighter position: {self.lighter_position}")
            self.order_execution_complete = False
            self.waiting_for_lighter_fill = False
            try:
                side = 'sell'
                await self.place_edgex_post_only_order(side, self.order_quantity)
            except Exception as e:
                self.logger.error(f"⚠️ Error in trading loop: {e}")
                self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                break

            while not self.order_execution_complete and not self.stop_flag:
                if self.waiting_for_lighter_fill:
                    await self.place_lighter_market_order(
                        self.current_lighter_side,
                        self.current_lighter_quantity,
                        self.current_lighter_price
                    )
                    break

                await asyncio.sleep(0.01)
                if time.time() - start_time > 180:
                    self.logger.error("❌ Timeout waiting for trade completion")
                    break

            self.logger.info(f"[STEP 3] EdgeX position: {self.edgex_position} | Lighter position: {self.lighter_position}")
            self.order_execution_complete = False
            self.waiting_for_lighter_fill = False
            if self.edgex_position == 0:
                continue
            elif self.edgex_position > 0:
                side = 'sell'
            else:
                side = 'buy'

            try:
                await self.place_edgex_post_only_order(side, abs(self.edgex_position))
            except Exception as e:
                self.logger.error(f"⚠️ Error in trading loop: {e}")
                self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                break

            while not self.order_execution_complete and not self.stop_flag:
                if self.waiting_for_lighter_fill:
                    await self.place_lighter_market_order(
                        self.current_lighter_side,
                        self.current_lighter_quantity,
                        self.current_lighter_price
                    )
                    break

                await asyncio.sleep(0.01)
                if time.time() - start_time > 180:
                    self.logger.error("❌ Timeout waiting for trade completion")
                    break

    async def run(self):
        """Run the hedge bot."""
        self.setup_signal_handlers()

        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Received interrupt signal...")
        finally:
            self.logger.info("🔄 Cleaning up...")
            self.shutdown()


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Trading bot for EdgeX and Lighter')
    parser.add_argument('--exchange', type=str,
                        help='Exchange')
    parser.add_argument('--ticker', type=str, default='BTC',
                        help='Ticker symbol (default: BTC)')
    parser.add_argument('--size', type=str,
                        help='Number of tokens to buy/sell per order')
    parser.add_argument('--iter', type=int,
                        help='Number of iterations to run')
    parser.add_argument('--fill-timeout', type=int, default=5,
                        help='Timeout in seconds for maker order fills (default: 5)')
    parser.add_argument('--ttl', type=int, default=30,
                        help='EdgeX order TTL in seconds (default: 30)')
    parser.add_argument('--rate-limit', type=float, default=6.0,
                        help='EdgeX API rate limit interval in seconds (default: 6.0)')
    parser.add_argument('--max-adjustments', type=int, default=2,
                        help='Maximum price adjustments per order (default: 2)')
    parser.add_argument('--adjustment-threshold', type=int, default=5,
                        help='Price adjustment threshold in ticks (default: 5)')

    return parser.parse_args()


def main():
    """Main function."""
    args = parse_arguments()
    
    # Create trading configuration
    config = TradingConfig(
        ticker=args.ticker,
        order_quantity=Decimal(args.size) if args.size else Decimal('0.001'),
        fill_timeout=args.fill_timeout,
        iterations=args.iter if args.iter else 20,
        edgex_ttl_seconds=args.ttl,
        rate_limit_interval=args.rate_limit,
        max_price_adjustments=args.max_adjustments,
        price_adjustment_threshold=args.adjustment_threshold
    )
    
    # Create and run bot
    bot = HedgeBot(config)
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
    except Exception as e:
        print(f"❌ Bot error: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
