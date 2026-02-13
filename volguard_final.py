
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client
        self.streamer: Optional[upstox_client.MarketDataStreamerV3] = None
        self._callbacks: Dict[str, List[Callable]] = {
            "message": [],
            "open": [],
            "close": [],
            "error": [],
            "reconnecting": [],
            "autoReconnectStopped": []
        }
        self._lock = threading.RLock()
        self._subscribed_instruments: Dict[str, str] = {}
        self.is_connected = False
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self._latest_prices: Dict[str, float] = {}
        self._latest_updates: Dict[str, MarketUpdate] = {}
        
    def on(self, event: str, callback: Callable):
        with self._lock:
            if event in self._callbacks:
                self._callbacks[event].append(callback)
    
    def connect(self, instrument_keys: Optional[List[str]] = None, mode: str = "ltpc"):
        try:
            sdk_mode = self.MODE_MAP.get(mode, "ltpc")
            
            if instrument_keys:
                self.streamer = upstox_client.MarketDataStreamerV3(
                    self.api_client,
                    instrument_keys,
                    sdk_mode
                )
            else:
                self.streamer = upstox_client.MarketDataStreamerV3(
                    self.api_client
                )
            
            self.streamer.on("open", self._on_sdk_open)
            self.streamer.on("close", self._on_sdk_close)
            self.streamer.on("message", self._on_sdk_message)
            self.streamer.on("error", self._on_sdk_error)
            self.streamer.on("reconnecting", self._on_sdk_reconnecting)
            self.streamer.on("autoReconnectStopped", self._on_sdk_auto_reconnect_stopped)
            
            self.streamer.auto_reconnect(True, 5, 10)
            self.streamer.connect()
            self.logger.info(f"MarketDataStreamerV3 connecting with mode={sdk_mode}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect MarketDataStreamerV3: {e}")
            self._dispatch("error", {"type": "connection_error", "message": str(e)})
    
    def subscribe(self, instrument_keys: List[str], mode: str):
        with self._lock:
            if not self.streamer:
                self.logger.error("Streamer not connected")
                return False
            if not self.is_connected:
                self.logger.error("Cannot subscribe - streamer not connected")
                return False
            
            try:
                sdk_mode = self.MODE_MAP.get(mode, "ltpc")
                
                for key in instrument_keys:
                    if '|' not in key:
                        self.logger.error(f"Invalid instrument key format: {key}")
                        return False
                    self._subscribed_instruments[key] = sdk_mode
                
                self.streamer.subscribe(instrument_keys, sdk_mode)
                self.logger.info(f"Subscribed to {len(instrument_keys)} instruments in {sdk_mode} mode")
                return True
                
            except Exception as e:
                self.logger.error(f"Subscribe failed: {e}")
                return False
    
    def unsubscribe(self, instrument_keys: List[str]):
        with self._lock:
            if not self.streamer:
                return False
            
            try:
                self.streamer.unsubscribe(instrument_keys)
                
                for key in instrument_keys:
                    self._subscribed_instruments.pop(key, None)
                    self._latest_prices.pop(key, None)
                    self._latest_updates.pop(key, None)
                    
                self.logger.info(f"Unsubscribed from {len(instrument_keys)} instruments")
                return True
                
            except Exception as e:
                self.logger.error(f"Unsubscribe failed: {e}")
                return False
    
    def change_mode(self, instrument_keys: List[str], mode: str):
        with self._lock:
            if not self.streamer:
                return False
            
            try:
                sdk_mode = self.MODE_MAP.get(mode, "ltpc")
                self.streamer.change_mode(instrument_keys, sdk_mode)
                
                for key in instrument_keys:
                    self._subscribed_instruments[key] = sdk_mode
                    
                self.logger.info(f"Changed mode to {sdk_mode} for {len(instrument_keys)} instruments")
                return True
                
            except Exception as e:
                self.logger.error(f"Change mode failed: {e}")
                return False
    
    def disconnect(self):
        with self._lock:
            if self.streamer:
                try:
                    self.streamer.disconnect()
                    self.is_connected = False
                    self.logger.info("MarketDataStreamerV3 disconnected")
                except Exception as e:
                    self.logger.error(f"Disconnect error: {e}")
    
    def get_ltp(self, instrument_key: str) -> Optional[float]:
        with self._lock:
            return self._latest_prices.get(instrument_key)
    
    def get_bulk_ltp(self, instrument_keys: List[str]) -> Dict[str, float]:
        with self._lock:
            return {k: self._latest_prices.get(k) for k in instrument_keys if k in self._latest_prices}
    
    def get_subscribed_instruments(self) -> Dict[str, str]:
        with self._lock:
            return self._subscribed_instruments.copy()
    
    def _on_sdk_open(self):
        self.is_connected = True
        self._latest_prices = {}
        self._latest_updates = {}
        self.logger.info("MarketDataStreamerV3 connected")
        self._dispatch("open", {"status": "connected"})
    
    def _on_sdk_close(self):
        self.is_connected = False
        self.logger.info("MarketDataStreamerV3 disconnected")
        self._dispatch("close", {"status": "disconnected"})
    
    def _on_sdk_message(self, message):
        try:
            if hasattr(message, 'feeds'):
                for instrument_key, feed_data in message.feeds.items():
                    update = MarketUpdate.from_feed(instrument_key, feed_data)
                    
                    with self._lock:
                        self._latest_prices[instrument_key] = update.ltp
                        self._latest_updates[instrument_key] = update
                    
                    self._dispatch("message", {
                        "type": "market_update",
                        "instrument_key": instrument_key,
                        "data": update
                    })
                
        except Exception as e:
            self.logger.error(f"Error processing market message: {e}")
    
    def _on_sdk_error(self, error):
        self.logger.error(f"MarketDataStreamerV3 error: {error}")
        self._dispatch("error", {"type": "sdk_error", "message": str(error)})
    
    def _on_sdk_reconnecting(self, attempt):
        self.logger.warning(f"MarketDataStreamerV3 reconnecting (attempt {attempt})")
        self._dispatch("reconnecting", {"attempt": attempt})
    
    def _on_sdk_auto_reconnect_stopped(self):
        self.logger.error("MarketDataStreamerV3 auto-reconnect stopped")
        self._dispatch("autoReconnectStopped", {"status": "stopped"})
    
    def _dispatch(self, event: str, data: Any):
        with self._lock:
            for callback in self._callbacks.get(event, []):
                try:
                    callback(data)
                except Exception as e:
                    self.logger.error(f"Callback error for {event}: {e}")

# ============================================================================
# PORTFOLIO DATA STREAMER
# ============================================================================

@dataclass
class PortfolioUpdate:
    update_type: str
    user_id: str
    order_id: Optional[str] = None
    instrument_key: Optional[str] = None
    transaction_type: Optional[str] = None
    quantity: Optional[int] = None
    filled_quantity: Optional[int] = None
    pending_quantity: Optional[int] = None
    status: Optional[str] = None
    average_price: Optional[float] = None
    price: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.now)
    
    @classmethod
    def from_sdk_message(cls, message: Dict) -> 'PortfolioUpdate':
        update = cls(
            update_type=message.get('update_type', 'unknown'),
            user_id=message.get('user_id', message.get('userId', '')),
            order_id=message.get('order_id'),
            instrument_key=message.get('instrument_key', message.get('instrument_token')),
            transaction_type=message.get('transaction_type'),
            quantity=message.get('quantity'),
            filled_quantity=message.get('filled_quantity'),
            pending_quantity=message.get('pending_quantity'),
            status=message.get('status'),
            average_price=message.get('average_price'),
            price=message.get('price')
        )
        return update

class VolGuardPortfolioStreamer:
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client
        self.streamer: Optional[upstox_client.PortfolioDataStreamer] = None
        self._callbacks: Dict[str, List[Callable]] = {
            "message": [],
            "open": [],
            "close": [],
            "error": [],
            "reconnecting": [],
            "autoReconnectStopped": []
        }
        self._lock = threading.RLock()
        self.is_connected = False
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self._latest_orders: Dict[str, Dict] = {}
    
    def on(self, event: str, callback: Callable):
        with self._lock:
            if event in self._callbacks:
                self._callbacks[event].append(callback)
    
    def connect(self, 
                order_update: bool = True,
                position_update: bool = True,
                holding_update: bool = True,
                gtt_update: bool = True):
        try:
            self.streamer = upstox_client.PortfolioDataStreamer(
                self.api_client,
                order_update=order_update,
                position_update=position_update,
                holding_update=holding_update,
                gtt_update=gtt_update
            )
            
            self.streamer.on("open", self._on_sdk_open)
            self.streamer.on("close", self._on_sdk_close)
            self.streamer.on("message", self._on_sdk_message)
            self.streamer.on("error", self._on_sdk_error)
            self.streamer.on("reconnecting", self._on_sdk_reconnecting)
            self.streamer.on("autoReconnectStopped", self._on_sdk_auto_reconnect_stopped)
            
            self.streamer.auto_reconnect(True, 5, 10)
            self.streamer.connect()
            
            update_types = []
            if order_update: update_types.append("order")
            if position_update: update_types.append("position")
            if holding_update: update_types.append("holding")
            if gtt_update: update_types.append("gtt_order")
            
            self.logger.info(f"PortfolioDataStreamer connecting - updates: {update_types}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect PortfolioDataStreamer: {e}")
            self._dispatch("error", {"type": "connection_error", "message": str(e)})
    
    def disconnect(self):
        with self._lock:
            if self.streamer:
                try:
                    self.streamer.disconnect()
                    self.is_connected = False
                    self.logger.info("PortfolioDataStreamer disconnected")
                except Exception as e:
                    self.logger.error(f"Disconnect error: {e}")
    
    def get_order_status(self, order_id: str) -> Optional[Dict]:
        with self._lock:
            status = self._latest_orders.get(order_id)
            if not status:
                # Fallback to REST API if WebSocket hasn't delivered update
                try:
                    from upstox_client.rest import ApiException
                    response = self.api_client.order_api.get_order_details("2.0", order_id=order_id)
                    if response.status == "success" and response.data:
                        status = {
                            "status": response.data.status,
                            "filled_quantity": response.data.filled_quantity,
                            "average_price": response.data.average_price,
                            "timestamp": datetime.now().isoformat()
                        }
                        self._latest_orders[order_id] = status
                except Exception as e:
                    self.logger.error(f"REST fallback for order {order_id} failed: {e}")
            return status
    
    def _on_sdk_open(self):
        self.is_connected = True
        self.logger.info("PortfolioDataStreamer connected")
        self._dispatch("open", {"status": "connected"})
    
    def _on_sdk_close(self):
        self.is_connected = False
        self.logger.info("PortfolioDataStreamer disconnected")
        self._dispatch("close", {"status": "disconnected"})
    
    def _on_sdk_message(self, message):
        try:
            update = PortfolioUpdate.from_sdk_message(message)
            
            with self._lock:
                if update.order_id:
                    self._latest_orders[update.order_id] = {
                        "status": update.status,
                        "filled_quantity": update.filled_quantity,
                        "average_price": update.average_price,
                        "timestamp": update.timestamp.isoformat()
                    }
            
            self._dispatch("message", {
                "type": "portfolio_update",
                "update_type": update.update_type,
                "data": update
            })
            
            if update.update_type == "order":
                self.logger.info(
                    f"Order Update: {update.order_id} - {update.status} "
                    f"Qty: {update.filled_quantity}/{update.quantity} "
                    f"Price: {update.average_price}"
                )
                
        except Exception as e:
            self.logger.error(f"Error processing portfolio message: {e}")
    
    def _on_sdk_error(self, error):
        self.logger.error(f"PortfolioDataStreamer error: {error}")
        self._dispatch("error", {"type": "sdk_error", "message": str(error)})
    
    def _on_sdk_reconnecting(self, attempt):
        self.logger.warning(f"PortfolioDataStreamer reconnecting (attempt {attempt})")
        self._dispatch("reconnecting", {"attempt": attempt})
    
    def _on_sdk_auto_reconnect_stopped(self):
        self.logger.error("PortfolioDataStreamer auto-reconnect stopped")
        self._dispatch("autoReconnectStopped", {"status": "stopped"})
    
    def _dispatch(self, event: str, data: Any):
        with self._lock:
            for callback in self._callbacks.get(event, []):
                try:
                    callback(data)
                except Exception as e:
                    self.logger.error(f"Callback error for {event}: {e}")

# ============================================================================
# UPSTOX FETCHER - FIXED: Added subscribe_market_data method
# ============================================================================

class UpstoxFetcher:
    def __init__(self, token: str):
        if not token:
            raise ValueError("Upstox access token is required!")
        
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = token
        self.api_client = upstox_client.ApiClient(self.configuration)
        
        self.history_api = upstox_client.HistoryV3Api(self.api_client)
        self.quote_api = upstox_client.MarketQuoteApi(self.api_client)
        self.options_api = upstox_client.OptionsApi(self.api_client)
        self.user_api = upstox_client.UserApi(self.api_client)
        self.order_api = upstox_client.OrderApi(self.api_client)
        self.order_api_v3 = upstox_client.OrderApiV3(self.api_client)
        self.quote_api_v3 = upstox_client.MarketQuoteV3Api(self.api_client)
        self.portfolio_api = upstox_client.PortfolioApi(self.api_client)
        self.charge_api = upstox_client.ChargeApi(self.api_client)
        self.pnl_api = upstox_client.TradeProfitAndLossApi(self.api_client)
        self.market_api = upstox_client.MarketHolidaysAndTimingsApi(self.api_client)
        
        # WebSocket Streamers
        self.market_streamer = VolGuardMarketStreamer(self.api_client)
        self.portfolio_streamer = VolGuardPortfolioStreamer(self.api_client)
        
        self.fill_tracker = FillQualityTracker()
        
        # Rate limiting
        self._request_timestamps = []
        self._rate_limit_lock = threading.RLock()
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="fetcher")
        
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("✅ UpstoxFetcher initialized - V3.3 Full SDK + WebSocket")
    
    def _check_rate_limit(self, max_requests: int = 50, window_seconds: int = 1):
        with self._rate_limit_lock:
            now = time.time()
            self._request_timestamps = [t for t in self._request_timestamps 
                                      if now - t < window_seconds]
            
            if len(self._request_timestamps) >= max_requests:
                sleep_time = self._request_timestamps[0] + window_seconds - now
                if sleep_time > 0:
                    self.logger.warning(f"Rate limit reached, sleeping {sleep_time:.2f}s")
                    time.sleep(sleep_time)
            
            self._request_timestamps.append(now)
    
    # ========================================================================
    # WEBSOCKET STREAMER MANAGEMENT - FIXED: Added missing methods
    # ========================================================================
    
    def start_market_streamer(self, instrument_keys: List[str], mode: str = "ltpc"):
        """Start market data WebSocket streamer"""
        self.market_streamer.connect(instrument_keys, mode)
        return self.market_streamer
    
    def start_portfolio_streamer(self, 
                                order_update: bool = True,
                                position_update: bool = True,
                                holding_update: bool = True,
                                gtt_update: bool = True):
        """Start portfolio WebSocket streamer"""
        self.portfolio_streamer.connect(
            order_update=order_update,
            position_update=position_update,
            holding_update=holding_update,
            gtt_update=gtt_update
        )
        return self.portfolio_streamer
    
    def subscribe_market_data(self, instrument_keys: List[str], mode: str = "ltpc"):
        """Subscribe to market data updates - FIXED: Method now exists"""
        return self.market_streamer.subscribe(instrument_keys, mode)
    
    def unsubscribe_market_data(self, instrument_keys: List[str]):
        """Unsubscribe from market data"""
        return self.market_streamer.unsubscribe(instrument_keys)
    
    def get_ltp(self, instrument_key: str) -> Optional[float]:
        """Get LTP from WebSocket cache"""
        return self.market_streamer.get_ltp(instrument_key)
    
    def get_bulk_ltp(self, instrument_keys: List[str]) -> Dict[str, float]:
        """Get bulk LTP from WebSocket cache"""
        return self.market_streamer.get_bulk_ltp(instrument_keys)
    
    # ========================================================================
    # PORTFOLIO API METHODS
    # ========================================================================
    
    def get_live_positions(self) -> Optional[List[Dict]]:
        try:
            self._check_rate_limit()
            response = self.portfolio_api.get_positions("2.0")
            
            if response.status == "success" and response.data:
                positions = []
                for pos in response.data:
                    positions.append({
                        "instrument_token": pos.instrument_token,
                        "quantity": pos.quantity,
                        "buy_price": pos.average_price,
                        "current_price": pos.last_price,
                        "pnl": pos.pnl,
                        "product": pos.product
                    })
                return positions
            
        except ApiException as e:
            self.logger.error(f"Portfolio fetch error: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected portfolio error: {e}")
        
        return []
    
    def reconcile_positions_with_db(self, db: Session) -> Dict:
        try:
            db_trades = db.query(TradeJournal).filter(
                TradeJournal.status == TradeStatus.ACTIVE.value
            ).all()
            
            db_instruments = set()
            for trade in db_trades:
                legs = json.loads(trade.legs_data)
                for leg in legs:
                    db_instruments.add(leg['instrument_token'])
            
            broker_positions = self.get_live_positions()
            broker_instruments = {p['instrument_token'] for p in broker_positions}
            
            in_db_not_broker = db_instruments - broker_instruments
            in_broker_not_db = broker_instruments - db_instruments
            
            reconciled = len(in_db_not_broker) == 0 and len(in_broker_not_db) == 0
            
            return {
                "timestamp": datetime.now().isoformat(),
                "db_positions": len(db_instruments),
                "broker_positions": len(broker_instruments),
                "matched": len(db_instruments.intersection(broker_instruments)),
                "in_db_not_broker": list(in_db_not_broker),
                "in_broker_not_db": list(in_broker_not_db),
                "reconciled": reconciled
            }
            
        except Exception as e:
            self.logger.error(f"Position reconciliation error: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "reconciled": False
            }
    
    # ========================================================================
    # CHARGE API METHODS - FIXED: Now actually called before execution
    # ========================================================================
    
    def validate_margin_for_strategy(self, legs: List[OptionLeg]) -> Tuple[bool, float, float]:
        """Pre-validate margin BEFORE placing orders"""
        try:
            self._check_rate_limit()
            
            instruments = []
            for leg in legs:
                instruments.append(upstox_client.Instrument(
                    instrument_key=leg.instrument_token,
                    quantity=leg.quantity,
                    transaction_type="SELL" if leg.action == "SELL" else "BUY",
                    product=leg.product
                ))
            
            body = upstox_client.GetMarginRequest(instruments=instruments)
            response = self.charge_api.get_margin(body, "2.0")
            
            if response.status == "success" and response.data:
                required_margin = float(response.data.required_margin)
                available_margin = self.get_funds() or 0.0
                
                has_sufficient = available_margin >= required_margin
                
                self.logger.info(
                    f"Margin Check: Required=₹{required_margin:,.2f}, "
                    f"Available=₹{available_margin:,.2f}, "
                    f"Sufficient={has_sufficient}"
                )
                
                return has_sufficient, required_margin, available_margin
            
        except ApiException as e:
            self.logger.error(f"Margin validation API error: {e}")
        except Exception as e:
            self.logger.error(f"Margin validation error: {e}")
        
        return False, 0.0, 0.0
    
    # ========================================================================
    # P&L API METHODS
    # ========================================================================
    
    def get_broker_pnl_for_date(self, target_date: date) -> Optional[float]:
        try:
            self._check_rate_limit()
            
            date_str = target_date.strftime("%Y-%m-%d")
            segment = "FO"
            
            if target_date.month >= 4:
                fy = f"{str(target_date.year)[2:]}{str(target_date.year + 1)[2:]}"
            else:
                fy = f"{str(target_date.year - 1)[2:]}{str(target_date.year)[2:]}"
            
            response = self.pnl_api.get_profit_and_loss_data(
                segment=segment,
                financial_year=fy,
                from_date=date_str,
                to_date=date_str,
                api_version="2.0"
            )
            
            if response.status == "success" and response.data:
                total_pnl = sum([trade.realised_profit for trade in response.data])
                self.logger.info(f"Broker P&L for {date_str}: ₹{total_pnl:,.2f}")
                return total_pnl
            
        except ApiException as e:
            self.logger.error(f"Broker P&L fetch API error: {e}")
        except Exception as e:
            self.logger.error(f"Broker P&L fetch error: {e}")
        
        return None
    
    # ========================================================================
    # MARKET STATUS API METHODS
    # ========================================================================
    
    def is_trading_day(self) -> bool:
        try:
            self._check_rate_limit()
            response = self.market_api.get_market_status("FO", "2.0")
            
            if response.status == "success" and response.data:
                for exchange in response.data:
                    if exchange.status and "open" in exchange.status.lower():
                        return True
            
        except ApiException as e:
            self.logger.error(f"Trading day check API error: {e}")
        except Exception as e:
            self.logger.error(f"Trading day check error: {e}")
        
        return True
    
    def get_market_status(self) -> str:
        try:
            self._check_rate_limit()
            response = self.market_api.get_market_status("FO", "2.0")
            
            if response.status == "success" and response.data:
                for exchange in response.data:
                    if exchange.status:
                        return exchange.status
            
        except ApiException as e:
            self.logger.error(f"Market status API error: {e}")
        except Exception as e:
            self.logger.error(f"Market status error: {e}")
        
        return "UNKNOWN"
    
    def is_market_open_now(self) -> bool:
        ist_tz = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist_tz)
        current_time = now.time()
        
        if current_time < SystemConfig.MARKET_OPEN_IST or current_time > SystemConfig.MARKET_CLOSE_IST:
            return False
        
        status = self.get_market_status()
        return "open" in status.lower()
    
    def get_market_holidays(self, days_ahead: int = 30) -> List[date]:
        try:
            self._check_rate_limit()
            response = self.market_api.get_market_holidays("FO", "2.0")
            
            if response.status == "success" and response.data:
                holidays = []
                today = date.today()
                cutoff = today + timedelta(days=days_ahead)
                
                for holiday in response.data:
                    if hasattr(holiday, 'holiday_date'):
                        holiday_date = datetime.strptime(
                            holiday.holiday_date, "%Y-%m-%d"
                        ).date()
                        if today <= holiday_date <= cutoff:
                            holidays.append(holiday_date)
                
                return sorted(holidays)
            
        except ApiException as e:
            self.logger.error(f"Market holidays fetch API error: {e}")
        except Exception as e:
            self.logger.error(f"Market holidays fetch error: {e}")
        
        return []
    
    # ========================================================================
    # EMERGENCY EXIT
    # ========================================================================
    
    def emergency_exit_all_positions(self) -> Dict:
        try:
            positions = self.get_live_positions()
            
            if not positions:
                return {
                    "success": True,
                    "message": "No positions to exit",
                    "orders_placed": 0
                }
            
            order_ids = []
            
            for pos in positions:
                try:
                    if pos['quantity'] == 0:
                        continue
                    
                    transaction_type = "SELL" if pos['quantity'] > 0 else "BUY"
                    qty = abs(pos['quantity'])
                    
                    body = upstox_client.PlaceOrderV3Request(
                        quantity=qty,
                        product="D",
                        validity="DAY",
                        price=0.0,
                        tag="EMERGENCY_EXIT",
                        instrument_token=pos['instrument_token'],
                        order_type="MARKET",
                        transaction_type=transaction_type,
                        disclosed_quantity=0,
                        trigger_price=0.0,
                        is_amo=False,
                        slice=True
                    )
                    
                    response = self.order_api_v3.place_order(
                        body, 
                        algo_name="VOLGUARD_EMERGENCY"
                    )
                    
                    if response.status == "success" and response.data:
                        if hasattr(response.data, 'order_ids') and response.data.order_ids:
                            order_ids.extend(response.data.order_ids)
                        elif hasattr(response.data, 'order_id'):
                            order_ids.append(response.data.order_id)
                        
                        self.logger.info(
                            f"Emergency exit order placed for {pos['instrument_token']}"
                        )
                
                except Exception as e:
                    self.logger.error(f"Emergency exit order failed: {e}")
            
            return {
                "success": len(order_ids) > 0,
                "message": f"Placed {len(order_ids)} emergency exit orders",
                "orders_placed": len(order_ids),
                "order_ids": order_ids
            }
            
        except Exception as e:
            self.logger.error(f"Emergency exit failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "orders_placed": 0
            }
    
    # ========================================================================
    # EXISTING METHODS
    # ========================================================================
    
    def get_funds(self) -> Optional[float]:
        try:
            self._check_rate_limit()
            response = self.user_api.get_user_fund_margin("2.0")
            if response.status == "success" and response.data:
                return float(response.data.equity.available_margin)
        except Exception as e:
            self.logger.error(f"Fund fetch error: {e}")
        return None
    
    def get_order_status(self, order_id: str) -> Optional[str]:
        try:
            self._check_rate_limit()
            response = self.order_api.get_order_details("2.0", order_id=order_id)
            if response.status == "success" and response.data:
                return response.data.status
        except Exception as e:
            self.logger.error(f"Order status fetch error: {e}")
        return None
    
    def get_order_details(self, order_id: str) -> Optional[Dict]:
        try:
            self._check_rate_limit()
            response = self.order_api.get_order_details("2.0", order_id=order_id)
            if response.status == "success" and response.data:
                data = response.data
                return {
                    "order_id": data.order_id,
                    "status": data.status,
                    "filled_quantity": data.filled_quantity,
                    "average_price": data.average_price,
                    "order_timestamp": data.order_timestamp,
                    "exchange_timestamp": data.exchange_timestamp,
                    "instrument_token": data.instrument_token,
                    "quantity": data.quantity,
                    "price": data.price,
                    "transaction_type": data.transaction_type
                }
        except Exception as e:
            self.logger.error(f"Order details fetch error: {e}")
        return None
    
    def history(self, key: str, days: int = 400) -> Optional[pd.DataFrame]:
        try:
            self._check_rate_limit()
            to_date = date.today().strftime("%Y-%m-%d")
            from_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            encoded_key = urllib.parse.quote(key, safe='')
            
            response = self.history_api.get_historical_candle_data_v3(
                instrument_key=encoded_key,
                interval="1day",
                to_date=to_date,
                from_date=from_date,
                api_version="2.0"
            )
            
            if response.status == "success" and response.data and response.data.candles:
                candles = response.data.candles
                df = pd.DataFrame(
                    candles, 
                    columns=["timestamp", "open", "high", "low", "close", "volume", "oi"]
                )
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                return df.astype(float).sort_index()
            
        except ApiException as e:
            self.logger.error(f"History V3 fetch error: {e}")
        except Exception as e:
            self.logger.error(f"History V3 fetch error: {e}")
        
        return None
    
    def live(self, keys: List[str]) -> Optional[Dict]:
        """DEPRECATED: Use WebSocket streamer instead"""
        self.logger.warning("live() using REST - migrate to WebSocket streamer")
        
        try:
            self._check_rate_limit()
            encoded_keys = [urllib.parse.quote(k, safe='') for k in keys]
            response = self.quote_api.get_ltp(instrument_key=",".join(encoded_keys))
            
            if response.status == "success" and response.data:
                result = {}
                for key in keys:
                    item = response.data.get(key)
                    if item:
                        result[key] = item.last_price
                return result
                
        except ApiException as e:
            self.logger.error(f"Live fetch error: {e}")
        
        return None
    
    def get_expiries(self) -> Tuple[Optional[date], Optional[date], Optional[date], int, List[date]]:
        try:
            self._check_rate_limit()
            response = self.options_api.get_option_contracts(
                instrument_key=SystemConfig.NIFTY_KEY
            )
            
            if response.status == "success" and response.data:
                data = response.data
                
                lot_size = 50
                if data and len(data) > 0:
                    lot_size = data[0].lot_size if hasattr(data[0], 'lot_size') else 50
                
                expiry_dates = sorted(list(set([
                    datetime.strptime(contract.expiry, "%Y-%m-%d").date()
                    for contract in data if hasattr(contract, 'expiry') and contract.expiry
                ])))
                
                valid_dates = [d for d in expiry_dates if d >= date.today()]
                if not valid_dates:
                    return None, None, None, lot_size, []
                
                weekly = valid_dates[0]
                next_weekly = valid_dates[1] if len(valid_dates) > 1 else valid_dates[0]
                
                current_month = weekly.month
                current_year = weekly.year
                monthly_candidates = [
                    d for d in valid_dates 
                    if d.month == current_month and d.year == current_year
                ]
                monthly = monthly_candidates[-1] if monthly_candidates else valid_dates[-1]
                
                if weekly == monthly and len(valid_dates) > 1:
                    next_month_num = current_month + 1 if current_month < 12 else 1
                    next_year_num = current_year if current_month < 12 else current_year + 1
                    next_month_candidates = [
                        d for d in valid_dates 
                        if d.month == next_month_num and d.year == next_year_num
                    ]
                    if next_month_candidates:
                        monthly = next_month_candidates[-1]
                
                return weekly, monthly, next_weekly, lot_size, expiry_dates
                
        except ApiException as e:
            self.logger.error(f"Expiries fetch error: {e}")
        
        return None, None, None, 50, []
    
    def chain(self, expiry_date: date) -> Optional[pd.DataFrame]:
        try:
            self._check_rate_limit()
            expiry_str = expiry_date.strftime("%Y-%m-%d")
            
            response = self.options_api.get_put_call_option_chain(
                instrument_key=SystemConfig.NIFTY_KEY,
                expiry_date=expiry_str
            )
            
            if response.status == "success" and response.data:
                rows = []
                for item in response.data:
                    try:
                        call_opts = item.call_options
                        put_opts = item.put_options
                        
                        def get_val(obj, attr, sub_attr=None):
                            if not obj: return 0
                            if sub_attr:
                                parent = getattr(obj, attr, None)
                                return getattr(parent, sub_attr, 0) if parent else 0
                            return getattr(obj, attr, 0)
                        
                        call_pop = 0.0
                        put_pop = 0.0
                        
                        if call_opts and hasattr(call_opts, 'option_greeks'):
                            call_pop = getattr(call_opts.option_greeks, 'pop', 0) or 0
                        
                        if put_opts and hasattr(put_opts, 'option_greeks'):
                            put_pop = getattr(put_opts.option_greeks, 'pop', 0) or 0
                        
                        rows.append({
                            'strike': item.strike_price,
                            'ce_instrument_key': get_val(call_opts, 'instrument_key'),
                            'ce_ltp': get_val(call_opts, 'market_data', 'ltp'),
                            'ce_bid': get_val(call_opts, 'bid_price'),
                            'ce_ask': get_val(call_opts, 'ask_price'),
                            'ce_oi': get_val(call_opts, 'market_data', 'oi'),
                            'ce_iv': get_val(call_opts, 'option_greeks', 'iv'),
                            'ce_delta': get_val(call_opts, 'option_greeks', 'delta'),
                            'ce_gamma': get_val(call_opts, 'option_greeks', 'gamma'),
                            'ce_theta': get_val(call_opts, 'option_greeks', 'theta'),
                            'ce_vega': get_val(call_opts, 'option_greeks', 'vega'),
                            'ce_pop': call_pop,
                            'pe_instrument_key': get_val(put_opts, 'instrument_key'),
                            'pe_ltp': get_val(put_opts, 'market_data', 'ltp'),
                            'pe_bid': get_val(put_opts, 'bid_price'),
                            'pe_ask': get_val(put_opts, 'ask_price'),
                            'pe_oi': get_val(put_opts, 'market_data', 'oi'),
                            'pe_iv': get_val(put_opts, 'option_greeks', 'iv'),
                            'pe_delta': get_val(put_opts, 'option_greeks', 'delta'),
                            'pe_gamma': get_val(put_opts, 'option_greeks', 'gamma'),
                            'pe_theta': get_val(put_opts, 'option_greeks', 'theta'),
                            'pe_vega': get_val(put_opts, 'option_greeks', 'vega'),
                            'pe_pop': put_pop,
                        })
                    except Exception as e:
                        self.logger.error(f"Chain row error: {e}")
                        continue
                
                if rows:
                    return pd.DataFrame(rows)
                
        except ApiException as e:
            self.logger.error(f"Chain fetch error: {e}")
        except Exception as e:
            self.logger.error(f"Chain fetch error: {e}")
        
        return None
    
    def get_greeks(self, instrument_keys: List[str]) -> Dict[str, Dict]:
        try:
            self._check_rate_limit()
            encoded_keys = [urllib.parse.quote(k, safe='') for k in instrument_keys]
            response = self.quote_api_v3.get_market_quote_option_greek(
                instrument_key=",".join(encoded_keys),
                api_version="2.0"
            )
            
            result = {}
            if response.status == "success" and response.data:
                for key, data in response.data.items():
                    greeks_data = getattr(data, 'option_greeks', None) if hasattr(data, 'option_greeks') else None
                    
                    if greeks_data:
                        result[key] = {
                            'iv': getattr(greeks_data, 'iv', 0) or 0,
                            'delta': getattr(greeks_data, 'delta', 0) or 0,
                            'gamma': getattr(greeks_data, 'gamma', 0) or 0,
                            'theta': getattr(greeks_data, 'theta', 0) or 0,
                            'vega': getattr(greeks_data, 'vega', 0) or 0,
                            'spot_price': getattr(data, 'underlying_spot_price', 0) or 0
                        }
                    else:
                        result[key] = {
                            'iv': 0,
                            'delta': 0,
                            'gamma': 0,
                            'theta': 0,
                            'vega': 0,
                            'spot_price': 0
                        }
            
            return result
        
        except Exception as e:
            self.logger.error(f"Greeks fetch error: {e}")
            return {}
    
    def __del__(self):
        """Cleanup executor on deletion"""
        if hasattr(self, '_executor'):
            self._executor.shutdown(wait=False)


# ============================================================================
# UPSTOX ORDER EXECUTOR - FIXED: GTT ENTRY rule removed, margin validation added
# ============================================================================

class UpstoxOrderExecutor:
    PRODUCT_MAP = {
        "I": "I",
        "D": "D",
        "MTF": "MTF",
        "CO": "CO"
    }
    
    ORDER_TYPE_MAP = {
        "MARKET": "MARKET",
        "LIMIT": "LIMIT",
        "SL": "SL",
        "SL-M": "SL-M"
    }
    
    VALIDITY_MAP = {
        "DAY": "DAY",
        "IOC": "IOC"
    }
    
    def __init__(self, fetcher: UpstoxFetcher):
        self.fetcher = fetcher
        self.logger = logging.getLogger(self.__class__.__name__)
        self.max_retries = 3
        self.base_delay = 1.0
        self.algo_name = "VOLGUARD_V33"
    
    def _wait_for_fills(self, order_ids: List[str], timeout_seconds: int = 15) -> List[str]:
        """Wait for PortfolioDataStreamer to confirm fills before placing GTT"""
        start_time = time.time()
        filled_orders = []
        
        while time.time() - start_time < timeout_seconds:
            for oid in order_ids:
                if oid in filled_orders:
                    continue
                    
                status = self.fetcher.portfolio_streamer.get_order_status(oid)
                if status and isinstance(status, dict) and status.get('status') in ['complete', 'filled']:
                    filled_orders.append(oid)
                    self.logger.info(f"✅ Order {oid} confirmed filled")
            
            if len(filled_orders) == len(order_ids):
                break
                
            time.sleep(0.5)
        
        if len(filled_orders) < len(order_ids):
            missing = set(order_ids) - set(filled_orders)
            self.logger.warning(f"⚠️ Orders not filled within timeout: {missing}")
        
        return filled_orders
    
    def place_multi_order(self, strategy: ConstructedStrategy) -> Dict:
        """
        Execute complete strategy with multi-order API
        - FIXED: Now calls validate_margin_for_strategy before execution
        - FIXED: Added max loss cap for position sizing
        """
        # ===== FIXED: Call margin validation =====
        has_margin, required, available = self.fetcher.validate_margin_for_strategy(strategy.legs)
        if not has_margin:
            msg = f"❌ INSUFFICIENT MARGIN. Required: ₹{required:,.2f}, Available: ₹{available:,.2f}"
            self.logger.error(msg)
            return {
                "success": False,
                "order_ids": [],
                "message": msg
            }
        
        # ===== FIXED: Check max loss against risk limit =====
        max_risk_per_trade = DynamicConfig.BASE_CAPITAL * (DynamicConfig.MAX_POSITION_RISK_PCT / 100)
        if strategy.max_loss > max_risk_per_trade:
            msg = f"❌ POSITION RISK TOO HIGH. Max loss: ₹{strategy.max_loss:,.2f} > Limit: ₹{max_risk_per_trade:,.2f}"
            self.logger.error(msg)
            return {
                "success": False,
                "order_ids": [],
                "message": msg
            }
        
        buy_legs = [leg for leg in strategy.legs if leg.action == "BUY"]
        sell_legs = [leg for leg in strategy.legs if leg.action == "SELL"]
        ordered_legs = buy_legs + sell_legs
        
        orders = []
        for i, leg in enumerate(ordered_legs):
            correlation_id = f"{strategy.strategy_id[-8:]}_leg{i}_{int(time.time())}"[:20]
            
            need_slicing = leg.quantity > 1000
            
            order = upstox_client.MultiOrderRequest(
                quantity=leg.quantity,
                product="D",
                validity=self.VALIDITY_MAP.get("DAY", "DAY"),
                price=leg.entry_price if leg.entry_price > 0 else 0.0,
                tag=strategy.strategy_id[:40],
                instrument_token=leg.instrument_token,
                order_type=self.ORDER_TYPE_MAP.get("LIMIT", "LIMIT"),
                transaction_type=leg.action,
                disclosed_quantity=0,
                trigger_price=0.0,
                is_amo=False,
                slice=need_slicing,
                correlation_id=correlation_id
            )
            orders.append(order)
        
        try:
            response = self.fetcher.order_api.place_multi_order(body=orders)
            
            if response.status in ["success", "partial_success"]:
                order_ids = []
                
                if hasattr(response, 'data') and response.data:
                    for item in response.data:
                        order_ids.append(item.order_id)
                
                errors = []
                if hasattr(response, 'errors') and response.errors:
                    for error in response.errors:
                        errors.append({
                            "correlation_id": getattr(error, 'correlation_id', None),
                            "error_code": getattr(error, 'error_code', 'UNKNOWN'),
                            "message": getattr(error, 'message', 'Unknown error'),
                            "instrument_key": getattr(error, 'instrument_key', None)
                        })
                        self.logger.error(
                            f"Order failed: {getattr(error, 'error_code', 'UNKNOWN')} "
                            f"- {getattr(error, 'message', '')}"
                        )
                
                # Wait for fills before placing GTT orders
                filled_orders = self._wait_for_fills(order_ids)
                
                # Capture entry Greeks for attribution
                instrument_keys = [leg.instrument_token for leg in strategy.legs]
                greeks_snapshot = self.fetcher.get_greeks(instrument_keys)
                
                # Place GTT only for filled short positions
                gtt_ids = []
                if filled_orders:
                    gtt_ids = self._place_gtt_stop_losses(strategy, filled_orders)
                
                return {
                    "success": True,
                    "order_ids": order_ids,
                    "filled_order_ids": filled_orders,
                    "gtt_order_ids": gtt_ids,
                    "entry_greeks": greeks_snapshot,
                    "errors": errors,
                    "message": f"Strategy executed. Orders: {len(order_ids)}, Filled: {len(filled_orders)}, GTTs: {len(gtt_ids)}"
                }
            
            return {"success": False, "order_ids": [], "message": f"Failed: {response.status}"}
            
        except ApiException as e:
            self.logger.error(f"Multi-order failed: {e}")
            
            if hasattr(self, 'max_retries') and self.max_retries > 0:
                self.max_retries -= 1
                delay = self.base_delay * (2 ** (3 - self.max_retries))
                self.logger.info(f"Retrying in {delay:.1f}s... (retries left: {self.max_retries})")
                time.sleep(delay)
                return self.place_multi_order(strategy)
            
            return {
                "success": False,
                "order_ids": [],
                "message": f"SDK API Exception: {str(e)}"
            }
    
    def _place_gtt_stop_losses(self, strategy: ConstructedStrategy, filled_order_ids: List[str]) -> List[str]:
        """
        Place multi-leg GTT orders with trailing stop loss
        - FIXED: REMOVED ENTRY rule (only STOPLOSS + TARGET)
        """
        gtt_ids = []
        
        for leg in strategy.legs:
            if leg.action != "SELL":
                continue
            
            stop_price = round(leg.entry_price * 2.0, 2)
            target_price = round(leg.entry_price * 0.3, 2)
            trailing_gap = 0.1
            
            try:
                # FIXED: Only TARGET and STOPLOSS rules, no ENTRY
                target_rule = upstox_client.GttRule(
                    strategy="TARGET",
                    trigger_type="IMMEDIATE",
                    trigger_price=target_price
                )
                
                stoploss_rule = upstox_client.GttRule(
                    strategy="STOPLOSS",
                    trigger_type="IMMEDIATE",
                    trigger_price=stop_price,
                    trailing_gap=trailing_gap
                )
                
                body = upstox_client.GttPlaceOrderRequest(
                    type="MULTIPLE",
                    instrument_token=leg.instrument_token,
                    quantity=leg.quantity,
                    product="D",
                    transaction_type="BUY",
                    rules=[target_rule, stoploss_rule]  # ✅ No ENTRY rule
                )
                
                response = self.fetcher.order_api_v3.place_gtt_order(body=body)
                
                if response.status == "success" and response.data:
                    gtt_id = response.data.gtt_order_ids[0] if hasattr(response.data, 'gtt_order_ids') else None
                    if gtt_id:
                        gtt_ids.append(gtt_id)
                        self.logger.info(
                            f"✅ GTT placed for {leg.strike} {leg.option_type} "
                            f"SL: ₹{stop_price}, Target: ₹{target_price}"
                        )
                else:
                    self.logger.error(f"❌ GTT failed for {leg.strike}: {response}")
                    
            except Exception as e:
                self.logger.error(f"❌ GTT exception for {leg.strike}: {e}")
        
        return gtt_ids
    
    def cancel_gtt_orders(self, gtt_ids: List[str]) -> bool:
        success = True
        for gtt_id in gtt_ids:
            try:
                self.fetcher.order_api_v3.cancel_gtt_order(
                    upstox_client.GttCancelOrderRequest(gtt_order_id=gtt_id)
                )
                self.logger.info(f"Cancelled GTT: {gtt_id}")
            except Exception as e:
                self.logger.error(f"Failed to cancel GTT {gtt_id}: {e}")
                success = False
        return success
    
    def exit_position(self, trade: TradeJournal, exit_reason: str, 
                     current_prices: Dict, db: Session) -> Dict:
        legs_data = json.loads(trade.legs_data)
        
        if trade.gtt_order_ids:
            gtt_ids = json.loads(trade.gtt_order_ids)
            self.cancel_gtt_orders(gtt_ids)
        
        orders_placed = []
        realized_pnl = 0.0
        
        for leg in legs_data:
            try:
                transaction_type = "BUY" if leg['action'] == 'SELL' else "SELL"
                qty = leg['quantity']
                
                order = upstox_client.PlaceOrderV3Request(
                    quantity=abs(qty),
                    product="D",
                    validity="DAY",
                    price=0.0,
                    tag=f"EXIT_{trade.strategy_id[:15]}",
                    instrument_token=leg['instrument_token'],
                    order_type="MARKET",
                    transaction_type=transaction_type,
                    disclosed_quantity=0,
                    trigger_price=0.0,
                    is_amo=False,
                    slice=True
                )
                
                response = self.fetcher.order_api_v3.place_order(
                    order,
                    algo_name="VOLGUARD_EXIT"
                )
                
                if response.status == "success" and response.data:
                    if hasattr(response.data, 'order_ids') and response.data.order_ids:
                        orders_placed.extend(response.data.order_ids)
                    elif hasattr(response.data, 'order_id'):
                        orders_placed.append(response.data.order_id)
                    
                    self.logger.info(f"Exit order placed for {leg['instrument_token']}")
                
            except Exception as e:
                self.logger.error(f"Exit order failed: {e}")
            
            exit_price = current_prices.get(leg['instrument_token'], leg['entry_price'])
            multiplier = -1 if leg['action'] == 'SELL' else 1
            leg_pnl = (exit_price - leg['entry_price']) * leg['quantity'] * multiplier
            realized_pnl += leg_pnl
        
        trade.exit_time = datetime.now()
        trade.status = exit_reason
        trade.exit_reason = exit_reason
        trade.realized_pnl = realized_pnl
        
        db.commit()
        
        self.logger.info(
            f"Trade {trade.strategy_id} closed: P&L=₹{realized_pnl:.2f}, Reason={exit_reason}"
        )
        
        return {
            "success": len(orders_placed) > 0,
            "orders_placed": orders_placed,
            "realized_pnl": realized_pnl,
            "exit_reason": exit_reason
        }


class MockExecutor:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.order_counter = 1000
    
    def place_multi_order(self, strategy: ConstructedStrategy) -> Dict:
        order_ids = []
        gtt_ids = []
        
        for leg in strategy.legs:
            order_id = f"MOCK_{self.order_counter}"
            self.order_counter += 1
            order_ids.append(order_id)
            
            self.logger.info(
                f"MOCK ORDER: {leg.action} {leg.quantity} {leg.option_type} {leg.strike} "
                f"@ ₹{leg.entry_price:.2f} | Order ID: {order_id}"
            )
            
            if leg.action == "SELL":
                gtt_id = f"MOCK_GTT_{self.order_counter}"
                gtt_ids.append(gtt_id)
        
        mock_greeks = {}
        for leg in strategy.legs:
            mock_greeks[leg.instrument_token] = {
                'iv': leg.iv if hasattr(leg, 'iv') else 20.0,
                'delta': leg.delta if hasattr(leg, 'delta') else 0.0,
                'gamma': leg.gamma if hasattr(leg, 'gamma') else 0.0,
                'theta': leg.theta if hasattr(leg, 'theta') else -10.0,
                'vega': leg.vega if hasattr(leg, 'vega') else 10.0,
                'spot_price': 22000.0
            }
        
        return {
            "success": True,
            "order_ids": order_ids,
            "gtt_order_ids": gtt_ids,
            "entry_greeks": mock_greeks,
            "message": "Mock orders placed successfully"
        }

# ============================================================================
# ANALYTICS ENGINE
# ============================================================================

class AnalyticsEngine:
    def __init__(self):
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_time_metrics(self, weekly: date, monthly: date, next_weekly: date, 
                        all_expiries: List[date]) -> TimeMetrics:
        today = date.today()
        now_ist = datetime.now(self.ist_tz)
        
        dte_w = (weekly - today).days
        dte_m = (monthly - today).days
        dte_nw = (next_weekly - today).days
        
        is_past_square_off = now_ist.time() >= SystemConfig.PRE_EXPIRY_SQUARE_OFF_TIME
        
        return TimeMetrics(
            current_date=today,
            current_time_ist=now_ist,
            weekly_exp=weekly,
            monthly_exp=monthly,
            next_weekly_exp=next_weekly,
            dte_weekly=dte_w,
            dte_monthly=dte_m,
            dte_next_weekly=dte_nw,
            is_expiry_day_weekly=(dte_w == 0),
            is_expiry_day_monthly=(dte_m == 0),
            is_expiry_day_next_weekly=(dte_nw == 0),
            is_past_square_off_time=is_past_square_off
        )
    
    def get_vol_metrics(self, nifty_hist: pd.DataFrame, vix_hist: pd.DataFrame, 
                       spot_live: float, vix_live: float) -> VolMetrics:
        is_fallback = False
        spot = spot_live if spot_live > 0 else (nifty_hist.iloc[-1]['close'] if nifty_hist is not None and not nifty_hist.empty else 0)
        vix = vix_live if vix_live > 0 else (vix_hist.iloc[-1]['close'] if vix_hist is not None and not vix_hist.empty else 0)
        
        if spot_live <= 0 or vix_live <= 0:
            is_fallback = True
        
        if nifty_hist is None or nifty_hist.empty:
            return self._fallback_vol_metrics(spot, vix, is_fallback)
        
        returns = np.log(nifty_hist['close'] / nifty_hist['close'].shift(1)).dropna()
        
        rv7 = returns.rolling(7).std(ddof=1).iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 7 else 0
        rv28 = returns.rolling(28).std(ddof=1).iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 28 else 0
        rv90 = returns.rolling(90).std(ddof=1).iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 90 else 0
        
        def fit_garch(horizon: int) -> float:
            try:
                from arch import arch_model
                if len(returns) < 100:
                    return 0
                model = arch_model(returns * 100, vol='Garch', p=1, q=1, dist='normal')
                res = model.fit(disp='off', show_warning=False)
                forecast = res.forecast(horizon=horizon, reindex=False)
                forecasted_variances = forecast.variance.values[-1, :]
                avg_variance = np.mean(forecasted_variances)
                return np.sqrt(avg_variance * 252)
            except:
                return 0
        
        garch7 = fit_garch(7) or rv7
        garch28 = fit_garch(28) or rv28
        
        const = 1.0 / (4.0 * np.log(2.0))
        park7 = np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(7).mean() * const) * np.sqrt(252) * 100
        park28 = np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(28).mean() * const) * np.sqrt(252) * 100
        
        if vix_hist is not None and not vix_hist.empty:
            vix_returns = np.log(vix_hist['close'] / vix_hist['close'].shift(1)).dropna()
            vix_vol_30d = vix_returns.rolling(30).std(ddof=1) * np.sqrt(252) * 100
            vov = vix_vol_30d.shift(1).iloc[-1] if len(vix_vol_30d) > 1 else 0
            if len(vix_vol_30d) >= 60:
                vov_mean = vix_vol_30d.shift(1).rolling(60).mean().iloc[-1]
                vov_std = vix_vol_30d.shift(1).rolling(60).std(ddof=1).iloc[-1]
                vov_zscore = (vov - vov_mean) / vov_std if vov_std > 0 else 0
            else:
                vov_mean, vov_std, vov_zscore = 0, 0, 0
        else:
            vov, vov_mean, vov_std, vov_zscore = 0, 0, 0, 0
        
        def calc_ivp(window: int) -> float:
            if vix_hist is None or len(vix_hist) < window:
                return 0.0
            history = vix_hist['close'].tail(window)
            return (history < vix).mean() * 100
        
        ivp_30d = calc_ivp(30)
        ivp_90d = calc_ivp(90)
        ivp_1yr = calc_ivp(252)
        
        ma20 = nifty_hist['close'].rolling(20).mean().iloc[-1] if len(nifty_hist) >= 20 else spot
        
        high_low = nifty_hist['high'] - nifty_hist['low']
        high_close = (nifty_hist['high'] - nifty_hist['close'].shift(1)).abs()
        low_close = (nifty_hist['low'] - nifty_hist['close'].shift(1)).abs()
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr14 = true_range.rolling(14).mean().iloc[-1] if len(true_range) >= 14 else 0
        
        trend_strength = abs(spot - ma20) / atr14 if atr14 > 0 else 0
        
        vix_5d_ago = vix_hist['close'].iloc[-6] if vix_hist is not None and len(vix_hist) >= 6 else vix
        vix_change_5d = ((vix / vix_5d_ago) - 1) * 100 if vix_5d_ago > 0 else 0
        
        if vix_change_5d > DynamicConfig.VIX_MOMENTUM_BREAKOUT:
            vix_momentum = "RISING"
        elif vix_change_5d < -DynamicConfig.VIX_MOMENTUM_BREAKOUT:
            vix_momentum = "FALLING"
        else:
            vix_momentum = "STABLE"
        
        if vov_zscore > DynamicConfig.VOV_CRASH_ZSCORE:
            vol_regime = "EXPLODING"
        elif ivp_1yr > DynamicConfig.HIGH_VOL_IVP and vix_momentum == "FALLING":
            vol_regime = "MEAN_REVERTING"
        elif ivp_1yr > DynamicConfig.HIGH_VOL_IVP and vix_momentum == "RISING":
            vol_regime = "BREAKOUT_RICH"
        elif ivp_1yr > DynamicConfig.HIGH_VOL_IVP:
            vol_regime = "RICH"
        elif ivp_1yr < DynamicConfig.LOW_VOL_IVP:
            vol_regime = "CHEAP"
        else:
            vol_regime = "FAIR"
        
        return VolMetrics(
            spot=spot, vix=vix,
            rv7=rv7, rv28=rv28, rv90=rv90,
            garch7=garch7, garch28=garch28,
            park7=park7, park28=park28,
            vov=vov, vov_zscore=vov_zscore,
            ivp_30d=ivp_30d, ivp_90d=ivp_90d, ivp_1yr=ivp_1yr,
            ma20=ma20, atr14=atr14, trend_strength=trend_strength,
            vol_regime=vol_regime, is_fallback=is_fallback,
            vix_change_5d=vix_change_5d, vix_momentum=vix_momentum
        )
    
    def _fallback_vol_metrics(self, spot: float, vix: float, is_fallback: bool) -> VolMetrics:
        return VolMetrics(
            spot=spot, vix=vix,
            rv7=0, rv28=0, rv90=0,
            garch7=0, garch28=0,
            park7=0, park28=0,
            vov=0, vov_zscore=0,
            ivp_30d=0, ivp_90d=0, ivp_1yr=0,
            ma20=spot, atr14=0, trend_strength=0,
            vol_regime="UNKNOWN", is_fallback=is_fallback,
            vix_change_5d=0, vix_momentum="UNKNOWN"
        )
    
    def get_struct_metrics(self, chain: pd.DataFrame, spot: float, lot_size: int) -> StructMetrics:
        if chain is None or chain.empty:
            return self._fallback_struct_metrics(lot_size)
        
        chain['call_gex'] = chain['ce_gamma'] * chain['ce_oi'] * lot_size * spot * spot * 0.01
        chain['put_gex'] = -chain['pe_gamma'] * chain['pe_oi'] * lot_size * spot * spot * 0.01
        net_gex = (chain['call_gex'] + chain['put_gex']).sum()
        
        total_call_oi_value = (chain['ce_oi'] * chain['ce_ltp'] * lot_size).sum()
        total_put_oi_value = (chain['pe_oi'] * chain['pe_ltp'] * lot_size).sum()
        total_oi_value = total_call_oi_value + total_put_oi_value
        
        gex_ratio = net_gex / total_oi_value if total_oi_value > 0 else 0
        gex_weighted = net_gex / 1_000_000
        
        if abs(gex_ratio) < DynamicConfig.GEX_STICKY_RATIO:
            gex_regime = "STICKY"
        elif gex_ratio > 0:
            gex_regime = "CALL_HEAVY"
        else:
            gex_regime = "PUT_HEAVY"
        
        total_ce_oi = chain['ce_oi'].sum()
        total_pe_oi = chain['pe_oi'].sum()
        pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 1.0
        
        atm_strike = min(chain['strike'].values, key=lambda x: abs(x - spot))
        atm_row = chain[chain['strike'] == atm_strike]
        pcr_atm = atm_row.iloc[0]['pe_oi'] / atm_row.iloc[0]['ce_oi'] if not atm_row.empty and atm_row.iloc[0]['ce_oi'] > 0 else 1.0
        
        chain['call_pain'] = chain.apply(
            lambda row: ((row['strike'] - spot) * row['ce_oi'] * lot_size) if row['strike'] > spot else 0,
            axis=1
        )
        chain['put_pain'] = chain.apply(
            lambda row: ((spot - row['strike']) * row['pe_oi'] * lot_size) if row['strike'] < spot else 0,
            axis=1
        )
        
        pain_by_strike = chain.groupby('strike').apply(
            lambda g: g['call_pain'].sum() + g['put_pain'].sum()
        )
        max_pain = pain_by_strike.idxmin() if len(pain_by_strike) > 0 else spot
        
        call_25d = chain.iloc[(chain['ce_delta'] - 0.25).abs().argsort()[:1]]
        put_25d = chain.iloc[(chain['pe_delta'] + 0.25).abs().argsort()[:1]]
        
        if not call_25d.empty and not put_25d.empty:
            skew_25d = put_25d.iloc[0]['pe_iv'] - call_25d.iloc[0]['ce_iv']
        else:
            skew_25d = 0
        
        if skew_25d > DynamicConfig.SKEW_CRASH_FEAR:
            skew_regime = "CRASH_FEAR"
        elif skew_25d < DynamicConfig.SKEW_MELT_UP:
            skew_regime = "MELT_UP"
        else:
            skew_regime = "NORMAL"
        
        if pcr > 1.3:
            oi_regime = "BULLISH"
        elif pcr < 0.7:
            oi_regime = "BEARISH"
        else:
            oi_regime = "NEUTRAL"
        
        return StructMetrics(
            net_gex=net_gex,
            gex_ratio=gex_ratio,
            total_oi_value=total_oi_value,
            gex_regime=gex_regime,
            pcr=pcr,
            max_pain=max_pain,
            skew_25d=skew_25d,
            oi_regime=oi_regime,
            lot_size=lot_size,
            pcr_atm=pcr_atm,
            skew_regime=skew_regime,
            gex_weighted=gex_weighted
        )
    
    def _fallback_struct_metrics(self, lot_size: int) -> StructMetrics:
        return StructMetrics(
            net_gex=0, gex_ratio=0, total_oi_value=0,
            gex_regime="UNKNOWN", pcr=1.0, max_pain=0,
            skew_25d=0, oi_regime="UNKNOWN", lot_size=lot_size,
            pcr_atm=1.0, skew_regime="UNKNOWN", gex_weighted=0
        )
    
    def get_edge_metrics(self, weekly_chain: pd.DataFrame, monthly_chain: pd.DataFrame,
                        next_weekly_chain: pd.DataFrame, spot: float, 
                        vol_metrics: VolMetrics, is_expiry_day: bool) -> EdgeMetrics:
        
        def get_iv(chain):
            if chain is None or chain.empty:
                return 0
            atm_strike = min(chain['strike'].values, key=lambda x: abs(x - spot))
            atm_row = chain[chain['strike'] == atm_strike].iloc[0]
            return (atm_row['ce_iv'] + atm_row['pe_iv']) / 2
        
        iv_weekly = get_iv(weekly_chain)
        iv_monthly = get_iv(monthly_chain)
        iv_next_weekly = get_iv(next_weekly_chain)
        
        vrp_rv_weekly = iv_weekly - vol_metrics.rv7
        vrp_garch_weekly = iv_weekly - vol_metrics.garch7
        vrp_park_weekly = iv_weekly - vol_metrics.park7
        
        vrp_rv_monthly = iv_monthly - vol_metrics.rv28
        vrp_garch_monthly = iv_monthly - vol_metrics.garch28
        vrp_park_monthly = iv_monthly - vol_metrics.park28
        
        vrp_rv_next_weekly = iv_next_weekly - vol_metrics.rv7
        vrp_garch_next_weekly = iv_next_weekly - vol_metrics.garch7
        vrp_park_next_weekly = iv_next_weekly - vol_metrics.park7
        
        expiry_risk_discount_weekly = 0.2 if is_expiry_day else 0.0
        expiry_risk_discount_monthly = 0.0
        expiry_risk_discount_next_weekly = 0.0
        
        if iv_weekly > 0 and iv_monthly > 0:
            term_structure_slope = iv_monthly - iv_weekly
        else:
            term_structure_slope = 0
        
        if term_structure_slope > 2:
            term_structure_regime = "BACKWARDATION"
        elif term_structure_slope < -2:
            term_structure_regime = "CONTANGO"
        else:
            term_structure_regime = "FLAT"
        
        return EdgeMetrics(
            iv_weekly=iv_weekly,
            vrp_rv_weekly=vrp_rv_weekly,
            vrp_garch_weekly=vrp_garch_weekly,
            vrp_park_weekly=vrp_park_weekly,
            iv_monthly=iv_monthly,
            vrp_rv_monthly=vrp_rv_monthly,
            vrp_garch_monthly=vrp_garch_monthly,
            vrp_park_monthly=vrp_park_monthly,
            iv_next_weekly=iv_next_weekly,
            vrp_rv_next_weekly=vrp_rv_next_weekly,
            vrp_garch_next_weekly=vrp_garch_next_weekly,
            vrp_park_next_weekly=vrp_park_next_weekly,
            expiry_risk_discount_weekly=expiry_risk_discount_weekly,
            expiry_risk_discount_monthly=expiry_risk_discount_monthly,
            expiry_risk_discount_next_weekly=expiry_risk_discount_next_weekly,
            term_structure_slope=term_structure_slope,
            term_structure_regime=term_structure_regime
        )

# ============================================================================
# REGIME ENGINE
# ============================================================================

class RegimeEngine:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def calculate_scores(self, vol_metrics: VolMetrics, struct_metrics: StructMetrics,
                        edge_metrics: EdgeMetrics, external_metrics: ExternalMetrics,
                        expiry_type: str, dte: int) -> RegimeScore:
        
        vol_score = 0.0
        if vol_metrics.vol_regime == "RICH":
            vol_score = 3.0
        elif vol_metrics.vol_regime == "MEAN_REVERTING":
            vol_score = 4.0
        elif vol_metrics.vol_regime == "EXPLODING":
            vol_score = -5.0
        elif vol_metrics.vol_regime == "CHEAP":
            vol_score = -2.0
        
        if vol_metrics.vix_momentum == "FALLING":
            vol_score += 1.0
        
        vol_signal = "SELL_VOL" if vol_score > 2 else "BUY_VOL" if vol_score < -2 else "NEUTRAL"
        
        struct_score = 0.0
        if struct_metrics.gex_regime == "STICKY":
            struct_score = 2.0
        
        if 0.9 < struct_metrics.pcr < 1.2:
            struct_score += 1.0
        
        if struct_metrics.skew_regime == "CRASH_FEAR":
            struct_score -= 2.0
        
        struct_signal = "FAVORABLE" if struct_score > 1 else "UNFAVORABLE" if struct_score < -1 else "NEUTRAL"
        
        if expiry_type == "WEEKLY":
            vrp = edge_metrics.vrp_garch_weekly
        elif expiry_type == "MONTHLY":
            vrp = edge_metrics.vrp_garch_monthly
        else:
            vrp = edge_metrics.vrp_garch_next_weekly
        
        edge_score = 0.0
        if vrp > 3:
            edge_score = 3.0
        elif vrp > 1:
            edge_score = 2.0
        elif vrp < -2:
            edge_score = -2.0
        
        if edge_metrics.term_structure_regime == "BACKWARDATION":
            edge_score += 1.0
        
        edge_signal = "POSITIVE" if edge_score > 2 else "NEGATIVE" if edge_score < -1 else "NEUTRAL"
        
        external_score = 0.0
        if external_metrics.veto_event_near:
            external_score = -10.0
        elif external_metrics.high_impact_event_near:
            external_score = -2.0
        
        if external_metrics.fii_conviction in ["HIGH", "VERY_HIGH"]:
            external_score += 1.0 if external_metrics.fii_sentiment == "BULLISH" else -1.0
        
        external_signal = "CLEAR" if external_score > -1 else "RISKY"
        
        total_score = vol_score + struct_score + edge_score + external_score
        
        if total_score > 5:
            overall_signal = "STRONG_SELL"
        elif total_score > 2:
            overall_signal = "SELL"
        elif total_score < -5:
            overall_signal = "AVOID"
        else:
            overall_signal = "CAUTIOUS"
        
        confidence = "HIGH" if abs(total_score) > 5 else "MEDIUM" if abs(total_score) > 2 else "LOW"
        
        return RegimeScore(
            total_score=total_score,
            vol_score=vol_score,
            struct_score=struct_score,
            edge_score=edge_score,
            external_score=external_score,
            vol_signal=vol_signal,
            struct_signal=struct_signal,
            edge_signal=edge_signal,
            external_signal=external_signal,
            overall_signal=overall_signal,
            confidence=confidence
        )
    
    def generate_mandate(self, score: RegimeScore, vol_metrics: VolMetrics,
                        struct_metrics: StructMetrics, edge_metrics: EdgeMetrics,
                        external_metrics: ExternalMetrics, time_metrics: TimeMetrics,
                        expiry_type: str, expiry_date: date, dte: int) -> TradingMandate:
        
        veto_reasons = []
        risk_notes = []
        
        if time_metrics.is_past_square_off_time:
            veto_reasons.append("PAST_SQUARE_OFF_TIME")
        
        if external_metrics.veto_event_near:
            veto_reasons.append("VETO_EVENT_NEAR")
        
        if vol_metrics.vol_regime == "EXPLODING":
            veto_reasons.append("VOL_EXPLODING")
        
        if expiry_type == "WEEKLY" and time_metrics.is_expiry_day_weekly:
            veto_reasons.append("EXPIRY_DAY_WEEKLY - NO TRADING")
        elif expiry_type == "MONTHLY" and time_metrics.is_expiry_day_monthly:
            veto_reasons.append("EXPIRY_DAY_MONTHLY - NO TRADING")
        elif expiry_type == "NEXT_WEEKLY" and time_metrics.is_expiry_day_next_weekly:
            veto_reasons.append("EXPIRY_DAY_NEXT_WEEKLY - NO TRADING")
        
        if time_metrics.is_expiry_day_weekly or time_metrics.is_expiry_day_monthly or time_metrics.is_expiry_day_next_weekly:
            veto_reasons.append("TODAY IS EXPIRY DAY - NO NEW POSITIONS")
        
        is_trade_allowed = len(veto_reasons) == 0 and score.total_score > 0
        
        if vol_metrics.vol_regime in ["RICH", "MEAN_REVERTING"]:
            if struct_metrics.gex_regime == "STICKY":
                suggested_structure = "IRON_FLY"
            else:
                suggested_structure = "IRON_CONDOR"
        elif vol_metrics.vol_regime == "FAIR":
            if struct_metrics.oi_regime == "BULLISH":
                suggested_structure = "BULL_PUT_SPREAD"
            elif struct_metrics.oi_regime == "BEARISH":
                suggested_structure = "BEAR_CALL_SPREAD"
            else:
                suggested_structure = "SHORT_STRANGLE"
        else:
            suggested_structure = "CASH"
        
        if expiry_type == "WEEKLY":
            deployment_pct = DynamicConfig.WEEKLY_ALLOCATION_PCT
        elif expiry_type == "MONTHLY":
            deployment_pct = DynamicConfig.MONTHLY_ALLOCATION_PCT
        else:
            deployment_pct = DynamicConfig.NEXT_WEEKLY_ALLOCATION_PCT
        
        deployment_amount = DynamicConfig.BASE_CAPITAL * (deployment_pct / 100)
        
        if external_metrics.high_impact_event_near:
            risk_notes.append("HIGH_IMPACT_EVENT_AHEAD")
        
        if vol_metrics.vix_momentum == "RISING":
            risk_notes.append("VIX_RISING")
        
        if struct_metrics.skew_regime == "CRASH_FEAR":
            risk_notes.append("HIGH_CRASH_FEAR")
        
        regime_summary = f"{vol_metrics.vol_regime} vol, {struct_metrics.gex_regime} GEX, " \
                        f"{edge_metrics.term_structure_regime} term structure"
        
        return TradingMandate(
            expiry_type=expiry_type,
            expiry_date=expiry_date,
            is_trade_allowed=is_trade_allowed,
            suggested_structure=suggested_structure,
            deployment_amount=deployment_amount,
            risk_notes=risk_notes,
            veto_reasons=veto_reasons,
            regime_summary=regime_summary,
            confidence_level=score.confidence
        )

# ============================================================================
# STRATEGY FACTORY - FIXED: Added max loss cap
# ============================================================================

class StrategyFactory:
    def __init__(self, fetcher: UpstoxFetcher, spot: float, lot_size: int):
        self.fetcher = fetcher
        self.spot = spot
        self.lot_size = lot_size
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _validate_strategy(self, legs: List[OptionLeg]) -> List[str]:
        errors = []
        
        for leg in legs:
            if leg.oi < DynamicConfig.MIN_OI:
                errors.append(
                    f"{leg.option_type} {leg.strike} OI {leg.oi:,.0f} < "
                    f"{DynamicConfig.MIN_OI:,.0f} minimum"
                )
        
        for leg in legs:
            if leg.ask > 0 and leg.bid > 0:
                spread_pct = ((leg.ask - leg.bid) / leg.ltp) * 100 if leg.ltp > 0 else 999
                if spread_pct > DynamicConfig.MAX_BID_ASK_SPREAD_PCT:
                    errors.append(
                        f"{leg.option_type} {leg.strike} spread {spread_pct:.1f}% > "
                        f"{DynamicConfig.MAX_BID_ASK_SPREAD_PCT}%"
                    )
        
        return errors
    
    def construct_iron_fly(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            atm_strike = min(chain['strike'].values, key=lambda x: abs(x - self.spot))
            atm_row = chain[chain['strike'] == atm_strike].iloc[0]
            
            ce_premium = atm_row['ce_ltp']
            pe_premium = atm_row['pe_ltp']
            straddle_premium = ce_premium + pe_premium
            
            wing_distance = straddle_premium * 1.10
            call_wing_strike = atm_strike + wing_distance
            put_wing_strike = atm_strike - wing_distance
            
            call_wing_row = chain.iloc[(chain['strike'] - call_wing_strike).abs().argsort()[:1]]
            put_wing_row = chain.iloc[(chain['strike'] - put_wing_strike).abs().argsort()[:1]]
            
            quantity_lots = int(allocation / (straddle_premium * self.lot_size))
            if quantity_lots == 0:
                return None
            
            quantity = quantity_lots * self.lot_size
            
            call_pop = atm_row.get('ce_pop', 50.0)
            put_pop = atm_row.get('pe_pop', 50.0)
            strategy_pop = (call_pop + put_pop) / 2
            
            legs = [
                OptionLeg(
                    instrument_token=atm_row['ce_instrument_key'],
                    strike=atm_strike,
                    option_type="CE",
                    action="SELL",
                    quantity=quantity,
                    delta=-atm_row['ce_delta'],
                    gamma=-atm_row['ce_gamma'],
                    vega=-atm_row['ce_vega'],
                    theta=atm_row['ce_theta'],
                    iv=atm_row['ce_iv'],
                    ltp=ce_premium,
                    bid=atm_row['ce_bid'],
                    ask=atm_row['ce_ask'],
                    oi=atm_row['ce_oi'],
                    lot_size=self.lot_size,
                    entry_price=ce_premium,
                    product="D",
                    pop=call_pop
                ),
                OptionLeg(
                    instrument_token=atm_row['pe_instrument_key'],
                    strike=atm_strike,
                    option_type="PE",
                    action="SELL",
                    quantity=quantity,
                    delta=-atm_row['pe_delta'],
                    gamma=-atm_row['pe_gamma'],
                    vega=-atm_row['pe_vega'],
                    theta=atm_row['pe_theta'],
                    iv=atm_row['pe_iv'],
                    ltp=pe_premium,
                    bid=atm_row['pe_bid'],
                    ask=atm_row['pe_ask'],
                    oi=atm_row['pe_oi'],
                    lot_size=self.lot_size,
                    entry_price=pe_premium,
                    product="D",
                    pop=put_pop
                ),
                OptionLeg(
                    instrument_token=call_wing_row.iloc[0]['ce_instrument_key'],
                    strike=call_wing_row.iloc[0]['strike'],
                    option_type="CE",
                    action="BUY",
                    quantity=quantity,
                    delta=call_wing_row.iloc[0]['ce_delta'],
                    gamma=call_wing_row.iloc[0]['ce_gamma'],
                    vega=call_wing_row.iloc[0]['ce_vega'],
                    theta=-call_wing_row.iloc[0]['ce_theta'],
                    iv=call_wing_row.iloc[0]['ce_iv'],
                    ltp=call_wing_row.iloc[0]['ce_ltp'],
                    bid=call_wing_row.iloc[0]['ce_bid'],
                    ask=call_wing_row.iloc[0]['ce_ask'],
                    oi=call_wing_row.iloc[0]['ce_oi'],
                    lot_size=self.lot_size,
                    entry_price=call_wing_row.iloc[0]['ce_ltp'],
                    product="D",
                    pop=call_wing_row.iloc[0].get('ce_pop', 0)
                ),
                OptionLeg(
                    instrument_token=put_wing_row.iloc[0]['pe_instrument_key'],
                    strike=put_wing_row.iloc[0]['strike'],
                    option_type="PE",
                    action="BUY",
                    quantity=quantity,
                    delta=put_wing_row.iloc[0]['pe_delta'],
                    gamma=put_wing_row.iloc[0]['pe_gamma'],
                    vega=put_wing_row.iloc[0]['pe_vega'],
                    theta=-put_wing_row.iloc[0]['pe_theta'],
                    iv=put_wing_row.iloc[0]['pe_iv'],
                    ltp=put_wing_row.iloc[0]['pe_ltp'],
                    bid=put_wing_row.iloc[0]['pe_bid'],
                    ask=put_wing_row.iloc[0]['pe_ask'],
                    oi=put_wing_row.iloc[0]['pe_oi'],
                    lot_size=self.lot_size,
                    entry_price=put_wing_row.iloc[0]['pe_ltp'],
                    product="D",
                    pop=put_wing_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            net_premium = (ce_premium + pe_premium - 
                          call_wing_row.iloc[0]['ce_ltp'] - put_wing_row.iloc[0]['pe_ltp'])
            max_profit = net_premium * quantity
            
            wing_spread = call_wing_row.iloc[0]['strike'] - atm_strike
            max_loss = (wing_spread - net_premium) * quantity
            
            net_theta = sum(leg.theta * leg.quantity for leg in legs)
            net_vega = sum(leg.vega * leg.quantity for leg in legs)
            net_delta = sum(leg.delta * leg.quantity for leg in legs)
            net_gamma = sum(leg.gamma * leg.quantity for leg in legs)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"IRON_FLY_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.IRON_FLY,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=strategy_pop,
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                max_risk_amount=max_loss,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Iron Fly: {e}")
            return None
    
    def construct_iron_condor(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            call_20d_row = chain.iloc[(chain['ce_delta'] - 0.20).abs().argsort()[:1]]
            put_20d_row = chain.iloc[(chain['pe_delta'] + 0.20).abs().argsort()[:1]]
            call_5d_row = chain.iloc[(chain['ce_delta'] - 0.05).abs().argsort()[:1]]
            put_5d_row = chain.iloc[(chain['pe_delta'] + 0.05).abs().argsort()[:1]]
            
            net_premium = (call_20d_row.iloc[0]['ce_ltp'] + put_20d_row.iloc[0]['pe_ltp'] -
                          call_5d_row.iloc[0]['ce_ltp'] - put_5d_row.iloc[0]['pe_ltp'])
            
            quantity_lots = int(allocation / (net_premium * self.lot_size))
            if quantity_lots == 0:
                return None
            quantity = quantity_lots * self.lot_size
            
            call_pop = call_20d_row.iloc[0].get('ce_pop', 50.0)
            put_pop = put_20d_row.iloc[0].get('pe_pop', 50.0)
            strategy_pop = (call_pop + put_pop) / 2
            
            legs = [
                OptionLeg(
                    instrument_token=call_20d_row.iloc[0]['ce_instrument_key'],
                    strike=call_20d_row.iloc[0]['strike'],
                    option_type="CE",
                    action="SELL",
                    quantity=quantity,
                    delta=-call_20d_row.iloc[0]['ce_delta'],
                    gamma=-call_20d_row.iloc[0]['ce_gamma'],
                    vega=-call_20d_row.iloc[0]['ce_vega'],
                    theta=call_20d_row.iloc[0]['ce_theta'],
                    iv=call_20d_row.iloc[0]['ce_iv'],
                    ltp=call_20d_row.iloc[0]['ce_ltp'],
                    bid=call_20d_row.iloc[0]['ce_bid'],
                    ask=call_20d_row.iloc[0]['ce_ask'],
                    oi=call_20d_row.iloc[0]['ce_oi'],
                    lot_size=self.lot_size,
                    entry_price=call_20d_row.iloc[0]['ce_ltp'],
                    product="D",
                    pop=call_pop
                ),
                OptionLeg(
                    instrument_token=put_20d_row.iloc[0]['pe_instrument_key'],
                    strike=put_20d_row.iloc[0]['strike'],
                    option_type="PE",
                    action="SELL",
                    quantity=quantity,
                    delta=-put_20d_row.iloc[0]['pe_delta'],
                    gamma=-put_20d_row.iloc[0]['pe_gamma'],
                    vega=-put_20d_row.iloc[0]['pe_vega'],
                    theta=put_20d_row.iloc[0]['pe_theta'],
                    iv=put_20d_row.iloc[0]['pe_iv'],
                    ltp=put_20d_row.iloc[0]['pe_ltp'],
                    bid=put_20d_row.iloc[0]['pe_bid'],
                    ask=put_20d_row.iloc[0]['pe_ask'],
                    oi=put_20d_row.iloc[0]['pe_oi'],
                    lot_size=self.lot_size,
                    entry_price=put_20d_row.iloc[0]['pe_ltp'],
                    product="D",
                    pop=put_pop
                ),
                OptionLeg(
                    instrument_token=call_5d_row.iloc[0]['ce_instrument_key'],
                    strike=call_5d_row.iloc[0]['strike'],
                    option_type="CE",
                    action="BUY",
                    quantity=quantity,
                    delta=call_5d_row.iloc[0]['ce_delta'],
                    gamma=call_5d_row.iloc[0]['ce_gamma'],
                    vega=call_5d_row.iloc[0]['ce_vega'],
                    theta=-call_5d_row.iloc[0]['ce_theta'],
                    iv=call_5d_row.iloc[0]['ce_iv'],
                    ltp=call_5d_row.iloc[0]['ce_ltp'],
                    bid=call_5d_row.iloc[0]['ce_bid'],
                    ask=call_5d_row.iloc[0]['ce_ask'],
                    oi=call_5d_row.iloc[0]['ce_oi'],
                    lot_size=self.lot_size,
                    entry_price=call_5d_row.iloc[0]['ce_ltp'],
                    product="D",
                    pop=call_5d_row.iloc[0].get('ce_pop', 0)
                ),
                OptionLeg(
                    instrument_token=put_5d_row.iloc[0]['pe_instrument_key'],
                    strike=put_5d_row.iloc[0]['strike'],
                    option_type="PE",
                    action="BUY",
                    quantity=quantity,
                    delta=put_5d_row.iloc[0]['pe_delta'],
                    gamma=put_5d_row.iloc[0]['pe_gamma'],
                    vega=put_5d_row.iloc[0]['pe_vega'],
                    theta=-put_5d_row.iloc[0]['pe_theta'],
                    iv=put_5d_row.iloc[0]['pe_iv'],
                    ltp=put_5d_row.iloc[0]['pe_ltp'],
                    bid=put_5d_row.iloc[0]['pe_bid'],
                    ask=put_5d_row.iloc[0]['pe_ask'],
                    oi=put_5d_row.iloc[0]['pe_oi'],
                    lot_size=self.lot_size,
                    entry_price=put_5d_row.iloc[0]['pe_ltp'],
                    product="D",
                    pop=put_5d_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            max_profit = net_premium * quantity
            call_spread = call_5d_row.iloc[0]['strike'] - call_20d_row.iloc[0]['strike']
            max_loss = (call_spread - net_premium) * quantity
            
            net_theta = sum(leg.theta * leg.quantity for leg in legs)
            net_vega = sum(leg.vega * leg.quantity for leg in legs)
            net_delta = sum(leg.delta * leg.quantity for leg in legs)
            net_gamma = sum(leg.gamma * leg.quantity for leg in legs)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"IRON_CONDOR_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.IRON_CONDOR,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=strategy_pop,
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                max_risk_amount=max_loss,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Iron Condor: {e}")
            return None
    
    def construct_short_straddle(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            atm_strike = min(chain['strike'].values, key=lambda x: abs(x - self.spot))
            atm_row = chain[chain['strike'] == atm_strike].iloc[0]
            
            call_wing_row = chain.iloc[(chain['ce_delta'] - 0.02).abs().argsort()[:1]]
            put_wing_row = chain.iloc[(chain['pe_delta'] + 0.02).abs().argsort()[:1]]
            
            net_premium = (atm_row['ce_ltp'] + atm_row['pe_ltp'] -
                          call_wing_row.iloc[0]['ce_ltp'] - put_wing_row.iloc[0]['pe_ltp'])
            
            quantity_lots = int(allocation / (net_premium * self.lot_size))
            if quantity_lots == 0:
                return None
            quantity = quantity_lots * self.lot_size
            
            call_pop = atm_row.get('ce_pop', 50.0)
            put_pop = atm_row.get('pe_pop', 50.0)
            strategy_pop = (call_pop + put_pop) / 2
            
            legs = [
                OptionLeg(
                    instrument_token=atm_row['ce_instrument_key'],
                    strike=atm_strike,
                    option_type="CE",
                    action="SELL",
                    quantity=quantity,
                    delta=-atm_row['ce_delta'],
                    gamma=-atm_row['ce_gamma'],
                    vega=-atm_row['ce_vega'],
                    theta=atm_row['ce_theta'],
                    iv=atm_row['ce_iv'],
                    ltp=atm_row['ce_ltp'],
                    bid=atm_row['ce_bid'],
                    ask=atm_row['ce_ask'],
                    oi=atm_row['ce_oi'],
                    lot_size=self.lot_size,
                    entry_price=atm_row['ce_ltp'],
                    product="D",
                    pop=call_pop
                ),
                OptionLeg(
                    instrument_token=atm_row['pe_instrument_key'],
                    strike=atm_strike,
                    option_type="PE",
                    action="SELL",
                    quantity=quantity,
                    delta=-atm_row['pe_delta'],
                    gamma=-atm_row['pe_gamma'],
                    vega=-atm_row['pe_vega'],
                    theta=atm_row['pe_theta'],
                    iv=atm_row['pe_iv'],
                    ltp=atm_row['pe_ltp'],
                    bid=atm_row['pe_bid'],
                    ask=atm_row['pe_ask'],
                    oi=atm_row['pe_oi'],
                    lot_size=self.lot_size,
                    entry_price=atm_row['pe_ltp'],
                    product="D",
                    pop=put_pop
                ),
                OptionLeg(
                    instrument_token=call_wing_row.iloc[0]['ce_instrument_key'],
                    strike=call_wing_row.iloc[0]['strike'],
                    option_type="CE",
                    action="BUY",
                    quantity=quantity,
                    delta=call_wing_row.iloc[0]['ce_delta'],
                    gamma=call_wing_row.iloc[0]['ce_gamma'],
                    vega=call_wing_row.iloc[0]['ce_vega'],
                    theta=-call_wing_row.iloc[0]['ce_theta'],
                    iv=call_wing_row.iloc[0]['ce_iv'],
                    ltp=call_wing_row.iloc[0]['ce_ltp'],
                    bid=call_wing_row.iloc[0]['ce_bid'],
                    ask=call_wing_row.iloc[0]['ce_ask'],
                    oi=call_wing_row.iloc[0]['ce_oi'],
                    lot_size=self.lot_size,
                    entry_price=call_wing_row.iloc[0]['ce_ltp'],
                    product="D",
                    pop=call_wing_row.iloc[0].get('ce_pop', 0)
                ),
                OptionLeg(
                    instrument_token=put_wing_row.iloc[0]['pe_instrument_key'],
                    strike=put_wing_row.iloc[0]['strike'],
                    option_type="PE",
                    action="BUY",
                    quantity=quantity,
                    delta=put_wing_row.iloc[0]['pe_delta'],
                    gamma=put_wing_row.iloc[0]['pe_gamma'],
                    vega=put_wing_row.iloc[0]['pe_vega'],
                    theta=-put_wing_row.iloc[0]['pe_theta'],
                    iv=put_wing_row.iloc[0]['pe_iv'],
                    ltp=put_wing_row.iloc[0]['pe_ltp'],
                    bid=put_wing_row.iloc[0]['pe_bid'],
                    ask=put_wing_row.iloc[0]['pe_ask'],
                    oi=put_wing_row.iloc[0]['pe_oi'],
                    lot_size=self.lot_size,
                    entry_price=put_wing_row.iloc[0]['pe_ltp'],
                    product="D",
                    pop=put_wing_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            max_profit = net_premium * quantity
            wing_spread = call_wing_row.iloc[0]['strike'] - atm_strike
            max_loss = (wing_spread - net_premium) * quantity
            
            net_theta = sum(leg.theta * leg.quantity for leg in legs)
            net_vega = sum(leg.vega * leg.quantity for leg in legs)
            net_delta = sum(leg.delta * leg.quantity for leg in legs)
            net_gamma = sum(leg.gamma * leg.quantity for leg in legs)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"SHORT_STRADDLE_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.SHORT_STRADDLE,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=strategy_pop,
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                max_risk_amount=max_loss,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Short Straddle: {e}")
            return None
    
    def construct_short_strangle(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            call_30d_row = chain.iloc[(chain['ce_delta'] - 0.30).abs().argsort()[:1]]
            put_30d_row = chain.iloc[(chain['pe_delta'] + 0.30).abs().argsort()[:1]]
            call_5d_row = chain.iloc[(chain['ce_delta'] - 0.05).abs().argsort()[:1]]
            put_5d_row = chain.iloc[(chain['pe_delta'] + 0.05).abs().argsort()[:1]]
            
            net_premium = (call_30d_row.iloc[0]['ce_ltp'] + put_30d_row.iloc[0]['pe_ltp'] -
                          call_5d_row.iloc[0]['ce_ltp'] - put_5d_row.iloc[0]['pe_ltp'])
            
            quantity_lots = int(allocation / (net_premium * self.lot_size))
            if quantity_lots == 0:
                return None
            quantity = quantity_lots * self.lot_size
            
            call_pop = call_30d_row.iloc[0].get('ce_pop', 50.0)
            put_pop = put_30d_row.iloc[0].get('pe_pop', 50.0)
            strategy_pop = (call_pop + put_pop) / 2
            
            legs = [
                OptionLeg(
                    instrument_token=call_30d_row.iloc[0]['ce_instrument_key'],
                    strike=call_30d_row.iloc[0]['strike'],
                    option_type="CE",
                    action="SELL",
                    quantity=quantity,
                    delta=-call_30d_row.iloc[0]['ce_delta'],
                    gamma=-call_30d_row.iloc[0]['ce_gamma'],
                    vega=-call_30d_row.iloc[0]['ce_vega'],
                    theta=call_30d_row.iloc[0]['ce_theta'],
                    iv=call_30d_row.iloc[0]['ce_iv'],
                    ltp=call_30d_row.iloc[0]['ce_ltp'],
                    bid=call_30d_row.iloc[0]['ce_bid'],
                    ask=call_30d_row.iloc[0]['ce_ask'],
                    oi=call_30d_row.iloc[0]['ce_oi'],
                    lot_size=self.lot_size,
                    entry_price=call_30d_row.iloc[0]['ce_ltp'],
                    product="D",
                    pop=call_pop
                ),
                OptionLeg(
                    instrument_token=put_30d_row.iloc[0]['pe_instrument_key'],
                    strike=put_30d_row.iloc[0]['strike'],
                    option_type="PE",
                    action="SELL",
                    quantity=quantity,
                    delta=-put_30d_row.iloc[0]['pe_delta'],
                    gamma=-put_30d_row.iloc[0]['pe_gamma'],
                    vega=-put_30d_row.iloc[0]['pe_vega'],
                    theta=put_30d_row.iloc[0]['pe_theta'],
                    iv=put_30d_row.iloc[0]['pe_iv'],
                    ltp=put_30d_row.iloc[0]['pe_ltp'],
                    bid=put_30d_row.iloc[0]['pe_bid'],
                    ask=put_30d_row.iloc[0]['pe_ask'],
                    oi=put_30d_row.iloc[0]['pe_oi'],
                    lot_size=self.lot_size,
                    entry_price=put_30d_row.iloc[0]['pe_ltp'],
                    product="D",
                    pop=put_pop
                ),
                OptionLeg(
                    instrument_token=call_5d_row.iloc[0]['ce_instrument_key'],
                    strike=call_5d_row.iloc[0]['strike'],
                    option_type="CE",
                    action="BUY",
                    quantity=quantity,
                    delta=call_5d_row.iloc[0]['ce_delta'],
                    gamma=call_5d_row.iloc[0]['ce_gamma'],
                    vega=call_5d_row.iloc[0]['ce_vega'],
                    theta=-call_5d_row.iloc[0]['ce_theta'],
                    iv=call_5d_row.iloc[0]['ce_iv'],
                    ltp=call_5d_row.iloc[0]['ce_ltp'],
                    bid=call_5d_row.iloc[0]['ce_bid'],
                    ask=call_5d_row.iloc[0]['ce_ask'],
                    oi=call_5d_row.iloc[0]['ce_oi'],
                    lot_size=self.lot_size,
                    entry_price=call_5d_row.iloc[0]['ce_ltp'],
                    product="D",
                    pop=call_5d_row.iloc[0].get('ce_pop', 0)
                ),
                OptionLeg(
                    instrument_token=put_5d_row.iloc[0]['pe_instrument_key'],
                    strike=put_5d_row.iloc[0]['strike'],
                    option_type="PE",
                    action="BUY",
                    quantity=quantity,
                    delta=put_5d_row.iloc[0]['pe_delta'],
                    gamma=put_5d_row.iloc[0]['pe_gamma'],
                    vega=put_5d_row.iloc[0]['pe_vega'],
                    theta=-put_5d_row.iloc[0]['pe_theta'],
                    iv=put_5d_row.iloc[0]['pe_iv'],
                    ltp=put_5d_row.iloc[0]['pe_ltp'],
                    bid=put_5d_row.iloc[0]['pe_bid'],
                    ask=put_5d_row.iloc[0]['pe_ask'],
                    oi=put_5d_row.iloc[0]['pe_oi'],
                    lot_size=self.lot_size,
                    entry_price=put_5d_row.iloc[0]['pe_ltp'],
                    product="D",
                    pop=put_5d_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            max_profit = net_premium * quantity
            call_spread = call_5d_row.iloc[0]['strike'] - call_30d_row.iloc[0]['strike']
            put_spread = put_30d_row.iloc[0]['strike'] - put_5d_row.iloc[0]['strike']
            max_spread = max(call_spread, put_spread)
            max_loss = (max_spread - net_premium) * quantity
            
            net_theta = sum(leg.theta * leg.quantity for leg in legs)
            net_vega = sum(leg.vega * leg.quantity for leg in legs)
            net_delta = sum(leg.delta * leg.quantity for leg in legs)
            net_gamma = sum(leg.gamma * leg.quantity for leg in legs)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"SHORT_STRANGLE_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.SHORT_STRANGLE,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=strategy_pop,
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                max_risk_amount=max_loss,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Short Strangle: {e}")
            return None
    
    def construct_bull_put_spread(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            put_30d_row = chain.iloc[(chain['pe_delta'] + 0.30).abs().argsort()[:1]]
            put_10d_row = chain.iloc[(chain['pe_delta'] + 0.10).abs().argsort()[:1]]
            
            net_premium = put_30d_row.iloc[0]['pe_ltp'] - put_10d_row.iloc[0]['pe_ltp']
            
            quantity_lots = int(allocation / (net_premium * self.lot_size))
            if quantity_lots == 0:
                return None
            quantity = quantity_lots * self.lot_size
            
            put_pop = put_30d_row.iloc[0].get('pe_pop', 70.0)
            strategy_pop = put_pop
            
            legs = [
                OptionLeg(
                    instrument_token=put_30d_row.iloc[0]['pe_instrument_key'],
                    strike=put_30d_row.iloc[0]['strike'],
                    option_type="PE",
                    action="SELL",
                    quantity=quantity,
                    delta=-put_30d_row.iloc[0]['pe_delta'],
                    gamma=-put_30d_row.iloc[0]['pe_gamma'],
                    vega=-put_30d_row.iloc[0]['pe_vega'],
                    theta=put_30d_row.iloc[0]['pe_theta'],
                    iv=put_30d_row.iloc[0]['pe_iv'],
                    ltp=put_30d_row.iloc[0]['pe_ltp'],
                    bid=put_30d_row.iloc[0]['pe_bid'],
                    ask=put_30d_row.iloc[0]['pe_ask'],
                    oi=put_30d_row.iloc[0]['pe_oi'],
                    lot_size=self.lot_size,
                    entry_price=put_30d_row.iloc[0]['pe_ltp'],
                    product="D",
                    pop=strategy_pop
                ),
                OptionLeg(
                    instrument_token=put_10d_row.iloc[0]['pe_instrument_key'],
                    strike=put_10d_row.iloc[0]['strike'],
                    option_type="PE",
                    action="BUY",
                    quantity=quantity,
                    delta=put_10d_row.iloc[0]['pe_delta'],
                    gamma=put_10d_row.iloc[0]['pe_gamma'],
                    vega=put_10d_row.iloc[0]['pe_vega'],
                    theta=-put_10d_row.iloc[0]['pe_theta'],
                    iv=put_10d_row.iloc[0]['pe_iv'],
                    ltp=put_10d_row.iloc[0]['pe_ltp'],
                    bid=put_10d_row.iloc[0]['pe_bid'],
                    ask=put_10d_row.iloc[0]['pe_ask'],
                    oi=put_10d_row.iloc[0]['pe_oi'],
                    lot_size=self.lot_size,
                    entry_price=put_10d_row.iloc[0]['pe_ltp'],
                    product="D",
                    pop=put_10d_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            max_profit = net_premium * quantity
            put_spread = put_30d_row.iloc[0]['strike'] - put_10d_row.iloc[0]['strike']
            max_loss = (put_spread - net_premium) * quantity
            
            net_theta = sum(leg.theta * leg.quantity for leg in legs)
            net_vega = sum(leg.vega * leg.quantity for leg in legs)
            net_delta = sum(leg.delta * leg.quantity for leg in legs)
            net_gamma = sum(leg.gamma * leg.quantity for leg in legs)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"BULL_PUT_SPREAD_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.BULL_PUT_SPREAD,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=strategy_pop,
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                max_risk_amount=max_loss,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Bull Put Spread: {e}")
            return None
    
    def construct_bear_call_spread(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            call_30d_row = chain.iloc[(chain['ce_delta'] - 0.30).abs().argsort()[:1]]
            call_10d_row = chain.iloc[(chain['ce_delta'] - 0.10).abs().argsort()[:1]]
            
            net_premium = call_30d_row.iloc[0]['ce_ltp'] - call_10d_row.iloc[0]['ce_ltp']
            
            quantity_lots = int(allocation / (net_premium * self.lot_size))
            if quantity_lots == 0:
                return None
            quantity = quantity_lots * self.lot_size
            
            call_pop = call_30d_row.iloc[0].get('ce_pop', 70.0)
            strategy_pop = call_pop
            
            legs = [
                OptionLeg(
                    instrument_token=call_30d_row.iloc[0]['ce_instrument_key'],
                    strike=call_30d_row.iloc[0]['strike'],
                    option_type="CE",
                    action="SELL",
                    quantity=quantity,
                    delta=-call_30d_row.iloc[0]['ce_delta'],
                    gamma=-call_30d_row.iloc[0]['ce_gamma'],
                    vega=-call_30d_row.iloc[0]['ce_vega'],
                    theta=call_30d_row.iloc[0]['ce_theta'],
                    iv=call_30d_row.iloc[0]['ce_iv'],
                    ltp=call_30d_row.iloc[0]['ce_ltp'],
                    bid=call_30d_row.iloc[0]['ce_bid'],
                    ask=call_30d_row.iloc[0]['ce_ask'],
                    oi=call_30d_row.iloc[0]['ce_oi'],
                    lot_size=self.lot_size,
                    entry_price=call_30d_row.iloc[0]['ce_ltp'],
                    product="D",
                    pop=strategy_pop
                ),
                OptionLeg(
                    instrument_token=call_10d_row.iloc[0]['ce_instrument_key'],
                    strike=call_10d_row.iloc[0]['strike'],
                    option_type="CE",
                    action="BUY",
                    quantity=quantity,
                    delta=call_10d_row.iloc[0]['ce_delta'],
                    gamma=call_10d_row.iloc[0]['ce_gamma'],
                    vega=call_10d_row.iloc[0]['ce_vega'],
                    theta=-call_10d_row.iloc[0]['ce_theta'],
                    iv=call_10d_row.iloc[0]['ce_iv'],
                    ltp=call_10d_row.iloc[0]['ce_ltp'],
                    bid=call_10d_row.iloc[0]['ce_bid'],
                    ask=call_10d_row.iloc[0]['ce_ask'],
                    oi=call_10d_row.iloc[0]['ce_oi'],
                    lot_size=self.lot_size,
                    entry_price=call_10d_row.iloc[0]['ce_ltp'],
                    product="D",
                    pop=call_10d_row.iloc[0].get('ce_pop', 0)
                )
            ]
            
            max_profit = net_premium * quantity
            call_spread = call_10d_row.iloc[0]['strike'] - call_30d_row.iloc[0]['strike']
            max_loss = (call_spread - net_premium) * quantity
            
            net_theta = sum(leg.theta * leg.quantity for leg in legs)
            net_vega = sum(leg.vega * leg.quantity for leg in legs)
            net_delta = sum(leg.delta * leg.quantity for leg in legs)
            net_gamma = sum(leg.gamma * leg.quantity for leg in legs)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"BEAR_CALL_SPREAD_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.BEAR_CALL_SPREAD,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=strategy_pop,
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                max_risk_amount=max_loss,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Bear Call Spread: {e}")
            return None


# ============================================================================
# ANALYTICS CACHE & SCHEDULER
# ============================================================================

class AnalyticsCache:
    def __init__(self):
        self._cache: Optional[Dict] = None
        self._last_spot: float = 0.0
        self._last_vix: float = 0.0
        self._last_calc_time: Optional[datetime] = None
        self._lock = threading.RLock()
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get(self) -> Optional[Dict]:
        with self._lock:
            if self._cache is None:
                return None
            return copy.deepcopy(self._cache)
    
    def should_recalculate(self, current_spot: float, current_vix: float) -> bool:
        with self._lock:
            if self._cache is None:
                return True
            
            now = datetime.now(self.ist_tz)
            last_time = self._last_calc_time
            
            if last_time is None:
                return True
            
            current_time = now.time()
            is_market_hours = (SystemConfig.MARKET_OPEN_IST <= current_time <= SystemConfig.MARKET_CLOSE_IST)
            
            if is_market_hours:
                interval = DynamicConfig.ANALYTICS_INTERVAL_MINUTES
            else:
                interval = DynamicConfig.ANALYTICS_OFFHOURS_INTERVAL_MINUTES
            
            elapsed_minutes = (now - last_time).total_seconds() / 60
            
            if elapsed_minutes >= interval:
                self.logger.info(f"Time-based recalculation: {elapsed_minutes:.1f}min elapsed")
                return True
            
            if self._last_spot > 0:
                spot_change_pct = abs(current_spot - self._last_spot) / self._last_spot * 100
                if spot_change_pct > DynamicConfig.SPOT_CHANGE_TRIGGER_PCT:
                    self.logger.info(f"Spot-triggered recalculation: {spot_change_pct:.2f}% change")
                    return True
            
            if self._last_vix > 0:
                vix_change_pct = abs(current_vix - self._last_vix) / self._last_vix * 100
                if vix_change_pct > DynamicConfig.VIX_CHANGE_TRIGGER_PCT:
                    self.logger.info(f"VIX-triggered recalculation: {vix_change_pct:.2f}% change")
                    return True
            
            return False
    
    def update(self, analysis_data: Dict, spot: float, vix: float):
        with self._lock:
            self._cache = copy.deepcopy(analysis_data)
            self._last_spot = spot
            self._last_vix = vix
            self._last_calc_time = datetime.now(self.ist_tz)
            self.logger.info(f"Analytics cache updated | Spot: {spot:.2f} | VIX: {vix:.2f}")


class AnalyticsScheduler:
    def __init__(self, volguard_system, cache: AnalyticsCache):
        self.system = volguard_system
        self.cache = cache
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
        self._running = False
        self._executor: Optional[ThreadPoolExecutor] = None
    
    async def start(self):
        self._running = True
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="analytics")
        self.logger.info("Analytics scheduler started with ThreadPoolExecutor")
        loop = asyncio.get_event_loop()
        
        while self._running:
            try:
                current_spot = self.system.fetcher.get_ltp(SystemConfig.NIFTY_KEY) or 0
                current_vix = self.system.fetcher.get_ltp(SystemConfig.VIX_KEY) or 0
                
                if current_spot <= 0 or current_vix <= 0:
                    live_data = await loop.run_in_executor(
                        self._executor,
                        lambda: self.system.fetcher.live([SystemConfig.NIFTY_KEY, SystemConfig.VIX_KEY])
                    )
                    if live_data:
                        current_spot = live_data.get(SystemConfig.NIFTY_KEY, 0)
                        current_vix = live_data.get(SystemConfig.VIX_KEY, 0)
                
                if current_spot <= 0 or current_vix <= 0:
                    self.logger.error(f"Invalid prices: Spot={current_spot}, VIX={current_vix}")
                    await asyncio.sleep(10)
                    continue
                
                should_run = self.cache.should_recalculate(current_spot, current_vix)
                
                if should_run:
                    self.logger.info("Running analytics in background thread...")
                    try:
                        analysis = await loop.run_in_executor(
                            self._executor,
                            self.system.run_complete_analysis
                        )
                        self.cache.update(analysis, current_spot, current_vix)
                        self.logger.info("Analytics calculation completed")
                    except Exception as e:
                        self.logger.error(f"Analytics calculation failed: {e}")
                
                await asyncio.sleep(5)
                
            except Exception as e:
                self.logger.error(f"Scheduler loop error: {e}")
                await asyncio.sleep(10)
    
    def stop(self):
        self._running = False
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        self.logger.info("Analytics scheduler stopped")


# ============================================================================
# POSITION MONITOR - FIXED: Circuit breaker uses MTM, fixed None return
# ============================================================================

class PositionMonitor:
    def __init__(self, fetcher: UpstoxFetcher, db_session_factory, analytics_cache: AnalyticsCache, 
                 config, alert_service: Optional[TelegramAlertService] = None):
        self.fetcher = fetcher
        self.db_session_factory = db_session_factory
        self.analytics_cache = analytics_cache
        self.config = config
        self.alert_service = alert_service
        self.pnl_engine = PnLAttributionEngine(fetcher)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.is_running = False
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self._breach_count = 0
        self._breach_threshold = 3
        self._subscribed_instruments = set()
    
    async def start_monitoring(self):
        self.is_running = True
        self.logger.info("Position monitoring started (5s intervals - WebSocket)")
        
        while self.is_running:
            try:
                await self.check_all_positions()
                await asyncio.sleep(DynamicConfig.MONITOR_INTERVAL_SECONDS)
            except Exception as e:
                self.logger.error(f"Monitor error: {e}")
                await asyncio.sleep(10)
    
    async def check_all_positions(self):
        db = self.db_session_factory()
        try:
            active_trades = db.query(TradeJournal).filter(
                TradeJournal.status == TradeStatus.ACTIVE.value
            ).all()
            
            if not active_trades:
                return
            
            all_instruments = []
            for trade in active_trades:
                legs_data = json.loads(trade.legs_data)
                all_instruments.extend([leg['instrument_token'] for leg in legs_data])
            
            new_instruments = set(all_instruments) - self._subscribed_instruments
            if new_instruments:
                self.fetcher.subscribe_market_data(list(new_instruments), "ltpc")
                self._subscribed_instruments.update(new_instruments)
            
            current_prices = {}
            for instrument in set(all_instruments):
                price = self.fetcher.get_ltp(instrument)
                if price:
                    current_prices[instrument] = price
            
            if not current_prices:
                self.logger.error("🚨 MARKET DATA UNAVAILABLE FROM WEBSOCKET")
                return
            
            cached_analysis = self.analytics_cache.get()
            
            total_realized_pnl = 0.0
            total_unrealized_pnl = 0.0
            
            for trade in active_trades:
                result = await self.check_single_position(trade, current_prices, db)
                if result is not None:
                    total_unrealized_pnl += result
                else:
                    self.logger.warning(f"Skipping trade {trade.strategy_id} due to missing data")
            
            today = datetime.now().date()
            closed_trades_today = db.query(TradeJournal).filter(
                TradeJournal.status != TradeStatus.ACTIVE.value,
                TradeJournal.exit_time >= datetime.combine(today, dt_time.min)
            ).all()
            
            total_realized_pnl = sum(t.realized_pnl or 0 for t in closed_trades_today)
            
            self._update_daily_stats(db, total_realized_pnl, total_unrealized_pnl)
            
            # ===== FIXED: Circuit breaker on TOTAL MTM (realized + unrealized) =====
            total_mtm = total_realized_pnl + total_unrealized_pnl
            threshold = -DynamicConfig.BASE_CAPITAL * DynamicConfig.CIRCUIT_BREAKER_PCT / 100
            
            if total_mtm < threshold:
                self._breach_count += 1
                if self._breach_count >= self._breach_threshold:
                    self.logger.critical(f"🚨 CIRCUIT BREAKER TRIGGERED! Total MTM: ₹{total_mtm:.2f} < ₹{threshold:.2f}")
                    await self.trigger_circuit_breaker(db)
            else:
                self._breach_count = 0
        
        finally:
            db.close()
    
    async def check_single_position(self, trade: TradeJournal, current_prices: Dict, db: Session):
        legs_data = json.loads(trade.legs_data)
        
        for leg in legs_data:
            instrument_key = leg['instrument_token']
            if instrument_key not in current_prices:
                self.logger.error(f"⚠️ Missing price for {instrument_key} - aborting P&L for {trade.strategy_id}")
                return None
            if current_prices[instrument_key] is None or current_prices[instrument_key] <= 0:
                self.logger.error(f"⚠️ Invalid price {current_prices[instrument_key]} for {instrument_key}")
                return None
        
        unrealized_pnl = 0.0
        for leg in legs_data:
            entry_price = leg['entry_price']
            current_price = current_prices[leg['instrument_token']]
            quantity = leg['quantity']
            multiplier = -1 if leg['action'] == 'SELL' else 1
            
            leg_pnl = (current_price - entry_price) * quantity * multiplier
            unrealized_pnl += leg_pnl
        
        exit_reason = None
        
        if unrealized_pnl < -trade.max_loss * DynamicConfig.STOP_LOSS_MULTIPLIER:
            exit_reason = TradeStatus.CLOSED_STOP_LOSS.value
            self.logger.warning(f"Stop loss triggered for {trade.strategy_id}: ₹{unrealized_pnl:.2f}")
        
        elif unrealized_pnl > trade.max_profit * DynamicConfig.PROFIT_TARGET_MULTIPLIER:
            exit_reason = TradeStatus.CLOSED_PROFIT_TARGET.value
            self.logger.info(f"Profit target hit for {trade.strategy_id}: ₹{unrealized_pnl:.2f}")
        
        else:
            should_exit, reason = SystemConfig.should_square_off_position(trade)
            if should_exit:
                if "PRE_EXPIRY" in reason:
                    exit_reason = TradeStatus.CLOSED_EXPIRY_EXIT.value
                else:
                    exit_reason = TradeStatus.CLOSED_VETO_EVENT.value
                self.logger.info(f"📅 Scheduled square off: {reason}")
        
        if exit_reason:
            await self.exit_position(trade, exit_reason, current_prices, db)
        
        return unrealized_pnl
    
    async def exit_position(self, trade: TradeJournal, exit_reason: str, 
                           current_prices: Dict, db: Session):
        executor = UpstoxOrderExecutor(self.fetcher)
        result = executor.exit_position(trade, exit_reason, current_prices, db)
        
        if result["success"] and self.alert_service:
            try:
                msg = f"""
<b>Strategy Closed:</b> {trade.strategy_id}
<b>Type:</b> {trade.strategy_type}
<b>Total P&L:</b> ₹{result['realized_pnl']:.2f}
<b>Exit Reason:</b> {exit_reason}
"""
                priority = AlertPriority.SUCCESS if result['realized_pnl'] > 0 else AlertPriority.HIGH
                throttle_key = f"exit_{trade.strategy_id}"
                self.alert_service.send("Trade Closed", msg, priority, throttle_key)
            except Exception as e:
                self.logger.error(f"Alert sending failed: {e}")
    
    async def trigger_circuit_breaker(self, db: Session):
        active_trades = db.query(TradeJournal).filter(
            TradeJournal.status == TradeStatus.ACTIVE.value
        ).all()
        
        executor = UpstoxOrderExecutor(self.fetcher)
        
        for trade in active_trades:
            legs_data = json.loads(trade.legs_data)
            instrument_tokens = [leg['instrument_token'] for leg in legs_data]
            
            current_prices = {}
            for instrument in instrument_tokens:
                price = self.fetcher.get_ltp(instrument)
                if price:
                    current_prices[instrument] = price
            
            if current_prices:
                executor.exit_position(
                    trade,
                    TradeStatus.CLOSED_CIRCUIT_BREAKER.value,
                    current_prices,
                    db
                )
        
        self.logger.critical("🚨 ALL POSITIONS CLOSED - CIRCUIT BREAKER ACTIVE")
    
    def _update_daily_stats(self, db: Session, realized_pnl: float, unrealized_pnl: float):
        today = datetime.now().date()
        stats = db.query(DailyStats).filter(DailyStats.date == today).first()
        
        if not stats:
            stats = DailyStats(date=today)
            db.add(stats)
        
        stats.realized_pnl = realized_pnl
        stats.unrealized_pnl = unrealized_pnl
        stats.total_pnl = realized_pnl + unrealized_pnl
        
        db.commit()
    
    def stop(self):
        self.is_running = False
        if self._subscribed_instruments:
            self.fetcher.unsubscribe_market_data(list(self._subscribed_instruments))
        self.logger.info("Position monitoring stopped")


# ============================================================================
# COMPLETE SYSTEM ORCHESTRATOR
# ============================================================================

class VolGuardSystem:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.fetcher = UpstoxFetcher(SystemConfig.UPSTOX_ACCESS_TOKEN)
        self.analytics = AnalyticsEngine()
        self.regime = RegimeEngine()
        
        self.json_cache = JSONCacheManager()
        self.analytics_cache = AnalyticsCache()
        self.correlation_manager = CorrelationManager(SessionLocal)
        
        if DynamicConfig.AUTO_TRADING and SystemConfig.UPSTOX_ACCESS_TOKEN:
            self.logger.info("🔴 REAL TRADING MODE ENABLED - Using UpstoxOrderExecutor")
            self.executor = UpstoxOrderExecutor(self.fetcher)
        else:
            self.logger.info("🟡 MOCK TRADING MODE ENABLED")
            self.executor = MockExecutor()
        
        self.analytics_scheduler: Optional[AnalyticsScheduler] = None
        self.monitor: Optional[PositionMonitor] = None
        self.alert_service: Optional[TelegramAlertService] = None
        
        self.market_streamer_started = False
        self.portfolio_streamer_started = False
        self._cached_expiries: List[date] = []
        
        self.logger.info("VolGuard System v3.3 initialized - FULL SDK + WEBSOCKET + CORRELATION")
    
    def start_market_streamer(self, instrument_keys: List[str] = None, mode: str = "ltpc"):
        if instrument_keys is None:
            instrument_keys = [SystemConfig.NIFTY_KEY, SystemConfig.VIX_KEY]
        
        self.fetcher.start_market_streamer(instrument_keys, mode)
        self.market_streamer_started = True
        self.logger.info(f"Market streamer started with {len(instrument_keys)} instruments")
    
    def start_portfolio_streamer(self):
        self.fetcher.start_portfolio_streamer(
            order_update=True,
            position_update=True,
            holding_update=True,
            gtt_update=True
        )
        self.portfolio_streamer_started = True
        self.logger.info("Portfolio streamer started")
    
    def run_complete_analysis(self) -> Dict:
        try:
            nifty_hist = self.fetcher.history(SystemConfig.NIFTY_KEY)
            vix_hist = self.fetcher.history(SystemConfig.VIX_KEY)
            
            spot = self.fetcher.get_ltp(SystemConfig.NIFTY_KEY) or 0
            vix = self.fetcher.get_ltp(SystemConfig.VIX_KEY) or 0
            
            if spot == 0 or vix == 0:
                live_data = self.fetcher.live([SystemConfig.NIFTY_KEY, SystemConfig.VIX_KEY])
                if live_data:
                    spot = live_data.get(SystemConfig.NIFTY_KEY, 0)
                    vix = live_data.get(SystemConfig.VIX_KEY, 0)
            
            if nifty_hist is None:
                raise ValueError("Failed to fetch Nifty historical data from Upstox")
            if vix_hist is None:
                raise ValueError("Failed to fetch VIX historical data from Upstox")
            if spot <= 0 or vix <= 0:
                raise ValueError(f"Invalid prices - Spot: {spot}, VIX: {vix}")
            
            weekly, monthly, next_weekly, lot_size, all_expiries = self.fetcher.get_expiries()
            self._cached_expiries = all_expiries
            
            if not weekly:
                raise ValueError("Cannot fetch expiries from Upstox SDK")
            
            weekly_chain = self.fetcher.chain(weekly)
            monthly_chain = self.fetcher.chain(monthly)
            next_weekly_chain = self.fetcher.chain(next_weekly)
            
            time_metrics = self.analytics.get_time_metrics(weekly, monthly, next_weekly, all_expiries)
            
            vol_metrics = self.analytics.get_vol_metrics(
                nifty_hist, vix_hist, spot, vix
            )
            
            struct_weekly = self.analytics.get_struct_metrics(weekly_chain, vol_metrics.spot, lot_size)
            struct_monthly = self.analytics.get_struct_metrics(monthly_chain, vol_metrics.spot, lot_size)
            struct_next_weekly = self.analytics.get_struct_metrics(next_weekly_chain, vol_metrics.spot, lot_size)
            
            edge_metrics = self.analytics.get_edge_metrics(
                weekly_chain, monthly_chain, next_weekly_chain,
                vol_metrics.spot, vol_metrics, time_metrics.is_expiry_day_weekly
            )
            
            external_metrics = self.json_cache.get_external_metrics()
            
            weekly_score = self.regime.calculate_scores(
                vol_metrics, struct_weekly, edge_metrics, external_metrics,
                "WEEKLY", time_metrics.dte_weekly
            )
            
            monthly_score = self.regime.calculate_scores(
                vol_metrics, struct_monthly, edge_metrics, external_metrics,
                "MONTHLY", time_metrics.dte_monthly
            )
            
            next_weekly_score = self.regime.calculate_scores(
                vol_metrics, struct_next_weekly, edge_metrics, external_metrics,
                "NEXT_WEEKLY", time_metrics.dte_next_weekly
            )
            
            weekly_mandate = self.regime.generate_mandate(
                weekly_score, vol_metrics, struct_weekly, edge_metrics,
                external_metrics, time_metrics, "WEEKLY", weekly, time_metrics.dte_weekly
            )
            
            monthly_mandate = self.regime.generate_mandate(
                monthly_score, vol_metrics, struct_monthly, edge_metrics,
                external_metrics, time_metrics, "MONTHLY", monthly, time_metrics.dte_monthly
            )
            
            next_weekly_mandate = self.regime.generate_mandate(
                next_weekly_score, vol_metrics, struct_next_weekly, edge_metrics,
                external_metrics, time_metrics, "NEXT_WEEKLY", next_weekly, time_metrics.dte_next_weekly
            )
            
            return {
                "time_metrics": time_metrics,
                "vol_metrics": vol_metrics,
                "struct_weekly": struct_weekly,
                "struct_monthly": struct_monthly,
                "struct_next_weekly": struct_next_weekly,
                "edge_metrics": edge_metrics,
                "external_metrics": external_metrics,
                "weekly_score": weekly_score,
                "monthly_score": monthly_score,
                "next_weekly_score": next_weekly_score,
                "weekly_mandate": weekly_mandate,
                "monthly_mandate": monthly_mandate,
                "next_weekly_mandate": next_weekly_mandate,
                "lot_size": lot_size,
                "weekly_chain": weekly_chain,
                "monthly_chain": monthly_chain,
                "next_weekly_chain": next_weekly_chain,
                "all_expiries": all_expiries
            }
        
        except Exception as e:
            self.logger.error(f"Analysis error: {e}")
            # Return cached data if available
            cached = self.analytics_cache.get()
            if cached:
                self.logger.info("Returning cached analysis due to error")
                return cached
            raise
    
    def construct_strategy_from_mandate(self, mandate: TradingMandate, 
                                       analysis_data: Dict) -> Optional[ConstructedStrategy]:
        if not mandate.is_trade_allowed:
            self.logger.info(f"Trade not allowed for {mandate.expiry_type}: {mandate.veto_reasons}")
            return None
        
        strategy_type_str = mandate.suggested_structure
        
        strategy_type_map = {
            "IRON_FLY": StrategyType.IRON_FLY,
            "IRON_CONDOR": StrategyType.IRON_CONDOR,
            "SHORT_STRADDLE": StrategyType.SHORT_STRADDLE,
            "SHORT_STRANGLE": StrategyType.SHORT_STRANGLE,
            "BULL_PUT_SPREAD": StrategyType.BULL_PUT_SPREAD,
            "BEAR_CALL_SPREAD": StrategyType.BEAR_CALL_SPREAD
        }
        
        strategy_type = strategy_type_map.get(strategy_type_str)
        if not strategy_type:
            self.logger.error(f"Unknown strategy type: {strategy_type_str}")
            return None
        
        factory = StrategyFactory(
            self.fetcher,
            analysis_data['vol_metrics'].spot,
            analysis_data['lot_size']
        )
        
        if strategy_type == StrategyType.IRON_FLY:
            strategy = factory.construct_iron_fly(mandate.expiry_date, mandate.deployment_amount)
        elif strategy_type == StrategyType.IRON_CONDOR:
            strategy = factory.construct_iron_condor(mandate.expiry_date, mandate.deployment_amount)
        elif strategy_type == StrategyType.SHORT_STRADDLE:
            strategy = factory.construct_short_straddle(mandate.expiry_date, mandate.deployment_amount)
        elif strategy_type == StrategyType.SHORT_STRANGLE:
            strategy = factory.construct_short_strangle(mandate.expiry_date, mandate.deployment_amount)
        elif strategy_type == StrategyType.BULL_PUT_SPREAD:
            strategy = factory.construct_bull_put_spread(mandate.expiry_date, mandate.deployment_amount)
        elif strategy_type == StrategyType.BEAR_CALL_SPREAD:
            strategy = factory.construct_bear_call_spread(mandate.expiry_date, mandate.deployment_amount)
        else:
            return None
        
        if not strategy:
            return None
        
        expiry_type_map = {
            "WEEKLY": ExpiryType.WEEKLY,
            "MONTHLY": ExpiryType.MONTHLY,
            "NEXT_WEEKLY": ExpiryType.NEXT_WEEKLY
        }
        strategy.expiry_type = expiry_type_map.get(mandate.expiry_type, ExpiryType.WEEKLY)
        
        allowed, violations = self.correlation_manager.can_take_position(strategy)
        if not allowed:
            self.logger.warning(f"🚫 Correlation violation - cannot take position: {violations[0].rule}")
            return None
        
        return strategy
    
    def execute_strategy(self, strategy: ConstructedStrategy, db: Session) -> Dict:
        if not strategy.validation_passed:
            return {
                "success": False,
                "message": "Strategy validation failed",
                "errors": strategy.validation_errors
            }
        
        result = self.executor.place_multi_order(strategy)
        
        if result['success']:
            entry_premium = sum(
                leg.entry_price * leg.quantity * (-1 if leg.action == 'SELL' else 1)
                for leg in strategy.legs
            )
            
            trade = TradeJournal(
                strategy_id=strategy.strategy_id,
                strategy_type=strategy.strategy_type.value,
                expiry_type=strategy.expiry_type.value,
                expiry_date=strategy.expiry_date,
                entry_time=datetime.now(),
                legs_data=json.dumps([asdict(leg) for leg in strategy.legs]),
                order_ids=json.dumps(result['order_ids']),
                gtt_order_ids=json.dumps(result.get('gtt_order_ids', [])),
                entry_greeks_snapshot=json.dumps(result.get('entry_greeks', {})),
                max_profit=strategy.max_profit,
                max_loss=strategy.max_loss,
                allocated_capital=strategy.allocated_capital,
                entry_premium=entry_premium,
                status=TradeStatus.ACTIVE.value,
                is_mock=not DynamicConfig.AUTO_TRADING
            )
            db.add(trade)
            db.commit()
            
            if self.alert_service:
                try:
                    msg = f"""
<b>New Position Opened</b>
<b>Strategy:</b> {strategy.strategy_type.value}
<b>Expiry:</b> {strategy.expiry_type.value}
<b>Max Profit:</b> ₹{strategy.max_profit:.2f}
<b>Max Loss:</b> ₹{strategy.max_loss:.2f}
<b>Legs:</b> {len(strategy.legs)}
<b>POP:</b> {strategy.pop:.1f}%
"""
                    self.alert_service.send("Position Opened", msg, AlertPriority.MEDIUM)
                except Exception as e:
                    self.logger.error(f"Alert sending failed: {e}")
            
            self.logger.info(f"Strategy executed: {strategy.strategy_id}")
        
        return result


# ============================================================================
# BACKGROUND RECONCILIATION JOBS - FIXED: Now use run_in_executor
# ============================================================================

async def position_reconciliation_job():
    logger.info("Position reconciliation job started")
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="recon")
    
    while True:
        try:
            if volguard_system and volguard_system.fetcher.is_market_open_now():
                def sync_reconcile():
                    db = next(get_db())
                    try:
                        return volguard_system.fetcher.reconcile_positions_with_db(db)
                    finally:
                        db.close()
                
                report = await loop.run_in_executor(executor, sync_reconcile)
                
                if not report["reconciled"] and volguard_system and volguard_system.alert_service:
                    volguard_system.alert_service.send(
                        "Position Mismatch Detected",
                        f"DB: {report['db_positions']}, Broker: {report['broker_positions']}\n"
                        f"In DB not Broker: {report['in_db_not_broker'][:3]}...",
                        AlertPriority.HIGH,
                        throttle_key="position_reconciliation"
                    )
                
                await asyncio.sleep(DynamicConfig.POSITION_RECONCILE_INTERVAL_MINUTES * 60)
            else:
                await asyncio.sleep(3600)
        except Exception as e:
            logger.error(f"Position reconciliation error: {e}")
            await asyncio.sleep(600)

async def daily_pnl_reconciliation():
    ist_tz = pytz.timezone('Asia/Kolkata')
    logger.info("Daily P&L reconciliation job started")
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pnl")
    
    while True:
        try:
            now = datetime.now(ist_tz)
            if now.time() >= SystemConfig.PNL_RECONCILE_TIME_IST:
                today = now.date()
                
                def sync_pnl_reconcile():
                    db = next(get_db())
                    try:
                        stats = db.query(DailyStats).filter(DailyStats.date == today).first()
                        if stats and volguard_system:
                            our_pnl = stats.total_pnl or 0.0
                            broker_pnl = volguard_system.fetcher.get_broker_pnl_for_date(today)
                            return stats, our_pnl, broker_pnl
                        return None, None, None
                    finally:
                        db.close()
                
                result = await loop.run_in_executor(executor, sync_pnl_reconcile)
                
                if result and result[0] and result[2] is not None:
                    stats, our_pnl, broker_pnl = result
                    discrepancy = abs(our_pnl - broker_pnl)
                    
                    def sync_update():
                        db = next(get_db())
                        try:
                            stats = db.query(DailyStats).filter(DailyStats.date == today).first()
                            if stats:
                                stats.broker_pnl = broker_pnl
                                stats.pnl_discrepancy = discrepancy
                                db.commit()
                        finally:
                            db.close()
                    
                    await loop.run_in_executor(executor, sync_update)
                    
                    if discrepancy > DynamicConfig.PNL_DISCREPANCY_THRESHOLD and volguard_system.alert_service:
                        volguard_system.alert_service.send(
                            "P&L Mismatch Detected",
                            f"Our P&L: ₹{our_pnl:,.2f}\n"
                            f"Broker P&L: ₹{broker_pnl:,.2f}\n"
                            f"Difference: ₹{discrepancy:,.2f}",
                            AlertPriority.HIGH,
                            throttle_key="pnl_reconciliation"
                        )
                
                tomorrow = now.date() + timedelta(days=1)
                next_run = datetime.combine(tomorrow, SystemConfig.PNL_RECONCILE_TIME_IST)
                next_run = ist_tz.localize(next_run)
                sleep_seconds = (next_run - now).total_seconds()
                await asyncio.sleep(sleep_seconds)
            else:
                await asyncio.sleep(3600)
        except Exception as e:
            logger.error(f"Daily P&L reconciliation error: {e}")
            await asyncio.sleep(3600)


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 70)
    logger.info("VolGuard v3.3 Starting... (FINAL PRODUCTION VERSION)")
    logger.info("=" * 70)
    
    DynamicConfig.initialize(SessionLocal)
    logger.info(f"Base Capital: ₹{DynamicConfig.BASE_CAPITAL:,.2f}")
    logger.info(f"Auto Trading: {'ENABLED 🔴' if DynamicConfig.AUTO_TRADING else 'DISABLED 🟡'}")
    logger.info(f"Mock Trading: {'ENABLED 🟡' if DynamicConfig.ENABLE_MOCK_TRADING else 'DISABLED'}")
    
    alert_service = None
    if SystemConfig.TELEGRAM_TOKEN and SystemConfig.TELEGRAM_CHAT_ID:
        alert_service = TelegramAlertService(
            SystemConfig.TELEGRAM_TOKEN,
            SystemConfig.TELEGRAM_CHAT_ID
        )
        await alert_service.start()
        alert_service.send(
            "🚀 VolGuard v3.3 Started",
            "System Online - Final Production Version\nAll Critical Bugs Fixed",
            AlertPriority.SUCCESS
        )
        logger.info("✅ Telegram Alerts Enabled")
    else:
        logger.warning("⚠️ Telegram credentials not configured - alerts disabled")
    
    global volguard_system
    volguard_system = VolGuardSystem()
    volguard_system.alert_service = alert_service
    
    if not volguard_system.json_cache.is_valid_for_today():
        logger.info("Fetching initial daily cache...")
        await asyncio.get_event_loop().run_in_executor(
            None, volguard_system.json_cache.fetch_and_cache, True
        )
    
    volguard_system.start_market_streamer()
    volguard_system.start_portfolio_streamer()
    
    volguard_system.analytics_scheduler = AnalyticsScheduler(volguard_system, volguard_system.analytics_cache)
    analytics_task = asyncio.create_task(volguard_system.analytics_scheduler.start())
    
    cache_task = asyncio.create_task(volguard_system.json_cache.schedule_daily_fetch())
    
    if DynamicConfig.ENABLE_MOCK_TRADING or DynamicConfig.AUTO_TRADING:
        volguard_system.monitor = PositionMonitor(
            volguard_system.fetcher, 
            SessionLocal,
            volguard_system.analytics_cache,
            SystemConfig,
            alert_service
        )
        monitor_task = asyncio.create_task(volguard_system.monitor.start_monitoring())
    
    position_recon_task = asyncio.create_task(position_reconciliation_job())
    pnl_recon_task = asyncio.create_task(daily_pnl_reconciliation())
    
    yield
    
    logger.info("VolGuard v3.3 shutting down...")
    if alert_service:
        await alert_service.stop()
    if volguard_system.analytics_scheduler:
        volguard_system.analytics_scheduler.stop()
    if volguard_system.monitor:
        volguard_system.monitor.stop()
    if volguard_system.fetcher.market_streamer:
        volguard_system.fetcher.market_streamer.disconnect()
    if volguard_system.fetcher.portfolio_streamer:
        volguard_system.fetcher.portfolio_streamer.disconnect()
    position_recon_task.cancel()
    pnl_recon_task.cancel()


app = FastAPI(
    title="VolGuard v3.3 - Production",
    description="Professional Options Trading System - Final Production Version",
    version="3.3.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

volguard_system: Optional[VolGuardSystem] = None


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/api/dashboard/analytics", response_model=DashboardAnalyticsResponse)
def get_dashboard_analytics(
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    try:
        cached = volguard_system.analytics_cache.get()
        if cached:
            analysis = cached
        else:
            analysis = volguard_system.run_complete_analysis()
        
        market_status = {
            "nifty_spot": round(analysis['vol_metrics'].spot, 2),
            "india_vix": round(analysis['vol_metrics'].vix, 2),
            "fii_sentiment": analysis['external_metrics'].fii_sentiment
        }
        
        primary_mandate = analysis['weekly_mandate']
        if not primary_mandate.is_trade_allowed and analysis['monthly_mandate'].is_trade_allowed:
            primary_mandate = analysis['monthly_mandate']
        
        mandate = {
            "status": "ALLOWED" if primary_mandate.is_trade_allowed else "VETOED",
            "strategy": primary_mandate.suggested_structure if primary_mandate.is_trade_allowed else "CASH",
            "score": round(analysis['weekly_score'].total_score, 1),
            "reason": ", ".join(primary_mandate.veto_reasons) if primary_mandate.veto_reasons else primary_mandate.regime_summary
        }
        
        scores = {
            "volatility": round(analysis['weekly_score'].vol_score, 1),
            "structure": round(analysis['weekly_score'].struct_score, 1),
            "edge": round(analysis['weekly_score'].edge_score, 1)
        }
        
        events = []
        for event in analysis['external_metrics'].economic_events[:5]:
            events.append({
                "name": event.title,
                "type": "VETO" if event.is_veto_event else event.impact_level,
                "time": f"{event.days_until} days" if event.days_until > 0 else "Today"
            })
        
        return {
            "market_status": market_status,
            "mandate": mandate,
            "scores": scores,
            "events": events
        }
        
    except Exception as e:
        logger.error(f"Dashboard analytics error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dashboard/professional")
def get_professional_dashboard(
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    try:
        analytics = volguard_system.analytics_cache.get()
        if not analytics:
            try:
                analytics = volguard_system.run_complete_analysis()
            except:
                raise HTTPException(status_code=503, detail="Analytics initializing...")
        
        daily_ctx = volguard_system.json_cache.get_context() or {}
        
        vol = analytics.get('vol_metrics')
        ext = analytics.get('external_metrics')
        
        def fmt_cr(val):
            return f"₹{val/10000000:+.2f} Cr" if val else "N/A"
            
        def fmt_pct(val):
            return f"{val:.2f}%" if val is not None else "0%"

        return {
            "timestamp": datetime.now(pytz.timezone('Asia/Kolkata')).isoformat(),
            "time_context": {
                "status": "BEFORE 2 PM" if datetime.now(pytz.timezone('Asia/Kolkata')).hour < 14 else "AFTER 2 PM",
                "weekly_expiry": {
                    "date": str(analytics['time_metrics'].weekly_exp),
                    "dte": analytics['time_metrics'].dte_weekly,
                    "trading_blocked": analytics['time_metrics'].is_expiry_day_weekly,
                    "square_off_today": analytics['time_metrics'].dte_weekly == 1
                },
                "monthly_expiry": {
                    "date": str(analytics['time_metrics'].monthly_exp),
                    "dte": analytics['time_metrics'].dte_monthly,
                    "trading_blocked": analytics['time_metrics'].is_expiry_day_monthly,
                    "square_off_today": analytics['time_metrics'].dte_monthly == 1
                },
                "next_weekly_expiry": {
                    "date": str(analytics['time_metrics'].next_weekly_exp),
                    "dte": analytics['time_metrics'].dte_next_weekly,
                    "trading_blocked": analytics['time_metrics'].is_expiry_day_next_weekly,
                    "square_off_today": analytics['time_metrics'].dte_next_weekly == 1
                }
            },
            "economic_calendar": {
                "veto_events": [
                    {
                        "event_name": e.title,
                        "time": e.event_date.strftime("%H:%M") if hasattr(e, 'event_date') and e.event_date else "Today",
                        "square_off_by": e.suggested_square_off_time.strftime("%Y-%m-%d %H:%M") if e.suggested_square_off_time else "N/A",
                        "action_required": "SQUARE OFF TODAY" if e.days_until <= 1 else f"Square off {e.days_until-1} days before"
                    }
                    for e in ext.economic_events if e.is_veto_event
                ],
                "other_events": [
                    {
                        "event_name": e.title,
                        "impact": e.impact_level,
                        "days_until": e.days_until
                    }
                    for e in ext.economic_events if not e.is_veto_event
                ][:10] 
            },
            "volatility_analysis": {
                "spot": vol.spot,
                "spot_ma20": vol.ma20,
                "vix": vol.vix,
                "vix_trend": vol.vix_momentum,
                "ivp_30d": vol.ivp_30d,
                "ivp_90d": vol.ivp_90d,
                "ivp_1y": vol.ivp_1yr,
                "rv_7d": vol.rv7,
                "rv_28d": vol.rv28,
                "rv_90d": vol.rv90,
                "garch_7d": vol.garch7,
                "garch_28d": vol.garch28,
                "parkinson_7d": vol.park7,
                "parkinson_28d": vol.park28,
                "vov": vol.vov,
                "vov_zscore": vol.vov_zscore,
                "trend_strength": vol.trend_strength
            },
            "structure_analysis": {
                "weekly": {
                    "net_gex_formatted": fmt_cr(analytics['struct_weekly'].net_gex),
                    "gex_regime": analytics['struct_weekly'].gex_regime,
                    "pcr_all": round(analytics['struct_weekly'].pcr, 2),
                    "max_pain": analytics['struct_weekly'].max_pain,
                    "skew_25d": fmt_pct(analytics['struct_weekly'].skew_25d),
                    "skew_regime": analytics['struct_weekly'].skew_regime
                },
                "next_weekly": {
                    "net_gex_formatted": fmt_cr(analytics['struct_next_weekly'].net_gex),
                    "gex_regime": analytics['struct_next_weekly'].gex_regime,
                    "pcr_all": round(analytics['struct_next_weekly'].pcr, 2),
                    "max_pain": analytics['struct_next_weekly'].max_pain,
                    "skew_25d": fmt_pct(analytics['struct_next_weekly'].skew_25d),
                    "skew_regime": analytics['struct_next_weekly'].skew_regime
                },
                "monthly": {
                    "net_gex_formatted": fmt_cr(analytics['struct_monthly'].net_gex),
                    "gex_regime": analytics['struct_monthly'].gex_regime,
                    "pcr_all": round(analytics['struct_monthly'].pcr, 2),
                    "max_pain": analytics['struct_monthly'].max_pain,
                    "skew_25d": fmt_pct(analytics['struct_monthly'].skew_25d),
                    "skew_regime": analytics['struct_monthly'].skew_regime
                }
            },
            "option_edges": {
                "weekly": {
                    "weighted_vrp": fmt_pct(analytics['edge_metrics'].vrp_garch_weekly),
                    "weighted_vrp_tag": "RICH" if analytics['edge_metrics'].vrp_garch_weekly > 0 else "CHEAP",
                    "atm_iv": fmt_pct(analytics['edge_metrics'].iv_weekly)
                },
                "next_weekly": {
                    "weighted_vrp": fmt_pct(analytics['edge_metrics'].vrp_garch_next_weekly),
                    "weighted_vrp_tag": "RICH" if analytics['edge_metrics'].vrp_garch_next_weekly > 0 else "CHEAP",
                    "atm_iv": fmt_pct(analytics['edge_metrics'].iv_next_weekly)
                },
                "monthly": {
                    "weighted_vrp": fmt_pct(analytics['edge_metrics'].vrp_garch_monthly),
                    "weighted_vrp_tag": "RICH" if analytics['edge_metrics'].vrp_garch_monthly > 0 else "CHEAP",
                    "atm_iv": fmt_pct(analytics['edge_metrics'].iv_monthly)
                },
                "term_spread_pct": fmt_pct(analytics['edge_metrics'].term_structure_slope),
                "primary_edge": analytics['edge_metrics'].term_structure_regime
            },
            "regime_scores": {
                "weekly": {
                    "composite": {
                        "score": round(analytics['weekly_score'].total_score, 1),
                        "confidence": analytics['weekly_score'].confidence
                    },
                    "components": {
                        "volatility": { "score": analytics['weekly_score'].vol_score },
                        "structure": { "score": analytics['weekly_score'].struct_score },
                        "edge": { "score": analytics['weekly_score'].edge_score }
                    }
                },
                "next_weekly": {
                    "composite": {
                        "score": round(analytics['next_weekly_score'].total_score, 1),
                        "confidence": analytics['next_weekly_score'].confidence
                    },
                    "components": {
                        "volatility": { "score": analytics['next_weekly_score'].vol_score },
                        "structure": { "score": analytics['next_weekly_score'].struct_score },
                        "edge": { "score": analytics['next_weekly_score'].edge_score }
                    }
                },
                "monthly": {
                    "composite": {
                        "score": round(analytics['monthly_score'].total_score, 1),
                        "confidence": analytics['monthly_score'].confidence
                    },
                    "components": {
                        "volatility": { "score": analytics['monthly_score'].vol_score },
                        "structure": { "score": analytics['monthly_score'].struct_score },
                        "edge": { "score": analytics['monthly_score'].edge_score }
                    }
                }
            },
            "mandates": {
                "weekly": {
                    "trade_status": "ALLOWED" if analytics['weekly_mandate'].is_trade_allowed else "BLOCKED",
                    "strategy": analytics['weekly_mandate'].suggested_structure,
                    "capital": {
                        "deployment_formatted": f"₹{analytics['weekly_mandate'].deployment_amount:,.0f}",
                        "allocation_pct": DynamicConfig.WEEKLY_ALLOCATION_PCT
                    },
                    "veto_reasons": analytics['weekly_mandate'].veto_reasons,
                    "risk_notes": analytics['weekly_mandate'].risk_notes
                },
                "next_weekly": {
                    "trade_status": "ALLOWED" if analytics['next_weekly_mandate'].is_trade_allowed else "BLOCKED",
                    "strategy": analytics['next_weekly_mandate'].suggested_structure,
                    "capital": {
                        "deployment_formatted": f"₹{analytics['next_weekly_mandate'].deployment_amount:,.0f}",
                        "allocation_pct": DynamicConfig.NEXT_WEEKLY_ALLOCATION_PCT
                    },
                    "veto_reasons": analytics['next_weekly_mandate'].veto_reasons,
                    "risk_notes": analytics['next_weekly_mandate'].risk_notes
                },
                "monthly": {
                    "trade_status": "ALLOWED" if analytics['monthly_mandate'].is_trade_allowed else "BLOCKED",
                    "strategy": analytics['monthly_mandate'].suggested_structure,
                    "capital": {
                        "deployment_formatted": f"₹{analytics['monthly_mandate'].deployment_amount:,.0f}",
                        "allocation_pct": DynamicConfig.MONTHLY_ALLOCATION_PCT
                    },
                    "veto_reasons": analytics['monthly_mandate'].veto_reasons,
                    "risk_notes": analytics['monthly_mandate'].risk_notes
                }
            },
            "correlation_report": volguard_system.correlation_manager.get_correlation_report(),
            "participant_positions": {
                "fii_direction": ext.fii_sentiment,
                "fii_conviction": ext.fii_conviction,
                "fii_net_change": ext.fii_net_change
            }
        }
        
    except Exception as e:
        logger.error(f"Professional dashboard error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/live/positions", response_model=LivePositionsResponse)
def get_live_positions(
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    try:
        active_trades = db.query(TradeJournal).filter(
            TradeJournal.status == TradeStatus.ACTIVE.value
        ).all()
        
        if not active_trades:
            return {
                "mtm_pnl": 0.0,
                "pnl_color": "GRAY",
                "greeks": {
                    "delta": 0.0,
                    "theta": 0.0,
                    "vega": 0.0,
                    "gamma": 0.0
                },
                "positions": []
            }
        
        all_instruments = []
        for trade in active_trades:
            legs_data = json.loads(trade.legs_data)
            all_instruments.extend([leg['instrument_token'] for leg in legs_data])
        
        current_prices = volguard_system.fetcher.get_bulk_ltp(list(set(all_instruments)))
        
        total_mtm_pnl = 0.0
        positions_list = []
        total_delta = 0.0
        total_theta = 0.0
        total_vega = 0.0
        total_gamma = 0.0
        
        for trade in active_trades:
            legs_data = json.loads(trade.legs_data)
            trade_pnl = 0.0
            
            for leg in legs_data:
                instrument_key = leg['instrument_token']
                current_price = current_prices.get(instrument_key, leg['entry_price'])
                qty = leg['quantity']
                multiplier = -1 if leg['action'] == 'SELL' else 1
                
                leg_pnl = (current_price - leg['entry_price']) * qty * multiplier
                trade_pnl += leg_pnl
                
                positions_list.append({
                    "symbol": instrument_key.split("|")[-1] if "|" in instrument_key else instrument_key,
                    "qty": qty * (-1 if leg['action'] == 'SELL' else 1),
                    "ltp": round(current_price, 2),
                    "pnl": round(leg_pnl, 2)
                })
                
                if trade.entry_greeks_snapshot:
                    entry_greeks = json.loads(trade.entry_greeks_snapshot)
                    leg_greeks = entry_greeks.get(instrument_key, {})
                    total_delta += leg_greeks.get('delta', 0) * qty * multiplier
                    total_theta += leg_greeks.get('theta', 0) * qty * multiplier
                    total_vega += leg_greeks.get('vega', 0) * qty * multiplier
                    total_gamma += leg_greeks.get('gamma', 0) * qty * multiplier
            
            total_mtm_pnl += trade_pnl
        
        if total_mtm_pnl > 0:
            pnl_color = "GREEN"
        elif total_mtm_pnl < 0:
            pnl_color = "RED"
        else:
            pnl_color = "GRAY"
        
        return {
            "mtm_pnl": round(total_mtm_pnl, 2),
            "pnl_color": pnl_color,
            "greeks": {
                "delta": round(total_delta, 2),
                "theta": round(total_theta, 2),
                "vega": round(total_vega, 2),
                "gamma": round(total_gamma, 2)
            },
            "positions": positions_list
        }
        
    except Exception as e:
        logger.error(f"Live positions error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/journal/history", response_model=List[TradeJournalEntry])
def get_journal_history(
    limit: int = 50,
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    try:
        trades = db.query(TradeJournal).filter(
            TradeJournal.status != TradeStatus.ACTIVE.value
        ).order_by(desc(TradeJournal.exit_time)).limit(limit).all()
        
        history = []
        for trade in trades:
            if trade.realized_pnl and trade.realized_pnl > 0:
                result = "WIN"
            elif trade.realized_pnl and trade.realized_pnl < 0:
                result = "LOSS"
            else:
                result = "BREAKEVEN"
            
            history.append({
                "date": trade.exit_time.strftime("%Y-%m-%d") if trade.exit_time else trade.entry_time.strftime("%Y-%m-%d"),
                "strategy": trade.strategy_type,
                "result": result,
                "pnl": round(trade.realized_pnl or 0, 2),
                "exit_reason": trade.exit_reason or "UNKNOWN"
            })
        
        return history
        
    except Exception as e:
        logger.error(f"Journal history error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/system/config")
def update_system_config(
    config_update: ConfigUpdateRequest,
    db: Session = Depends(get_db),
    token: str = Depends(verify_token)
):
    try:
        updates = {}
        if config_update.max_loss is not None:
            updates["MAX_LOSS_PCT"] = config_update.max_loss
        if config_update.profit_target is not None:
            updates["PROFIT_TARGET"] = config_update.profit_target
        if config_update.base_capital is not None:
            updates["BASE_CAPITAL"] = config_update.base_capital
        if config_update.auto_trading is not None:
            updates["AUTO_TRADING"] = config_update.auto_trading
        if config_update.min_oi is not None:
            updates["MIN_OI"] = config_update.min_oi
        if config_update.max_spread_pct is not None:
            updates["MAX_BID_ASK_SPREAD_PCT"] = config_update.max_spread_pct
        if config_update.max_position_risk_pct is not None:
            updates["MAX_POSITION_RISK_PCT"] = config_update.max_position_risk_pct
        if config_update.max_concurrent_same_strategy is not None:
            updates["MAX_CONCURRENT_SAME_STRATEGY"] = config_update.max_concurrent_same_strategy
        
        changed = DynamicConfig.update(updates)
        
        logger.info(f"Configuration updated via API: {changed}")
        
        if "AUTO_TRADING" in changed and volguard_system and volguard_system.alert_service:
            status = "ENABLED" if changed["AUTO_TRADING"] else "DISABLED"
            volguard_system.alert_service.send(
                "Auto Trading Toggled",
                f"Auto trading has been {status} via system config",
                AlertPriority.HIGH if changed["AUTO_TRADING"] else AlertPriority.MEDIUM
            )
        
        return {
            "success": True,
            "updated": changed,
            "current_config": DynamicConfig.to_dict()
        }
        
    except Exception as e:
        logger.error(f"Config update error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/system/logs", response_model=SystemLogsResponse)
def get_system_logs(
    lines: int = 50,
    level: Optional[str] = None,
    token: str = Depends(verify_token)
):
    try:
        logs = log_buffer.get_logs(lines=lines, level=level)
        return {
            "logs": logs,
            "total_lines": len(logs)
        }
    except Exception as e:
        logger.error(f"Logs fetch error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/system/config/current")
def get_current_config(
    token: str = Depends(verify_token)
):
    return DynamicConfig.to_dict()


@app.get("/api/risk/correlation-report")
def get_correlation_report(
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    report = volguard_system.correlation_manager.get_correlation_report()
    
    return {
        "timestamp": datetime.now().isoformat(),
        "report": report
    }


@app.get("/api/risk/expiries")
def get_expiry_status(
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today = now.date()
    
    weekly, monthly, next_weekly, lot_size, all_expiries = volguard_system.fetcher.get_expiries()
    
    return {
        "timestamp": now.isoformat(),
        "expiries": {
            "weekly": {
                "date": weekly.isoformat() if weekly else None,
                "dte": (weekly - today).days if weekly else None,
                "trading_blocked": (weekly == today) if weekly else False,
                "square_off_required": (weekly - today).days == 1 if weekly else False,
                "square_off_time": "14:00 IST"
            },
            "monthly": {
                "date": monthly.isoformat() if monthly else None,
                "dte": (monthly - today).days if monthly else None,
                "trading_blocked": (monthly == today) if monthly else False,
                "square_off_required": (monthly - today).days == 1 if monthly else False,
                "square_off_time": "14:00 IST"
            },
            "next_weekly": {
                "date": next_weekly.isoformat() if next_weekly else None,
                "dte": (next_weekly - today).days if next_weekly else None,
                "trading_blocked": (next_weekly == today) if next_weekly else False,
                "square_off_required": (next_weekly - today).days == 1 if next_weekly else False,
                "square_off_time": "14:00 IST"
            }
        },
        "all_expiries": [e.isoformat() for e in all_expiries]
    }


@app.get("/")
def root():
    return {
        "system": "VolGuard v3.3",
        "version": "3.3.0",
        "status": "operational",
        "trading_mode": "OVERNIGHT OPTION SELLING",
        "product_type": "D (Delivery/Carryforward)",
        "square_off": "1 day before expiry @ 14:00 IST",
        "websocket": {
            "market_streamer": "ACTIVE" if volguard_system and volguard_system.market_streamer_started else "INACTIVE",
            "portfolio_streamer": "ACTIVE" if volguard_system and volguard_system.portfolio_streamer_started else "INACTIVE"
        },
        "endpoints": {
            "analytics": "/api/dashboard/analytics",
            "professional": "/api/dashboard/professional",
            "live": "/api/live/positions",
            "journal": "/api/journal/history",
            "config": "/api/system/config",
            "logs": "/api/system/logs",
            "correlation": "/api/risk/correlation-report",
            "expiries": "/api/risk/expiries"
        }
    }


@app.get("/api/health")
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = True
    except:
        db_status = False
    
    circuit_breaker_state = db.query(TradeJournal).filter(
        TradeJournal.status == TradeStatus.CLOSED_CIRCUIT_BREAKER.value
    ).first()
    
    circuit_breaker_active = circuit_breaker_state is not None
    
    cache_status = "VALID" if volguard_system and volguard_system.json_cache.is_valid_for_today() else "MISSING"
    
    market_streamer_status = "CONNECTED" if volguard_system and volguard_system.fetcher.market_streamer.is_connected else "DISCONNECTED"
    portfolio_streamer_status = "CONNECTED" if volguard_system and volguard_system.fetcher.portfolio_streamer.is_connected else "DISCONNECTED"
    
    return {
        "status": "healthy" if (db_status and not circuit_breaker_active) else "degraded",
        "database": db_status,
        "daily_cache": cache_status,
        "auto_trading": DynamicConfig.AUTO_TRADING,
        "mock_trading": DynamicConfig.ENABLE_MOCK_TRADING,
        "product_type": "D (Overnight)",
        "circuit_breaker": "ACTIVE" if circuit_breaker_active else "NORMAL",
        "websocket": {
            "market_streamer": market_streamer_status,
            "portfolio_streamer": portfolio_streamer_status,
            "subscribed_instruments": len(volguard_system.fetcher.market_streamer.get_subscribed_instruments()) if volguard_system else 0
        },
        "analytics_cache_age": (
            (datetime.now() - volguard_system.analytics_cache._last_calc_time).total_seconds() // 60
            if volguard_system and volguard_system.analytics_cache._last_calc_time
            else "N/A"
        ),
        "timestamp": datetime.now().isoformat()
    }


@app.post("/api/emergency/exit-all")
def emergency_exit_all(token: str = Depends(verify_token)):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    result = volguard_system.fetcher.emergency_exit_all_positions()
    if result["success"] and volguard_system.alert_service:
        volguard_system.alert_service.send(
            "EMERGENCY EXIT",
            f"Orders: {result['orders_placed']}",
            AlertPriority.CRITICAL,
            throttle_key="emergency_exit"
        )
    return result


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 80)
    print("🚀 VolGuard v3.3 - FINAL PRODUCTION VERSION")
    print("=" * 80)
    print(f"🎯 Trading Mode:    OVERNIGHT OPTION SELLING")
    print(f"📦 Product Type:    D (Delivery/Carryforward)")
    print(f"💰 Base Capital:    ₹{DynamicConfig.DEFAULTS['BASE_CAPITAL']:,.2f}")
    print(f"🤖 Auto Trading:    {'ENABLED 🔴' if DynamicConfig.DEFAULTS['AUTO_TRADING'] else 'DISABLED 🟡'}")
    print(f"🔄 Market Streamer: ✅ WebSocket (ltpc)")
    print(f"📊 Portfolio Streamer: ✅ WebSocket (all updates)")
    print(f"🛡️ GTT Orders:      ✅ Multi-leg with Trailing Stop (FIXED)")
    print(f"🚪 Exit Orders:     ✅ Actual MARKET orders")
    print(f"📅 Square Off:      ✅ 1 day BEFORE expiry @ 14:00 IST")
    print(f"🚫 Expiry Trading:  ✅ BLOCKED")
    print(f"🎯 Correlation Mgr: ✅ Active")
    print(f"📊 POP:             ✅ From Upstox SDK")
    print(f"🔒 Auth:            ✅ X-Upstox-Token Required (No Fallback)")
    print(f"⚡ Async:           ✅ DB/HTTP in executors (FIXED)")
    print(f"📈 Circuit Breaker: ✅ Uses Total MTM (FIXED)")
    print("=" * 80)
    print(f"📚 API Documentation: http://localhost:{SystemConfig.PORT}/docs")
    print("=" * 80)
    
    uvicorn.run(
        "volguard_final:app",
        host=SystemConfig.HOST,
        port=SystemConfig.PORT,
        log_level="info"
    )
