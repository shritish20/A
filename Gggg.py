"""
VolGuard v4.0

"""

import os
import sys
import asyncio
import aiohttp
import logging
import threading
import time
import uuid
from datetime import datetime, date, time as dt_time, timedelta
from typing import List, Dict, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
import json
from contextlib import asynccontextmanager, contextmanager
import urllib.parse
import copy

# FastAPI WebSocket
from fastapi import WebSocket, WebSocketDisconnect

# Third-party imports
import pandas as pd
import numpy as np
import pytz

# FastAPI
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Header, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field

# Database - SQLAlchemy 2.0 compatible
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean, JSON, desc, event, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# Upstox SDK (required)
try:
    import upstox_client
    from upstox_client.rest import ApiException
    UPSTOX_AVAILABLE = True
except ImportError:
    UPSTOX_AVAILABLE = False
    logging.error("upstox_client NOT INSTALLED! Please install: pip install upstox-python-sdk")

# For FII data fetching
import requests

# ============================================================================
# LOGGING SETUP
# ============================================================================

class LogBufferHandler(logging.Handler):
    def __init__(self, capacity=1000):
        super().__init__()
        self.capacity = capacity
        self.buffer = []
        self._lock = threading.Lock()
        
    def emit(self, record):
        with self._lock:
            msg = self.format(record)
            self.buffer.append({
                "timestamp": datetime.fromtimestamp(record.created).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": msg
            })
            if len(self.buffer) > self.capacity:
                self.buffer.pop(0)
    
    def get_logs(self, lines=50, level=None):
        with self._lock:
            logs = self.buffer[-lines:] if lines < len(self.buffer) else self.buffer
            if level:
                logs = [l for l in logs if l["level"] == level.upper()]
            return logs

log_buffer = LogBufferHandler(capacity=1000)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('volguard.log'),
        logging.StreamHandler(),
        log_buffer
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# DYNAMIC CONFIGURATION
# ============================================================================

class DynamicConfig:
    DEFAULTS = {
        "BASE_CAPITAL": 1500000.0,
        "MAX_LOSS_PCT": 3.0,
        "PROFIT_TARGET": 70.0,
        "MAX_DAILY_LOSS_PCT": 3.0,
        "MAX_CONSECUTIVE_LOSSES": 3,
        "CIRCUIT_BREAKER_PCT": 3.0,
        "AUTO_TRADING": False,
        "ENABLE_MOCK_TRADING": True,
        "MIN_OI": 50000,
        "MAX_BID_ASK_SPREAD_PCT": 2.0,
        "MAX_POSITION_RISK_PCT": 2.0,
        "HIGH_VOL_IVP": 75.0,
        "LOW_VOL_IVP": 25.0,
        "VOV_CRASH_ZSCORE": 2.5,
        "VOV_WARNING_ZSCORE": 2.0,
        "VIX_MOMENTUM_BREAKOUT": 5.0,
        "GEX_STICKY_RATIO": 0.03,
        "SKEW_CRASH_FEAR": 5.0,
        "SKEW_MELT_UP": -2.0,
        "FII_VERY_HIGH_CONVICTION": 150000,
        "FII_HIGH_CONVICTION": 80000,
        "FII_MODERATE_CONVICTION": 40000,
        "WEEKLY_ALLOCATION_PCT": 40.0,
        "MONTHLY_ALLOCATION_PCT": 40.0,
        "NEXT_WEEKLY_ALLOCATION_PCT": 20.0,
        "STOP_LOSS_MULTIPLIER": 2.0,
        "PROFIT_TARGET_MULTIPLIER": 0.30,
        "ANALYTICS_INTERVAL_MINUTES": 15,
        "ANALYTICS_OFFHOURS_INTERVAL_MINUTES": 60,
        "POSITION_RECONCILE_INTERVAL_MINUTES": 10,
        "SPOT_CHANGE_TRIGGER_PCT": 0.3,
        "VIX_CHANGE_TRIGGER_PCT": 2.0,
        "PNL_DISCREPANCY_THRESHOLD": 100.0,
        "MAX_CONCURRENT_SAME_STRATEGY": 2,
        "GTT_STOP_LOSS_MULTIPLIER": 2.0,
        "GTT_PROFIT_TARGET_MULTIPLIER": 0.30,
        "GTT_TRAILING_GAP": 0.1,
        "ORDER_FILL_TIMEOUT_SECONDS": 15,
        "ORDER_FILL_CHECK_INTERVAL": 0.5,
        "PROTECTED_STRADDLE_WING_DELTA": 0.02,
        "PROTECTED_STRANGLE_WING_DELTA": 0.05,
        "IRON_CONDOR_WING_DELTA": 0.15,
        "IRON_FLY_WING_MULTIPLIER": 1.10,
        "MONITOR_INTERVAL_SECONDS": 5,
    }
    _values = {}
    _db_session_factory = None
    _initialized = False
    _lock = threading.RLock()
    
    @classmethod
    def initialize(cls, db_session_factory):
        with cls._lock:
            cls._db_session_factory = db_session_factory
            db = db_session_factory()
            try:
                db.execute(text("""
                    CREATE TABLE IF NOT EXISTS dynamic_config (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                db.commit()
                
                for key, default_val in cls.DEFAULTS.items():
                    result = db.execute(
                        text("SELECT value FROM dynamic_config WHERE key = :key"),
                        {"key": key}
                    ).fetchone()
                    
                    if result:
                        stored = result[0]
                        try:
                            if isinstance(default_val, bool):
                                cls._values[key] = stored.lower() == 'true'
                            elif isinstance(default_val, int):
                                cls._values[key] = int(stored)
                            elif isinstance(default_val, float):
                                cls._values[key] = float(stored)
                            else:
                                cls._values[key] = stored
                        except Exception:
                            cls._values[key] = default_val
                    else:
                        cls._values[key] = default_val
                        cls._persist(key, default_val, db)
                
                cls._initialized = True
                logger.info(f"✅ DynamicConfig initialized with {len(cls._values)} settings")
            finally:
                db.close()
    
    @classmethod
    def _persist(cls, key, value, db=None):
        close_db = False
        if db is None:
            db = cls._db_session_factory()
            close_db = True
        
        try:
            str_val = str(value)
            db.execute(
                text("""
                    INSERT INTO dynamic_config (key, value, updated_at) 
                    VALUES (:key, :value, :updated_at)
                    ON CONFLICT(key) DO UPDATE SET 
                    value = excluded.value, 
                    updated_at = excluded.updated_at
                """),
                {"key": key, "value": str_val, "updated_at": datetime.now()}
            )
            db.commit()
        finally:
            if close_db:
                db.close()
    
    @classmethod
    def get(cls, key, default=None):
        with cls._lock:
            return cls._values.get(key, default)
    
    @classmethod
    def update(cls, updates: dict):
        with cls._lock:
            if not cls._initialized:
                raise RuntimeError("DynamicConfig not initialized")
            
            changed = {}
            db = cls._db_session_factory()
            try:
                for key, new_val in updates.items():
                    if key in cls.DEFAULTS:
                        default_type = type(cls.DEFAULTS[key])
                        try:
                            if default_type == bool:
                                if isinstance(new_val, str):
                                    new_val = new_val.lower() == 'true'
                                else:
                                    new_val = bool(new_val)
                            elif default_type == int:
                                new_val = int(new_val)
                            elif default_type == float:
                                new_val = float(new_val)
                            
                            cls._values[key] = new_val
                            cls._persist(key, new_val, db)
                            changed[key] = new_val
                            logger.info(f"Config updated: {key} = {new_val}")
                        except Exception as e:
                            logger.error(f"Failed to update {key}: {e}")
                
                return changed
            finally:
                db.close()
    
    @classmethod
    def to_dict(cls):
        with cls._lock:
            return cls._values.copy()


# ============================================================================
# LEGACY SystemConfig
# ============================================================================

class SystemConfig:
    UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    
    NIFTY_KEY = "NSE_INDEX|Nifty 50"
    VIX_KEY = "NSE_INDEX|India VIX"
    
    VETO_KEYWORDS = [
        "RBI Monetary Policy", "RBI Policy", "Reserve Bank of India",
        "Repo Rate Decision", "MPC Meeting",
        "FOMC", "Federal Reserve Meeting", "Fed Meeting",
        "Federal Funds Rate Decision", "Union Budget", "Budget Speech"
    ]
    HIGH_IMPACT_KEYWORDS = [
        "GDP", "Gross Domestic Product", "NFP", "Non-Farm Payroll",
        "CPI", "Consumer Price Index"
    ]
    EVENT_RISK_DAYS_AHEAD = 14
    
    PRE_EXPIRY_SQUARE_OFF_DAYS = 1
    PRE_EXPIRY_SQUARE_OFF_TIME = dt_time(14, 0)
    PRE_EVENT_SQUARE_OFF_DAYS = 1
    PRE_EVENT_SQUARE_OFF_TIME = dt_time(14, 0)
    
    DAILY_FETCH_TIME_IST = dt_time(21, 0)
    PRE_MARKET_WARM_TIME_IST = dt_time(8, 55)
    MARKET_OPEN_IST = dt_time(9, 15)
    MARKET_CLOSE_IST = dt_time(15, 30)
    PNL_RECONCILE_TIME_IST = dt_time(16, 0)
    
    HOST = "0.0.0.0"
    PORT = int(os.getenv("PORT", "8000"))
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./volguard.db")
    
    @classmethod
    def should_square_off_position(cls, trade) -> Tuple[bool, str]:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        today = now.date()
        
        expiry_date = trade.expiry_date.date()
        days_to_expiry = (expiry_date - today).days
        
        if days_to_expiry == cls.PRE_EXPIRY_SQUARE_OFF_DAYS:
            square_off_time = ist.localize(datetime.combine(
                today, 
                cls.PRE_EXPIRY_SQUARE_OFF_TIME
            ))
            if now >= square_off_time:
                return True, f"PRE_EXPIRY_SQUARE_OFF - {days_to_expiry} day before expiry"
        
        if hasattr(trade, 'associated_event_date') and trade.associated_event_date:
            event_date = trade.associated_event_date.date()
            days_to_event = (event_date - today).days
            
            if days_to_event == cls.PRE_EVENT_SQUARE_OFF_DAYS:
                square_off_time = ist.localize(datetime.combine(
                    today,
                    cls.PRE_EVENT_SQUARE_OFF_TIME
                ))
                if now >= square_off_time:
                    return True, f"PRE_EVENT_SQUARE_OFF - {trade.associated_event_name}"
        
        return False, ""
    
    @classmethod
    def is_expiry_day(cls, date_to_check: date, all_expiries: List[date]) -> bool:
        return date_to_check in all_expiries


# ============================================================================
# AUTHENTICATION
# ============================================================================

security = HTTPBearer(auto_error=False)

async def verify_token(x_upstox_token: Optional[str] = Header(None, alias="X-Upstox-Token")):
    if not x_upstox_token:
        if SystemConfig.UPSTOX_ACCESS_TOKEN:
            logger.warning("⚠️ Using UPSTOX_ACCESS_TOKEN from environment - only for development!")
            return SystemConfig.UPSTOX_ACCESS_TOKEN
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Upstox-Token header"
        )
    return x_upstox_token


# ============================================================================
# WEBSOCKET CONNECTION MANAGER
# ============================================================================

class WebSocketManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self._lock = threading.RLock()
        self.logger = logging.getLogger("WebSocketManager")
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        with self._lock:
            self.active_connections.append(websocket)
        self.logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
        self.logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
    
    async def send_message(self, message: dict, websocket: WebSocket):
        try:
            await websocket.send_json(message)
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: dict):
        disconnected = []
        with self._lock:
            connections = self.active_connections.copy()
        
        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)
        
        for connection in disconnected:
            self.disconnect(connection)


# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class ConfigUpdateRequest(BaseModel):
    max_loss: Optional[float] = Field(None, ge=0.1, le=10.0)
    profit_target: Optional[float] = Field(None, ge=10.0, le=95.0)
    base_capital: Optional[float] = Field(None, ge=100000, le=100000000)
    auto_trading: Optional[bool] = Field(None)
    min_oi: Optional[int] = Field(None, ge=10000, le=500000)
    max_spread_pct: Optional[float] = Field(None, ge=0.1, le=10.0)
    max_position_risk_pct: Optional[float] = Field(None, ge=0.5, le=5.0)
    max_concurrent_same_strategy: Optional[int] = Field(None, ge=1, le=5)
    gtt_stop_loss_multiplier: Optional[float] = Field(None, ge=1.0, le=5.0)
    gtt_profit_target_multiplier: Optional[float] = Field(None, ge=0.1, le=1.0)
    gtt_trailing_gap: Optional[float] = Field(None, ge=0.05, le=1.0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "max_loss": 1.5,
                "profit_target": 75,
                "base_capital": 2000000,
                "auto_trading": True,
                "min_oi": 50000,
                "max_spread_pct": 2.0,
                "max_position_risk_pct": 2.0,
                "max_concurrent_same_strategy": 2,
                "gtt_stop_loss_multiplier": 2.0,
                "gtt_profit_target_multiplier": 0.30,
                "gtt_trailing_gap": 0.1
            }
        }

class DashboardAnalyticsResponse(BaseModel):
    market_status: Dict
    mandate: Dict
    scores: Dict
    events: List[Dict]

class LivePositionsResponse(BaseModel):
    mtm_pnl: float
    pnl_color: str
    greeks: Dict[str, float]
    positions: List[Dict]

class TradeJournalEntry(BaseModel):
    date: str
    strategy: str
    result: str
    pnl: float
    exit_reason: str

class SystemLogsResponse(BaseModel):
    logs: List[Dict]
    total_lines: int


# ============================================================================
# ENUMS & DATA MODELS
# ============================================================================

class AlertPriority(Enum):
    CRITICAL = "🔴 CRITICAL"
    HIGH = "🟠 HIGH"
    MEDIUM = "🟡 MEDIUM"
    LOW = "🔵 INFO"
    SUCCESS = "🟢 SUCCESS"

class StrategyType(str, Enum):
    IRON_FLY = "IRON_FLY"
    IRON_CONDOR = "IRON_CONDOR"
    PROTECTED_STRADDLE = "PROTECTED_STRADDLE"
    PROTECTED_STRANGLE = "PROTECTED_STRANGLE"
    BULL_PUT_SPREAD = "BULL_PUT_SPREAD"
    BEAR_CALL_SPREAD = "BEAR_CALL_SPREAD"
    CASH = "CASH"

class ExpiryType(str, Enum):
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    NEXT_WEEKLY = "NEXT_WEEKLY"

class OrderStatus(str, Enum):
    PENDING = "PENDING"
    PLACED = "PLACED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    PARTIAL = "PARTIAL"

class TradeStatus(str, Enum):
    ACTIVE = "ACTIVE"
    CLOSED_PROFIT_TARGET = "CLOSED_PROFIT_TARGET"
    CLOSED_STOP_LOSS = "CLOSED_STOP_LOSS"
    CLOSED_EXPIRY_EXIT = "CLOSED_EXPIRY_EXIT"
    CLOSED_VETO_EVENT = "CLOSED_VETO_EVENT"
    CLOSED_CIRCUIT_BREAKER = "CLOSED_CIRCUIT_BREAKER"
    PENDING_EXIT = "PENDING_EXIT"


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class TimeMetrics:
    current_date: date
    current_time_ist: datetime
    weekly_exp: date
    monthly_exp: date
    next_weekly_exp: date
    dte_weekly: int
    dte_monthly: int
    dte_next_weekly: int
    is_expiry_day_weekly: bool
    is_expiry_day_monthly: bool
    is_expiry_day_next_weekly: bool
    is_past_square_off_time: bool

@dataclass
class VolMetrics:
    spot: float
    vix: float
    rv7: float
    rv28: float
    rv90: float
    garch7: float
    garch28: float
    park7: float
    park28: float
    vov: float
    vov_zscore: float
    ivp_30d: float
    ivp_90d: float
    ivp_1yr: float
    ma20: float
    atr14: float
    trend_strength: float
    vol_regime: str
    is_fallback: bool
    vix_change_5d: float
    vix_momentum: str

@dataclass
class StructMetrics:
    net_gex: float
    gex_ratio: float
    total_oi_value: float
    gex_regime: str
    pcr: float
    max_pain: float
    skew_25d: float
    oi_regime: str
    lot_size: int
    pcr_atm: float
    skew_regime: str
    gex_weighted: float

@dataclass
class EdgeMetrics:
    iv_weekly: float
    vrp_rv_weekly: float
    vrp_garch_weekly: float
    vrp_park_weekly: float
    iv_monthly: float
    vrp_rv_monthly: float
    vrp_garch_monthly: float
    vrp_park_monthly: float
    iv_next_weekly: float
    vrp_rv_next_weekly: float
    vrp_garch_next_weekly: float
    vrp_park_next_weekly: float
    expiry_risk_discount_weekly: float
    expiry_risk_discount_monthly: float
    expiry_risk_discount_next_weekly: float
    term_structure_slope: float
    term_structure_regime: str
    weighted_vrp_weekly: float
    weighted_vrp_monthly: float
    weighted_vrp_next_weekly: float

@dataclass
class ParticipantData:
    fut_long: float
    fut_short: float
    fut_net: float
    opt_long: float
    opt_short: float
    opt_net: float
    total_net: float

@dataclass
class EconomicEvent:
    title: str
    country: str
    event_date: datetime
    impact_level: str
    event_type: str
    forecast: str
    previous: str
    days_until: int
    hours_until: float
    is_veto_event: bool
    suggested_square_off_time: Optional[datetime]

@dataclass
class ExternalMetrics:
    fii_data: Optional[Dict[str, ParticipantData]]
    fii_secondary: Optional[Dict[str, ParticipantData]]
    fii_net_change: float
    fii_conviction: str
    fii_sentiment: str
    fii_data_date: str
    fii_is_fallback: bool
    economic_events: List[EconomicEvent]
    veto_event_near: bool
    high_impact_event_near: bool
    suggested_square_off_time: Optional[datetime]
    risk_score: float

@dataclass
class DynamicWeights:
    vol_weight: float
    struct_weight: float
    edge_weight: float
    rationale: str

@dataclass
class RegimeScore:
    total_score: float
    vol_score: float
    struct_score: float
    edge_score: float
    external_score: float
    vol_signal: str
    struct_signal: str
    edge_signal: str
    external_signal: str
    overall_signal: str
    confidence: str
    score_drivers: List[str] = field(default_factory=list)

@dataclass
class TradingMandate:
    expiry_type: str
    expiry_date: date
    is_trade_allowed: bool
    suggested_structure: str
    deployment_amount: float
    risk_notes: List[str]
    veto_reasons: List[str]
    regime_summary: str
    confidence_level: str
    square_off_instruction: Optional[str] = None

@dataclass
class OptionLeg:
    instrument_token: str
    strike: float
    option_type: str
    action: str
    quantity: int
    delta: float
    gamma: float
    vega: float
    theta: float
    iv: float
    ltp: float
    bid: float
    ask: float
    oi: float
    lot_size: int
    entry_price: float = 0.0
    entry_bid: float = 0.0
    entry_ask: float = 0.0
    product: str = "D"
    pop: float = 0.0
    filled_quantity: int = 0
    order_id: Optional[str] = None
    correlation_id: Optional[str] = None
    fill_price: Optional[float] = None
    fill_time: Optional[datetime] = None

@dataclass
class ConstructedStrategy:
    strategy_id: str
    strategy_type: StrategyType
    expiry_type: ExpiryType
    expiry_date: date
    legs: List[OptionLeg]
    max_profit: float
    max_loss: float
    pop: float
    net_theta: float
    net_vega: float
    net_delta: float
    net_gamma: float
    allocated_capital: float
    required_margin: float
    max_risk_amount: float
    validation_passed: bool
    validation_errors: List[str] = field(default_factory=list)
    construction_time: datetime = field(default_factory=datetime.now)


# ============================================================================
# SMART DATA FETCHER WITH FALLBACK
# ============================================================================

class SmartDataFetcher:
    def __init__(self, fetcher):
        self.fetcher = fetcher
        self.logger = logging.getLogger(self.__class__.__name__)
        self.ist_tz = pytz.timezone('Asia/Kolkata')
    
    def is_market_open(self) -> bool:
        status = self.fetcher.get_market_status_detailed()
        return status == "NORMAL_OPEN"
    
    def get_market_status(self) -> Dict:
        now = datetime.now(self.ist_tz)
        is_open = self.is_market_open()
        detailed_status = self.fetcher.get_market_status_detailed()
        
        next_open = None
        if not is_open:
            next_day = now
            while True:
                next_day = next_day + timedelta(days=1)
                if next_day.weekday() < 5:
                    next_open = next_day.replace(hour=9, minute=15, second=0, microsecond=0)
                    break
        
        return {
            "is_open": is_open,
            "current_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "timezone": "Asia/Kolkata",
            "next_open": next_open.strftime("%Y-%m-%d %H:%M:%S") if next_open else None,
            "message": f"Market is {detailed_status}" if is_open else "Market is closed",
            "detailed_status": detailed_status
        }
    
    def get_ltp(self, instrument_key: str) -> Optional[float]:
        market_open = self.is_market_open()
        
        if market_open and self.fetcher.market_streamer.is_connected:
            ws_price = self.fetcher.market_streamer.get_ltp(instrument_key)
            if ws_price and ws_price > 0:
                self.logger.debug(f"WebSocket LTP for {instrument_key}: {ws_price}")
                return ws_price
        
        try:
            self.fetcher._check_rate_limit()
            response = self.fetcher.quote_api_v3.get_ltp(
                instrument_key=instrument_key
            )
            
            if response.status == 'success' and response.data:
                response_key = instrument_key.replace('|', ':')
                if response_key in response.data:
                    item = response.data[response_key]
                    if hasattr(item, 'last_price'):
                        price = float(item.last_price)
                        return price
        except Exception as e:
            self.logger.error(f"REST API LTP fallback failed for {instrument_key}: {e}")
        
        return None
    
    def get_bulk_ltp(self, instrument_keys: List[str]) -> Dict[str, Optional[float]]:
        result = {}
        market_open = self.is_market_open()
        
        if market_open and self.fetcher.market_streamer.is_connected:
            ws_prices = self.fetcher.market_streamer.get_bulk_ltp(instrument_keys)
            for key in instrument_keys:
                if key in ws_prices and ws_prices[key] and ws_prices[key] > 0:
                    result[key] = ws_prices[key]
        
        missing = [k for k in instrument_keys if k not in result]
        if missing:
            try:
                self.fetcher._check_rate_limit()
                chunks = [missing[i:i+50] for i in range(0, len(missing), 50)]
                
                for chunk in chunks:
                    response = self.fetcher.quote_api_v3.get_ltp(
                        instrument_key=",".join(chunk)
                    )
                    
                    if response.status == 'success' and response.data:
                        for key in chunk:
                            response_key = key.replace('|', ':')
                            if response_key in response.data:
                                item = response.data[response_key]
                                if hasattr(item, 'last_price'):
                                    result[key] = float(item.last_price)
                    
                    if len(chunks) > 1:
                        time.sleep(0.1)
                        
            except Exception as e:
                self.logger.error(f"Bulk REST LTP failed: {e}")
        
        return result
    
    def get_ohlc(self, instrument_key: str, interval: str = "1d") -> Optional[Dict]:
        try:
            self.fetcher._check_rate_limit()
            response = self.fetcher.quote_api_v3.get_market_quote_ohlc(
                interval=interval,
                instrument_key=instrument_key
            )
            
            if response.status == 'success' and response.data:
                response_key = instrument_key.replace('|', ':')
                if response_key in response.data:
                    item = response.data[response_key]
                    return {
                        'open': float(item.ohlc.open) if hasattr(item.ohlc, 'open') else None,
                        'high': float(item.ohlc.high) if hasattr(item.ohlc, 'high') else None,
                        'low': float(item.ohlc.low) if hasattr(item.ohlc, 'low') else None,
                        'close': float(item.ohlc.close) if hasattr(item.ohlc, 'close') else None,
                        'volume': int(item.volume) if hasattr(item, 'volume') else 0,
                        'timestamp': getattr(item, 'ts', None)
                    }
        except Exception as e:
            self.logger.error(f"OHLC fetch failed for {instrument_key}: {e}")
        
        return None
    
    def get_full_quote(self, instrument_key: str) -> Optional[Dict]:
        try:
            self.fetcher._check_rate_limit()
            response = self.fetcher.quote_api.get_full_market_quote(
                symbol=instrument_key,
                api_version="2.0"
            )
            
            if response.status == 'success' and response.data:
                for api_key, item in response.data.items():
                    actual_token = getattr(item, 'instrument_token', api_key)
                    if actual_token == instrument_key:
                        return {
                            'last_price': float(item.last_price) if hasattr(item, 'last_price') else None,
                            'volume': int(item.volume) if hasattr(item, 'volume') else 0,
                            'open': float(item.ohlc.open) if hasattr(item, 'ohlc') and hasattr(item.ohlc, 'open') else None,
                            'high': float(item.ohlc.high) if hasattr(item, 'ohlc') and hasattr(item.ohlc, 'high') else None,
                            'low': float(item.ohlc.low) if hasattr(item, 'ohlc') and hasattr(item.ohlc, 'low') else None,
                            'close': float(item.ohlc.close) if hasattr(item, 'ohlc') and hasattr(item.ohlc, 'close') else None,
                            'timestamp': getattr(item, 'timestamp', None)
                        }
        except Exception as e:
            self.logger.error(f"Full quote fetch failed for {instrument_key}: {e}")
        
        return None


# ============================================================================
# CORRELATION MANAGER
# ============================================================================

@dataclass
class CorrelationRule:
    primary_strategy: StrategyType
    blocked_strategies: List[StrategyType]
    same_expiry_only: bool = True
    reason: str = ""

@dataclass
class CorrelationViolation:
    existing_trade_id: str
    existing_strategy: str
    existing_expiry: str
    proposed_strategy: str
    proposed_expiry: str
    rule: str
    severity: str

class CorrelationManager:
    def __init__(self, db_session_factory):
        self.db_session_factory = db_session_factory
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.rules = [
            CorrelationRule(
                primary_strategy=StrategyType.IRON_FLY,
                blocked_strategies=[StrategyType.IRON_FLY],
                same_expiry_only=False,
                reason="Cannot hold IRON_FLY in multiple expiries simultaneously"
            ),
            CorrelationRule(
                primary_strategy=StrategyType.IRON_CONDOR,
                blocked_strategies=[StrategyType.IRON_CONDOR],
                same_expiry_only=False,
                reason="Cannot hold IRON_CONDOR in multiple expiries simultaneously"
            ),
            CorrelationRule(
                primary_strategy=StrategyType.PROTECTED_STRADDLE,
                blocked_strategies=[StrategyType.PROTECTED_STRADDLE],
                same_expiry_only=False,
                reason="Cannot hold PROTECTED_STRADDLE in multiple expiries simultaneously"
            ),
            CorrelationRule(
                primary_strategy=StrategyType.PROTECTED_STRANGLE,
                blocked_strategies=[StrategyType.PROTECTED_STRANGLE],
                same_expiry_only=False,
                reason="Cannot hold PROTECTED_STRANGLE in multiple expiries simultaneously"
            ),
            CorrelationRule(
                primary_strategy=StrategyType.PROTECTED_STRADDLE,
                blocked_strategies=[StrategyType.PROTECTED_STRANGLE],
                same_expiry_only=True,
                reason="PROTECTED_STRADDLE and PROTECTED_STRANGLE are similar - choose one per expiry"
            ),
        ]
    
    def can_take_position(self, proposed_strategy: ConstructedStrategy) -> Tuple[bool, List[CorrelationViolation]]:
        with self.db_session_factory() as db:
            active_trades = db.query(TradeJournal).filter(
                TradeJournal.status == TradeStatus.ACTIVE.value
            ).all()
            
            violations = []
            
            for rule in self.rules:
                if (proposed_strategy.strategy_type != rule.primary_strategy and
                    proposed_strategy.strategy_type not in rule.blocked_strategies):
                    continue
                
                for trade in active_trades:
                    try:
                        trade_strategy = StrategyType(trade.strategy_type)
                        
                        is_primary_violation = (
                            proposed_strategy.strategy_type == rule.primary_strategy and
                            trade_strategy in rule.blocked_strategies
                        )
                        
                        is_blocked_violation = (
                            proposed_strategy.strategy_type in rule.blocked_strategies and
                            trade_strategy == rule.primary_strategy
                        )
                        
                        if is_primary_violation or is_blocked_violation:
                            if rule.same_expiry_only:
                                if trade.expiry_date.date() != proposed_strategy.expiry_date:
                                    continue
                            
                            violations.append(CorrelationViolation(
                                existing_trade_id=trade.strategy_id,
                                existing_strategy=trade.strategy_type,
                                existing_expiry=trade.expiry_type,
                                proposed_strategy=proposed_strategy.strategy_type.value,
                                proposed_expiry=proposed_strategy.expiry_type.value,
                                rule=rule.reason,
                                severity="BLOCK"
                            ))
                            break
                            
                    except Exception as e:
                        self.logger.error(f"Error checking trade {trade.strategy_id}: {e}")
                        continue
            
            strategy_counts = {}
            for trade in active_trades:
                strategy_counts[trade.strategy_type] = strategy_counts.get(trade.strategy_type, 0) + 1
            
            proposed_name = proposed_strategy.strategy_type.value
            strategy_counts[proposed_name] = strategy_counts.get(proposed_name, 0) + 1
            
            max_concurrent = DynamicConfig.get("MAX_CONCURRENT_SAME_STRATEGY")
            if strategy_counts.get(proposed_name, 0) > max_concurrent:
                violations.append(CorrelationViolation(
                    existing_trade_id="AGGREGATE",
                    existing_strategy="MULTIPLE",
                    existing_expiry="VARIOUS",
                    proposed_strategy=proposed_name,
                    proposed_expiry=proposed_strategy.expiry_type.value,
                    rule=f"Cannot have more than {max_concurrent} concurrent {proposed_name} positions",
                    severity="BLOCK"
                ))
            
            return len(violations) == 0, violations
    
    def get_correlation_report(self) -> Dict:
        with self.db_session_factory() as db:
            active_trades = db.query(TradeJournal).filter(
                TradeJournal.status == TradeStatus.ACTIVE.value
            ).all()
            
            report = {
                "by_strategy": {},
                "warnings": []
            }
            
            for trade in active_trades:
                strategy = trade.strategy_type
                if strategy not in report["by_strategy"]:
                    report["by_strategy"][strategy] = {
                        "count": 0,
                        "trades": [],
                        "expiries": set()
                    }
                
                report["by_strategy"][strategy]["count"] += 1
                report["by_strategy"][strategy]["trades"].append(trade.strategy_id)
                report["by_strategy"][strategy]["expiries"].add(trade.expiry_type)
            
            max_concurrent = DynamicConfig.get("MAX_CONCURRENT_SAME_STRATEGY")
            for strategy, data in report["by_strategy"].items():
                if data["count"] > max_concurrent:
                    report["warnings"].append(
                        f"⚠️ {data['count']} concurrent {strategy} positions - maximum is {max_concurrent}"
                    )
                
                if strategy in ["IRON_FLY", "IRON_CONDOR", "PROTECTED_STRADDLE", "PROTECTED_STRANGLE"]:
                    if len(data["expiries"]) > 1:
                        report["warnings"].append(
                            f"⚠️ {strategy} held across multiple expiries: {', '.join(data['expiries'])}"
                        )
            
            for strategy in report["by_strategy"]:
                report["by_strategy"][strategy]["expiries"] = list(
                    report["by_strategy"][strategy]["expiries"]
                )
            
            return report


# ============================================================================
# DATABASE MODELS
# ============================================================================

Base = declarative_base()

class TradeJournal(Base):
    __tablename__ = "trades"
    
    id = Column(Integer, primary_key=True)
    strategy_id = Column(String, unique=True, index=True)
    strategy_type = Column(String)
    expiry_type = Column(String)
    expiry_date = Column(DateTime)
    entry_time = Column(DateTime)
    exit_time = Column(DateTime, nullable=True)
    legs_data = Column(JSON)
    order_ids = Column(JSON)
    filled_quantities = Column(JSON, nullable=True)
    fill_prices = Column(JSON, nullable=True)
    gtt_order_ids = Column(JSON, nullable=True)
    entry_greeks_snapshot = Column(JSON, nullable=True)
    max_profit = Column(Float)
    max_loss = Column(Float)
    allocated_capital = Column(Float)
    required_margin = Column(Float, default=0.0)
    entry_premium = Column(Float)
    exit_premium = Column(Float, nullable=True)
    realized_pnl = Column(Float, nullable=True)
    pnl_approximate = Column(Boolean, default=False)
    theta_pnl = Column(Float, nullable=True)
    vega_pnl = Column(Float, nullable=True)
    gamma_pnl = Column(Float, nullable=True)
    status = Column(String)
    exit_reason = Column(String, nullable=True)
    is_mock = Column(Boolean, default=False)
    associated_event_date = Column(DateTime, nullable=True)
    associated_event_name = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class DailyStats(Base):
    __tablename__ = "daily_stats"
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, unique=True, index=True)
    total_pnl = Column(Float, default=0.0)
    realized_pnl = Column(Float, default=0.0)
    unrealized_pnl = Column(Float, default=0.0)
    trades_count = Column(Integer, default=0)
    wins = Column(Integer, default=0)
    losses = Column(Integer, default=0)
    theta_pnl = Column(Float, default=0.0)
    vega_pnl = Column(Float, default=0.0)
    broker_pnl = Column(Float, nullable=True)
    pnl_discrepancy = Column(Float, nullable=True)
    circuit_breaker_triggered = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# ============================================================================
# DATABASE SETUP
# ============================================================================

engine = create_engine(
    SystemConfig.DATABASE_URL, 
    connect_args={"check_same_thread": False} if "sqlite" in SystemConfig.DATABASE_URL else {},
    pool_pre_ping=True
)

@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
Base.metadata.create_all(engine)

@contextmanager
def get_db_context():
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ============================================================================
# TELEGRAM ALERT SERVICE - THREAD SAFE
# ============================================================================

@dataclass
class AlertMessage:
    title: str
    message: str
    priority: AlertPriority
    timestamp: datetime

class TelegramAlertService:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self._queue = asyncio.Queue(maxsize=100)
        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self.logger = logging.getLogger("TelegramBot")
        self._last_alert_time = {}
        self._loop = None

    async def start(self):
        self._loop = asyncio.get_running_loop()
        self._session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._process_queue())
        self.logger.info("✅ Telegram Service Started")

    async def stop(self):
        if self._task:
            self._task.cancel()
        if self._session:
            await self._session.close()

    def send(self, title: str, message: str, priority: AlertPriority = AlertPriority.MEDIUM, throttle_key: str = None):
        if throttle_key:
            last = self._last_alert_time.get(throttle_key)
            if last and (datetime.now() - last).total_seconds() < 300:
                return
            self._last_alert_time[throttle_key] = datetime.now()

        alert = AlertMessage(title, message, priority, datetime.now())
        
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._queue.put(alert), self._loop)
        else:
            try:
                self._queue.put_nowait(alert)
            except asyncio.QueueFull:
                self.logger.error("⚠️ Alert queue full, dropping message")

    async def _process_queue(self):
        while True:
            try:
                alert = await self._queue.get()
                await self._post_to_api(alert)
                self._queue.task_done()
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Telegram Dispatch Error: {e}")

    async def _post_to_api(self, alert: AlertMessage):
        if not self._session:
            return
        text = f"{alert.priority.value} <b>{alert.title}</b>\n\n{alert.message}\n\n<i>{alert.timestamp.strftime('%H:%M:%S')}</i>"
        try:
            payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
            async with self._session.post(f"{self.base_url}/sendMessage", json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    self.logger.error(f"Telegram Failed: {resp.status}")
        except Exception as e:
            self.logger.error(f"Telegram Network Error: {e}")


# ============================================================================
# AUTO TRADING ENGINE
# ============================================================================

class AutoTradingEngine:
    def __init__(self, volguard_system, db_session_factory):
        self.system = volguard_system
        self.db_session_factory = db_session_factory
        self.logger = logging.getLogger(self.__class__.__name__)
        self._lock = threading.RLock()
        self._last_trade_time = {}
        
    def should_trade_expiry(self, expiry_type: str) -> bool:
        with self.db_session_factory() as db:
            active = db.query(TradeJournal).filter(
                TradeJournal.status == TradeStatus.ACTIVE.value,
                TradeJournal.expiry_type == expiry_type
            ).first()
            return active is None
    
    def execute_mandate(self, analysis_data: Dict, mandate: TradingMandate) -> bool:
        auto_trading = DynamicConfig.get("AUTO_TRADING")
        mock_trading = DynamicConfig.get("ENABLE_MOCK_TRADING")
        
        if not (auto_trading or mock_trading):
            self.logger.info("Auto trading disabled - skipping execution")
            return False
        
        if not mandate.is_trade_allowed:
            self.logger.info(f"Trade not allowed by mandate: {mandate.veto_reasons}")
            return False
        
        if not self.should_trade_expiry(mandate.expiry_type):
            self.logger.info(f"Already have active {mandate.expiry_type} trade - skipping")
            return False
        
        now = datetime.now()
        last = self._last_trade_time.get(mandate.expiry_type)
        if last and (now - last).total_seconds() < 300:
            self.logger.info(f"Cooldown active for {mandate.expiry_type} - skipping")
            return False
        
        strategy = self.system.construct_strategy_from_mandate(mandate, analysis_data)
        
        if not strategy:
            self.logger.error(f"Failed to construct strategy for {mandate.expiry_type}")
            return False
        
        if not strategy.validation_passed:
            self.logger.error(f"Strategy validation failed: {strategy.validation_errors}")
            return False
        
        with self.db_session_factory() as db:
            result = self.system.execute_strategy(strategy, db)
            
            if result.get("success"):
                self._last_trade_time[mandate.expiry_type] = now
                self.logger.info(f"✅ Auto-executed {mandate.expiry_type} {strategy.strategy_type.value}")
                return True
            else:
                self.logger.error(f"❌ Execution failed: {result.get('message')}")
                return False
    
    def evaluate_all_mandates(self, analysis_data: Dict) -> Dict[str, bool]:
        """
        Evaluate and execute all three mandates in sequence.

        Capital allocation design:
          weekly_mandate     → 40% of BASE_CAPITAL
          monthly_mandate    → 40% of BASE_CAPITAL
          next_weekly_mandate → 20% of BASE_CAPITAL

        IMPORTANT: All three must be evaluated every session. The previous 'break'
        after weekly_mandate success was leaving 60% of intended capital idle every day.
        Each mandate is independent — a failure in one must not abort the others.
        """
        results = {}

        mandates = [
            ("weekly_mandate",      analysis_data.get('weekly_mandate')),
            ("monthly_mandate",     analysis_data.get('monthly_mandate')),
            ("next_weekly_mandate", analysis_data.get('next_weekly_mandate'))
        ]

        for mandate_key, mandate in mandates:
            if mandate and mandate.is_trade_allowed:
                try:
                    success = self.execute_mandate(analysis_data, mandate)
                    results[mandate_key] = success
                    self.logger.info(
                        f"Mandate [{mandate_key}]: {'EXECUTED ✅' if success else 'SKIPPED (conditions not met)'}"
                    )
                except Exception as e:
                    self.logger.error(f"Mandate [{mandate_key}] raised exception: {e}")
                    results[mandate_key] = False
                    # Continue to next mandate — one failure must not block the others
            else:
                results[mandate_key] = False
                reason = "trade not allowed" if mandate else "mandate missing from analysis_data"
                self.logger.info(f"Mandate [{mandate_key}]: SKIPPED ({reason})")

        return results


# ============================================================================
# P&L ATTRIBUTION ENGINE
# ============================================================================

@dataclass
class AttributionResult:
    total_pnl: float
    theta_pnl: float
    vega_pnl: float
    delta_pnl: float
    other_pnl: float
    iv_change: float
    
    def to_dict(self):
        return {k: round(v, 2) for k, v in self.__dict__.items()}

class PnLAttributionEngine:
    def __init__(self, fetcher):
        self.fetcher = fetcher

    def calculate(self, trade_obj, live_prices: Dict, live_greeks: Dict) -> Optional[AttributionResult]:
        if not trade_obj.entry_greeks_snapshot:
            return None

        entry_greeks = json.loads(trade_obj.entry_greeks_snapshot)
        legs_data = json.loads(trade_obj.legs_data)
        
        total_pnl = 0.0
        theta_pnl = 0.0
        vega_pnl = 0.0
        delta_pnl = 0.0
        avg_iv_change = 0.0
        
        for leg in legs_data:
            key = leg['instrument_token']
            qty = leg.get('filled_quantity', leg['quantity'])
            direction = -1 if leg['action'] == 'SELL' else 1
            
            start = entry_greeks.get(key)
            now = live_greeks.get(key)
            current_price = live_prices.get(key)
            
            if not start or not now or not current_price:
                continue

            leg_pnl = (current_price - leg['entry_price']) * qty * direction
            total_pnl += leg_pnl

            avg_theta = (start.get('theta', 0) + now.get('theta', 0)) / 2
            days_held = (datetime.now() - trade_obj.entry_time).total_seconds() / 86400
            theta_pnl += (avg_theta * days_held * qty * direction * -1)

            avg_vega = (start.get('vega', 0) + now.get('vega', 0)) / 2
            iv_diff = now.get('iv', 0) - start.get('iv', 0)
            vega_pnl += (avg_vega * iv_diff * qty * direction)
            
            avg_delta = (start.get('delta', 0) + now.get('delta', 0)) / 2
            spot_diff = now.get('spot_price', 0) - start.get('spot_price', 0)
            delta_pnl += (avg_delta * spot_diff * qty * direction)
            
            avg_iv_change += iv_diff

        other_pnl = total_pnl - (theta_pnl + vega_pnl + delta_pnl)

        return AttributionResult(
            total_pnl=total_pnl,
            theta_pnl=theta_pnl,
            vega_pnl=vega_pnl,
            delta_pnl=delta_pnl,
            other_pnl=other_pnl,
            iv_change=avg_iv_change / len(legs_data) if legs_data else 0
        )


# ============================================================================
# FILL QUALITY TRACKER
# ============================================================================

@dataclass
class FillQualityMetrics:
    order_id: str
    instrument_token: str
    limit_price: float
    fill_price: float
    slippage: float
    slippage_pct: float
    time_to_fill_seconds: float
    partial_fill: bool
    filled_quantity: int
    requested_quantity: int
    timestamp: datetime

class FillQualityTracker:
    def __init__(self):
        self.fills: List[FillQualityMetrics] = []
      
    def record_fill(self, order_id: str, instrument: str, limit_price: float, 
                    fill_price: float, order_time: datetime, fill_time: datetime,
                    filled_qty: int, requested_qty: int, partial: bool = False):
        slippage = fill_price - limit_price
        slippage_pct = (slippage / limit_price * 100) if limit_price > 0 else 0
          
        metric = FillQualityMetrics(
            order_id=order_id,
            instrument_token=instrument,
            limit_price=limit_price,
            fill_price=fill_price,
            slippage=slippage,
            slippage_pct=slippage_pct,
            time_to_fill_seconds=(fill_time - order_time).total_seconds(),
            partial_fill=partial,
            filled_quantity=filled_qty,
            requested_quantity=requested_qty,
            timestamp=fill_time
        )
          
        self.fills.append(metric)
          
        if abs(slippage_pct) > 0.5:
            logger.warning(f"High slippage: {slippage_pct:.2f}% on {instrument}")
      
    def get_stats(self) -> Dict:
        if not self.fills:
            return {"count": 0}
          
        return {
            "total_fills": len(self.fills),
            "avg_slippage_pct": sum(f.slippage_pct for f in self.fills) / len(self.fills),
            "max_slippage_pct": max(f.slippage_pct for f in self.fills),
            "avg_time_to_fill": sum(f.time_to_fill_seconds for f in self.fills) / len(self.fills),
            "partial_fills": sum(1 for f in self.fills if f.partial_fill)
        }


# ============================================================================
# JSON CACHE MANAGER
# ============================================================================

class JSONCacheManager:
    FILE_PATH = "daily_context.json"
    
    def __init__(self, ist_tz=None):
        self.ist_tz = ist_tz or pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
        self._last_fetch_attempt: Optional[datetime] = None
        self._lock = threading.Lock()
        self._data = self._load()
        from concurrent.futures import ThreadPoolExecutor
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cache")
        self._calendar_engine = EconomicCalendarEngine()
        
    def _load(self) -> Dict:
        if not os.path.exists(self.FILE_PATH):
            return {}
        try:
            with open(self.FILE_PATH, 'r') as f:
                return json.load(f)
        except:
            return {}
    
    def _save(self) -> bool:
        try:
            temp = self.FILE_PATH + ".tmp"
            with open(temp, 'w') as f:
                json.dump(self._data, f, indent=4, default=str)
            os.replace(temp, self.FILE_PATH)
            return True
        except Exception as e:
            self.logger.error(f"Save failed: {e}")
            return False
    
    def set_alert_service(self, alert_service):
        self._calendar_engine.set_alert_service(alert_service)
    
    def get_today_cache(self) -> Optional[Dict]:
        with self._lock:
            if not self._data.get("is_valid"):
                return None
            if self._data.get("cache_date") != str(date.today()):
                return None
            return self._data.copy()
    
    def is_valid_for_today(self) -> bool:
        cache = self.get_today_cache()
        return cache is not None and cache.get("is_valid", False)
    
    def get_context(self) -> Dict:
        return self._data.copy()
    
    def fetch_and_cache(self, force: bool = False) -> bool:
        with self._lock:
            now = datetime.now(self.ist_tz)
            today = now.date()
            
            if not force:
                existing = self.get_today_cache()
                if existing:
                    self.logger.info("Daily cache already exists")
                    return True
            
            self.logger.info(f"Starting daily fetch at {now}")
            self._last_fetch_attempt = now
            
            try:
                fii_primary, fii_secondary, fii_net_change, fii_date_str, is_fallback = \
                    ParticipantDataFetcher.fetch_smart_participant_data()
                
                events = self._calendar_engine.fetch_calendar(SystemConfig.EVENT_RISK_DAYS_AHEAD)
                
                self._data = {
                    "cache_date": str(today),
                    "fetch_timestamp": now.isoformat(),
                    "fii_data": {k: asdict(v) if v else None for k, v in fii_primary.items()} if fii_primary else None,
                    "fii_secondary": {k: asdict(v) if v else None for k, v in fii_secondary.items()} if fii_secondary else None,
                    "fii_net_change": fii_net_change or 0.0,
                    "fii_data_date_str": fii_date_str or "NO DATA",
                    "fii_is_fallback": is_fallback,
                    "economic_events": [asdict(e) for e in events],
                    "is_valid": True
                }
                
                success = self._save()
                if success:
                    self.logger.info("Daily cache saved")
                return success
                
            except Exception as e:
                self.logger.error(f"Daily fetch failed: {e}")
                self._data = {
                    "cache_date": str(today),
                    "fetch_timestamp": now.isoformat(),
                    "is_valid": False,
                    "error": str(e)
                }
                self._save()
                return False
    
    def get_external_metrics(self) -> ExternalMetrics:
        cache = self.get_today_cache()
        
        if not cache or not cache.get("is_valid"):
            return ExternalMetrics(
                fii_data=None, fii_secondary=None, fii_net_change=0.0,
                fii_conviction="NO_DATA", fii_sentiment="NO_DATA",
                fii_data_date="NO_DATA", fii_is_fallback=True,
                economic_events=[], veto_event_near=False,
                high_impact_event_near=False, suggested_square_off_time=None,
                risk_score=0.0
            )
        
        fii_data = None
        if cache.get("fii_data") and "FII" in cache["fii_data"]:
            fii_dict = cache["fii_data"]["FII"]
            if fii_dict:
                fii_data = {"FII": ParticipantData(**fii_dict)}
        
        fii_secondary = None
        if cache.get("fii_secondary"):
            fii_secondary = {k: ParticipantData(**v) if v else None 
                           for k, v in cache["fii_secondary"].items()}
        
        events = []
        for e_dict in cache.get("economic_events", []):
            e_dict_copy = e_dict.copy()
            
            if 'event_date' in e_dict_copy:
                if isinstance(e_dict_copy['event_date'], str):
                    e_dict_copy['event_date'] = datetime.fromisoformat(e_dict_copy['event_date'])
            
            if e_dict_copy.get('suggested_square_off_time'):
                if isinstance(e_dict_copy['suggested_square_off_time'], str):
                    e_dict_copy['suggested_square_off_time'] = datetime.fromisoformat(
                        e_dict_copy['suggested_square_off_time']
                    )
            
            events.append(EconomicEvent(**e_dict_copy))
        
        fii_net_change = cache.get("fii_net_change", 0.0)
        
        if abs(fii_net_change) > DynamicConfig.get("FII_VERY_HIGH_CONVICTION"):
            conviction = "VERY_HIGH"
        elif abs(fii_net_change) > DynamicConfig.get("FII_HIGH_CONVICTION"):
            conviction = "HIGH"
        elif abs(fii_net_change) > DynamicConfig.get("FII_MODERATE_CONVICTION"):
            conviction = "MODERATE"
        else:
            conviction = "LOW"
        
        sentiment = "BULLISH" if fii_net_change > 0 else "BEARISH" if fii_net_change < 0 else "NEUTRAL"
        
        veto_event_near = any(e.is_veto_event and e.days_until <= 1 for e in events)
        high_impact_event_near = any(
            e.impact_level == "HIGH" and e.days_until <= 2 for e in events
        )
        
        suggested_square_off = None
        for e in events:
            if e.is_veto_event and e.days_until == 1 and e.suggested_square_off_time:
                suggested_square_off = e.suggested_square_off_time
                break
        
        risk_score = 0.0
        if veto_event_near:
            risk_score += 5.0
        if high_impact_event_near:
            risk_score += 2.0
        if abs(fii_net_change) > DynamicConfig.get("FII_VERY_HIGH_CONVICTION"):
            risk_score += 1.0
        
        return ExternalMetrics(
            fii_data=fii_data,
            fii_secondary=fii_secondary,
            fii_net_change=fii_net_change,
            fii_conviction=conviction,
            fii_sentiment=sentiment,
            fii_data_date=cache.get("fii_data_date_str", "NO DATA"),
            fii_is_fallback=cache.get("fii_is_fallback", True),
            economic_events=events,
            veto_event_near=veto_event_near,
            high_impact_event_near=high_impact_event_near,
            suggested_square_off_time=suggested_square_off,
            risk_score=risk_score
        )
    
    async def schedule_daily_fetch(self):
        while True:
            try:
                now = datetime.now(self.ist_tz)
                current_time = now.time()
                
                if current_time.hour == SystemConfig.DAILY_FETCH_TIME_IST.hour and \
                   current_time.minute == SystemConfig.DAILY_FETCH_TIME_IST.minute:
                    await asyncio.get_event_loop().run_in_executor(
                        self._executor, self.fetch_and_cache, True
                    )
                    await asyncio.sleep(60)
                
                elif current_time.hour == SystemConfig.PRE_MARKET_WARM_TIME_IST.hour and \
                     current_time.minute == SystemConfig.PRE_MARKET_WARM_TIME_IST.minute:
                    if not self.is_valid_for_today():
                        await asyncio.get_event_loop().run_in_executor(
                            self._executor, self.fetch_and_cache, True
                        )
                    await asyncio.sleep(60)
                
                else:
                    await asyncio.sleep(30)
                    
            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(60)
    
    def __del__(self):
        if hasattr(self, '_executor'):
            self._executor.shutdown(wait=False)


# ============================================================================
# PARTICIPANT DATA FETCHER
# ============================================================================

class ParticipantDataFetcher:
    @staticmethod
    def fetch_smart_participant_data() -> Tuple[Optional[Dict], Optional[Dict], float, str, bool]:
        sources = [
            {
                "url": "https://www.nseindia.com/api/fo-participant-oi-data",
                "parser": "nse"
            },
            {
                "url": "https://public.fyers.in/fo-participant-oi.json",
                "parser": "fyers"
            }
        ]
        
        for source in sources:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'keep-alive',
                }
                
                session = requests.Session()
                session.headers.update(headers)
                
                if source["parser"] == "nse":
                    session.get("https://www.nseindia.com", timeout=10)
                    time.sleep(2)
                
                response = session.get(source["url"], timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if source["parser"] == "nse":
                        return ParticipantDataFetcher._parse_nse_data(data)
                    elif source["parser"] == "fyers":
                        return ParticipantDataFetcher._parse_fyers_data(data)
                        
            except Exception as e:
                logger.warning(f"FII data fetch from {source['parser']} failed: {e}")
                continue
        
        logger.error("All FII data sources failed")
        return None, None, 0.0, "FALLBACK", True
    
    @staticmethod
    def _parse_nse_data(data: Dict) -> Tuple[Optional[Dict], Optional[Dict], float, str, bool]:
        try:
            date_str = data.get('timestamp', datetime.now().strftime("%d-%b-%Y"))
            
            fii_data = None
            all_participants = {}
            
            for entry in data.get('data', []):
                participant = entry.get('participant', '').upper()
                
                if 'FII' in participant:
                    key = 'FII'
                elif 'DII' in participant:
                    key = 'DII'
                elif 'PRO' in participant or participant == 'PRO':
                    key = 'PRO'
                elif 'CLIENT' in participant or participant == 'CLIENT':
                    key = 'CLIENT'
                else:
                    continue
                
                participant_obj = ParticipantData(
                    fut_long=float(entry.get('futLong', 0)),
                    fut_short=float(entry.get('futShort', 0)),
                    fut_net=float(entry.get('futLong', 0) - entry.get('futShort', 0)),
                    opt_long=float(entry.get('optLong', 0)),
                    opt_short=float(entry.get('optShort', 0)),
                    opt_net=float(entry.get('optLong', 0) - entry.get('optShort', 0)),
                    total_net=(float(entry.get('futLong', 0) - entry.get('futShort', 0)) + 
                              float(entry.get('optLong', 0) - entry.get('optShort', 0)))
                )
                
                all_participants[key] = participant_obj
                
                if key == 'FII':
                    fii_data = participant_obj
            
            primary = {"FII": fii_data} if fii_data else None
            net_change = fii_data.total_net if fii_data else 0.0
            
            return primary, all_participants, net_change, date_str, False
            
        except Exception as e:
            logger.error(f"Error parsing NSE data: {e}")
            return None, None, 0.0, "FALLBACK", True
    
    @staticmethod
    def _parse_fyers_data(data: Dict) -> Tuple[Optional[Dict], Optional[Dict], float, str, bool]:
        try:
            date_str = data.get('date', datetime.now().strftime("%Y-%m-%d"))
            
            fii_data = None
            all_participants = {}
            
            for item in data.get('data', []):
                participant = item.get('participant', '').upper()
                
                participant_obj = ParticipantData(
                    fut_long=float(item.get('fut_long', 0)),
                    fut_short=float(item.get('fut_short', 0)),
                    fut_net=float(item.get('fut_net', 0)),
                    opt_long=float(item.get('opt_long', 0)),
                    opt_short=float(item.get('opt_short', 0)),
                    opt_net=float(item.get('opt_net', 0)),
                    total_net=float(item.get('total_net', 0))
                )
                
                all_participants[participant] = participant_obj
                
                if participant == 'FII':
                    fii_data = participant_obj
            
            primary = {"FII": fii_data} if fii_data else None
            net_change = fii_data.total_net if fii_data else 0.0
            
            return primary, all_participants, net_change, date_str, False
            
        except Exception as e:
            logger.error(f"Error parsing Fyers data: {e}")
            return None, None, 0.0, "FALLBACK", True


# ============================================================================
# ECONOMIC CALENDAR ENGINE - WITH PAID API FALLBACK
# ============================================================================

class EconomicCalendarEngine:
    def __init__(self):
        self.utc_tz = pytz.UTC
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cache = {}
        self.cache_timestamp = None
        self.alert_service = None
        
        self.alpha_vantage_key = os.getenv("ALPHA_VANTAGE_API_KEY", "")
        self.eodhd_key = os.getenv("EODHD_API_KEY", "")
    
    def set_alert_service(self, alert_service):
        self.alert_service = alert_service
    
    def get_square_off_for_event(self, event_date: datetime) -> Optional[datetime]:
        ist_date = event_date.astimezone(self.ist_tz)
        event_day = ist_date.date()
        today = datetime.now(self.ist_tz).date()
        
        days_until = (event_day - today).days
        
        if days_until <= 1:
            square_off_date = today
        else:
            square_off_date = event_day - timedelta(days=1)
        
        if square_off_date.weekday() == 5:
            square_off_date = square_off_date - timedelta(days=1)
        elif square_off_date.weekday() == 6:
            square_off_date = square_off_date - timedelta(days=2)
        
        square_off = self.ist_tz.localize(
            datetime.combine(square_off_date, SystemConfig.PRE_EVENT_SQUARE_OFF_TIME)
        )
        
        return square_off
    
    def classify_event(self, title: str, country: str, importance: int, 
                       event_datetime: datetime) -> Tuple[str, bool, Optional[datetime]]:
        is_veto = False
        event_type = "OTHER"
        suggested_square_off = None
        
        title_upper = title.upper()
        
        for keyword in SystemConfig.VETO_KEYWORDS:
            if keyword.upper() in title_upper:
                is_veto = True
                if "RBI" in keyword or "REPO" in keyword or "MPC" in keyword:
                    event_type = "RBI_POLICY"
                elif "FOMC" in keyword or "FED" in keyword:
                    event_type = "FOMC"
                elif "BUDGET" in keyword:
                    event_type = "BUDGET"
                break
        
        if not is_veto:
            for keyword in SystemConfig.HIGH_IMPACT_KEYWORDS:
                if keyword.upper() in title_upper:
                    event_type = "HIGH_IMPACT"
                    break
        
        if importance == 1 and country in ["IN", "US"]:
            if event_type == "OTHER":
                event_type = "HIGH_IMPACT"
        
        if is_veto:
            suggested_square_off = self.get_square_off_for_event(event_datetime)
        
        return event_type, is_veto, suggested_square_off
    
    def fetch_calendar(self, days_ahead: int = 14) -> List[EconomicEvent]:
        now = datetime.now(self.ist_tz)
        if self.cache_timestamp and (now - self.cache_timestamp).total_seconds() < 21600:
            if self.cache.get('events'):
                return self.cache['events']
        
        events = []
        
        sources = [
            self._fetch_from_alpha_vantage,
            self._fetch_from_eodhd,
            self._fetch_from_investing_com,
            self._fetch_from_forexfactory
        ]
        
        for source in sources:
            try:
                events = source(days_ahead)
                if events:
                    self.cache['events'] = events
                    self.cache_timestamp = now
                    self.logger.info(f"✅ Fetched {len(events)} events from {source.__name__}")
                    break
            except Exception as e:
                self.logger.warning(f"Calendar source {source.__name__} failed: {e}")
                continue
        
        if not events:
            self.logger.error("No calendar events fetched from any source")
            if self.alert_service:
                self.alert_service.send(
                    "🚨 Calendar API Failure",
                    "All economic calendar APIs failed - no veto events detected. Trading decisions may be at risk.",
                    AlertPriority.CRITICAL,
                    throttle_key="calendar_failure"
                )
        
        return events
    
    def _fetch_from_alpha_vantage(self, days_ahead: int) -> List[EconomicEvent]:
        if not self.alpha_vantage_key:
            return []
        
        events = []
        try:
            url = f"https://www.alphavantage.co/query"
            params = {
                "function": "ECONOMIC_CALENDAR",
                "apikey": self.alpha_vantage_key,
                "horizon": "3month"
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                for item in data.get('economic_calendar', []):
                    pass
                    
        except Exception as e:
            self.logger.error(f"Alpha Vantage fetch error: {e}")
        
        return events
    
    def _fetch_from_eodhd(self, days_ahead: int) -> List[EconomicEvent]:
        if not self.eodhd_key:
            return []
        
        events = []
        try:
            today = datetime.now(self.ist_tz).date()
            from_date = today.strftime("%Y-%m-%d")
            to_date = (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
            
            url = f"https://eodhistoricaldata.com/api/economic-events"
            params = {
                "api_token": self.eodhd_key,
                "fmt": "json",
                "from": from_date,
                "to": to_date,
                "country": "US,IN"
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                now_ist = datetime.now(self.ist_tz)
                
                for item in data:
                    try:
                        event_date = datetime.fromisoformat(item.get('date', '').replace('Z', '+00:00'))
                        ist_time = event_date.astimezone(self.ist_tz)
                        
                        days_until = (ist_time.date() - today).days
                        if days_until < 0 or days_until > days_ahead:
                            continue
                        
                        title = item.get('event', '')
                        country = item.get('country', '')
                        importance = 1 if item.get('impact', '').lower() == 'high' else 2
                        
                        event_type, is_veto, square_off = self.classify_event(
                            title, country, importance, ist_time
                        )
                        
                        impact_label = "VETO" if is_veto else "HIGH" if importance <= 2 else "MEDIUM"
                        
                        event = EconomicEvent(
                            title=title,
                            country=country,
                            event_date=ist_time,
                            impact_level=impact_label,
                            event_type=event_type,
                            forecast=item.get('forecast', '-'),
                            previous=item.get('previous', '-'),
                            days_until=days_until,
                            hours_until=(ist_time - now_ist).total_seconds() / 3600,
                            is_veto_event=is_veto,
                            suggested_square_off_time=square_off
                        )
                        events.append(event)
                        
                    except Exception as e:
                        self.logger.debug(f"Error parsing EODHD event: {e}")
                        continue
                        
        except Exception as e:
            self.logger.error(f"EODHD fetch error: {e}")
        
        return events
    
    def _fetch_from_investing_com(self, days_ahead: int) -> List[EconomicEvent]:
        events = []
        
        try:
            now_ist = datetime.now(self.ist_tz)
            today = now_ist.date()
            
            urls = [
                "https://api.investing.com/api/financialdata/events/economic",
                "https://economic-calendar.tradingview.com/events"
            ]
            
            for url in urls:
                try:
                    params = {
                        "limit": 100,
                        "from": today.isoformat(),
                        "to": (today + timedelta(days=days_ahead)).isoformat(),
                        "importance": "1,2,3"
                    }
                    
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Accept': 'application/json',
                    }
                    
                    response = requests.get(url, params=params, headers=headers, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        for e in data.get('data', []):
                            try:
                                date_str = e.get('date', e.get('time', ''))
                                if 'T' in date_str:
                                    utc_time = datetime.strptime(
                                        date_str.split('.')[0], "%Y-%m-%dT%H:%M:%S"
                                    ).replace(tzinfo=self.utc_tz)
                                else:
                                    utc_time = datetime.strptime(date_str, "%Y-%m-%d").replace(
                                        hour=12, minute=0, tzinfo=self.utc_tz
                                    )
                                
                                ist_time = utc_time.astimezone(self.ist_tz)
                                
                                days_until = (ist_time.date() - today).days
                                
                                if days_until < 0 or days_until > days_ahead:
                                    continue
                                
                                title = e.get('title', e.get('event', 'Unknown Event'))
                                country = e.get('country', e.get('region', ''))
                                importance = int(e.get('importance', e.get('impact', 3)))
                                
                                event_type, is_veto, square_off = self.classify_event(
                                    title, country, importance, ist_time
                                )
                                
                                impact_label = "VETO" if is_veto else "HIGH" if importance <= 2 else "MEDIUM"
                                
                                event = EconomicEvent(
                                    title=title,
                                    country=country,
                                    event_date=ist_time,
                                    impact_level=impact_label,
                                    event_type=event_type,
                                    forecast=str(e.get('forecast', '-')),
                                    previous=str(e.get('previous', '-')),
                                    days_until=days_until,
                                    hours_until=(ist_time - now_ist).total_seconds() / 3600,
                                    is_veto_event=is_veto,
                                    suggested_square_off_time=square_off
                                )
                                events.append(event)
                                
                            except Exception as e:
                                self.logger.debug(f"Error parsing event: {e}")
                                continue
                        
                        if events:
                            break
                            
                except Exception as e:
                    self.logger.debug(f"Investing.com endpoint failed: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Investing.com fetch error: {e}")
        
        events.sort(key=lambda x: (0 if x.is_veto_event else 1, x.days_until, x.event_date))
        
        return events
    
    def _fetch_from_forexfactory(self, days_ahead: int) -> List[EconomicEvent]:
        events = []
        
        try:
            import feedparser
            feed = feedparser.parse('https://nfs.faireconomy.media/ff_calendar_thisweek.xml')
            
            now_ist = datetime.now(self.ist_tz)
            today = now_ist.date()
            
            for entry in feed.entries[:50]:
                try:
                    event_date = datetime.strptime(entry.get('date', ''), '%Y-%m-%d %H:%M:%S')
                    event_date = pytz.UTC.localize(event_date)
                    ist_time = event_date.astimezone(self.ist_tz)
                    
                    days_until = (ist_time.date() - today).days
                    if days_until < 0 or days_until > days_ahead:
                        continue
                    
                    title = entry.get('title', '')
                    country = entry.get('country', '')
                    impact = entry.get('impact', 'Low')
                    
                    importance = 1 if impact == 'High' else 2 if impact == 'Medium' else 3
                    
                    event_type, is_veto, square_off = self.classify_event(
                        title, country, importance, ist_time
                    )
                    
                    if event_type == "OTHER" and not is_veto:
                        continue
                    
                    impact_label = "VETO" if is_veto else impact.upper()
                    
                    event = EconomicEvent(
                        title=title,
                        country=country,
                        event_date=ist_time,
                        impact_level=impact_label,
                        event_type=event_type,
                        forecast=entry.get('forecast', '-'),
                        previous=entry.get('previous', '-'),
                        days_until=days_until,
                        hours_until=(ist_time - now_ist).total_seconds() / 3600,
                        is_veto_event=is_veto,
                        suggested_square_off_time=square_off
                    )
                    events.append(event)
                    
                except Exception as e:
                    continue
                    
        except Exception as e:
            self.logger.error(f"ForexFactory fetch error: {e}")
        
        return events


# ============================================================================
# MARKET DATA STREAMER
# ============================================================================

@dataclass
class MarketUpdate:
    instrument_key: str
    ltp: float
    ltt: Optional[int] = None
    ltq: Optional[int] = None
    cp: Optional[float] = None
    volume: Optional[int] = None
    oi: Optional[int] = None
    bid_price: Optional[float] = None
    bid_qty: Optional[int] = None
    ask_price: Optional[float] = None
    ask_qty: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.now)
    
    @classmethod
    def from_feed(cls, instrument_key: str, feed_data: Any) -> 'MarketUpdate':
        update = cls(instrument_key=instrument_key, ltp=0.0)
        
        if hasattr(feed_data, 'ltpc'):
            update.ltp = getattr(feed_data.ltpc, 'ltp', 0.0) or 0.0
            update.ltt = getattr(feed_data.ltpc, 'ltt', None)
            update.ltq = getattr(feed_data.ltpc, 'ltq', None)
            update.cp = getattr(feed_data.ltpc, 'cp', None)
        
        if hasattr(feed_data, 'full'):
            full = feed_data.full
            update.ltp = getattr(full, 'ltp', update.ltp) or update.ltp
            update.volume = getattr(full, 'volume', None)
            update.oi = getattr(full, 'oi', None)
            
            if hasattr(full, 'market_quotes') and full.market_quotes:
                depth = full.market_quotes
                if hasattr(depth, 'bid'):
                    bids = depth.bid
                    if bids and len(bids) > 0:
                        update.bid_price = getattr(bids[0], 'price', None)
                        update.bid_qty = getattr(bids[0], 'quantity', None)
                if hasattr(depth, 'ask'):
                    asks = depth.ask
                    if asks and len(asks) > 0:
                        update.ask_price = getattr(asks[0], 'price', None)
                        update.ask_qty = getattr(asks[0], 'quantity', None)
        
        return update

class VolGuardMarketStreamer:
    MODE_MAP = {
        "ltpc": "ltpc",
        "full": "full", 
        "option_greeks": "option_greeks",
        "full_d30": "full_d30"
    }
    
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
        self._ws_request_timestamps = []
        self._ws_rate_limit_lock = threading.RLock()
        
    def _check_ws_rate_limit(self):
        with self._ws_rate_limit_lock:
            now = time.time()
            self._ws_request_timestamps = [t for t in self._ws_request_timestamps 
                                          if now - t < 1.0]
            
            if len(self._ws_request_timestamps) >= 50:
                sleep_time = self._ws_request_timestamps[0] + 1.0 - now
                if sleep_time > 0:
                    self.logger.warning(f"WebSocket rate limit reached, sleeping {sleep_time:.2f}s")
                    time.sleep(sleep_time)
            
            self._ws_request_timestamps.append(now)
        
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
            
            self.streamer.auto_reconnect(True, 2, 30)
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
                
                self._check_ws_rate_limit()
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
                self._check_ws_rate_limit()
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
                self._check_ws_rate_limit()
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
    
    def get_bulk_ltp(self, instrument_keys: List[str]) -> Dict[str, Optional[float]]:
        with self._lock:
            return {k: self._latest_prices.get(k) for k in instrument_keys}
    
    def get_subscribed_instruments(self) -> Dict[str, str]:
        with self._lock:
            return self._subscribed_instruments.copy()
    
    def _on_sdk_open(self):
        self.is_connected = True
        self._latest_prices = {}
        self._latest_updates = {}
        self.logger.info("MarketDataStreamerV3 connected")
        self._dispatch("open", {"status": "connected"})
    
    def _on_sdk_close(self, *args, **kwargs):
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

class VolGuardPortfolioStreamer:
    UPSTOX_STATUS_MAP = {
        "put order req received": "PENDING",
        "validation pending": "PENDING",
        "open pending": "PENDING",
        "open": "OPEN",
        "complete": "FILLED",
        "rejected": "REJECTED",
        "cancelled": "CANCELLED",
        "partial": "PARTIAL"
    }
    
    def __init__(self, api_client: upstox_client.ApiClient, fetcher):
        self.api_client = api_client
        self.fetcher = fetcher
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
        self._order_fills: Dict[str, Dict] = {}
    
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
            
            self.streamer.auto_reconnect(True, 2, 30)
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
            if order_id in self._latest_orders:
                return self._latest_orders[order_id]
            
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(
                        self._fetch_order_status_async(order_id),
                        loop
                    )
                    return future.result(timeout=5)
                else:
                    return self._fetch_order_status_sync(order_id)
                    
            except Exception as e:
                self.logger.error(f"Failed to fetch order {order_id} status: {e}")
                return None
    
    def _fetch_order_status_sync(self, order_id: str) -> Optional[Dict]:
        try:
            response = self.fetcher.order_api.get_order_status(
                api_version="2.0", 
                order_id=order_id
            )
            
            if response.status == "success" and response.data:
                data = response.data
                status_info = {
                    "order_id": data.order_id,
                    "status": data.status,
                    "filled_quantity": data.filled_quantity,
                    "average_price": data.average_price,
                    "pending_quantity": data.pending_quantity,
                    "timestamp": datetime.now().isoformat(),
                    "source": "rest_fallback"
                }
                
                mapped_status = self.UPSTOX_STATUS_MAP.get(data.status, "UNKNOWN")
                status_info["mapped_status"] = mapped_status
                
                if data.filled_quantity > 0 and data.filled_quantity < data.quantity:
                    status_info["is_partial"] = True
                    status_info["remaining_quantity"] = data.quantity - data.filled_quantity
                else:
                    status_info["is_partial"] = False
                
                with self._lock:
                    self._latest_orders[order_id] = status_info
                    self._order_fills[order_id] = {
                        "filled_qty": data.filled_quantity,
                        "avg_price": data.average_price,
                        "last_update": datetime.now().isoformat()
                    }
                
                return status_info
                
        except Exception as e:
            self.logger.error(f"REST order status failed for {order_id}: {e}")
        
        return None
    
    async def _fetch_order_status_async(self, order_id: str) -> Optional[Dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._fetch_order_status_sync, order_id
        )
    
    def get_order_fills(self, order_id: str) -> Optional[Dict]:
        with self._lock:
            return self._order_fills.get(order_id)
    
    def _on_sdk_open(self):
        self.is_connected = True
        self.logger.info("PortfolioDataStreamer connected")
        self._dispatch("open", {"status": "connected"})
    
    def _on_sdk_close(self, *args, **kwargs):
        self.is_connected = False
        self.logger.info("PortfolioDataStreamer disconnected")
        self._dispatch("close", {"status": "disconnected"})
    
    def _on_sdk_message(self, message):
        try:
            if hasattr(message, 'update_type') and message.update_type == "order":
                order_data = message
                
                order_id = getattr(order_data, 'order_id', None)
                if order_id:
                    status_info = {
                        "order_id": order_id,
                        "status": getattr(order_data, 'status', 'UNKNOWN'),
                        "filled_quantity": getattr(order_data, 'filled_quantity', 0),
                        "average_price": getattr(order_data, 'average_price', 0),
                        "pending_quantity": getattr(order_data, 'pending_quantity', 0),
                        "timestamp": datetime.now().isoformat(),
                        "source": "websocket"
                    }
                    
                    mapped_status = self.UPSTOX_STATUS_MAP.get(status_info["status"], "UNKNOWN")
                    status_info["mapped_status"] = mapped_status
                    
                    if (status_info["filled_quantity"] > 0 and 
                        status_info["filled_quantity"] < getattr(order_data, 'quantity', 0)):
                        status_info["is_partial"] = True
                    else:
                        status_info["is_partial"] = False
                    
                    with self._lock:
                        self._latest_orders[order_id] = status_info
                        
                        if status_info["filled_quantity"] > 0:
                            self._order_fills[order_id] = {
                                "filled_qty": status_info["filled_quantity"],
                                "avg_price": status_info["average_price"],
                                "last_update": status_info["timestamp"]
                            }
            
            self._dispatch("message", message)
            
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
# UPSTOX FETCHER - WITH CONNECTION POOLING
# ============================================================================

import socket
import urllib3
from concurrent.futures import ThreadPoolExecutor

class UpstoxFetcher:
    def __init__(self, token: str):
        if not token:
            raise ValueError("Upstox access token is required!")
        
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = token
        self.configuration.host = "https://api.upstox.com"
        self.configuration.discard_unknown_keys = True
        
        self.api_client = upstox_client.ApiClient(self.configuration)
        
        pool_manager = urllib3.PoolManager(
            num_pools=10,
            maxsize=10,
            block=False,
            socket_options=[
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60),
                (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10),
                (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6),
            ]
        )
        self.api_client.rest_client.pool_manager = pool_manager
        
        socket.setdefaulttimeout(10)
        
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
        
        self.market_streamer = VolGuardMarketStreamer(self.api_client)
        self.portfolio_streamer = VolGuardPortfolioStreamer(self.api_client, self)
        
        self.smart_fetcher = SmartDataFetcher(self)
        
        self.fill_tracker = FillQualityTracker()
        
        self._request_timestamps = []
        self._rate_limit_lock = threading.RLock()
        self._rate_limit_condition = threading.Condition(self._rate_limit_lock)
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="fetcher")
        
        self._lot_size_cache: Dict[str, int] = {}
        self._lot_size_cache_date: Optional[date] = None
        
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("✅ UpstoxFetcher initialized")
    
    def _check_rate_limit(self, max_requests: int = 50, window_seconds: int = 1):
        with self._rate_limit_condition:
            now = time.time()
            self._request_timestamps = [t for t in self._request_timestamps 
                                      if now - t < window_seconds]
            
            while len(self._request_timestamps) >= max_requests:
                sleep_time = self._request_timestamps[0] + window_seconds - now
                if sleep_time > 0:
                    self.logger.warning(f"Rate limit reached, waiting {sleep_time:.2f}s")
                    self._rate_limit_condition.wait(timeout=sleep_time)
                    now = time.time()
                    self._request_timestamps = [t for t in self._request_timestamps 
                                              if now - t < window_seconds]
            
            self._request_timestamps.append(now)
            self._rate_limit_condition.notify_all()
    
    def get_market_status(self) -> Dict:
        return self.smart_fetcher.get_market_status()
    
    def get_ltp_with_fallback(self, instrument_key: str) -> Optional[float]:
        return self.smart_fetcher.get_ltp(instrument_key)
    
    def get_bulk_ltp_with_fallback(self, instrument_keys: List[str]) -> Dict[str, Optional[float]]:
        return self.smart_fetcher.get_bulk_ltp(instrument_keys)
    
    def get_ohlc_with_fallback(self, instrument_key: str, interval: str = "1d") -> Optional[Dict]:
        return self.smart_fetcher.get_ohlc(instrument_key, interval)
    
    def get_full_quote_with_fallback(self, instrument_key: str) -> Optional[Dict]:
        return self.smart_fetcher.get_full_quote(instrument_key)
    
    def get_lot_size_for_expiry(self, expiry_date: date) -> int:
        today = date.today()
        
        if self._lot_size_cache_date != today:
            self._lot_size_cache = {}
            self._lot_size_cache_date = today
        
        expiry_str = expiry_date.strftime("%Y-%m-%d")
        
        if expiry_str in self._lot_size_cache:
            return self._lot_size_cache[expiry_str]
        
        try:
            self._check_rate_limit()
            response = self.options_api.get_option_contracts(
                instrument_key=SystemConfig.NIFTY_KEY
            )
            
            if response.status == "success" and response.data:
                for contract in response.data:
                    if hasattr(contract, 'expiry') and contract.expiry:
                        if isinstance(contract.expiry, str):
                            contract_expiry = datetime.strptime(contract.expiry, "%Y-%m-%d").date()
                        elif isinstance(contract.expiry, datetime):
                            contract_expiry = contract.expiry.date()
                        else:
                            continue
                            
                        if contract_expiry == expiry_date:
                            if hasattr(contract, 'lot_size'):
                                lot_size = contract.lot_size
                                self._lot_size_cache[expiry_str] = lot_size
                                self.logger.info(f"Lot size for {expiry_str}: {lot_size}")
                                return lot_size
            error_msg = f"API succeeded but no lot size found for expiry {expiry_str}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)
            
        except Exception as e:
            error_msg = f"Failed to fetch lot size for {expiry_str}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)
    
    def start_market_streamer(self, instrument_keys: List[str], mode: str = "ltpc"):
        self.market_streamer.connect(instrument_keys, mode)
        return self.market_streamer
    
    def start_portfolio_streamer(self, 
                                order_update: bool = True,
                                position_update: bool = True,
                                holding_update: bool = True,
                                gtt_update: bool = True):
        self.portfolio_streamer.connect(
            order_update=order_update,
            position_update=position_update,
            holding_update=holding_update,
            gtt_update=gtt_update
        )
        return self.portfolio_streamer
    
    def subscribe_market_data(self, instrument_keys: List[str], mode: str = "ltpc"):
        return self.market_streamer.subscribe(instrument_keys, mode)
    
    def unsubscribe_market_data(self, instrument_keys: List[str]):
        return self.market_streamer.unsubscribe(instrument_keys)
    
    def get_ltp(self, instrument_key: str) -> Optional[float]:
        if not self.market_streamer.is_connected:
            return None
        return self.market_streamer.get_ltp(instrument_key)
    
    def get_bulk_ltp(self, instrument_keys: List[str]) -> Dict[str, Optional[float]]:
        if not self.market_streamer.is_connected:
            return {}
        return self.market_streamer.get_bulk_ltp(instrument_keys)
    
    def get_live_positions(self) -> Optional[List[Dict]]:
        try:
            self._check_rate_limit()
            response = self.portfolio_api.get_positions(api_version="2.0")
            
            if response.status == "success" and response.data:
                positions = []
                for pos in response.data:
                    positions.append({
                        "instrument_token": pos.instrument_token,
                        "quantity": pos.quantity,
                        "buy_price": pos.average_price,
                        "current_price": pos.last_price,
                        "pnl": pos.pnl,
                        "unrealised": pos.unrealised if hasattr(pos, 'unrealised') else None,
                        "product": pos.product,
                        "symbol": pos.trading_symbol if hasattr(pos, 'trading_symbol') else pos.instrument_token.split('|')[-1]
                    })
                return positions
            
        except Exception as e:
            self.logger.error(f"Portfolio fetch error: {e}")
        
        return []
    
    def reconcile_positions_with_db(self, db: Session) -> Dict:
        try:
            db_trades = db.query(TradeJournal).filter(
                TradeJournal.status == TradeStatus.ACTIVE.value
            ).all()
            
            db_instruments = set()
            db_quantities = {}
            
            for trade in db_trades:
                legs = json.loads(trade.legs_data)
                for leg in legs:
                    instrument = leg['instrument_token']
                    db_instruments.add(instrument)
                    qty = leg['quantity']
                    if leg['action'] == 'SELL':
                        qty = -qty
                    db_quantities[instrument] = db_quantities.get(instrument, 0) + qty
            
            broker_positions = self.get_live_positions()
            broker_instruments = {p['instrument_token']: p['quantity'] for p in broker_positions}
            
            discrepancies = []
            for instrument in set(db_instruments) | set(broker_instruments.keys()):
                db_qty = db_quantities.get(instrument, 0)
                broker_qty = broker_instruments.get(instrument, 0)
                if db_qty != broker_qty:
                    discrepancies.append({
                        "instrument": instrument,
                        "db_qty": db_qty,
                        "broker_qty": broker_qty,
                        "diff": db_qty - broker_qty
                    })
            
            reconciled = len(discrepancies) == 0
            
            return {
                "timestamp": datetime.now().isoformat(),
                "db_positions": len(db_instruments),
                "broker_positions": len(broker_instruments),
                "matched": len(set(db_instruments).intersection(set(broker_instruments.keys()))),
                "discrepancies": discrepancies,
                "reconciled": reconciled
            }
            
        except Exception as e:
            self.logger.error(f"Position reconciliation error: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "reconciled": False
            }
    
    def validate_margin_for_strategy(self, legs: List[OptionLeg]) -> Tuple[bool, float, float]:
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
            
            body = upstox_client.MarginRequest(instruments=instruments)
            response = self.charge_api.post_margin(body)
            
            if response.status == "success" and response.data:
                required_margin = float(response.data.required_margin)
                available_margin = self.get_funds() or 0.0
                
                has_sufficient = available_margin >= required_margin
                
                margin_details = ""
                if hasattr(response.data, 'margins') and response.data.margins:
                    m = response.data.margins[0]
                    span = getattr(m, 'span_margin', 0)
                    exp = getattr(m, 'exposure_margin', 0)
                    margin_details = f" [Span: ₹{span:,.2f}, Exposure: ₹{exp:,.2f}]"
                
                self.logger.info(
                    f"Margin Check: Required=₹{required_margin:,.2f}{margin_details}, "
                    f"Available=₹{available_margin:,.2f}, "
                    f"Sufficient={has_sufficient}"
                )
                
                return has_sufficient, required_margin, available_margin
            
        except Exception as e:
            self.logger.error(f"Margin validation error: {e}")
        
        return False, 0.0, 0.0
    
    def get_broker_pnl_for_date(self, target_date: date) -> Optional[float]:
        try:
            self._check_rate_limit()
            
            date_str = target_date.strftime("%Y-%m-%d")
            segment = "FO"
            
            if target_date.month >= 4:
                fy = f"{str(target_date.year)[2:]}{str(target_date.year + 1)[2:]}"
            else:
                fy = f"{str(target_date.year - 1)[2:]}{str(target_date.year)[2:]}"
            
            response = self.pnl_api.get_trade_wise_profit_and_loss_data(
                segment=segment,
                financial_year=fy,
                page_number=1,
                page_size=100,
                from_date=date_str,
                to_date=date_str,
                api_version="2.0"
            )
            
            if response.status == "success" and response.data:
                total_pnl = sum([trade.realised_profit for trade in response.data])
                self.logger.info(f"Broker P&L for {date_str}: ₹{total_pnl:,.2f}")
                return total_pnl
            
        except Exception as e:
            self.logger.error(f"Broker P&L fetch error: {e}")
        
        return None
    
    def is_trading_day(self) -> bool:
        try:
            self._check_rate_limit()
            response = self.market_api.get_market_status(exchange="NSE")
            
            if response.status == "success" and response.data:
                status = getattr(response.data, 'status', '')
                return "open" in status.lower()
            
        except Exception as e:
            self.logger.error(f"Trading day check error: {e}")
        
        return True
    
    def get_market_status_detailed(self) -> str:
        try:
            self._check_rate_limit()
            response = self.market_api.get_market_status(exchange="NSE")
            
            if response.status == "success" and response.data:
                return getattr(response.data, 'status', "UNKNOWN")
            
        except Exception as e:
            self.logger.error(f"Market status error: {e}")
        
        return "UNKNOWN"
    
    def is_market_open_now(self) -> bool:
        status = self.get_market_status_detailed()
        return status == "NORMAL_OPEN"
    
    def get_market_holidays(self, days_ahead: int = 30) -> List[date]:
        try:
            self._check_rate_limit()
            response = self.market_api.get_holidays()
            
            if response.status == "success" and response.data:
                holidays = []
                today = date.today()
                cutoff = today + timedelta(days=days_ahead)
                
                for holiday in response.data:
                    if hasattr(holiday, 'date'):
                        holiday_date = datetime.strptime(
                            holiday.date, "%Y-%m-%d"
                        ).date()
                        if today <= holiday_date <= cutoff:
                            holidays.append(holiday_date)
                
                return sorted(holidays)
            
        except Exception as e:
            self.logger.error(f"Market holidays fetch error: {e}")
        
        return []
    
    def emergency_exit_all_positions(self) -> Dict:
        try:
            self._check_rate_limit()
            
            response = self.order_api.exit_positions(api_version="2.0")
            
            if response.status == "success":
                self.logger.info("Emergency exit all positions successful")
                return {
                    "success": True,
                    "message": "Emergency exit orders placed successfully via Upstox API",
                    "orders_placed": 1,
                    "order_ids": getattr(response.data, 'order_ids', []) if hasattr(response, 'data') else []
                }
            else:
                return {
                    "success": False,
                    "message": f"Emergency exit failed: {response}",
                    "orders_placed": 0
                }
                
        except Exception as e:
            self.logger.error(f"Emergency exit failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "orders_placed": 0
            }
    
    def get_funds(self) -> Optional[float]:
        try:
            self._check_rate_limit()
            response = self.user_api.get_user_fund_margin(api_version="2.0")
            if response.status == "success" and response.data:
                return float(response.data.equity.available_margin)
        except Exception as e:
            self.logger.error(f"Fund fetch error: {e}")
        return None
    
    def get_order_status(self, order_id: str) -> Optional[str]:
        try:
            self._check_rate_limit()
            response = self.order_api.get_order_status(api_version="2.0", order_id=order_id)
            if response.status == "success" and response.data:
                return response.data.status
        except Exception as e:
            self.logger.error(f"Order status fetch error: {e}")
        return None
    
    def get_order_details(self, order_id: str) -> Optional[Dict]:
        try:
            self._check_rate_limit()
            response = self.order_api.get_order_details(api_version="2.0", order_id=order_id)
            if response.status == "success" and response.data and len(response.data) > 0:
                data = response.data[0]
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
                    "transaction_type": data.transaction_type,
                    "pending_quantity": data.pending_quantity,
                    "status_message_raw": getattr(data, 'status_message_raw', None),
                    "exchange_order_id": getattr(data, 'exchange_order_id', None),
                    "order_ref_id": getattr(data, 'order_ref_id', None),
                    "variety": getattr(data, 'variety', None)
                }
        except Exception as e:
            self.logger.error(f"Order details fetch error: {e}")
        return None
    
    def history(self, key: str, days: int = 400) -> Optional[pd.DataFrame]:
        try:
            self._check_rate_limit()
            to_date = date.today().strftime("%Y-%m-%d")
            from_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            response = self.history_api.get_historical_candle_data1(
                key,
                "days",
                "1",
                to_date,
                from_date
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
            
        except Exception as e:
            self.logger.error(f"History fetch error: {e}")
        
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
                
                expiry_dates = []
                weekly_expiries = []
                monthly_expiries = []
                
                for contract in data:
                    if hasattr(contract, 'expiry') and contract.expiry:
                        if isinstance(contract.expiry, str):
                            expiry_date = datetime.strptime(contract.expiry, "%Y-%m-%d").date()
                        elif isinstance(contract.expiry, datetime):
                            expiry_date = contract.expiry.date()
                        else:
                            continue
                        
                        expiry_dates.append(expiry_date)
                        
                        is_weekly = getattr(contract, 'weekly', False)
                        if is_weekly:
                            weekly_expiries.append(expiry_date)
                        else:
                            monthly_expiries.append(expiry_date)
                
                expiry_dates = sorted(list(set(expiry_dates)))
                weekly_expiries = sorted(list(set(weekly_expiries)))
                monthly_expiries = sorted(list(set(monthly_expiries)))
                
                valid_dates = [d for d in expiry_dates if d >= date.today()]
                if not valid_dates:
                    return None, None, None, lot_size, []
                
                weekly = weekly_expiries[0] if weekly_expiries else valid_dates[0]
                monthly = monthly_expiries[0] if monthly_expiries else valid_dates[-1]
                
                if len(weekly_expiries) > 1:
                    next_weekly = weekly_expiries[1]
                else:
                    next_weekly = monthly_expiries[0] if len(monthly_expiries) > 0 else weekly
                
                return weekly, monthly, next_weekly, lot_size, expiry_dates
                
        except Exception as e:
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
                            if sub_attr and hasattr(obj, attr):
                                parent = getattr(obj, attr)
                                return getattr(parent, sub_attr, 0) if parent else 0
                            return getattr(obj, attr, 0)
                        
                        call_pop = 0.0
                        put_pop = 0.0
                        
                        if call_opts and hasattr(call_opts, 'option_greeks'):
                            call_pop = getattr(call_opts.option_greeks, 'pop', 0) or 0
                        
                        if put_opts and hasattr(put_opts, 'option_greeks'):
                            put_pop = getattr(put_opts.option_greeks, 'pop', 0) or 0
                        
                        call_market = getattr(call_opts, 'market_data', None) if call_opts else None
                        put_market = getattr(put_opts, 'market_data', None) if put_opts else None
                        
                        rows.append({
                            'strike': item.strike_price,
                            'ce_instrument_key': get_val(call_opts, 'instrument_key'),
                            'ce_ltp': get_val(call_market, 'ltp') if call_market else 0,
                            'ce_bid': get_val(call_market, 'bid_price') if call_market else 0,
                            'ce_ask': get_val(call_market, 'ask_price') if call_market else 0,
                            'ce_oi': get_val(call_market, 'oi') if call_market else 0,
                            'ce_iv': get_val(call_opts, 'option_greeks', 'iv'),
                            'ce_delta': get_val(call_opts, 'option_greeks', 'delta'),
                            'ce_gamma': get_val(call_opts, 'option_greeks', 'gamma'),
                            'ce_theta': get_val(call_opts, 'option_greeks', 'theta'),
                            'ce_vega': get_val(call_opts, 'option_greeks', 'vega'),
                            'ce_pop': call_pop,
                            'pe_instrument_key': get_val(put_opts, 'instrument_key'),
                            'pe_ltp': get_val(put_market, 'ltp') if put_market else 0,
                            'pe_bid': get_val(put_market, 'bid_price') if put_market else 0,
                            'pe_ask': get_val(put_market, 'ask_price') if put_market else 0,
                            'pe_oi': get_val(put_market, 'oi') if put_market else 0,
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
                
        except Exception as e:
            self.logger.error(f"Chain fetch error: {e}")
        
        return None
    
    def get_greeks(self, instrument_keys: List[str]) -> Dict[str, Dict]:
        try:
            self._check_rate_limit()
            encoded_keys = [urllib.parse.quote(k, safe='') for k in instrument_keys]
            response = self.quote_api_v3.get_market_quote_option_greek(
                instrument_key=",".join(encoded_keys)
            )
            
            result = {}
            if response.status == "success" and response.data:
                for api_key, data in response.data.items():
                    actual_token = getattr(data, 'instrument_token', api_key)
                    
                    greeks_data = getattr(data, 'option_greeks', None) if hasattr(data, 'option_greeks') else None
                    
                    if greeks_data:
                        result[actual_token] = {
                            'iv': getattr(greeks_data, 'iv', 0) or 0,
                            'delta': getattr(greeks_data, 'delta', 0) or 0,
                            'gamma': getattr(greeks_data, 'gamma', 0) or 0,
                            'theta': getattr(greeks_data, 'theta', 0) or 0,
                            'vega': getattr(greeks_data, 'vega', 0) or 0,
                            'spot_price': 0
                        }
                    else:
                        result[actual_token] = {
                            'iv': 0,
                            'delta': 0,
                            'gamma': 0,
                            'theta': 0,
                            'vega': 0,
                            'spot_price': 0
                        }
            
            for key in result:
                spot = self.get_ltp_with_fallback(key)
                if spot:
                    result[key]['spot_price'] = spot
            
            return result
        
        except Exception as e:
            self.logger.error(f"Greeks fetch error: {e}")
            return {}
    
    def __del__(self):
        if hasattr(self, '_executor'):
            self._executor.shutdown(wait=False)


# ============================================================================
# UPSTOX ORDER EXECUTOR
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
        self.base_delay = 1.0
        self.algo_name = "VOLGUARD_V35"
    
    def _wait_for_fills(self, order_ids: List[str], expected_quantities: Dict[str, int] = None) -> Dict[str, Dict]:
        start_time = time.time()
        timeout = DynamicConfig.get("ORDER_FILL_TIMEOUT_SECONDS")
        check_interval = DynamicConfig.get("ORDER_FILL_CHECK_INTERVAL")
        
        filled_orders = {}
        
        while time.time() - start_time < timeout:
            for oid in order_ids:
                if oid in filled_orders:
                    continue
                
                status = self.fetcher.portfolio_streamer.get_order_status(oid)
                
                if status:
                    if status.get('mapped_status') in ['FILLED', 'PARTIAL']:
                        filled_qty = status.get('filled_quantity', 0)
                        
                        if status.get('mapped_status') == 'FILLED':
                            filled_orders[oid] = {
                                "filled": True,
                                "partial": False,
                                "filled_quantity": filled_qty,
                                "average_price": status.get('average_price', 0),
                                "status": status
                            }
                            self.logger.info(f"Order {oid} fully filled: {filled_qty} units")
                            
                        elif status.get('mapped_status') == 'PARTIAL':
                            filled_orders[oid] = {
                                "filled": False,
                                "partial": True,
                                "filled_quantity": filled_qty,
                                "average_price": status.get('average_price', 0),
                                "remaining": status.get('pending_quantity', 0),
                                "status": status
                            }
                            self.logger.info(f"Order {oid} partially filled: {filled_qty} units")
            
            if all(o.get('filled', False) for o in filled_orders.values()):
                break
                
            time.sleep(check_interval)
        
        unfilled = set(order_ids) - set(filled_orders.keys())
        if unfilled:
            self.logger.warning(f"Orders not filled within timeout: {unfilled}")
        
        return filled_orders
    
    def _get_fill_prices(self, order_ids: List[str]) -> Dict[str, float]:
        fill_prices = {}
        for order_id in order_ids:
            try:
                order_details = self.fetcher.get_order_details(order_id)
                if order_details and order_details.get('average_price', 0) > 0:
                    fill_prices[order_id] = order_details['average_price']
                else:
                    status = self.fetcher.portfolio_streamer.get_order_status(order_id)
                    if status and status.get('average_price', 0) > 0:
                        fill_prices[order_id] = status['average_price']
            except Exception as e:
                self.logger.error(f"Failed to fetch fill price for order {order_id}: {e}")
        return fill_prices
    
    def _place_gtt_stop_losses(self, strategy: ConstructedStrategy, filled_orders: Dict[str, Dict]) -> List[str]:
        """
        Place two SINGLE GTT orders per SELL leg:
          1. Stop-loss: SINGLE GTT with trigger_type=ABOVE  fires BUY when LTP rises above stop_price
          2. Target:    SINGLE GTT with trigger_type=BELOW  fires BUY when LTP drops below target_price

        IMPORTANT: Do NOT use MULTIPLE GTT here.  MULTIPLE GTT is designed for entering a NEW
        position (ENTRY leg fires immediately, then TARGET/STOPLOSS activate).  Our position is
        already open, so ENTRY=IMMEDIATE would place a second unintended BUY order on a live short.
        """
        gtt_ids = []

        for leg in strategy.legs:
            if leg.action != "SELL":
                continue

            stop_price  = round(leg.entry_price * DynamicConfig.get("GTT_STOP_LOSS_MULTIPLIER"), 2)
            target_price = round(leg.entry_price * DynamicConfig.get("GTT_PROFIT_TARGET_MULTIPLIER"), 2)
            actual_qty  = leg.filled_quantity if leg.filled_quantity > 0 else leg.quantity

            # ── 1. STOP-LOSS GTT: BUY to close when price rises ABOVE stop level ──────────
            try:
                sl_rule = upstox_client.GttRule(
                    strategy="ENTRY",
                    trigger_type="ABOVE",
                    trigger_price=stop_price
                )
                sl_body = upstox_client.GttPlaceOrderRequest(
                    type="SINGLE",
                    instrument_token=leg.instrument_token,
                    quantity=actual_qty,
                    product="D",
                    transaction_type="BUY",
                    rules=[sl_rule]
                )
                sl_response = self.fetcher.order_api_v3.place_gtt_order(body=sl_body)

                if sl_response.status == "success" and sl_response.data:
                    sl_ids = sl_response.data.gtt_order_ids if hasattr(sl_response.data, 'gtt_order_ids') else []
                    gtt_ids.extend(sl_ids)
                    self.logger.info(
                        f"[GTT-SL] Placed for {leg.strike} {leg.option_type} | "
                        f"Trigger ABOVE ₹{stop_price} | Qty: {actual_qty} | GTT IDs: {sl_ids}"
                    )
                else:
                    self.logger.error(f"[GTT-SL] Failed for {leg.strike}: {sl_response}")

            except Exception as e:
                self.logger.error(f"[GTT-SL] Exception for {leg.strike}: {e}")

            # ── 2. TARGET GTT: BUY to close when price falls BELOW target level ───────────
            try:
                tgt_rule = upstox_client.GttRule(
                    strategy="ENTRY",
                    trigger_type="BELOW",
                    trigger_price=target_price
                )
                tgt_body = upstox_client.GttPlaceOrderRequest(
                    type="SINGLE",
                    instrument_token=leg.instrument_token,
                    quantity=actual_qty,
                    product="D",
                    transaction_type="BUY",
                    rules=[tgt_rule]
                )
                tgt_response = self.fetcher.order_api_v3.place_gtt_order(body=tgt_body)

                if tgt_response.status == "success" and tgt_response.data:
                    tgt_ids = tgt_response.data.gtt_order_ids if hasattr(tgt_response.data, 'gtt_order_ids') else []
                    gtt_ids.extend(tgt_ids)
                    self.logger.info(
                        f"[GTT-TGT] Placed for {leg.strike} {leg.option_type} | "
                        f"Trigger BELOW ₹{target_price} | Qty: {actual_qty} | GTT IDs: {tgt_ids}"
                    )
                else:
                    self.logger.error(f"[GTT-TGT] Failed for {leg.strike}: {tgt_response}")

            except Exception as e:
                self.logger.error(f"[GTT-TGT] Exception for {leg.strike}: {e}")

        return gtt_ids
    
    def place_multi_order(self, strategy: ConstructedStrategy, retries_left: int = 3) -> Dict:
        has_margin, required, available = self.fetcher.validate_margin_for_strategy(strategy.legs)
        if not has_margin:
            msg = f"INSUFFICIENT MARGIN. Required: ₹{required:,.2f}, Available: ₹{available:,.2f}"
            self.logger.error(msg)
            return {
                "success": False,
                "order_ids": [],
                "message": msg
            }
        
        strategy.required_margin = required
        
        max_risk_per_trade = DynamicConfig.get("BASE_CAPITAL") * (DynamicConfig.get("MAX_POSITION_RISK_PCT") / 100)
        if strategy.max_loss > max_risk_per_trade:
            msg = f"POSITION RISK TOO HIGH. Max loss: ₹{strategy.max_loss:,.2f} > Limit: ₹{max_risk_per_trade:,.2f}"
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
        expected_quantities = {}
        correlation_map = {}
        
        for i, leg in enumerate(ordered_legs):
            correlation_id = f"{strategy.strategy_id[-8:]}_leg{i}_{uuid.uuid4().hex[:6]}"
            leg.correlation_id = correlation_id
            correlation_map[correlation_id] = leg
            
            if leg.action == "SELL" and leg.bid > 0:
                limit_price = leg.bid
            elif leg.action == "BUY" and leg.ask > 0:
                limit_price = leg.ask
            else:
                limit_price = leg.entry_price
            
            order = upstox_client.MultiOrderRequest(
                quantity=leg.quantity,
                product="D",
                validity="DAY",
                price=limit_price,
                tag=strategy.strategy_id[:40],
                instrument_token=leg.instrument_token,
                order_type=self.ORDER_TYPE_MAP.get("LIMIT", "LIMIT"),
                transaction_type=leg.action,
                disclosed_quantity=0,
                trigger_price=0.0,
                is_amo=False,
                slice=True,
                correlation_id=correlation_id
            )
            orders.append(order)
            expected_quantities[correlation_id] = leg.quantity
        
        try:
            self.fetcher._check_rate_limit(max_requests=4)
            response = self.fetcher.order_api.place_multi_order(body=orders)
            
            if response.status in ["success", "partial_success"]:
                order_ids = []
                response_correlation_map = {}
                
                if hasattr(response, 'data') and response.data:
                    for item in response.data:
                        order_ids.append(item.order_id)
                        if hasattr(item, 'correlation_id'):
                            response_correlation_map[item.correlation_id] = item.order_id
                
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
                
                filled_orders = self._wait_for_fills(order_ids)
                fill_prices = self._get_fill_prices(order_ids)
                
                for correlation_id, leg in correlation_map.items():
                    if correlation_id in response_correlation_map:
                        order_id = response_correlation_map[correlation_id]
                        if order_id in filled_orders:
                            leg.filled_quantity = filled_orders[order_id].get('filled_quantity', 0)
                            leg.order_id = order_id
                            if order_id in fill_prices:
                                leg.fill_price = fill_prices[order_id]
                                leg.fill_time = datetime.now()
                
                instrument_keys = [leg.instrument_token for leg in strategy.legs if leg.filled_quantity > 0]
                greeks_snapshot = self.fetcher.get_greeks(instrument_keys) if instrument_keys else {}
                
                gtt_ids = []
                if filled_orders:
                    gtt_ids = self._place_gtt_stop_losses(strategy, filled_orders)
                
                for correlation_id, leg in correlation_map.items():
                    if correlation_id in response_correlation_map and leg.fill_price:
                        self.fetcher.fill_tracker.record_fill(
                            order_id=leg.order_id,
                            instrument=leg.instrument_token,
                            limit_price=leg.entry_price,
                            fill_price=leg.fill_price,
                            order_time=leg.construction_time,
                            fill_time=leg.fill_time or datetime.now(),
                            filled_qty=leg.filled_quantity,
                            requested_qty=leg.quantity,
                            partial=(leg.filled_quantity < leg.quantity)
                        )
                
                return {
                    "success": True,
                    "order_ids": order_ids,
                    "filled_orders": filled_orders,
                    "fill_prices": fill_prices,
                    "gtt_order_ids": gtt_ids,
                    "entry_greeks": greeks_snapshot,
                    "errors": errors,
                    "message": f"Strategy executed. Orders: {len(order_ids)}, Fully filled: {len([o for o in filled_orders.values() if o.get('filled')])}, Partially filled: {len([o for o in filled_orders.values() if o.get('partial')])}, GTTs: {len(gtt_ids)}"
                }
            
            return {"success": False, "order_ids": [], "message": f"Failed: {response.status}"}
            
        except Exception as e:
            self.logger.error(f"Multi-order failed: {e}")
            
            if retries_left > 0:
                delay = self.base_delay * (2 ** (3 - retries_left))
                self.logger.info(f"Retrying in {delay:.1f}s... (retries left: {retries_left})")
                time.sleep(delay)
                return self.place_multi_order(strategy, retries_left - 1)
            
            return {
                "success": False,
                "order_ids": [],
                "message": f"Exception: {str(e)}"
            }
    
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
        exit_success = True
        order_errors = []
        
        for leg in legs_data:
            try:
                qty = leg.get('filled_quantity', leg['quantity'])
                if qty == 0:
                    continue
                    
                transaction_type = "BUY" if leg['action'] == 'SELL' else "SELL"
                
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
                    
                    self.logger.info(f"Exit order placed for {leg['instrument_token']} (qty: {qty})")
                else:
                    exit_success = False
                    order_errors.append(f"Exit order failed for {leg['instrument_token']}: {response}")
                    
            except Exception as e:
                exit_success = False
                order_errors.append(f"Exit order exception for {leg['instrument_token']}: {e}")
        
        if not exit_success:
            error_msg = "; ".join(order_errors)
            self.logger.error(f"Exit orders failed for trade {trade.strategy_id}: {error_msg}")
            
            if self.fetcher.alert_service:
                self.fetcher.alert_service.send(
                    "Exit Order Failure",
                    f"Trade {trade.strategy_id} exit failed. Errors: {error_msg}\nDB status NOT changed.",
                    AlertPriority.CRITICAL,
                    throttle_key=f"exit_failure_{trade.strategy_id}"
                )
            
            return {
                "success": False,
                "orders_placed": orders_placed,
                "realized_pnl": 0.0,
                "exit_reason": exit_reason,
                "errors": order_errors
            }
        
        exit_order_ids = orders_placed
        filled_exits = self._wait_for_fills(exit_order_ids)
        exit_fill_prices = self._get_fill_prices(exit_order_ids)
        
        realized_pnl = 0.0
        pnl_approximate = False
        fill_prices_map = {}
        
        for leg in legs_data:
            instrument_key = leg['instrument_token']
            qty = leg.get('filled_quantity', leg['quantity'])
            multiplier = -1 if leg['action'] == 'SELL' else 1
            
            leg_fill_price = None
            for order_id in exit_order_ids:
                if order_id in exit_fill_prices:
                    leg_fill_price = exit_fill_prices[order_id]
                    break
            
            if leg_fill_price is not None and leg_fill_price > 0:
                exit_price = leg_fill_price
                fill_prices_map[instrument_key] = leg_fill_price
            else:
                exit_price = current_prices.get(instrument_key, leg['entry_price'])
                pnl_approximate = True
                self.logger.warning(f"Missing fill price for {instrument_key} - using LTP for P&L")
            
            leg_pnl = (exit_price - leg['entry_price']) * qty * multiplier
            realized_pnl += leg_pnl
        
        trade.exit_time = datetime.now()
        trade.status = exit_reason
        trade.exit_reason = exit_reason
        trade.realized_pnl = realized_pnl
        trade.pnl_approximate = pnl_approximate
        trade.fill_prices = json.dumps(fill_prices_map) if fill_prices_map else None
        
        db.commit()
        
        pnl_message = f"P&L=₹{realized_pnl:.2f}"
        if pnl_approximate:
            pnl_message += " (approximate - using LTP, not fill prices)"
        
        self.logger.info(
            f"Trade {trade.strategy_id} closed: {pnl_message}, Reason={exit_reason}"
        )
        
        return {
            "success": True,
            "orders_placed": orders_placed,
            "realized_pnl": realized_pnl,
            "pnl_approximate": pnl_approximate,
            "fill_prices": fill_prices_map,
            "exit_reason": exit_reason
        }


class MockExecutor:
    def __init__(self, fetcher: Optional[UpstoxFetcher] = None):
        self.fetcher = fetcher
        self.logger = logging.getLogger(self.__class__.__name__)
        self.order_counter = 1000
    
    def place_multi_order(self, strategy: ConstructedStrategy) -> Dict:
        order_ids = []
        gtt_ids = []
        
        spot_price = 22000.0
        if self.fetcher:
            real_spot = self.fetcher.get_ltp_with_fallback(SystemConfig.NIFTY_KEY)
            if real_spot:
                spot_price = real_spot
        
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
            moneyness = (spot_price - leg.strike) / leg.strike if leg.strike > 0 else 0
            
            if leg.option_type == "CE":
                delta = 0.5 + moneyness * 2
                delta = max(0.01, min(0.99, delta))
                iv = 15.0 + abs(moneyness) * 10
            else:
                delta = -0.5 + moneyness * 2
                delta = max(-0.99, min(-0.01, delta))
                iv = 15.0 + abs(moneyness) * 10
            
            mock_greeks[leg.instrument_token] = {
                'iv': round(iv, 2),
                'delta': round(delta, 2),
                'gamma': 0.05,
                'theta': round(-iv / 10, 2),
                'vega': round(iv * 2, 2),
                'spot_price': round(spot_price, 2)
            }
        
        return {
            "success": True,
            "order_ids": order_ids,
            "filled_orders": {oid: {"filled": True, "filled_quantity": leg.quantity} 
                            for oid, leg in zip(order_ids, strategy.legs)},
            "gtt_order_ids": gtt_ids,
            "entry_greeks": mock_greeks,
            "message": "Mock orders placed successfully"
        }


# ============================================================================
# ANALYTICS ENGINE - CORRECTED WITH 70/15/15 WEIGHTS AND ±5% WINDOW
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
            is_expiry_day_weekly=SystemConfig.is_expiry_day(weekly, all_expiries),
            is_expiry_day_monthly=SystemConfig.is_expiry_day(monthly, all_expiries),
            is_expiry_day_next_weekly=SystemConfig.is_expiry_day(next_weekly, all_expiries),
            is_past_square_off_time=is_past_square_off
        )
    
    def get_vol_metrics(self, nifty_hist: pd.DataFrame, vix_hist: pd.DataFrame, 
                       spot_live: float, vix_live: float) -> VolMetrics:
        is_fallback = False
        spot = spot_live if spot_live > 0 else (nifty_hist.iloc[-1]['close'] if nifty_hist is not None and not nifty_hist.empty else 0)
        vix = vix_live if vix_live > 0 else (vix_hist.iloc[-1]['close'] if vix_hist is not None and not vix_hist.empty else 0)
        
        spot = round(spot, 2) if spot else 0
        vix = round(vix, 2) if vix else 0
        
        if spot_live <= 0 or vix_live <= 0:
            is_fallback = True
        
        if nifty_hist is None or nifty_hist.empty:
            return self._fallback_vol_metrics(spot, vix, is_fallback)
        
        returns = np.log(nifty_hist['close'] / nifty_hist['close'].shift(1)).dropna()
        
        rv7 = round(returns.rolling(7).std(ddof=1).iloc[-1] * np.sqrt(252) * 100, 2) if len(returns) >= 7 else 0
        rv28 = round(returns.rolling(28).std(ddof=1).iloc[-1] * np.sqrt(252) * 100, 2) if len(returns) >= 28 else 0
        rv90 = round(returns.rolling(90).std(ddof=1).iloc[-1] * np.sqrt(252) * 100, 2) if len(returns) >= 90 else 0
        
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
                return round(np.sqrt(avg_variance * 252), 2)
            except:
                return 0
        
        garch7 = fit_garch(7) or rv7
        garch28 = fit_garch(28) or rv28
        
        const = 1.0 / (4.0 * np.log(2.0))
        park7 = round(np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(7).mean() * const) * np.sqrt(252) * 100, 2)
        park28 = round(np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(28).mean() * const) * np.sqrt(252) * 100, 2)
        
        if vix_hist is not None and not vix_hist.empty:
            vix_returns = np.log(vix_hist['close'] / vix_hist['close'].shift(1)).dropna()
            vix_vol_30d = vix_returns.rolling(30).std(ddof=1) * np.sqrt(252) * 100
            vov = round(vix_vol_30d.shift(1).iloc[-1], 2) if len(vix_vol_30d) > 1 else 0
            if len(vix_vol_30d) >= 60:
                vov_mean = vix_vol_30d.shift(1).rolling(60).mean().iloc[-1]
                vov_std = vix_vol_30d.shift(1).rolling(60).std(ddof=1).iloc[-1]
                vov_zscore = round((vov - vov_mean) / vov_std, 2) if vov_std > 0 else 0
            else:
                vov_mean, vov_std, vov_zscore = 0, 0, 0
        else:
            vov, vov_mean, vov_std, vov_zscore = 0, 0, 0, 0
        
        def calc_ivp(window: int) -> float:
            if vix_hist is None or len(vix_hist) < window:
                return 0.0
            history = vix_hist['close'].tail(window)
            raw_value = (history < vix).mean() * 100
            return round(raw_value, 1)
        
        ivp_30d = calc_ivp(30)
        ivp_90d = calc_ivp(90)
        ivp_1yr = calc_ivp(252)
        
        ma20 = round(nifty_hist['close'].rolling(20).mean().iloc[-1], 2) if len(nifty_hist) >= 20 else round(spot, 2)
        
        high_low = nifty_hist['high'] - nifty_hist['low']
        high_close = (nifty_hist['high'] - nifty_hist['close'].shift(1)).abs()
        low_close = (nifty_hist['low'] - nifty_hist['close'].shift(1)).abs()
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr14 = round(true_range.rolling(14).mean().iloc[-1], 2) if len(true_range) >= 14 else 0
        
        trend_strength = round(abs(spot - ma20) / atr14, 2) if atr14 > 0 else 0
        
        vix_5d_ago = vix_hist['close'].iloc[-6] if vix_hist is not None and len(vix_hist) >= 6 else vix
        vix_change_5d = round(((vix / vix_5d_ago) - 1) * 100, 2) if vix_5d_ago > 0 else 0
        
        if vix_change_5d > DynamicConfig.get("VIX_MOMENTUM_BREAKOUT"):
            vix_momentum = "RISING"
        elif vix_change_5d < -DynamicConfig.get("VIX_MOMENTUM_BREAKOUT"):
            vix_momentum = "FALLING"
        else:
            vix_momentum = "STABLE"
        
        if vov_zscore > DynamicConfig.get("VOV_CRASH_ZSCORE"):
            vol_regime = "EXPLODING"
        elif ivp_1yr > DynamicConfig.get("HIGH_VOL_IVP") and vix_momentum == "FALLING":
            vol_regime = "MEAN_REVERTING"
        elif ivp_1yr > DynamicConfig.get("HIGH_VOL_IVP") and vix_momentum == "RISING":
            vol_regime = "BREAKOUT_RICH"
        elif ivp_1yr > DynamicConfig.get("HIGH_VOL_IVP"):
            vol_regime = "RICH"
        elif ivp_1yr < DynamicConfig.get("LOW_VOL_IVP"):
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
        if chain is None or chain.empty or spot == 0:
            return self._fallback_struct_metrics(lot_size)
        
        chain['proximity_weight'] = np.exp(-((chain['strike'] - spot) / spot) ** 2 / 0.02)
        
        chain['call_gex'] = chain['ce_gamma'] * chain['ce_oi'] * chain['proximity_weight'] * spot * lot_size * 0.01
        chain['put_gex'] = -chain['pe_gamma'] * chain['pe_oi'] * chain['proximity_weight'] * spot * lot_size * 0.01
        net_gex = (chain['call_gex'] + chain['put_gex']).sum()
        gex_weighted = round(net_gex / 1_000_000, 2)
        
        total_oi_sum = (chain['ce_oi'].sum() + chain['pe_oi'].sum())
        total_notional_oi_value = total_oi_sum * spot * lot_size
        
        if total_notional_oi_value > 0:
            gex_ratio_pct = round((abs(gex_weighted) / total_notional_oi_value) * 100, 6)
        else:
            gex_ratio_pct = 0
        
        sticky_threshold = DynamicConfig.get("GEX_STICKY_RATIO") or 0.03
        if gex_ratio_pct > sticky_threshold:
            gex_regime = "STICKY"
        elif gex_ratio_pct < (sticky_threshold * 0.5):
            gex_regime = "SLIPPERY"
        else:
            gex_regime = "NEUTRAL"
        
        total_ce_oi = chain['ce_oi'].sum()
        total_pe_oi = chain['pe_oi'].sum()
        pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi > 0 else 1.0
        
        atm_chain = chain[(chain['strike'] >= spot * 0.95) & (chain['strike'] <= spot * 1.05)]
        
        if not atm_chain.empty and atm_chain['ce_oi'].sum() > 0:
            atm_ce_oi = atm_chain['ce_oi'].sum()
            atm_pe_oi = atm_chain['pe_oi'].sum()
            pcr_atm = round(atm_pe_oi / atm_ce_oi, 2)
        else:
            pcr_atm = 1.0
        
        strikes = chain['strike'].values
        losses = []
        for s in strikes:
            call_pain = np.sum(np.maximum(0, s - strikes) * chain['ce_oi'].values)
            put_pain = np.sum(np.maximum(0, strikes - s) * chain['pe_oi'].values)
            losses.append(call_pain + put_pain)
        
        max_pain = strikes[np.argmin(losses)] if strikes.size > 0 else spot
        
        try:
            call_25d = chain.iloc[(chain['ce_delta'] - 0.25).abs().argsort()[:1]]
            put_25d = chain.iloc[(chain['pe_delta'] + 0.25).abs().argsort()[:1]]
            skew_25d = round(put_25d.iloc[0]['pe_iv'] - call_25d.iloc[0]['ce_iv'], 2)
        except:
            skew_25d = 0
        
        if skew_25d > 5:
            skew_regime = "CRASH_FEAR"
        elif skew_25d < -2:
            skew_regime = "MELT_UP"
        else:
            skew_regime = "BALANCED"
        
        if pcr_atm > 1.2:
            oi_regime = "BULLISH"
        elif pcr_atm < 0.8:
            oi_regime = "BEARISH"
        else:
            oi_regime = "NEUTRAL"
        
        return StructMetrics(
            net_gex=net_gex,
            gex_ratio=gex_ratio_pct,
            total_oi_value=total_notional_oi_value,
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
            idx = (chain['strike'] - spot).abs().argsort()[:1]
            return round((chain.iloc[idx].iloc[0]['ce_iv'] + chain.iloc[idx].iloc[0]['pe_iv']) / 2, 2)
        
        iv_weekly = get_iv(weekly_chain)
        iv_monthly = get_iv(monthly_chain)
        iv_next_weekly = get_iv(next_weekly_chain)
        
        vrp_rv_weekly = round(iv_weekly - vol_metrics.rv7, 2)
        vrp_garch_weekly = round(iv_weekly - vol_metrics.garch7, 2)
        vrp_park_weekly = round(iv_weekly - vol_metrics.park7, 2)
        
        vrp_rv_monthly = round(iv_monthly - vol_metrics.rv28, 2)
        vrp_garch_monthly = round(iv_monthly - vol_metrics.garch28, 2)
        vrp_park_monthly = round(iv_monthly - vol_metrics.park28, 2)
        
        vrp_rv_next_weekly = round(iv_next_weekly - vol_metrics.rv7, 2)
        vrp_garch_next_weekly = round(iv_next_weekly - vol_metrics.garch7, 2)
        vrp_park_next_weekly = round(iv_next_weekly - vol_metrics.park7, 2)
        
        weighted_vrp_weekly = round((vrp_garch_weekly * 0.70 + vrp_park_weekly * 0.15 + vrp_rv_weekly * 0.15), 2)
        weighted_vrp_monthly = round((vrp_garch_monthly * 0.70 + vrp_park_monthly * 0.15 + vrp_rv_monthly * 0.15), 2)
        weighted_vrp_next_weekly = round((vrp_garch_next_weekly * 0.70 + vrp_park_next_weekly * 0.15 + vrp_rv_next_weekly * 0.15), 2)
        
        expiry_risk_discount_weekly = 0.2 if is_expiry_day else 0.0
        expiry_risk_discount_monthly = 0.0
        expiry_risk_discount_next_weekly = 0.0
        
        if iv_weekly > 0 and iv_monthly > 0:
            term_structure_slope = round(iv_monthly - iv_weekly, 2)
        else:
            term_structure_slope = 0
        
        if term_structure_slope < -1:
            term_structure_regime = "BACKWARDATION"
        elif term_structure_slope > 1:
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
            term_structure_regime=term_structure_regime,
            weighted_vrp_weekly=weighted_vrp_weekly,
            weighted_vrp_monthly=weighted_vrp_monthly,
            weighted_vrp_next_weekly=weighted_vrp_next_weekly
        )


# ============================================================================
# REGIME ENGINE - WITH PROPER DRIVERS APPENDED
# ============================================================================

class RegimeEngine:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def calculate_dynamic_weights(self, vol_metrics: VolMetrics, external_metrics: ExternalMetrics, dte: int) -> DynamicWeights:
        vol_weight = 0.40
        struct_weight = 0.30
        edge_weight = 0.30
        rationale = "Base: 40% Vol, 30% Struct, 30% Edge"
        
        if vol_metrics.vov_zscore > 2.0 or vol_metrics.vix_momentum == "RISING":
            vol_weight += 0.10
            struct_weight -= 0.05
            edge_weight -= 0.05
            rationale = "High Vol Environment: Vol↑ to 50%, Struct/Edge↓"
        elif vol_metrics.ivp_1yr < 25.0:
            edge_weight += 0.10
            vol_weight -= 0.10
            rationale = "Low Vol Environment: Edge↑ to 40%, Vol↓ to 30%"
        
        if dte <= 2:
            struct_weight += 0.10
            vol_weight -= 0.05
            edge_weight -= 0.05
            rationale += " | Low DTE: Struct↑ (Gamma dominant)"
        
        return DynamicWeights(vol_weight, struct_weight, edge_weight, rationale)
    
    def calculate_scores(self, vol_metrics: VolMetrics, struct_metrics: StructMetrics,
                        edge_metrics: EdgeMetrics, external_metrics: ExternalMetrics,
                        expiry_type: str, dte: int) -> RegimeScore:
        
        drivers = []
        
        if expiry_type == "WEEKLY":
            weighted_vrp = edge_metrics.weighted_vrp_weekly
        elif expiry_type == "NEXT_WEEKLY":
            weighted_vrp = edge_metrics.weighted_vrp_next_weekly
        else:
            weighted_vrp = edge_metrics.weighted_vrp_monthly
        
        edge_score = 5.0
        if weighted_vrp > 2.0:
            edge_score += 2.0
            drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Good) +2.0")
        elif weighted_vrp < 0:
            edge_score -= 3.0
            drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Neg) -3.0")
        
        vol_score = 5.0
        if vol_metrics.vov_zscore > 2.5:
            vol_score = 0.0
            drivers.append(f"Vol: VOV Crash ({vol_metrics.vov_zscore:.1f}σ) → ZERO")
        elif vol_metrics.ivp_1yr > 75:
            vol_score += 1.0
            drivers.append(f"Vol: Rich IVP +1.0")
        
        struct_score = 5.0
        if struct_metrics.gex_regime == "STICKY":
            struct_score += 2.5
            drivers.append(f"Struct: Sticky GEX +2.5")
        elif struct_metrics.gex_regime == "SLIPPERY":
            struct_score -= 1.0
            drivers.append(f"Struct: Slippery GEX -1.0")
        
        if struct_metrics.pcr_atm > 1.2:
            struct_score += 1.0
            drivers.append(f"Struct: PCR ATM Bullish {struct_metrics.pcr_atm:.2f} +1.0")
        elif struct_metrics.pcr_atm < 0.8:
            struct_score -= 1.0
            drivers.append(f"Struct: PCR ATM Bearish {struct_metrics.pcr_atm:.2f} -1.0")
        
        if struct_metrics.skew_regime == "CRASH_FEAR":
            struct_score -= 2.0
            drivers.append(f"Struct: Skew Crash Fear {struct_metrics.skew_25d:+.2f}% -2.0")
        elif struct_metrics.skew_regime == "MELT_UP":
            struct_score += 1.0
            drivers.append(f"Struct: Skew Melt-up {struct_metrics.skew_25d:+.2f}% +1.0")
        
        external_score = 0.0
        if external_metrics.veto_event_near:
            external_score = -10.0
            drivers.append(f"Ext: Veto event near -10.0")
        elif external_metrics.high_impact_event_near:
            external_score = -2.0
            drivers.append(f"Ext: High impact event near -2.0")
        
        if external_metrics.fii_conviction in ["HIGH", "VERY_HIGH"]:
            if external_metrics.fii_sentiment == "BULLISH":
                external_score += 1.0
                drivers.append(f"Ext: FII bullish {external_metrics.fii_conviction} +1.0")
            else:
                external_score -= 1.0
                drivers.append(f"Ext: FII bearish {external_metrics.fii_conviction} -1.0")
        
        weights = self.calculate_dynamic_weights(vol_metrics, external_metrics, dte)
        
        total_score = (vol_score * weights.vol_weight + 
                      struct_score * weights.struct_weight + 
                      edge_score * weights.edge_weight + 
                      external_score)
        
        total_score = round(total_score, 2)
        
        if total_score > 7:
            overall_signal = "STRONG_SELL"
            confidence = "HIGH"
        elif total_score > 5:
            overall_signal = "SELL"
            confidence = "HIGH"
        elif total_score > 3:
            overall_signal = "CAUTIOUS"
            confidence = "MEDIUM"
        else:
            overall_signal = "AVOID"
            confidence = "LOW"
        
        return RegimeScore(
            total_score=total_score,
            vol_score=round(vol_score, 2),
            struct_score=round(struct_score, 2),
            edge_score=round(edge_score, 2),
            external_score=round(external_score, 2),
            vol_signal="SELL_VOL" if vol_score > 6 else "BUY_VOL" if vol_score < 4 else "NEUTRAL",
            struct_signal="FAVORABLE" if struct_score > 6 else "UNFAVORABLE" if struct_score < 4 else "NEUTRAL",
            edge_signal="POSITIVE" if edge_score > 6 else "NEGATIVE" if edge_score < 4 else "NEUTRAL",
            external_signal="CLEAR" if external_score > -1 else "RISKY",
            overall_signal=overall_signal,
            confidence=confidence,
            score_drivers=drivers
        )
    
    def generate_mandate(self, score: RegimeScore, vol_metrics: VolMetrics,
                        struct_metrics: StructMetrics, edge_metrics: EdgeMetrics,
                        external_metrics: ExternalMetrics, time_metrics: TimeMetrics,
                        expiry_type: str, expiry_date: date, dte: int) -> TradingMandate:
        
        veto_reasons = []
        risk_notes = []
        square_off_instruction = None
        bias = "NEUTRAL"
        
        if struct_metrics.pcr_atm > 1.3:
            bias = "BULLISH"
        elif struct_metrics.pcr_atm < 0.7:
            bias = "BEARISH"
        elif external_metrics.fii_sentiment == "BULLISH":
            bias = "MILDLY_BULLISH"
        elif external_metrics.fii_sentiment == "BEARISH":
            bias = "MILDLY_BEARISH"
        
        if score.total_score >= 6.0:
            if bias == "NEUTRAL":
                suggested_structure = "PROTECTED_STRANGLE"
            else:
                suggested_structure = "IRON_FLY"
            regime = "AGGRESSIVE_SHORT"
        elif score.total_score >= 4.0:
            if bias == "NEUTRAL":
                suggested_structure = "IRON_CONDOR"
            elif "BULL" in bias:
                suggested_structure = "BULL_PUT_SPREAD"
            elif "BEAR" in bias:
                suggested_structure = "BEAR_CALL_SPREAD"
            else:
                suggested_structure = "IRON_CONDOR"
            regime = "DEFENSIVE"
        else:
            suggested_structure = "CASH"
            regime = "CASH"
        
        if dte == 0:
            is_trade_allowed = False
            veto_reasons.append(f"EXPIRY DAY BLOCKED: {expiry_type}")
        elif dte == 1:
            is_trade_allowed = False
            veto_reasons.append(f"STRICT 1 DTE EXIT RULE")
            square_off_instruction = f"Square off ALL {expiry_type} positions by 2:00 PM IST TODAY"
        else:
            if time_metrics.is_past_square_off_time:
                veto_reasons.append("PAST_SQUARE_OFF_TIME")
            
            if external_metrics.veto_event_near:
                veto_reasons.append("VETO: High Impact Event Tomorrow")
            
            if vol_metrics.vol_regime == "EXPLODING":
                veto_reasons.append("VOL_EXPLODING")
            
            is_trade_allowed = len(veto_reasons) == 0 and score.total_score > 3
        
        if is_trade_allowed:
            if expiry_type == "WEEKLY":
                deployment_pct = DynamicConfig.get("WEEKLY_ALLOCATION_PCT")
            elif expiry_type == "MONTHLY":
                deployment_pct = DynamicConfig.get("MONTHLY_ALLOCATION_PCT")
            else:
                deployment_pct = DynamicConfig.get("NEXT_WEEKLY_ALLOCATION_PCT")
        else:
            deployment_pct = 0.0
        
        deployment_amount = DynamicConfig.get("BASE_CAPITAL") * (deployment_pct / 100)
        
        if external_metrics.high_impact_event_near:
            risk_notes.append("HIGH_IMPACT_EVENT_AHEAD")
        
        if vol_metrics.vix_momentum == "RISING":
            risk_notes.append("VIX_RISING")
        
        if struct_metrics.skew_regime == "CRASH_FEAR":
            risk_notes.append("HIGH_CRASH_FEAR")
        
        regime_summary = f"{regime} - {bias} bias"
        
        return TradingMandate(
            expiry_type=expiry_type,
            expiry_date=expiry_date,
            is_trade_allowed=is_trade_allowed,
            suggested_structure=suggested_structure,
            deployment_amount=deployment_amount,
            risk_notes=risk_notes,
            veto_reasons=veto_reasons,
            regime_summary=regime_summary,
            confidence_level=score.confidence,
            square_off_instruction=square_off_instruction
        )


# ============================================================================
# STRATEGY FACTORY
# ============================================================================

class StrategyFactory:
    def __init__(self, fetcher: UpstoxFetcher, spot: float):
        self.fetcher = fetcher
        self.spot = spot
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _validate_strategy(self, legs: List[OptionLeg]) -> List[str]:
        errors = []
        
        for leg in legs:
            if leg.oi < DynamicConfig.get("MIN_OI"):
                errors.append(
                    f"{leg.option_type} {leg.strike} OI {leg.oi:,.0f} < "
                    f"{DynamicConfig.get('MIN_OI'):,.0f} minimum"
                )
        
        for leg in legs:
            if leg.ask > 0 and leg.bid > 0:
                spread_pct = ((leg.ask - leg.bid) / leg.ltp) * 100 if leg.ltp > 0 else 999
                if spread_pct > DynamicConfig.get("MAX_BID_ASK_SPREAD_PCT"):
                    errors.append(
                        f"{leg.option_type} {leg.strike} spread {spread_pct:.1f}% > "
                        f"{DynamicConfig.get('MAX_BID_ASK_SPREAD_PCT')}%"
                    )
        
        return errors
    
    def construct_iron_fly(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            lot_size = self.fetcher.get_lot_size_for_expiry(expiry_date)
            
            atm_strike = min(chain['strike'].values, key=lambda x: abs(x - self.spot))
            atm_row = chain[chain['strike'] == atm_strike].iloc[0]
            
            ce_premium = atm_row['ce_ltp']
            pe_premium = atm_row['pe_ltp']
            straddle_premium = ce_premium + pe_premium
            
            wing_distance = straddle_premium * DynamicConfig.get("IRON_FLY_WING_MULTIPLIER")
            call_wing_strike = atm_strike + wing_distance
            put_wing_strike = atm_strike - wing_distance
            
            call_wing_row = chain.iloc[(chain['strike'] - call_wing_strike).abs().argsort()[:1]]
            put_wing_row = chain.iloc[(chain['strike'] - put_wing_strike).abs().argsort()[:1]]
            
            quantity_lots = int(allocation / (straddle_premium * lot_size))
            if quantity_lots == 0:
                return None
            
            quantity = quantity_lots * lot_size
            
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
                    lot_size=lot_size,
                    entry_price=atm_row['ce_ltp'],
                    entry_bid=atm_row['ce_bid'],
                    entry_ask=atm_row['ce_ask'],
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
                    lot_size=lot_size,
                    entry_price=atm_row['pe_ltp'],
                    entry_bid=atm_row['pe_bid'],
                    entry_ask=atm_row['pe_ask'],
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
                    lot_size=lot_size,
                    entry_price=call_wing_row.iloc[0]['ce_ltp'],
                    entry_bid=call_wing_row.iloc[0]['ce_bid'],
                    entry_ask=call_wing_row.iloc[0]['ce_ask'],
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
                    lot_size=lot_size,
                    entry_price=put_wing_row.iloc[0]['pe_ltp'],
                    entry_bid=put_wing_row.iloc[0]['pe_bid'],
                    entry_ask=put_wing_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=put_wing_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            net_premium = (ce_premium + pe_premium - 
                          call_wing_row.iloc[0]['ce_ltp'] - put_wing_row.iloc[0]['pe_ltp'])
            max_profit = round(net_premium * quantity, 2)
            
            call_wing_spread = call_wing_row.iloc[0]['strike'] - atm_strike
            put_wing_spread = atm_strike - put_wing_row.iloc[0]['strike']
            wing_spread = max(call_wing_spread, put_wing_spread)
            max_loss = round((wing_spread - net_premium) * quantity, 2)
            
            net_theta = round(sum(leg.theta * leg.quantity for leg in legs), 2)
            net_vega = round(sum(leg.vega * leg.quantity for leg in legs), 2)
            net_delta = round(sum(leg.delta * leg.quantity for leg in legs), 2)
            net_gamma = round(sum(leg.gamma * leg.quantity for leg in legs), 2)
            
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
                pop=round(strategy_pop, 1),
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
            
            lot_size = self.fetcher.get_lot_size_for_expiry(expiry_date)
            
            call_20d_row = chain.iloc[(chain['ce_delta'] - 0.20).abs().argsort()[:1]]
            put_20d_row = chain.iloc[(chain['pe_delta'] + 0.20).abs().argsort()[:1]]
            call_5d_row = chain.iloc[(chain['ce_delta'] - 0.05).abs().argsort()[:1]]
            put_5d_row = chain.iloc[(chain['pe_delta'] + 0.05).abs().argsort()[:1]]
            
            net_premium = (call_20d_row.iloc[0]['ce_ltp'] + put_20d_row.iloc[0]['pe_ltp'] -
                          call_5d_row.iloc[0]['ce_ltp'] - put_5d_row.iloc[0]['pe_ltp'])
            
            quantity_lots = int(allocation / (net_premium * lot_size))
            if quantity_lots == 0:
                return None
            
            quantity = quantity_lots * lot_size
            
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
                    lot_size=lot_size,
                    entry_price=call_20d_row.iloc[0]['ce_ltp'],
                    entry_bid=call_20d_row.iloc[0]['ce_bid'],
                    entry_ask=call_20d_row.iloc[0]['ce_ask'],
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
                    lot_size=lot_size,
                    entry_price=put_20d_row.iloc[0]['pe_ltp'],
                    entry_bid=put_20d_row.iloc[0]['pe_bid'],
                    entry_ask=put_20d_row.iloc[0]['pe_ask'],
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
                    lot_size=lot_size,
                    entry_price=call_5d_row.iloc[0]['ce_ltp'],
                    entry_bid=call_5d_row.iloc[0]['ce_bid'],
                    entry_ask=call_5d_row.iloc[0]['ce_ask'],
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
                    lot_size=lot_size,
                    entry_price=put_5d_row.iloc[0]['pe_ltp'],
                    entry_bid=put_5d_row.iloc[0]['pe_bid'],
                    entry_ask=put_5d_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=put_5d_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            max_profit = round(net_premium * quantity, 2)
            call_spread = call_5d_row.iloc[0]['strike'] - call_20d_row.iloc[0]['strike']
            max_loss = round((call_spread - net_premium) * quantity, 2)
            
            net_theta = round(sum(leg.theta * leg.quantity for leg in legs), 2)
            net_vega = round(sum(leg.vega * leg.quantity for leg in legs), 2)
            net_delta = round(sum(leg.delta * leg.quantity for leg in legs), 2)
            net_gamma = round(sum(leg.gamma * leg.quantity for leg in legs), 2)
            
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
                pop=round(strategy_pop, 1),
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
    
    def construct_protected_straddle(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            lot_size = self.fetcher.get_lot_size_for_expiry(expiry_date)
            
            atm_strike = min(chain['strike'].values, key=lambda x: abs(x - self.spot))
            atm_row = chain[chain['strike'] == atm_strike].iloc[0]
            
            wing_delta = DynamicConfig.get("PROTECTED_STRADDLE_WING_DELTA")
            call_wing_row = chain.iloc[(chain['ce_delta'] - wing_delta).abs().argsort()[:1]]
            put_wing_row = chain.iloc[(chain['pe_delta'] + wing_delta).abs().argsort()[:1]]
            
            net_premium = (atm_row['ce_ltp'] + atm_row['pe_ltp'] -
                          call_wing_row.iloc[0]['ce_ltp'] - put_wing_row.iloc[0]['pe_ltp'])
            
            quantity_lots = int(allocation / (net_premium * lot_size))
            if quantity_lots == 0:
                return None
            
            quantity = quantity_lots * lot_size
            
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
                    lot_size=lot_size,
                    entry_price=atm_row['ce_ltp'],
                    entry_bid=atm_row['ce_bid'],
                    entry_ask=atm_row['ce_ask'],
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
                    lot_size=lot_size,
                    entry_price=atm_row['pe_ltp'],
                    entry_bid=atm_row['pe_bid'],
                    entry_ask=atm_row['pe_ask'],
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
                    lot_size=lot_size,
                    entry_price=call_wing_row.iloc[0]['ce_ltp'],
                    entry_bid=call_wing_row.iloc[0]['ce_bid'],
                    entry_ask=call_wing_row.iloc[0]['ce_ask'],
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
                    lot_size=lot_size,
                    entry_price=put_wing_row.iloc[0]['pe_ltp'],
                    entry_bid=put_wing_row.iloc[0]['pe_bid'],
                    entry_ask=put_wing_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=put_wing_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            max_profit = round(net_premium * quantity, 2)
            wing_spread = max(
                call_wing_row.iloc[0]['strike'] - atm_strike,
                atm_strike - put_wing_row.iloc[0]['strike']
            )
            max_loss = round((wing_spread - net_premium) * quantity, 2)
            
            net_theta = round(sum(leg.theta * leg.quantity for leg in legs), 2)
            net_vega = round(sum(leg.vega * leg.quantity for leg in legs), 2)
            net_delta = round(sum(leg.delta * leg.quantity for leg in legs), 2)
            net_gamma = round(sum(leg.gamma * leg.quantity for leg in legs), 2)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"PROTECTED_STRADDLE_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.PROTECTED_STRADDLE,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=round(strategy_pop, 1),
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
            self.logger.error(f"Error constructing Protected Straddle: {e}")
            return None
    
    def construct_protected_strangle(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            lot_size = self.fetcher.get_lot_size_for_expiry(expiry_date)
            
            wing_delta = DynamicConfig.get("PROTECTED_STRANGLE_WING_DELTA")
            call_30d_row = chain.iloc[(chain['ce_delta'] - 0.30).abs().argsort()[:1]]
            put_30d_row = chain.iloc[(chain['pe_delta'] + 0.30).abs().argsort()[:1]]
            call_wing_row = chain.iloc[(chain['ce_delta'] - wing_delta).abs().argsort()[:1]]
            put_wing_row = chain.iloc[(chain['pe_delta'] + wing_delta).abs().argsort()[:1]]
            
            net_premium = (call_30d_row.iloc[0]['ce_ltp'] + put_30d_row.iloc[0]['pe_ltp'] -
                          call_wing_row.iloc[0]['ce_ltp'] - put_wing_row.iloc[0]['pe_ltp'])
            
            quantity_lots = int(allocation / (net_premium * lot_size))
            if quantity_lots == 0:
                return None
            
            quantity = quantity_lots * lot_size
            
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
                    lot_size=lot_size,
                    entry_price=call_30d_row.iloc[0]['ce_ltp'],
                    entry_bid=call_30d_row.iloc[0]['ce_bid'],
                    entry_ask=call_30d_row.iloc[0]['ce_ask'],
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
                    lot_size=lot_size,
                    entry_price=put_30d_row.iloc[0]['pe_ltp'],
                    entry_bid=put_30d_row.iloc[0]['pe_bid'],
                    entry_ask=put_30d_row.iloc[0]['pe_ask'],
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
                    lot_size=lot_size,
                    entry_price=call_wing_row.iloc[0]['ce_ltp'],
                    entry_bid=call_wing_row.iloc[0]['ce_bid'],
                    entry_ask=call_wing_row.iloc[0]['ce_ask'],
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
                    lot_size=lot_size,
                    entry_price=put_wing_row.iloc[0]['pe_ltp'],
                    entry_bid=put_wing_row.iloc[0]['pe_bid'],
                    entry_ask=put_wing_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=put_wing_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            max_profit = round(net_premium * quantity, 2)
            call_spread = call_wing_row.iloc[0]['strike'] - call_30d_row.iloc[0]['strike']
            put_spread = put_30d_row.iloc[0]['strike'] - put_wing_row.iloc[0]['strike']
            max_spread = max(call_spread, put_spread)
            max_loss = round((max_spread - net_premium) * quantity, 2)
            
            net_theta = round(sum(leg.theta * leg.quantity for leg in legs), 2)
            net_vega = round(sum(leg.vega * leg.quantity for leg in legs), 2)
            net_delta = round(sum(leg.delta * leg.quantity for leg in legs), 2)
            net_gamma = round(sum(leg.gamma * leg.quantity for leg in legs), 2)
            
            errors = self._validate_strategy(legs)
            
            strategy_id = f"PROTECTED_STRANGLE_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.PROTECTED_STRANGLE,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=round(strategy_pop, 1),
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
            self.logger.error(f"Error constructing Protected Strangle: {e}")
            return None
    
    def construct_bull_put_spread(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        try:
            chain = self.fetcher.chain(expiry_date)
            if chain is None or chain.empty:
                return None
            
            lot_size = self.fetcher.get_lot_size_for_expiry(expiry_date)
            
            put_30d_row = chain.iloc[(chain['pe_delta'] + 0.30).abs().argsort()[:1]]
            put_10d_row = chain.iloc[(chain['pe_delta'] + 0.10).abs().argsort()[:1]]
            
            net_premium = put_30d_row.iloc[0]['pe_ltp'] - put_10d_row.iloc[0]['pe_ltp']
            
            quantity_lots = int(allocation / (net_premium * lot_size))
            if quantity_lots == 0:
                return None
            
            quantity = quantity_lots * lot_size
            
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
                    lot_size=lot_size,
                    entry_price=put_30d_row.iloc[0]['pe_ltp'],
                    entry_bid=put_30d_row.iloc[0]['pe_bid'],
                    entry_ask=put_30d_row.iloc[0]['pe_ask'],
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
                    lot_size=lot_size,
                    entry_price=put_10d_row.iloc[0]['pe_ltp'],
                    entry_bid=put_10d_row.iloc[0]['pe_bid'],
                    entry_ask=put_10d_row.iloc[0]['pe_ask'],
                    product="D",
                    pop=put_10d_row.iloc[0].get('pe_pop', 0)
                )
            ]
            
            max_profit = round(net_premium * quantity, 2)
            put_spread = put_30d_row.iloc[0]['strike'] - put_10d_row.iloc[0]['strike']
            max_loss = round((put_spread - net_premium) * quantity, 2)
            
            net_theta = round(sum(leg.theta * leg.quantity for leg in legs), 2)
            net_vega = round(sum(leg.vega * leg.quantity for leg in legs), 2)
            net_delta = round(sum(leg.delta * leg.quantity for leg in legs), 2)
            net_gamma = round(sum(leg.gamma * leg.quantity for leg in legs), 2)
            
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
                pop=round(strategy_pop, 1),
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
            
            lot_size = self.fetcher.get_lot_size_for_expiry(expiry_date)
            
            call_30d_row = chain.iloc[(chain['ce_delta'] - 0.30).abs().argsort()[:1]]
            call_10d_row = chain.iloc[(chain['ce_delta'] - 0.10).abs().argsort()[:1]]
            
            net_premium = call_30d_row.iloc[0]['ce_ltp'] - call_10d_row.iloc[0]['ce_ltp']
            
            quantity_lots = int(allocation / (net_premium * lot_size))
            if quantity_lots == 0:
                return None
            
            quantity = quantity_lots * lot_size
            
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
                    lot_size=lot_size,
                    entry_price=call_30d_row.iloc[0]['ce_ltp'],
                    entry_bid=call_30d_row.iloc[0]['ce_bid'],
                    entry_ask=call_30d_row.iloc[0]['ce_ask'],
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
                    lot_size=lot_size,
                    entry_price=call_10d_row.iloc[0]['ce_ltp'],
                    entry_bid=call_10d_row.iloc[0]['ce_bid'],
                    entry_ask=call_10d_row.iloc[0]['ce_ask'],
                    product="D",
                    pop=call_10d_row.iloc[0].get('ce_pop', 0)
                )
            ]
            
            max_profit = round(net_premium * quantity, 2)
            call_spread = call_10d_row.iloc[0]['strike'] - call_30d_row.iloc[0]['strike']
            max_loss = round((call_spread - net_premium) * quantity, 2)
            
            net_theta = round(sum(leg.theta * leg.quantity for leg in legs), 2)
            net_vega = round(sum(leg.vega * leg.quantity for leg in legs), 2)
            net_delta = round(sum(leg.delta * leg.quantity for leg in legs), 2)
            net_gamma = round(sum(leg.gamma * leg.quantity for leg in legs), 2)
            
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
                pop=round(strategy_pop, 1),
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
                interval = DynamicConfig.get("ANALYTICS_INTERVAL_MINUTES")
            else:
                interval = DynamicConfig.get("ANALYTICS_OFFHOURS_INTERVAL_MINUTES")
            
            elapsed_minutes = (now - last_time).total_seconds() / 60
            
            if elapsed_minutes >= interval:
                self.logger.info(f"Time-based recalculation: {elapsed_minutes:.1f}min elapsed")
                return True
            
            if self._last_spot > 0:
                spot_change_pct = abs(current_spot - self._last_spot) / self._last_spot * 100
                if spot_change_pct > DynamicConfig.get("SPOT_CHANGE_TRIGGER_PCT"):
                    self.logger.info(f"Spot-triggered recalculation: {spot_change_pct:.2f}% change")
                    return True
            
            if self._last_vix > 0:
                vix_change_pct = abs(current_vix - self._last_vix) / self._last_vix * 100
                if vix_change_pct > DynamicConfig.get("VIX_CHANGE_TRIGGER_PCT"):
                    self.logger.info(f"VIX-triggered recalculation: {vix_change_pct:.2f}% change")
                    return True
            
            return False
    
    def update(self, analysis_data: Dict, spot: float, vix: float):
        with self._lock:
            self._cache = copy.deepcopy(analysis_data)
            self._last_spot = spot
            self._last_vix = vix
            self._last_calc_time = datetime.now(pytz.UTC)
            self.logger.info(f"Analytics cache updated | Spot: {spot:.2f} | VIX: {vix:.2f}")


class AnalyticsScheduler:
    def __init__(self, volguard_system, cache: AnalyticsCache):
        self.system = volguard_system
        self.cache = cache
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
        self._running = False
        self._executor: Optional[ThreadPoolExecutor] = None
        self._auto_trader = AutoTradingEngine(volguard_system, SessionLocal)
    
    async def start(self):
        self._running = True
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="analytics")
        self.logger.info("Analytics scheduler started with ThreadPoolExecutor")
        loop = asyncio.get_event_loop()
        
        self.logger.info("Waiting for first price data...")
        for i in range(30):
            current_spot = self.system.fetcher.get_ltp_with_fallback(SystemConfig.NIFTY_KEY)
            current_vix = self.system.fetcher.get_ltp_with_fallback(SystemConfig.VIX_KEY)
            
            if current_spot is not None and current_spot > 0 and current_vix is not None and current_vix > 0:
                self.logger.info(f"Price data received after {i+1} seconds - Spot: {current_spot:.2f}, VIX: {current_vix:.2f}")
                break
                
            await asyncio.sleep(1)
        else:
            self.logger.warning("No price data after 30 seconds, continuing anyway...")
        
        while self._running:
            try:
                current_spot = self.system.fetcher.get_ltp_with_fallback(SystemConfig.NIFTY_KEY)
                current_vix = self.system.fetcher.get_ltp_with_fallback(SystemConfig.VIX_KEY)
                
                if current_spot is None or current_spot <= 0 or current_vix is None or current_vix <= 0:
                    await asyncio.sleep(2)
                    continue
                
                should_run = self.cache.should_recalculate(current_spot, current_vix)
                
                if should_run:
                    self.logger.info(f"Running analytics - Spot: {current_spot:.2f}, VIX: {current_vix:.2f}")
                    try:
                        analysis = await loop.run_in_executor(
                            self._executor,
                            self.system.run_complete_analysis
                        )
                        self.cache.update(analysis, current_spot, current_vix)
                        
                        if DynamicConfig.get("AUTO_TRADING") or DynamicConfig.get("ENABLE_MOCK_TRADING"):
                            self.logger.info("Auto-trading enabled - evaluating mandates for execution")
                            results = await loop.run_in_executor(
                                self._executor,
                                self._auto_trader.evaluate_all_mandates,
                                analysis
                            )
                            for mandate_key, success in results.items():
                                if success:
                                    self.logger.info(f"Auto-executed {mandate_key}")
                        
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
# POSITION MONITOR
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
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="monitor")
    
    async def start_monitoring(self):
        self.is_running = True
        self.logger.info(f"Position monitoring started ({DynamicConfig.get('MONITOR_INTERVAL_SECONDS')}s intervals - Smart Fallback)")
        
        while self.is_running:
            try:
                await self.check_all_positions()
                await asyncio.sleep(DynamicConfig.get("MONITOR_INTERVAL_SECONDS"))
            except Exception as e:
                self.logger.error(f"Monitor error: {e}")
                await asyncio.sleep(10)
    
    async def check_all_positions(self):
        with self.db_session_factory() as db:
            loop = asyncio.get_event_loop()
            
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
                    await loop.run_in_executor(
                        self._executor,
                        self.fetcher.subscribe_market_data,
                        list(new_instruments), "ltpc"
                    )
                    self._subscribed_instruments.update(new_instruments)
                
                current_prices = await loop.run_in_executor(
                    self._executor,
                    self.fetcher.get_bulk_ltp_with_fallback,
                    list(set(all_instruments))
                )
                
                if not current_prices:
                    self.logger.error("PRICE DATA UNAVAILABLE")
                    return
                
                cached_analysis = self.analytics_cache.get()
                
                total_realized_pnl = 0.0
                total_unrealized_pnl = 0.0
                
                for trade in active_trades:
                    result = await self.check_single_position(trade, current_prices, db, loop)
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
                
                total_mtm = total_realized_pnl + total_unrealized_pnl
                threshold = -DynamicConfig.get("BASE_CAPITAL") * DynamicConfig.get("CIRCUIT_BREAKER_PCT") / 100
                
                if total_mtm < threshold:
                    self._breach_count += 1
                    if self._breach_count >= self._breach_threshold:
                        self.logger.critical(f"CIRCUIT BREAKER TRIGGERED! Total MTM: ₹{total_mtm:.2f} < ₹{threshold:.2f}")
                        await self.trigger_circuit_breaker(db, loop)
                else:
                    self._breach_count = 0
            
            except Exception as e:
                self.logger.error(f"Position check error: {e}")
    
    async def check_single_position(self, trade: TradeJournal, current_prices: Dict, db: Session, loop):
        """
        Checks a single position for exit conditions and computes unrealized P&L.

        EXIT RESPONSIBILITY SPLIT — CRITICAL:
          • Stop-loss and profit-target exits → handled exclusively by GTT orders placed at entry.
            The GTT fires a BUY directly at the exchange when LTP crosses the trigger level.
            DO NOT replicate that logic here — doing so sends a second exit order on the same
            position within the same 5-second cycle, creating a net-long or net-short residual.

          • This monitor is responsible ONLY for:
            1. Pre-expiry square-off  (1 day before expiry @ 14:00 IST)
            2. Veto-event forced exit  (per SystemConfig.should_square_off_position)
            Circuit-breaker exits are handled in trigger_circuit_breaker() via check_all_positions.
        """
        legs_data = json.loads(trade.legs_data)

        for leg in legs_data:
            instrument_key = leg['instrument_token']
            if instrument_key not in current_prices:
                self.logger.error(f"Missing price for {instrument_key} - aborting P&L for {trade.strategy_id}")
                return None
            if current_prices[instrument_key] is None or current_prices[instrument_key] <= 0:
                self.logger.error(f"Invalid price {current_prices[instrument_key]} for {instrument_key}")
                return None

        # ── Compute unrealized P&L for dashboard / circuit-breaker aggregation ──────────
        unrealized_pnl = 0.0
        for leg in legs_data:
            entry_price  = leg['entry_price']
            current_price = current_prices[leg['instrument_token']]
            qty          = leg.get('filled_quantity', leg['quantity'])
            multiplier   = -1 if leg['action'] == 'SELL' else 1
            unrealized_pnl += round((current_price - entry_price) * qty * multiplier, 2)

        # ── Only scheduled/rule-based exits are triggered here ───────────────────────────
        exit_reason = None
        should_exit, reason = SystemConfig.should_square_off_position(trade)
        if should_exit:
            if "PRE_EXPIRY" in reason:
                exit_reason = TradeStatus.CLOSED_EXPIRY_EXIT.value
            else:
                exit_reason = TradeStatus.CLOSED_VETO_EVENT.value
            self.logger.info(f"Scheduled square-off triggered for {trade.strategy_id}: {reason}")

        if exit_reason:
            # Cancel live GTT orders BEFORE placing the manual exit to avoid double-fill
            # (executor.exit_position already calls cancel_gtt_orders internally)
            await self.exit_position(trade, exit_reason, current_prices, db, loop)

        return unrealized_pnl
    
    async def exit_position(self, trade: TradeJournal, exit_reason: str, 
                           current_prices: Dict, db: Session, loop):
        executor = UpstoxOrderExecutor(self.fetcher)
        result = await loop.run_in_executor(
            self._executor,
            executor.exit_position,
            trade, exit_reason, current_prices, db
        )
        
        if result["success"] and self.alert_service:
            try:
                msg = f"""
<b>Strategy Closed:</b> {trade.strategy_id}
<b>Type:</b> {trade.strategy_type}
<b>Total P&L:</b> ₹{result['realized_pnl']:.2f}{' (approximate)' if result.get('pnl_approximate') else ''}
<b>Exit Reason:</b> {exit_reason}
"""
                priority = AlertPriority.SUCCESS if result['realized_pnl'] > 0 else AlertPriority.HIGH
                throttle_key = f"exit_{trade.strategy_id}"
                self.alert_service.send("Trade Closed", msg, priority, throttle_key)
            except Exception as e:
                self.logger.error(f"Alert sending failed: {e}")
    
    async def trigger_circuit_breaker(self, db: Session, loop):
        active_trades = db.query(TradeJournal).filter(
            TradeJournal.status == TradeStatus.ACTIVE.value
        ).all()
        
        executor = UpstoxOrderExecutor(self.fetcher)
        
        for trade in active_trades:
            legs_data = json.loads(trade.legs_data)
            instrument_tokens = [leg['instrument_token'] for leg in legs_data]
            
            current_prices = await loop.run_in_executor(
                self._executor,
                self.fetcher.get_bulk_ltp_with_fallback,
                instrument_tokens
            )
            
            if current_prices:
                await loop.run_in_executor(
                    self._executor,
                    executor.exit_position,
                    trade, TradeStatus.CLOSED_CIRCUIT_BREAKER.value, current_prices, db
                )
        
        today = datetime.now().date()
        stats = db.query(DailyStats).filter(DailyStats.date == today).first()
        if stats:
            stats.circuit_breaker_triggered = True
            db.commit()
        
        self.logger.critical("ALL POSITIONS CLOSED - CIRCUIT BREAKER ACTIVE")
    
    def _update_daily_stats(self, db: Session, realized_pnl: float, unrealized_pnl: float):
        today = datetime.now().date()
        stats = db.query(DailyStats).filter(DailyStats.date == today).first()
        
        if not stats:
            stats = DailyStats(date=today)
            db.add(stats)
        
        stats.realized_pnl = round(realized_pnl, 2)
        stats.unrealized_pnl = round(unrealized_pnl, 2)
        stats.total_pnl = round(realized_pnl + unrealized_pnl, 2)
        
        db.commit()
    
    def stop(self):
        self.is_running = False
        if self._subscribed_instruments:
            self.fetcher.unsubscribe_market_data(list(self._subscribed_instruments))
        if self._executor:
            self._executor.shutdown(wait=True)
        self.logger.info("Position monitoring stopped")


# ============================================================================
# COMPLETE SYSTEM ORCHESTRATOR
# ============================================================================

class VolGuardSystem:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        if not UPSTOX_AVAILABLE:
            raise RuntimeError("Upstox SDK not installed. Cannot initialize VolGuardSystem.")
        
        self.fetcher = UpstoxFetcher(SystemConfig.UPSTOX_ACCESS_TOKEN)
        self.analytics = AnalyticsEngine()
        self.regime = RegimeEngine()
        
        self.json_cache = JSONCacheManager()
        self.analytics_cache = AnalyticsCache()
        self.correlation_manager = CorrelationManager(SessionLocal)
        
        if DynamicConfig.get("AUTO_TRADING") and SystemConfig.UPSTOX_ACCESS_TOKEN:
            self.logger.info("REAL TRADING MODE ENABLED - Using UpstoxOrderExecutor")
            self.executor = UpstoxOrderExecutor(self.fetcher)
        else:
            self.logger.info("MOCK TRADING MODE ENABLED")
            self.executor = MockExecutor(self.fetcher)
        
        self.analytics_scheduler: Optional[AnalyticsScheduler] = None
        self.monitor: Optional[PositionMonitor] = None
        self.alert_service: Optional[TelegramAlertService] = None
        
        self.market_streamer_started = False
        self.portfolio_streamer_started = False
        self._cached_expiries: List[date] = []
        
        self.ws_manager = WebSocketManager()
        
        self.logger.info("VolGuard System v3.5 initialized")
    
    def start_market_streamer(self, instrument_keys: List[str] = None, mode: str = "ltpc"):
        if instrument_keys is None:
            instrument_keys = [SystemConfig.NIFTY_KEY, SystemConfig.VIX_KEY]
        
        self.fetcher.start_market_streamer(instrument_keys, mode)
        self.market_streamer_started = True
        self.logger.info(f"Market streamer started with {len(instrument_keys)} instruments")
    
    def start_portfolio_streamer(self):
        self.fetcher.start_portfolio_streamer(
            order_update=True,
            position_update=False,
            holding_update=False,
            gtt_update=True
        )
        self.portfolio_streamer_started = True
        self.logger.info("Portfolio streamer started")
    
    def run_complete_analysis(self) -> Dict:
        try:
            nifty_hist = self.fetcher.history(SystemConfig.NIFTY_KEY)
            vix_hist = self.fetcher.history(SystemConfig.VIX_KEY)
            
            spot = self.fetcher.get_ltp_with_fallback(SystemConfig.NIFTY_KEY) or 0
            vix = self.fetcher.get_ltp_with_fallback(SystemConfig.VIX_KEY) or 0
            
            if nifty_hist is None:
                raise ValueError("Failed to fetch Nifty historical data")
            if vix_hist is None:
                raise ValueError("Failed to fetch VIX historical data")
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
            
            primary_mandate = weekly_mandate if weekly_mandate.is_trade_allowed else \
                             monthly_mandate if monthly_mandate.is_trade_allowed else \
                             next_weekly_mandate if next_weekly_mandate.is_trade_allowed else None
            
            professional_recommendation = {
                "primary": {
                    "expiry_type": primary_mandate.expiry_type if primary_mandate else "NONE",
                    "strategy": primary_mandate.suggested_structure if primary_mandate else "CASH",
                    "capital_deploy_formatted": f"₹{primary_mandate.deployment_amount:,.0f}" if primary_mandate else "₹0"
                }
            }
            
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
                "all_expiries": all_expiries,
                "professional_recommendation": professional_recommendation
            }
        
        except Exception as e:
            self.logger.error(f"Analysis error: {e}")
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
            "PROTECTED_STRADDLE": StrategyType.PROTECTED_STRADDLE,
            "PROTECTED_STRANGLE": StrategyType.PROTECTED_STRANGLE,
            "BULL_PUT_SPREAD": StrategyType.BULL_PUT_SPREAD,
            "BEAR_CALL_SPREAD": StrategyType.BEAR_CALL_SPREAD
        }
        
        strategy_type = strategy_type_map.get(strategy_type_str)
        if not strategy_type:
            self.logger.error(f"Unknown strategy type: {strategy_type_str}")
            return None
        
        factory = StrategyFactory(
            self.fetcher,
            analysis_data['vol_metrics'].spot
        )
        
        if strategy_type == StrategyType.IRON_FLY:
            strategy = factory.construct_iron_fly(mandate.expiry_date, mandate.deployment_amount)
        elif strategy_type == StrategyType.IRON_CONDOR:
            strategy = factory.construct_iron_condor(mandate.expiry_date, mandate.deployment_amount)
        elif strategy_type == StrategyType.PROTECTED_STRADDLE:
            strategy = factory.construct_protected_straddle(mandate.expiry_date, mandate.deployment_amount)
        elif strategy_type == StrategyType.PROTECTED_STRANGLE:
            strategy = factory.construct_protected_strangle(mandate.expiry_date, mandate.deployment_amount)
        elif strategy_type == StrategyType.BULL_PUT_SPREAD:
            strategy = factory.construct_bull_put_spread(mandate.expiry_date, mandate.deployment_amount)
        elif strategy_type == StrategyType.BEAR_CALL_SPREAD:
            strategy = factory.construct_bear_call_spread(mandate.expiry_date, mandate.deployment_amount)
        else:
            return None
        
        if not strategy:
            return None

        # ── Set correct expiry_type BEFORE correlation check ──────────────────────────
        # Each factory construct_* method hardcodes ExpiryType.WEEKLY as a default.
        # We must override it with the mandate's actual type here so that the
        # correlation manager evaluates the right expiry category (weekly vs monthly).
        expiry_type_map = {
            "WEEKLY":      ExpiryType.WEEKLY,
            "MONTHLY":     ExpiryType.MONTHLY,
            "NEXT_WEEKLY": ExpiryType.NEXT_WEEKLY
        }
        strategy.expiry_type = expiry_type_map.get(mandate.expiry_type, ExpiryType.WEEKLY)

        allowed, violations = self.correlation_manager.can_take_position(strategy)
        if not allowed:
            self.logger.warning(f"Correlation violation - cannot take position: {violations[0].rule}")
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
            entry_premium = round(sum(
                leg.entry_price * leg.quantity * (-1 if leg.action == 'SELL' else 1)
                for leg in strategy.legs
            ), 2)
            
            filled_quantities = {}
            fill_prices = {}
            if 'filled_orders' in result:
                for order_id, fill_info in result['filled_orders'].items():
                    filled_quantities[order_id] = fill_info.get('filled_quantity', 0)
            
            if 'fill_prices' in result:
                fill_prices = result['fill_prices']
            
            trade = TradeJournal(
                strategy_id=strategy.strategy_id,
                strategy_type=strategy.strategy_type.value,
                expiry_type=strategy.expiry_type.value,
                expiry_date=strategy.expiry_date,
                entry_time=datetime.now(),
                legs_data=json.dumps([asdict(leg) for leg in strategy.legs]),
                order_ids=json.dumps(result.get('order_ids', [])),
                filled_quantities=json.dumps(filled_quantities) if filled_quantities else None,
                fill_prices=json.dumps(fill_prices) if fill_prices else None,
                gtt_order_ids=json.dumps(result.get('gtt_order_ids', [])),
                entry_greeks_snapshot=json.dumps(result.get('entry_greeks', {})),
                max_profit=round(strategy.max_profit, 2),
                max_loss=round(strategy.max_loss, 2),
                required_margin=round(strategy.required_margin, 2),
                allocated_capital=round(strategy.allocated_capital, 2),
                entry_premium=entry_premium,
                status=TradeStatus.ACTIVE.value,
                is_mock=not DynamicConfig.get("AUTO_TRADING")
            )
            db.add(trade)
            db.commit()
            
            if self.alert_service:
                try:
                    msg = f"""
<b>New Position Opened</b>
<b>Strategy:</b> {strategy.strategy_type.value}
<b>Expiry:</b> {strategy.expiry_type.value}
<b>Max Profit:</b> ₹{strategy.max_profit:,.2f}
<b>Max Loss:</b> ₹{strategy.max_loss:,.2f}
<b>Required Margin:</b> ₹{strategy.required_margin:,.2f}
<b>Legs:</b> {len(strategy.legs)}
<b>POP:</b> {strategy.pop:.1f}%
"""
                    self.alert_service.send("Position Opened", msg, AlertPriority.MEDIUM)
                except Exception as e:
                    self.logger.error(f"Alert sending failed: {e}")
            
            self.logger.info(f"Strategy executed: {strategy.strategy_id}")
        
        return result


# ============================================================================
# BACKGROUND RECONCILIATION JOBS
# ============================================================================

async def position_reconciliation_job():
    logger.info("Position reconciliation job started")
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="recon")
    
    while True:
        try:
            if volguard_system and volguard_system.fetcher.is_market_open_now():
                def sync_reconcile():
                    with SessionLocal() as db:
                        return volguard_system.fetcher.reconcile_positions_with_db(db)
                
                report = await loop.run_in_executor(executor, sync_reconcile)
                
                if not report["reconciled"] and volguard_system and volguard_system.alert_service:
                    volguard_system.alert_service.send(
                        "Position Mismatch Detected",
                        f"DB: {report['db_positions']}, Broker: {report['broker_positions']}\n"
                        f"Discrepancies: {len(report.get('discrepancies', []))}",
                        AlertPriority.HIGH,
                        throttle_key="position_reconciliation"
                    )
                
                await asyncio.sleep(DynamicConfig.get("POSITION_RECONCILE_INTERVAL_MINUTES") * 60)
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
                    with SessionLocal() as db:
                        stats = db.query(DailyStats).filter(DailyStats.date == today).first()
                        if stats and volguard_system:
                            our_pnl = stats.total_pnl or 0.0
                            broker_pnl = volguard_system.fetcher.get_broker_pnl_for_date(today)
                            return stats, our_pnl, broker_pnl
                        return None, None, None
                
                result = await loop.run_in_executor(executor, sync_pnl_reconcile)
                
                if result and result[0] and result[2] is not None:
                    stats, our_pnl, broker_pnl = result
                    discrepancy = round(abs(our_pnl - broker_pnl), 2)
                    
                    def sync_update():
                        with SessionLocal() as db:
                            stats = db.query(DailyStats).filter(DailyStats.date == today).first()
                            if stats:
                                stats.broker_pnl = round(broker_pnl, 2)
                                stats.pnl_discrepancy = discrepancy
                                db.commit()
                    
                    await loop.run_in_executor(executor, sync_update)
                    
                    if discrepancy > DynamicConfig.get("PNL_DISCREPANCY_THRESHOLD") and volguard_system.alert_service:
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
# AUTOMATED TOKEN REFRESH SCHEDULER
# ============================================================================

async def scheduled_token_refresh():
    """
    Docker-native scheduled task.
    Runs continuously and triggers the Upstox v3 token request at 08:30 IST daily.
    """
    ist_tz = pytz.timezone('Asia/Kolkata')
    logger.info("✅ Token refresh scheduler started (Target: 08:30 AM IST daily)")
    
    while True:
        try:
            now = datetime.now(ist_tz)
            
            # Trigger exactly at 08:30 AM IST
            if now.hour == 8 and now.minute == 30:
                logger.info("⏰ Scheduled Token Refresh Triggered!")
                client_id = os.environ.get("UPSTOX_CLIENT_ID", "")
                client_secret = os.environ.get("UPSTOX_CLIENT_SECRET", "")
                
                if client_id and client_secret:
                    # Hitting the Upstox V3 Token Request endpoint
                    resp = requests.post(
                        f"https://api.upstox.com/v3/login/auth/token/request/{client_id}",
                        headers={"Content-Type": "application/json"},
                        json={"client_secret": client_secret},
                        timeout=15
                    )
                    
                    if resp.status_code == 200:
                        logger.info("✅ Token request sent to Upstox. Waiting for mobile approval...")
                        if volguard_system and volguard_system.alert_service:
                            volguard_system.alert_service.send(
                                "🟡 VolGuard: Upstox Token Renewal",
                                "Open your Upstox app and approve the token request.\nSystem will auto-update once approved.",
                                AlertPriority.HIGH
                            )
                    else:
                        logger.error(f"Failed to request token: {resp.status_code} - {resp.text}")
                
                # Sleep for 61 seconds to prevent double-firing
                await asyncio.sleep(61)
            else:
                await asyncio.sleep(30)
                
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Token refresh scheduler error: {e}")
            await asyncio.sleep(60)


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 70)
    logger.info("VolGuard v3.5 Starting...")
    logger.info("=" * 70)
    
    if not UPSTOX_AVAILABLE:
        raise RuntimeError("Upstox SDK not installed. Cannot start application.")
    
    DynamicConfig.initialize(SessionLocal)
    logger.info(f"Base Capital: ₹{DynamicConfig.get('BASE_CAPITAL'):,.2f}")
    logger.info(f"Auto Trading: {'ENABLED 🔴' if DynamicConfig.get('AUTO_TRADING') else 'DISABLED 🟡'}")
    logger.info(f"Mock Trading: {'ENABLED 🟡' if DynamicConfig.get('ENABLE_MOCK_TRADING') else 'DISABLED'}")
    logger.info(f"GTT Stop Loss Multiplier: {DynamicConfig.get('GTT_STOP_LOSS_MULTIPLIER')}x")
    logger.info(f"GTT Profit Target Multiplier: {DynamicConfig.get('GTT_PROFIT_TARGET_MULTIPLIER')}x")
    logger.info(f"GTT Trailing Gap: {DynamicConfig.get('GTT_TRAILING_GAP')}")
    
    alert_service = None
    if SystemConfig.TELEGRAM_TOKEN and SystemConfig.TELEGRAM_CHAT_ID:
        alert_service = TelegramAlertService(
            SystemConfig.TELEGRAM_TOKEN,
            SystemConfig.TELEGRAM_CHAT_ID
        )
        await alert_service.start()
        alert_service.send(
            "VolGuard v3.5 Started",
            "System Online - Full Auto-Trading Enabled",
            AlertPriority.SUCCESS
        )
        logger.info("Telegram Alerts Enabled")
    else:
        logger.warning("Telegram credentials not configured - alerts disabled")
    
    global volguard_system
    volguard_system = VolGuardSystem()
    volguard_system.alert_service = alert_service
    
    if hasattr(volguard_system.json_cache, '_calendar_engine') and volguard_system.json_cache._calendar_engine:
        volguard_system.json_cache._calendar_engine.set_alert_service(alert_service)
    
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
    
    # 1. Start the automated token renewal scheduler
    token_cron_task = asyncio.create_task(scheduled_token_refresh())
    
    if DynamicConfig.get("ENABLE_MOCK_TRADING") or DynamicConfig.get("AUTO_TRADING"):
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
    
    logger.info("VolGuard v3.5 shutting down...")
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

    # 2. Cancel ALL background async tasks gracefully to prevent zombie processes
    tasks_to_cancel = [position_recon_task, pnl_recon_task, analytics_task, cache_task, token_cron_task]
    if 'monitor_task' in locals():
        tasks_to_cancel.append(monitor_task)

    for task in tasks_to_cancel:
        if task and not task.done():
            task.cancel()

    if tasks_to_cancel:
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

    # Force SQLite WAL checkpoint for clean DB state before exit
    try:
        with SessionLocal() as _db:
            _db.execute(text("PRAGMA wal_checkpoint(FULL)"))
    except Exception as _e:
        logger.warning(f"WAL checkpoint failed on shutdown: {_e}")

    logger.info("VolGuard v4.0 shutdown complete.")


app = FastAPI(
    title="VolGuard v3.5 - Production",
    description="Professional Options Trading System - Fully Automated",
    version="3.5.0",
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
# TOKEN REFRESH HELPERS  (Bug 3 Fix)
# ============================================================================
# Upstox access tokens expire at 3:30 AM IST every day regardless of when
# they were generated.  The v3 token endpoint requires physical user approval
# in the Upstox app — there is NO silent refresh or refresh-token mechanism.
#
# Workflow:
#   1. A cron job (or systemd timer) runs refresh_token_cron.py at 08:30 IST.
#   2. That script POSTs to Upstox /v3/login/auth/token/request/{client_id}.
#   3. Upstox sends a push notification to your phone.
#   4. You approve it → Upstox calls the Notifier Webhook below with the new token.
#   5. The webhook updates the live config and persists to .env (no restart needed).
#
# Setup once in Upstox Developer Console:
#   Notifier Webhook URL = https://your-server/webhook/upstox-token
# ============================================================================

def _update_env_token(new_token: str) -> None:
    """Update UPSTOX_ACCESS_TOKEN in the .env file in-place."""
    env_path = ".env"
    if not os.path.exists(env_path):
        logger.warning(".env file not found — token not persisted to disk")
        return
    try:
        with open(env_path, "r") as f:
            lines = f.readlines()
        with open(env_path, "w") as f:
            replaced = False
            for line in lines:
                if line.startswith("UPSTOX_ACCESS_TOKEN="):
                    f.write(f"UPSTOX_ACCESS_TOKEN={new_token}\n")
                    replaced = True
                else:
                    f.write(line)
            if not replaced:
                f.write(f"\nUPSTOX_ACCESS_TOKEN={new_token}\n")
        logger.info("UPSTOX_ACCESS_TOKEN updated in .env file")
    except Exception as e:
        logger.error(f"Failed to persist new token to .env: {e}")


@app.post("/webhook/upstox-token")
async def receive_upstox_token(payload: dict):
    """
    Upstox Notifier Webhook endpoint.
    Called automatically by Upstox after the user approves the daily token request.

    Payload from Upstox contains: access_token, issued_at, expires_at
    This endpoint hot-swaps the token in all live components without a restart.
    """
    new_token = payload.get("access_token")
    if not new_token:
        logger.error("Token webhook called but 'access_token' missing from payload")
        raise HTTPException(status_code=400, detail="access_token missing from payload")

    logger.info(f"New Upstox token received via webhook (expires: {payload.get('expires_at', 'unknown')})")

    if volguard_system:
        try:
            # Hot-swap token in the Upstox SDK configuration
            cfg = upstox_client.Configuration()
            cfg.access_token = new_token

            # Update all API clients inside the fetcher
            fetcher = volguard_system.fetcher
            fetcher.configuration.access_token = new_token

            # Rebuild API clients with new token
            api_client = upstox_client.ApiClient(cfg)
            fetcher.market_quote_api  = upstox_client.MarketQuoteApi(api_client)
            fetcher.order_api         = upstox_client.OrderApi(api_client)
            fetcher.order_api_v3      = upstox_client.OrderApiV3(api_client)
            fetcher.portfolio_api     = upstox_client.PortfolioApi(api_client)
            fetcher.charge_api        = upstox_client.ChargeApi(api_client)
            fetcher.history_api       = upstox_client.HistoryV3Api(api_client)
            fetcher.option_chain_api  = upstox_client.OptionsApi(api_client)
            fetcher.market_data_api   = upstox_client.MarketDataApi(api_client)

            logger.info("✅ All Upstox API clients updated with new token")
        except Exception as e:
            logger.error(f"Failed to hot-swap token in live clients: {e}")
            raise HTTPException(status_code=500, detail=f"Token hot-swap failed: {e}")

    # Also update environment variable in memory + persist to .env
    os.environ["UPSTOX_ACCESS_TOKEN"] = new_token
    _update_env_token(new_token)

    return {
        "status": "success",
        "message": "Token updated successfully. No restart required.",
        "expires_at": payload.get("expires_at")
    }


@app.post("/api/token/refresh-request")
async def trigger_token_refresh_request(token: str = Depends(verify_token)):
    """
    Manually trigger a new token request to Upstox (sends push notification to your phone).
    Use this if the cron job didn't fire or you need to refresh early.
    Upstox will call /webhook/upstox-token automatically after you approve in the app.
    """
    client_id     = os.environ.get("UPSTOX_CLIENT_ID", "")
    client_secret = os.environ.get("UPSTOX_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="UPSTOX_CLIENT_ID / UPSTOX_CLIENT_SECRET not set in environment")

    try:
        resp = requests.post(
            f"https://api.upstox.com/v3/login/auth/token/request/{client_id}",
            headers={"Content-Type": "application/json"},
            json={"client_secret": client_secret},
            timeout=10
        )
        if resp.status_code == 200:
            logger.info("Token refresh request sent to Upstox — check phone to approve")
            return {"status": "success", "message": "Token request sent. Approve in Upstox app. Webhook will auto-update token."}
        else:
            raise HTTPException(status_code=resp.status_code, detail=f"Upstox returned: {resp.text}")
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Could not reach Upstox API: {e}")


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/api/fii/summary")
def get_fii_summary(token: str = Depends(verify_token)):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    ext_metrics = volguard_system.json_cache.get_external_metrics()
    return {
        "fii_net_change": ext_metrics.fii_net_change,
        "fii_direction": ext_metrics.fii_sentiment,
        "fii_conviction": ext_metrics.fii_conviction,
        "data_date": ext_metrics.fii_data_date,
        "is_fallback": ext_metrics.fii_is_fallback
    }


@app.get("/api/orders/fill-quality")
def get_fill_quality(token: str = Depends(verify_token)):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    return volguard_system.fetcher.fill_tracker.get_stats()


@app.get("/api/pnl/attribution")
def get_pnl_attribution(db: Session = Depends(get_db), token: str = Depends(verify_token)):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    active_trades = db.query(TradeJournal).filter(TradeJournal.status == TradeStatus.ACTIVE.value).all()
    if not active_trades:
        return {"total_pnl": 0.0, "theta_pnl": 0.0, "vega_pnl": 0.0, "delta_pnl": 0.0, "other_pnl": 0.0, "iv_change": 0.0}
    
    all_instruments = []
    for trade in active_trades:
        legs_data = json.loads(trade.legs_data)
        all_instruments.extend([leg['instrument_token'] for leg in legs_data])
    
    all_instruments = list(set(all_instruments))
    prices = volguard_system.fetcher.get_bulk_ltp_with_fallback(all_instruments)
    greeks = volguard_system.fetcher.get_greeks(all_instruments)
    
    total_pnl = 0.0
    total_theta = 0.0
    total_vega = 0.0
    total_delta = 0.0
    total_other = 0.0
    total_iv_change = 0.0
    trade_count = 0
    
    engine = PnLAttributionEngine(volguard_system.fetcher)
    
    for trade in active_trades:
        attr = engine.calculate(trade, prices, greeks)
        if attr:
            total_pnl += attr.total_pnl
            total_theta += attr.theta_pnl
            total_vega += attr.vega_pnl
            total_delta += attr.delta_pnl
            total_other += attr.other_pnl
            total_iv_change += attr.iv_change
            trade_count += 1
    
    avg_iv_change = total_iv_change / trade_count if trade_count > 0 else 0
    
    return {
        "total_pnl": round(total_pnl, 2),
        "theta_pnl": round(total_theta, 2),
        "vega_pnl": round(total_vega, 2),
        "delta_pnl": round(total_delta, 2),
        "other_pnl": round(total_other, 2),
        "iv_change": round(avg_iv_change, 2)
    }


@app.get("/api/gtt/list")
def list_gtts(db: Session = Depends(get_db), token: str = Depends(verify_token)):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    active_trades = db.query(TradeJournal).filter(TradeJournal.status == TradeStatus.ACTIVE.value).all()
    gtts = []
    
    for trade in active_trades:
        if trade.gtt_order_ids:
            gtt_ids = json.loads(trade.gtt_order_ids)
            for gtt_id in gtt_ids:
                legs_data = json.loads(trade.legs_data)
                instrument_info = next((leg for leg in legs_data if leg.get('action') == 'SELL'), legs_data[0] if legs_data else None)
                
                gtts.append({
                    "gtt_id": gtt_id,
                    "strategy_id": trade.strategy_id,
                    "instrument_token": instrument_info.get('instrument_token') if instrument_info else None,
                    "trading_symbol": instrument_info.get('instrument_token', '').split('|')[-1] if instrument_info else None,
                    "type": "MULTIPLE",
                    "status": "ACTIVE"
                })
    
    return {"gtt_orders": gtts}


@app.delete("/api/gtt/{gtt_id}")
def cancel_gtt(gtt_id: str, token: str = Depends(verify_token)):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    success = volguard_system.executor.cancel_gtt_orders([gtt_id])
    return {"success": success, "gtt_id": gtt_id}


@app.websocket("/api/ws/subscribe")
async def websocket_subscribe(websocket: WebSocket, token: str = Depends(verify_token)):
    await volguard_system.ws_manager.connect(websocket)
    
    subscribed = set()
    
    try:
        while True:
            data = await websocket.receive_json()
            
            if data.get("action") == "subscribe":
                instruments = data.get("instruments", [])
                mode = data.get("mode", "ltpc")
                
                new_instruments = [i for i in instruments if i not in subscribed]
                if new_instruments:
                    success = volguard_system.fetcher.subscribe_market_data(new_instruments, mode)
                    if success:
                        subscribed.update(new_instruments)
                
                await websocket.send_json({
                    "type": "subscription_result",
                    "action": "subscribe",
                    "instruments": instruments,
                    "success": True,
                    "subscribed_count": len(subscribed)
                })
                
            elif data.get("action") == "unsubscribe":
                instruments = data.get("instruments", [])
                
                to_unsubscribe = [i for i in instruments if i in subscribed]
                if to_unsubscribe:
                    success = volguard_system.fetcher.unsubscribe_market_data(to_unsubscribe)
                    if success:
                        for i in to_unsubscribe:
                            subscribed.discard(i)
                
                await websocket.send_json({
                    "type": "subscription_result",
                    "action": "unsubscribe",
                    "instruments": instruments,
                    "success": True,
                    "subscribed_count": len(subscribed)
                })
                
            elif data.get("action") == "change_mode":
                instruments = data.get("instruments", [])
                mode = data.get("mode", "ltpc")
                
                to_change = [i for i in instruments if i in subscribed]
                if to_change:
                    volguard_system.fetcher.market_streamer.change_mode(to_change, mode)
                
                await websocket.send_json({
                    "type": "subscription_result",
                    "action": "change_mode",
                    "instruments": instruments,
                    "mode": mode,
                    "success": True
                })
                
    except WebSocketDisconnect:
        if subscribed:
            volguard_system.fetcher.unsubscribe_market_data(list(subscribed))
        volguard_system.ws_manager.disconnect(websocket)


@app.websocket("/api/ws/market/{instrument_key}")
async def websocket_market_data(websocket: WebSocket, instrument_key: str, token: str = Depends(verify_token)):
    await volguard_system.ws_manager.connect(websocket)
    try:
        volguard_system.fetcher.subscribe_market_data([instrument_key], "ltpc")
        
        while True:
            price = volguard_system.fetcher.get_ltp_with_fallback(instrument_key)
            
            await websocket.send_json({
                "type": "market_update",
                "instrument_key": instrument_key,
                "data": {
                    "ltp": price,
                    "timestamp": datetime.now().isoformat()
                }
            })
            await asyncio.sleep(1)
            
    except WebSocketDisconnect:
        volguard_system.fetcher.unsubscribe_market_data([instrument_key])
        volguard_system.ws_manager.disconnect(websocket)


@app.websocket("/api/ws/portfolio")
async def websocket_portfolio(websocket: WebSocket, token: str = Depends(verify_token)):
    await volguard_system.ws_manager.connect(websocket)
    try:
        while True:
            positions = volguard_system.fetcher.get_live_positions()
            
            await websocket.send_json({
                "type": "portfolio_update",
                "data": {
                    "positions": positions,
                    "timestamp": datetime.now().isoformat()
                }
            })
            await asyncio.sleep(5)
            
    except WebSocketDisconnect:
        volguard_system.ws_manager.disconnect(websocket)


@app.get("/api/market/status")
def get_market_status(
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    return volguard_system.fetcher.smart_fetcher.get_market_status()

@app.get("/api/market/last-price/{instrument_key}")
def get_last_price(
    instrument_key: str,
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    price = volguard_system.fetcher.get_ltp_with_fallback(instrument_key)
    market_status = volguard_system.fetcher.smart_fetcher.get_market_status()
    
    return {
        "success": True,
        "instrument_key": instrument_key,
        "last_price": price,
        "timestamp": datetime.now().isoformat(),
        "market_status": market_status,
        "data_source": "smart_fallback"
    }

@app.get("/api/market/bulk-last-price")
def get_bulk_last_price(
    instruments: str = "NSE_INDEX|Nifty 50,NSE_INDEX|India VIX",
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    instrument_list = [i.strip() for i in instruments.split(',')]
    prices = volguard_system.fetcher.get_bulk_ltp_with_fallback(instrument_list)
    market_status = volguard_system.fetcher.smart_fetcher.get_market_status()
    
    return {
        "success": True,
        "prices": prices,
        "timestamp": datetime.now().isoformat(),
        "market_status": market_status,
        "data_source": "smart_fallback"
    }

@app.get("/api/market/ohlc/{instrument_key}")
def get_ohlc(
    instrument_key: str,
    interval: str = "1d",
    token: str = Depends(verify_token)
):
    if not volguard_system:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    ohlc = volguard_system.fetcher.get_ohlc_with_fallback(instrument_key, interval)
    
    return {
        "success": True,
        "instrument_key": instrument_key,
        "interval": interval,
        "data": ohlc,
        "timestamp": datetime.now().isoformat()
    }

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
        
        market_status = volguard_system.fetcher.smart_fetcher.get_market_status()
        
        market_status_display = {
            "nifty_spot": round(analysis['vol_metrics'].spot, 2) if analysis['vol_metrics'].spot > 0 else "Market Closed",
            "india_vix": round(analysis['vol_metrics'].vix, 2) if analysis['vol_metrics'].vix > 0 else "Market Closed",
            "market_open": market_status['is_open'],
            "message": market_status['message']
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
            "market_status": market_status_display,
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
            except Exception as e:
                logger.error(f"Analytics failed: {e}")
                raise HTTPException(status_code=503, detail="Analytics initializing...")
        
        daily_ctx = volguard_system.json_cache.get_context() or {}
        market_status = volguard_system.fetcher.smart_fetcher.get_market_status()
        
        vol = analytics.get('vol_metrics')
        ext = analytics.get('external_metrics')
        
        def fmt_cr(val):
            return f"₹{val/10000000:+.2f} Cr" if val else "N/A"
            
        def fmt_pct(val):
            return f"{val:.2f}%" if val is not None and val > 0 else "Market Closed"

        participant_positions = {
            "fii": {
                "direction": ext.fii_sentiment if ext.fii_data else "No data",
                "conviction": ext.fii_conviction,
                "flow_regime": "AGGRESSIVE_BEAR" if ext.fii_conviction == "VERY_HIGH" and ext.fii_sentiment == "BEARISH" else "NEUTRAL",
                "net_change": ext.fii_net_change,
                "net_change_formatted": f"{ext.fii_net_change:+,} contracts" if ext.fii_net_change != 0 else "0 contracts",
                "data_date": ext.fii_data_date
            }
        }
        
        if ext.fii_data:
            participant_positions["participants"] = {}
            for key, data in ext.fii_data.items():
                participant_positions["participants"][key] = {
                    "fut_net": data.fut_net,
                    "call_net": data.opt_net if hasattr(data, 'opt_net') else 0,
                    "put_net": -data.opt_net if hasattr(data, 'opt_net') else 0
                }
        
        warnings = {
            "weekly": [],
            "next_weekly": [],
            "monthly": []
        }
        
        if vol.vov_zscore > 2:
            warning = {
                "type": "VOL_OF_VOL",
                "message": f"VOL-OF-VOL: {vol.vov_zscore}σ - Market unstable",
                "severity": "HIGH"
            }
            warnings["weekly"].append(warning)
            warnings["next_weekly"].append(warning)
            warnings["monthly"].append(warning)
        
        if ext.fii_conviction == "VERY_HIGH" and ext.fii_sentiment == "BEARISH":
            warning = {
                "type": "FII",
                "message": "FII VERY HIGH BEARISH: Heavy selling pressure",
                "severity": "MEDIUM"
            }
            warnings["weekly"].append(warning)
            warnings["next_weekly"].append(warning)
            warnings["monthly"].append(warning)
        
        return {
            "timestamp": datetime.now(pytz.timezone('Asia/Kolkata')).isoformat(),
            "market_status": market_status,
            "time_context": {
                "status": market_status['message'],
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
                "spot": vol.spot if vol.spot > 0 else None,
                "spot_ma20": vol.ma20 if vol.ma20 > 0 else None,
                "vix": vol.vix if vol.vix > 0 else None,
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
            "participant_positions": participant_positions,
            "structure_analysis": {
                "weekly": {
                    "net_gex_formatted": fmt_cr(analytics['struct_weekly'].net_gex),
                    "weighted_gex_formatted": fmt_cr(analytics['struct_weekly'].gex_weighted * 1_000_000),
                    "gex_regime": analytics['struct_weekly'].gex_regime,
                    "gex_ratio_pct": f"{analytics['struct_weekly'].gex_ratio:.4f}%",
                    "pcr_all": analytics['struct_weekly'].pcr,
                    "pcr_atm": analytics['struct_weekly'].pcr_atm,
                    "max_pain": analytics['struct_weekly'].max_pain,
                    "skew_25d": f"{analytics['struct_weekly'].skew_25d:+.2f}%",
                    "skew_regime": analytics['struct_weekly'].skew_regime
                },
                "next_weekly": {
                    "net_gex_formatted": fmt_cr(analytics['struct_next_weekly'].net_gex),
                    "weighted_gex_formatted": fmt_cr(analytics['struct_next_weekly'].gex_weighted * 1_000_000),
                    "gex_regime": analytics['struct_next_weekly'].gex_regime,
                    "gex_ratio_pct": f"{analytics['struct_next_weekly'].gex_ratio:.4f}%",
                    "pcr_all": analytics['struct_next_weekly'].pcr,
                    "pcr_atm": analytics['struct_next_weekly'].pcr_atm,
                    "max_pain": analytics['struct_next_weekly'].max_pain,
                    "skew_25d": f"{analytics['struct_next_weekly'].skew_25d:+.2f}%",
                    "skew_regime": analytics['struct_next_weekly'].skew_regime
                },
                "monthly": {
                    "net_gex_formatted": fmt_cr(analytics['struct_monthly'].net_gex),
                    "weighted_gex_formatted": fmt_cr(analytics['struct_monthly'].gex_weighted * 1_000_000),
                    "gex_regime": analytics['struct_monthly'].gex_regime,
                    "gex_ratio_pct": f"{analytics['struct_monthly'].gex_ratio:.4f}%",
                    "pcr_all": analytics['struct_monthly'].pcr,
                    "pcr_atm": analytics['struct_monthly'].pcr_atm,
                    "max_pain": analytics['struct_monthly'].max_pain,
                    "skew_25d": f"{analytics['struct_monthly'].skew_25d:+.2f}%",
                    "skew_regime": analytics['struct_monthly'].skew_regime
                }
            },
            "option_edges": {
                "weekly": {
                    "atm_iv": fmt_pct(analytics['edge_metrics'].iv_weekly),
                    "vrp_vs_rv": fmt_pct(analytics['edge_metrics'].vrp_rv_weekly),
                    "vrp_vs_garch": fmt_pct(analytics['edge_metrics'].vrp_garch_weekly),
                    "vrp_vs_parkinson": fmt_pct(analytics['edge_metrics'].vrp_park_weekly),
                    "weighted_vrp": fmt_pct(analytics['edge_metrics'].weighted_vrp_weekly),
                    "weighted_vrp_tag": "RICH" if analytics['edge_metrics'].weighted_vrp_weekly > 0 else "CHEAP"
                },
                "next_weekly": {
                    "atm_iv": fmt_pct(analytics['edge_metrics'].iv_next_weekly),
                    "vrp_vs_rv": fmt_pct(analytics['edge_metrics'].vrp_rv_next_weekly),
                    "vrp_vs_garch": fmt_pct(analytics['edge_metrics'].vrp_garch_next_weekly),
                    "vrp_vs_parkinson": fmt_pct(analytics['edge_metrics'].vrp_park_next_weekly),
                    "weighted_vrp": fmt_pct(analytics['edge_metrics'].weighted_vrp_next_weekly),
                    "weighted_vrp_tag": "RICH" if analytics['edge_metrics'].weighted_vrp_next_weekly > 0 else "CHEAP"
                },
                "monthly": {
                    "atm_iv": fmt_pct(analytics['edge_metrics'].iv_monthly),
                    "vrp_vs_rv": fmt_pct(analytics['edge_metrics'].vrp_rv_monthly),
                    "vrp_vs_garch": fmt_pct(analytics['edge_metrics'].vrp_garch_monthly),
                    "vrp_vs_parkinson": fmt_pct(analytics['edge_metrics'].vrp_park_monthly),
                    "weighted_vrp": fmt_pct(analytics['edge_metrics'].weighted_vrp_monthly),
                    "weighted_vrp_tag": "RICH" if analytics['edge_metrics'].weighted_vrp_monthly > 0 else "CHEAP"
                },
                "term_spread_pct": fmt_pct(analytics['edge_metrics'].term_structure_slope),
                "primary_edge": analytics['edge_metrics'].term_structure_regime
            },
            "regime_scores": {
                "weekly": {
                    "composite": {
                        "score": round(analytics['weekly_score'].total_score, 2),
                        "confidence": analytics['weekly_score'].confidence
                    },
                    "components": {
                        "volatility": {"score": round(analytics['weekly_score'].vol_score, 2)},
                        "structure": {"score": round(analytics['weekly_score'].struct_score, 2)},
                        "edge": {"score": round(analytics['weekly_score'].edge_score, 2)}
                    },
                    "score_drivers": analytics['weekly_score'].score_drivers
                },
                "next_weekly": {
                    "composite": {
                        "score": round(analytics['next_weekly_score'].total_score, 2),
                        "confidence": analytics['next_weekly_score'].confidence
                    },
                    "components": {
                        "volatility": {"score": round(analytics['next_weekly_score'].vol_score, 2)},
                        "structure": {"score": round(analytics['next_weekly_score'].struct_score, 2)},
                        "edge": {"score": round(analytics['next_weekly_score'].edge_score, 2)}
                    },
                    "score_drivers": analytics['next_weekly_score'].score_drivers
                },
                "monthly": {
                    "composite": {
                        "score": round(analytics['monthly_score'].total_score, 2),
                        "confidence": analytics['monthly_score'].confidence
                    },
                    "components": {
                        "volatility": {"score": round(analytics['monthly_score'].vol_score, 2)},
                        "structure": {"score": round(analytics['monthly_score'].struct_score, 2)},
                        "edge": {"score": round(analytics['monthly_score'].edge_score, 2)}
                    },
                    "score_drivers": analytics['monthly_score'].score_drivers
                }
            },
            "mandates": {
                "weekly": {
                    "trade_status": "ALLOWED" if analytics['weekly_mandate'].is_trade_allowed else "BLOCKED",
                    "strategy": analytics['weekly_mandate'].suggested_structure,
                    "square_off_instruction": analytics['weekly_mandate'].square_off_instruction,
                    "capital": {
                        "deployment_formatted": f"₹{analytics['weekly_mandate'].deployment_amount:,.0f}",
                        "allocation_pct": DynamicConfig.get("WEEKLY_ALLOCATION_PCT")
                    },
                    "rationale": analytics['weekly_score'].score_drivers,
                    "warnings": warnings["weekly"]
                },
                "next_weekly": {
                    "trade_status": "ALLOWED" if analytics['next_weekly_mandate'].is_trade_allowed else "BLOCKED",
                    "strategy": analytics['next_weekly_mandate'].suggested_structure,
                    "square_off_instruction": analytics['next_weekly_mandate'].square_off_instruction,
                    "capital": {
                        "deployment_formatted": f"₹{analytics['next_weekly_mandate'].deployment_amount:,.0f}",
                        "allocation_pct": DynamicConfig.get("NEXT_WEEKLY_ALLOCATION_PCT")
                    },
                    "rationale": analytics['next_weekly_score'].score_drivers,
                    "warnings": warnings["next_weekly"]
                },
                "monthly": {
                    "trade_status": "ALLOWED" if analytics['monthly_mandate'].is_trade_allowed else "BLOCKED",
                    "strategy": analytics['monthly_mandate'].suggested_structure,
                    "square_off_instruction": analytics['monthly_mandate'].square_off_instruction,
                    "capital": {
                        "deployment_formatted": f"₹{analytics['monthly_mandate'].deployment_amount:,.0f}",
                        "allocation_pct": DynamicConfig.get("MONTHLY_ALLOCATION_PCT")
                    },
                    "rationale": analytics['monthly_score'].score_drivers,
                    "warnings": warnings["monthly"]
                }
            },
            "professional_recommendation": analytics.get('professional_recommendation', {
                "primary": {
                    "expiry_type": "NONE",
                    "strategy": "CASH",
                    "capital_deploy_formatted": "₹0"
                }
            })
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
        
        current_prices = volguard_system.fetcher.get_bulk_ltp_with_fallback(list(set(all_instruments)))
        market_status = volguard_system.fetcher.smart_fetcher.get_market_status()
        
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
                qty = leg.get('filled_quantity', leg['quantity'])
                multiplier = -1 if leg['action'] == 'SELL' else 1
                
                leg_pnl = (current_price - leg['entry_price']) * qty * multiplier
                trade_pnl += leg_pnl
                
                symbol = instrument_key.split("|")[-1] if "|" in instrument_key else instrument_key
                positions_list.append({
                    "symbol": symbol,
                    "qty": qty * (-1 if leg['action'] == 'SELL' else 1),
                    "ltp": round(current_price, 2) if current_price else None,
                    "pnl": round(leg_pnl, 2),
                    "avg_price": round(leg['entry_price'], 2)
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
            "positions": positions_list,
            "market_status": market_status
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
        if config_update.gtt_stop_loss_multiplier is not None:
            updates["GTT_STOP_LOSS_MULTIPLIER"] = config_update.gtt_stop_loss_multiplier
        if config_update.gtt_profit_target_multiplier is not None:
            updates["GTT_PROFIT_TARGET_MULTIPLIER"] = config_update.gtt_profit_target_multiplier
        if config_update.gtt_trailing_gap is not None:
            updates["GTT_TRAILING_GAP"] = config_update.gtt_trailing_gap
        
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

@app.get("/")
def root():
    return {
        "system": "VolGuard v4.0",
        "version": "4.0.0",
        "status": "operational",
        "trading_mode": "OVERNIGHT OPTION SELLING",
        "product_type": "D (Delivery/Carryforward)",
        "square_off": "1 day before expiry @ 14:00 IST",
        "data_source": "Smart Fallback (WebSocket + REST)",
        "auto_trading": "FULLY AUTOMATED",
        "gtt_config": {
            "stop_loss_multiplier": DynamicConfig.get("GTT_STOP_LOSS_MULTIPLIER"),
            "profit_target_multiplier": DynamicConfig.get("GTT_PROFIT_TARGET_MULTIPLIER"),
            "trailing_gap": DynamicConfig.get("GTT_TRAILING_GAP")
        },
        "websocket": {
            "market_streamer": "ACTIVE" if volguard_system and volguard_system.market_streamer_started else "INACTIVE",
            "portfolio_streamer": "ACTIVE" if volguard_system and volguard_system.portfolio_streamer_started else "INACTIVE"
        },
        "endpoints": {
            "market_status": "/api/market/status",
            "last_price": "/api/market/last-price/{instrument_key}",
            "bulk_prices": "/api/market/bulk-last-price",
            "ohlc": "/api/market/ohlc/{instrument_key}",
            "analytics": "/api/dashboard/analytics",
            "professional": "/api/dashboard/professional",
            "live": "/api/live/positions",
            "journal": "/api/journal/history",
            "config": "/api/system/config",
            "logs": "/api/system/logs",
            "correlation": "/api/risk/correlation-report",
            "expiries": "/api/risk/expiries",
            "fii_summary": "/api/fii/summary",
            "fill_quality": "/api/orders/fill-quality",
            "pnl_attribution": "/api/pnl/attribution",
            "gtt_list": "/api/gtt/list",
            "websocket": {
                "market": "/api/ws/market/{instrument_key}",
                "portfolio": "/api/ws/portfolio",
                "subscribe": "/api/ws/subscribe"
            }
        }
    }

@app.get("/api/health")
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = True
    except:
        db_status = False
    
    today = datetime.now().date()
    daily_stats = db.query(DailyStats).filter(DailyStats.date == today).first()
    circuit_breaker_active = daily_stats.circuit_breaker_triggered if daily_stats else False
    
    cache_status = "VALID" if volguard_system and volguard_system.json_cache.is_valid_for_today() else "MISSING"
    
    market_streamer_status = "CONNECTED" if volguard_system and volguard_system.fetcher.market_streamer.is_connected else "DISCONNECTED"
    portfolio_streamer_status = "CONNECTED" if volguard_system and volguard_system.fetcher.portfolio_streamer.is_connected else "DISCONNECTED"
    
    analytics_cache_age = "N/A"
    if volguard_system and volguard_system.analytics_cache._last_calc_time:
        try:
            now = datetime.now(pytz.UTC)
            cache_time = volguard_system.analytics_cache._last_calc_time
            
            if cache_time.tzinfo is None:
                cache_time = pytz.UTC.localize(cache_time)
            
            age_seconds = (now - cache_time).total_seconds()
            analytics_cache_age = int(age_seconds // 60)
        except Exception as e:
            logger.error(f"Error calculating analytics cache age: {e}")
            analytics_cache_age = "ERROR"
    
    return {
        "status": "healthy" if (db_status and not circuit_breaker_active) else "degraded",
        "database": db_status,
        "daily_cache": cache_status,
        "auto_trading": DynamicConfig.get("AUTO_TRADING"),
        "mock_trading": DynamicConfig.get("ENABLE_MOCK_TRADING"),
        "product_type": "D (Overnight)",
        "circuit_breaker": "ACTIVE" if circuit_breaker_active else "NORMAL",
        "data_source": "Smart Fallback",
        "websocket": {
            "market_streamer": market_streamer_status,
            "portfolio_streamer": portfolio_streamer_status,
            "subscribed_instruments": len(volguard_system.fetcher.market_streamer.get_subscribed_instruments()) if volguard_system else 0
        },
        "gtt_config": {
            "stop_loss_multiplier": DynamicConfig.get("GTT_STOP_LOSS_MULTIPLIER"),
            "profit_target_multiplier": DynamicConfig.get("GTT_PROFIT_TARGET_MULTIPLIER"),
            "trailing_gap": DynamicConfig.get("GTT_TRAILING_GAP")
        },
        "analytics_cache_age": analytics_cache_age,
        "timestamp": datetime.now(pytz.UTC).isoformat()
    }


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 80)
    print("VolGuard v4.0")
    print("=" * 80)
    print(f"Trading Mode:    OVERNIGHT OPTION SELLING")
    print(f"Product Type:    D (Delivery/Carryforward)")
    print(f"Base Capital:    ₹{DynamicConfig.get('BASE_CAPITAL'):,.2f}")
    print(f"Auto Trading:    {'ENABLED 🔴' if DynamicConfig.get('AUTO_TRADING') else 'DISABLED 🟡'}")
    print(f"Data Source:     Smart Fallback (WebSocket + REST)")
    print(f"Market Hours:    WebSocket (real-time) / REST API (24/7)")
    print(f"GTT Orders:      Multi-leg with Trailing Stop")
    print(f"   • Stop Loss:     {DynamicConfig.get('GTT_STOP_LOSS_MULTIPLIER')}x")
    print(f"   • Profit Target: {DynamicConfig.get('GTT_PROFIT_TARGET_MULTIPLIER')}x")
    print(f"   • Trailing Gap:  {DynamicConfig.get('GTT_TRAILING_GAP')}")
    print(f"Partial Fills:   Tracked and reconciled")
    print(f"Exit Orders:     Actual MARKET orders with fill price tracking")
    print(f"Square Off:      1 day BEFORE expiry @ 14:00 IST")
    print(f"Expiry Trading:  BLOCKED")
    print(f"Correlation Mgr: Active")
    print(f"POP:             From Upstox SDK")
    print(f"Auth:            Environment fallback with warning")
    print(f"Lot Size:        Dynamic from Upstox (CRITICAL ERROR if missing)")
    print(f"Async:           DB/HTTP in executors")
    print(f"Circuit Breaker: Uses DailyStats flag")
    print(f"Fill Prices:     Actual fill prices used for P&L")
    print(f"Term Structure:  CONTANGO/BACKWARDATION correct")
    print(f"Auto-Entry:      Fully automated trade execution")
    print("=" * 80)
    print(f"API Documentation: http://localhost:{SystemConfig.PORT}/docs")
    print(f"WebSocket Test:   ws://localhost:{SystemConfig.PORT}/api/ws/subscribe")
    print("=" * 80)
    
    uvicorn.run(
        "volguard_final:app",
        host=SystemConfig.HOST,
        port=SystemConfig.PORT,
        log_level="info"
    )
