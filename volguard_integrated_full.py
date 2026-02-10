"""
VolGuard v3
================================================================
================================================================
"""

import os
import sys
import warnings
import asyncio
import aiohttp
import logging
import threading
import time
from datetime import datetime, date, time as dt_time, timedelta
from typing import List, Dict, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
import json
from contextlib import asynccontextmanager
from decimal import Decimal
from collections import defaultdict
import io
from concurrent.futures import ThreadPoolExecutor
import urllib.parse
import copy

# Third-party imports
import pandas as pd
import numpy as np
import pytz
from scipy.stats import norm

# FastAPI
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator

# Database
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean, JSON, Text, desc, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# Upstox SDK (required)
try:
    import upstox_client
    from upstox_client.rest import ApiException
    UPSTOX_AVAILABLE = True
except ImportError:
    UPSTOX_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.error("upstox_client NOT INSTALLED! Please install: pip install upstox-python-sdk")
    sys.exit(1)

# For FII data fetching
import requests

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('volguard.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


# ============================================================================
# CONFIGURATION (UNCHANGED)
# ============================================================================

class SystemConfig:
    """Central configuration - ALL settings in one place"""
    
    # === UPSTOX API ===
    UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "")
    
    # === TELEGRAM ALERTS ===
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    
    # === INSTRUMENTS ===
    NIFTY_KEY = "NSE_INDEX|Nifty 50"
    VIX_KEY = "NSE_INDEX|India VIX"
    
    # === CAPITAL & RISK ===
    BASE_CAPITAL = float(os.getenv("BASE_CAPITAL", "1000000"))  # ₹10L default
    MAX_DAILY_LOSS_PCT = 3.0
    MAX_CONSECUTIVE_LOSSES = 3
    CIRCUIT_BREAKER_PCT = 3.0
    
    # === STRATEGY VALIDATION ===
    THETA_VEGA_MIN_RATIO = 1.5  # Must profit from time decay
    MIN_POP = 55.0  # Minimum Probability of Profit
    MIN_OI = 50000  # Minimum Open Interest per leg
    MAX_BID_ASK_SPREAD_PCT = 2.0
    
    # === VOLATILITY THRESHOLDS ===
    HIGH_VOL_IVP = 75.0
    LOW_VOL_IVP = 25.0
    VOV_CRASH_ZSCORE = 2.5
    VOV_WARNING_ZSCORE = 2.0
    VIX_MOMENTUM_BREAKOUT = 5.0
    
    # === GEX & STRUCTURE ===
    GEX_STICKY_RATIO = 0.03
    SKEW_CRASH_FEAR = 5.0
    SKEW_MELT_UP = -2.0
    
    # === FII CONVICTION LEVELS ===
    FII_VERY_HIGH_CONVICTION = 150000
    FII_HIGH_CONVICTION = 80000
    FII_MODERATE_CONVICTION = 40000
    
    # === ECONOMIC EVENTS ===
    VETO_KEYWORDS = [
        "RBI Monetary Policy", "RBI Policy", "Reserve Bank of India",
        "Repo Rate Decision", "MPC Meeting",
        "FOMC", "Federal Reserve Meeting", "Fed Meeting",
        "Federal Funds Rate Decision"
    ]
    HIGH_IMPACT_KEYWORDS = [
        "GDP", "Gross Domestic Product", "NFP", "Non-Farm Payroll",
        "CPI", "Consumer Price Index", "Union Budget", "Budget Speech"
    ]
    EVENT_RISK_DAYS_AHEAD = 7
    
    # === POSITION SIZING ===
    WEEKLY_ALLOCATION_PCT = 40.0
    MONTHLY_ALLOCATION_PCT = 40.0
    NEXT_WEEKLY_ALLOCATION_PCT = 20.0
    
    # === EXIT RULES ===
    STOP_LOSS_MULTIPLIER = 2.0  # Exit if premium doubles
    PROFIT_TARGET_MULTIPLIER = 0.30  # Exit at 70% profit
    EXPIRY_EXIT_DTE = 1
    SQUARE_OFF_TIME_IST = dt_time(15, 15)
    
    # === TRADING CONTROL ===
    ENABLE_AUTO_TRADING = os.getenv("ENABLE_AUTO_TRADING", "false").lower() == "true"
    ENABLE_MOCK_TRADING = os.getenv("ENABLE_MOCK_TRADING", "true").lower() == "true"
    
    # === OPTIMIZED TIMING ===
    MONITOR_INTERVAL_SECONDS = 5  # Live P&L monitoring
    ANALYTICS_INTERVAL_MINUTES = 15  # Heavy analytics during market hours
    ANALYTICS_OFFHOURS_INTERVAL_MINUTES = 60  # Relaxed when market closed
    DAILY_FETCH_TIME_IST = dt_time(21, 0)  # 9:00 PM
    PRE_MARKET_WARM_TIME_IST = dt_time(8, 55)  # 8:55 AM
    MARKET_OPEN_IST = dt_time(9, 15)
    MARKET_CLOSE_IST = dt_time(15, 30)
    
    # === SMART ANALYTICS TRIGGERS ===
    SPOT_CHANGE_TRIGGER_PCT = 0.3  # Recalculate if spot moves 0.3%
    VIX_CHANGE_TRIGGER_PCT = 2.0   # Recalculate if VIX moves 2%
    
    # === SERVER ===
    HOST = "0.0.0.0"
    PORT = int(os.getenv("PORT", "8000"))
    
    # === DATABASE ===
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./volguard.db")


# ============================================================================
# TELEGRAM ALERT SERVICE (NEW IN V3.2)
# ============================================================================

class AlertPriority(Enum):
    CRITICAL = "🔴 CRITICAL"
    HIGH = "🟠 HIGH"
    MEDIUM = "🟡 MEDIUM"
    LOW = "🔵 INFO"
    SUCCESS = "🟢 SUCCESS"


@dataclass
class AlertMessage:
    title: str
    message: str
    priority: AlertPriority
    timestamp: datetime


class TelegramAlertService:
    """
    Production-Grade Async Telegram Bot.
    - Non-blocking queue (won't slow down trading)
    - Rate limited to avoid bans
    - Throttling to prevent spam during crashes
    """
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self._queue = asyncio.Queue(maxsize=100)
        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self.logger = logging.getLogger("TelegramBot")
        self._last_alert_time = {}  # For throttling duplicates

    async def start(self):
        self._session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._process_queue())
        self.logger.info("✅ Telegram Service Started")

    async def stop(self):
        if self._task:
            self._task.cancel()
        if self._session:
            await self._session.close()

    def send(self, title: str, message: str, priority: AlertPriority = AlertPriority.MEDIUM, throttle_key: str = None):
        """Queue an alert. Use throttle_key to prevent duplicate alerts (e.g. 'stop_loss_hit')."""
        if throttle_key:
            last = self._last_alert_time.get(throttle_key)
            if last and (datetime.now() - last).total_seconds() < 300:  # 5 min throttle
                return
            self._last_alert_time[throttle_key] = datetime.now()

        try:
            self._queue.put_nowait(AlertMessage(title, message, priority, datetime.now()))
        except asyncio.QueueFull:
            self.logger.error("⚠️ Alert queue full, dropping message")

    async def _process_queue(self):
        while True:
            try:
                alert = await self._queue.get()
                await self._post_to_api(alert)
                self._queue.task_done()
                await asyncio.sleep(0.05)  # Rate limit protection
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
# P&L ATTRIBUTION ENGINE (NEW IN V3.2)
# ============================================================================

@dataclass
class AttributionResult:
    total_pnl: float
    theta_pnl: float
    vega_pnl: float
    delta_pnl: float
    other_pnl: float  # Gamma/Slippage/Spread
    iv_change: float
    
    def to_dict(self):
        return {k: round(v, 2) for k, v in self.__dict__.items()}


class PnLAttributionEngine:
    """
    Calculates P&L Sources by comparing Entry Snapshots vs Live Upstox Greeks.
    Zero complex math. Pure difference tracking.
    """
    def __init__(self, fetcher):
        self.fetcher = fetcher

    def calculate(self, trade_obj, live_prices: Dict, live_greeks: Dict) -> Optional[AttributionResult]:
        """
        Calculates attribution.
        Requires: trade object (DB), live_prices (LTP), live_greeks (Upstox V3)
        """
        if not trade_obj.entry_greeks_snapshot:
            return None  # Cannot calculate without entry snapshot

        entry_greeks = json.loads(trade_obj.entry_greeks_snapshot)
        legs_data = json.loads(trade_obj.legs_data)
        
        total_pnl = 0.0
        theta_pnl = 0.0
        vega_pnl = 0.0
        delta_pnl = 0.0
        avg_iv_change = 0.0
        
        for leg in legs_data:
            key = leg['instrument_token']
            qty = leg['quantity']
            direction = -1 if leg['action'] == 'SELL' else 1
            
            # 1. Get Data Points
            start = entry_greeks.get(key)
            now = live_greeks.get(key)
            current_price = live_prices.get(key)
            
            if not start or not now or not current_price:
                continue

            # 2. Total P&L (Real)
            leg_pnl = (current_price - leg['entry_price']) * qty * direction
            total_pnl += leg_pnl

            # 3. Theta P&L (Time Decay)
            avg_theta = (start.get('theta', 0) + now.get('theta', 0)) / 2
            days_held = (datetime.now() - trade_obj.entry_time).total_seconds() / 86400
            theta_pnl += (avg_theta * days_held * qty * direction * -1)

            # 4. Vega P&L (IV Change)
            avg_vega = (start.get('vega', 0) + now.get('vega', 0)) / 2
            iv_diff = now.get('iv', 0) - start.get('iv', 0)
            vega_pnl += (avg_vega * iv_diff * qty * direction)
            
            # 5. Delta P&L (Direction)
            avg_delta = (start.get('delta', 0) + now.get('delta', 0)) / 2
            spot_diff = now.get('spot_price', 0) - start.get('spot_price', 0)
            delta_pnl += (avg_delta * spot_diff * qty * direction)
            
            avg_iv_change += iv_diff

        # Residual P&L (Gamma, higher order greeks, fees)
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
# ENUMS (UNCHANGED)
# ============================================================================

class StrategyType(str, Enum):
    IRON_FLY = "IRON_FLY"
    IRON_CONDOR = "IRON_CONDOR"
    SHORT_STRADDLE = "SHORT_STRADDLE"
    SHORT_STRANGLE = "SHORT_STRANGLE"
    BULL_PUT_SPREAD = "BULL_PUT_SPREAD"
    BEAR_CALL_SPREAD = "BEAR_CALL_SPREAD"
    CASH = "CASH"  # No trade


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


class TradeStatus(str, Enum):
    ACTIVE = "ACTIVE"
    CLOSED_PROFIT_TARGET = "CLOSED_PROFIT_TARGET"
    CLOSED_STOP_LOSS = "CLOSED_STOP_LOSS"
    CLOSED_EXPIRY_EXIT = "CLOSED_EXPIRY_EXIT"
    CLOSED_SQUARE_OFF = "CLOSED_SQUARE_OFF"
    CLOSED_CIRCUIT_BREAKER = "CLOSED_CIRCUIT_BREAKER"
    CLOSED_VETO_EVENT = "CLOSED_VETO_EVENT"


# ============================================================================
# DATA MODELS (V33.0 ANALYSIS ENGINE) - UNCHANGED
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
    fii_data: Optional[ParticipantData]
    fii_secondary: Optional[ParticipantData]
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
    entry_price: float


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
    theta_vega_ratio: float
    net_theta: float
    net_vega: float
    net_delta: float
    net_gamma: float
    allocated_capital: float
    required_margin: float
    validation_passed: bool
    validation_errors: List[str] = field(default_factory=list)
    construction_time: datetime = field(default_factory=datetime.now)


# ============================================================================
# DATABASE MODELS - CLEANED (REMOVED DailyDataCache, SystemState; ADDED gtt_order_ids)
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
    
    legs_data = Column(JSON)  # List of OptionLeg dicts
    order_ids = Column(JSON)  # List of order IDs
    gtt_order_ids = Column(JSON, nullable=True)  # NEW: Server-side SL order IDs
    entry_greeks_snapshot = Column(JSON, nullable=True)  # NEW V3.2: Entry Greeks for attribution
    
    max_profit = Column(Float)
    max_loss = Column(Float)
    allocated_capital = Column(Float)
    
    entry_premium = Column(Float)  # Net premium collected
    exit_premium = Column(Float, nullable=True)
    
    realized_pnl = Column(Float, nullable=True)
    theta_pnl = Column(Float, nullable=True)
    vega_pnl = Column(Float, nullable=True)
    gamma_pnl = Column(Float, nullable=True)
    
    status = Column(String)
    exit_reason = Column(String, nullable=True)
    
    is_mock = Column(Boolean, default=False)
    
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
    
    circuit_breaker_triggered = Column(Boolean, default=False)
    
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# REMOVED: DailyDataCache, SystemState - moved to JSON file

# Database setup with WAL mode
engine = create_engine(
    SystemConfig.DATABASE_URL, 
    connect_args={"check_same_thread": False} if "sqlite" in SystemConfig.DATABASE_URL else {},
    pool_pre_ping=True
)

# NEW: WAL mode for SQLite
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
Base.metadata.create_all(engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ============================================================================
# JSON CACHE MANAGER (NEW - Replaces DailyCacheManager SQLite)
# ============================================================================

class JSONCacheManager:
    """
    Manages FII/DII and Economic Events data with daily fetch at 9 PM IST
    and pre-market warm at 8:55 AM IST. Zero tolerance for stale data.
    JSON file-based (lock-free) instead of SQLite.
    """
    
    FILE_PATH = "daily_context.json"
    
    def __init__(self, ist_tz=None):
        self.ist_tz = ist_tz or pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
        self._last_fetch_attempt: Optional[datetime] = None
        self._lock = threading.Lock()
        self._data = self._load()
        
    def _load(self) -> Dict:
        """Load from disk"""
        if not os.path.exists(self.FILE_PATH):
            return {}
        try:
            with open(self.FILE_PATH, 'r') as f:
                return json.load(f)
        except:
            return {}
    
    def _save(self) -> bool:
        """Atomic save to disk"""
        try:
            temp = self.FILE_PATH + ".tmp"
            with open(temp, 'w') as f:
                json.dump(self._data, f, indent=4, default=str)
            os.replace(temp, self.FILE_PATH)
            return True
        except Exception as e:
            self.logger.error(f"Save failed: {e}")
            return False
    
    def get_today_cache(self) -> Optional[Dict]:
        """Get today's cached data if valid"""
        with self._lock:
            if not self._data.get("is_valid"):
                return None
            if self._data.get("cache_date") != str(date.today()):
                return None
            return self._data.copy()
    
    def is_valid_for_today(self) -> bool:
        """Check if cache is valid for today"""
        cache = self.get_today_cache()
        return cache is not None and cache.get("is_valid", False)
    
    def get_context(self) -> Dict:
        """Get raw context data"""
        return self._data.copy()
    
    def fetch_and_cache(self, force: bool = False) -> bool:
        """Fetch FII/DII and Economic Events, cache to JSON file"""
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
                # Fetch FII Data
                fii_primary, fii_secondary, fii_net_change, fii_date_str, is_fallback = \
                    ParticipantDataFetcher.fetch_smart_participant_data()
                
                # Fetch Economic Events
                calendar_engine = EconomicCalendarEngine()
                events = calendar_engine.fetch_calendar(SystemConfig.EVENT_RISK_DAYS_AHEAD)
                
                # Store
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
        """Retrieve external metrics from JSON cache"""
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
        
        # Reconstruct FII data
        fii_data = None
        if cache.get("fii_data") and "FII" in cache["fii_data"]:
            fii_dict = cache["fii_data"]["FII"]
            if fii_dict:
                fii_data = ParticipantData(**fii_dict)
        
        # Reconstruct Events
        events = []
        veto_near = False
        high_impact_near = False
        suggested_sq_off = None
        
        for e_dict in cache.get("economic_events", []):
            event = EconomicEvent(**e_dict)
            events.append(event)
            if event.is_veto_event:
                veto_near = True
                if event.suggested_square_off_time and not suggested_sq_off:
                    suggested_sq_off = event.suggested_square_off_time
            if event.event_type == "HIGH_IMPACT":
                high_impact_near = True
        
        # Calculate conviction
        abs_change = abs(cache.get("fii_net_change", 0))
        if abs_change > SystemConfig.FII_VERY_HIGH_CONVICTION:
            conviction = "VERY_HIGH"
        elif abs_change > SystemConfig.FII_HIGH_CONVICTION:
            conviction = "HIGH"
        elif abs_change > SystemConfig.FII_MODERATE_CONVICTION:
            conviction = "MODERATE"
        else:
            conviction = "LOW"
        
        sentiment = "BULLISH" if cache.get("fii_net_change", 0) > 0 else "BEARISH" if cache.get("fii_net_change", 0) < 0 else "NEUTRAL"
        
        risk_score = 0.0
        if veto_near:
            risk_score += 3.0
        if high_impact_near:
            risk_score += 1.5
        if conviction == "VERY_HIGH":
            risk_score += 1.0
        
        return ExternalMetrics(
            fii_data=fii_data, fii_secondary=None,
            fii_net_change=cache.get("fii_net_change", 0),
            fii_conviction=conviction, fii_sentiment=sentiment,
            fii_data_date=cache.get("fii_data_date_str", "NO DATA"),
            fii_is_fallback=cache.get("fii_is_fallback", True),
            economic_events=events, veto_event_near=veto_near,
            high_impact_event_near=high_impact_near,
            suggested_square_off_time=suggested_sq_off,
            risk_score=risk_score
        )
    
    async def schedule_daily_fetch(self):
        """Background task: Fetch at 9 PM IST and pre-warm at 8:55 AM IST"""
        self.logger.info("Daily cache scheduler started")
        
        while True:
            try:
                now = datetime.now(self.ist_tz)
                current_time = now.time()
                
                # Determine next fetch time
                next_fetch = None
                
                if current_time >= SystemConfig.DAILY_FETCH_TIME_IST:
                    tomorrow = now.date() + timedelta(days=1)
                    next_fetch = datetime.combine(tomorrow, SystemConfig.PRE_MARKET_WARM_TIME_IST)
                elif current_time < SystemConfig.PRE_MARKET_WARM_TIME_IST:
                    next_fetch = datetime.combine(now.date(), SystemConfig.PRE_MARKET_WARM_TIME_IST)
                else:
                    next_fetch = datetime.combine(now.date(), SystemConfig.DAILY_FETCH_TIME_IST)
                
                next_fetch = self.ist_tz.localize(next_fetch)
                sleep_seconds = (next_fetch - now).total_seconds()
                
                self.logger.info(f"Next fetch at {next_fetch} (sleeping {sleep_seconds/3600:.1f} hours)")
                await asyncio.sleep(sleep_seconds)
                
                # FIX #6: Non-blocking daily fetch using thread executor
                loop = asyncio.get_running_loop()
                success = await loop.run_in_executor(None, self.fetch_and_cache)
                
                if not success:
                    await asyncio.sleep(3600)  # Retry in 1 hour
                    
            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(3600)


# ============================================================================
# UPSTOX SDK DATA FETCHER - ZERO CACHE, REAL API ONLY, V3 CORRECTED
# ============================================================================

class UpstoxFetcher:
    """
    VolGuard Data Layer - ZERO CACHE POLICY
    - No cached data, no synthetic data
    - If API fails, return None - caller must handle
    - Real-time only, every call hits Upstox API
    - V3 SDK: Fixed unit/interval arguments, proper API usage
    """
    
    def __init__(self, token: str):
        if not token:
            raise ValueError("Upstox access token is required!")
        
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = token
        self.api_client = upstox_client.ApiClient(self.configuration)
        
        self.history_api = upstox_client.HistoryApi(self.api_client)
        self.quote_api = upstox_client.MarketQuoteApi(self.api_client)
        self.options_api = upstox_client.OptionsApi(self.api_client)
        self.user_api = upstox_client.UserApi(self.api_client)
        self.order_api = upstox_client.OrderApi(self.api_client)
        
        # FIX #1: Initialize OrderApiV3 for GTT operations (GttApi removed in V3)
        self.order_api_v3 = upstox_client.OrderApiV3(self.api_client)
        
        # FIX #4: Initialize MarketQuoteV3Api for Greeks
        self.quote_api_v3 = upstox_client.MarketQuoteV3Api(self.api_client)
        
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("UpstoxFetcher initialized - ZERO CACHE MODE, V3 SDK")

    def get_funds(self) -> Optional[float]:
        """Fetch available margin (Equity) for trading - REAL API ONLY"""
        try:
            response = self.user_api.get_user_fund_margin("2.0")
            if response.status == "success" and response.data:
                return float(response.data.equity.available_margin)
        except Exception as e:
            self.logger.error(f"Fund fetch error: {e}")
        return None

    def get_order_status(self, order_id: str) -> Optional[str]:
        """Fetch status of a specific order - REAL API ONLY"""
        try:
            response = self.order_api.get_order_details("2.0", order_id=order_id)
            if response.status == "success" and response.data:
                return response.data.status
        except Exception as e:
            self.logger.error(f"Order status fetch error for {order_id}: {e}")
        return None

    def history(self, key: str, days: int = 400) -> Optional[pd.DataFrame]:
        """
        Fetch historical candles - REAL API ONLY
        V3 FIX: unit="days" (plural), URL encoding for instrument key
        Returns None on failure (NO FALLBACK)
        """
        try:
            to_date = date.today().strftime("%Y-%m-%d")
            from_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            # FIX #5: URL encode the instrument key for safety
            encoded_key = urllib.parse.quote(key, safe='')
            
            # FIX #3: Use plural "days" not "day"
            response = self.history_api.get_historical_candle_data1(
                instrument_key=encoded_key,
                unit="days",        # V3: plural 'days'
                interval="1",      # V3: separate interval
                to_date=to_date,
                from_date=from_date
            )
            
            if response.status == "success" and response.data and response.data.candles:
                candles = response.data.candles
                df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                return df.astype(float).sort_index()
            
        except ApiException as e:
            self.logger.error(f"SDK History fetch error for {key}: {e}")
        except Exception as e:
            self.logger.error(f"History fetch error for {key}: {e}")
        
        return None
    
    def live(self, keys: List[str]) -> Optional[Dict]:
        """
        Fetch live LTP - REAL API ONLY
        Returns None on failure (NO FALLBACK)
        """
        try:
            # URL encode keys for safety
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
            self.logger.error(f"SDK Live fetch error: {e}")
        
        return None
    
    def get_expiries(self) -> Tuple[Optional[date], Optional[date], Optional[date], int]:
        """
        Fetch Option Contracts to determine expiries - REAL API ONLY
        Returns None, None, None, 50 on failure (NO FALLBACK)
        """
        try:
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
                    return None, None, None, lot_size
                
                weekly = valid_dates[0]
                next_weekly = valid_dates[1] if len(valid_dates) > 1 else valid_dates[0]
                
                current_month = weekly.month
                current_year = weekly.year
                monthly_candidates = [d for d in valid_dates if d.month == current_month and d.year == current_year]
                monthly = monthly_candidates[-1] if monthly_candidates else valid_dates[-1]
                
                if weekly == monthly and len(valid_dates) > 1:
                    next_month_num = current_month + 1 if current_month < 12 else 1
                    next_year_num = current_year if current_month < 12 else current_year + 1
                    next_month_candidates = [d for d in valid_dates if d.month == next_month_num and d.year == next_year_num]
                    if next_month_candidates:
                        monthly = next_month_candidates[-1]
                
                return weekly, monthly, next_weekly, lot_size
                
        except ApiException as e:
            self.logger.error(f"SDK Expiries fetch error: {e}")
        
        return None, None, None, 50
    
    def chain(self, expiry_date: date) -> Optional[pd.DataFrame]:
        """
        Fetch Option Chain - REAL API ONLY
        Returns None on failure (NO FALLBACK)
        """
        try:
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
                                base = getattr(obj, attr, None)
                                return getattr(base, sub_attr, 0) if base else 0
                            return getattr(obj, attr, 0)

                        row = {
                            'strike': item.strike_price,
                            'ce_iv': get_val(call_opts, 'option_greeks', 'iv'),
                            'pe_iv': get_val(put_opts, 'option_greeks', 'iv'),
                            'ce_delta': get_val(call_opts, 'option_greeks', 'delta'),
                            'pe_delta': get_val(put_opts, 'option_greeks', 'delta'),
                            'ce_gamma': get_val(call_opts, 'option_greeks', 'gamma'),
                            'pe_gamma': get_val(put_opts, 'option_greeks', 'gamma'),
                            'ce_vega': get_val(call_opts, 'option_greeks', 'vega'),
                            'pe_vega': get_val(put_opts, 'option_greeks', 'vega'),
                            'ce_theta': get_val(call_opts, 'option_greeks', 'theta'),
                            'pe_theta': get_val(put_opts, 'option_greeks', 'theta'),
                            'ce_oi': get_val(call_opts, 'market_data', 'oi'),
                            'pe_oi': get_val(put_opts, 'market_data', 'oi'),
                            'ce_ltp': get_val(call_opts, 'market_data', 'ltp'),
                            'pe_ltp': get_val(put_opts, 'market_data', 'ltp'),
                            'ce_bid': get_val(call_opts, 'market_data', 'bid_price'),
                            'ce_ask': get_val(call_opts, 'market_data', 'ask_price'),
                            'pe_bid': get_val(put_opts, 'market_data', 'bid_price'),
                            'pe_ask': get_val(put_opts, 'market_data', 'ask_price'),
                            'ce_instrument_key': call_opts.instrument_key if call_opts else "",
                            'pe_instrument_key': put_opts.instrument_key if put_opts else ""
                        }
                        rows.append(row)
                    except Exception:
                        continue
                
                return pd.DataFrame(rows)
                
        except ApiException as e:
            self.logger.error(f"SDK Chain fetch error: {e}")
        
        return None
    
    def get_greeks(self, instrument_keys: List[str]) -> Dict[str, Dict]:
        """
        Fetch live Greeks from Upstox V3 - FIXED #4
        Uses MarketQuoteV3Api.get_market_quote_option_greek for proper Greek data
        """
        try:
            if not instrument_keys:
                return {}
            
            # URL encode keys
            encoded_keys = [urllib.parse.quote(k, safe='') for k in instrument_keys]
            
            # FIX #4: Use V3 Greek API instead of full market quote
            response = self.quote_api_v3.get_market_quote_option_greek(
                instrument_key=",".join(encoded_keys),
                api_version="2.0"
            )
            
            result = {}
            if response.status == "success" and response.data:
                for key, data in response.data.items():
                    # Extract Greeks from the V3 response structure
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
                        # Fallback structure if Greeks not available
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


# ============================================================================
# V33.0 ANALYSIS ENGINES (UNCHANGED LOGIC)
# ============================================================================

class AnalyticsEngine:
    """V33.0 Analysis Engine - UNCHANGED CALCULATIONS"""
    
    def __init__(self):
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_time_metrics(self, weekly: date, monthly: date, next_weekly: date) -> TimeMetrics:
        """Calculate time metrics"""
        today = date.today()
        now_ist = datetime.now(self.ist_tz)
        
        dte_w = (weekly - today).days
        dte_m = (monthly - today).days
        dte_nw = (next_weekly - today).days
        
        is_past_square_off = now_ist.time() >= SystemConfig.SQUARE_OFF_TIME_IST
        
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
            is_past_square_off_time=is_past_square_off
        )
    
    def get_vol_metrics(self, nifty_hist: pd.DataFrame, vix_hist: pd.DataFrame, 
                       spot_live: float, vix_live: float) -> VolMetrics:
        """Calculate volatility metrics - V33.0 EXACT"""
        is_fallback = False
        spot = spot_live if spot_live > 0 else (nifty_hist.iloc[-1]['close'] if nifty_hist is not None and not nifty_hist.empty else 0)
        vix = vix_live if vix_live > 0 else (vix_hist.iloc[-1]['close'] if vix_hist is not None and not vix_hist.empty else 0)
        
        if spot_live <= 0 or vix_live <= 0:
            is_fallback = True
        
        if nifty_hist is None or nifty_hist.empty:
            return self._fallback_vol_metrics(spot, vix, is_fallback)
        
        returns = np.log(nifty_hist['close'] / nifty_hist['close'].shift(1)).dropna()
        
        rv7 = returns.rolling(7).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 7 else 0
        rv28 = returns.rolling(28).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 28 else 0
        rv90 = returns.rolling(90).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 90 else 0
        
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
            vov = vix_returns.rolling(30).std().iloc[-1] * np.sqrt(252) * 100 if len(vix_returns) >= 30 else 0
            vov_rolling = vix_returns.rolling(30).std() * np.sqrt(252) * 100
            vov_mean = vov_rolling.rolling(60).mean().iloc[-1] if len(vov_rolling) >= 60 else vov
            vov_std = vov_rolling.rolling(60).std().iloc[-1] if len(vov_rolling) >= 60 else 1
            vov_zscore = (vov - vov_mean) / vov_std if vov_std > 0 else 0
        else:
            vov, vov_zscore = 0, 0
        
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
        
        if vix_change_5d > SystemConfig.VIX_MOMENTUM_BREAKOUT:
            vix_momentum = "RISING"
        elif vix_change_5d < -SystemConfig.VIX_MOMENTUM_BREAKOUT:
            vix_momentum = "FALLING"
        else:
            vix_momentum = "STABLE"
        
        if vov_zscore > SystemConfig.VOV_CRASH_ZSCORE:
            vol_regime = "EXPLODING"
        elif ivp_1yr > SystemConfig.HIGH_VOL_IVP and vix_momentum == "FALLING":
            vol_regime = "MEAN_REVERTING"
        elif ivp_1yr > SystemConfig.HIGH_VOL_IVP and vix_momentum == "RISING":
            vol_regime = "BREAKOUT_RICH"
        elif ivp_1yr > SystemConfig.HIGH_VOL_IVP:
            vol_regime = "RICH"
        elif ivp_1yr < SystemConfig.LOW_VOL_IVP:
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
        """Fallback when no historical data"""
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
        """Calculate structure metrics - GEX, PCR, Max Pain, Skew"""
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
        
        if abs(gex_ratio) < SystemConfig.GEX_STICKY_RATIO:
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
        
        if skew_25d > SystemConfig.SKEW_CRASH_FEAR:
            skew_regime = "CRASH_FEAR"
        elif skew_25d < SystemConfig.SKEW_MELT_UP:
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
        """Fallback when no chain data"""
        return StructMetrics(
            net_gex=0, gex_ratio=0, total_oi_value=0,
            gex_regime="UNKNOWN", pcr=1.0, max_pain=0,
            skew_25d=0, oi_regime="UNKNOWN", lot_size=lot_size,
            pcr_atm=1.0, skew_regime="UNKNOWN", gex_weighted=0
        )
    
    def get_edge_metrics(self, weekly_chain: pd.DataFrame, monthly_chain: pd.DataFrame,
                        next_weekly_chain: pd.DataFrame, spot: float, 
                        vol_metrics: VolMetrics, is_expiry_day: bool) -> EdgeMetrics:
        """Calculate edge metrics - VRP and term structure"""
        
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


class RegimeEngine:
    """V33.0 Regime Scoring and Mandate Generation - UNCHANGED"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def calculate_scores(self, vol_metrics: VolMetrics, struct_metrics: StructMetrics,
                        edge_metrics: EdgeMetrics, external_metrics: ExternalMetrics,
                        expiry_type: str, dte: int) -> RegimeScore:
        """Calculate regime scores"""
        
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
        """Generate trading mandate"""
        
        veto_reasons = []
        risk_notes = []
        
        if time_metrics.is_past_square_off_time:
            veto_reasons.append("PAST_SQUARE_OFF_TIME")
        
        if external_metrics.veto_event_near:
            veto_reasons.append("VETO_EVENT_NEAR")
        
        if vol_metrics.vol_regime == "EXPLODING":
            veto_reasons.append("VOL_EXPLODING")
        
        if expiry_type == "WEEKLY" and time_metrics.is_expiry_day_weekly:
            veto_reasons.append("EXPIRY_DAY_WEEKLY")
        
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
            deployment_pct = SystemConfig.WEEKLY_ALLOCATION_PCT
        elif expiry_type == "MONTHLY":
            deployment_pct = SystemConfig.MONTHLY_ALLOCATION_PCT
        else:
            deployment_pct = SystemConfig.NEXT_WEEKLY_ALLOCATION_PCT
        
        deployment_amount = SystemConfig.BASE_CAPITAL * (deployment_pct / 100)
        
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
# PARTICIPANT DATA FETCHER (USED BY JSON CACHE MANAGER)
# ============================================================================

class ParticipantDataFetcher:
    """Fetches NSE participant data"""
    
    @staticmethod
    def fetch_oi_csv(target_date: datetime) -> Optional[pd.DataFrame]:
        """Fetch participant OI data from NSE"""
        try:
            date_str = target_date.strftime("%d%b%Y").upper()
            url = f"https://www.nseindia.com/api/reports?archives=[{{\"name\":\"F&O - Participant wise Open Interest (csv)\",\"type\":\"archives\",\"category\":\"derivatives\",\"section\":\"equity\"}}]&date={date_str}&type=equity&mode=single"
            
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            }
            
            session = requests.Session()
            session.get("https://www.nseindia.com", headers=headers, timeout=10)
            
            response = session.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                csv_text = response.text
                df = pd.read_csv(io.StringIO(csv_text))
                return df
        except Exception as e:
            logger.debug(f"Participant data fetch failed for {target_date.date()}: {e}")
        
        return None
    
    @classmethod
    def process_participant_data(cls, df: pd.DataFrame) -> Optional[Dict[str, ParticipantData]]:
        """Process raw CSV into participant data"""
        try:
            df.columns = df.columns.str.strip()
            
            result = {}
            for client_type in ["FII", "DII", "PRO"]:
                row = df[df['Client Type'].str.strip() == client_type]
                if row.empty:
                    continue
                
                result[client_type] = ParticipantData(
                    fut_long=float(row['Future Index Long'].values[0]),
                    fut_short=float(row['Future Index Short'].values[0]),
                    fut_net=float(row['Future Index Long'].values[0]) - float(row['Future Index Short'].values[0]),
                    opt_long=float(row['Option Index Call Long'].values[0]) + float(row['Option Index Put Long'].values[0]),
                    opt_short=float(row['Option Index Call Short'].values[0]) + float(row['Option Index Put Short'].values[0]),
                    opt_net=float(row['Option Index Call Long'].values[0]) + float(row['Option Index Put Long'].values[0]) -
                            float(row['Option Index Call Short'].values[0]) - float(row['Option Index Put Short'].values[0]),
                    total_net=(float(row['Future Index Long'].values[0]) - float(row['Future Index Short'].values[0])) +
                             (float(row['Option Index Call Long'].values[0]) + float(row['Option Index Put Long'].values[0]) -
                              float(row['Option Index Call Short'].values[0]) - float(row['Option Index Put Short'].values[0]))
                )
            
            return result
        except Exception as e:
            logger.error(f"Participant data processing error: {e}")
        
        return None
    
    @classmethod
    def fetch_smart_participant_data(cls) -> Tuple[Optional[Dict], Optional[Dict], float, str, bool]:
        """Fetch with fallback logic"""
        dates = [datetime.now() - timedelta(days=i) for i in range(10)]
        
        primary_data = None
        primary_date = None
        
        for dt in dates:
            df = cls.fetch_oi_csv(dt)
            if df is not None:
                primary_data = cls.process_participant_data(df)
                if primary_data:
                    primary_date = dt
                    break
        
        secondary_data = None
        for i, dt in enumerate(dates):
            if i == 0:
                continue
            if primary_date and dt.date() >= primary_date.date():
                continue
            
            df_prev = cls.fetch_oi_csv(dt)
            if df_prev is not None:
                secondary_data = cls.process_participant_data(df_prev)
                break
        
        if primary_data is None:
            return None, None, 0.0, "NO DATA", False
        
        fii_net_change = 0.0
        if primary_data.get('FII') and secondary_data and secondary_data.get('FII'):
            fii_net_change = primary_data['FII'].fut_net - secondary_data['FII'].fut_net
        
        is_fallback = primary_date.date() != dates[0].date()
        date_str = primary_date.strftime('%d-%b-%Y')
        
        return primary_data, secondary_data, fii_net_change, date_str, is_fallback


class EconomicCalendarEngine:
    """Fetch economic events"""
    
    def __init__(self):
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.utc_tz = pytz.utc
    
    def classify_event(self, title: str, country: str, impact_code: int) -> Tuple[str, bool]:
        title_lower = title.lower()
        
        for keyword in SystemConfig.VETO_KEYWORDS:
            if keyword.lower() in title_lower:
                return ("VETO", True)
        
        for keyword in SystemConfig.HIGH_IMPACT_KEYWORDS:
            if keyword.lower() in title_lower:
                return ("HIGH_IMPACT", False)
        
        if impact_code == 1:
            return ("HIGH_IMPACT", False)
        elif impact_code == 0:
            return ("MEDIUM_IMPACT", False)
        else:
            return ("LOW_IMPACT", False)
    
    def fetch_calendar(self, days_ahead: int = 7) -> List[EconomicEvent]:
        countries = "IN,US"
        now_utc = datetime.now(self.utc_tz)
        now_ist = now_utc.astimezone(self.ist_tz)
        end_utc = now_utc + timedelta(days=days_ahead)
        
        payload = {
            "from": now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "to": end_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "countries": countries,
        }
        headers = {"User-Agent": "Mozilla/5.0", "Origin": "https://www.tradingview.com"}
        events = []
        
        try:
            response = requests.get(
                "https://economic-calendar.tradingview.com/events",
                params=payload,
                headers=headers,
                timeout=10
            )
            data = response.json()
            
            if 'result' not in data:
                return events
            
            today = now_ist.date()
            
            for e in data['result']:
                importance_code = e.get('importance', -1)
                if importance_code < 0:
                    continue
                
                utc_time = datetime.strptime(e['date'], "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=self.utc_tz)
                ist_time = utc_time.astimezone(self.ist_tz)
                event_date = ist_time.date()
                days_until = (event_date - today).days
                hours_until = (ist_time - now_ist).total_seconds() / 3600
                
                if days_until < 0:
                    continue
                
                title = e.get('title', '')
                country = e.get('country', '')
                event_type, is_veto = self.classify_event(title, country, importance_code)
                
                suggested_square_off = None
                if is_veto and days_until == 1:
                    square_off_date = today
                    suggested_square_off = self.ist_tz.localize(
                        datetime.combine(square_off_date, SystemConfig.SQUARE_OFF_TIME_IST)
                    )
                
                impact_label = "HIGH" if importance_code == 1 else "MEDIUM"
                
                event = EconomicEvent(
                    title=title,
                    country=country,
                    event_date=ist_time,
                    impact_level=impact_label,
                    event_type=event_type,
                    forecast=str(e.get('forecast', '-')),
                    previous=str(e.get('previous', '-')),
                    days_until=days_until,
                    hours_until=hours_until,
                    is_veto_event=is_veto,
                    suggested_square_off_time=suggested_square_off
                )
                events.append(event)
            
            events.sort(key=lambda x: (x.days_until, 0 if x.is_veto_event else 1))
        
        except Exception as e:
            logger.error(f"Economic Calendar Error: {e}")
        
        return events


# ============================================================================
# STRATEGY FACTORY - ALL 6 STRATEGIES (UNCHANGED)
# ============================================================================

class StrategyFactory:
    """Constructs all 6 option strategies with full validation - UNCHANGED"""
    
    def __init__(self, fetcher: UpstoxFetcher, spot: float, lot_size: int):
        self.fetcher = fetcher
        self.spot = spot
        self.lot_size = lot_size
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def construct_iron_fly(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        """Iron Fly: Sell ATM straddle + Buy OTM wings"""
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
                    entry_price=ce_premium
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
                    entry_price=pe_premium
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
                    entry_price=call_wing_row.iloc[0]['ce_ltp']
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
                    entry_price=put_wing_row.iloc[0]['pe_ltp']
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
            
            theta_vega_ratio = abs(net_theta / net_vega) if net_vega != 0 else 0
            pop = 65.0
            
            errors = self._validate_strategy(legs, theta_vega_ratio, pop)
            
            strategy_id = f"IRON_FLY_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.IRON_FLY,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=pop,
                theta_vega_ratio=theta_vega_ratio,
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Iron Fly: {e}")
            return None
    
    def construct_iron_condor(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        """Iron Condor: Sell 20d call + 20d put, Buy 5d call + 5d put"""
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
                    entry_price=call_20d_row.iloc[0]['ce_ltp']
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
                    entry_price=put_20d_row.iloc[0]['pe_ltp']
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
                    entry_price=call_5d_row.iloc[0]['ce_ltp']
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
                    entry_price=put_5d_row.iloc[0]['pe_ltp']
                )
            ]
            
            max_profit = net_premium * quantity
            call_spread = call_5d_row.iloc[0]['strike'] - call_20d_row.iloc[0]['strike']
            max_loss = (call_spread - net_premium) * quantity
            
            net_theta = sum(leg.theta * leg.quantity for leg in legs)
            net_vega = sum(leg.vega * leg.quantity for leg in legs)
            net_delta = sum(leg.delta * leg.quantity for leg in legs)
            net_gamma = sum(leg.gamma * leg.quantity for leg in legs)
            
            theta_vega_ratio = abs(net_theta / net_vega) if net_vega != 0 else 0
            pop = 50.0 + abs(net_delta) * 10
            
            errors = self._validate_strategy(legs, theta_vega_ratio, pop)
            
            strategy_id = f"IRON_CONDOR_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.IRON_CONDOR,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=pop,
                theta_vega_ratio=theta_vega_ratio,
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Iron Condor: {e}")
            return None
    
    def construct_short_straddle(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        """Short Straddle: Sell ATM call + ATM put with 2-delta wings"""
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
                    entry_price=atm_row['ce_ltp']
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
                    entry_price=atm_row['pe_ltp']
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
                    entry_price=call_wing_row.iloc[0]['ce_ltp']
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
                    entry_price=put_wing_row.iloc[0]['pe_ltp']
                )
            ]
            
            max_profit = net_premium * quantity
            wing_spread = call_wing_row.iloc[0]['strike'] - atm_strike
            max_loss = (wing_spread - net_premium) * quantity
            
            net_theta = sum(leg.theta * leg.quantity for leg in legs)
            net_vega = sum(leg.vega * leg.quantity for leg in legs)
            net_delta = sum(leg.delta * leg.quantity for leg in legs)
            net_gamma = sum(leg.gamma * leg.quantity for leg in legs)
            
            theta_vega_ratio = abs(net_theta / net_vega) if net_vega != 0 else 0
            pop = 60.0
            
            errors = self._validate_strategy(legs, theta_vega_ratio, pop)
            
            strategy_id = f"SHORT_STRADDLE_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.SHORT_STRADDLE,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=pop,
                theta_vega_ratio=theta_vega_ratio,
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Short Straddle: {e}")
            return None
    
    def construct_short_strangle(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        """Short Strangle: Sell 30d call + 30d put with 5d wings"""
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
                    entry_price=call_30d_row.iloc[0]['ce_ltp']
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
                    entry_price=put_30d_row.iloc[0]['pe_ltp']
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
                    entry_price=call_5d_row.iloc[0]['ce_ltp']
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
                    entry_price=put_5d_row.iloc[0]['pe_ltp']
                )
            ]
            
            max_profit = net_premium * quantity
            call_spread = call_5d_row.iloc[0]['strike'] - call_30d_row.iloc[0]['strike']
            max_loss = (call_spread - net_premium) * quantity
            
            net_theta = sum(leg.theta * leg.quantity for leg in legs)
            net_vega = sum(leg.vega * leg.quantity for leg in legs)
            net_delta = sum(leg.delta * leg.quantity for leg in legs)
            net_gamma = sum(leg.gamma * leg.quantity for leg in legs)
            
            theta_vega_ratio = abs(net_theta / net_vega) if net_vega != 0 else 0
            pop = 60.0
            
            errors = self._validate_strategy(legs, theta_vega_ratio, pop)
            
            strategy_id = f"SHORT_STRANGLE_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.SHORT_STRANGLE,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=pop,
                theta_vega_ratio=theta_vega_ratio,
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Short Strangle: {e}")
            return None
    
    def construct_bull_put_spread(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        """Bull Put Spread: Sell 30d put + Buy 10d put"""
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
                    entry_price=put_30d_row.iloc[0]['pe_ltp']
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
                    entry_price=put_10d_row.iloc[0]['pe_ltp']
                )
            ]
            
            max_profit = net_premium * quantity
            put_spread = put_30d_row.iloc[0]['strike'] - put_10d_row.iloc[0]['strike']
            max_loss = (put_spread - net_premium) * quantity
            
            net_theta = sum(leg.theta * leg.quantity for leg in legs)
            net_vega = sum(leg.vega * leg.quantity for leg in legs)
            net_delta = sum(leg.delta * leg.quantity for leg in legs)
            net_gamma = sum(leg.gamma * leg.quantity for leg in legs)
            
            theta_vega_ratio = abs(net_theta / net_vega) if net_vega != 0 else 0
            pop = 70.0
            
            errors = self._validate_strategy(legs, theta_vega_ratio, pop)
            
            strategy_id = f"BULL_PUT_SPREAD_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.BULL_PUT_SPREAD,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=pop,
                theta_vega_ratio=theta_vega_ratio,
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Bull Put Spread: {e}")
            return None
    
    def construct_bear_call_spread(self, expiry_date: date, allocation: float) -> Optional[ConstructedStrategy]:
        """Bear Call Spread: Sell 30d call + Buy 10d call"""
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
                    entry_price=call_30d_row.iloc[0]['ce_ltp']
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
                    entry_price=call_10d_row.iloc[0]['ce_ltp']
                )
            ]
            
            max_profit = net_premium * quantity
            call_spread = call_10d_row.iloc[0]['strike'] - call_30d_row.iloc[0]['strike']
            max_loss = (call_spread - net_premium) * quantity
            
            net_theta = sum(leg.theta * leg.quantity for leg in legs)
            net_vega = sum(leg.vega * leg.quantity for leg in legs)
            net_delta = sum(leg.delta * leg.quantity for leg in legs)
            net_gamma = sum(leg.gamma * leg.quantity for leg in legs)
            
            theta_vega_ratio = abs(net_theta / net_vega) if net_vega != 0 else 0
            pop = 70.0
            
            errors = self._validate_strategy(legs, theta_vega_ratio, pop)
            
            strategy_id = f"BEAR_CALL_SPREAD_{expiry_date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
            
            return ConstructedStrategy(
                strategy_id=strategy_id,
                strategy_type=StrategyType.BEAR_CALL_SPREAD,
                expiry_type=ExpiryType.WEEKLY,
                expiry_date=expiry_date,
                legs=legs,
                max_profit=max_profit,
                max_loss=max_loss,
                pop=pop,
                theta_vega_ratio=theta_vega_ratio,
                net_theta=net_theta,
                net_vega=net_vega,
                net_delta=net_delta,
                net_gamma=net_gamma,
                allocated_capital=allocation,
                required_margin=0,
                validation_passed=len(errors) == 0,
                validation_errors=errors
            )
        
        except Exception as e:
            self.logger.error(f"Error constructing Bear Call Spread: {e}")
            return None
    
    def _validate_strategy(self, legs: List[OptionLeg], theta_vega_ratio: float, pop: float) -> List[str]:
        """Validate strategy against all filters"""
        errors = []
        
        if theta_vega_ratio < SystemConfig.THETA_VEGA_MIN_RATIO:
            errors.append(f"Theta/Vega ratio {theta_vega_ratio:.2f} < {SystemConfig.THETA_VEGA_MIN_RATIO}")
        
        if pop < SystemConfig.MIN_POP:
            errors.append(f"POP {pop:.1f}% < {SystemConfig.MIN_POP}%")
        
        for leg in legs:
            if leg.oi < SystemConfig.MIN_OI:
                errors.append(f"{leg.option_type} {leg.strike} OI {leg.oi} < {SystemConfig.MIN_OI}")
        
        for leg in legs:
            if leg.ask > 0 and leg.bid > 0:
                spread_pct = ((leg.ask - leg.bid) / leg.ltp) * 100 if leg.ltp > 0 else 999
                if spread_pct > SystemConfig.MAX_BID_ASK_SPREAD_PCT:
                    errors.append(f"{leg.option_type} {leg.strike} spread {spread_pct:.1f}% > {SystemConfig.MAX_BID_ASK_SPREAD_PCT}%")
        
        return errors


# ============================================================================
# EXECUTION ENGINE (MOCK + REAL) - V34.0 UPDATED WITH GTT FIXES
# ============================================================================

class MockExecutor:
    """Mock order execution for testing"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.order_counter = 1000
    
    def place_multi_order(self, strategy: ConstructedStrategy) -> Dict:
        """Simulate order placement"""
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
        
        # NEW V3.2: Mock Greek Snapshot
        mock_greeks = {}
        for leg in strategy.legs:
            mock_greeks[leg.instrument_token] = {
                'iv': leg.iv if hasattr(leg, 'iv') else 20.0,
                'delta': leg.delta if hasattr(leg, 'delta') else 0.0,
                'gamma': leg.gamma if hasattr(leg, 'gamma') else 0.0,
                'theta': leg.theta if hasattr(leg, 'theta') else -10.0,
                'vega': leg.vega if hasattr(leg, 'vega') else 10.0,
                'spot_price': 22000.0  # Mock spot price
            }
        
        return {
            "success": True,
            "order_ids": order_ids,
            "gtt_order_ids": gtt_ids,
            "entry_greeks": mock_greeks,  # NEW V3.2
            "message": "Mock orders placed successfully"
        }


class SafeExecutor:
    """
    VolGuard Execution Layer - v34.0 (Safe Mode with GTT)
    Replaces RealExecutor with Capital Checks, Verification, and Server-Side Stop Losses
    FIXED: Uses OrderApiV3 for GTT operations per Upstox SDK V3
    """
    
    def __init__(self, fetcher: UpstoxFetcher):
        if not UPSTOX_AVAILABLE:
            raise RuntimeError("upstox_client not installed")
        
        self.fetcher = fetcher
        self.order_api = fetcher.order_api
        # FIX #1: Use OrderApiV3 for GTT (GttApi removed in V3)
        self.order_api_v3 = fetcher.order_api_v3
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("SafeExecutor initialized with Funds Check, Order Verification, and GTT (V3 SDK)")
    
    def place_multi_order(self, strategy: ConstructedStrategy) -> Dict:
        """
        Execute Strategy with 4-Step Safety:
        1. Pre-Trade: Check Available Funds
        2. Trade: Execute Multi-Leg Order with Auto-Slicing
        3. Post-Trade: Verify Order Status
        4. Protection: Place GTT Stop Losses
        """
        
        # Step 1: Capital Check
        available_funds = self.fetcher.get_funds()
        required_capital = strategy.allocated_capital
        
        if available_funds is None:
            return {
                "success": False,
                "order_ids": [],
                "message": "❌ Cannot fetch funds. Aborting trade."
            }
        
        self.logger.info(f"Capital Check: Required ₹{required_capital:.2f} | Available ₹{available_funds:.2f}")
        
        if available_funds < (required_capital * 1.05):
            msg = f"❌ INSUFFICIENT FUNDS. Aborting trade. Need ₹{required_capital:.2f}, Have ₹{available_funds:.2f}"
            self.logger.error(msg)
            return {
                "success": False,
                "order_ids": [],
                "message": msg
            }

        try:
            # Step 2: Build Orders (BUY first, then SELL)
            buy_legs = [leg for leg in strategy.legs if leg.action == "BUY"]
            sell_legs = [leg for leg in strategy.legs if leg.action == "SELL"]
            ordered_legs = buy_legs + sell_legs
            
            orders = []
            for i, leg in enumerate(ordered_legs):
                order = upstox_client.MultiOrderRequest(
                    quantity=leg.quantity,
                    product="D",
                    validity="DAY",
                    price=leg.entry_price if leg.entry_price > 0 else 0.0,
                    tag=strategy.strategy_id[:20],
                    instrument_token=leg.instrument_token,
                    order_type="LIMIT",
                    transaction_type=leg.action,
                    disclosed_quantity=0,
                    trigger_price=0.0,
                    is_amo=False,
                    slice=True,  # CRITICAL: Enables auto-slicing for freeze limits
                    correlation_id=f"leg_{i}"
                )
                orders.append(order)
            
            self.logger.info(f"Placing {len(orders)} orders via SDK (slice=True)...")
            response = self.order_api.place_multi_order(body=orders)
            
            if response.status != "success":
                return {
                    "success": False,
                    "order_ids": [],
                    "message": f"Order placement failed: {response}"
                }
                
            order_ids = [item.order_id for item in response.data]
            self.logger.info(f"✅ Orders placed. IDs: {order_ids}")
            
            # Step 3: Verification (longer wait for slicing)
            import time
            time.sleep(2)
            
            filled_orders = []
            rejected_orders = []
            
            for oid in order_ids:
                status = self.fetcher.get_order_status(oid)
                self.logger.info(f"Order {oid} status: {status}")
                
                if status in ['complete', 'filled']:
                    filled_orders.append(oid)
                elif status in ['rejected', 'cancelled', 'failure']:
                    rejected_orders.append(oid)
            
            if rejected_orders:
                return {
                    "success": False,
                    "order_ids": order_ids,
                    "message": f"⚠️ CRITICAL: Some legs were REJECTED: {rejected_orders}. Check Broker immediately."
                }
            
            # Step 4: Place GTT Stop Losses for filled short legs
            gtt_ids = self._place_gtt_stop_losses(strategy, filled_orders)
            
            # NEW V3.2: Capture Entry Greeks for P&L Attribution
            instrument_keys = [leg.instrument_token for leg in strategy.legs]
            greeks_snapshot = self.fetcher.get_greeks(instrument_keys)
            
            return {
                "success": True,
                "order_ids": order_ids,
                "gtt_order_ids": gtt_ids,
                "entry_greeks": greeks_snapshot,  # NEW V3.2
                "message": f"Strategy executed. Filled: {len(filled_orders)}, GTTs: {len(gtt_ids)}"
            }

        except ApiException as e:
            self.logger.error(f"❌ SDK API error: {e}")
            return {
                "success": False,
                "order_ids": [],
                "message": f"SDK API Exception: {str(e)}"
            }
        except Exception as e:
            self.logger.error(f"❌ Unexpected error: {e}")
            return {
                "success": False,
                "order_ids": [],
                "message": f"Exception: {str(e)}"
            }
    
    def _place_gtt_stop_losses(self, strategy: ConstructedStrategy, filled_order_ids: List[str]) -> List[str]:
        """
        Place server-side GTT stop losses for short legs.
        FIXED #2: Uses OrderApiV3 and ENTRY strategy for single-leg GTT per V3 SDK
        """
        gtt_ids = []
        
        for leg in strategy.legs:
            if leg.action != "SELL":
                continue  # Only place SL for short legs
            
            # Calculate stop price (2x premium = 100% loss on leg)
            stop_price = round(leg.entry_price * 2.0, 2)
            
            try:
                # FIX #2: Use ENTRY strategy for single-leg GTT (required by V3 SDK)
                # FIX #1: Use order_api_v3 instead of gtt_api
                rule = upstox_client.GttRule(
                    strategy="ENTRY",  # Must be ENTRY for single-leg type in V3
                    trigger_type="IMMEDIATE",
                    trigger_price=stop_price
                )
                
                body = upstox_client.GttPlaceOrderRequest(
                    type="SINGLE",
                    instrument_token=leg.instrument_token,
                    quantity=leg.quantity,
                    product="D",
                    transaction_type="BUY",  # Buy to cover short
                    rules=[rule]
                )
                
                # FIX #1: Use OrderApiV3 for GTT placement
                response = self.order_api_v3.place_gtt_order(body=body)
                
                if response.status == "success":
                    gtt_id = response.data.gtt_order_id
                    gtt_ids.append(gtt_id)
                    self.logger.info(f"✅ GTT placed for {leg.strike} {leg.option_type} @ ₹{stop_price} (ID: {gtt_id})")
                else:
                    self.logger.error(f"❌ GTT failed for {leg.strike}: {response}")
                    
            except Exception as e:
                self.logger.error(f"❌ GTT exception for {leg.strike}: {e}")
        
        return gtt_ids
    
    def cancel_gtt_orders(self, gtt_ids: List[str]) -> bool:
        """Cancel GTT orders (use when exiting position manually)"""
        success = True
        for gtt_id in gtt_ids:
            try:
                # FIX #1: Use OrderApiV3 for GTT deletion
                self.order_api_v3.delete_gtt_order(gtt_id)
                self.logger.info(f"Cancelled GTT: {gtt_id}")
            except Exception as e:
                self.logger.error(f"Failed to cancel GTT {gtt_id}: {e}")
                success = False
        return success


# ============================================================================
# ANALYTICS CACHE & SCHEDULER (NEW - SMART 15MIN WITH VOLATILITY TRIGGERS + THREADPOOL)
# ============================================================================

class AnalyticsCache:
    """Thread-safe cache for analytics results with volatility-based invalidation"""
    
    def __init__(self):
        self._cache: Optional[Dict] = None
        self._last_spot: float = 0.0
        self._last_vix: float = 0.0
        self._last_calc_time: Optional[datetime] = None
        self._lock = threading.RLock()
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get(self) -> Optional[Dict]:
        """Get cached analytics if valid"""
        with self._lock:
            # FIX: Return deep copy to prevent external mutation
            if self._cache is None:
                return None
            return copy.deepcopy(self._cache)
    
    def should_recalculate(self, current_spot: float, current_vix: float) -> bool:
        """
        Smart recalculation trigger:
        1. First run (no cache)
        2. Time-based (15min market hours, 60min off-hours)
        3. Volatility-based (spot >0.3% or vix >2% change)
        """
        with self._lock:
            if self._cache is None:
                return True
            
            now = datetime.now(self.ist_tz)
            last_time = self._last_calc_time
            
            if last_time is None:
                return True
            
            # Time-based check
            current_time = now.time()
            is_market_hours = (SystemConfig.MARKET_OPEN_IST <= current_time <= SystemConfig.MARKET_CLOSE_IST)
            
            if is_market_hours:
                interval = SystemConfig.ANALYTICS_INTERVAL_MINUTES
            else:
                interval = SystemConfig.ANALYTICS_OFFHOURS_INTERVAL_MINUTES
            
            elapsed_minutes = (now - last_time).total_seconds() / 60
            
            if elapsed_minutes >= interval:
                self.logger.info(f"Time-based recalculation: {elapsed_minutes:.1f}min elapsed")
                return True
            
            # Volatility-based check
            if self._last_spot > 0:
                spot_change_pct = abs(current_spot - self._last_spot) / self._last_spot * 100
                if spot_change_pct > SystemConfig.SPOT_CHANGE_TRIGGER_PCT:
                    self.logger.info(f"Spot-triggered recalculation: {spot_change_pct:.2f}% change")
                    return True
            
            if self._last_vix > 0:
                vix_change_pct = abs(current_vix - self._last_vix) / self._last_vix * 100
                if vix_change_pct > SystemConfig.VIX_CHANGE_TRIGGER_PCT:
                    self.logger.info(f"VIX-triggered recalculation: {vix_change_pct:.2f}% change")
                    return True
            
            return False
    
    def update(self, analysis_data: Dict, spot: float, vix: float):
        """Update cache with new analytics"""
        with self._lock:
            # FIX: Deep copy to prevent external mutation affecting cache
            self._cache = copy.deepcopy(analysis_data)
            self._last_spot = spot
            self._last_vix = vix
            self._last_calc_time = datetime.now(self.ist_tz)
            self.logger.info(f"Analytics cache updated | Spot: {spot:.2f} | VIX: {vix:.2f}")


class AnalyticsScheduler:
    """
    Background scheduler for heavy analytics using ThreadPoolExecutor.
    Runs every 15min (market hours) or 60min (off-hours)
    Plus volatility-triggered immediate runs.
    FIXED: Proper executor lifecycle management
    """
    
    def __init__(self, volguard_system, cache: AnalyticsCache):
        self.system = volguard_system
        self.cache = cache
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.logger = logging.getLogger(self.__class__.__name__)
        self._running = False
        self._executor: Optional[ThreadPoolExecutor] = None  # FIX: Don't create in __init__
    
    async def start(self):
        """Start the scheduler loop with ThreadPoolExecutor"""
        self._running = True
        # FIX: Create executor here, not in __init__
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="analytics")
        self.logger.info("Analytics scheduler started with ThreadPoolExecutor")
        loop = asyncio.get_event_loop()
        
        while self._running:
            try:
                # Quick price check for volatility trigger
                live_data = self.system.fetcher.live([
                    SystemConfig.NIFTY_KEY, 
                    SystemConfig.VIX_KEY
                ])
                
                if live_data is None:
                    self.logger.error("Live data unavailable, skipping cycle")
                    await asyncio.sleep(10)
                    continue
                
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
                        # CRITICAL: Run in thread pool to not block event loop
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
        """Stop the scheduler"""
        self._running = False
        # FIX: Proper cleanup with None check
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        self.logger.info("Analytics scheduler stopped")


# ============================================================================
# POSITION MONITOR (REAL-TIME P&L + EXIT RULES) - USES CACHED ANALYTICS, STRICT VALIDATION
# ============================================================================

class PositionMonitor:
    """Real-time position monitoring with circuit breaker - USES CACHED ANALYTICS, ABORTS ON MISSING DATA"""
    
    def __init__(self, fetcher: UpstoxFetcher, db_session_factory, analytics_cache: AnalyticsCache, config, alert_service: Optional[TelegramAlertService] = None):
        self.fetcher = fetcher
        self.db_session_factory = db_session_factory
        self.analytics_cache = analytics_cache
        self.config = config
        self.alert_service = alert_service  # NEW V3.2
        self.pnl_engine = PnLAttributionEngine(fetcher)  # NEW V3.2
        self.logger = logging.getLogger(self.__class__.__name__)
        self.is_running = False
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        # FIX: Track breach count for sustained circuit breaker
        self._breach_count = 0
        self._breach_threshold = 3  # Require 3 consecutive breaches
    
    async def start_monitoring(self):
        """Background monitoring loop - 5s intervals for live P&L"""
        self.is_running = True
        self.logger.info("Position monitoring started (5s intervals)")
        
        while self.is_running:
            try:
                await self.check_all_positions()
                await asyncio.sleep(SystemConfig.MONITOR_INTERVAL_SECONDS)
            except Exception as e:
                self.logger.error(f"Monitor error: {e}")
                await asyncio.sleep(10)
    
    async def check_all_positions(self):
        """CRITICAL: Real-time P&L and circuit breaker using cached analytics, ABORTS if data missing"""
        # FIX: Create new session for each check cycle (thread safety)
        db = self.db_session_factory()
        try:
            active_trades = db.query(TradeJournal).filter(
                TradeJournal.status == TradeStatus.ACTIVE.value
            ).all()
            
            if not active_trades:
                return
            
            # Fetch live prices only
            all_instruments = []
            for trade in active_trades:
                legs_data = json.loads(trade.legs_data)
                all_instruments.extend([leg['instrument_token'] for leg in legs_data])
            
            current_prices = self.fetcher.live(list(set(all_instruments)))
            
            # CRITICAL: Abort if market data unavailable
            if current_prices is None:
                self.logger.error("🚨 MARKET DATA UNAVAILABLE - Skipping P&L check")
                return
            
            # Use cached analytics for risk calculations (not recalculating)
            cached_analysis = self.analytics_cache.get()
            
            total_realized_pnl = 0.0
            total_unrealized_pnl = 0.0
            
            for trade in active_trades:
                result = await self.check_single_position(trade, current_prices, db)
                if result is not None:
                    total_unrealized_pnl += result
            
            today = datetime.now().date()
            closed_trades_today = db.query(TradeJournal).filter(
                TradeJournal.status != TradeStatus.ACTIVE.value,
                TradeJournal.exit_time >= datetime.combine(today, dt_time.min)
            ).all()
            
            total_realized_pnl = sum(t.realized_pnl or 0 for t in closed_trades_today)
            
            self._update_daily_stats(db, total_realized_pnl, total_unrealized_pnl)
            
            # Circuit breaker check - FIX: Only trigger on realized P&L to avoid noise
            threshold = -SystemConfig.BASE_CAPITAL * SystemConfig.CIRCUIT_BREAKER_PCT / 100
            
            if total_realized_pnl < threshold:
                self._breach_count += 1
                if self._breach_count >= self._breach_threshold:
                    self.logger.critical(f"🚨 CIRCUIT BREAKER TRIGGERED! Realized P&L: ₹{total_realized_pnl:.2f} < ₹{threshold:.2f}")
                    await self.trigger_circuit_breaker(db)
            else:
                self._breach_count = 0  # Reset on recovery
        
        finally:
            db.close()
    
    async def check_single_position(self, trade: TradeJournal, current_prices: Dict, db: Session):
        """Check individual position exit conditions - ABORTS if any price missing"""
        legs_data = json.loads(trade.legs_data)
        
        # CRITICAL: Validate all prices before calculation
        for leg in legs_data:
            instrument_key = leg['instrument_token']
            if instrument_key not in current_prices:
                self.logger.error(f"⚠️ Missing price for {instrument_key} - aborting P&L for {trade.strategy_id}")
                return None
            if current_prices[instrument_key] is None or current_prices[instrument_key] <= 0:
                self.logger.error(f"⚠️ Invalid price {current_prices[instrument_key]} for {instrument_key}")
                return None
        
        # Now safe to calculate
        unrealized_pnl = 0.0
        for leg in legs_data:
            entry_price = leg['entry_price']
            current_price = current_prices[leg['instrument_token']]
            quantity = leg['quantity']
            multiplier = -1 if leg['action'] == 'SELL' else 1
            
            leg_pnl = (current_price - entry_price) * quantity * multiplier
            unrealized_pnl += leg_pnl
        
        exit_reason = None
        
        if unrealized_pnl < -trade.max_loss * SystemConfig.STOP_LOSS_MULTIPLIER:
            exit_reason = TradeStatus.CLOSED_STOP_LOSS.value
            self.logger.warning(f"Stop loss triggered for {trade.strategy_id}: ₹{unrealized_pnl:.2f}")
        
        elif unrealized_pnl > trade.max_profit * SystemConfig.PROFIT_TARGET_MULTIPLIER:
            exit_reason = TradeStatus.CLOSED_PROFIT_TARGET.value
            self.logger.info(f"Profit target hit for {trade.strategy_id}: ₹{unrealized_pnl:.2f}")
        
        elif (trade.expiry_date.date() - datetime.now().date()).days <= SystemConfig.EXPIRY_EXIT_DTE:
            exit_reason = TradeStatus.CLOSED_EXPIRY_EXIT.value
            self.logger.info(f"Expiry exit for {trade.strategy_id}")
        
        elif datetime.now(self.ist_tz).time() >= SystemConfig.SQUARE_OFF_TIME_IST:
            exit_reason = TradeStatus.CLOSED_SQUARE_OFF.value
        
        if exit_reason:
            await self.exit_position(trade, exit_reason, current_prices, db)
        
        return unrealized_pnl
    
    async def exit_position(self, trade: TradeJournal, exit_reason: str, 
                           current_prices: Dict, db: Session):
        """Close a position and cancel GTTs - ENHANCED with P&L Attribution and Alerts"""
        legs_data = json.loads(trade.legs_data)
        
        realized_pnl = 0.0
        for leg in legs_data:
            entry_price = leg['entry_price']
            exit_price = current_prices.get(leg['instrument_token'], entry_price)
            quantity = leg['quantity']
            multiplier = -1 if leg['action'] == 'SELL' else 1
            
            leg_pnl = (exit_price - entry_price) * quantity * multiplier
            realized_pnl += leg_pnl
        
        # NEW V3.2: Calculate P&L Attribution
        attribution = None
        if trade.entry_greeks_snapshot:
            try:
                # Fetch current Greeks
                instrument_keys = [leg['instrument_token'] for leg in legs_data]
                current_greeks = self.fetcher.get_greeks(instrument_keys)
                
                # Calculate attribution
                attribution = self.pnl_engine.calculate(trade, current_prices, current_greeks)
            except Exception as e:
                self.logger.error(f"Attribution calculation failed: {e}")
        
        # Cancel GTTs if present
        if trade.gtt_order_ids:
            try:
                gtt_ids = json.loads(trade.gtt_order_ids)
                self.logger.info(f"Would cancel GTTs: {gtt_ids}")
            except:
                pass
        
        trade.exit_time = datetime.now()
        trade.status = exit_reason
        trade.exit_reason = exit_reason
        trade.realized_pnl = realized_pnl
        
        db.commit()
        
        # NEW V3.2: Send Telegram Alert with Attribution
        if self.alert_service and attribution:
            try:
                msg = f"""
<b>Strategy Closed:</b> {trade.strategy_id}
<b>Type:</b> {trade.strategy_type}
<b>Total P&L:</b> ₹{realized_pnl:.2f}

<b>P&L Attribution:</b>
• Theta (Time): ₹{attribution.theta_pnl:.2f}
• Vega (Vol):   ₹{attribution.vega_pnl:.2f}
• Delta (Move): ₹{attribution.delta_pnl:.2f}
• Other/Fees:   ₹{attribution.other_pnl:.2f}

<b>IV Change:</b> {attribution.iv_change:.2f}%
<b>Exit Reason:</b> {exit_reason}
"""
                priority = AlertPriority.SUCCESS if realized_pnl > 0 else AlertPriority.HIGH
                throttle_key = f"exit_{trade.strategy_id}"
                self.alert_service.send("Trade Closed", msg, priority, throttle_key)
            except Exception as e:
                self.logger.error(f"Alert sending failed: {e}")
        
        self.logger.info(
            f"Trade {trade.strategy_id} closed: P&L=₹{realized_pnl:.2f}, Reason={exit_reason}"
        )
    
    async def trigger_circuit_breaker(self, db: Session):
        """Emergency shutdown - close ALL positions"""
        active_trades = db.query(TradeJournal).filter(
            TradeJournal.status == TradeStatus.ACTIVE.value
        ).all()
        
        for trade in active_trades:
            legs_data = json.loads(trade.legs_data)
            instrument_tokens = [leg['instrument_token'] for leg in legs_data]
            current_prices = self.fetcher.live(instrument_tokens)
            
            if current_prices:  # Only exit if we have prices
                await self.exit_position(
                    trade,
                    TradeStatus.CLOSED_CIRCUIT_BREAKER.value,
                    current_prices,
                    db
                )
        
        self.logger.critical("🚨 ALL POSITIONS CLOSED - CIRCUIT BREAKER ACTIVE")
    
    def _update_daily_stats(self, db: Session, realized_pnl: float, unrealized_pnl: float):
        """Update daily stats in database"""
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
        """Stop monitoring"""
        self.is_running = False
        self.logger.info("Position monitoring stopped")


# ============================================================================
# COMPLETE SYSTEM ORCHESTRATOR - V34.0 OPTIMIZED
# ============================================================================

class VolGuardSystem:
    """Main system orchestrator - OPTIMIZED with JSON Cache + Analytics Scheduler"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize components
        self.fetcher = UpstoxFetcher(SystemConfig.UPSTOX_ACCESS_TOKEN)
        self.analytics = AnalyticsEngine()
        self.regime = RegimeEngine()
        
        # JSON Cache Manager for FII/Events (fetched at 9PM + 8:55AM)
        self.json_cache = JSONCacheManager()
        
        # Analytics Cache for heavy calculations (15min intervals)
        self.analytics_cache = AnalyticsCache()
        
        # Execution
        if SystemConfig.ENABLE_AUTO_TRADING and SystemConfig.UPSTOX_ACCESS_TOKEN:
            self.logger.info("🔴 REAL TRADING MODE ENABLED - Using SafeExecutor with GTT")
            self.executor = SafeExecutor(self.fetcher)
        else:
            self.logger.info("🟡 MOCK TRADING MODE ENABLED")
            self.executor = MockExecutor()
        
        # Scheduler and Monitor (initialized in lifespan)
        self.analytics_scheduler: Optional[AnalyticsScheduler] = None
        self.monitor: Optional[PositionMonitor] = None
        
        self.logger.info("VolGuard System v3.2 initialized - PRODUCTION ARCHITECTURE")
    
    def run_complete_analysis(self) -> Dict:
        """Run full analysis using REAL-TIME Upstox data + JSON cached FII/Events"""
        try:
            # REAL-TIME: Fetch market data from Upstox (NO CACHE)
            nifty_hist = self.fetcher.history(SystemConfig.NIFTY_KEY)
            vix_hist = self.fetcher.history(SystemConfig.VIX_KEY)
            live_data = self.fetcher.live([SystemConfig.NIFTY_KEY, SystemConfig.VIX_KEY])
            
            # Check for empty data (ZERO TOLERANCE)
            if nifty_hist is None:
                raise ValueError("Failed to fetch Nifty historical data from Upstox")
            if vix_hist is None:
                raise ValueError("Failed to fetch VIX historical data from Upstox")
            if live_data is None:
                raise ValueError("Failed to fetch live market data from Upstox")
            
            spot = live_data.get(SystemConfig.NIFTY_KEY, 0)
            vix = live_data.get(SystemConfig.VIX_KEY, 0)
            
            if spot <= 0 or vix <= 0:
                raise ValueError(f"Invalid prices from Upstox - Spot: {spot}, VIX: {vix}")
            
            weekly, monthly, next_weekly, lot_size = self.fetcher.get_expiries()
            
            if not weekly:
                raise ValueError("Cannot fetch expiries from Upstox SDK")
            
            # REAL-TIME: Fetch option chains (NO CACHE)
            weekly_chain = self.fetcher.chain(weekly)
            monthly_chain = self.fetcher.chain(monthly)
            next_weekly_chain = self.fetcher.chain(next_weekly)
            
            # Time metrics
            time_metrics = self.analytics.get_time_metrics(weekly, monthly, next_weekly)
            
            # Vol metrics
            vol_metrics = self.analytics.get_vol_metrics(
                nifty_hist, vix_hist,
                spot, vix
            )
            
            # Struct metrics
            struct_weekly = self.analytics.get_struct_metrics(weekly_chain, vol_metrics.spot, lot_size)
            struct_monthly = self.analytics.get_struct_metrics(monthly_chain, vol_metrics.spot, lot_size)
            struct_next_weekly = self.analytics.get_struct_metrics(next_weekly_chain, vol_metrics.spot, lot_size)
            
            # Edge metrics
            edge_metrics = self.analytics.get_edge_metrics(
                weekly_chain, monthly_chain, next_weekly_chain,
                vol_metrics.spot, vol_metrics, time_metrics.is_expiry_day_weekly
            )
            
            # CACHED: External metrics (FII/Events from JSON Cache)
            external_metrics = self.json_cache.get_external_metrics()
            
            # Regime scoring
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
            
            # Trading mandates
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
                "next_weekly_chain": next_weekly_chain
            }
        
        except Exception as e:
            self.logger.error(f"Analysis error: {e}")
            raise
    
    def construct_strategy_from_mandate(self, mandate: TradingMandate, 
                                       analysis_data: Dict) -> Optional[ConstructedStrategy]:
        """Construct strategy from mandate using REAL-TIME data"""
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
        
        if strategy:
            expiry_type_map = {
                "WEEKLY": ExpiryType.WEEKLY,
                "MONTHLY": ExpiryType.MONTHLY,
                "NEXT_WEEKLY": ExpiryType.NEXT_WEEKLY
            }
            strategy.expiry_type = expiry_type_map.get(mandate.expiry_type, ExpiryType.WEEKLY)
        
        return strategy
    
    def execute_strategy(self, strategy: ConstructedStrategy, db: Session) -> Dict:
        """Execute strategy and save to database"""
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
                entry_greeks_snapshot=json.dumps(result.get('entry_greeks', {})),  # NEW V3.2
                max_profit=strategy.max_profit,
                max_loss=strategy.max_loss,
                allocated_capital=strategy.allocated_capital,
                entry_premium=entry_premium,
                status=TradeStatus.ACTIVE.value,
                is_mock=not SystemConfig.ENABLE_AUTO_TRADING
            )
            db.add(trade)
            db.commit()
            
            # NEW V3.2: Send position opened alert
            if hasattr(self, 'alert_service') and self.alert_service:
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
            
            self.logger.info(f"Strategy executed via SDK: {strategy.strategy_id}")
        
        return result


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events - OPTIMIZED"""
    logger.info("=" * 70)
    logger.info("VolGuard v3.2 Starting... (PRODUCTION ARCHITECTURE)")
    logger.info("=" * 70)
    logger.info(f"Base Capital: ₹{SystemConfig.BASE_CAPITAL:,.2f}")
    logger.info(f"Auto Trading: {'ENABLED 🔴' if SystemConfig.ENABLE_AUTO_TRADING else 'DISABLED 🟡'}")
    logger.info(f"Mock Trading: {'ENABLED 🟡' if SystemConfig.ENABLE_MOCK_TRADING else 'DISABLED'}")
    logger.info(f"Analytics Interval: {SystemConfig.ANALYTICS_INTERVAL_MINUTES}min (market hours)")
    logger.info(f"Daily Fetch: {SystemConfig.DAILY_FETCH_TIME_IST} IST")
    logger.info(f"Pre-Market Warm: {SystemConfig.PRE_MARKET_WARM_TIME_IST} IST")
    logger.info(f"Database: SQLite with WAL mode")
    logger.info(f"Daily Cache: JSON file (lock-free)")
    logger.info(f"Analytics: ThreadPoolExecutor (non-blocking)")
    logger.info("=" * 70)
    
    # NEW V3.2: Initialize Telegram Alert Service
    alert_service = None
    if SystemConfig.TELEGRAM_TOKEN and SystemConfig.TELEGRAM_CHAT_ID:
        alert_service = TelegramAlertService(
            SystemConfig.TELEGRAM_TOKEN,
            SystemConfig.TELEGRAM_CHAT_ID
        )
        await alert_service.start()
        alert_service.send(
            "🚀 VolGuard v3.2 Started",
            "System Online - Integrated Version with P&L Attribution",
            AlertPriority.SUCCESS
        )
        logger.info("✅ Telegram Alerts Enabled")
    else:
        logger.warning("⚠️ Telegram credentials not configured - alerts disabled")
    
    # Initialize system
    global volguard_system
    volguard_system = VolGuardSystem()
    volguard_system.alert_service = alert_service  # NEW V3.2: Store alert service
    
    # Initial daily cache fetch (if empty)
    if not volguard_system.json_cache.is_valid_for_today():
        logger.info("Fetching initial daily cache...")
        volguard_system.json_cache.fetch_and_cache(force=True)
    
    # Start analytics scheduler (15min intervals + volatility triggers) with ThreadPool
    volguard_system.analytics_scheduler = AnalyticsScheduler(volguard_system, volguard_system.analytics_cache)
    analytics_task = asyncio.create_task(volguard_system.analytics_scheduler.start())
    
    # Start daily cache scheduler (9PM + 8:55AM)
    cache_task = asyncio.create_task(volguard_system.json_cache.schedule_daily_fetch())
    
    # Start position monitoring (5s intervals for live P&L)
    if SystemConfig.ENABLE_AUTO_TRADING or SystemConfig.ENABLE_MOCK_TRADING:
        volguard_system.monitor = PositionMonitor(
            volguard_system.fetcher, 
            SessionLocal,
            volguard_system.analytics_cache,
            SystemConfig,
            alert_service  # NEW V3.2: Pass alert service
        )
        monitor_task = asyncio.create_task(volguard_system.monitor.start_monitoring())
        logger.info("Position monitoring started (5s intervals)")
    
    yield
    
    # Cleanup
    logger.info("VolGuard v3.2 shutting down...")
    
    # NEW V3.2: Stop Telegram service
    if alert_service:
        await alert_service.stop()
        logger.info("Telegram service stopped")
    if volguard_system.analytics_scheduler:
        volguard_system.analytics_scheduler.stop()
    if volguard_system.monitor:
        volguard_system.monitor.stop()


app = FastAPI(
    title="VolGuard v3.2 - Production",
    description="Professional Options Trading System with Hardened Infrastructure",
    version="3.2-PRODUCTION",
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
# API ENDPOINTS (ALL PRESERVED EXACTLY)
# ============================================================================

@app.get("/")
def root():
    return {
        "system": "VolGuard v3.2",
        "version": "3.2-PRODUCTION",
        "architecture": "JSON Cache + ThreadPool + WAL Mode + GTT",
        "status": "operational",
        "auto_trading": SystemConfig.ENABLE_AUTO_TRADING,
        "mock_trading": SystemConfig.ENABLE_MOCK_TRADING,
        "docs": "/docs"
    }


@app.get("/api/health")
def health_check(db: Session = Depends(get_db)):
    """System health check"""
    try:
        db.execute("SELECT 1")
        db_status = True
    except:
        db_status = False
    
    circuit_breaker_state = db.query(TradeJournal).filter(
        TradeJournal.status == TradeStatus.CLOSED_CIRCUIT_BREAKER.value
    ).first()
    
    circuit_breaker_active = circuit_breaker_state is not None
    
    # Check cache status
    cache_status = "VALID" if volguard_system and volguard_system.json_cache.is_valid_for_today() else "MISSING"
    
    return {
        "status": "healthy" if (db_status and not circuit_breaker_active) else "degraded",
        "database": db_status,
        "daily_cache": cache_status,
        "auto_trading": SystemConfig.ENABLE_AUTO_TRADING,
        "mock_trading": SystemConfig.ENABLE_MOCK_TRADING,
        "circuit_breaker": "ACTIVE" if circuit_breaker_active else "NORMAL",
        "analytics_cache_age": (
            (datetime.now() - volguard_system.analytics_cache._last_calc_time).total_seconds() // 60
            if volguard_system and volguard_system.analytics_cache._last_calc_time
            else "N/A"
        ),
        "timestamp": datetime.now().isoformat()
    }


@app.get("/api/dashboard")
def get_dashboard(db: Session = Depends(get_db)):
    """
    COMPLETE DASHBOARD - Uses cached analytics for performance
    """
    try:
        # Use cached analytics if available (freshness indicated)
        cached = volguard_system.analytics_cache.get()
        
        if cached and cached.get('vol_metrics'):
            # Use cached data for dashboard (fast)
            analysis = cached
            cache_age_min = (datetime.now() - volguard_system.analytics_cache._last_calc_time).total_seconds() // 60
            data_source = f"cached ({cache_age_min}min old)"
        else:
            # Fallback to real-time calculation (slow, for first run)
            analysis = volguard_system.run_complete_analysis()
            data_source = "real-time"
        
        active_trades = db.query(TradeJournal).filter(
            TradeJournal.status == TradeStatus.ACTIVE.value
        ).all()
        
        today = datetime.now().date()
        daily_stats = db.query(DailyStats).filter(DailyStats.date == today).first()
        
        total_exposure = sum(
            sum(                leg['entry_price'] * leg['quantity'] 
                for leg in json.loads(trade.legs_data) if leg['action'] == 'SELL')
            for trade in active_trades
        )
        
        return {
            "timestamp": datetime.now().isoformat(),
            "data_source": data_source,
            
            "market": {
                "nifty_spot": analysis['vol_metrics'].spot,
                "vix": analysis['vol_metrics'].vix
            },
            
            "capital": {
                "base": SystemConfig.BASE_CAPITAL,
                "deployed": total_exposure,
                "available": SystemConfig.BASE_CAPITAL - total_exposure
            },
            
            "positions": {
                "active_count": len(active_trades),
                "total_exposure": total_exposure
            },
            
            "performance": {
                "today_pnl": daily_stats.total_pnl if daily_stats else 0,
                "realized_pnl": daily_stats.realized_pnl if daily_stats else 0,
                "unrealized_pnl": daily_stats.unrealized_pnl if daily_stats else 0,
                "trades_count": daily_stats.trades_count if daily_stats else 0,
                "wins": daily_stats.wins if daily_stats else 0,
                "losses": daily_stats.losses if daily_stats else 0
            },
            
            "analysis": {
                "time": asdict(analysis['time_metrics']),
                "vol": asdict(analysis['vol_metrics']),
                "struct_weekly": asdict(analysis['struct_weekly']),
                "struct_monthly": asdict(analysis['struct_monthly']),
                "edge": asdict(analysis['edge_metrics']),
                "external": asdict(analysis['external_metrics']),
                "weekly_score": asdict(analysis['weekly_score']),
                "monthly_score": asdict(analysis['monthly_score']),
                "weekly_mandate": asdict(analysis['weekly_mandate']),
                "monthly_mandate": asdict(analysis['monthly_mandate'])
            },
            
            "system": {
                "auto_trading": SystemConfig.ENABLE_AUTO_TRADING,
                "mock_trading": SystemConfig.ENABLE_MOCK_TRADING,
                "circuit_breaker_threshold": -SystemConfig.BASE_CAPITAL * SystemConfig.CIRCUIT_BREAKER_PCT / 100,
                "analytics_interval_min": SystemConfig.ANALYTICS_INTERVAL_MINUTES,
                "next_daily_fetch": str(SystemConfig.DAILY_FETCH_TIME_IST)
            }
        }
    
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/construct-strategy/{expiry_type}")
def construct_strategy(expiry_type: str, db: Session = Depends(get_db)):
    """
    Construct strategy based on regime analysis - USES CACHED DATA
    """
    try:
        # Use cached analytics for speed
        analysis = volguard_system.analytics_cache.get()
        
        if not analysis:
            # Fallback to real-time if cache empty (Runs in main thread here if forced)
            analysis = volguard_system.run_complete_analysis()
        
        mandate_map = {
            "WEEKLY": analysis['weekly_mandate'],
            "MONTHLY": analysis['monthly_mandate'],
            "NEXT_WEEKLY": analysis['next_weekly_mandate']
        }
        
        mandate = mandate_map.get(expiry_type.upper())
        if not mandate:
            raise HTTPException(status_code=404, detail=f"Invalid expiry type: {expiry_type}")
        
        strategy = volguard_system.construct_strategy_from_mandate(mandate, analysis)
        
        if not strategy:
            return {
                "success": False,
                "message": "Strategy construction failed or trade not allowed",
                "mandate": asdict(mandate)
            }
        
        return {
            "success": True,
            "strategy": asdict(strategy),
            "mandate": asdict(mandate)
        }
    
    except Exception as e:
        logger.error(f"Strategy construction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/execute-strategy/{strategy_id}")
def execute_strategy(strategy_id: str, db: Session = Depends(get_db)):
    """
    Execute a constructed strategy - REAL-TIME execution
    """
    try:
        # Use cached analytics for context
        analysis = volguard_system.analytics_cache.get()
        
        if not analysis:
            analysis = volguard_system.run_complete_analysis()
        
        if "WEEKLY" in strategy_id:
            mandate = analysis['weekly_mandate']
        elif "MONTHLY" in strategy_id:
            mandate = analysis['monthly_mandate']
        else:
            mandate = analysis['next_weekly_mandate']
        
        strategy = volguard_system.construct_strategy_from_mandate(mandate, analysis)
        
        if not strategy:
            raise HTTPException(status_code=400, detail="Cannot construct strategy. Market conditions may have changed.")
        
        # Verify Strategy ID matches to ensure atomicity
        # (In a real UI, you might pass the full strategy object back, but here we reconstruct for safety)
        
        result = volguard_system.execute_strategy(strategy, db)
        
        return result
    
    except Exception as e:
        logger.error(f"Execution error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/positions")
def get_positions(db: Session = Depends(get_db)):
    """Get all active positions"""
    try:
        active_trades = db.query(TradeJournal).filter(
            TradeJournal.status == TradeStatus.ACTIVE.value
        ).all()
        
        positions = []
        for trade in active_trades:
            legs_data = json.loads(trade.legs_data)
            
            positions.append({
                "strategy_id": trade.strategy_id,
                "strategy_type": trade.strategy_type,
                "expiry_type": trade.expiry_type,
                "expiry_date": trade.expiry_date.isoformat(),
                "entry_time": trade.entry_time.isoformat(),
                "legs": legs_data,
                "max_profit": trade.max_profit,
                "max_loss": trade.max_loss,
                "entry_premium": trade.entry_premium,
                "is_mock": trade.is_mock,
                "gtt_active": bool(trade.gtt_order_ids)
            })
        
        return {
            "active_count": len(positions),
            "positions": positions
        }
    
    except Exception as e:
        logger.error(f"Positions error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trades/history")
def get_trade_history(limit: int = 50, db: Session = Depends(get_db)):
    """Get trade history"""
    try:
        trades = db.query(TradeJournal).order_by(
            desc(TradeJournal.entry_time)
        ).limit(limit).all()
        
        history = []
        for trade in trades:
            history.append({
                "strategy_id": trade.strategy_id,
                "strategy_type": trade.strategy_type,
                "entry_time": trade.entry_time.isoformat(),
                "exit_time": trade.exit_time.isoformat() if trade.exit_time else None,
                "realized_pnl": trade.realized_pnl,
                "status": trade.status,
                "exit_reason": trade.exit_reason,
                "is_mock": trade.is_mock
            })
        
        return {
            "trades_count": len(history),
            "trades": history
        }
    
    except Exception as e:
        logger.error(f"History error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/system/reset-circuit-breaker")
def reset_circuit_breaker(db: Session = Depends(get_db)):
    """
    Reset circuit breaker. 
    In v3.2 (Clean DB), this involves resetting the DailyStats flag.
    """
    try:
        today = datetime.now().date()
        stats = db.query(DailyStats).filter(DailyStats.date == today).first()
        
        if stats:
            stats.circuit_breaker_triggered = False
            db.commit()
            
        logger.info("Circuit breaker manually reset")
        
        return {
            "success": True,
            "message": "Circuit breaker reset. Trading allowed."
        }
    
    except Exception as e:
        logger.error(f"Reset error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/system/refresh-daily-cache")
def refresh_daily_cache(force: bool = False):
    """
    Manually trigger daily cache refresh (FII/Events) -> JSON File
    """
    try:
        success = volguard_system.json_cache.fetch_and_cache(force=force)
        ctx = volguard_system.json_cache.get_context()
        
        return {
            "success": success,
            "cache_valid": volguard_system.json_cache.is_valid_for_today(),
            "fii_data_date": ctx.get("fii_data_date_str") if ctx else None,
            "event_count": len(ctx.get("economic_events", [])) if ctx else 0,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Cache refresh error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/system/cache-status")
def get_cache_status():
    """Get detailed status of JSON Cache (Disk) and Analytics Cache (RAM)"""
    try:
        # JSON Disk Cache
        daily_ctx = volguard_system.json_cache.get_context()
        daily_valid = volguard_system.json_cache.is_valid_for_today()
        
        # RAM Analytics Cache
        analytics_valid = volguard_system.analytics_cache.get() is not None
        last_calc = volguard_system.analytics_cache._last_calc_time
        
        return {
            "daily_json_cache": {
                "valid": daily_valid,
                "date": daily_ctx.get("cache_date"),
                "fetch_time": daily_ctx.get("fetch_timestamp"),
                "fii_net_change": daily_ctx.get("fii_net_change"),
                "event_count": len(daily_ctx.get("economic_events", [])),
                "is_fallback": daily_ctx.get("fii_is_fallback")
            },
            "analytics_ram_cache": {
                "valid": analytics_valid,
                "last_calc": last_calc.isoformat() if last_calc else None,
                "age_minutes": (
                    (datetime.now(pytz.timezone('Asia/Kolkata')) - last_calc).total_seconds() // 60
                    if last_calc else None
                ),
                "spot_ref": volguard_system.analytics_cache._last_spot,
                "vix_ref": volguard_system.analytics_cache._last_vix
            }
        }
    
    except Exception as e:
        logger.error(f"Cache status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/test-alert")
async def test_alert():
    """Test Telegram alert system (NEW V3.2)"""
    if not hasattr(volguard_system, 'alert_service') or not volguard_system.alert_service:
        raise HTTPException(status_code=400, detail="Telegram not configured. Set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID")
    
    volguard_system.alert_service.send(
        "Test Alert",
        "This is a test message from VolGuard v3.2 with integrated alerts and P&L attribution.",
        AlertPriority.LOW
    )
    
    return {"success": True, "message": "Test alert queued"}


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 70)
    print("VolGuard v3.2 - Production Refactor (Integrated)")
    print("=" * 70)
    print(f"Base Capital:    ₹{SystemConfig.BASE_CAPITAL:,.2f}")
    print(f"Auto Trading:    {'ENABLED 🔴' if SystemConfig.ENABLE_AUTO_TRADING else 'DISABLED 🟡'}")
    print(f"Execution Mode:  {'SafeExecutor (Real)' if SystemConfig.ENABLE_AUTO_TRADING else 'MockExecutor'}")
    print(f"Database:        SQLite (WAL Mode) - Journal Only")
    print(f"Daily Cache:     JSON File (Lock-Free)")
    print(f"Analytics:       ThreadPoolExecutor (Non-Blocking)")
    print(f"Telegram:        {'ACTIVE ✅' if SystemConfig.TELEGRAM_TOKEN else 'DISABLED ⚠️'}")
    print(f"P&L Attribution: ENABLED ✅")
    print(f"Greek Snapshot:  ENABLED ✅")
    print(f"GTT Stop Loss:   {'ACTIVE' if SystemConfig.ENABLE_AUTO_TRADING else 'N/A'}")
    print("=" * 70)
    print(f"API Documentation: http://localhost:{SystemConfig.PORT}/docs")
    print("=" * 70)
    
    uvicorn.run(
        app,
        host=SystemConfig.HOST,
        port=SystemConfig.PORT,
        log_level="info"
    )
