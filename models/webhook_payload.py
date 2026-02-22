"""
TradingView Agent - Webhook Payload Models (V3.5)

Defines the structure for TradingView webhook alerts and order responses.
V3.5 uses strategy_id and side fields for cleaner, more descriptive payloads.
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, Literal, Dict, Any
from datetime import datetime


class WebhookPayload(BaseModel):
    """
    TradingView webhook payload schema (V3.5)
    
    Example from TradingView alert:
    {
      "strategy_id": "ichimoku-coinbase-usa-5m",
      "side": "buy",
      "price": 98500.00,
      "bar_time": "2025-11-06T12:30:00",
      "cid": "123456"
    }
    
    Required Fields:
    - strategy_id: Unique strategy identifier (e.g., "ichimoku-coinbase-usa-5m")
    - side: "buy", "sell", or "exit"
    
    Optional Fields:
    - price: Current price from TradingView (for validation)
    - bar_time: Bar timestamp
    - cid: Client order ID (for idempotency)
    """
    
    # REQUIRED FIELDS
    strategy_id: str = Field(..., description="Strategy ID (e.g., ichimoku-coinbase-usa-5m)")
    side: str = Field(..., description="Side: buy, sell, exit")
    
    # OPTIONAL FIELDS
    price: Optional[float] = Field(None, description="Current price from TradingView")
    bar_time: Optional[str] = Field(None, description="Bar timestamp from TradingView")
    cid: Optional[str] = Field(None, description="Client order ID from TradingView")
    
    # Additional optional fields
    strategy: Optional[str] = Field(None, description="Strategy name (optional, for logging)")
    symbol: Optional[str] = Field(None, description="Symbol provided by alert")
    timeframe: Optional[str] = Field(None, description="Chart timeframe provided by alert")
    leverage: Optional[int] = Field(None, ge=1, le=125, description="Leverage override (optional)")
    qty_pct: Optional[float] = Field(None, ge=1, le=100, description="% of equity override (optional)")
    qty_usdt: Optional[float] = Field(None, ge=5, description="Fixed USDT amount (optional)")
    qty_units: Optional[float] = Field(None, description="Fixed units/contracts (optional)")
    
    # TP/SL (optional - most strategies don't use)
    tp_pct: Optional[float] = Field(None, description="Take profit % (e.g., 5.5)")
    sl_pct: Optional[float] = Field(None, description="Stop loss % (e.g., 1.3)")
    tp_price: Optional[float] = Field(None, description="Take profit price")
    sl_price: Optional[float] = Field(None, description="Stop loss price")
    
    # Metadata
    timestamp: Optional[str] = Field(None, description="TradingView timestamp")
    client_tag: Optional[str] = Field(None, description="Custom client order ID tag")
    historical_enhancer: Optional[Dict[str, Any]] = Field(None, description="Historical Enhancer data payload")
    source: Optional[str] = Field(None, description="Source of the signal: 'Agent Exit' for Agent-level exits, 'TradingView webhook' for TradingView signals")
    exit_reason: Optional[str] = Field(None, description="Exit reason for Agent-level exits (e.g., 'RSI_EXIT_LONG', 'BREAKEVEN_STOP', 'TRAILING_STOP')")
    
    class Config:
        extra = "ignore"
    
    @validator('side')
    def validate_side(cls, v):
        """Normalize side to lowercase"""
        v_lower = v.lower()
        # Normalize common variations
        side_map = {
            'long': 'buy',
            'short': 'sell',
            'close': 'exit',
            'buy': 'buy',
            'sell': 'sell',
            'exit': 'exit'
        }
        
        if v_lower not in side_map:
            raise ValueError(f"Invalid side: {v}. Must be: buy, sell, exit, long, short, or close")
        
        return side_map[v_lower]
    
    def is_entry(self) -> bool:
        """Check if this is an entry signal"""
        return self.side in ['buy', 'sell']
    
    def is_exit(self) -> bool:
        """Check if this is an exit signal"""
        return self.side == 'exit'
    
    def is_heartbeat(self) -> bool:
        """TradingView heartbeat payloads use strategy_id or side to signal keep-alive"""
        strategy = (self.strategy_id or "").strip().lower()
        side = (self.side or "").strip().lower()
        return strategy in {'heartbeat', 'ping', 'keepalive'} or side in {'heartbeat', 'ping', 'keepalive'}
    
    def is_long(self) -> bool:
        """Check if this is a long entry"""
        return self.side == 'buy'
    
    def is_short(self) -> bool:
        """Check if this is a short entry"""
        return self.side == 'sell'
    
    def get_side(self) -> str:
        """Get order side for broker API"""
        if self.side == 'buy':
            return "BUY"
        elif self.side == 'sell':
            return "SELL"
        elif self.side == 'exit':
            return "CLOSE"
        return "UNKNOWN"
    
    def get_client_order_id(self) -> str:
        """Generate idempotent client order ID"""
        # Use cid from payload if provided
        if self.cid:
            return self.cid
        
        if self.client_tag:
            return self.client_tag
        
        # Generate from strategy + timestamp for idempotency
        ts = self.bar_time or self.timestamp or datetime.utcnow().isoformat()
        return f"{self.strategy_id}-{self.side}-{ts}".replace(":", "").replace(".", "").replace(" ", "")[:36]


class OrderResponse(BaseModel):
    """Response from broker after order execution"""
    
    status: Literal["success", "failed", "rejected", "skipped"]
    order_id: Optional[str] = None
    client_order_id: str
    account_id: Optional[str] = None
    symbol: str
    side: str
    quantity: float
    price: Optional[float] = None
    fill_price: Optional[float] = None
    commission: Optional[float] = None
    funding_fee: Optional[float] = None  # Funding fee for futures positions
    timestamp: str
    broker: str
    strategy: str
    leverage: Optional[int] = 1
    error: Optional[str] = None
    capital_deployed: Optional[float] = None  # Capital/margin deployed for the trade
    # P&L data (for exit orders)
    pnl_usd: Optional[float] = None  # Realized P&L in USD (net, after commissions)
    pnl_pct: Optional[float] = None  # Realized P&L as percentage
    entry_price: Optional[float] = None  # Average entry price for closed position
    holding_time_seconds: Optional[float] = None  # Time position was held
    # Historical Enhancer data (for exit orders - includes peak_profit_pct, peak_profit_time, exit_reason)
    historical_enhancer: Optional[Dict[str, Any]] = Field(None, description="Historical Enhancer data including peak profit and exit reason")


class PositionInfo(BaseModel):
    """Current position information"""
    
    symbol: str
    side: Literal["LONG", "SHORT", "NONE"]
    quantity: float
    entry_price: Optional[float] = None
    current_price: Optional[float] = None
    leverage: int = 1
    margin_used: Optional[float] = None
    notional_value: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    unrealized_pnl_pct: Optional[float] = None
    liquidation_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
