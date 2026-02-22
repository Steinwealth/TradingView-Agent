"""
TradingView Agent - Unified Broker Interface (V3.6)

Abstract base class for all broker implementations
Ensures consistent interface across Coinbase, Binance, Kraken, etc.

Key Feature: Unified balance/collateral handling
- Coinbase: USD, USDC
- Binance: USDT, BUSD, BNB
- Kraken: USD, EUR

All brokers return balance in USD equivalent for consistency
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
from decimal import Decimal


class BrokerInterface(ABC):
    """
    Abstract base class for all broker implementations
    
    All brokers must implement these methods with consistent return types
    """
    
    def __init__(self, api_key: str, api_secret: str, **kwargs):
        self.api_key = api_key
        self.api_secret = api_secret
        self.broker_name = self.__class__.__name__
    
    # ============================================
    # ACCOUNT BALANCE (UNIFIED)
    # ============================================
    
    @abstractmethod
    async def get_account_balance_usd(self) -> float:
        """
        Get total account balance in USD equivalent
        
        CRITICAL: This is the UNIFIED interface
        
        Coinbase: Returns USD or USDC balance
        Binance: Returns USDT balance (1:1 with USD)
        Kraken: Returns USD balance or EUR converted
        
        Returns:
            float: Total account balance in USD equivalent
            
        Example:
            Coinbase: $1,000.00 USD
            Binance: 1,000.00 USDT → $1,000.00 USD
            Kraken: $1,000.00 USD
        """
        pass
    
    @abstractmethod
    async def get_available_balance_usd(self) -> float:
        """
        Get available balance for trading in USD equivalent
        
        CRITICAL: This is what partitions use for sizing
        
        Returns available balance (not locked in positions)
        Accounts for broker-specific margin requirements
        
        Coinbase: Available margin in USD/USDC
        Binance: Available margin in USDT
        Kraken: Available margin in USD
        
        Returns:
            float: Available balance for new positions in USD
        """
        pass
    
    @abstractmethod
    async def get_balance_details(self) -> Dict:
        """
        Get detailed balance breakdown (for debugging)
        
        Returns:
            Dict containing:
            - total_balance_usd: Total account value
            - available_balance_usd: Available for trading
            - margin_balance_usd: Margin/collateral
            - locked_balance_usd: Locked in positions
            - currency: Primary currency (USD, USDT, etc.)
            - other_currencies: Dict of other currency balances
        """
        pass
    
    # ============================================
    # POSITION QUERIES (UNIFIED)
    # ============================================
    
    @abstractmethod
    async def get_all_positions(self) -> List[Dict]:
        """
        Get all open positions (UNIFIED format)
        
        Returns:
            List of position dicts with STANDARD fields:
            
            {
                'symbol': str,              # BTC-PERP, BTCUSDT, etc.
                'position_amt': float,      # Signed (+ for LONG, - for SHORT)
                'entry_price': float,       # Average entry price
                'mark_price': float,        # Current mark price
                'unrealized_pnl': float,    # Unrealized P&L in USD
                'leverage': int,            # Leverage used
                'margin_usd': float,        # Margin/collateral in USD
                'notional_usd': float,      # Position size in USD
                'liquidation_price': float, # Liquidation price
                'side': str,                # 'LONG' or 'SHORT'
                'client_order_id': str      # Order ID (for partition matching)
            }
        """
        pass
    
    @abstractmethod
    async def get_position(self, symbol: str) -> Optional[Dict]:
        """
        Get specific position (UNIFIED format)
        
        Returns None if no position exists
        Returns same format as get_all_positions()
        """
        pass
    
    # ============================================
    # LEVERAGE MANAGEMENT (UNIFIED)
    # ============================================
    
    @abstractmethod
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """
        Set leverage for a symbol
        
        Args:
            symbol: Trading symbol
            leverage: Desired leverage (1-125)
            
        Returns:
            bool: True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def get_current_leverage(self, symbol: str) -> Optional[int]:
        """
        Get current leverage setting for symbol
        
        Returns:
            int: Current leverage or None if not set
        """
        pass
    
    # ============================================
    # TRADING (UNIFIED)
    # ============================================
    
    @abstractmethod
    async def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        client_order_id: str
    ) -> Dict:
        """
        Place market order (UNIFIED)
        
        Args:
            symbol: Trading symbol
            side: 'BUY' or 'SELL'
            quantity: Order quantity in base currency
            client_order_id: Unique order ID (for partition tracking)
            
        Returns:
            Dict with STANDARD fields:
            {
                'order_id': str,
                'client_order_id': str,
                'symbol': str,
                'side': str,
                'quantity': float,
                'executed_qty': float,
                'avg_price': float,
                'status': str,
                'commission': float,  # In USD
                'commission_asset': str
            }
        """
        pass
    
    @abstractmethod
    async def close_position_market(self, symbol: str) -> Dict:
        """
        Close position at market (UNIFIED)
        
        Returns same format as place_market_order()
        """
        pass
    
    # ============================================
    # HELPER METHODS
    # ============================================
    
    @abstractmethod
    async def get_current_price(self, symbol: str) -> float:
        """Get current market price"""
        pass
    
    @abstractmethod
    def get_broker_name(self) -> str:
        """Get broker name for logging"""
        return self.broker_name
    
    # ============================================
    # CURRENCY CONVERSION (For multi-currency brokers)
    # ============================================
    
    async def convert_to_usd(self, amount: float, currency: str) -> float:
        """
        Convert any currency to USD equivalent
        
        Args:
            amount: Amount in source currency
            currency: Currency code (USDT, BNB, EUR, etc.)
            
        Returns:
            float: USD equivalent
            
        Default implementation (can be overridden):
        - USDT, USDC, BUSD → 1:1 with USD
        - BNB, ETH → Query conversion rate
        - EUR → Query conversion rate
        """
        # Stablecoins (1:1 with USD)
        if currency in ['USD', 'USDT', 'USDC', 'BUSD', 'DAI', 'TUSD']:
            return amount
        
        # For other currencies, must be implemented by broker
        raise NotImplementedError(
            f"Currency conversion for {currency} not implemented. "
            f"Override convert_to_usd() in {self.broker_name}"
        )

