"""
TradingView Agent - Position Sizer Service
Calculates position sizes based on account equity and leverage
Matches TradingView backtest: 90% of equity per trade
"""

import logging
from typing import Dict, Optional
from decimal import Decimal

logger = logging.getLogger(__name__)


class PositionSizer:
    """
    Calculates optimal position sizes based on:
    - Current account equity
    - Configured equity percentage (90% default)
    - Leverage (1x, 3x, etc.)
    - Minimum position size requirements
    """
    
    def __init__(self, settings, brokers: Dict = None):
        self.settings = settings
        self.brokers = brokers or {}
        
        # Position sizing configuration
        self.use_dynamic_sizing = getattr(settings, 'USE_DYNAMIC_POSITION_SIZING', True)
        self.equity_percentage = getattr(settings, 'EQUITY_PERCENTAGE', 90.0)
        self.min_position_size = getattr(settings, 'MIN_POSITION_SIZE_USD', 10.0)
        
        logger.info(
            f"PositionSizer initialized - "
            f"Dynamic sizing: {self.use_dynamic_sizing}, "
            f"Equity %: {self.equity_percentage}%, "
            f"Min size: ${self.min_position_size}"
        )
    
    async def calculate_position_size(
        self,
        broker_name: str,
        symbol: str,
        leverage: int = 1,
        requested_qty_usd: Optional[float] = None,
        qty_pct: Optional[float] = None
    ) -> Dict:
        """
        Calculate position size based on account equity
        
        Matches TradingView backtest behavior:
        - Uses current account balance
        - Applies equity percentage (90% default)
        - Accounts for leverage
        - Ensures minimum position size
        
        Args:
            broker_name: Broker identifier (e.g., "binance_futures")
            symbol: Trading symbol (e.g., "BTCUSDT")
            leverage: Leverage multiplier (1x, 3x, etc.)
            requested_qty_usd: Optional fixed USD amount (overrides dynamic sizing)
            qty_pct: Optional percentage of equity (overrides global equity_percentage)
        
        Returns:
            {
                'collateral_usd': float,        # Amount of capital to use (for leveraged: margin)
                'notional_usd': float,          # Total position size (collateral * leverage)
                'leverage': int,                # Applied leverage
                'account_balance': float,       # Current account balance
                'equity_pct_used': float,       # Percentage of equity used
                'sizing_method': str,           # How size was calculated
                'can_trade': bool,              # Whether position size is valid
                'reason': str                   # Explanation
            }
        """
        try:
            # If dynamic sizing disabled, use requested amount
            if not self.use_dynamic_sizing:
                if requested_qty_usd:
                    return self._fixed_size_response(requested_qty_usd, leverage)
                else:
                    return self._error_response("Dynamic sizing disabled but no qty_usdt provided")
            
            # Get broker
            if broker_name not in self.brokers:
                logger.warning(f"Broker {broker_name} not available for position sizing")
                # Fallback to requested amount if provided
                if requested_qty_usd:
                    return self._fixed_size_response(requested_qty_usd, leverage)
                else:
                    return self._error_response(f"Broker {broker_name} not initialized")
            
            broker = self.brokers[broker_name]
            
            # Fetch current account balance
            account_balance = await self._get_account_balance(broker_name, broker)
            
            if account_balance is None or account_balance <= 0:
                logger.error(f"Invalid account balance: {account_balance}")
                # Fallback to requested amount if provided
                if requested_qty_usd:
                    return self._fixed_size_response(requested_qty_usd, leverage)
                else:
                    return self._error_response("Could not fetch account balance")
            
            logger.info(f"Account balance for {broker_name}: ${account_balance:,.2f}")
            
            # Calculate position size based on equity percentage
            equity_pct = qty_pct if qty_pct is not None else self.equity_percentage
            
            # Calculate collateral (the amount of capital to use)
            # For 1x leverage: collateral = notional position
            # For 3x leverage: collateral = notional / 3 (margin)
            collateral_usd = account_balance * (equity_pct / 100.0)
            
            # Calculate notional position size (total exposure)
            notional_usd = collateral_usd * leverage
            
            # Check minimum position size
            if collateral_usd < self.min_position_size:
                return {
                    'collateral_usd': 0,
                    'notional_usd': 0,
                    'leverage': leverage,
                    'account_balance': account_balance,
                    'equity_pct_used': equity_pct,
                    'sizing_method': 'dynamic_equity',
                    'can_trade': False,
                    'reason': f'Position too small: ${collateral_usd:.2f} < ${self.min_position_size} minimum'
                }
            
            logger.info(
                f"Position sized: {symbol} @ {leverage}x - "
                f"Account: ${account_balance:,.2f}, "
                f"Using {equity_pct}% = ${collateral_usd:,.2f} collateral, "
                f"Notional: ${notional_usd:,.2f}"
            )
            
            return {
                'collateral_usd': collateral_usd,
                'notional_usd': notional_usd,
                'leverage': leverage,
                'account_balance': account_balance,
                'equity_pct_used': equity_pct,
                'sizing_method': 'dynamic_equity',
                'can_trade': True,
                'reason': f'{equity_pct}% of ${account_balance:,.2f} balance = ${collateral_usd:,.2f} @ {leverage}x leverage'
            }
            
        except Exception as e:
            logger.error(f"Error calculating position size: {str(e)}")
            # Fallback to requested amount on error
            if requested_qty_usd:
                return self._fixed_size_response(requested_qty_usd, leverage)
            else:
                return self._error_response(f"Position sizing error: {str(e)}")
    
    async def _get_account_balance(self, broker_name: str, broker) -> Optional[float]:
        """
        Fetch account balance from broker
        
        Returns available balance for trading
        """
        try:
            # Handle different broker types
            if broker_name == "binance_futures":
                # Binance Futures - get from broker's get_account_balance
                if hasattr(broker, 'broker') and hasattr(broker.broker, 'get_account_balance'):
                    balance_info = broker.broker.get_account_balance()
                    # Use available balance (not total wallet balance)
                    return balance_info.get('available_balance', 0)
                else:
                    logger.warning("Binance Futures broker does not support balance fetching")
                    return None
            
            elif broker_name == "binance_spot":
                # Binance Spot - get USDT balance
                if hasattr(broker, 'broker') and hasattr(broker.broker, 'get_account_balance'):
                    balance_info = broker.broker.get_account_balance()
                    return balance_info.get('USDT', 0)
                else:
                    logger.warning("Binance Spot broker does not support balance fetching")
                    return None
            
            elif broker_name == "coinbase_futures" or broker_name.startswith("coinbase_futures_"):
                # Coinbase Futures - get available balance
                if hasattr(broker, 'broker') and hasattr(broker.broker, 'get_account_balance'):
                    balance_info = broker.broker.get_account_balance()
                    # Use available balance (not total wallet balance)
                    return balance_info.get('available_balance', 0)
                else:
                    logger.warning("Coinbase Futures broker does not support balance fetching")
                    return None
            
            elif broker_name == "base_dex":
                # Base DEX - get USDC balance
                if hasattr(broker, 'broker') and hasattr(broker.broker, 'get_balances'):
                    balances = await broker.broker.get_balances()
                    usdc_balance = float(balances.get('USDC', 0))
                    return usdc_balance
                else:
                    logger.warning("Base DEX broker does not support balance fetching")
                    return None
            
            elif broker_name == "oanda":
                # OANDA - get account balance
                if hasattr(broker, 'broker') and hasattr(broker.broker, 'get_account_info'):
                    account_info = broker.broker.get_account_info()
                    return account_info.get('balance', 0)
                else:
                    logger.warning("OANDA broker does not support balance fetching")
                    return None
            
            else:
                logger.warning(f"Unknown broker type for balance fetch: {broker_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching balance from {broker_name}: {str(e)}")
            return None
    
    def _fixed_size_response(self, qty_usd: float, leverage: int) -> Dict:
        """Return response for fixed position size (not dynamic)"""
        return {
            'collateral_usd': qty_usd,
            'notional_usd': qty_usd * leverage,
            'leverage': leverage,
            'account_balance': None,
            'equity_pct_used': None,
            'sizing_method': 'fixed_amount',
            'can_trade': True,
            'reason': f'Fixed amount: ${qty_usd:,.2f} @ {leverage}x'
        }
    
    def _error_response(self, reason: str) -> Dict:
        """Return error response"""
        return {
            'collateral_usd': 0,
            'notional_usd': 0,
            'leverage': 1,
            'account_balance': None,
            'equity_pct_used': None,
            'sizing_method': 'error',
            'can_trade': False,
            'reason': reason
        }
    
    def get_sizing_stats(self) -> Dict:
        """Get position sizing statistics for monitoring"""
        return {
            'dynamic_sizing_enabled': self.use_dynamic_sizing,
            'equity_percentage': self.equity_percentage,
            'min_position_size_usd': self.min_position_size,
            'brokers_available': list(self.brokers.keys())
        }


def calculate_leverage_metrics(
    collateral_usd: float,
    leverage: int,
    entry_price: float,
    liquidation_buffer: float = 0.004  # 0.4% maintenance margin for BTC
) -> Dict:
    """
    Calculate leverage-related metrics
    
    Args:
        collateral_usd: Amount of capital (margin) used
        leverage: Leverage multiplier
        entry_price: Entry price of asset
        liquidation_buffer: Maintenance margin rate (default 0.4% for BTC)
    
    Returns:
        {
            'notional_value': float,           # Total position size
            'quantity': float,                 # Asset quantity
            'margin_used': float,              # Margin used (= collateral for futures)
            'liquidation_price_long': float,   # Liquidation price for LONG
            'liquidation_price_short': float,  # Liquidation price for SHORT
            'max_loss_pct': float,             # Max loss % before liquidation
            'risk_level': str                  # 'LOW', 'MEDIUM', 'HIGH'
        }
    """
    notional_value = collateral_usd * leverage
    quantity = notional_value / entry_price
    
    # Liquidation calculation for Binance Futures
    # LONG: Liquidation = Entry * (1 - 1/Leverage - Maintenance Margin Rate)
    # SHORT: Liquidation = Entry * (1 + 1/Leverage + Maintenance Margin Rate)
    liq_price_long = entry_price * (1 - (1 / leverage) - liquidation_buffer)
    liq_price_short = entry_price * (1 + (1 / leverage) + liquidation_buffer)
    
    # Max loss % before liquidation (for LONG)
    max_loss_pct = ((entry_price - liq_price_long) / entry_price) * 100
    
    # Risk level assessment
    if leverage == 1:
        risk_level = 'NONE'  # No liquidation risk at 1x
    elif leverage <= 3:
        risk_level = 'LOW'
    elif leverage <= 5:
        risk_level = 'MEDIUM'
    else:
        risk_level = 'HIGH'
    
    return {
        'notional_value': notional_value,
        'quantity': quantity,
        'margin_used': collateral_usd,
        'liquidation_price_long': liq_price_long,
        'liquidation_price_short': liq_price_short,
        'max_loss_pct': max_loss_pct,
        'risk_level': risk_level
    }

