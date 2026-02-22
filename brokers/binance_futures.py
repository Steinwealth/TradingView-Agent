"""
Binance Futures Broker - USDT-M Perpetuals Integration
Executes trades on Binance Futures with 1x-125x leverage
Supports DEMO (paper trading) and LIVE modes
"""

from binance.client import Client
from binance.enums import *
from binance.exceptions import BinanceAPIException, BinanceOrderException
from decimal import Decimal, ROUND_DOWN
import logging
import time
from typing import Dict, Optional, Tuple, List, Any
import asyncio
from datetime import datetime

logger = logging.getLogger(__name__)


class BinanceFuturesBroker:
    """
    Binance USDT-M Futures broker for leveraged BTC trading
    
    Provides:
    - 0.04% maker/taker fees (lower than spot 0.075%)
    - 1x to 125x leverage (3x recommended)
    - Long and short positions
    - Demo mode for paper trading
    """
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, demo_mode: bool = False):
        """
        Initialize Binance Futures connection
        
        Args:
            api_key: Binance API key
            api_secret: Binance API secret
            testnet: Use testnet (default: False)
            demo_mode: Paper trading mode (no real orders, default: False)
        """
        self.demo_mode = demo_mode
        self.testnet = testnet
        
        # Initialize Binance client
        if testnet:
            self.client = Client(
                api_key,
                api_secret,
                testnet=True,
                tld='com'
            )
            logger.info("Binance Futures initialized - TESTNET MODE")
        else:
            self.client = Client(api_key, api_secret)
            if demo_mode:
                logger.warning("Binance Futures initialized - DEMO MODE (Paper Trading)")
            else:
                logger.info("Binance Futures initialized - LIVE MODE")
        
        # Set to futures mode
        self.client.API_URL = 'https://fapi.binance.com'
        
        # Paper trading state (for demo mode)
        self.demo_position = {
            'symbol': None,
            'side': 'NONE',
            'quantity': 0.0,
            'entry_price': 0.0,
            'leverage': 1,
            'entry_time': None,
            'unrealized_pnl': 0.0,
            'tp_price': None,
            'sl_price': None
        }
        
        # Liquidation calculation constants
        self.MAINTENANCE_MARGIN_RATE = 0.004  # 0.4% for BTC (up to 50x)
        
        logger.info("BinanceFuturesBroker initialized")
    
    # ============================================
    # UNIFIED INTERFACE METHODS (V3.6 - For Partitions)
    # ============================================
    
    async def get_account_balance_usd(self) -> float:
        """
        Get total account balance in USD equivalent (unified interface for partitions)
        
        Binance returns balance in USDT (1:1 with USD)
        May also have BNB or other assets - converts to USD
        
        Returns:
            float: Total balance in USD equivalent
        """
        balance_dict = self.get_account_balance()
        total = balance_dict.get('total_wallet_balance', 0.0)
        return float(total)  # USDT = USD 1:1
    
    async def get_available_balance_usd(self) -> float:
        """
        Get available balance for trading in USD equivalent (unified interface)
        
        Returns:
            float: Available balance in USD equivalent
        """
        balance_dict = self.get_account_balance()
        available = balance_dict.get('available_balance', 0.0)
        return float(available)  # USDT = USD 1:1
    
    async def get_all_positions(self) -> List[Dict]:
        """
        Get all open positions in UNIFIED format (for partition sync)
        
        Returns positions with standardized fields including margin_usd
        Works with partition system's full reconciliation
        
        Returns:
            List of positions in unified format
        """
        try:
            if self.demo_mode:
                # Demo mode - return demo position if exists
                if self.demo_position['side'] != 'NONE':
                    position_amt = self.demo_position['quantity']
                    if self.demo_position['side'] == 'SHORT':
                        position_amt = -position_amt
                    
                    return [{
                        'symbol': self.demo_position['symbol'],
                        'position_amt': position_amt,
                        'entry_price': self.demo_position['entry_price'],
                        'mark_price': self.demo_position['entry_price'],
                        'unrealized_pnl': self.demo_position['unrealized_pnl'],
                        'leverage': self.demo_position['leverage'],
                        'margin_usd': abs(position_amt) * self.demo_position['entry_price'],
                        'notional_usd': abs(position_amt) * self.demo_position['entry_price'] * self.demo_position['leverage'],
                        'liquidation_price': 0.0,
                        'side': self.demo_position['side'],
                        'client_order_id': ''
                    }]
                return []
            
            # Get account info which includes all positions
            account = self.client.futures_account()
            
            unified_positions = []
            
            for pos in account.get('positions', []):
                position_amt = float(pos.get('positionAmt', 0))
                
                if abs(position_amt) < 0.000001:  # No position
                    continue
                
                entry_price = float(pos.get('entryPrice', 0))
                mark_price = float(pos.get('markPrice', entry_price))
                leverage = int(pos.get('leverage', 1))
                
                # Convert to unified format (USDT = USD 1:1)
                unified_pos = {
                    'symbol': pos['symbol'],
                    'position_amt': position_amt,
                    'entry_price': entry_price,
                    'mark_price': mark_price,
                    'unrealized_pnl': float(pos.get('unRealizedProfit', 0)),
                    'leverage': leverage,
                    'margin_usd': abs(position_amt) * entry_price,  # USDT = USD
                    'notional_usd': abs(position_amt) * mark_price * leverage,
                    'liquidation_price': float(pos.get('liquidationPrice', 0)),
                    'side': 'LONG' if position_amt > 0 else 'SHORT',
                    'client_order_id': ''
                }
                
                unified_positions.append(unified_pos)
            
            return unified_positions
            
        except Exception as e:
            logger.error(f"❌ Failed to get Binance positions: {str(e)}")
            return []
    
    def get_broker_name(self) -> str:
        """Get broker name (for unified interface)"""
        return "Binance Futures"
    
    # ============================================
    # EXISTING METHODS (Keep all functionality)
    # ============================================
    
    def get_account_balance(self) -> Dict:
        """
        Get futures account balance
        
        Returns:
            {
                'total_wallet_balance': 1000.0,
                'available_balance': 950.0,
                'total_unrealized_pnl': 50.0,
                'total_margin_balance': 1050.0
            }
        """
        if self.demo_mode:
            # Return demo balance
            return {
                'total_wallet_balance': 1000.0,
                'available_balance': 950.0 - abs(self.demo_position['unrealized_pnl']),
                'total_unrealized_pnl': self.demo_position['unrealized_pnl'],
                'total_margin_balance': 1000.0 + self.demo_position['unrealized_pnl'],
                'mode': 'DEMO'
            }
        
        try:
            # Get futures account info
            account = self.client.futures_account()
            
            return {
                'total_wallet_balance': float(account['totalWalletBalance']),
                'available_balance': float(account['availableBalance']),
                'total_unrealized_pnl': float(account['totalUnrealizedProfit']),
                'total_margin_balance': float(account['totalMarginBalance']),
                'mode': 'LIVE'
            }
        except Exception as e:
            logger.error(f"Error getting account balance: {str(e)}")
            raise
    
    def get_current_price(self, symbol: str) -> float:
        """
        Get current mark price for symbol
        
        Args:
            symbol: Symbol (e.g., 'BTCUSDT')
        
        Returns:
            Current mark price
        """
        try:
            ticker = self.client.futures_mark_price(symbol=symbol)
            return float(ticker['markPrice'])
        except Exception as e:
            logger.error(f"Error getting price for {symbol}: {str(e)}")
            raise
    
    def get_position(self, symbol: str = 'BTCUSDT') -> Dict:
        """
        Get current position for symbol
        
        Args:
            symbol: Symbol to check (default: BTCUSDT)
        
        Returns:
            {
                'symbol': 'BTCUSDT',
                'side': 'LONG' | 'SHORT' | 'NONE',
                'quantity': 0.5,
                'entry_price': 100000.0,
                'current_price': 101000.0,
                'unrealized_pnl': 500.0,
                'unrealized_pnl_pct': 1.0,
                'leverage': 3,
                'liquidation_price': 75000.0,
                'tp_price': None,
                'sl_price': None
            }
        """
        if self.demo_mode:
            # Return demo position
            current_price = self.get_current_price(symbol)
            
            if self.demo_position['side'] != 'NONE' and self.demo_position['symbol'] == symbol:
                # Calculate unrealized PnL
                qty = self.demo_position['quantity']
                entry = self.demo_position['entry_price']
                
                if self.demo_position['side'] == 'LONG':
                    pnl_usd = (current_price - entry) * qty
                else:  # SHORT
                    pnl_usd = (entry - current_price) * qty
                
                self.demo_position['unrealized_pnl'] = pnl_usd
                pnl_pct = (pnl_usd / (entry * qty)) * 100
                
                return {
                    'symbol': symbol,
                    'side': self.demo_position['side'],
                    'quantity': qty,
                    'entry_price': entry,
                    'current_price': current_price,
                    'unrealized_pnl': pnl_usd,
                    'unrealized_pnl_pct': pnl_pct,
                    'leverage': self.demo_position['leverage'],
                    'liquidation_price': self._calculate_liquidation_price(
                        entry, qty, self.demo_position['leverage'], self.demo_position['side']
                    ),
                    'tp_price': self.demo_position.get('tp_price'),
                    'sl_price': self.demo_position.get('sl_price'),
                    'mode': 'DEMO'
                }
            else:
                # No demo position
                return {
                    'symbol': symbol,
                    'side': 'NONE',
                    'quantity': 0.0,
                    'entry_price': 0.0,
                    'current_price': current_price,
                    'unrealized_pnl': 0.0,
                    'unrealized_pnl_pct': 0.0,
                    'leverage': 1,
                    'mode': 'DEMO'
                }
        
        try:
            # Get all positions
            positions = self.client.futures_position_information(symbol=symbol)
            
            for pos in positions:
                if pos['symbol'] == symbol:
                    qty = float(pos['positionAmt'])
                    
                    if abs(qty) > 0:
                        side = 'LONG' if qty > 0 else 'SHORT'
                        entry_price = float(pos['entryPrice'])
                        current_price = self.get_current_price(symbol)
                        pnl = float(pos['unRealizedProfit'])
                        pnl_pct = (pnl / (entry_price * abs(qty))) * 100 if abs(qty) > 0 else 0
                        leverage = int(pos['leverage'])
                        
                        return {
                            'symbol': symbol,
                            'side': side,
                            'quantity': abs(qty),
                            'entry_price': entry_price,
                            'current_price': current_price,
                            'unrealized_pnl': pnl,
                            'unrealized_pnl_pct': pnl_pct,
                            'leverage': leverage,
                            'liquidation_price': float(pos['liquidationPrice']) if pos['liquidationPrice'] else None,
                            'mode': 'LIVE'
                        }
                    else:
                        # No position
                        return {
                            'symbol': symbol,
                            'side': 'NONE',
                            'quantity': 0.0,
                            'entry_price': 0.0,
                            'current_price': self.get_current_price(symbol),
                            'unrealized_pnl': 0.0,
                            'unrealized_pnl_pct': 0.0,
                            'leverage': 1,
                            'mode': 'LIVE'
                        }
            
            # Symbol not found in positions
            return {
                'symbol': symbol,
                'side': 'NONE',
                'quantity': 0.0,
                'mode': 'LIVE'
            }
            
        except Exception as e:
            logger.error(f"Error getting position: {str(e)}")
            raise
    
    def set_leverage(self, symbol: str, leverage: int):
        """
        Set leverage for symbol
        
        Args:
            symbol: Symbol (e.g., 'BTCUSDT')
            leverage: Leverage (1-125)
        """
        if self.demo_mode:
            logger.info(f"[DEMO] Set leverage {symbol}: {leverage}x")
            return
        
        try:
            result = self.client.futures_change_leverage(
                symbol=symbol,
                leverage=leverage
            )
            logger.info(f"Leverage set for {symbol}: {leverage}x")
            return result
        except BinanceAPIException as e:
            logger.error(f"Error setting leverage: {str(e)}")
            raise
    
    def set_margin_type(self, symbol: str, margin_type: str = 'CROSSED'):
        """
        Set margin type (CROSSED or ISOLATED)
        
        Args:
            symbol: Symbol
            margin_type: 'CROSSED' or 'ISOLATED' (CROSSED recommended for safety)
        """
        if self.demo_mode:
            logger.info(f"[DEMO] Set margin type {symbol}: {margin_type}")
            return
        
        try:
            result = self.client.futures_change_margin_type(
                symbol=symbol,
                marginType=margin_type
            )
            logger.info(f"Margin type set for {symbol}: {margin_type}")
            return result
        except BinanceAPIException as e:
            # Ignore if already set to this margin type
            if 'No need to change margin type' in str(e):
                logger.info(f"Margin type already {margin_type}")
                return
            logger.error(f"Error setting margin type: {str(e)}")
            raise
    
    def _calculate_quantity(self, symbol: str, usdt_amount: float, price: float, leverage: int = 1) -> float:
        """
        Calculate quantity in BTC from USDT amount
        
        Args:
            symbol: Symbol
            usdt_amount: USDT to use
            price: Current BTC price
            leverage: Leverage multiplier
        
        Returns:
            Quantity in BTC (3 decimal precision for BTCUSDT)
        """
        # With leverage, you can control more BTC with same USDT
        notional_value = usdt_amount * leverage
        quantity = notional_value / price
        
        # Round to 3 decimals for BTCUSDT
        quantity = float(Decimal(str(quantity)).quantize(Decimal('0.001'), rounding=ROUND_DOWN))
        
        logger.info(f"Calculated quantity: ${usdt_amount} @ {price} with {leverage}x = {quantity} BTC")
        
        return quantity
    
    def _calculate_liquidation_price(self, entry_price: float, quantity: float, leverage: int, side: str) -> float:
        """
        Calculate estimated liquidation price
        
        Formula (simplified):
        LONG: Liq Price = Entry * (1 - 1/leverage + MMR)
        SHORT: Liq Price = Entry * (1 + 1/leverage - MMR)
        
        Args:
            entry_price: Entry price
            quantity: Position size
            leverage: Leverage used
            side: 'LONG' or 'SHORT'
        
        Returns:
            Liquidation price
        """
        mmr = self.MAINTENANCE_MARGIN_RATE
        
        if side == 'LONG':
            liq_price = entry_price * (1 - (1/leverage) + mmr)
        else:  # SHORT
            liq_price = entry_price * (1 + (1/leverage) - mmr)
        
        return round(liq_price, 2)
    
    def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        leverage: int = 1,
        reduce_only: bool = False
    ) -> Dict:
        """
        Place market order on Binance Futures
        
        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            side: 'BUY' or 'SELL'
            quantity: Quantity in BTC
            leverage: Leverage (1-125)
            reduce_only: Only reduce position (default: False)
        
        Returns:
            Order result dict
        """
        if self.demo_mode:
            # DEMO MODE: Simulate order
            current_price = self.get_current_price(symbol)
            
            logger.warning(f"[DEMO] Market order: {side} {quantity} {symbol} @ {current_price} (leverage: {leverage}x)")
            
            # Update demo position
            if side == 'BUY':
                if self.demo_position['side'] == 'SHORT':
                    # Closing short
                    self.demo_position = {
                        'symbol': None,
                        'side': 'NONE',
                        'quantity': 0.0,
                        'entry_price': 0.0,
                        'leverage': 1,
                        'entry_time': None,
                        'unrealized_pnl': 0.0
                    }
                else:
                    # Opening long
                    self.demo_position = {
                        'symbol': symbol,
                        'side': 'LONG',
                        'quantity': quantity,
                        'entry_price': current_price,
                        'leverage': leverage,
                        'entry_time': datetime.utcnow(),
                        'unrealized_pnl': 0.0,
                        'tp_price': None,
                        'sl_price': None
                    }
            elif side == 'SELL':
                if self.demo_position['side'] == 'LONG':
                    # Closing long
                    self.demo_position = {
                        'symbol': None,
                        'side': 'NONE',
                        'quantity': 0.0,
                        'entry_price': 0.0,
                        'leverage': 1,
                        'entry_time': None,
                        'unrealized_pnl': 0.0
                    }
                else:
                    # Opening short
                    self.demo_position = {
                        'symbol': symbol,
                        'side': 'SHORT',
                        'quantity': quantity,
                        'entry_price': current_price,
                        'leverage': leverage,
                        'entry_time': datetime.utcnow(),
                        'unrealized_pnl': 0.0,
                        'tp_price': None,
                        'sl_price': None
                    }
            
            return {
                'orderId': f"DEMO-{int(time.time())}",
                'symbol': symbol,
                'status': 'FILLED',
                'side': side,
                'type': 'MARKET',
                'quantity': quantity,
                'price': current_price,
                'avgPrice': current_price,
                'executedQty': quantity,
                'commission': quantity * current_price * 0.0004,  # 0.04% taker fee
                'mode': 'DEMO'
            }
        
        try:
            # Set leverage first
            self.set_leverage(symbol, leverage)
            
            # Place market order
            order = self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type=ORDER_TYPE_MARKET,
                quantity=quantity,
                reduceOnly=reduce_only
            )
            
            logger.info(f"[LIVE] Market order placed: {side} {quantity} {symbol}")
            
            return order
            
        except BinanceAPIException as e:
            logger.error(f"Binance API error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error placing order: {str(e)}")
            raise
    
    def close_position(self, symbol: str = 'BTCUSDT') -> Dict:
        """
        Close entire position for symbol
        
        Args:
            symbol: Symbol to close
        
        Returns:
            Order result
        """
        pos = self.get_position(symbol)
        
        if pos['side'] == 'NONE':
            logger.info(f"No position to close for {symbol}")
            return {'status': 'no_position'}
        
        # Determine side to close position
        close_side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
        quantity = pos['quantity']
        
        logger.info(f"Closing {pos['side']} position: {close_side} {quantity} {symbol}")
        
        return self.place_market_order(
            symbol=symbol,
            side=close_side,
            quantity=quantity,
            leverage=pos['leverage'],
            reduce_only=True
        )
    
    def set_stop_loss_take_profit(
        self,
        symbol: str,
        sl_price: Optional[float] = None,
        tp_price: Optional[float] = None
    ) -> Dict:
        """
        Set stop loss and take profit orders
        
        Args:
            symbol: Symbol
            sl_price: Stop loss price (optional)
            tp_price: Take profit price (optional)
        
        Returns:
            Order results
        """
        if self.demo_mode:
            logger.info(f"[DEMO] Set SL/TP for {symbol}: SL={sl_price}, TP={tp_price}")
            self.demo_position['sl_price'] = sl_price
            self.demo_position['tp_price'] = tp_price
            return {'status': 'demo', 'sl_price': sl_price, 'tp_price': tp_price}
        
        try:
            pos = self.get_position(symbol)
            
            if pos['side'] == 'NONE':
                logger.warning("No position to set SL/TP for")
                return {'status': 'no_position'}
            
            # Cancel existing TP/SL orders first
            self.cancel_all_orders(symbol)
            
            results = []
            
            # Determine side for closing orders
            close_side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
            
            # Set stop loss (STOP_MARKET)
            if sl_price:
                sl_order = self.client.futures_create_order(
                    symbol=symbol,
                    side=close_side,
                    type=FUTURE_ORDER_TYPE_STOP_MARKET,
                    stopPrice=sl_price,
                    closePosition=True
                )
                results.append({'type': 'stop_loss', 'order': sl_order})
                logger.info(f"Stop loss set at {sl_price}")
            
            # Set take profit (TAKE_PROFIT_MARKET)
            if tp_price:
                tp_order = self.client.futures_create_order(
                    symbol=symbol,
                    side=close_side,
                    type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
                    stopPrice=tp_price,
                    closePosition=True
                )
                results.append({'type': 'take_profit', 'order': tp_order})
                logger.info(f"Take profit set at {tp_price}")
            
            return {'status': 'success', 'orders': results}
            
        except BinanceAPIException as e:
            logger.error(f"Error setting SL/TP: {str(e)}")
            raise
    
    def cancel_all_orders(self, symbol: str):
        """Cancel all open orders for symbol"""
        if self.demo_mode:
            logger.info(f"[DEMO] Cancel all orders for {symbol}")
            return
        
        try:
            result = self.client.futures_cancel_all_open_orders(symbol=symbol)
            logger.info(f"Cancelled all orders for {symbol}")
            return result
        except Exception as e:
            # Ignore if no orders to cancel
            if 'Unknown order sent' in str(e):
                return
            logger.error(f"Error cancelling orders: {str(e)}")
            raise
    
    def get_open_orders(self, symbol: str) -> list:
        """
        Get all open orders for symbol.
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
        
        Returns:
            List of open orders
        """
        if self.demo_mode:
            logger.info(f"[DEMO] Get open orders for {symbol}")
            return []
        
        try:
            orders = self.client.futures_get_open_orders(symbol=symbol)
            return orders
        except Exception as e:
            logger.error(f"Error getting open orders: {str(e)}")
            raise
    
    def verify_sl_tp_exist(
        self,
        symbol: str,
        expected_sl: Optional[float] = None,
        expected_tp: Optional[float] = None
    ) -> Dict[str, bool]:
        """
        Verify that SL/TP orders exist on Binance.
        
        Args:
            symbol: Trading symbol
            expected_sl: Expected stop loss price (optional, for validation)
            expected_tp: Expected take profit price (optional, for validation)
        
        Returns:
            {
                'sl_exists': bool,
                'tp_exists': bool,
                'sl_order': dict or None,
                'tp_order': dict or None
            }
        """
        if self.demo_mode:
            logger.info(f"[DEMO] Verify SL/TP for {symbol}")
            return {
                'sl_exists': expected_sl is not None,
                'tp_exists': expected_tp is not None,
                'sl_order': None,
                'tp_order': None
            }
        
        try:
            orders = self.get_open_orders(symbol)
            
            sl_order = None
            tp_order = None
            
            for order in orders:
                order_type = order.get('type')
                stop_price = float(order.get('stopPrice', 0))
                
                # Check for stop loss order
                if order_type == FUTURE_ORDER_TYPE_STOP_MARKET:
                    sl_order = order
                    if expected_sl and abs(stop_price - expected_sl) / expected_sl > 0.01:
                        logger.warning(
                            f"SL price mismatch: expected ${expected_sl:.2f}, "
                            f"found ${stop_price:.2f}"
                        )
                
                # Check for take profit order
                elif order_type == FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET:
                    tp_order = order
                    if expected_tp and abs(stop_price - expected_tp) / expected_tp > 0.01:
                        logger.warning(
                            f"TP price mismatch: expected ${expected_tp:.2f}, "
                            f"found ${stop_price:.2f}"
                        )
            
            result = {
                'sl_exists': sl_order is not None,
                'tp_exists': tp_order is not None,
                'sl_order': sl_order,
                'tp_order': tp_order
            }
            
            logger.info(
                f"SL/TP verification for {symbol}: "
                f"SL={'✅' if result['sl_exists'] else '❌'}, "
                f"TP={'✅' if result['tp_exists'] else '❌'}"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error verifying SL/TP: {str(e)}")
            raise
    
    def verify_position(
        self,
        symbol: str,
        expected_side: str,
        expected_qty: float,
        tolerance: float = 0.001
    ) -> Dict[str, Any]:
        """
        Verify position matches expected state.
        
        Args:
            symbol: Trading symbol
            expected_side: Expected position side ('LONG', 'SHORT', or 'NONE')
            expected_qty: Expected quantity
            tolerance: Tolerance for quantity mismatch (default: 0.001 BTC)
        
        Returns:
            {
                'matches': bool,
                'actual_side': str,
                'actual_qty': float,
                'actual_position': dict,
                'mismatch_details': str or None
            }
        """
        try:
            actual_pos = self.get_position(symbol)
            actual_side = actual_pos['side']
            actual_qty = actual_pos['quantity']
            
            # Check side match
            side_matches = (actual_side == expected_side)
            
            # Check quantity match (with tolerance)
            qty_diff = abs(actual_qty - expected_qty)
            qty_matches = qty_diff <= tolerance
            
            matches = side_matches and qty_matches
            
            mismatch_details = None
            if not matches:
                details = []
                if not side_matches:
                    details.append(f"Side: expected {expected_side}, got {actual_side}")
                if not qty_matches:
                    details.append(
                        f"Qty: expected {expected_qty:.4f}, got {actual_qty:.4f} "
                        f"(diff: {qty_diff:.4f})"
                    )
                mismatch_details = "; ".join(details)
                logger.warning(f"Position mismatch for {symbol}: {mismatch_details}")
            
            return {
                'matches': matches,
                'actual_side': actual_side,
                'actual_qty': actual_qty,
                'actual_position': actual_pos,
                'mismatch_details': mismatch_details
            }
            
        except Exception as e:
            logger.error(f"Error verifying position: {str(e)}")
            raise
    
    def get_exchange_info(self, symbol: str = 'BTCUSDT') -> Dict:
        """
        Get trading rules for symbol (min qty, price precision, etc.)
        
        Returns:
            {
                'min_qty': 0.001,
                'step_size': 0.001,
                'price_precision': 2,
                'qty_precision': 3
            }
        """
        try:
            info = self.client.futures_exchange_info()
            
            for s in info['symbols']:
                if s['symbol'] == symbol:
                    # Parse filters
                    min_qty = 0.001
                    step_size = 0.001
                    
                    for f in s['filters']:
                        if f['filterType'] == 'LOT_SIZE':
                            min_qty = float(f['minQty'])
                            step_size = float(f['stepSize'])
                    
                    return {
                        'min_qty': min_qty,
                        'step_size': step_size,
                        'price_precision': s['pricePrecision'],
                        'qty_precision': s['quantityPrecision']
                    }
            
            raise Exception(f"Symbol {symbol} not found in exchange info")
            
        except Exception as e:
            logger.error(f"Error getting exchange info: {str(e)}")
            raise


class BinanceFuturesTrader:
    """
    High-level trader interface for Binance Futures
    Handles signals from TradingView Agent
    """
    
    def __init__(self, broker: BinanceFuturesBroker, default_leverage: int = 1):
        """
        Initialize trader
        
        Args:
            broker: BinanceFuturesBroker instance
            default_leverage: Default leverage (1 or 3)
        """
        self.broker = broker
        self.default_leverage = default_leverage
        logger.info(f"BinanceFuturesTrader initialized (default leverage: {default_leverage}x)")
    
    async def execute_signal(self, signal: Dict) -> Dict:
        """
        Execute trading signal from TradingView
        
        Args:
            signal: {
                'action': 'buy' | 'sell' | 'close',
                'symbol': 'BTCUSDT',
                'qty_usdt': 900,  # USDT to use
                'leverage': 3,  # Optional, defaults to default_leverage
                'tp_pct': 5.0,  # Optional take profit %
                'sl_pct': 1.5,  # Optional stop loss %
            }
        
        Returns:
            Execution result dict
        """
        action = signal['action'].lower()
        symbol = signal.get('symbol', 'BTCUSDT')
        leverage = signal.get('leverage', self.default_leverage)
        
        try:
            # 1. Get current state
            current_pos = self.broker.get_position(symbol)
            current_price = self.broker.get_current_price(symbol)
            
            logger.info(f"Current position: {current_pos['side']}, Price: {current_price}")
            
            # 2. CLOSE signal
            if action in ['close', 'exit']:
                if current_pos['side'] == 'NONE':
                    logger.info("No position to close")
                    return {'status': 'no_action', 'reason': 'no_position'}
                
                result = self.broker.close_position(symbol)
                return {
                    'status': 'success',
                    'action': 'close',
                    'side': current_pos['side'],
                    'quantity': current_pos['quantity'],
                    'price': current_price,
                    'order': result
                }
            
            # 3. ENTRY signal (BUY/SELL)
            elif action in ['buy', 'long', 'sell', 'short']:
                # Close opposite position first
                if current_pos['side'] != 'NONE':
                    opposite = (action in ['buy', 'long'] and current_pos['side'] == 'SHORT') or \
                              (action in ['sell', 'short'] and current_pos['side'] == 'LONG')
                    
                    if opposite:
                        logger.info(f"Closing opposite {current_pos['side']} position first")
                        self.broker.close_position(symbol)
                        time.sleep(0.5)  # Brief pause
                    elif current_pos['side'] != 'NONE':
                        logger.warning(f"Already in {current_pos['side']} position, ignoring {action} signal")
                        return {'status': 'no_action', 'reason': 'already_in_position'}
                
                # Calculate quantity
                qty_usdt = signal.get('qty_usdt', 900)  # Default 90% of $1k
                quantity = self.broker._calculate_quantity(symbol, qty_usdt, current_price, leverage)
                
                # Validate minimum
                exchange_info = self.broker.get_exchange_info(symbol)
                if quantity < exchange_info['min_qty']:
                    raise Exception(f"Quantity {quantity} below minimum {exchange_info['min_qty']}")
                
                # Set leverage
                self.broker.set_leverage(symbol, leverage)
                
                # Place entry order
                side_binance = 'BUY' if action in ['buy', 'long'] else 'SELL'
                order = self.broker.place_market_order(
                    symbol=symbol,
                    side=side_binance,
                    quantity=quantity,
                    leverage=leverage
                )
                
                # Calculate TP/SL prices
                tp_price = None
                sl_price = None
                
                if signal.get('tp_pct'):
                    tp_pct = signal['tp_pct'] / 100
                    if side_binance == 'BUY':
                        tp_price = current_price * (1 + tp_pct)
                    else:
                        tp_price = current_price * (1 - tp_pct)
                
                if signal.get('sl_pct'):
                    sl_pct = abs(signal['sl_pct']) / 100
                    if side_binance == 'BUY':
                        sl_price = current_price * (1 - sl_pct)
                    else:
                        sl_price = current_price * (1 + sl_pct)
                
                # Set TP/SL if provided
                if tp_price or sl_price:
                    time.sleep(0.3)  # Brief pause for order to settle
                    tp_sl_result = self.broker.set_stop_loss_take_profit(
                        symbol=symbol,
                        sl_price=sl_price,
                        tp_price=tp_price
                    )
                else:
                    tp_sl_result = None
                
                return {
                    'status': 'success',
                    'action': action,
                    'side': side_binance,
                    'quantity': quantity,
                    'price': current_price,
                    'leverage': leverage,
                    'tp_price': tp_price,
                    'sl_price': sl_price,
                    'order': order,
                    'tp_sl_orders': tp_sl_result
                }
            
            else:
                raise Exception(f"Unknown action: {action}")
                
        except Exception as e:
            logger.error(f"Error executing signal: {str(e)}")
            raise


def create_binance_futures_trader(api_key: str, api_secret: str, leverage: int = 1, demo_mode: bool = False) -> BinanceFuturesTrader:
    """
    Factory function to create Binance Futures trader
    
    Args:
        api_key: Binance API key
        api_secret: Binance API secret  
        leverage: Default leverage (1 or 3)
        demo_mode: Enable paper trading mode
    
    Returns:
        BinanceFuturesTrader instance
    """
    broker = BinanceFuturesBroker(api_key, api_secret, demo_mode=demo_mode)
    trader = BinanceFuturesTrader(broker, default_leverage=leverage)
    return trader

