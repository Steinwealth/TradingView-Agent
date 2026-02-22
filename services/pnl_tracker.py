"""
TradingView Agent - P&L Tracker Service
Tracks performance, trades, and statistics across all accounts and strategies
"""

import logging
from datetime import datetime, timedelta
import math
from typing import Dict, List, Optional
from collections import defaultdict
from google.cloud import firestore

logger = logging.getLogger(__name__)


class PnLTracker:
    """
    Comprehensive P&L and performance tracking
    
    Features:
    - Real-time P&L (open and closed positions)
    - Best/worst trades tracking
    - Per-account statistics
    - Per-strategy statistics
    - Weekly/monthly aggregations
    - Liquidation risk monitoring
    """
    
    def __init__(self, use_firestore: bool = True):
        """
        Initialize P&L tracker
        
        Args:
            use_firestore: Use Firestore for persistence (default: True)
        """
        self.use_firestore = use_firestore
        
        # In-memory tracking (even if Firestore fails)
        self.open_positions = {}  # Keyed by symbol_strategy_account
        self.closed_trades = []
        self.daily_stats = defaultdict(lambda: {
            'pnl': 0.0,
            'trades': 0,
            'wins': 0,
            'losses': 0
        })
        
        if use_firestore:
            try:
                self.db = firestore.Client()
                self.trades_collection = self.db.collection('trades')
                self.positions_collection = self.db.collection('open_positions')
                self.stats_collection = self.db.collection('daily_stats')
                logger.info("PnLTracker: Firestore initialized")
                
                # Restore open positions so exits can be reconciled after restarts
                restored = list(self.positions_collection.where('status', '==', 'open').stream())
                for doc in restored:
                    data = doc.to_dict()
                    key = f"{data.get('symbol')}_{data.get('strategy')}_{data.get('account','unknown')}"
                    self.open_positions[key] = data
                if restored:
                    logger.info(f"PnLTracker: Restored {len(restored)} open position(s) from persistence")
                
                # CRITICAL: Reconcile with partition state to remove stale positions
                # Positions that don't exist in partition state should be marked as closed
                # Note: This will be called asynchronously after partitions are warmed
                # For now, we'll add a method that can be called after startup
            except Exception as e:
                logger.error(f"Failed to initialize Firestore: {str(e)}")
                self.use_firestore = False
                self.db = None
    
    async def log_trade_open(
        self,
        strategy: str,
        symbol: str,
        side: str,
        quantity: float,
        entry_price: float,
        leverage: int,
        broker: str,
        tp_price: Optional[float] = None,
        sl_price: Optional[float] = None,
        liquidation_price: Optional[float] = None,
        account: Optional[str] = None,
        account_mode: Optional[str] = None,
        deployed_capital: Optional[float] = None
    ) -> str:
        """
        Log position open
        
        Args:
            account_mode: "DEMO" or "LIVE" (optional, defaults to None)
            deployed_capital: Capital deployed (margin) for this position (optional)
        
        Returns:
            trade_id: Unique identifier for this position
        """
        timestamp = datetime.utcnow()
        trade_id = f"{strategy}_{symbol}_{timestamp.timestamp()}"
        
        # Calculate deployed capital if not provided
        # For futures: deployed_capital = notional_value / leverage
        if deployed_capital is None:
            notional_value = quantity * entry_price
            deployed_capital = notional_value / leverage if leverage > 0 else notional_value
        
        position_data = {
            'trade_id': trade_id,
            'strategy': strategy,
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'entry_price': entry_price,
            'current_price': entry_price,
            'leverage': leverage,
            'broker': broker,
            'tp_price': tp_price,
            'sl_price': sl_price,
            'liquidation_price': liquidation_price,
            'entry_time': timestamp,
            'status': 'open',
            'unrealized_pnl': 0.0,
            'unrealized_pnl_pct': 0.0,
            'account': account or "unknown",
            'account_mode': account_mode or "unknown",
            'deployed_capital': deployed_capital
        }
        
        # Store in memory
        account_part = account or "unknown"
        position_key = f"{symbol}_{strategy}_{account_part}"
        self.open_positions[position_key] = position_data
        
        # Store in Firestore
        if self.use_firestore:
            try:
                self.positions_collection.document(trade_id).set(position_data)
                logger.info(f"✅ P&L: Logged position open: {trade_id} ({strategy} - {symbol})")
            except Exception as e:
                logger.error(f"Failed to log to Firestore: {str(e)}")
        
        return trade_id
    
    async def log_trade_close(
        self,
        strategy: str,
        symbol: str,
        exit_price: float,
        exit_time: Optional[datetime] = None,
        account: Optional[str] = None,
        account_mode: Optional[str] = None,
        commission: Optional[float] = None,
        funding_fee: Optional[float] = None
    ) -> Optional[Dict]:
        """
        Log position close and calculate realized P&L
        
        CRITICAL: When an exit signal is received, this closes ALL open positions
        for the symbol, not just the one matching the strategy. This ensures stale
        positions from previous days are also closed.
        
        Returns:
            Trade data with realized P&L, or None if position wasn't tracked
        """
        account_part = account or "unknown"
        position_key = f"{symbol}_{strategy}_{account_part}"
        
        # CRITICAL: Find ALL open positions for this symbol (not just strategy-specific)
        # This ensures stale positions from previous days are closed
        symbol_normalized = symbol.upper().replace('-CBSE', '').replace('-PERP', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
        matching_positions = []
        
        # Find all positions matching this symbol (exact or normalized)
        for key, position in self.open_positions.items():
            pos_symbol = position.get('symbol', '').upper()
            pos_account = position.get('account', 'unknown')
            
            # Match by account and symbol (exact or normalized)
            if pos_account == account_part:
                pos_symbol_norm = pos_symbol.replace('-CBSE', '').replace('-PERP', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
                if pos_symbol == symbol.upper() or pos_symbol_norm == symbol_normalized:
                    matching_positions.append((key, position))
        
        # If no positions found in memory, check Firestore for open positions
        if not matching_positions and self.use_firestore:
            try:
                logger.info(f"🔍 No open positions in memory for {symbol}, checking Firestore...")
                # Query Firestore for open positions matching this symbol and account
                firestore_positions = self.positions_collection.where('status', '==', 'open').where('symbol', '==', symbol.upper()).where('account', '==', account_part).stream()
                
                for doc in firestore_positions:
                    position_data = doc.to_dict()
                    # Restore to in-memory cache
                    position_key = f"{position_data.get('symbol', '')}_{position_data.get('strategy', '')}_{position_data.get('account', 'unknown')}"
                    self.open_positions[position_key] = position_data
                    
                    # Check if this position matches (using normalized symbol matching)
                    pos_symbol = position_data.get('symbol', '').upper()
                    pos_symbol_norm = pos_symbol.replace('-CBSE', '').replace('-PERP', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
                    if pos_symbol == symbol.upper() or pos_symbol_norm == symbol_normalized:
                        matching_positions.append((position_key, position_data))
                        logger.info(f"✅ Found open position in Firestore: {position_key}")
                
                # If still no matches, try strategy-agnostic symbol matching
                if not matching_positions:
                    firestore_positions = self.positions_collection.where('status', '==', 'open').where('account', '==', account_part).stream()
                    for doc in firestore_positions:
                        position_data = doc.to_dict()
                        pos_symbol = position_data.get('symbol', '').upper()
                        pos_symbol_norm = pos_symbol.replace('-CBSE', '').replace('-PERP', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
                        if pos_symbol_norm == symbol_normalized:
                            position_key = f"{pos_symbol}_{position_data.get('strategy', '')}_{account_part}"
                            self.open_positions[position_key] = position_data
                            matching_positions.append((position_key, position_data))
                            logger.info(f"✅ Found open position in Firestore (normalized match): {position_key}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to check Firestore for open positions: {str(e)}")
        
        if not matching_positions:
            logger.warning(f"⚠️ No open positions found for {symbol} (account: {account_part}) in memory or Firestore")
            return None
        
        # If multiple positions found, log and close all of them
        if len(matching_positions) > 1:
            logger.info(f"🔍 Exit signal for {symbol}: Found {len(matching_positions)} open positions - closing all")
        
        # Close all matching positions
        closed_trades = []
        for key, position in matching_positions:
            # Process each position
            exit_time = exit_time or datetime.utcnow()
            
            # Calculate realized P&L for this position
            if position['side'] == 'LONG':
                price_change_pct = ((exit_price - position['entry_price']) / position['entry_price']) * 100
                realized_pnl_usd = (exit_price - position['entry_price']) * position['quantity']
            else:  # SHORT
                price_change_pct = ((position['entry_price'] - exit_price) / position['entry_price']) * 100
                realized_pnl_usd = (position['entry_price'] - exit_price) * position['quantity']
            
            # Get deployed capital (margin) - use stored value or calculate from notional/leverage
            deployed_capital = position.get('deployed_capital')
            if deployed_capital is None or deployed_capital <= 0:
                # Fallback: calculate from notional value and leverage
                notional_value = position['quantity'] * position['entry_price']
                leverage = position.get('leverage', 1)
                deployed_capital = notional_value / leverage if leverage > 0 else notional_value
            
            # Get commission and funding fees (if provided)
            # If not provided, calculate defaults
            if commission is None:
                # Default commission: 0.04% total (0.02% entry + 0.02% exit)
                notional_value = position['quantity'] * position['entry_price']
                commission = notional_value * 0.0004  # 0.04% total
            
            # Calculate trade duration (used for funding fee calculation)
            duration_hours = (exit_time - position['entry_time']).total_seconds() / 3600
            
            if funding_fee is None:
                # Calculate funding fee based on holding time and funding rate
                # Coinbase: ~0.0003%/hr (0.000003), Binance: ~0.00125%/hr (0.0000125)
                # Default to Coinbase rate (0.0003%/hr)
                # TODO: In LIVE mode, fetch actual funding rate from broker API
                funding_rate_per_hour = 0.000003  # 0.0003% = 0.000003 (Coinbase default)
                
                # Funding payment = Funding Rate × Position Size × Hours
                # Position Size = Quantity × Mark Price (average during holding period)
                mark_price = (position['entry_price'] + exit_price) / 2  # Average price during holding
                position_size = position['quantity'] * mark_price
                
                # Calculate base funding payment (positive rate = longs pay, shorts receive)
                base_funding = funding_rate_per_hour * position_size * duration_hours
                
                if position['side'] == 'LONG':
                    # Longs pay when funding rate is positive (negative for us)
                    funding_fee = -base_funding
                else:  # SHORT
                    # Shorts receive when funding rate is positive (positive for us)
                    funding_fee = base_funding
            
            # Calculate NET P&L (gross P&L - commission - funding fees)
            realized_pnl_usd_gross = realized_pnl_usd
            realized_pnl_usd_net = realized_pnl_usd_gross - commission - funding_fee
            
            # Calculate P&L percentages
            # Price change percentage (unlevered)
            realized_pnl_pct = price_change_pct
            
            # Gross leveraged P&L percentage (as % of deployed capital)
            realized_pnl_pct_gross = (realized_pnl_usd_gross / deployed_capital * 100) if deployed_capital > 0 else 0.0
            
            # Net leveraged P&L percentage (as % of deployed capital) - includes commissions and funding
            realized_pnl_pct_levered = (realized_pnl_usd_net / deployed_capital * 100) if deployed_capital > 0 else 0.0
            
            # Build closed trade record
            trade_data = {
                **position,
                'exit_price': exit_price,
                'exit_time': exit_time,
                'realized_pnl_gross': realized_pnl_usd_gross,
                'realized_pnl_net': realized_pnl_usd_net,
                'realized_pnl': realized_pnl_usd_net,  # Keep for backward compatibility
                'realized_pnl_pct': realized_pnl_pct,
                'realized_pnl_pct_gross': realized_pnl_pct_gross,
                'realized_pnl_pct_levered': realized_pnl_pct_levered,
                'commission': commission,
                'funding_fee': funding_fee,
                'duration_hours': duration_hours,
                'status': 'closed',
                'win': realized_pnl_usd_net > 0
            }
            
            # Remove from open positions
            del self.open_positions[key]
            
            # Update daily stats (use NET P&L including commissions and funding)
            date_key = exit_time.date().isoformat()
            if date_key not in self.daily_stats:
                self.daily_stats[date_key] = {'pnl': 0.0, 'trades': 0, 'wins': 0, 'losses': 0}
            self.daily_stats[date_key]['pnl'] += realized_pnl_usd_net
            self.daily_stats[date_key]['trades'] += 1
            if trade_data['win']:
                self.daily_stats[date_key]['wins'] += 1
            else:
                self.daily_stats[date_key]['losses'] += 1
            
            # Persist to Firestore if enabled
            if self.use_firestore:
                try:
                    # Update position document
                    self.positions_collection.document(position['trade_id']).update({
                        'status': 'closed',
                        'exit_price': exit_price,
                        'exit_time': exit_time,
                        'realized_pnl_gross': realized_pnl_usd_gross,
                        'realized_pnl_net': realized_pnl_usd_net,
                        'realized_pnl': realized_pnl_usd_net,  # Keep for backward compatibility
                        'realized_pnl_pct': realized_pnl_pct_levered,
                        'realized_pnl_pct_gross': realized_pnl_pct_gross,
                        'realized_pnl_pct_unlevered': realized_pnl_pct,
                        'commission': commission,
                        'funding_fee': funding_fee,
                        'duration_hours': duration_hours,
                        'win': trade_data['win']
                    })
                    
                    # Also add to trades history
                    self.trades_collection.add(trade_data)
                    
                    logger.info(f"✅ P&L: Logged trade close: {position['trade_id']} - P&L NET: ${realized_pnl_usd_net:.2f} ({realized_pnl_pct_levered:+.2f}%) [Gross: ${realized_pnl_usd_gross:.2f}, Commission: ${commission:.2f}, Funding: ${funding_fee:.2f}]")
                except Exception as e:
                    logger.error(f"Failed to log to Firestore: {str(e)}")
            
            closed_trades.append(trade_data)
        
        # Return the first closed trade (or aggregate if needed)
        if closed_trades:
            if len(closed_trades) > 1:
                logger.info(f"✅ Closed {len(closed_trades)} positions for {symbol}")
            return closed_trades[0]  # Return first trade for backward compatibility
        
        return None
        
        position = self.open_positions[position_key]
        exit_time = exit_time or datetime.utcnow()
        
        # Calculate realized P&L
        # For futures contracts: P&L USD = (exit_price - entry_price) * quantity * side_multiplier
        if position['side'] == 'LONG':
            price_change_pct = ((exit_price - position['entry_price']) / position['entry_price']) * 100
            realized_pnl_usd = (exit_price - position['entry_price']) * position['quantity']
        else:  # SHORT
            price_change_pct = ((position['entry_price'] - exit_price) / position['entry_price']) * 100
            realized_pnl_usd = (position['entry_price'] - exit_price) * position['quantity']
        
        # Get deployed capital (margin) - use stored value or calculate from notional/leverage
        deployed_capital = position.get('deployed_capital')
        if deployed_capital is None or deployed_capital <= 0:
            # Fallback: calculate from notional value and leverage
            notional_value = position['quantity'] * position['entry_price']
            leverage = position.get('leverage', 1)
            deployed_capital = notional_value / leverage if leverage > 0 else notional_value
        
        # Get commission and funding fees (if provided)
        # If not provided, calculate defaults
        if commission is None:
            # Default commission: 0.04% total (0.02% entry + 0.02% exit)
            notional_value = position['quantity'] * position['entry_price']
            commission = notional_value * 0.0004  # 0.04% total
        
        # Calculate trade duration (used for funding fee calculation)
        duration_hours = (exit_time - position['entry_time']).total_seconds() / 3600
        
        if funding_fee is None:
            # Calculate funding fee based on holding time and funding rate
            # Coinbase: ~0.0003%/hr (0.000003), Binance: ~0.00125%/hr (0.0000125)
            # Default to Coinbase rate (0.0003%/hr)
            # TODO: In LIVE mode, fetch actual funding rate from broker API
            funding_rate_per_hour = 0.000003  # 0.0003% = 0.000003 (Coinbase default)
            
            # Funding payment = Funding Rate × Position Size × Hours
            # Position Size = Quantity × Mark Price (average during holding period)
            mark_price = (position['entry_price'] + exit_price) / 2  # Average price during holding
            position_size = position['quantity'] * mark_price
            
            # Calculate base funding payment (positive rate = longs pay, shorts receive)
            base_funding = funding_rate_per_hour * position_size * duration_hours
            
            if position['side'] == 'LONG':
                # Longs pay when funding rate is positive (negative for us)
                funding_fee = -base_funding
            else:  # SHORT
                # Shorts receive when funding rate is positive (positive for us)
                funding_fee = base_funding
        
        # Calculate NET P&L (gross P&L - commission - funding fees)
        realized_pnl_usd_gross = realized_pnl_usd
        realized_pnl_usd_net = realized_pnl_usd_gross - commission - funding_fee
        
        # Calculate P&L percentages
        # Price change percentage (unlevered)
        realized_pnl_pct = price_change_pct
        
        # Gross leveraged P&L percentage (as % of deployed capital)
        realized_pnl_pct_gross = (realized_pnl_usd_gross / deployed_capital * 100) if deployed_capital > 0 else 0.0
        
        # Net leveraged P&L percentage (as % of deployed capital) - includes commissions and funding
        realized_pnl_pct_levered = (realized_pnl_usd_net / deployed_capital * 100) if deployed_capital > 0 else 0.0
        
        # Build closed trade record
        trade_data = {
            **position,
            'exit_price': exit_price,
            'exit_time': exit_time,
            'realized_pnl_gross': realized_pnl_usd_gross,
            'realized_pnl_net': realized_pnl_usd_net,
            'realized_pnl': realized_pnl_usd_net,  # Keep for backward compatibility
            'realized_pnl_pct': realized_pnl_pct,
            'realized_pnl_pct_gross': realized_pnl_pct_gross,
            'realized_pnl_pct_levered': realized_pnl_pct_levered,
            'commission': commission,
            'funding_fee': funding_fee,
            'duration_hours': duration_hours,
            'status': 'closed',
            'win': realized_pnl_usd_net > 0
        }
        
        # Update daily stats (use NET P&L including commissions and funding)
        date_key = exit_time.date().isoformat()
        self.daily_stats[date_key]['pnl'] += realized_pnl_usd_net
        self.daily_stats[date_key]['trades'] += 1
        if trade_data['win']:
            self.daily_stats[date_key]['wins'] += 1
        else:
            self.daily_stats[date_key]['losses'] += 1
        
        # Store closed trade
        self.closed_trades.append(trade_data)
        
        # Remove from open positions
        del self.open_positions[position_key]
        
        # Update Firestore
        if self.use_firestore:
            try:
                # Update position document
                self.positions_collection.document(position['trade_id']).update({
                    'status': 'closed',
                    'exit_price': exit_price,
                    'exit_time': exit_time,
                    'realized_pnl_gross': realized_pnl_usd_gross,
                    'realized_pnl_net': realized_pnl_usd_net,
                    'realized_pnl': realized_pnl_usd_net,  # Keep for backward compatibility
                    'realized_pnl_pct': realized_pnl_pct_levered,
                    'realized_pnl_pct_gross': realized_pnl_pct_gross,
                    'realized_pnl_pct_unlevered': realized_pnl_pct,
                    'commission': commission,
                    'funding_fee': funding_fee,
                    'duration_hours': duration_hours,
                    'win': trade_data['win']
                })
                
                # Also add to trades history
                self.trades_collection.add(trade_data)
                
                logger.info(f"✅ P&L: Logged trade close: {position['trade_id']} - P&L NET: ${realized_pnl_usd_net:.2f} ({realized_pnl_pct_levered:+.2f}%) [Gross: ${realized_pnl_usd_gross:.2f}, Commission: ${commission:.2f}, Funding: ${funding_fee:.2f}]")
            except Exception as e:
                logger.error(f"Failed to log to Firestore: {str(e)}")
        
        logger.info(f"✅ P&L: Position closed and removed from open_positions: {position_key}")
        return trade_data
    
    async def update_open_pnl(self, symbol: str, strategy: str, current_price: float, account: Optional[str] = None):
        """
        Update unrealized P&L for open position
        
        Args:
            symbol: Trading symbol
            strategy: Strategy name
            current_price: Current market price
            account: Account ID (optional, if None will update all matching positions)
        """
        account_part = account or "unknown"
        position_key = f"{symbol}_{strategy}_{account_part}"
        
        if position_key not in self.open_positions:
            return
        
        position = self.open_positions[position_key]
        
        # Calculate unrealized P&L
        if position['side'] == 'LONG':
            unrealized_pnl_pct = ((current_price - position['entry_price']) / position['entry_price']) * 100
        else:  # SHORT
            unrealized_pnl_pct = ((position['entry_price'] - current_price) / position['entry_price']) * 100
        
        # Apply leverage
        unrealized_pnl_pct_levered = unrealized_pnl_pct * position['leverage']
        
        # Calculate dollar PnL
        notional_value = position['quantity'] * position['entry_price']
        unrealized_pnl_usd = (unrealized_pnl_pct / 100) * notional_value
        
        # Update position
        position['current_price'] = current_price
        position['unrealized_pnl'] = unrealized_pnl_usd
        position['unrealized_pnl_pct'] = unrealized_pnl_pct_levered
        
        return position
    
    async def get_open_positions_all(self) -> List[Dict]:
        """Get all open positions across all accounts/strategies"""
        return list(self.open_positions.values())
    
    async def get_closed_trades(self, days: int = 7) -> List[Dict]:
        """
        Get closed trades from last N days
        
        Args:
            days: Number of days to look back (default: 7)
        """
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        if self.use_firestore:
            try:
                trades = self.trades_collection.where(
                    'exit_time', '>=', cutoff
                ).order_by('exit_time', direction=firestore.Query.DESCENDING).stream()
                
                return [trade.to_dict() for trade in trades]
            except Exception as e:
                logger.error(f"Failed to fetch from Firestore: {str(e)}")
        
        # Fallback to in-memory
        return [
            trade for trade in self.closed_trades
            if trade['exit_time'] >= cutoff
        ]
    
    async def get_statistics(self, period: str = 'today') -> Dict:
        """
        Get trading statistics for a period
        
        Args:
            period: 'today', 'week', 'month', 'all'
        
        Returns:
            Statistics dict with P&L, trades, win rate, etc.
        """
        now = datetime.utcnow()
        
        if period == 'today':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period == 'week':
            start_date = now - timedelta(days=7)
        elif period == 'month':
            start_date = now - timedelta(days=30)
        else:  # all
            start_date = datetime(2020, 1, 1)
        
        # Determine lookback window (at least 1 day) for fetching trades
        lookback_seconds = max((now - start_date).total_seconds(), 0)
        lookback_days = max(1, math.ceil(lookback_seconds / 86400))

        trades = await self.get_closed_trades(days=lookback_days)

        filtered_trades = []
        for trade in trades:
            exit_time = trade.get('exit_time')

            exit_dt = None
            if isinstance(exit_time, datetime):
                exit_dt = exit_time
            elif isinstance(exit_time, str):
                iso_time = exit_time.replace("Z", "+00:00") if exit_time.endswith("Z") else exit_time
                try:
                    exit_dt = datetime.fromisoformat(iso_time)
                except ValueError:
                    exit_dt = None

            if exit_dt is None:
                # If we cannot parse, include the trade to avoid losing data
                filtered_trades.append(trade)
                continue

            if exit_dt >= start_date:
                filtered_trades.append(trade)

        trades = filtered_trades
        
        if not trades:
            return {
                'period': period,
                'total_trades': 0,
                'total_pnl': 0.0,
                'total_pnl_pct': 0.0,
                'wins': 0,
                'losses': 0,
                'win_rate': 0.0,
                'avg_win': 0.0,
                'avg_loss': 0.0,
                'best_trade': None,
                'worst_trade': None
            }
        
        # Calculate stats
        total_pnl = sum(t.get('realized_pnl', 0) for t in trades)
        total_pnl_pct = sum(t.get('realized_pnl_pct_levered', 0) for t in trades)
        wins = [t for t in trades if t.get('win', False)]
        losses = [t for t in trades if not t.get('win', False)]
        
        win_pnls = [t['realized_pnl'] for t in wins] if wins else [0]
        loss_pnls = [t['realized_pnl'] for t in losses] if losses else [0]
        
        # Sort by P&L for best/worst
        sorted_trades = sorted(trades, key=lambda t: t.get('realized_pnl', 0), reverse=True)
        
        return {
            'period': period,
            'start_date': start_date.isoformat(),
            'end_date': now.isoformat(),
            'total_trades': len(trades),
            'total_pnl': total_pnl,
            'total_pnl_pct': total_pnl_pct,
            'wins': len(wins),
            'losses': len(losses),
            'win_rate': (len(wins) / len(trades) * 100) if trades else 0.0,
            'avg_win': sum(win_pnls) / len(wins) if wins else 0.0,
            'avg_loss': sum(loss_pnls) / len(losses) if losses else 0.0,
            'profit_factor': abs(sum(win_pnls) / sum(loss_pnls)) if sum(loss_pnls) != 0 else 0.0,
            'best_trade': sorted_trades[0] if sorted_trades else None,
            'worst_trade': sorted_trades[-1] if sorted_trades else None
        }
    
    async def get_account_stats(self) -> List[Dict]:
        """
        Get statistics per account (grouped by account_id and mode)
        
        Returns:
            List of account stats sorted by P&L, with account_id and mode fields
        """
        trades = await self.get_closed_trades(days=30)  # Last 30 days
        
        # Group by account_id and mode
        accounts = defaultdict(lambda: {
            'trades': [],
            'pnl': 0.0,
            'pnl_pct': 0.0,
            'wins': 0,
            'losses': 0,
            'account_id': None,
            'account_mode': None
        })
        
        for trade in trades:
            account_id = trade.get('account', 'unknown')
            account_mode = trade.get('account_mode', 'unknown')
            
            # Create composite key: account_id_mode
            account_key = f"{account_id}_{account_mode}"
            
            accounts[account_key]['trades'].append(trade)
            accounts[account_key]['pnl'] += trade.get('realized_pnl', 0)
            accounts[account_key]['pnl_pct'] += trade.get('realized_pnl_pct_levered', 0)
            accounts[account_key]['account_id'] = account_id
            accounts[account_key]['account_mode'] = account_mode
            
            if trade.get('win', False):
                accounts[account_key]['wins'] += 1
            else:
                accounts[account_key]['losses'] += 1
        
        # Build result
        result = []
        for account_key, stats in accounts.items():
            total_trades = len(stats['trades'])
            result.append({
                'account': stats['account_id'],
                'account_id': stats['account_id'],
                'account_mode': stats['account_mode'],
                'total_trades': total_trades,
                'total_pnl': stats['pnl'],
                'total_pnl_pct': stats['pnl_pct'],
                'wins': stats['wins'],
                'losses': stats['losses'],
                'win_rate': (stats['wins'] / total_trades * 100) if total_trades > 0 else 0.0,
                'avg_pnl_per_trade': stats['pnl'] / total_trades if total_trades > 0 else 0.0
            })
        
        # Sort by total P&L
        result.sort(key=lambda x: x['total_pnl'], reverse=True)
        
        return result
    
    async def get_account_stats_by_mode(self, mode: str) -> List[Dict]:
        """
        Get statistics for accounts filtered by mode (DEMO or LIVE)
        
        Args:
            mode: "DEMO" or "LIVE"
        
        Returns:
            List of account stats for the specified mode, sorted by P&L
        """
        all_stats = await self.get_account_stats()
        return [stat for stat in all_stats if stat.get('account_mode', '').upper() == mode.upper()]
    
    async def get_demo_account_stats(self) -> List[Dict]:
        """Get statistics for all DEMO accounts"""
        return await self.get_account_stats_by_mode("DEMO")
    
    async def get_live_account_stats(self) -> List[Dict]:
        """Get statistics for all LIVE accounts"""
        return await self.get_account_stats_by_mode("LIVE")
    
    async def get_liquidation_risk_all(self, current_prices: Dict[str, float]) -> List[Dict]:
        """
        Calculate liquidation risk for all open positions
        
        Args:
            current_prices: Dict of {symbol: current_price}
        
        Returns:
            List of positions with liquidation risk assessment
        """
        risks = []
        
        for position_key, position in self.open_positions.items():
            symbol = position['symbol']
            current_price = current_prices.get(symbol, position['current_price'])
            
            if not position.get('liquidation_price'):
                continue
            
            # Calculate distance to liquidation
            if position['side'] == 'LONG':
                distance_to_liq_pct = ((current_price - position['liquidation_price']) / current_price) * 100
            else:  # SHORT
                distance_to_liq_pct = ((position['liquidation_price'] - current_price) / current_price) * 100
            
            # Determine risk level
            if distance_to_liq_pct < 5:
                risk_level = "CRITICAL"
            elif distance_to_liq_pct < 10:
                risk_level = "HIGH"
            elif distance_to_liq_pct < 20:
                risk_level = "MODERATE"
            else:
                risk_level = "LOW"
            
            risks.append({
                'strategy': position['strategy'],
                'symbol': position['symbol'],
                'side': position['side'],
                'leverage': position['leverage'],
                'entry_price': position['entry_price'],
                'current_price': current_price,
                'liquidation_price': position['liquidation_price'],
                'distance_to_liquidation_pct': distance_to_liq_pct,
                'risk_level': risk_level,
                'unrealized_pnl': position.get('unrealized_pnl', 0),
                'unrealized_pnl_pct': position.get('unrealized_pnl_pct', 0)
            })
        
        # Sort by risk (most critical first)
        risk_order = {"CRITICAL": 0, "HIGH": 1, "MODERATE": 2, "LOW": 3}
        risks.sort(key=lambda x: risk_order[x['risk_level']])
        
        return risks
    
    async def should_halt_trading(self, current_prices: Dict[str, float]) -> tuple[bool, str]:
        """
        Determine if trading should be halted due to high risk
        
        Halts trading if:
        - Any position within 10% of liquidation
        - Daily loss exceeds 20% (configurable)
        - Multiple consecutive losses (>5)
        
        Returns:
            tuple: (should_halt, reason)
        """
        # Check liquidation risk
        risks = await self.get_liquidation_risk_all(current_prices)
        
        for risk in risks:
            if risk['risk_level'] in ['CRITICAL', 'HIGH']:
                return (
                    True,
                    f"LIQUIDATION RISK: {risk['strategy']} {risk['symbol']} @ {risk['leverage']}x "
                    f"is {risk['distance_to_liquidation_pct']:.1f}% from liquidation. "
                    f"Current: ${risk['current_price']:,.2f}, Liq: ${risk['liquidation_price']:,.2f}"
                )
        
        # Check daily loss
        today = datetime.utcnow().date().isoformat()
        today_stats = self.daily_stats[today]
        
        if today_stats['pnl'] < -200:  # More than $200 daily loss
            return (
                True,
                f"DAILY LOSS LIMIT: Lost ${abs(today_stats['pnl']):.2f} today. "
                f"Halting to prevent further losses."
            )
        
        # Check consecutive losses (last 5 trades)
        recent_trades = self.closed_trades[-5:]  # Last 5
        if len(recent_trades) >= 5:
            consecutive_losses = all(not t.get('win', False) for t in recent_trades)
            if consecutive_losses:
                return (
                    True,
                    f"CONSECUTIVE LOSSES: 5 losing trades in a row. "
                    f"Halting to review strategy."
                )
        
        return (False, "All checks passed")
    
    async def get_best_and_worst_trades(self, days: int = 30) -> Dict:
        """
        Get best and worst trades from last N days
        
        Returns:
            Dict with best and worst trades
        """
        trades = await self.get_closed_trades(days=days)
        
        if not trades:
            return {
                'best_trade': None,
                'worst_trade': None,
                'period_days': days
            }
        
        sorted_trades = sorted(trades, key=lambda t: t.get('realized_pnl', 0), reverse=True)
        
        return {
            'best_trade': {
                'strategy': sorted_trades[0]['strategy'],
                'symbol': sorted_trades[0]['symbol'],
                'side': sorted_trades[0]['side'],
                'pnl': sorted_trades[0]['realized_pnl'],
                'pnl_pct': sorted_trades[0]['realized_pnl_pct_levered'],
                'entry_price': sorted_trades[0]['entry_price'],
                'exit_price': sorted_trades[0]['exit_price'],
                'duration_hours': sorted_trades[0]['duration_hours'],
                'exit_time': sorted_trades[0]['exit_time'].isoformat() if isinstance(sorted_trades[0]['exit_time'], datetime) else sorted_trades[0]['exit_time']
            },
            'worst_trade': {
                'strategy': sorted_trades[-1]['strategy'],
                'symbol': sorted_trades[-1]['symbol'],
                'side': sorted_trades[-1]['side'],
                'pnl': sorted_trades[-1]['realized_pnl'],
                'pnl_pct': sorted_trades[-1]['realized_pnl_pct_levered'],
                'entry_price': sorted_trades[-1]['entry_price'],
                'exit_price': sorted_trades[-1]['exit_price'],
                'duration_hours': sorted_trades[-1]['duration_hours'],
                'exit_time': sorted_trades[-1]['exit_time'].isoformat() if isinstance(sorted_trades[-1]['exit_time'], datetime) else sorted_trades[-1]['exit_time']
            },
            'period_days': days,
            'total_trades': len(trades)
        }
    
    async def reconcile_with_partition_state(self):
        """
        Reconcile P&L tracker open positions with partition state.
        
        If a position exists in P&L tracker but not in partition state,
        it means the position was closed but the P&L tracker wasn't updated.
        Mark these positions as closed in Firestore and remove from memory.
        """
        try:
            from services.partition_manager import partition_state
            
            # Get all open positions from partition state (source of truth)
            partition_positions = {}
            for account_id, account_data in partition_state.items():
                partitions = account_data.get('partitions', {})
                for partition_id, partition_data in partitions.items():
                    open_positions = partition_data.get('open_positions', {})
                    for symbol, position in open_positions.items():
                        # Create key: symbol_strategy_account
                        strategy_id = position.get('strategy_id', 'unknown')
                        key = f"{symbol}_{strategy_id}_{account_id}"
                        partition_positions[key] = position
            
            # Check each P&L tracker position
            stale_positions = []
            for key, position in list(self.open_positions.items()):
                pos_symbol = position.get('symbol', '')
                pos_strategy = position.get('strategy', '')
                pos_account = position.get('account', 'unknown')
                
                # Normalize symbol for comparison
                symbol_normalized = pos_symbol.upper().replace('-CBSE', '').replace('-PERP', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
                
                # Check if position exists in partition state
                found = False
                for part_key, part_pos in partition_positions.items():
                    part_symbol = part_pos.get('symbol', '').upper()
                    part_strategy = part_pos.get('strategy_id', '')
                    part_account = part_key.split('_')[-1] if '_' in part_key else 'unknown'
                    
                    part_symbol_norm = part_symbol.replace('-CBSE', '').replace('-PERP', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
                    
                    if (part_account == pos_account and 
                        (part_symbol == pos_symbol.upper() or part_symbol_norm == symbol_normalized) and
                        part_strategy == pos_strategy):
                        found = True
                        break
                
                if not found:
                    # Position doesn't exist in partition state - it's stale
                    stale_positions.append((key, position))
            
            # Mark stale positions as closed
            if stale_positions:
                logger.warning(f"🔍 P&L Tracker: Found {len(stale_positions)} stale position(s) not in partition state - marking as closed")
                
                for key, position in stale_positions:
                    # Remove from memory
                    del self.open_positions[key]
                    
                    # Update Firestore to mark as closed
                    if self.use_firestore:
                        try:
                            trade_id = position.get('trade_id')
                            if trade_id:
                                self.positions_collection.document(trade_id).update({
                                    'status': 'closed',
                                    'reconciled': True,
                                    'reconciled_at': datetime.utcnow()
                                })
                                logger.info(f"✅ P&L Tracker: Marked stale position as closed: {trade_id} ({position.get('symbol')})")
                        except Exception as e:
                            logger.error(f"Failed to update stale position in Firestore: {e}")
        
        except Exception as e:
            logger.error(f"Error reconciling P&L tracker with partition state: {e}")

