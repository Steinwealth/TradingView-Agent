"""
TradingView Agent - Agent-Level Exit Monitoring Service
Monitors all open positions and executes exits based on v13 Ichimoku strategy conditions

This service runs independently of TradingView signals, providing lightning-fast
exit execution for breakeven, RSI exits, trailing stops, and other exit conditions.
"""

import logging
import asyncio
import aiohttp
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum

# Lazy imports to avoid circular dependency
from services.telegram_notifier import telegram_notifier
from config import settings

# Lazy import to avoid circular dependency
order_executor = None

def _get_order_executor():
    """Get order_executor instance (lazy import to avoid circular dependency)"""
    global order_executor
    if order_executor is None:
        from main import order_executor as oe
        order_executor = oe
    return order_executor

logger = logging.getLogger(__name__)


def get_price_precision(symbol: str) -> int:
    """
    Get appropriate price precision for a symbol.
    
    XRP and other low-price assets need 4 decimal places for precision.
    BTC, ETH, and other high-price assets use 2 decimal places.
    
    Args:
        symbol: Trading symbol (e.g., "XPP-CBSE", "BTC-PERP", "ETP-CBSE")
    
    Returns:
        Number of decimal places (2 or 4)
    """
    symbol_upper = symbol.upper()
    # XRP and other low-price assets need 4 decimals
    if "XRP" in symbol_upper or "XPP" in symbol_upper:
        return 4
    # BTC, ETH, SOL use 2 decimals
    return 2


def format_price(price: float, symbol: str) -> str:
    """
    Format price with appropriate precision for symbol.
    
    Args:
        price: Price value
        symbol: Trading symbol
    
    Returns:
        Formatted price string
    """
    precision = get_price_precision(symbol)
    return f"${price:,.{precision}f}"


class ExitReason(Enum):
    """Exit reason codes"""
    RSI_EXIT_LONG = "RSI_EXIT_LONG"
    RSI_EXIT_SHORT = "RSI_EXIT_SHORT"
    BREAKEVEN_STOP = "BREAKEVEN_STOP"
    BREAKEVEN_3H = "BREAKEVEN_3H"  # 3-hour breakeven exit
    BREAKEVEN_6H = "BREAKEVEN_6H"  # 6-hour persistent breakeven exit
    TRAILING_STOP = "TRAILING_STOP"
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"
    EOD_AUTO_CLOSE = "EOD_AUTO_CLOSE"
    AGENT_MONITORED = "AGENT_MONITORED"  # Generic agent exit
    ICHIMOKU_CLOUD_EXIT = "ICHIMOKU_CLOUD_EXIT"  # Exit based on Ichimoku cloud violation
    ICHIMOKU_TK_CROSS_EXIT = "ICHIMOKU_TK_CROSS_EXIT"  # Exit based on TK cross opposite direction


@dataclass
class ExitSettings:
    """Symbol-specific exit settings from v13 Ichimoku strategy"""
    # RSI Exit Settings
    use_rsi_exit: bool = True
    rsi_exit_long: float = 30.0  # Close long when RSI falls below this
    rsi_exit_short: float = 74.0  # Close short when RSI rises above this
    rsi_exit_activation_pct: float = 0.7  # RSI exit only activates after this profit %
    
    # Breakeven Settings
    use_breakeven: bool = True
    breakeven_trigger: float = 1.5  # Activate breakeven at this profit %
    breakeven_level: float = 0.3  # Lock profit at this %
    
    # 3-Hour Breakeven Exit Settings (Available but disabled by default)
    use_3hour_breakeven: bool = False  # Disabled by default - will be calibrated for v14 preset later
    breakeven_3h_min_profit: float = 0.3  # Minimum profit % to trigger breakeven (e.g., +0.3%)
    breakeven_3h_min_pct: float = 0.02  # Minimum P&L % to exit (e.g., +0.02%)
    breakeven_3h_max_pct: float = 0.05  # Maximum P&L % to exit (e.g., +0.05%)
    
    # 6-Hour Persistent Breakeven Settings (Enhanced)
    use_6hour_breakeven: bool = True  # Enable 6-hour persistent breakeven
    breakeven_6h_min_pct: float = 0.1  # Minimum P&L % to exit (e.g., +0.1%)
    breakeven_6h_max_pct: float = 0.5  # Maximum P&L % to exit (e.g., +0.5%)
    
    # Trailing Stop Settings
    use_trailing: bool = True
    trailing_trigger: float = 2.0  # Activate trailing at this profit %
    trailing_dist: float = 1.0  # Trail distance %
    
    # Stop Loss / Take Profit
    use_agent_stop_loss: bool = True  # Enable/disable Agent-level stop loss monitoring
    # Default: True - Agent monitors stop loss to ensure exits execute even if TradingView webhooks delayed
    # If false, Agent will NOT monitor stop loss (TradingView strategy manages it via webhooks only)
    # If true, Agent monitors stop loss (uses TradingView stop loss if provided in alert, otherwise calculates with minimum protection)
    stop_mode: str = "ATR-Based"  # "ATR-Based" or "Fixed %"
    stop_loss_pct: float = 1.5
    take_profit_pct: float = 4.0
    atr_length: int = 8
    atr_mult: float = 13.0
    
    # EOD Auto-Close
    auto_close_eod: bool = False
    eod_close_time: str = "15:45"  # ET time (3:45 PM ET)
    
    # RSI Calculation
    rsi_length: int = 3
    
    # ATR Calculation (for dynamic stops)
    atr_calculation_enabled: bool = True
    
    # Ichimoku Cloud Exit Monitoring (NEW - Backup to TradingView signals)
    use_ichimoku_cloud_exit: bool = True  # Enable Ichimoku cloud-based exit monitoring
    ichimoku_conversion_periods: int = 9  # Tenkan period (default, overridden by preset)
    ichimoku_base_periods: int = 26  # Kijun period (default, overridden by preset)
    ichimoku_lagging_span2_periods: int = 52  # Senkou B period (default, overridden by preset)
    ichimoku_displacement: int = 26  # Displacement (default, overridden by preset)


@dataclass
class MonitoredPosition:
    """Position being monitored for exits"""
    account_id: str
    partition_id: str
    symbol: str
    side: str  # "LONG" or "SHORT"
    quantity: float
    entry_price: float
    entry_time: datetime
    leverage: int
    strategy_id: str
    timeframe: Optional[str] = None  # Strategy timeframe (5m, 1h, etc.) - used for correct candle granularity
    
    # Dynamic tracking
    current_price: float = 0.0
    highest_price: float = 0.0  # For trailing stop (LONG)
    lowest_price: float = float('inf')  # For trailing stop (SHORT)
    current_stop: Optional[float] = None
    breakeven_set: bool = False
    breakeven_hit_count: int = 0  # Track consecutive hits for confirmation
    trailing_active: bool = False
    
    # Current indicators (updated every check)
    rsi: Optional[float] = None  # None = not calculated yet, otherwise actual RSI value
    atr: float = 0.0
    
    # Ichimoku Cloud indicators (for cloud-based exit monitoring)
    ichimoku_tenkan: Optional[float] = None
    ichimoku_kijun: Optional[float] = None
    ichimoku_senkou_a: Optional[float] = None
    ichimoku_senkou_b: Optional[float] = None
    ichimoku_cloud_top: Optional[float] = None
    ichimoku_cloud_bottom: Optional[float] = None
    ichimoku_cloud_bullish: Optional[bool] = None  # True if senkouA > senkouB
    price_above_cloud: Optional[bool] = None  # True if price > cloud_top
    price_below_cloud: Optional[bool] = None  # True if price < cloud_bottom
    tk_cross_down: bool = False  # True if tenkan crossed below kijun (exit signal for LONG)
    tk_cross_up: bool = False  # True if tenkan crossed above kijun (exit signal for SHORT)
    previous_tenkan: Optional[float] = None  # Previous tenkan value for cross detection
    previous_kijun: Optional[float] = None  # Previous kijun value for cross detection
    
    # P&L tracking
    unrealized_pnl_pct: float = 0.0
    unrealized_pnl_usd: float = 0.0
    peak_profit_pct: float = 0.0  # Maximum profit % reached during trade (for optimization analysis)
    peak_profit_time: Optional[datetime] = None  # Time when peak profit occurred
    peak_price_change_pct: float = 0.0  # Maximum price change % (unleveraged) - for 6H breakeven tracking
    was_in_6h_range: bool = False  # Track if price has been in 6H breakeven range at any point (for entry-based exit)
    last_price_change_pct: float = 0.0  # Previous price change % (to detect when entering range)
    breakeven_6h_stop_set: bool = False  # Track if 6H breakeven stop has been moved to max level
    breakeven_6h_stop_price: Optional[float] = None  # Stop price at max level for 6H breakeven
    
    # TradingView stop loss (source of truth - use this instead of calculating)
    tradingview_stop_loss: Optional[float] = None  # Stop loss price from TradingView alert (if provided)
    
    # Exit settings (symbol-specific)
    exit_settings: ExitSettings = field(default_factory=ExitSettings)


@dataclass
class ExitDecision:
    """Decision to exit a position"""
    position: MonitoredPosition
    reason: ExitReason
    exit_price: float
    message: str


class ExitMonitor:
    """
    Agent-level exit monitoring service
    
    Monitors all open positions every 30 seconds and executes exits when:
    - RSI exit conditions are met
    - Breakeven protection triggers
    - Trailing stop is hit
    - Stop loss is hit
    - Take profit is hit
    - EOD auto-close time arrives
    """
    
    def __init__(self):
        self.is_running = False
        self.monitor_task: Optional[asyncio.Task] = None
        self.check_interval_seconds = 30  # Check every 30 seconds
        self.monitored_positions: Dict[str, MonitoredPosition] = {}  # Key: f"{account_id}_{partition_id}_{symbol}"
        self.exit_settings_cache: Dict[str, ExitSettings] = {}  # Key: symbol
        self.last_check_time: Optional[datetime] = None
        
    async def start_monitoring(self):
        """Start the exit monitoring service"""
        if self.is_running:
            logger.warning("Exit monitor is already running")
            return
        
        self.is_running = True
        self.monitor_task = asyncio.create_task(self._monitoring_loop())
        logger.info(f"🚀 Agent-level exit monitoring started (check interval: {self.check_interval_seconds}s)")
    
    async def stop_monitoring(self):
        """Stop the exit monitoring service"""
        if not self.is_running:
            return
        
        self.is_running = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        
        logger.info("🛑 Agent-level exit monitoring stopped")
    
    async def _monitoring_loop(self):
        """Main monitoring loop - checks positions every 30 seconds"""
        logger.info("🔄 Exit monitoring loop started")
        
        while self.is_running:
            try:
                await self._check_all_positions()
                self.last_check_time = datetime.utcnow()
                
                # Wait for next check
                await asyncio.sleep(self.check_interval_seconds)
                
            except asyncio.CancelledError:
                logger.info("Exit monitoring cancelled")
                break
            except Exception as e:
                logger.error(f"Error in exit monitoring loop: {e}", exc_info=True)
                # Continue monitoring despite errors
                await asyncio.sleep(self.check_interval_seconds)
    
    async def _check_all_positions(self):
        """Check all open positions across all accounts"""
        try:
            # Get all account states (source of truth for open positions)
            # We need to get all accounts from order_executor or config
            oe = _get_order_executor()
            all_account_ids = list(oe.account_configs.keys()) if hasattr(oe, 'account_configs') else []
            
            if not all_account_ids:
                logger.debug("No accounts configured")
                return
            
            # Lazy import to avoid circular dependency
            from services.partition_manager import get_account_state, get_partition_summaries
            
            # Build partition state dict from all accounts
            partition_state = {}
            for account_id in all_account_ids:
                account_state = get_account_state(account_id)
                if account_state:
                    partition_state[account_id] = account_state
            
            if not partition_state:
                logger.debug("No partition state available")
                return
            
            # Update monitored positions from partition state
            await self._sync_positions_from_partition_state(partition_state)
            
            # Check each monitored position for exit conditions
            total_positions = len(self.monitored_positions)
            logger.info(f"🔍 Checking {total_positions} position(s) for exit conditions")
            
            # Collect all exit decisions first (don't execute immediately)
            # This ensures we check ALL positions before executing any exits
            exit_decisions = []
            
            for position_key, position in list(self.monitored_positions.items()):
                try:
                    logger.info(f"🔍 Checking position: {position.symbol} {position.side} in {position.partition_id}")
                    
                    # Update current price and indicators
                    await self._update_position_data(position)
                    
                    # Check exit conditions
                    exit_decision = await self._check_exit_conditions(position)
                    
                    if exit_decision:
                        logger.info(f"✅ Exit condition met for {position.symbol} {position.side} in {position.partition_id}: {exit_decision.reason.value}")
                        exit_decisions.append((position_key, exit_decision))
                    else:
                        logger.debug(f"⏸️ No exit condition met for {position.symbol} {position.side} in {position.partition_id}")
                        
                except Exception as e:
                    logger.error(f"❌ Error checking position {position_key}: {e}", exc_info=True)
                    # Continue checking other positions even if one fails
                    continue
            
            # Now execute all exits (this ensures we check all positions first)
            # CRITICAL: Deduplicate by symbol - if multiple positions with the same symbol
            # meet exit conditions, only create ONE exit payload (order executor will close all)
            symbols_exited = set()
            
            for position_key, exit_decision in exit_decisions:
                try:
                    symbol = exit_decision.position.symbol
                    
                    # Skip if we've already created an exit payload for this symbol
                    # The order executor will close ALL positions with this symbol
                    if symbol in symbols_exited:
                        logger.debug(f"⏭️ Skipping duplicate exit for {symbol} - already processing")
                        # Still remove from monitoring since the order executor will close it
                        if position_key in self.monitored_positions:
                            del self.monitored_positions[position_key]
                        continue
                    
                    logger.info(f"🚪 Executing exit for {exit_decision.position.symbol} {exit_decision.position.side} in {exit_decision.position.partition_id}")
                    exit_success = await self._execute_exit(exit_decision)
                    
                    # Mark this symbol as exited
                    symbols_exited.add(symbol)
                    
                    # Only remove from monitoring if exit was successful
                    # If exit failed, keep monitoring for retry
                    if exit_success:
                        # Remove ALL positions with this symbol from monitoring
                        # (order executor will close all of them)
                        positions_to_remove = [
                            key for key, pos in list(self.monitored_positions.items())
                            if pos.symbol == symbol and pos.account_id == exit_decision.position.account_id
                        ]
                        for key in positions_to_remove:
                            del self.monitored_positions[key]
                            logger.info(f"✅ Position {key} removed from monitoring after successful exit")
                except Exception as e:
                    logger.error(f"❌ Error executing exit for {position_key}: {e}", exc_info=True)
                    # Continue with other exits even if one fails
                    continue
                    
        except Exception as e:
            logger.error(f"Error checking all positions: {e}", exc_info=True)
    
    async def _sync_positions_from_partition_state(self, partition_state: Dict):
        """Sync monitored positions from partition state"""
        try:
            current_position_keys: Set[str] = set()
            positions_found = []
            
            # Iterate through all accounts and partitions
            for account_id, account_data in partition_state.items():
                partitions = account_data.get('partitions', {})
                
                for partition_id, partition_data in partitions.items():
                    open_positions = partition_data.get('open_positions', {})
                    
                    logger.debug(f"🔍 Partition {partition_id} has {len(open_positions)} open position(s)")
                    
                    for symbol, position_data in open_positions.items():
                        position_key = f"{account_id}_{partition_id}_{symbol}"
                        current_position_keys.add(position_key)
                        side = position_data.get('side', 'UNKNOWN')
                        positions_found.append(f"{symbol} {side} in {partition_id}")
                        
                        # Get or create monitored position
                        if position_key not in self.monitored_positions:
                            # New position - create monitored position
                            monitored_pos = await self._create_monitored_position(
                                account_id, partition_id, symbol, position_data, partition_data
                            )
                            if monitored_pos:
                                self.monitored_positions[position_key] = monitored_pos
                                logger.info(f"📊 Started monitoring: {symbol} {monitored_pos.side} in {partition_id}")
                        else:
                            # Update existing position
                            monitored_pos = self.monitored_positions[position_key]
                            monitored_pos.quantity = position_data.get('quantity', 0.0)
                            # Entry price and time should remain constant
                            
            if positions_found:
                logger.debug(f"📊 Found {len(positions_found)} position(s) in partition state: {', '.join(positions_found)}")
            else:
                logger.debug("📊 No open positions found in partition state")
            
            # Remove positions that no longer exist in partition state
            positions_to_remove = set(self.monitored_positions.keys()) - current_position_keys
            for position_key in positions_to_remove:
                logger.info(f"📊 Stopped monitoring: {position_key} (position closed)")
                del self.monitored_positions[position_key]
                
        except Exception as e:
            logger.error(f"Error syncing positions from partition state: {e}", exc_info=True)
    
    async def _create_monitored_position(
        self,
        account_id: str,
        partition_id: str,
        symbol: str,
        position_data: Dict,
        partition_data: Dict
    ) -> Optional[MonitoredPosition]:
        """Create a monitored position from partition state data"""
        try:
            side = position_data.get('side', 'LONG').upper()
            quantity = float(position_data.get('quantity', 0.0))
            entry_price = float(position_data.get('entry_price', 0.0))
            leverage = int(position_data.get('leverage', 1))
            
            # Get entry time
            entry_time_str = position_data.get('entry_time')
            if entry_time_str:
                if isinstance(entry_time_str, str):
                    entry_time = datetime.fromisoformat(entry_time_str.replace('Z', '+00:00'))
                else:
                    entry_time = entry_time_str
            else:
                entry_time = datetime.utcnow()
            
            # Get strategy ID from partition data or infer from symbol
            strategy_id = partition_data.get('strategy_id') or self._infer_strategy_id(symbol)
            
            # Get timeframe from strategy config (needed for correct candle granularity)
            timeframe = await self._get_strategy_timeframe(strategy_id)
            
            # Get exit settings for this symbol
            exit_settings = await self._get_exit_settings(symbol, strategy_id)
            
            # Initialize highest/lowest price tracking
            highest_price = entry_price if side == "LONG" else 0.0
            lowest_price = entry_price if side == "SHORT" else float('inf')
            
            # Get TradingView stop loss price if stored in position data
            tradingview_stop_loss = position_data.get('tradingview_stop_loss')
            if tradingview_stop_loss:
                tradingview_stop_loss = float(tradingview_stop_loss)
                precision = get_price_precision(symbol)
                logger.debug(f"📌 Using TradingView stop loss for {symbol}: {format_price(tradingview_stop_loss, symbol)}")
            
            return MonitoredPosition(
                account_id=account_id,
                partition_id=partition_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                entry_price=entry_price,
                entry_time=entry_time,
                leverage=leverage,
                strategy_id=strategy_id,
                timeframe=timeframe,  # Store timeframe for correct candle granularity
                current_price=entry_price,  # Initial price
                highest_price=highest_price,
                lowest_price=lowest_price,
                tradingview_stop_loss=tradingview_stop_loss,
                exit_settings=exit_settings
            )
            
        except Exception as e:
            logger.error(f"Error creating monitored position: {e}", exc_info=True)
            return None
    
    def _infer_strategy_id(self, symbol: str) -> str:
        """Infer strategy ID from symbol (defaults to 1H strategies)"""
        symbol_upper = symbol.upper()
        if 'BIP' in symbol_upper or 'BTC' in symbol_upper:
            return "ichimoku-coinbase-btc-1h"  # Updated to 1H
        elif 'ETP' in symbol_upper or 'ETH' in symbol_upper:
            return "ichimoku-coinbase-eth-1h"  # Updated to 1H
        elif 'SLP' in symbol_upper or 'SOL' in symbol_upper:
            return "ichimoku-coinbase-sol-1h"  # Updated to 1H
        elif 'XPP' in symbol_upper or 'XRP' in symbol_upper:
            return "ichimoku-coinbase-xrp-1h"  # Updated to 1H
        else:
            return "unknown"
    
    async def _get_strategy_timeframe(self, strategy_id: str) -> Optional[str]:
        """Get strategy timeframe from config (5m, 1h, etc.)"""
        try:
            config_path = Path(__file__).parent.parent / "config.yaml"
            with open(config_path, 'r') as f:
                raw_config = yaml.safe_load(f)
            
            for strategy in raw_config.get('strategies', []):
                if strategy.get('id') == strategy_id:
                    return strategy.get('timeframe')  # Returns '5min', '1h', etc.
            
            return None
        except Exception as e:
            logger.warning(f"Failed to get timeframe for {strategy_id}: {e}")
            return None
    
    async def _get_exit_settings(self, symbol: str, strategy_id: str) -> ExitSettings:
        """
        Get exit settings for a symbol (from config or cache)
        
        Priority:
        1. Strategy-specific exit_settings (if defined)
        2. exit_settings_preset (if defined) - uses timeframe-based preset
        3. Auto-detect from strategy timeframe - uses matching preset
        4. Default settings
        """
        # Check cache first
        cache_key = f"{strategy_id}_{symbol}"
        if cache_key in self.exit_settings_cache:
            return self.exit_settings_cache[cache_key]
        
        # Get from config.yaml
        try:
            # Load raw YAML to access exit_settings (not in Pydantic model)
            config_path = Path(__file__).parent.parent / "config.yaml"
            with open(config_path, 'r') as f:
                raw_config = yaml.safe_load(f)
            
            # Find strategy in raw YAML
            strategy_config = None
            for strategy in raw_config.get('strategies', []):
                if strategy.get('id') == strategy_id:
                    strategy_config = strategy
                    break
            
            if strategy_config:
                # Priority 1: Check for strategy-specific exit_settings
                exit_config = strategy_config.get('exit_settings')
                
                # Priority 2: Check for exit_settings_preset
                if not exit_config:
                    preset_name = strategy_config.get('exit_settings_preset')
                    if preset_name:
                        presets = raw_config.get('exit_settings_presets', {})
                        exit_config = presets.get(preset_name)
                        if exit_config:
                            logger.debug(f"Using exit_settings_preset '{preset_name}' for {strategy_id}")
                
                # Priority 3: Auto-detect from timeframe
                if not exit_config:
                    timeframe = strategy_config.get('timeframe')
                    if timeframe:
                        # Normalize timeframe (5m, 1h, 4h, etc.)
                        timeframe_normalized = timeframe.lower().replace('min', 'm').replace('hour', 'h')
                        # Map common variations
                        if timeframe_normalized in ['5m', '5']:
                            preset_name = '5m'
                        elif timeframe_normalized in ['1h', '1', '60m', '60']:
                            preset_name = '1h'
                        elif timeframe_normalized in ['4h', '4', '240m', '240']:
                            preset_name = '4h'
                        else:
                            preset_name = None
                        
                        if preset_name:
                            presets = raw_config.get('exit_settings_presets', {})
                            exit_config = presets.get(preset_name)
                            if exit_config:
                                logger.debug(f"Auto-detected exit_settings_preset '{preset_name}' from timeframe '{timeframe}' for {strategy_id}")
            
            if exit_config:
                # Get Ichimoku preset values based on strategy_id
                ichimoku_preset = self._get_ichimoku_preset_values(strategy_id)
                
                # Create ExitSettings from config dict
                exit_settings = ExitSettings(
                    use_rsi_exit=exit_config.get('use_rsi_exit', True),
                    rsi_exit_long=float(exit_config.get('rsi_exit_long', 30.0)),
                    rsi_exit_short=float(exit_config.get('rsi_exit_short', 74.0)),
                    rsi_exit_activation_pct=float(exit_config.get('rsi_exit_activation_pct', 0.7)),
                    use_breakeven=exit_config.get('use_breakeven', True),
                    breakeven_trigger=float(exit_config.get('breakeven_trigger', 1.5)),
                    breakeven_level=float(exit_config.get('breakeven_level', 0.3)),
                    use_3hour_breakeven=exit_config.get('use_3hour_breakeven', False),  # Disabled by default
                    breakeven_3h_min_profit=float(exit_config.get('breakeven_3h_min_profit', 0.3)),
                    breakeven_3h_min_pct=float(exit_config.get('breakeven_3h_min_pct', 0.02)),
                    breakeven_3h_max_pct=float(exit_config.get('breakeven_3h_max_pct', 0.05)),
                    use_6hour_breakeven=exit_config.get('use_6hour_breakeven', True),
                    breakeven_6h_min_pct=float(exit_config.get('breakeven_6h_min_pct', 0.1)),
                    breakeven_6h_max_pct=float(exit_config.get('breakeven_6h_max_pct', 0.5)),
                    use_trailing=exit_config.get('use_trailing', True),
                    trailing_trigger=float(exit_config.get('trailing_trigger', 2.0)),
                    trailing_dist=float(exit_config.get('trailing_dist', 1.0)),
                    use_agent_stop_loss=bool(exit_config.get('use_agent_stop_loss', True)),  # Default: enabled - Agent monitors stop loss for timely exits
                    stop_mode=str(exit_config.get('stop_mode', 'ATR-Based')),
                    stop_loss_pct=float(exit_config.get('stop_loss_pct', 1.5)),
                    take_profit_pct=float(exit_config.get('take_profit_pct', 4.0)),
                    atr_length=int(exit_config.get('atr_length', 8)),
                    atr_mult=float(exit_config.get('atr_mult', 13.0)),
                    auto_close_eod=exit_config.get('auto_close_eod', False),
                    eod_close_time=str(exit_config.get('eod_close_time', '15:45')),
                    rsi_length=int(exit_config.get('rsi_length', 3)),
                    # Ichimoku Cloud Exit settings (NEW)
                    use_ichimoku_cloud_exit=exit_config.get('use_ichimoku_cloud_exit', True),  # Enabled by default for 1H strategies
                    ichimoku_conversion_periods=int(exit_config.get('ichimoku_conversion_periods', ichimoku_preset['conversion_periods'])),
                    ichimoku_base_periods=int(exit_config.get('ichimoku_base_periods', ichimoku_preset['base_periods'])),
                    ichimoku_lagging_span2_periods=int(exit_config.get('ichimoku_lagging_span2_periods', ichimoku_preset['lagging_span2_periods'])),
                    ichimoku_displacement=int(exit_config.get('ichimoku_displacement', ichimoku_preset['displacement']))
                )
                
                # Cache settings
                self.exit_settings_cache[cache_key] = exit_settings
                logger.debug(f"Loaded exit settings for {symbol} ({strategy_id}) from config")
                return exit_settings
        except Exception as e:
            logger.warning(f"Error loading exit settings from config for {symbol} ({strategy_id}): {e}, using defaults")
        
        # Fallback to default settings with Ichimoku preset values
        ichimoku_preset = self._get_ichimoku_preset_values(strategy_id)
        settings = ExitSettings(
            ichimoku_conversion_periods=ichimoku_preset['conversion_periods'],
            ichimoku_base_periods=ichimoku_preset['base_periods'],
            ichimoku_lagging_span2_periods=ichimoku_preset['lagging_span2_periods'],
            ichimoku_displacement=ichimoku_preset['displacement']
        )
        self.exit_settings_cache[cache_key] = settings
        return settings
    
    async def _update_position_data(self, position: MonitoredPosition):
        """Update current price, RSI, ATR, and P&L for a position"""
        try:
            # Get current price from broker
            current_price = await self._get_current_price(position.symbol, position.account_id)
            if current_price > 0:
                position.current_price = current_price
                
                # Update highest/lowest price tracking (only if price is valid)
                if position.side == "LONG":
                    position.highest_price = max(position.highest_price, current_price)
                else:  # SHORT
                    position.lowest_price = min(position.lowest_price, current_price)
            # If current_price is 0 (fetch failed), keep existing price and don't update tracking
            
            # Calculate P&L
            await self._calculate_position_pnl(position)
            
            # Calculate RSI from price history
            if position.exit_settings.use_rsi_exit:
                rsi = await self._calculate_rsi(position.symbol, position.exit_settings.rsi_length, position.timeframe)
                if rsi is not None:
                    position.rsi = rsi
            
            # Calculate ATR from price history
            if position.exit_settings.stop_mode == "ATR-Based":
                atr = await self._calculate_atr(position.symbol, position.exit_settings.atr_length, position.timeframe)
                if atr is not None:
                    position.atr = atr
            
            # Calculate Ichimoku Cloud indicators (for cloud-based exit monitoring)
            if position.exit_settings.use_ichimoku_cloud_exit:
                ichimoku_data = await self._calculate_ichimoku(
                    position.symbol,
                    position.exit_settings.ichimoku_conversion_periods,
                    position.exit_settings.ichimoku_base_periods,
                    position.exit_settings.ichimoku_lagging_span2_periods,
                    position.exit_settings.ichimoku_displacement,
                    position.timeframe,
                    position.strategy_id  # Pass strategy_id for timeframe inference
                )
                if ichimoku_data:
                    # Update Ichimoku values
                    position.ichimoku_tenkan = ichimoku_data.get('tenkan')
                    position.ichimoku_kijun = ichimoku_data.get('kijun')
                    position.ichimoku_senkou_a = ichimoku_data.get('senkou_a')
                    position.ichimoku_senkou_b = ichimoku_data.get('senkou_b')
                    position.ichimoku_cloud_top = ichimoku_data.get('cloud_top')
                    position.ichimoku_cloud_bottom = ichimoku_data.get('cloud_bottom')
                    position.ichimoku_cloud_bullish = ichimoku_data.get('cloud_bullish')
                    
                    # Check price position relative to cloud
                    if position.current_price > 0 and position.ichimoku_cloud_top and position.ichimoku_cloud_bottom:
                        position.price_above_cloud = position.current_price > position.ichimoku_cloud_top
                        position.price_below_cloud = position.current_price < position.ichimoku_cloud_bottom
                    
                    # Detect TK cross (opposite direction = exit signal)
                    if position.ichimoku_tenkan and position.ichimoku_kijun:
                        # Check for TK cross down (exit signal for LONG)
                        if position.previous_tenkan and position.previous_kijun:
                            if position.previous_tenkan > position.previous_kijun and position.ichimoku_tenkan <= position.ichimoku_kijun:
                                position.tk_cross_down = True
                                logger.info(f"🔻 TK Cross Down detected for {position.symbol} (exit signal for LONG)")
                            else:
                                position.tk_cross_down = False
                            
                            # Check for TK cross up (exit signal for SHORT)
                            if position.previous_tenkan < position.previous_kijun and position.ichimoku_tenkan >= position.ichimoku_kijun:
                                position.tk_cross_up = True
                                logger.info(f"🔺 TK Cross Up detected for {position.symbol} (exit signal for SHORT)")
                            else:
                                position.tk_cross_up = False
                        
                        # Store current values for next check
                        position.previous_tenkan = position.ichimoku_tenkan
                        position.previous_kijun = position.ichimoku_kijun
            
        except Exception as e:
            logger.error(f"Error updating position data for {position.symbol}: {e}", exc_info=True)
    
    async def _get_current_price(self, symbol: str, account_id: str) -> float:
        """Get current market price for a symbol"""
        try:
            # Get broker instance from order executor
            oe = _get_order_executor()
            broker = oe.account_brokers.get(account_id)
            if not broker:
                logger.warning(f"No broker instance for {account_id}")
                return 0.0
            
            # Get current price
            if hasattr(broker, 'get_current_price'):
                price = broker.get_current_price(symbol)
                return float(price) if price else 0.0
            else:
                logger.warning(f"Broker {type(broker).__name__} does not have get_current_price method")
                return 0.0
                
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}", exc_info=True)
            return 0.0
    
    async def _calculate_position_pnl(self, position: MonitoredPosition):
        """Calculate unrealized P&L for a position"""
        try:
            if position.current_price <= 0 or position.entry_price <= 0:
                return
            
            # Calculate price change percentage (unleveraged)
            if position.side.upper() == "LONG":
                price_change_pct = ((position.current_price - position.entry_price) / position.entry_price) * 100
            else:  # SHORT
                price_change_pct = ((position.entry_price - position.current_price) / position.entry_price) * 100
            
            # Track peak price change (unleveraged) - for 6H breakeven logic
            if price_change_pct > position.peak_price_change_pct:
                position.peak_price_change_pct = price_change_pct
                logger.debug(f"📈 Peak price change updated for {position.symbol} {position.side}: {price_change_pct:+.2f}% (unleveraged)")
            
            # Apply leverage to get P&L percentage
            leverage = position.leverage if position.leverage > 0 else 1
            pnl_pct = price_change_pct * leverage
            
            # Calculate deployed capital (margin)
            notional_value = position.quantity * position.entry_price
            deployed_capital = notional_value / leverage if leverage > 0 else notional_value
            
            # Calculate P&L in USD
            pnl_usd = deployed_capital * (pnl_pct / 100)
            
            position.unrealized_pnl_pct = pnl_pct
            position.unrealized_pnl_usd = pnl_usd
            
            # Track peak profit (maximum profit % reached during trade - leveraged)
            # This helps identify if losing trades were in profit before closing at a loss
            if pnl_pct > position.peak_profit_pct:
                position.peak_profit_pct = pnl_pct
                position.peak_profit_time = datetime.utcnow()
                logger.debug(f"📈 Peak profit updated for {position.symbol} {position.side}: {pnl_pct:+.2f}% (leveraged)")
            
        except Exception as e:
            logger.error(f"Error calculating P&L for {position.symbol}: {e}", exc_info=True)
    
    def _symbol_to_coinbase_api(self, symbol: str) -> str:
        """Convert broker symbol to Coinbase public API format"""
        symbol_upper = symbol.upper()
        # Map broker symbols to Coinbase public API format
        symbol_map = {
            'BIP-CBSE': 'BTC-USD',
            'BTC-PERP': 'BTC-USD',
            'ETP-CBSE': 'ETH-USD',
            'ETH-PERP': 'ETH-USD',
            'SLP-CBSE': 'SOL-USD',
            'SOL-PERP': 'SOL-USD',
            'XPP-CBSE': 'XRP-USD',
            'XRP-PERP': 'XRP-USD',
        }
        return symbol_map.get(symbol_upper, symbol_upper.replace('-PERP', '-USD').replace('-CBSE', '-USD'))
    
    async def _fetch_ohlcv_candles(self, symbol: str, limit: int = 20, granularity: int = 300) -> Optional[List[Dict]]:
        """
        Fetch OHLCV candles from Coinbase public API
        
        Args:
            symbol: Symbol in Coinbase API format (e.g., 'BTC-USD')
            limit: Number of candles to fetch (default: 20)
            granularity: Candle size in seconds (300 = 5 minutes)
        
        Returns:
            List of candles: [time, low, high, open, close, volume]
            Returns None on error
        """
        try:
            # Convert symbol to Coinbase API format
            api_symbol = self._symbol_to_coinbase_api(symbol)
            
            # Calculate time range (need enough candles for indicators)
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(seconds=granularity * (limit + 5))  # Extra buffer
            
            url = f"https://api.exchange.coinbase.com/products/{api_symbol}/candles"
            params = {
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                "granularity": granularity
            }
            headers = {"User-Agent": "Mozilla/5.0"}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Coinbase returns: [time, low, high, open, close, volume]
                        # Sort by time (ascending)
                        candles = sorted(data, key=lambda x: x[0])
                        return candles[-limit:] if len(candles) > limit else candles
                    else:
                        logger.warning(f"Failed to fetch OHLCV for {symbol} ({api_symbol}): HTTP {response.status}")
                        return None
                        
        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching OHLCV for {symbol}")
            return None
        except Exception as e:
            logger.error(f"Error fetching OHLCV for {symbol}: {e}", exc_info=True)
            return None
    
    async def _calculate_rsi(self, symbol: str, length: int = 3, timeframe: Optional[str] = None) -> Optional[float]:
        """
        Calculate RSI (Relative Strength Index) from price history using Wilder's smoothing (RMA)
        Matches TradingView's ta.rsi() function
        
        Args:
            symbol: Trading symbol
            length: RSI period (default: 3)
            timeframe: Strategy timeframe (5m, 1h, etc.) - determines candle granularity
        
        Returns:
            RSI value (0-100) or None on error
        """
        try:
            # Determine granularity based on timeframe
            # Default to 5m (300 seconds) if timeframe not provided
            if timeframe:
                timeframe_lower = timeframe.lower()
                if '1h' in timeframe_lower or '1hour' in timeframe_lower:
                    granularity = 3600  # 1 hour = 3600 seconds
                elif '15m' in timeframe_lower or '15min' in timeframe_lower:
                    granularity = 900  # 15 minutes = 900 seconds
                elif '5m' in timeframe_lower or '5min' in timeframe_lower:
                    granularity = 300  # 5 minutes = 300 seconds
                else:
                    granularity = 300  # Default to 5m
            else:
                granularity = 300  # Default to 5m if timeframe not provided
            
            # Fetch enough candles for RSI calculation
            # Need at least length + 1 candles for RSI calculation
            # Fetch extra for Wilder's smoothing initialization
            candles = await self._fetch_ohlcv_candles(symbol, limit=length + 15, granularity=granularity)
            if not candles or len(candles) < length + 1:
                logger.debug(f"Insufficient candles for RSI calculation: {symbol} (got {len(candles) if candles else 0}, need {length + 1}, timeframe: {timeframe})")
                return None
            
            # Extract close prices (most recent first, reverse to oldest first)
            closes = [float(candle[4]) for candle in reversed(candles)]  # Index 4 = close, reverse for chronological order
            
            # Calculate price changes
            deltas = []
            for i in range(1, len(closes)):
                deltas.append(closes[i] - closes[i - 1])
            
            if len(deltas) < length:
                return None
            
            # Separate gains and losses
            gains = [delta if delta > 0 else 0.0 for delta in deltas]
            losses = [-delta if delta < 0 else 0.0 for delta in deltas]
            
            # Calculate initial average gain and loss (simple average for first period)
            # This matches TradingView's ta.rsi() initialization
            avg_gain = sum(gains[:length]) / length
            avg_loss = sum(losses[:length]) / length
            
            # Apply Wilder's smoothing (RMA) for remaining periods
            # RMA = (Previous RMA × (period - 1) + Current Value) / period
            # This matches TradingView's ta.rsi() calculation exactly
            for i in range(length, len(gains)):
                avg_gain = (avg_gain * (length - 1) + gains[i]) / length
                avg_loss = (avg_loss * (length - 1) + losses[i]) / length
            
            # Calculate RSI
            if avg_loss == 0:
                return 100.0  # All gains, no losses
            
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))
            
            return round(rsi, 2)
            
        except Exception as e:
            logger.error(f"Error calculating RSI for {symbol}: {e}", exc_info=True)
            return None
    
    async def _calculate_atr(self, symbol: str, length: int = 8, timeframe: Optional[str] = None) -> Optional[float]:
        """
        Calculate ATR (Average True Range) from price history
        Uses RMA (Wilder's smoothing) to match TradingView's ta.atr() function
        
        Args:
            symbol: Trading symbol
            length: ATR period (default: 8)
            timeframe: Strategy timeframe (5m, 1h, etc.) - determines candle granularity
        
        Returns:
            ATR value (in price units) or None on error
        """
        try:
            # Determine granularity based on timeframe
            # Default to 5m (300 seconds) if timeframe not provided
            if timeframe:
                timeframe_lower = timeframe.lower()
                if '1h' in timeframe_lower or '1hour' in timeframe_lower:
                    granularity = 3600  # 1 hour = 3600 seconds
                elif '15m' in timeframe_lower or '15min' in timeframe_lower:
                    granularity = 900  # 15 minutes = 900 seconds
                elif '5m' in timeframe_lower or '5min' in timeframe_lower:
                    granularity = 300  # 5 minutes = 300 seconds
                else:
                    granularity = 300  # Default to 5m
            else:
                granularity = 300  # Default to 5m if timeframe not provided
            
            # Fetch enough candles for ATR calculation
            candles = await self._fetch_ohlcv_candles(symbol, limit=length + 5, granularity=granularity)
            if not candles or len(candles) < length + 1:
                logger.debug(f"Insufficient candles for ATR calculation: {symbol} (got {len(candles) if candles else 0}, need {length + 1})")
                return None
            
            # Calculate True Range for each candle
            true_ranges = []
            
            for i in range(1, len(candles)):
                # Current candle: [time, low, high, open, close, volume]
                high = float(candles[i][2])  # Index 2 = high
                low = float(candles[i][1])  # Index 1 = low
                prev_close = float(candles[i - 1][4])  # Previous close
                
                # True Range = max(high - low, abs(high - prev_close), abs(low - prev_close))
                tr1 = high - low
                tr2 = abs(high - prev_close)
                tr3 = abs(low - prev_close)
                true_range = max(tr1, tr2, tr3)
                true_ranges.append(true_range)
            
            if len(true_ranges) < length:
                return None
            
            # Calculate ATR using RMA (Wilder's smoothing) to match TradingView's ta.atr()
            # RMA formula: RMA = (RMA_prev * (period - 1) + current_value) / period
            # For the first value, use SMA
            atr = sum(true_ranges[:length]) / length  # Initial ATR = SMA of first 'length' TR values
            
            # Apply RMA smoothing for remaining values
            for tr in true_ranges[length:]:
                atr = (atr * (length - 1) + tr) / length
            
            return round(atr, 2)
            
        except Exception as e:
            logger.error(f"Error calculating ATR for {symbol}: {e}", exc_info=True)
            return None
    
    async def _check_exit_conditions(self, position: MonitoredPosition) -> Optional[ExitDecision]:
        """Check all exit conditions for a position"""
        try:
            # 1. Check RSI exit
            rsi_exit = await self._check_rsi_exit(position)
            if rsi_exit:
                return rsi_exit
            
            # 2. Check stop loss (only if Agent stop loss monitoring is enabled)
            # TradingView strategy also manages stop loss via webhooks, so Agent stop loss is optional
            if position.exit_settings.use_agent_stop_loss:
                stop_loss_exit = await self._check_stop_loss(position)
                if stop_loss_exit:
                    return stop_loss_exit
            
            # 3. Check take profit
            take_profit_exit = await self._check_take_profit(position)
            if take_profit_exit:
                return take_profit_exit
            
            # 4. Check breakeven protection
            breakeven_exit = await self._check_breakeven(position)
            if breakeven_exit:
                return breakeven_exit
            
            # 4.5. Check 3-hour breakeven exit
            breakeven_3h_exit = await self._check_3hour_breakeven(position)
            if breakeven_3h_exit:
                return breakeven_3h_exit
            
            # 4.6. Check 6-hour persistent breakeven (enhanced)
            breakeven_6h_exit = await self._check_6hour_breakeven(position)
            if breakeven_6h_exit:
                return breakeven_6h_exit
            
            # 5. Check trailing stop
            trailing_exit = await self._check_trailing_stop(position)
            if trailing_exit:
                return trailing_exit
            
            # 6. Check EOD auto-close
            eod_exit = await self._check_eod_auto_close(position)
            if eod_exit:
                return eod_exit
            
            # 7. Check Ichimoku Cloud exit conditions (NEW - Backup to TradingView signals)
            # This ensures positions exit even if TradingView webhook doesn't arrive
            if position.exit_settings.use_ichimoku_cloud_exit:
                ichimoku_exit = await self._check_ichimoku_cloud_exit(position)
                if ichimoku_exit:
                    return ichimoku_exit
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking exit conditions for {position.symbol}: {e}", exc_info=True)
            return None
    
    async def _check_rsi_exit(self, position: MonitoredPosition) -> Optional[ExitDecision]:
        """
        Check RSI exit conditions
        
        CRITICAL FIX: Uses UNLEVERAGED profit percentage for activation check (matches TradingView)
        TradingView uses: short_profit_pct_for_exit = (entry_price - close) / entry_price * 100
        Agent must match this calculation, not use leveraged P&L.
        """
        if not position.exit_settings.use_rsi_exit:
            return None
        
        # Skip RSI exit check if RSI hasn't been calculated yet
        # RSI is None until calculation succeeds (see _update_position_data)
        if position.rsi is None:
            # RSI calculation hasn't succeeded yet, skip exit check
            return None
        
        # Validate current_price and entry_price before using them
        if position.current_price <= 0 or position.entry_price <= 0:
            return None
        
        # CRITICAL FIX: Calculate UNLEVERAGED profit percentage (matches TradingView)
        # TradingView uses: short_profit_pct_for_exit = (strategy.position_avg_price - close) / strategy.position_avg_price * 100
        # This is the raw price change, NOT multiplied by leverage
        if position.side.upper() == "LONG":
            unleveraged_profit_pct = ((position.current_price - position.entry_price) / position.entry_price) * 100
        else:  # SHORT
            unleveraged_profit_pct = ((position.entry_price - position.current_price) / position.entry_price) * 100
        
        # RSI exit only activates after reaching activation profit % (UNLEVERAGED, not leveraged)
        # This matches TradingView: short_profit_pct_for_exit >= rsi_exit_activation_pct
        if unleveraged_profit_pct < position.exit_settings.rsi_exit_activation_pct:
            logger.debug(
                f"⏸️  RSI Exit: {position.symbol} {position.side} unleveraged profit {unleveraged_profit_pct:.2f}% "
                f"< activation {position.exit_settings.rsi_exit_activation_pct:.2f}% (leveraged P&L: {position.unrealized_pnl_pct:+.2f}%)"
            )
            return None
        
        # Check RSI exit for LONG
        if position.side == "LONG" and position.rsi <= position.exit_settings.rsi_exit_long:
            logger.info(
                f"✅ RSI Exit LONG: {position.symbol} | "
                f"RSI {position.rsi:.2f} <= {position.exit_settings.rsi_exit_long} | "
                f"Unleveraged profit: {unleveraged_profit_pct:.2f}% >= activation {position.exit_settings.rsi_exit_activation_pct:.2f}%"
            )
            return ExitDecision(
                position=position,
                reason=ExitReason.RSI_EXIT_LONG,
                exit_price=position.current_price,
                message=f"RSI exit: RSI {position.rsi:.2f} <= {position.exit_settings.rsi_exit_long} (unleveraged profit: {unleveraged_profit_pct:.2f}%)"
            )
        
        # Check RSI exit for SHORT
        if position.side == "SHORT" and position.rsi >= position.exit_settings.rsi_exit_short:
            logger.info(
                f"✅ RSI Exit SHORT: {position.symbol} | "
                f"RSI {position.rsi:.2f} >= {position.exit_settings.rsi_exit_short} | "
                f"Unleveraged profit: {unleveraged_profit_pct:.2f}% >= activation {position.exit_settings.rsi_exit_activation_pct:.2f}%"
            )
            return ExitDecision(
                position=position,
                reason=ExitReason.RSI_EXIT_SHORT,
                exit_price=position.current_price,
                message=f"RSI exit: RSI {position.rsi:.2f} >= {position.exit_settings.rsi_exit_short} (unleveraged profit: {unleveraged_profit_pct:.2f}%)"
            )
        
        return None
    
    async def _check_stop_loss(self, position: MonitoredPosition) -> Optional[ExitDecision]:
        """
        Check stop loss conditions
        
        Priority:
        1. Use TradingView stop loss price (if provided) - source of truth
        2. Calculate using Agent's method (ATR-based or fixed %) - fallback only
        
        This ensures Agent uses TradingView's stop loss level instead of calculating
        a different stop that may conflict with the strategy.
        """
        # Validate current_price before using it
        if position.current_price <= 0 or position.entry_price <= 0:
            return None
        
        # PRIORITY 1: Use TradingView stop loss price (source of truth)
        # This ensures Agent uses the same stop loss level as TradingView strategy
        if position.tradingview_stop_loss and position.tradingview_stop_loss > 0:
            stop_price = position.tradingview_stop_loss
            stop_source = "TradingView"
        else:
            # PRIORITY 2: Calculate stop loss using Agent's method (fallback)
            # Only used if TradingView stop loss not provided
            if position.exit_settings.stop_mode == "ATR-Based":
                # ATR-based stop (requires ATR calculation)
                if position.atr > 0:
                    if position.side == "LONG":
                        stop_price = position.entry_price - (position.atr * position.exit_settings.atr_mult)
                    else:  # SHORT
                        stop_price = position.entry_price + (position.atr * position.exit_settings.atr_mult)
                    
                    # SAFETY: For very short ATR lengths (length=1), ATR can be unstable
                    # Enforce minimum stop distance based on fixed % fallback to prevent overly tight stops
                    # This ensures stop loss is reasonable even if ATR calculation gives very small values
                    min_stop_pct = position.exit_settings.stop_loss_pct
                    if position.side == "LONG":
                        min_stop_price = position.entry_price * (1 - min_stop_pct / 100)
                        # For LONG: stop should not be above min_stop_price (tighter = lower price)
                        # If calculated stop is tighter than min %, use min % stop instead
                        if stop_price > min_stop_price:
                            logger.warning(
                                f"⚠️ ATR stop ({format_price(stop_price, position.symbol)}) tighter than min {min_stop_pct}% ({format_price(min_stop_price, position.symbol)}) "
                                f"for {position.symbol} {position.side}. Using min {min_stop_pct}% stop."
                            )
                            stop_price = min_stop_price
                    else:  # SHORT
                        min_stop_price = position.entry_price * (1 + min_stop_pct / 100)
                        # For SHORT: stop should not be below min_stop_price (tighter = higher price)
                        # If calculated stop is tighter than min %, use min % stop instead
                        if stop_price < min_stop_price:
                            logger.warning(
                                f"⚠️ ATR stop ({format_price(stop_price, position.symbol)}) tighter than min {min_stop_pct}% ({format_price(min_stop_price, position.symbol)}) "
                                f"for {position.symbol} {position.side}. Using min {min_stop_pct}% stop."
                            )
                            stop_price = min_stop_price
                else:
                    # Fallback to fixed % if ATR not available
                    if position.side == "LONG":
                        stop_price = position.entry_price * (1 - position.exit_settings.stop_loss_pct / 100)
                    else:  # SHORT
                        stop_price = position.entry_price * (1 + position.exit_settings.stop_loss_pct / 100)
            else:
                # Fixed % stop
                if position.side == "LONG":
                    stop_price = position.entry_price * (1 - position.exit_settings.stop_loss_pct / 100)
                else:  # SHORT
                    stop_price = position.entry_price * (1 + position.exit_settings.stop_loss_pct / 100)
            stop_source = "Agent calculated"
        
        # Log which stop loss is being used (for debugging)
        if position.tradingview_stop_loss:
            precision = get_price_precision(position.symbol)
            logger.debug(
                f"📌 Stop loss check for {position.symbol} {position.side}: "
                f"Using TradingView stop @ {format_price(stop_price, position.symbol)} "
                f"(entry: {format_price(position.entry_price, position.symbol)}, "
                f"current: {format_price(position.current_price, position.symbol)})"
            )
        
        # Check if stop is hit
        if position.side == "LONG" and position.current_price <= stop_price:
            precision = get_price_precision(position.symbol)
            return ExitDecision(
                position=position,
                reason=ExitReason.STOP_LOSS,
                exit_price=stop_price,
                message=f"Stop loss hit ({stop_source}): {format_price(position.current_price, position.symbol)} <= {format_price(stop_price, position.symbol)}"
            )
        
        if position.side == "SHORT" and position.current_price >= stop_price:
            precision = get_price_precision(position.symbol)
            return ExitDecision(
                position=position,
                reason=ExitReason.STOP_LOSS,
                exit_price=stop_price,
                message=f"Stop loss hit ({stop_source}): {format_price(position.current_price, position.symbol)} >= {format_price(stop_price, position.symbol)}"
            )
        
        return None
    
    async def _check_take_profit(self, position: MonitoredPosition) -> Optional[ExitDecision]:
        """Check take profit conditions"""
        # Validate current_price before using it
        if position.current_price <= 0 or position.entry_price <= 0:
            return None
        
        # Calculate take profit price
        if position.side == "LONG":
            tp_price = position.entry_price * (1 + position.exit_settings.take_profit_pct / 100)
            if position.current_price >= tp_price:
                return ExitDecision(
                    position=position,
                    reason=ExitReason.TAKE_PROFIT,
                    exit_price=tp_price,
                    message=f"Take profit hit: {format_price(position.current_price, position.symbol)} >= {format_price(tp_price, position.symbol)}"
                )
        else:  # SHORT
            tp_price = position.entry_price * (1 - position.exit_settings.take_profit_pct / 100)
            if position.current_price <= tp_price:
                return ExitDecision(
                    position=position,
                    reason=ExitReason.TAKE_PROFIT,
                    exit_price=tp_price,
                    message=f"Take profit hit: {format_price(position.current_price, position.symbol)} <= {format_price(tp_price, position.symbol)}"
                )
        
        return None
    
    async def _check_breakeven(self, position: MonitoredPosition) -> Optional[ExitDecision]:
        """Check breakeven protection"""
        if not position.exit_settings.use_breakeven:
            return None
        
        # Validate current_price before using it
        if position.current_price <= 0 or position.entry_price <= 0:
            return None
        
        # Check if breakeven should be activated
        # IMPORTANT: TradingView checks price change percentage (NOT leveraged P&L)
        # This matches the Pine script: current_profit_short = (entry_price - close) / entry_price * 100
        if not position.breakeven_set:
            # Calculate price change percentage (without leverage) to match TradingView
            if position.side.upper() == "LONG":
                price_change_pct = ((position.current_price - position.entry_price) / position.entry_price) * 100
            else:  # SHORT
                price_change_pct = ((position.entry_price - position.current_price) / position.entry_price) * 100
            
            # Check breakeven trigger using price change (not leveraged P&L) to match TradingView
            if price_change_pct >= position.exit_settings.breakeven_trigger:
                # Activate breakeven
                breakeven_pct = position.exit_settings.breakeven_level
                if position.side == "LONG":
                    position.current_stop = position.entry_price * (1 + breakeven_pct / 100)
                else:  # SHORT
                    position.current_stop = position.entry_price * (1 - breakeven_pct / 100)
                
                position.breakeven_set = True
                position.breakeven_hit_count = 0  # Reset hit count when activated
                logger.info(f"🔒 Breakeven activated for {position.symbol} @ {format_price(position.current_stop, position.symbol)} "
                           f"(entry: {format_price(position.entry_price, position.symbol)}, current: {format_price(position.current_price, position.symbol)}, "
                           f"price_change: {price_change_pct:.2f}% >= trigger: {position.exit_settings.breakeven_trigger}%)")
        
        # Check if breakeven stop is hit (with price confirmation)
        if position.breakeven_set and position.current_stop:
            # Check if price is at stop level
            price_at_stop = False
            if position.side == "LONG" and position.current_price <= position.current_stop:
                price_at_stop = True
            elif position.side == "SHORT" and position.current_price >= position.current_stop:
                price_at_stop = True
            
            if price_at_stop:
                # Increment hit count for confirmation
                position.breakeven_hit_count = getattr(position, 'breakeven_hit_count', 0) + 1
                
                # Log detailed price comparison (with appropriate precision)
                price_diff = abs(position.current_price - position.current_stop)
                price_diff_pct = (price_diff / position.current_stop * 100) if position.current_stop > 0 else 0.0
                precision = get_price_precision(position.symbol)
                logger.info(f"🔍 Breakeven check: {position.symbol} {position.side} | "
                           f"Current: {format_price(position.current_price, position.symbol)} | "
                           f"Stop: {format_price(position.current_stop, position.symbol)} | "
                           f"Diff: ${price_diff:,.{precision}f} ({price_diff_pct:.3f}%) | "
                           f"Hits: {position.breakeven_hit_count}/2")
                
                # Require 2 consecutive hits (60 seconds) to confirm sustained price movement
                if position.breakeven_hit_count >= 2:
                    logger.info(f"✅ Breakeven stop confirmed after {position.breakeven_hit_count} consecutive hits")
                    return ExitDecision(
                        position=position,
                        reason=ExitReason.BREAKEVEN_STOP,
                        exit_price=position.current_stop,
                        message=f"Breakeven stop hit: {format_price(position.current_price, position.symbol)} {'<=' if position.side == 'LONG' else '>='} {format_price(position.current_stop, position.symbol)} (confirmed after {position.breakeven_hit_count} hits)"
                    )
            else:
                # Price moved away from stop - reset hit count
                if position.breakeven_hit_count > 0:
                    logger.debug(f"🔄 Breakeven hit count reset for {position.symbol} (price moved away: {format_price(position.current_price, position.symbol)})")
                position.breakeven_hit_count = 0
        
        return None
    
    async def _check_3hour_breakeven(self, position: MonitoredPosition) -> Optional[ExitDecision]:
        """
        Check 3-hour breakeven exit
        
        After 3 hours, if profit reaches the minimum profit threshold (0.3%),
        exit if price change (UNLEVERAGED) is within range [0.02%, 0.05%].
        This locks in small profits for trades that have moved into profit but haven't
        moved significantly further.
        
        IMPORTANT: Uses UNLEVERAGED price change percentage to match TradingView behavior.
        
        Logic:
        1. Check if position is older than 3 hours
        2. Check if profit has reached minimum profit threshold (0.3% unleveraged)
        3. If yes, exit if price change is within range [0.02%, 0.05%] (unleveraged)
        """
        if not position.exit_settings.use_3hour_breakeven:
            return None
        
        # Validate current_price and entry_price
        if position.current_price <= 0 or position.entry_price <= 0:
            return None
        
        # Calculate holding time in hours
        holding_time = datetime.utcnow() - position.entry_time
        holding_hours = holding_time.total_seconds() / 3600.0
        
        # Only check after 3 hours
        if holding_hours < 3.0:
            return None
        
        # CRITICAL FIX: Calculate UNLEVERAGED price change percentage (matches TradingView)
        if position.side.upper() == "LONG":
            price_change_pct = ((position.current_price - position.entry_price) / position.entry_price) * 100
        else:  # SHORT
            price_change_pct = ((position.entry_price - position.current_price) / position.entry_price) * 100
        
        # Check if profit has reached minimum profit threshold (unleveraged)
        min_profit = position.exit_settings.breakeven_3h_min_profit
        if price_change_pct < min_profit:
            # Profit hasn't reached threshold yet, don't exit
            return None
        
        # Profit has reached threshold, check if price change is within exit range (unleveraged)
        min_pct = position.exit_settings.breakeven_3h_min_pct
        max_pct = position.exit_settings.breakeven_3h_max_pct
        
        # Only exit if price change (unleveraged) is within the range [min_pct, max_pct]
        # If price change is above max_pct, let it run (preserve winners)
        # If price change is below min_pct, do NOT exit (let other exits handle it)
        if min_pct <= price_change_pct <= max_pct:
            return ExitDecision(
                position=position,
                reason=ExitReason.BREAKEVEN_3H,
                exit_price=position.current_price,
                message=f"3-hour breakeven exit: Price change {price_change_pct:+.2f}% (unleveraged) is within range [{min_pct:+.2f}%, {max_pct:+.2f}%] after {holding_hours:.1f}h (triggered at {min_profit:+.2f}% profit)"
            )
        
        # If price change is outside the range, do not exit via 3-hour breakeven
        return None
    
    async def _check_6hour_breakeven(self, position: MonitoredPosition) -> Optional[ExitDecision]:
        """
        Check 6-hour persistent breakeven exit
        
        After 6 hours, if price change (UNLEVERAGED) is between min and max %, exit immediately.
        This preserves winners above max % while locking in breakeven for trades
        that haven't moved significantly.
        
        IMPORTANT: Uses UNLEVERAGED price change percentage to match TradingView behavior.
        TradingView calculates: current_profit_short = (entry_price - close) / entry_price * 100
        Agent must match this calculation (not leveraged P&L).
        
        Only exits when price change is in the range [min_pct, max_pct] (unleveraged).
        Does NOT exit if price change is negative or below min_pct.
        """
        if not position.exit_settings.use_6hour_breakeven:
            logger.debug(f"6H breakeven disabled for {position.symbol} {position.side}")
            return None
        
        # Validate current_price and entry_price
        if position.current_price <= 0 or position.entry_price <= 0:
            logger.warning(
                f"6H breakeven check skipped for {position.symbol} {position.side}: "
                f"current_price={position.current_price}, entry_price={position.entry_price}"
            )
            return None
        
        # Calculate holding time in hours
        holding_time = datetime.utcnow() - position.entry_time
        holding_hours = holding_time.total_seconds() / 3600.0
        
        # Only check after 6 hours
        if holding_hours < 6.0:
            logger.info(
                f"⏸️ 6H Breakeven: {position.symbol} {position.side} holding time {holding_hours:.2f}h < 6.0h (will check when 6+ hours)"
            )
            return None
        
        # CRITICAL FIX: Calculate UNLEVERAGED price change percentage (matches TradingView)
        # TradingView uses: current_profit_short = (entry_price - close) / entry_price * 100
        # This is the raw price change, NOT multiplied by leverage
        if position.side.upper() == "LONG":
            price_change_pct = ((position.current_price - position.entry_price) / position.entry_price) * 100
        else:  # SHORT
            price_change_pct = ((position.entry_price - position.current_price) / position.entry_price) * 100
        
        # Check if price change (unleveraged) is within the breakeven range
        min_pct = position.exit_settings.breakeven_6h_min_pct
        max_pct = position.exit_settings.breakeven_6h_max_pct
        
        # Track if price is currently in range
        currently_in_range = min_pct <= price_change_pct <= max_pct
        
        # CRITICAL FIX: Detect when price ENTERS the range (not just when it's currently in range)
        # This catches cases where price moved through the range during a bar but closed outside
        # Check if price was outside range last check and is now in range
        was_outside_range = True  # Default to True for first check
        if hasattr(position, 'last_price_change_pct') and position.last_price_change_pct != 0.0:
            was_outside_range = (position.last_price_change_pct < min_pct or position.last_price_change_pct > max_pct)
        
        entered_range = was_outside_range and currently_in_range
        
        # ENHANCED: Detect if price CROSSED THROUGH the range between checks
        # This catches cases where price was below min at last check and is now above max (or vice versa)
        # If price crossed from one side of range to the other, it must have passed through the range
        crossed_through_range = False
        if hasattr(position, 'last_price_change_pct') and position.last_price_change_pct != 0.0:
            last_pct = position.last_price_change_pct
            # Case 1: Price was below min and is now above max (crossed through range upward)
            if last_pct < min_pct and price_change_pct > max_pct:
                crossed_through_range = True
            # Case 2: Price was above max and is now below min (crossed through range downward)
            elif last_pct > max_pct and price_change_pct < min_pct:
                crossed_through_range = True
            # Case 3: Price was below min and is now in range (entered from below)
            elif last_pct < min_pct and currently_in_range:
                crossed_through_range = True
            # Case 4: Price was above max and is now in range (entered from above)
            elif last_pct > max_pct and currently_in_range:
                crossed_through_range = True
        
        # Mark that price has been in range at some point
        if currently_in_range:
            position.was_in_6h_range = True
        
        # Enhanced logging for 6H breakeven check
        logger.info(
            f"🔍 6H Breakeven Check: {position.symbol} {position.side} | "
            f"Holding: {holding_hours:.2f}h | "
            f"Entry: {format_price(position.entry_price, position.symbol)} | "
            f"Current: {format_price(position.current_price, position.symbol)} | "
            f"Price Change (unleveraged): {price_change_pct:+.2f}% | "
            f"Last Price Change: {position.last_price_change_pct:+.2f}% | "
            f"Leveraged P&L: {position.unrealized_pnl_pct:+.2f}% | "
            f"Range: [{min_pct:+.2f}%, {max_pct:+.2f}%] (unleveraged) | "
            f"In Range: {currently_in_range} | Entered Range: {entered_range} | Crossed Through: {crossed_through_range}"
        )
        
        # PRIMARY EXIT: Exit if price is currently in range [min_pct, max_pct]
        # This is the main exit condition - close when price is between min and max
        if currently_in_range:
            logger.info(
                f"✅ 6H Breakeven TRIGGERED: {position.symbol} {position.side} | "
                f"Price change {price_change_pct:+.2f}% (unleveraged) is within range [{min_pct:+.2f}%, {max_pct:+.2f}%]"
            )
            # Update last price change before returning
            position.last_price_change_pct = price_change_pct
            return ExitDecision(
                position=position,
                reason=ExitReason.BREAKEVEN_6H,
                exit_price=position.current_price,
                message=f"6-hour breakeven exit: Price change {price_change_pct:+.2f}% (unleveraged) is within range [{min_pct:+.2f}%, {max_pct:+.2f}%] after {holding_hours:.1f}h"
            )
        
        # CRITICAL FIX: Exit if price ENTERED the range (was outside, now inside)
        # This catches cases where price moved through the range during a bar but closed outside
        # Example: Bar opened below min, moved through range, closed above max - this catches it
        if entered_range:
            logger.info(
                f"✅ 6H Breakeven TRIGGERED (Entered Range): {position.symbol} {position.side} | "
                f"Price ENTERED range: was {position.last_price_change_pct:+.2f}% (outside), now {price_change_pct:+.2f}% (in range) - locking profit"
            )
            position.was_in_6h_range = True
            position.last_price_change_pct = price_change_pct
            return ExitDecision(
                position=position,
                reason=ExitReason.BREAKEVEN_6H,
                exit_price=position.current_price,
                message=f"6-hour breakeven exit: Price entered range [{min_pct:+.2f}%, {max_pct:+.2f}%] at {price_change_pct:+.2f}% after {holding_hours:.1f}h"
            )
        
        # ENHANCED: Exit if price CROSSED THROUGH the range between checks
        # This catches cases where price was below min at last check and is now above max (or vice versa)
        # If price crossed from one side to the other, it must have passed through the range
        # Example: Last check: 0.2% (below min 0.3%), Current check: 0.6% (above max 0.5%) → Price passed through [0.3%, 0.5%]
        if crossed_through_range:
            logger.info(
                f"✅ 6H Breakeven TRIGGERED (Crossed Through Range): {position.symbol} {position.side} | "
                f"Price CROSSED THROUGH range: was {position.last_price_change_pct:+.2f}% (outside), now {price_change_pct:+.2f}% (outside), "
                f"but must have passed through [{min_pct:+.2f}%, {max_pct:+.2f}%] - locking profit"
            )
            position.was_in_6h_range = True
            position.last_price_change_pct = price_change_pct
            # Use midpoint of range as exit price for fairness
            midpoint_pct = (min_pct + max_pct) / 2
            if position.side.upper() == "LONG":
                exit_price = position.entry_price * (1 + midpoint_pct / 100)
            else:  # SHORT
                exit_price = position.entry_price * (1 - midpoint_pct / 100)
            return ExitDecision(
                position=position,
                reason=ExitReason.BREAKEVEN_6H,
                exit_price=exit_price,
                message=f"6-hour breakeven exit: Price crossed through range [{min_pct:+.2f}%, {max_pct:+.2f}%] (was {position.last_price_change_pct:+.2f}%, now {price_change_pct:+.2f}%) after {holding_hours:.1f}h"
            )
        
        # NEW LOGIC: If price is above max, set stop loss to max level (lock in max profit)
        # Then exit if price hits that stop level
        if price_change_pct > max_pct:
            # Calculate stop price at max level
            if position.side.upper() == "LONG":
                max_stop_price = position.entry_price * (1 + max_pct / 100)
                # For LONG: exit if price drops to or below max stop
                stop_hit = position.current_price <= max_stop_price
            else:  # SHORT
                max_stop_price = position.entry_price * (1 - max_pct / 100)
                # For SHORT: exit if price rises to or above max stop
                stop_hit = position.current_price >= max_stop_price
            
            # Set or update the stop loss to max level
            if not position.breakeven_6h_stop_set:
                position.breakeven_6h_stop_price = max_stop_price
                position.breakeven_6h_stop_set = True
                logger.info(
                    f"🔒 6H Breakeven Stop Set: {position.symbol} {position.side} | "
                    f"Price {price_change_pct:+.2f}% above max {max_pct:+.2f}%, "
                    f"stop loss moved to max level: {format_price(max_stop_price, position.symbol)}"
                )
            else:
                # Update stop if it's better (higher for LONG, lower for SHORT)
                if position.side.upper() == "LONG":
                    if max_stop_price > position.breakeven_6h_stop_price:
                        position.breakeven_6h_stop_price = max_stop_price
                        logger.info(
                            f"🔒 6H Breakeven Stop Updated: {position.symbol} {position.side} | "
                            f"Stop raised to: {format_price(max_stop_price, position.symbol)}"
                        )
                else:  # SHORT
                    if max_stop_price < position.breakeven_6h_stop_price:
                        position.breakeven_6h_stop_price = max_stop_price
                        logger.info(
                            f"🔒 6H Breakeven Stop Updated: {position.symbol} {position.side} | "
                            f"Stop lowered to: {format_price(max_stop_price, position.symbol)}"
                        )
            
            # Exit if price has hit the max level stop
            if stop_hit:
                logger.info(
                    f"✅ 6H Breakeven TRIGGERED (Stop Hit): {position.symbol} {position.side} | "
                    f"Price {price_change_pct:+.2f}% hit max level stop at {format_price(max_stop_price, position.symbol)} - locking profit"
                )
                position.last_price_change_pct = price_change_pct
                return ExitDecision(
                    position=position,
                    reason=ExitReason.BREAKEVEN_6H,
                    exit_price=position.current_price,
                    message=f"6-hour breakeven exit: Price hit max level stop at {format_price(max_stop_price, position.symbol)} (was {price_change_pct:+.2f}% above max) after {holding_hours:.1f}h"
                )
            else:
                # Price is above max but hasn't hit stop yet - preserve winner
                logger.debug(
                    f"✅ 6H Breakeven: {position.symbol} {position.side} price change {price_change_pct:+.2f}% "
                    f"above maximum {max_pct:+.2f}% (preserving winner - stop at {format_price(position.breakeven_6h_stop_price, position.symbol)})"
                )
        
        # ENHANCED: If price was in range at any point, exit if price returns to or near range
        # This handles cases where price was in range but moved out, then returns
        # Example: Price was 0.4% (in range), moved to 0.6% (above max), then falls to 0.55% (near range)
        # We should exit to preserve profit since price was eligible before
        if position.was_in_6h_range:
            # Price was in range at some point - check if it's returning to range
            # Exit if price is within 2x max level (was in range, preserve profit)
            if price_change_pct >= min_pct and price_change_pct <= (max_pct * 2):
                logger.info(
                    f"✅ 6H Breakeven TRIGGERED (Was In Range, Returning): {position.symbol} {position.side} | "
                    f"Price was in range [{min_pct:+.2f}%, {max_pct:+.2f}%] before, now {price_change_pct:+.2f}% (returning to range) - locking profit"
                )
                position.last_price_change_pct = price_change_pct
                return ExitDecision(
                    position=position,
                    reason=ExitReason.BREAKEVEN_6H,
                    exit_price=position.current_price,
                    message=f"6-hour breakeven exit: Price was in range before, now returning at {price_change_pct:+.2f}% after {holding_hours:.1f}h"
                )
        
        # Update last price change for next check (track price movement)
        position.last_price_change_pct = price_change_pct
        
        # Log why exit didn't trigger
        if price_change_pct < min_pct:
            logger.debug(
                f"⏸️  6H Breakeven: {position.symbol} {position.side} price change {price_change_pct:+.2f}% "
                f"below minimum {min_pct:+.2f}% (let other exits handle)"
            )
        
        # If price change is outside the range, do not exit via 6-hour breakeven
        return None
    
    async def _check_trailing_stop(self, position: MonitoredPosition) -> Optional[ExitDecision]:
        """
        Check trailing stop conditions
        
        CRITICAL FIX: Uses UNLEVERAGED profit percentage for activation check (matches TradingView)
        TradingView uses: current_profit_long = (close - entry_price) / entry_price * 100
        Agent must match this calculation, not use leveraged P&L.
        """
        if not position.exit_settings.use_trailing:
            return None
        
        # Validate current_price and entry_price before using them
        if position.current_price <= 0 or position.entry_price <= 0:
            return None
        
        # CRITICAL FIX: Calculate UNLEVERAGED profit percentage for activation check (matches TradingView)
        # TradingView uses: current_profit_long = (close - entry_price) / entry_price * 100
        if position.side.upper() == "LONG":
            unleveraged_profit_pct = ((position.current_price - position.entry_price) / position.entry_price) * 100
        else:  # SHORT
            unleveraged_profit_pct = ((position.entry_price - position.current_price) / position.entry_price) * 100
        
        # Check if trailing should be activated using UNLEVERAGED profit (matches TradingView)
        if not position.trailing_active:
            if unleveraged_profit_pct >= position.exit_settings.trailing_trigger:
                position.trailing_active = True
                logger.info(
                    f"📈 Trailing stop activated for {position.symbol} {position.side} | "
                    f"Unleveraged profit: {unleveraged_profit_pct:.2f}% >= trigger {position.exit_settings.trailing_trigger:.2f}% "
                    f"(leveraged P&L: {position.unrealized_pnl_pct:+.2f}%)"
                )
        
        # Update trailing stop if active
        if position.trailing_active:
            trailing_dist_pct = position.exit_settings.trailing_dist
            
            if position.side == "LONG":
                # Validate highest_price
                if position.highest_price <= 0:
                    return None
                    
                new_stop = position.highest_price * (1 - trailing_dist_pct / 100)
                if position.current_stop is None or new_stop > position.current_stop:
                    position.current_stop = new_stop
                
                # Check if trailing stop is hit
                if position.current_stop and position.current_price <= position.current_stop:
                    return ExitDecision(
                        position=position,
                        reason=ExitReason.TRAILING_STOP,
                        exit_price=position.current_stop,
                        message=f"Trailing stop hit: {format_price(position.current_price, position.symbol)} <= {format_price(position.current_stop, position.symbol)}"
                    )
            else:  # SHORT
                # Validate lowest_price
                if position.lowest_price == float('inf') or position.lowest_price <= 0:
                    return None
                    
                new_stop = position.lowest_price * (1 + trailing_dist_pct / 100)
                if position.current_stop is None or new_stop < position.current_stop:
                    position.current_stop = new_stop
                
                # Check if trailing stop is hit
                if position.current_stop and position.current_price >= position.current_stop:
                    return ExitDecision(
                        position=position,
                        reason=ExitReason.TRAILING_STOP,
                        exit_price=position.current_stop,
                        message=f"Trailing stop hit: {format_price(position.current_price, position.symbol)} >= {format_price(position.current_stop, position.symbol)}"
                    )
        
        return None
    
    async def _check_eod_auto_close(self, position: MonitoredPosition) -> Optional[ExitDecision]:
        """Check EOD auto-close conditions"""
        if not position.exit_settings.auto_close_eod:
            return None
        
        # Parse EOD close time (ET timezone)
        try:
            from zoneinfo import ZoneInfo
            et_tz = ZoneInfo('America/New_York')
            now_et = datetime.now(et_tz)
            
            # Parse close time (format: "HH:MM")
            close_hour, close_minute = map(int, position.exit_settings.eod_close_time.split(':'))
            
            # Check if current time matches EOD close time
            if now_et.hour == close_hour and now_et.minute == close_minute:
                return ExitDecision(
                    position=position,
                    reason=ExitReason.EOD_AUTO_CLOSE,
                    exit_price=position.current_price,
                    message=f"EOD auto-close: {position.exit_settings.eod_close_time} ET"
                )
        except Exception as e:
            logger.error(f"Error checking EOD auto-close: {e}", exc_info=True)
        
        return None
    
    def _get_ichimoku_preset_values(self, strategy_id: str) -> Dict[str, int]:
        """
        Get Ichimoku preset values for a strategy based on v14 PineScript presets
        
        Returns:
            Dict with conversion_periods, base_periods, lagging_span2_periods, displacement
        """
        strategy_lower = strategy_id.lower()
        
        # 1H Strategy Presets (from v14 PineScript)
        if 'ichimoku-coinbase-btc-1h' in strategy_lower:
            return {
                'conversion_periods': 6,
                'base_periods': 8,
                'lagging_span2_periods': 8,
                'displacement': 1
            }
        elif 'ichimoku-coinbase-eth-1h' in strategy_lower:
            return {
                'conversion_periods': 2,
                'base_periods': 5,
                'lagging_span2_periods': 10,
                'displacement': 5
            }
        elif 'ichimoku-coinbase-sol-1h' in strategy_lower:
            return {
                'conversion_periods': 4,
                'base_periods': 9,
                'lagging_span2_periods': 9,
                'displacement': 1
            }
        elif 'ichimoku-coinbase-xrp-1h' in strategy_lower:
            return {
                'conversion_periods': 5,
                'base_periods': 11,
                'lagging_span2_periods': 10,
                'displacement': 3
            }
        # 5M Strategy Presets (for reference, but 1H is priority)
        elif 'ichimoku-coinbase-btc-5m' in strategy_lower:
            return {
                'conversion_periods': 4,
                'base_periods': 12,
                'lagging_span2_periods': 15,
                'displacement': 3
            }
        elif 'ichimoku-coinbase-eth-5m' in strategy_lower:
            return {
                'conversion_periods': 16,
                'base_periods': 12,
                'lagging_span2_periods': 10,
                'displacement': 10
            }
        elif 'ichimoku-coinbase-sol-5m' in strategy_lower:
            return {
                'conversion_periods': 16,
                'base_periods': 5,
                'lagging_span2_periods': 3,
                'displacement': 1
            }
        elif 'ichimoku-coinbase-xrp-5m' in strategy_lower:
            return {
                'conversion_periods': 13,
                'base_periods': 12,
                'lagging_span2_periods': 21,
                'displacement': 1
            }
        else:
            # Default values (standard Ichimoku)
            return {
                'conversion_periods': 9,
                'base_periods': 26,
                'lagging_span2_periods': 52,
                'displacement': 26
            }
    
    async def _calculate_ichimoku(
        self,
        symbol: str,
        conversion_periods: int,
        base_periods: int,
        lagging_span2_periods: int,
        displacement: int,
        timeframe: Optional[str] = None,
        strategy_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Calculate Ichimoku Cloud indicators from OHLCV data
        
        Args:
            symbol: Trading symbol
            conversion_periods: Tenkan period
            base_periods: Kijun period
            lagging_span2_periods: Senkou B period
            displacement: Displacement for Senkou spans
            timeframe: Strategy timeframe (5m, 1h, etc.)
        
        Returns:
            Dict with tenkan, kijun, senkou_a, senkou_b, cloud_top, cloud_bottom, cloud_bullish
            Returns None on error
        """
        try:
            # Determine granularity based on timeframe
            # CRITICAL: Use correct timeframe for Ichimoku calculation
            # 1H strategies need 1-hour candles, 5M strategies need 5-minute candles
            if timeframe:
                timeframe_lower = timeframe.lower()
                if '1h' in timeframe_lower or '1hour' in timeframe_lower or '1 h' in timeframe_lower:
                    granularity = 3600  # 1 hour = 3600 seconds
                elif '15m' in timeframe_lower or '15min' in timeframe_lower or '15 m' in timeframe_lower:
                    granularity = 900  # 15 minutes = 900 seconds
                elif '5m' in timeframe_lower or '5min' in timeframe_lower or '5 m' in timeframe_lower:
                    granularity = 300  # 5 minutes = 300 seconds
                else:
                    # Try to infer from strategy_id if timeframe not clear
                    if strategy_id and '1h' in strategy_id.lower():
                        granularity = 3600  # 1H strategy
                    else:
                        granularity = 300  # Default to 5m
            else:
                # Infer from strategy_id if timeframe not provided
                if strategy_id and '1h' in strategy_id.lower():
                    granularity = 3600  # 1H strategy - use 1-hour candles
                else:
                    granularity = 300  # Default to 5m
            
            # Need enough candles for all indicators (use max period + displacement)
            max_period = max(conversion_periods, base_periods, lagging_span2_periods)
            limit = max_period + displacement + 5  # Extra buffer
            
            # Fetch OHLCV candles
            candles = await self._fetch_ohlcv_candles(symbol, limit=limit, granularity=granularity)
            if not candles or len(candles) < max_period:
                logger.debug(f"Insufficient candles for Ichimoku calculation: {symbol} (got {len(candles) if candles else 0}, need {max_period})")
                return None
            
            # Extract high, low, close arrays (candles are [time, low, high, open, close, volume])
            highs = [float(c[2]) for c in candles]  # Index 2 = high
            lows = [float(c[1]) for c in candles]   # Index 1 = low
            closes = [float(c[4]) for c in candles]  # Index 4 = close
            
            # Calculate Tenkan (Conversion Line): (highest high + lowest low) / 2 over conversion_periods
            tenkan_high = max(highs[-conversion_periods:])
            tenkan_low = min(lows[-conversion_periods:])
            tenkan = (tenkan_high + tenkan_low) / 2
            
            # Calculate Kijun (Base Line): (highest high + lowest low) / 2 over base_periods
            kijun_high = max(highs[-base_periods:])
            kijun_low = min(lows[-base_periods:])
            kijun = (kijun_high + kijun_low) / 2
            
            # Calculate Senkou Span A (Leading Span A): (Tenkan + Kijun) / 2
            # Note: In Ichimoku, Senkou A is shifted forward by displacement, but for current cloud we use current values
            senkou_a = (tenkan + kijun) / 2
            
            # Calculate Senkou Span B (Leading Span B): (highest high + lowest low) / 2 over lagging_span2_periods
            senkou_b_high = max(highs[-lagging_span2_periods:])
            senkou_b_low = min(lows[-lagging_span2_periods:])
            senkou_b = (senkou_b_high + senkou_b_low) / 2
            
            # Cloud boundaries (current cloud - not displaced)
            cloud_top = max(senkou_a, senkou_b)
            cloud_bottom = min(senkou_a, senkou_b)
            cloud_bullish = senkou_a > senkou_b
            
            return {
                'tenkan': tenkan,
                'kijun': kijun,
                'senkou_a': senkou_a,
                'senkou_b': senkou_b,
                'cloud_top': cloud_top,
                'cloud_bottom': cloud_bottom,
                'cloud_bullish': cloud_bullish
            }
            
        except Exception as e:
            logger.error(f"Error calculating Ichimoku for {symbol}: {e}", exc_info=True)
            return None
    
    async def _check_ichimoku_cloud_exit(self, position: MonitoredPosition) -> Optional[ExitDecision]:
        """
        Check Ichimoku Cloud exit conditions
        
        Exit conditions (matching v14 PineScript logic):
        - LONG: Exit if price crosses below cloud OR cloud turns bearish OR TK cross down
        - SHORT: Exit if price crosses above cloud OR cloud turns bullish OR TK cross up
        
        This acts as a backup to TradingView signals - ensures positions exit even if webhook doesn't arrive
        """
        try:
            # Skip if Ichimoku data not available
            if position.ichimoku_cloud_top is None or position.ichimoku_cloud_bottom is None:
                return None
            
            if position.current_price <= 0:
                return None
            
            # Check exit conditions for LONG positions
            if position.side.upper() == "LONG":
                # Exit if price crosses below cloud
                if position.price_below_cloud:
                    logger.warning(
                        f"☁️ ICHIMOKU CLOUD EXIT (LONG): {position.symbol} | "
                        f"Price {format_price(position.current_price, position.symbol)} below cloud "
                        f"(cloud bottom: {format_price(position.ichimoku_cloud_bottom, position.symbol)})"
                    )
                    return ExitDecision(
                        position=position,
                        reason=ExitReason.ICHIMOKU_CLOUD_EXIT,
                        exit_price=position.current_price,
                        message=f"Ichimoku Cloud Exit: Price below cloud (cloud bottom: {format_price(position.ichimoku_cloud_bottom, position.symbol)})"
                    )
                
                # Exit if cloud turns bearish (senkouA < senkouB)
                if position.ichimoku_cloud_bullish is False:
                    logger.warning(
                        f"☁️ ICHIMOKU CLOUD EXIT (LONG): {position.symbol} | "
                        f"Cloud turned bearish (Senkou A < Senkou B)"
                    )
                    return ExitDecision(
                        position=position,
                        reason=ExitReason.ICHIMOKU_CLOUD_EXIT,
                        exit_price=position.current_price,
                        message="Ichimoku Cloud Exit: Cloud turned bearish (Senkou A < Senkou B)"
                    )
                
                # Exit if TK cross down (tenkan crosses below kijun)
                if position.tk_cross_down:
                    logger.warning(
                        f"☁️ ICHIMOKU TK CROSS EXIT (LONG): {position.symbol} | "
                        f"Tenkan crossed below Kijun (bearish signal)"
                    )
                    return ExitDecision(
                        position=position,
                        reason=ExitReason.ICHIMOKU_TK_CROSS_EXIT,
                        exit_price=position.current_price,
                        message="Ichimoku TK Cross Exit: Tenkan crossed below Kijun (bearish signal)"
                    )
            
            # Check exit conditions for SHORT positions
            elif position.side.upper() == "SHORT":
                # Exit if price crosses above cloud
                if position.price_above_cloud:
                    logger.warning(
                        f"☁️ ICHIMOKU CLOUD EXIT (SHORT): {position.symbol} | "
                        f"Price {format_price(position.current_price, position.symbol)} above cloud "
                        f"(cloud top: {format_price(position.ichimoku_cloud_top, position.symbol)})"
                    )
                    return ExitDecision(
                        position=position,
                        reason=ExitReason.ICHIMOKU_CLOUD_EXIT,
                        exit_price=position.current_price,
                        message=f"Ichimoku Cloud Exit: Price above cloud (cloud top: {format_price(position.ichimoku_cloud_top, position.symbol)})"
                    )
                
                # Exit if cloud turns bullish (senkouA > senkouB)
                if position.ichimoku_cloud_bullish is True:
                    logger.warning(
                        f"☁️ ICHIMOKU CLOUD EXIT (SHORT): {position.symbol} | "
                        f"Cloud turned bullish (Senkou A > Senkou B)"
                    )
                    return ExitDecision(
                        position=position,
                        reason=ExitReason.ICHIMOKU_CLOUD_EXIT,
                        exit_price=position.current_price,
                        message="Ichimoku Cloud Exit: Cloud turned bullish (Senkou A > Senkou B)"
                    )
                
                # Exit if TK cross up (tenkan crosses above kijun)
                if position.tk_cross_up:
                    logger.warning(
                        f"☁️ ICHIMOKU TK CROSS EXIT (SHORT): {position.symbol} | "
                        f"Tenkan crossed above Kijun (bullish signal)"
                    )
                    return ExitDecision(
                        position=position,
                        reason=ExitReason.ICHIMOKU_TK_CROSS_EXIT,
                        exit_price=position.current_price,
                        message="Ichimoku TK Cross Exit: Tenkan crossed above Kijun (bullish signal)"
                    )
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking Ichimoku cloud exit for {position.symbol}: {e}", exc_info=True)
            return None
    
    async def _execute_exit(self, decision: ExitDecision) -> bool:
        """
        Execute an exit for a position
        
        Returns:
            True if exit was successful, False otherwise
        """
        try:
            position = decision.position
            
            # CRITICAL: Check if position is already being exited (prevents duplicate exits)
            from services.partition_manager import is_position_exiting, mark_position_exiting, clear_position_exiting
            
            if is_position_exiting(position.account_id, position.partition_id, position.symbol):
                logger.warning(f"⚠️ AGENT EXIT: Position {position.symbol} in {position.partition_id} is already being exited - skipping duplicate")
                return False  # Not successful (already in progress)
            
            logger.info(f"🚪 AGENT EXIT: {position.symbol} {position.side} | {decision.reason.value} | {decision.message}")
            
            # CRITICAL: Do NOT mark position as exiting here
            # The order executor will mark it when it processes the exit
            # This allows the order executor to close ALL positions with the same symbol
            # across ALL partitions, not just the one that triggered the exit
            
            try:
                # Create exit webhook payload
                from models.webhook_payload import WebhookPayload
                
                # Include peak profit in historical enhancer data
                historical_enhancer_data = {
                    "peak_profit_pct": position.peak_profit_pct,
                    "peak_profit_time": position.peak_profit_time.isoformat() if position.peak_profit_time else None,
                    "exit_reason": decision.reason.value,
                    "source": "Agent Exit"
                }
                
                exit_payload = WebhookPayload(
                    strategy_id=position.strategy_id,
                    side="exit",
                    price=decision.exit_price,
                    symbol=position.symbol,
                    timestamp=datetime.utcnow().isoformat(),
                    historical_enhancer=historical_enhancer_data,
                    source="Agent Exit",
                    exit_reason=decision.reason.value  # Pass exit reason to payload
                )
                
                # Execute exit via order executor
                # The order executor will:
                # 1. Find ALL positions with this symbol across ALL partitions
                # 2. Mark each as exiting before closing
                # 3. Close all positions
                oe = _get_order_executor()
                result = await oe.execute(exit_payload)
            
                if result and result.status == "success":
                    logger.info(f"✅ Agent exit executed: {position.symbol} {position.side} | {decision.reason.value}")
                    # Note: POSITION CLOSED alert will be sent by order_executor with source="Agent Exit" and exit_reason
                    # No need for duplicate alert here
                    return True  # Exit successful
                else:
                    logger.error(f"❌ Agent exit failed: {position.symbol} | Status: {result.status if result else 'None'} | Error: {result.error if result else 'Unknown'}")
                    # Clear exit marker on failure (allows retry)
                    clear_position_exiting(position.account_id, position.partition_id, position.symbol)
                    return False  # Exit failed
            except Exception as exit_error:
                logger.error(f"Error executing agent exit: {exit_error}", exc_info=True)
                # Clear exit marker on error (allows retry)
                clear_position_exiting(position.account_id, position.partition_id, position.symbol)
                return False  # Exit failed
                
        except Exception as e:
            logger.error(f"Error in agent exit handler: {e}", exc_info=True)
            return False  # Exit failed
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status"""
        return {
            "is_running": self.is_running,
            "check_interval_seconds": self.check_interval_seconds,
            "monitored_positions_count": len(self.monitored_positions),
            "last_check_time": self.last_check_time.isoformat() if self.last_check_time else None,
            "monitored_positions": [
                {
                    "symbol": pos.symbol,
                    "side": pos.side,
                    "partition_id": pos.partition_id,
                    "unrealized_pnl_pct": pos.unrealized_pnl_pct,
                    "breakeven_set": pos.breakeven_set,
                    "trailing_active": pos.trailing_active
                }
                for pos in self.monitored_positions.values()
            ]
        }


# Global instance
exit_monitor = ExitMonitor()

