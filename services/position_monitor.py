"""
TradingView Agent - Position Monitor Service
Background monitoring service for position health and alerts
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set

# Import demo starting balance from config
try:
    from config import settings
    DEMO_STARTING_BALANCE = settings.DEMO_STARTING_BALANCE
except Exception:
    # Fallback if config not loaded (shouldn't happen in normal operation)
    DEMO_STARTING_BALANCE = 2000.0
from dataclasses import dataclass

from services.position_reconciler import position_reconciler
from services.realtime_pnl import realtime_pnl
from services.telegram_notifier import telegram_notifier

logger = logging.getLogger(__name__)


@dataclass
class MonitoringConfig:
    """Configuration for position monitoring"""
    check_interval_seconds: int = 60  # Check every minute
    stale_position_hours: float = 24.0  # Alert if position > 24 hours
    large_loss_threshold_pct: float = 10.0  # Alert if loss > 10%
    reconcile_interval_minutes: int = 5  # Reconcile every 5 minutes
    max_alerts_per_position: int = 3  # Max alerts per position per day
    enable_stale_alerts: bool = True
    enable_loss_alerts: bool = True
    enable_reconciliation_alerts: bool = False


@dataclass
class PositionAlert:
    """Position alert tracking"""
    account_id: str
    symbol: str
    partition_id: str
    alert_type: str
    alert_count: int
    last_alert_time: datetime
    first_alert_time: datetime


class PositionMonitor:
    """
    Background position monitoring service
    
    Monitors all open positions for:
    - Stale positions (open too long)
    - Large unrealized losses
    - Position reconciliation issues
    - Portfolio health metrics
    """
    
    def __init__(self, config: Optional[MonitoringConfig] = None):
        self.config = config or MonitoringConfig()
        self.is_running = False
        self.monitor_task: Optional[asyncio.Task] = None
        self.alert_history: Dict[str, PositionAlert] = {}
        self.last_reconcile_time = datetime.utcnow()
        
    async def start_monitoring(self):
        """Start the background monitoring task"""
        if self.is_running:
            logger.warning("Position monitor is already running")
            return
        
        self.is_running = True
        self.monitor_task = asyncio.create_task(self._monitoring_loop())
        logger.info(f"🔍 Position monitor started (check interval: {self.config.check_interval_seconds}s)")
    
    async def stop_monitoring(self):
        """Stop the background monitoring task"""
        if not self.is_running:
            return
        
        self.is_running = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        
        logger.info("🛑 Position monitor stopped")
    
    async def _monitoring_loop(self):
        """Main monitoring loop"""
        logger.info("🔍 Position monitoring loop started")
        
        while self.is_running:
            try:
                await self._check_all_positions()
                
                # Periodic reconciliation
                now = datetime.utcnow()
                if (now - self.last_reconcile_time).total_seconds() >= (self.config.reconcile_interval_minutes * 60):
                    await self._perform_reconciliation()
                    self.last_reconcile_time = now
                
                # Clean up old alert history (daily cleanup)
                await self._cleanup_alert_history()
                
                # Wait for next check
                await asyncio.sleep(self.config.check_interval_seconds)
                
            except asyncio.CancelledError:
                logger.info("Position monitoring cancelled")
                break
            except Exception as e:
                logger.error(f"Error in position monitoring loop: {e}")
                # Continue monitoring despite errors
                await asyncio.sleep(self.config.check_interval_seconds)
    
    async def _check_all_positions(self):
        """Check all positions across all accounts"""
        try:
            # Get all accounts with positions
            results = await position_reconciler.reconcile_all_accounts()
            
            for account_id, result in results.items():
                if result.errors:
                    logger.warning(f"Reconciliation errors for {account_id}: {result.errors}")
                
                await self._check_account_positions(account_id, result.positions)
                
        except Exception as e:
            logger.error(f"Error checking all positions: {e}")
    
    async def _check_account_positions(self, account_id: str, positions):
        """Check positions for a specific account"""
        if not positions:
            return
        
        try:
            # Get real-time P&L for the account
            account_pnl = await realtime_pnl.calculate_account_pnl(account_id)
            
            # Check each position
            for position in positions:
                await self._check_position_health(account_id, position, account_pnl)
                
            # Check portfolio-level metrics
            await self._check_portfolio_health(account_id, account_pnl)
            
        except Exception as e:
            logger.error(f"Error checking positions for {account_id}: {e}")
    
    async def _check_position_health(self, account_id: str, position, account_pnl):
        """Check health of a single position"""
        try:
            # Find P&L data for this position
            position_pnl = None
            for pnl in account_pnl.positions:
                if (pnl.symbol == position.symbol and 
                    pnl.partition_id == position.partition_id):
                    position_pnl = pnl
                    break
            
            if not position_pnl:
                logger.debug(f"No P&L data found for {position.symbol} in {position.partition_id}")
                return
            
            # Check for stale positions
            if (self.config.enable_stale_alerts and 
                position.age_hours > self.config.stale_position_hours):
                await self._handle_stale_position_alert(account_id, position_pnl)
            
            # Check for large losses
            if (self.config.enable_loss_alerts and 
                position_pnl.loss_pct > self.config.large_loss_threshold_pct):
                await self._handle_large_loss_alert(account_id, position_pnl)
                
        except Exception as e:
            logger.error(f"Error checking position health for {position.symbol}: {e}")
    
    async def _check_portfolio_health(self, account_id: str, account_pnl):
        """Check overall portfolio health"""
        try:
            # Check for portfolio-wide issues
            if account_pnl.total_positions > 10:
                logger.info(f"📊 High position count for {account_id}: {account_pnl.total_positions} positions")
            
            # Check for large portfolio losses (based on total account balance)
            # Get total account balance for proper portfolio calculation
            try:
                from services.position_reconciler import position_reconciler
                unified_positions = await position_reconciler.get_unified_positions(account_id)
                
                # Get account balance from partition manager or order executor
                total_account_balance = DEMO_STARTING_BALANCE  # Default fallback from config
                try:
                    from services.order_executor import order_executor
                    if hasattr(order_executor, 'get_demo_account_balance'):
                        demo_balance = order_executor.get_demo_account_balance(account_id)
                        if demo_balance and 'current_balance' in demo_balance:
                            total_account_balance = demo_balance['current_balance']
                    elif hasattr(order_executor, '_get_account_config'):
                        # For live accounts, we'd get from broker API
                        # For now, use partition manager data
                        from services.partition_manager import get_partition_summaries
                        summaries = get_partition_summaries(account_id)
                        if summaries and len(summaries) > 0:
                            real_account = summaries[0].get('real_account', {})
                            total_account_balance = real_account.get('total_balance', DEMO_STARTING_BALANCE)
                except Exception as e:
                    logger.debug(f"Could not get account balance for portfolio calculation: {e}")
                
                # Calculate true portfolio P&L percentage
                true_portfolio_pnl_pct = account_pnl.get_account_portfolio_pnl_pct(total_account_balance)
                
                if true_portfolio_pnl_pct < -20:  # More than 20% true portfolio loss
                    await self._handle_portfolio_loss_alert(account_id, account_pnl, total_account_balance, true_portfolio_pnl_pct)
                    
            except Exception as e:
                logger.error(f"Error calculating true portfolio P&L for {account_id}: {e}")
                # Fallback to old calculation if needed
                if account_pnl.portfolio_pnl_pct < -20:
                    await self._handle_portfolio_loss_alert(account_id, account_pnl)
                
        except Exception as e:
            logger.error(f"Error checking portfolio health for {account_id}: {e}")
    
    async def _handle_stale_position_alert(self, account_id: str, position_pnl):
        """Handle alert for stale position"""
        alert_key = f"{account_id}_{position_pnl.symbol}_{position_pnl.partition_id}_stale"
        
        if not self._should_send_alert(alert_key):
            return
        
        try:
            # Normalize symbol for display
            from services.telegram_notifier import normalize_symbol_for_display
            display_symbol = normalize_symbol_for_display(position_pnl.symbol)
            
            alert_text = f"""🕐 STALE POSITION ALERT

Symbol: {display_symbol}
Partition: {position_pnl.partition_id}
Age: {position_pnl.age_hours:.1f} hours
Current P&L: ${position_pnl.unrealized_pnl_usd:+.2f} ({position_pnl.unrealized_pnl_pct:+.2f}%)

⚠️ Position has been open for {position_pnl.age_hours:.1f} hours"""
            
            await telegram_notifier.send_message(alert_text)
            self._record_alert(alert_key, account_id, position_pnl.symbol, position_pnl.partition_id, "stale")
            
            logger.info(f"📱 Sent stale position alert for {display_symbol} ({position_pnl.age_hours:.1f}h)")
            
        except Exception as e:
            logger.error(f"Failed to send stale position alert: {e}")
    
    async def _handle_large_loss_alert(self, account_id: str, position_pnl):
        """Handle alert for large position loss (now called DRAWDOWN ALERT)"""
        alert_key = f"{account_id}_{position_pnl.symbol}_{position_pnl.partition_id}_loss"
        
        if not self._should_send_alert(alert_key):
            return
        
        try:
            # Normalize symbol for display and format prices
            from services.telegram_notifier import normalize_symbol_for_display, format_price
            display_symbol = normalize_symbol_for_display(position_pnl.symbol)
            
            # Determine side emoji and text
            side_emoji = "🟢" if position_pnl.side.upper() == "LONG" else "🔴"
            side_text = position_pnl.side.title()  # "Long" or "Short"
            
            # Format quantity (remove trailing zeros)
            quantity_str = f"{position_pnl.quantity:.6f}".rstrip('0').rstrip('.')
            
            # Format loss percentage (always show negative for losses)
            loss_pct = abs(position_pnl.unrealized_pnl_pct) if position_pnl.unrealized_pnl_usd < 0 else 0.0
            loss_pct_str = f"(-{loss_pct:.2f}%)"
            loss_usd_str = f"${position_pnl.unrealized_pnl_usd:,.2f}"
            
            alert_text = f"""====================================================================



ℹ️ 📉 DRAWDOWN ALERT

Current Loss:

          {loss_pct_str} {loss_usd_str}

{position_pnl.partition_id}:

  • {side_emoji} {side_text} {display_symbol}

          {quantity_str} @ {format_price(position_pnl.entry_price, position_pnl.symbol)}

          Current Price:

          {format_price(position_pnl.current_price, position_pnl.symbol)}

          Position Value:

          ${position_pnl.deployed_capital:,.2f}

⚠️ Position loss exceeds {self.config.large_loss_threshold_pct}% threshold"""
            
            await telegram_notifier.send_message(alert_text)
            self._record_alert(alert_key, account_id, position_pnl.symbol, position_pnl.partition_id, "loss")
            
            logger.info(f"📱 Sent drawdown alert for {display_symbol} ({loss_pct:.1f}%)")
            
        except Exception as e:
            logger.error(f"Failed to send drawdown alert: {e}")
    
    async def _handle_portfolio_loss_alert(self, account_id: str, account_pnl, total_account_balance: float = None, true_portfolio_pnl_pct: float = None):
        """Handle alert for large portfolio loss"""
        alert_key = f"{account_id}_portfolio_loss"
        
        if not self._should_send_alert(alert_key):
            return
        
        try:
            # Use corrected portfolio calculation if available
            if true_portfolio_pnl_pct is not None and total_account_balance is not None:
                portfolio_pct = true_portfolio_pnl_pct
                balance_info = f"Total Account Balance: ${total_account_balance:,.2f}"
                calculation_note = f"(True Portfolio Impact: ${account_pnl.total_unrealized_pnl_usd:+.2f} / ${total_account_balance:,.2f})"
            else:
                portfolio_pct = account_pnl.portfolio_pnl_pct
                balance_info = f"Deployed Capital: ${account_pnl.total_deployed_capital:,.2f}"
                calculation_note = f"(Position Impact: ${account_pnl.total_unrealized_pnl_usd:+.2f} / ${account_pnl.total_deployed_capital:,.2f})"
            
            alert_text = f"""
🚨 PORTFOLIO LOSS ALERT

Account: {account_id}
Total P&L: ${account_pnl.total_unrealized_pnl_usd:+.2f} ({portfolio_pct:+.2f}%)
{balance_info}
{calculation_note}
Positions: {account_pnl.total_positions} ({account_pnl.win_rate:.0f}% winning)

Largest Loser: ${account_pnl.largest_loser_usd:,.2f}

⚠️ Portfolio loss exceeds 20% threshold
Review positions and consider risk management actions.
"""
            
            await telegram_notifier.send_message(alert_text)
            self._record_alert(alert_key, account_id, "PORTFOLIO", "ALL", "portfolio_loss")
            
            logger.info(f"📱 Sent portfolio loss alert for {account_id} ({portfolio_pct:.1f}%)")
            
        except Exception as e:
            logger.error(f"Failed to send portfolio loss alert: {e}")
    
    async def _perform_reconciliation(self):
        """Perform periodic position reconciliation"""
        try:
            logger.debug("🔄 Performing periodic position reconciliation")
            
            results = await position_reconciler.reconcile_all_accounts()
            
            total_accounts = len(results)
            total_positions = sum(result.total_positions for result in results.values())
            total_errors = sum(len(result.errors) for result in results.values())
            
            if self.config.enable_reconciliation_alerts and total_errors > 0:
                alert_text = f"""
🔄 RECONCILIATION REPORT

Accounts Checked: {total_accounts}
Total Positions: {total_positions}
Errors Found: {total_errors}

⚠️ Some reconciliation errors detected
Check logs for details.
"""
                await telegram_notifier.send_message(alert_text)
            
            logger.debug(f"✅ Reconciliation complete: {total_accounts} accounts, {total_positions} positions, {total_errors} errors")
            
        except Exception as e:
            logger.error(f"Error during periodic reconciliation: {e}")
    
    def _should_send_alert(self, alert_key: str) -> bool:
        """Check if we should send an alert based on rate limiting"""
        now = datetime.utcnow()
        
        if alert_key not in self.alert_history:
            return True
        
        alert = self.alert_history[alert_key]
        
        # Check if we've exceeded max alerts for today
        if alert.alert_count >= self.config.max_alerts_per_position:
            # Reset if it's been more than 24 hours since first alert
            if (now - alert.first_alert_time).total_seconds() >= 86400:  # 24 hours
                del self.alert_history[alert_key]
                return True
            return False
        
        # Check if enough time has passed since last alert (minimum 1 hour)
        if (now - alert.last_alert_time).total_seconds() < 3600:  # 1 hour
            return False
        
        return True
    
    def _record_alert(self, alert_key: str, account_id: str, symbol: str, partition_id: str, alert_type: str):
        """Record that an alert was sent"""
        now = datetime.utcnow()
        
        if alert_key in self.alert_history:
            alert = self.alert_history[alert_key]
            alert.alert_count += 1
            alert.last_alert_time = now
        else:
            self.alert_history[alert_key] = PositionAlert(
                account_id=account_id,
                symbol=symbol,
                partition_id=partition_id,
                alert_type=alert_type,
                alert_count=1,
                last_alert_time=now,
                first_alert_time=now
            )
    
    async def _cleanup_alert_history(self):
        """Clean up old alert history entries"""
        now = datetime.utcnow()
        cutoff_time = now - timedelta(days=1)  # Keep 1 day of history
        
        keys_to_remove = []
        for alert_key, alert in self.alert_history.items():
            if alert.last_alert_time < cutoff_time:
                keys_to_remove.append(alert_key)
        
        for key in keys_to_remove:
            del self.alert_history[key]
        
        if keys_to_remove:
            logger.debug(f"🧹 Cleaned up {len(keys_to_remove)} old alert history entries")
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status"""
        return {
            "is_running": self.is_running,
            "config": {
                "check_interval_seconds": self.config.check_interval_seconds,
                "stale_position_hours": self.config.stale_position_hours,
                "large_loss_threshold_pct": self.config.large_loss_threshold_pct,
                "reconcile_interval_minutes": self.config.reconcile_interval_minutes,
                "max_alerts_per_position": self.config.max_alerts_per_position,
                "enable_stale_alerts": self.config.enable_stale_alerts,
                "enable_loss_alerts": self.config.enable_loss_alerts,
                "enable_reconciliation_alerts": self.config.enable_reconciliation_alerts
            },
            "alert_history_count": len(self.alert_history),
            "last_reconcile_time": self.last_reconcile_time.isoformat(),
            "uptime_seconds": (datetime.utcnow() - self.last_reconcile_time).total_seconds() if self.is_running else 0
        }


# Global instance
position_monitor = PositionMonitor()
