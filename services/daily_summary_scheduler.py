"""
TradingView Agent - Daily Summary Scheduler

Sends end-of-day summary alerts to Telegram at market close (4:00 PM ET).
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from zoneinfo import ZoneInfo

from config import settings
from services import partition_manager
from services.telegram_notifier import telegram_notifier

logger = logging.getLogger(__name__)


class DailySummaryScheduler:
    """
    Schedules and sends the TradingView Agent daily summary alert.
    """

    def __init__(self, order_executor, pnl_tracker):
        self.order_executor = order_executor
        self.pnl_tracker = pnl_tracker

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.target_hour_local = 16  # 4:00 PM local time (configurable)
        self.target_minute_local = 0
        try:
            self.timezone = settings.LOCAL_TIMEZONE_INFO
        except AttributeError:
            self.timezone = ZoneInfo("UTC")
            logger.warning("LOCAL_TIMEZONE_INFO not set; daily summary scheduler defaulting to UTC.")

    async def start(self):
        if self._running:
            logger.warning("Daily summary scheduler already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info("✅ Daily summary scheduler started")

    async def stop(self):
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Daily summary scheduler stopped")

    async def trigger_now(self):
        """Send the summary immediately (useful for manual tests)."""
        await self._send_daily_summary()

    async def _run(self):
        while self._running:
            seconds_until_run = self._seconds_until_next_run()
            logger.debug(f"Daily summary scheduled in {seconds_until_run / 60:.2f} minutes")
            try:
                await asyncio.sleep(seconds_until_run)
                await self._send_daily_summary()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"Daily summary scheduler error: {exc}", exc_info=True)
                await asyncio.sleep(300)  # Wait 5 minutes before retrying

    def _seconds_until_next_run(self) -> float:
        now_local = datetime.now(self.timezone)
        target = now_local.replace(
            hour=self.target_hour_local,
            minute=self.target_minute_local,
            second=0,
            microsecond=0,
        )

        if now_local >= target:
            target += timedelta(days=1)

        seconds = (target - now_local).total_seconds()
        # Safety: ensure we sleep at least 60 seconds to avoid tight loops
        return max(seconds, 60.0)

    async def _send_daily_summary(self):
        try:
            logger.info("📊 EOD report generation started")
            
            # DUPLICATE PREVENTION: Check if EOD was already sent in the last hour
            # This prevents duplicate EOD reports when both internal scheduler and Cloud Scheduler are active
            now = datetime.now(self.timezone)
            last_eod_check = getattr(self, '_last_eod_sent', None)
            
            if last_eod_check and (now - last_eod_check).total_seconds() < 3600:  # 1 hour
                logger.info(f"📊 EOD already sent at {last_eod_check.strftime('%H:%M:%S')} - skipping duplicate")
                return
            
            # CRITICAL: Ensure partitions are initialized before getting account stats
            # This prevents EOD from being skipped after deployments when partitions haven't been initialized yet
            try:
                await self.order_executor.warm_partitions()
                logger.debug("Partitions warmed before EOD report generation")
            except Exception as warm_exc:
                logger.warning(f"Failed to warm partitions before EOD: {warm_exc}")
                # Continue anyway - partitions might already be initialized
            
            account_stats = self.order_executor.get_daily_account_stats()

            # ENHANCED: Also collect historical trade data from Firestore for cross-mode performance
            historical_stats = await self._get_historical_trade_stats()
            
            # Merge historical stats with current account stats
            if historical_stats:
                account_stats = self._merge_historical_stats(account_stats, historical_stats)
                logger.info(f"📊 Merged historical trade data: {len(historical_stats)} historical trades found")

            if not account_stats:
                # Enhanced logging to diagnose why EOD was skipped
                partition_overview = self.order_executor.get_partition_overview()
                demo_accounts = list(self.order_executor.demo_accounts.keys()) if hasattr(self.order_executor, 'demo_accounts') else []
                
                logger.warning(
                    f"⚠️ No account statistics available. "
                    f"Partition overview entries: {len(partition_overview)}, "
                    f"Demo accounts: {len(demo_accounts)}"
                )
                
                # IMPROVEMENT: Create minimal account stats from demo accounts to ensure EOD always sends
                if demo_accounts:
                    logger.info(f"📊 Creating minimal EOD report from {len(demo_accounts)} demo account(s)...")
                    account_stats = {}
                    for account_id in demo_accounts:
                        try:
                            demo_account = self.order_executor.demo_accounts[account_id]
                            # Try to get account label, fallback to account_id
                            try:
                                if hasattr(self.order_executor, '_get_account_label'):
                                    account_label = self.order_executor._get_account_label(account_id)
                                else:
                                    account_label = account_id
                            except Exception:
                                account_label = account_id
                            
                            current_balance = float(demo_account.get('current_balance', 0.0))
                            starting_balance = float(demo_account.get('starting_balance', 0.0))
                            
                            # Ensure we have valid balances
                            if starting_balance <= 0:
                                starting_balance = 2000.0  # Default demo starting balance
                            if current_balance <= 0:
                                current_balance = starting_balance
                            
                            account_stats[account_label] = {
                                'total_trades': 0,
                                'winning_trades': 0,
                                'losing_trades': 0,
                                'realized_pnl': 0.0,
                                'win_rate': 0.0,
                                'current_balance': current_balance,
                                'starting_balance': starting_balance,
                                'daily_pnl': 0.0,
                                'daily_pnl_pct': 0.0
                            }
                        except Exception as acc_exc:
                            logger.warning(f"Failed to create stats for account {account_id}: {acc_exc}")
                            continue
                    
                    if account_stats:
                        logger.info(f"✅ Created minimal account stats for {len(account_stats)} account(s) - EOD will send with zero trades")
                    else:
                        logger.error("❌ Failed to create any account stats from demo accounts")
                        return
                else:
                    logger.error("❌ No demo accounts available - cannot create EOD report")
                    return

            # Get open positions and compute daily stats with error handling
            try:
                open_positions = await self._get_open_positions_snapshot()
            except Exception as pos_exc:
                logger.warning(f"Failed to get open positions snapshot: {pos_exc}")
                open_positions = {}
            
            try:
                daily_totals, account_daily_stats = await self._compute_daily_partition_stats()
            except Exception as stats_exc:
                logger.warning(f"Failed to compute daily partition stats: {stats_exc}")
                # Create minimal daily totals from account_stats
                daily_totals = {
                    'daily_pnl': sum(acc.get('daily_pnl', 0.0) for acc in account_stats.values()),
                    'daily_pnl_pct': sum(acc.get('daily_pnl_pct', 0.0) for acc in account_stats.values()),
                    'win_rate': sum(acc.get('win_rate', 0.0) for acc in account_stats.values()) / len(account_stats) if account_stats else 0.0,
                    'total_trades': sum(acc.get('total_trades', 0) for acc in account_stats.values()),
                    'avg_win': 0.0,
                    'avg_loss': 0.0
                }
                account_daily_stats = {}

            # Send EOD report with error handling
            try:
                sent = await telegram_notifier.send_daily_summary(
                    account_stats=account_stats,
                    open_positions=open_positions,
                    daily_pnl=daily_totals.get('daily_pnl', 0.0),
                    daily_pnl_pct=daily_totals.get('daily_pnl_pct', 0.0),
                    win_rate=daily_totals.get('win_rate', 0.0),
                    total_trades=daily_totals.get('total_trades', 0),
                    avg_win=daily_totals.get('avg_win', 0.0),
                    avg_loss=daily_totals.get('avg_loss', 0.0),
                    account_daily_stats=account_daily_stats
                )
            except Exception as send_exc:
                logger.error(f"Failed to send daily summary via Telegram: {send_exc}", exc_info=True)
                sent = False

            if sent:
                # Mark EOD as sent to prevent duplicates
                self._last_eod_sent = now
                logger.info("📊 Daily summary alert sent successfully")
            else:
                logger.warning("Daily summary could not be sent (Telegram disabled?)")

        except Exception as exc:
            logger.error(f"Failed to send daily summary: {exc}", exc_info=True)

    async def _get_historical_trade_stats(self) -> Dict[str, Any]:
        """
        Get historical trade statistics from Firestore for cross-mode performance tracking.
        This includes trades from both DEMO and LIVE modes.
        """
        try:
            if not self.pnl_tracker or not self.pnl_tracker.use_firestore:
                return {}
            
            # Get trades from the last 30 days to include recent performance
            from datetime import timedelta
            cutoff_date = datetime.now(self.timezone) - timedelta(days=30)
            
            # Query Firestore for recent closed trades
            # Increased limit from 100 to 500 to capture more historical data for active accounts
            trades_query = self.pnl_tracker.trades_collection.where(
                'exit_time', '>=', cutoff_date
            ).where(
                'status', '==', 'closed'
            ).limit(500)  # Increased limit to prevent missing historical data
            
            trades = list(trades_query.stream())
            
            if not trades:
                return {}
            
            # Aggregate trade statistics
            total_trades = len(trades)
            winning_trades = 0
            losing_trades = 0
            total_pnl = 0.0
            total_commission = 0.0
            
            for trade_doc in trades:
                trade_data = trade_doc.to_dict()
                pnl = trade_data.get('realized_pnl_net', 0.0)
                commission = trade_data.get('commission', 0.0)
                
                total_pnl += pnl
                total_commission += commission
                
                if pnl > 0:
                    winning_trades += 1
                else:
                    losing_trades += 1
            
            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0
            
            logger.info(f"📊 Historical stats: {total_trades} trades, {win_rate:.1f}% win rate, ${total_pnl:+.2f} P&L")
            
            return {
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'total_pnl': total_pnl,
                'total_commission': total_commission,
                'win_rate': win_rate
            }
            
        except Exception as e:
            logger.error(f"Error getting historical trade stats: {e}")
            return {}

    def _merge_historical_stats(self, current_stats: Dict, historical_stats: Dict) -> Dict:
        """
        Merge current account stats with historical trade data for comprehensive EOD reporting.
        """
        if not historical_stats or not current_stats:
            return current_stats or {}
        
        # Update each account's statistics with historical data
        for account_label, account_data in current_stats.items():
            if isinstance(account_data, dict):
                # Add historical trade counts
                current_trades = account_data.get('total_trades', 0)
                historical_trades = historical_stats.get('total_trades', 0)
                account_data['total_trades'] = current_trades + historical_trades
                
                # Add historical win/loss counts
                current_wins = account_data.get('winning_trades', 0)
                historical_wins = historical_stats.get('winning_trades', 0)
                account_data['winning_trades'] = current_wins + historical_wins
                
                current_losses = account_data.get('losing_trades', 0)
                historical_losses = historical_stats.get('losing_trades', 0)
                account_data['losing_trades'] = current_losses + historical_losses
                
                # Recalculate win rate
                total_trades = account_data['total_trades']
                if total_trades > 0:
                    account_data['win_rate'] = (account_data['winning_trades'] / total_trades * 100)
                
                # Add historical P&L to balance (if not already included)
                historical_pnl = historical_stats.get('total_pnl', 0.0)
                if historical_pnl != 0:
                    account_data['historical_pnl'] = historical_pnl
                    logger.debug(f"Added historical P&L: ${historical_pnl:+.2f} to {account_label}")
        
        return current_stats

    async def _get_open_positions_snapshot(self) -> Dict[str, List[Dict[str, Any]]]:
        overview = self.order_executor.get_partition_overview()
        grouped: Dict[str, List[Dict[str, Any]]] = {}

        for entry in overview:
            account_id = entry.get('account_id')
            account_label = self.order_executor._get_account_label(account_id) if account_id else account_id or "Account"
            positions_for_account = grouped.setdefault(account_label, [])

            for partition in entry.get('partitions', []):
                partition_id = partition.get('partition_id', 'Partition')
                for pos in partition.get('open_position_details', []):
                    unrealized_usd, unrealized_pct = await self.order_executor.compute_unrealized_position_pnl(
                        account_id=account_id,
                        position=pos
                    )
                    side_text = pos.get('side', 'UNKNOWN')
                    if side_text:
                        side_text = side_text.upper()
                    if partition_id:
                        side_value = f"{side_text} ({partition_id})"
                    else:
                        side_value = side_text
                    positions_for_account.append({
                        'symbol': pos.get('symbol', 'UNKNOWN'),
                        'side': side_value,
                        'unrealized_pnl': unrealized_usd,
                        'unrealized_pnl_pct': unrealized_pct,
                    })

        return grouped

    async def _compute_daily_partition_stats(self) -> Tuple[Dict[str, float], Dict[str, Dict[str, float]]]:
        today_iso = datetime.now(self.timezone).date().isoformat()
        now = datetime.now(self.timezone)
        
        # Calculate week boundaries (Monday to Sunday)
        # Get Monday of current week
        days_since_monday = now.weekday()  # 0 = Monday, 6 = Sunday
        week_start = (now - timedelta(days=days_since_monday)).date()
        week_start_iso = week_start.isoformat()
        
        # Calculate month boundaries (1st to last day of month)
        month_start = now.replace(day=1).date()
        month_start_iso = month_start.isoformat()

        per_account: Dict[str, Dict[str, Any]] = {}

        # Handle case where partition_state might be empty or None
        partition_state = getattr(partition_manager, 'partition_state', {}) or {}
        if not partition_state:
            logger.debug("No partition state available - returning empty stats")
            # Return empty stats that won't break the EOD report
            return {
                'daily_pnl': 0.0,
                'daily_pnl_pct': 0.0,
                'win_rate': 0.0,
                'total_trades': 0,
                'avg_win': 0.0,
                'avg_loss': 0.0
            }, {}

        for account_id, account_state in partition_state.items():
            account_entry = per_account.setdefault(account_id, {
                'daily_pnl': 0.0,
                'weekly_pnl': 0.0,
                'monthly_pnl': 0.0,
                'starting_total': 0.0,
                'total_trades': 0,
                'wins': 0,
                'losses': 0,
                'win_values': [],
                'loss_values': []
            })

            partitions = account_state.get('partitions', {})
            # Risk telemetry approximations
            real_account = account_state.get('real_account', {}) or {}
            total_balance = float(real_account.get('total_balance') or 0.0)
            deployed_sum = 0.0
            contract_qty_sum = 0.0
            # Per-strategy intraday telemetry accumulators
            strategy_cum_pnl: Dict[str, float] = {}
            strategy_min_cum_pnl: Dict[str, Tuple[float, str]] = {}  # (min_value, time)
            strategy_peak_margin: Dict[str, Tuple[float, str]] = {}  # (peak_pct, time)
            # Track margin usage throughout the day from trade history
            strategy_margin_history: Dict[str, List[Tuple[float, str, float]]] = {}  # strategy_id -> [(margin_pct, time, quantity)]
            
            # For DEMO accounts, try to get starting_total from demo account (most accurate)
            # For LIVE accounts or accounts without demo account, use sum of partition starting_balances
            starting_total_from_demo = None
            try:
                # Try to get order_executor instance (may not be available during EOD)
                import sys
                if 'main' in sys.modules:
                    from main import order_executor
                    if hasattr(order_executor, 'demo_accounts') and account_id in order_executor.demo_accounts:
                        demo_account = order_executor.demo_accounts[account_id]
                        demo_starting = demo_account.get('starting_balance', 0.0)
                        if demo_starting > 0:
                            starting_total_from_demo = demo_starting
                            logger.debug(f"EOD: Using demo account starting_balance ${starting_total_from_demo:,.2f} for {account_id}")
            except Exception as e:
                logger.debug(f"EOD: Could not get demo account starting_balance: {e}")
            
            if starting_total_from_demo is not None and starting_total_from_demo > 0:
                account_entry['starting_total'] = starting_total_from_demo
            else:
                # Fallback to sum of partition starting_balances
                partition_starting_total = 0.0
                for partition in partitions.values():
                    partition_starting_total += partition.get('starting_balance', 0.0)
                account_entry['starting_total'] = partition_starting_total
                if partition_starting_total > 0:
                    logger.debug(f"EOD: Using sum of partition starting_balances ${partition_starting_total:,.2f} for {account_id}")
            
            # Process partitions for deployed cash and contract quantities
            for partition in partitions.values():
                # Deployed cash approximates current margin usage basis
                deployed_sum += float(partition.get('deployed_cash') or 0.0)
                # Contract qty sum from open position details if present
                for pos in partition.get('open_position_details', []):
                    try:
                        contract_qty_sum += abs(float(pos.get('quantity') or 0.0))
                    except Exception:
                        continue
                    # Current margin usage per-strategy (for open positions)
                    # Use cash_used from position (actual margin used) if available
                    try:
                        sid = pos.get('strategy_id')
                        if sid and total_balance > 0:
                            # Use cash_used from position if available (most accurate)
                            pos_cash_used = pos.get('cash_used') or 0.0
                            if pos_cash_used > 0:
                                margin_used = float(pos_cash_used)
                            else:
                                # Fallback: Use partition deployed_cash (approximation)
                                margin_used = float(partition.get('deployed_cash') or 0.0)
                            
                            pct = (margin_used / total_balance) * 100.0
                            # Cap at 100% (margin can't exceed account balance)
                            pct = min(pct, 100.0)
                            
                            now_t = datetime.now(self.timezone).strftime("%H:%M")
                            qty = abs(float(pos.get('quantity') or 0.0))
                            # Track current margin
                            if sid not in strategy_margin_history:
                                strategy_margin_history[sid] = []
                            strategy_margin_history[sid].append((pct, now_t, qty))
                    except Exception:
                        pass

                for trade in partition.get('trade_history', []):
                    exit_time = trade.get('exit_time')
                    if not exit_time:
                        continue
                    
                    # Parse exit time to date
                    try:
                        exit_date = datetime.fromisoformat(exit_time.replace('Z', '+00:00')).astimezone(self.timezone).date()
                    except Exception:
                        try:
                            exit_date = datetime.strptime(exit_time[:10], '%Y-%m-%d').date()
                        except Exception as date_parse_error:
                            logger.warning(f"EOD: Failed to parse exit_time '{exit_time}' for trade: {date_parse_error}")
                            continue
                    
                    pnl = trade.get('pnl_usd_net', trade.get('pnl_usd', 0.0))
                    
                    # Today's P&L
                    if exit_date.isoformat() == today_iso:
                        account_entry['daily_pnl'] += pnl
                        account_entry['total_trades'] += 1

                        if pnl > 0:
                            account_entry['wins'] += 1
                            account_entry['win_values'].append(pnl)
                        elif pnl < 0:
                            account_entry['losses'] += 1
                            account_entry['loss_values'].append(pnl)
                    
                    # This Week's P&L (Monday to Sunday)
                    if exit_date >= week_start:
                        account_entry['weekly_pnl'] += pnl
                    
                    # This Month's P&L (1st to last day of month)
                    if exit_date >= month_start:
                        account_entry['monthly_pnl'] += pnl
                    # Per-strategy intraday cumulative pnl and min (intraday DD proxy)
                    sid = trade.get('strategy_id') or trade.get('strategy') or None
                    if sid:
                        cumulative = strategy_cum_pnl.get(sid, 0.0) + pnl
                        strategy_cum_pnl[sid] = cumulative
                        tlabel = "--:--"
                        try:
                            tlabel = datetime.fromisoformat(exit_time).astimezone(self.timezone).strftime("%H:%M")
                        except Exception:
                            pass
                        prev_min = strategy_min_cum_pnl.get(sid)
                        if not prev_min or cumulative < prev_min[0]:
                            strategy_min_cum_pnl[sid] = (cumulative, tlabel)
                        
                        # Track margin usage at entry time (from trade history) - only for today's trades
                        if exit_date.isoformat() == today_iso:
                            entry_time = trade.get('entry_time')
                            
                            # Use cash_used from trade record (actual margin used) - most accurate
                            # Fallback to calculated value if cash_used not available
                            cash_used = trade.get('cash_used') or trade.get('total_cash_used', 0.0)
                            
                            if entry_time and total_balance > 0:
                                try:
                                    # Use actual cash_used from trade record if available (most accurate)
                                    if cash_used > 0:
                                        margin_used = float(cash_used)
                                    else:
                                        # Fallback: Calculate margin using entry price and quantity
                                        # Note: This is less accurate as it doesn't account for actual Coinbase margin rates
                                        entry_price = trade.get('entry_price', 0.0)
                                        quantity = abs(float(trade.get('quantity', 0.0)))
                                        leverage = int(trade.get('leverage', 1))
                                        
                                        if entry_price > 0 and quantity > 0:
                                            # Simplified calculation (notional / leverage)
                                            # This is approximate - actual margin uses Coinbase's margin rates
                                            notional = entry_price * quantity
                                            margin_used = notional / leverage
                                        else:
                                            continue
                                    
                                    # Calculate margin as percentage of total account balance
                                    margin_pct = (margin_used / total_balance) * 100.0
                                    
                                    # Parse entry time
                                    entry_tlabel = "--:--"
                                    try:
                                        entry_tlabel = datetime.fromisoformat(entry_time.replace('Z', '+00:00')).astimezone(self.timezone).strftime("%H:%M")
                                    except Exception:
                                        try:
                                            entry_tlabel = datetime.strptime(entry_time[:16], '%Y-%m-%dT%H:%M').strftime("%H:%M")
                                        except Exception:
                                            pass
                                    
                                    # Update peak margin if this is higher
                                    # Cap at 100% (margin can't exceed 100% of account balance)
                                    margin_pct_capped = min(margin_pct, 100.0)
                                    
                                    prev_peak = strategy_peak_margin.get(sid)
                                    if not prev_peak or margin_pct_capped > prev_peak[0]:
                                        strategy_peak_margin[sid] = (margin_pct_capped, entry_tlabel)
                                        
                                    # Log if we had to cap the margin percentage (indicates calculation issue)
                                    if margin_pct > 100.0:
                                        logger.warning(
                                            f"EOD: Capped margin percentage for {sid} from {margin_pct:.1f}% to 100% "
                                            f"(margin_used=${margin_used:.2f}, total_balance=${total_balance:.2f})"
                                        )
                                except Exception as e:
                                    logger.debug(f"EOD: Error calculating margin for trade: {e}")
                                    pass

        total_pnl = 0.0
        total_trades = 0
        wins = 0
        losses = 0
        win_values: List[float] = []
        loss_values: List[float] = []
        starting_total = 0.0

        for account_id, account_entry in per_account.items():
            account_pnl = account_entry['daily_pnl']
            account_trades = account_entry['total_trades']
            account_wins = account_entry['wins']
            account_losses = account_entry['losses']
            account_starting = account_entry['starting_total']

            # Get current account balance for percentage calculations
            # P&L percentages should be calculated against current account balance, not starting balance
            current_account_balance = None
            try:
                state = partition_manager.partition_state.get(account_id, {})
                real_account = state.get('real_account', {}) or {}
                account_mode = real_account.get('mode', 'DEMO')
                total_balance = float(real_account.get('total_balance') or 0.0)
                
                # For DEMO accounts, try to get the actual current balance from demo_accounts
                if account_mode == 'DEMO':
                    try:
                        import sys
                        if 'main' in sys.modules:
                            from main import order_executor
                            if hasattr(order_executor, 'demo_accounts') and account_id in order_executor.demo_accounts:
                                demo_account = order_executor.demo_accounts[account_id]
                                demo_balance = float(demo_account.get('current_balance', 0.0))
                                demo_starting = float(demo_account.get('starting_balance', 0.0))
                                # Verify demo_balance is valid (should be >= starting_balance)
                                if demo_balance > 0 and demo_starting > 0 and demo_balance >= demo_starting:
                                    current_account_balance = demo_balance
                                elif demo_starting > 0:
                                    # Recalculate from starting + P&L
                                    current_account_balance = demo_starting + account_entry.get('realized_pnl', 0.0)
                                else:
                                    current_account_balance = demo_balance if demo_balance > 0 else total_balance
                            else:
                                current_account_balance = total_balance
                        else:
                            current_account_balance = total_balance
                    except Exception:
                        current_account_balance = total_balance
                else:
                    # LIVE: use broker balance
                    current_account_balance = total_balance
            except Exception:
                # Fallback: use starting balance + realized P&L
                current_account_balance = account_starting + account_entry.get('realized_pnl', 0.0)
            
            # If we still don't have a current balance, fallback to starting + realized P&L
            if current_account_balance is None or current_account_balance <= 0:
                current_account_balance = account_starting + account_entry.get('realized_pnl', 0.0)
            
            # Calculate P&L percentages against CURRENT account balance (not starting balance)
            account_entry['daily_pnl_pct'] = (account_pnl / current_account_balance * 100) if current_account_balance > 0 else 0.0
            account_entry['weekly_pnl_pct'] = (account_entry['weekly_pnl'] / current_account_balance * 100) if current_account_balance > 0 else 0.0
            account_entry['monthly_pnl_pct'] = (account_entry['monthly_pnl'] / current_account_balance * 100) if current_account_balance > 0 else 0.0
            account_entry['win_rate'] = (account_wins / account_trades * 100) if account_trades else 0.0
            account_entry['avg_win'] = (
                sum(account_entry['win_values']) / len(account_entry['win_values'])
                if account_entry['win_values'] else 0.0
            )
            account_entry['avg_loss'] = (
                sum(account_entry['loss_values']) / len(account_entry['loss_values'])
                if account_entry['loss_values'] else 0.0
            )
            # Use real-time telemetry from broker (if available), otherwise fallback to approximations
            try:
                state = partition_manager.partition_state.get(account_id, {})
                real_account = state.get('real_account', {}) or {}
                # CRITICAL: total_balance is the MAIN ACCOUNT WALLET BALANCE from Coinbase
                # This comes from broker.get_account_balance_usd() -> total_wallet_balance
                # All margin calculations use this main wallet balance, not partition balances
                total_balance = float(real_account.get('total_balance') or 0.0)
                
                # Try to get real-time telemetry from broker
                telemetry = state.get('runtime_telemetry', {})
                account_telemetry = telemetry.get('account', {})
                
                if account_telemetry:
                    # Use real broker data
                    margin_usage_pct = account_telemetry.get('current_margin_usage_pct', 0.0)
                    free_margin_pct = account_telemetry.get('current_free_margin_pct', 100.0)
                    worst_margin_pct = account_telemetry.get('worst_margin_usage_pct_today')
                    worst_margin_time = account_telemetry.get('worst_margin_usage_time_today', '--:--')
                    worst_free_pct = account_telemetry.get('worst_free_margin_pct_today')
                    worst_free_time = account_telemetry.get('worst_free_margin_time_today', '--:--')
                    contract_qty_sum = account_telemetry.get('current_total_qty', 0.0)
                    
                    # Fallback: If telemetry quantity is 0 or missing, calculate from partition positions
                    if contract_qty_sum == 0.0:
                        for part in (state.get('partitions', {}) or {}).values():
                            for pos in part.get('open_position_details', []):
                                try:
                                    q = abs(float(pos.get('quantity') or 0.0))
                                    contract_qty_sum += q
                                except Exception:
                                    continue
                    
                    # Get period-specific worst margin usage (intraday, overnight, weekend)
                    worst_intraday = account_telemetry.get('worst_margin_usage_pct_intraday')
                    worst_intraday_time = account_telemetry.get('worst_margin_usage_time_intraday', '--:--')
                    worst_overnight = account_telemetry.get('worst_margin_usage_pct_overnight')
                    worst_overnight_time = account_telemetry.get('worst_margin_usage_time_overnight', '--:--')
                    worst_weekend = account_telemetry.get('worst_margin_usage_pct_weekend')
                    worst_weekend_time = account_telemetry.get('worst_margin_usage_time_weekend', '--:--')
                    
                    account_entry['worst_margin_intraday'] = worst_intraday
                    account_entry['worst_margin_intraday_time'] = worst_intraday_time
                    account_entry['worst_margin_overnight'] = worst_overnight
                    account_entry['worst_margin_overnight_time'] = worst_overnight_time
                    account_entry['worst_margin_weekend'] = worst_weekend
                    account_entry['worst_margin_weekend_time'] = worst_weekend_time
                else:
                    # Fallback to approximation if telemetry not available
                    deployed_sum = 0.0
                    contract_qty_sum = 0.0
                    for part in (state.get('partitions', {}) or {}).values():
                        deployed_sum += float(part.get('deployed_cash') or 0.0)
                        for pos in part.get('open_position_details', []):
                            try:
                                q = abs(float(pos.get('quantity') or 0.0))
                                contract_qty_sum += q
                            except Exception:
                                continue
                    margin_usage_pct = (deployed_sum / total_balance * 100.0) if total_balance > 0 else 0.0
                    free_margin_pct = max(0.0, 100.0 - margin_usage_pct)
                    worst_margin_pct = margin_usage_pct
                    worst_margin_time = datetime.now(self.timezone).strftime("%H:%M")
                    worst_free_pct = free_margin_pct
                    worst_free_time = datetime.now(self.timezone).strftime("%H:%M")
            except Exception:
                margin_usage_pct = 0.0
                free_margin_pct = 100.0
                worst_margin_pct = 0.0
                worst_margin_time = '--:--'
                worst_free_pct = 100.0
                worst_free_time = '--:--'
                contract_qty_sum = 0.0

            account_entry['margin_usage_pct'] = margin_usage_pct
            account_entry['margin_usage_worst_pct'] = worst_margin_pct
            account_entry['margin_usage_worst_time'] = worst_margin_time
            account_entry['free_margin_pct'] = free_margin_pct
            account_entry['free_margin_worst_pct'] = worst_free_pct
            account_entry['free_margin_worst_time'] = worst_free_time
            account_entry['contract_qty_current'] = contract_qty_sum
            
            # Track all-time peak margin usage and lowest free margin (like maxDD)
            # Load from account state
            try:
                state = partition_manager.partition_state.get(account_id, {})
                real_account = state.get('real_account', {})
                state_updated = False
                
                # All-time peak margin usage (highest margin % used)
                all_time_peak_margin_pct = real_account.get('all_time_peak_margin_pct', 0.0)
                all_time_peak_margin_time = real_account.get('all_time_peak_margin_time', '--:--')
                
                # Update if current worst is higher than all-time peak
                if worst_margin_pct is not None and worst_margin_pct > all_time_peak_margin_pct:
                    all_time_peak_margin_pct = worst_margin_pct
                    all_time_peak_margin_time = worst_margin_time
                    real_account['all_time_peak_margin_pct'] = all_time_peak_margin_pct
                    real_account['all_time_peak_margin_time'] = all_time_peak_margin_time
                    state_updated = True
                
                # All-time lowest free margin (tightest margin)
                all_time_lowest_free_margin_pct = real_account.get('all_time_lowest_free_margin_pct', 100.0)
                all_time_lowest_free_margin_time = real_account.get('all_time_lowest_free_margin_time', '--:--')
                
                # Update if current worst is lower than all-time lowest
                if worst_free_pct is not None and worst_free_pct < all_time_lowest_free_margin_pct:
                    all_time_lowest_free_margin_pct = worst_free_pct
                    all_time_lowest_free_margin_time = worst_free_time
                    real_account['all_time_lowest_free_margin_pct'] = all_time_lowest_free_margin_pct
                    real_account['all_time_lowest_free_margin_time'] = all_time_lowest_free_margin_time
                    state_updated = True
                
                # Save updated state only once if any changes were made
                if state_updated:
                    await partition_manager.state_store.save_partition_state(account_id, state)
                
                # Store for EOD report
                account_entry['all_time_peak_margin_pct'] = all_time_peak_margin_pct
                account_entry['all_time_peak_margin_time'] = all_time_peak_margin_time
                account_entry['all_time_lowest_free_margin_pct'] = all_time_lowest_free_margin_pct
                account_entry['all_time_lowest_free_margin_time'] = all_time_lowest_free_margin_time
            except Exception as exc:
                logger.debug(f"Error tracking all-time margin metrics for {account_id}: {exc}")
                # Fallback to today's values
                account_entry['all_time_peak_margin_pct'] = worst_margin_pct if worst_margin_pct is not None else 0.0
                account_entry['all_time_peak_margin_time'] = worst_margin_time
                account_entry['all_time_lowest_free_margin_pct'] = worst_free_pct if worst_free_pct is not None else 100.0
                account_entry['all_time_lowest_free_margin_time'] = worst_free_time
            
            # Calculate Min Liq Cushion (% and $)
            # This is position-specific (distance to liquidation price), not account-level
            # Displayed in account-level sections to show overall account risk
            min_liq_cushion_pct = None
            min_liq_cushion_usd = 0.0
            try:
                state = partition_manager.partition_state.get(account_id, {})
                partitions = state.get('partitions', {})
                
                # Try broker positions first (if available with liquidation_price)
                broker = self.order_executor.account_brokers.get(account_id)
                broker_positions = []
                if broker and hasattr(broker, 'get_all_positions'):
                    try:
                        broker_positions = await asyncio.to_thread(broker.get_all_positions) if hasattr(broker, 'get_all_positions') else []
                    except Exception:
                        broker_positions = []
                
                min_cushion_pct = float('inf')
                min_cushion_usd = float('inf')
                
                # First, try to get liquidation prices from broker positions
                for pos in broker_positions:
                    current_price = float(pos.get('mark_price') or pos.get('current_price') or pos.get('entry_price', 0))
                    liq_price = float(pos.get('liquidation_price', 0))
                    qty = abs(float(pos.get('position_amt') or pos.get('quantity', 0)))
                    side = pos.get('side', 'LONG').upper()
                    
                    if current_price > 0 and liq_price > 0 and qty > 0:
                        if side == 'LONG':
                            cushion_pct = ((current_price - liq_price) / current_price) * 100.0
                            cushion_usd = (current_price - liq_price) * qty
                        else:  # SHORT
                            cushion_pct = ((liq_price - current_price) / current_price) * 100.0
                            cushion_usd = (liq_price - current_price) * qty
                        
                        if cushion_pct < min_cushion_pct:
                            min_cushion_pct = cushion_pct
                        if cushion_usd < min_cushion_usd:
                            min_cushion_usd = cushion_usd
                
                # Fallback: Calculate liquidation prices from partition state positions
                if min_cushion_pct == float('inf') and partitions:
                    # Try to get current prices using realtime_pnl calculator
                    current_prices = {}
                    try:
                        from services.realtime_pnl import RealtimePnLCalculator
                        pnl_calc = RealtimePnLCalculator()
                        account_pnl = await pnl_calc.calculate_account_pnl(account_id, force_refresh=True)
                        # Extract current prices from PnL positions
                        for pos_pnl in account_pnl.positions:
                            current_prices[pos_pnl.symbol] = pos_pnl.current_price
                    except Exception:
                        # If PnL calculator fails, try to fetch prices from broker directly
                        try:
                            broker = self.order_executor.account_brokers.get(account_id)
                            if broker:
                                # Get all broker positions which should have current prices
                                broker_pos = await asyncio.to_thread(broker.get_all_positions) if hasattr(broker, 'get_all_positions') else []
                                for pos in broker_pos:
                                    symbol = pos.get('symbol') or pos.get('product_id', '')
                                    if symbol:
                                        # Try multiple price fields
                                        price = (pos.get('mark_price') or 
                                                pos.get('current_price') or 
                                                pos.get('last_price') or 
                                                pos.get('price'))
                                        if price:
                                            current_prices[symbol] = float(price)
                        except Exception:
                            # If we still can't get prices, we'll skip these positions
                            logger.debug(f"Could not fetch current prices for min liq cushion calculation for {account_id}")
                    
                    # Calculate liquidation prices from partition positions
                    # Note: 0.4% maintenance margin is used consistently throughout codebase
                    # (see coinbase_futures.py line 126, position_sizer.py line 261)
                    # TODO: Fetch actual maintenance margin from Coinbase API when available
                    liquidation_buffer = 0.004  # 0.4% maintenance margin (Coinbase default, consistent across codebase)
                    for part in partitions.values():
                        for pos in part.get('open_position_details', []):
                            entry_price = float(pos.get('entry_price', 0))
                            leverage = float(pos.get('leverage', 1))
                            qty = abs(float(pos.get('quantity', 0)))
                            side = pos.get('side', 'LONG').upper()
                            symbol = pos.get('symbol', '')
                            
                            if entry_price > 0 and leverage > 0 and qty > 0:
                                # Calculate liquidation price
                                if side == 'LONG':
                                    liq_price = entry_price * (1 - (1 / leverage) - liquidation_buffer)
                                else:  # SHORT
                                    liq_price = entry_price * (1 + (1 / leverage) + liquidation_buffer)
                                
                                # Get current price - CRITICAL: Only use if we have actual current price
                                # Never use entry_price as current_price (would give incorrect cushion)
                                current_price = current_prices.get(symbol)
                                
                                # Only calculate cushion if we have a valid current price
                                if current_price and current_price > 0:
                                    if side == 'LONG':
                                        cushion_pct = ((current_price - liq_price) / current_price) * 100.0
                                        cushion_usd = (current_price - liq_price) * qty
                                    else:  # SHORT
                                        cushion_pct = ((liq_price - current_price) / current_price) * 100.0
                                        cushion_usd = (liq_price - current_price) * qty
                                    
                                    if cushion_pct < min_cushion_pct:
                                        min_cushion_pct = cushion_pct
                                    if cushion_usd < min_cushion_usd:
                                        min_cushion_usd = cushion_usd
                
                if min_cushion_pct != float('inf'):
                    min_liq_cushion_pct = max(0.0, min_cushion_pct)
                if min_cushion_usd != float('inf'):
                    min_liq_cushion_usd = max(0.0, min_cushion_usd)
            except Exception as exc:
                logger.debug(f"Error calculating min liq cushion for {account_id}: {exc}")
            
            account_entry['min_liq_cushion_pct'] = min_liq_cushion_pct
            account_entry['min_liq_cushion_usd'] = min_liq_cushion_usd
            # Maintenance buffer PASS/FAIL vs threshold
            account_entry['maint_buffer_pass'] = (free_margin_pct >= settings.MIN_FREE_MARGIN_PCT_THRESHOLD)
            # Aggregate commissions from trade history
            commissions_today = 0.0
            commissions_mtd = 0.0
            try:
                state = partition_manager.partition_state.get(account_id, {})
                now = datetime.now(self.timezone)
                today_iso = now.date().isoformat()
                month_start = now.replace(day=1).date().isoformat()
                for partition in (state.get('partitions', {}) or {}).values():
                    for trade in partition.get('trade_history', []):
                        exit_time = trade.get('exit_time')
                        if not exit_time:
                            continue
                        try:
                            trade_date = datetime.fromisoformat(exit_time.replace('Z', '+00:00')).astimezone(self.timezone).date().isoformat()
                        except Exception:
                            try:
                                trade_date = exit_time[:10]  # Fallback to ISO date string
                            except Exception as date_parse_error:
                                logger.warning(f"EOD: Failed to parse exit_time '{exit_time}' for commission tracking: {date_parse_error}")
                                continue
                        commission = float(trade.get('commission', 0.0))
                        if trade_date == today_iso:
                            commissions_today += commission
                        if trade_date >= month_start:
                            commissions_mtd += commission
            except Exception as exc:
                logger.debug(f"Error aggregating commissions for {account_id}: {exc}")
            account_entry['commissions_today'] = commissions_today
            account_entry['commissions_mtd'] = commissions_mtd
            # Finalize per-strategy risk map for notifier
            # Convert cumulative-pnl min to pct of starting_total to provide a conservative % basis
            # If starting_total is zero, leave None and the notifier will show n/a or 0.
            try:
                starting_total = account_entry.get('starting_total') or 0.0
                strategy_risk: Dict[str, Dict[str, Any]] = {}
                
                # Intraday drawdown from trade history
                for sid, (min_cum, tlabel) in strategy_min_cum_pnl.items():
                    dd_pct = (min_cum / starting_total * 100.0) if starting_total > 0 else None
                    strategy_risk.setdefault(sid, {})['intraday_dd_pct'] = dd_pct
                    strategy_risk.setdefault(sid, {})['intraday_dd_time'] = tlabel
                
                # Use real-time telemetry from broker for per-strategy margin
                strategy_telemetry = telemetry.get('strategies', {})
                all_strategy_ids = set(list(strategy_min_cum_pnl.keys()) + list(strategy_peak_margin.keys()) + list(strategy_telemetry.keys()))
                
                for sid in all_strategy_ids:
                    strat_telemetry = strategy_telemetry.get(sid, {})
                    qty_from_telemetry = 0.0
                    
                    # Priority 1: Use telemetry worst margin (from broker updates throughout the day)
                    worst_margin_from_telemetry = strat_telemetry.get('worst_margin_usage_pct_today') if strat_telemetry else None
                    worst_time_from_telemetry = strat_telemetry.get('worst_margin_usage_time_today', '--:--') if strat_telemetry else '--:--'
                    
                    # Priority 2: Use calculated peak margin from trade history
                    peak_from_history = strategy_peak_margin.get(sid)
                    
                    # Priority 3: Use current margin from telemetry
                    current_margin_from_telemetry = strat_telemetry.get('current_margin_usage_pct', 0.0) if strat_telemetry else 0.0
                    
                    # Determine worst margin: prefer telemetry, then history, then current
                    if worst_margin_from_telemetry is not None:
                        worst_margin_pct = worst_margin_from_telemetry
                        worst_margin_time = worst_time_from_telemetry
                    elif peak_from_history:
                        worst_margin_pct = peak_from_history[0]
                        worst_margin_time = peak_from_history[1]
                    else:
                        worst_margin_pct = current_margin_from_telemetry
                        worst_margin_time = '--:--'
                    
                    if strat_telemetry:
                        # Use real broker data (preferred)
                        qty_from_telemetry = strat_telemetry.get('current_total_qty', 0.0)
                        strategy_risk.setdefault(sid, {})['qty_current'] = qty_from_telemetry
                        strategy_risk.setdefault(sid, {})['margin_usage_current_pct'] = current_margin_from_telemetry
                    else:
                        strategy_risk.setdefault(sid, {})['qty_current'] = 0.0
                        strategy_risk.setdefault(sid, {})['margin_usage_current_pct'] = 0.0
                    
                    # Set worst margin (peak) from best available source
                    strategy_risk.setdefault(sid, {})['margin_usage_worst_pct'] = worst_margin_pct
                    strategy_risk.setdefault(sid, {})['margin_usage_worst_time'] = worst_margin_time
                    
                    # Fallback: If telemetry quantity is 0 or missing, calculate from partition positions
                    if qty_from_telemetry == 0.0:
                        qty_fallback = 0.0
                        # Map strategy_id to symbol (partition positions use symbol, not strategy_id)
                        symbol_map = {
                            'ichimoku-coinbase-btc-5m': 'BTC-PERP',
                            'ichimoku-coinbase-eth-5m': 'ETH-PERP',
                            'ichimoku-coinbase-sol-5m': 'SOL-PERP',
                            'ichimoku-coinbase-xrp-5m': 'XRP-PERP',
                        }
                        target_symbol = symbol_map.get(sid, '')
                        if target_symbol:
                            # Also check broker symbol formats (ETP-CBSE, BIP-CBSE, etc.)
                            broker_symbol_map = {
                                'BTC-PERP': ['BIP-CBSE', 'BIPZ2030'],
                                'ETH-PERP': ['ETP-CBSE', 'ETPZ2030'],
                                'SOL-PERP': ['SLP-CBSE', 'SLPZ2030'],
                                'XRP-PERP': ['XPP-CBSE', 'XPPZ2030'],
                            }
                            symbol_variants = [target_symbol] + broker_symbol_map.get(target_symbol, [])
                            
                            for part in (state.get('partitions', {}) or {}).values():
                                for pos in part.get('open_position_details', []):
                                    pos_symbol = pos.get('symbol', '').upper()
                                    # Match by symbol (check all variants)
                                    if any(variant.upper() in pos_symbol or pos_symbol in variant.upper() for variant in symbol_variants):
                                        try:
                                            qty_fallback += abs(float(pos.get('quantity') or 0.0))
                                        except Exception:
                                            continue
                        strategy_risk.setdefault(sid, {})['qty_current'] = qty_fallback
                    
                    # Get Margin Per 1 contract from cached margin rates (same cache used in trade validation)
                    # These values come from the broker's daily margin cache (updated during trade validation):
                    # - margin_per_1_1pm (intraday): Margin rate during 6PM-4PM ET
                    # - margin_per_1_1am (afterhours): Margin rate during 4PM-6PM ET
                    # Priority: Use cached rates (most recent from trade validation) > telemetry (from trades)
                    margin_per_1_1pm = 0.0
                    margin_per_1_1am = 0.0
                    
                    # PRIMARY: Get from broker's daily margin cache (same rates used in trade validation)
                    # These are the most recent rates, updated during trade validation
                    try:
                        broker = self.order_executor.account_brokers.get(account_id)
                        if broker and hasattr(broker, 'broker') and hasattr(broker.broker, 'get_margin_requirements'):
                            symbol_map = {
                                'ichimoku-coinbase-btc-5m': 'BTC-PERP',
                                'ichimoku-coinbase-eth-5m': 'ETH-PERP',
                                'ichimoku-coinbase-sol-5m': 'SOL-PERP',
                                'ichimoku-coinbase-xrp-5m': 'XRP-PERP',
                            }
                            symbol = symbol_map.get(sid, '')
                            if symbol:
                                try:
                                    from config.yaml_config import get_strategy_by_id
                                    strategy_config = get_strategy_by_id(self.order_executor.config, sid)
                                    if strategy_config:
                                        contract_size = float(strategy_config.sizing.min_qty or 0.0)  # min_qty = contract size
                                        leverage = 3  # Default leverage (could get from account config)
                                        
                                        # Ensure margin rates are cached (use most recent cached rates)
                                        # EOD report uses the most recent cached rates (from trade validation or initialization)
                                        # force_refresh=False means use cached rates if available (most recent from API)
                                        broker.broker.ensure_daily_margin_rates(symbol, "LONG", force_refresh=False)
                                        
                                        # Get margin rates (intraday and overnight) from cache
                                        # force_period='intraday'/'overnight' with force_fresh=False uses cached rates
                                        # These are the most recent rates fetched from API (during trade validation or initialization)
                                        margin_req_intraday = broker.broker.get_margin_requirements(symbol, "LONG", force_period='intraday', force_fresh=False)
                                        margin_req_overnight = broker.broker.get_margin_requirements(symbol, "LONG", force_period='overnight', force_fresh=False)
                                        
                                        # Get current price - use robust price fetching with multiple fallbacks
                                        current_price = 0.0
                                        
                                        # Method 1: Try broker wrapper's get_current_price
                                        if hasattr(broker, 'get_current_price'):
                                            try:
                                                current_price = await asyncio.to_thread(broker.get_current_price, symbol)
                                            except Exception:
                                                pass
                                        
                                        # Method 2: Try broker's get_current_price directly
                                        if current_price == 0.0 and hasattr(broker, 'broker') and hasattr(broker.broker, 'get_current_price'):
                                            try:
                                                current_price = await asyncio.to_thread(broker.broker.get_current_price, symbol)
                                            except Exception:
                                                pass
                                        
                                        # Method 3: Try REST client (works in DEMO mode)
                                        if current_price == 0.0 and hasattr(broker.broker, 'rest_client') and broker.broker.rest_client:
                                            try:
                                                product_id_map = {
                                                    'BTC-PERP': 'BIP-CBSE',
                                                    'ETH-PERP': 'ETP-CBSE',
                                                    'SOL-PERP': 'SLP-CBSE',
                                                    'XRP-PERP': 'XPP-CBSE'
                                                }
                                                product_id = product_id_map.get(symbol)
                                                if product_id:
                                                    response = await asyncio.to_thread(broker.broker.rest_client.get_product, product_id)
                                                    p = getattr(response, 'product', None)
                                                    if p:
                                                        price = (
                                                            getattr(p, 'price', None) or 
                                                            getattr(p, 'mid_market_price', None) or
                                                            getattr(p, 'last_price', None) or
                                                            getattr(p, 'mark_price', None)
                                                        )
                                                        if price:
                                                            current_price = float(price)
                                            except Exception:
                                                pass
                                        
                                        # Method 4: Fallback to yfinance
                                        if current_price == 0.0:
                                            try:
                                                import yfinance as yf
                                                yf_ticker_map = {
                                                    'BTC-PERP': 'BTC-USD',
                                                    'ETH-PERP': 'ETH-USD',
                                                    'SOL-PERP': 'SOL-USD',
                                                    'XRP-PERP': 'XRP-USD'
                                                }
                                                yf_ticker = yf_ticker_map.get(symbol)
                                                if yf_ticker:
                                                    ticker = yf.Ticker(yf_ticker)
                                                    try:
                                                        fast_info = await asyncio.to_thread(getattr, ticker, 'fast_info', None)
                                                        if fast_info and hasattr(fast_info, 'lastPrice'):
                                                            current_price = float(fast_info.lastPrice)
                                                        else:
                                                            info = await asyncio.to_thread(ticker.history, period='1d', interval='1m')
                                                            if not info.empty:
                                                                current_price = float(info['Close'].iloc[-1])
                                                    except Exception:
                                                        info = await asyncio.to_thread(ticker.history, period='1d', interval='1m')
                                                        if not info.empty:
                                                            current_price = float(info['Close'].iloc[-1])
                                            except Exception:
                                                pass
                                        
                                        # Method 5: Final fallback to Coinbase Public API
                                        if current_price == 0.0:
                                            try:
                                                import requests
                                                spot_symbol_map = {
                                                    'BTC-PERP': 'BTC',
                                                    'ETH-PERP': 'ETH',
                                                    'SOL-PERP': 'SOL',
                                                    'XRP-PERP': 'XRP'
                                                }
                                                currency = spot_symbol_map.get(symbol)
                                                if currency:
                                                    url = f"https://api.coinbase.com/v2/exchange-rates?currency={currency}"
                                                    response = requests.get(url, timeout=5)
                                                    if response.status_code == 200:
                                                        data = response.json()
                                                        rates = data.get('data', {}).get('rates', {})
                                                        usd_rate = rates.get('USD')
                                                        if usd_rate:
                                                            current_price = float(usd_rate)
                                            except Exception:
                                                pass
                                        
                                        if current_price > 0:
                                            # Margin per 1 contract = (price * contract_size * margin_rate) / leverage
                                            intraday_rate = margin_req_intraday.get('daytime_rate', margin_req_intraday.get('current_rate', 0.20))
                                            overnight_rate = margin_req_overnight.get('overnight_rate', margin_req_overnight.get('current_rate', intraday_rate * 1.2))
                                            
                                            margin_per_1_intraday = (current_price * contract_size * intraday_rate) / leverage
                                            margin_per_1_overnight = (current_price * contract_size * overnight_rate) / leverage
                                            
                                            # Use cached margin rates (most recent from trade validation)
                                            margin_per_1_1pm = margin_per_1_intraday
                                            margin_per_1_1am = margin_per_1_overnight
                                            
                                            logger.debug(f"✅ EOD margin per 1 for {sid}: intraday=${margin_per_1_1pm:,.2f}, afterhours=${margin_per_1_1am:,.2f} (price=${current_price:,.2f})")
                                        else:
                                            logger.warning(f"⚠️ Could not fetch price for {symbol} in EOD report - margin_per_1 will be 0.0")
                                except Exception as exc:
                                    logger.warning(f"Error getting margin rates from cache for {sid}: {exc}")
                    except Exception as exc:
                        logger.warning(f"Error calculating margin per 1 for {sid}: {exc}")
                    
                    # FALLBACK: Get from runtime telemetry (captured when trades open) if cache unavailable
                    if margin_per_1_1pm == 0.0 or margin_per_1_1am == 0.0:
                        if strategy_telemetry:
                            if margin_per_1_1pm == 0.0:
                                margin_per_1_1pm = strategy_telemetry.get('margin_per_1_1pm', 0.0)
                            if margin_per_1_1am == 0.0:
                                margin_per_1_1am = strategy_telemetry.get('margin_per_1_1am', 0.0)
                    
                    strategy_risk.setdefault(sid, {})['margin_per_1_1pm'] = margin_per_1_1pm
                    strategy_risk.setdefault(sid, {})['margin_per_1_1am'] = margin_per_1_1am
                
                account_entry['strategy_risk'] = strategy_risk
            except Exception:
                account_entry['strategy_risk'] = {}

            total_pnl += account_pnl
            total_trades += account_trades
            wins += account_wins
            losses += account_losses
            starting_total += account_starting
            win_values.extend(account_entry['win_values'])
            loss_values.extend(account_entry['loss_values'])

        daily_pnl_pct = (total_pnl / starting_total * 100) if starting_total else 0.0
        win_rate = (wins / total_trades * 100) if total_trades else 0.0
        avg_win = sum(win_values) / len(win_values) if win_values else 0.0
        avg_loss = sum(loss_values) / len(loss_values) if loss_values else 0.0

        for account_entry in per_account.values():
            account_entry.pop('win_values', None)
            account_entry.pop('loss_values', None)

        aggregate = {
            'daily_pnl': total_pnl,
            'daily_pnl_pct': daily_pnl_pct,
            'win_rate': win_rate,
            'total_trades': total_trades,
            'avg_win': avg_win,
            'avg_loss': avg_loss
        }

        return aggregate, per_account

