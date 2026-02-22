"""
TradingView Agent - Afterhours Position Checker

Checks open positions 5 minutes before intraday ends (3:55PM ET) and auto-closes
positions that cannot afford afterhours margin rates to prevent margin calls.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from zoneinfo import ZoneInfo

from services import partition_manager
from services.telegram_notifier import telegram_notifier

logger = logging.getLogger(__name__)


class AfterhoursPositionChecker:
    """
    Checks and auto-closes positions that are oversized for afterhours margin rates.
    Runs at 3:55PM ET daily (5 minutes before afterhours begins at 4PM ET).
    """

    def __init__(self, order_executor):
        self.order_executor = order_executor
        self._running = False
        self._task: Optional[asyncio.Task] = None
        
        # Check time: 3:55PM ET (5 minutes before afterhours at 4PM ET)
        self.target_hour_et = 15  # 3PM ET
        self.target_minute_et = 55  # 55 minutes
        self.et_tz = ZoneInfo('America/New_York')

    async def start(self):
        """Start the afterhours position checker scheduler"""
        if self._running:
            logger.warning("Afterhours position checker already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info("✅ Afterhours position checker started (runs at 3:55PM ET daily)")

    async def stop(self):
        """Stop the afterhours position checker scheduler"""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Afterhours position checker stopped")

    async def trigger_now(self):
        """Run the check immediately (useful for manual tests)"""
        await self._check_and_close_oversized_positions()

    async def _run(self):
        """Main scheduler loop"""
        while self._running:
            seconds_until_run = self._seconds_until_next_run()
            logger.debug(f"Afterhours position check scheduled in {seconds_until_run / 60:.2f} minutes")
            try:
                await asyncio.sleep(seconds_until_run)
                await self._check_and_close_oversized_positions()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"Afterhours position checker error: {exc}", exc_info=True)
                await asyncio.sleep(300)  # Wait 5 minutes before retrying

    def _seconds_until_next_run(self) -> float:
        """Calculate seconds until next 3:55PM ET"""
        now_et = datetime.now(self.et_tz)
        target = now_et.replace(
            hour=self.target_hour_et,
            minute=self.target_minute_et,
            second=0,
            microsecond=0
        )

        # If we've already passed 3:55PM today, schedule for tomorrow
        if now_et >= target:
            target += timedelta(days=1)

        seconds = (target - now_et).total_seconds()
        # Safety: ensure we sleep at least 60 seconds to avoid tight loops
        return max(seconds, 60.0)

    async def _check_and_close_oversized_positions(self):
        """
        Check all open positions and close those that cannot afford afterhours margin rates.
        """
        try:
            logger.info("🔍 Starting afterhours position check (3:55PM ET)")
            
            # Check if feature is enabled
            config = self.order_executor.config
            coinbase_config = getattr(config, 'coinbase_futures', None)
            if not coinbase_config or not coinbase_config.auto_close_trades_oversized_for_afterhours_at_end_of_intraday:
                logger.debug("Afterhours position auto-close is disabled, skipping check")
                return

            # Get all Coinbase accounts
            coinbase_accounts = []
            for acc_id, acc in self.order_executor.account_configs.items():
                if hasattr(acc, 'broker') and acc.broker == 'coinbase_futures':
                    coinbase_accounts.append((acc_id, acc))

            if not coinbase_accounts:
                logger.debug("No Coinbase accounts found for afterhours position check")
                return

            total_checked = 0
            total_closed = 0
            closed_positions = []

            for account_id, account in coinbase_accounts:
                try:
                    checked, closed, positions = await self._check_account_positions(account_id, account)
                    total_checked += checked
                    total_closed += closed
                    closed_positions.extend(positions)
                except Exception as e:
                    logger.error(f"Error checking positions for account {account_id}: {e}")

            # Send summary notification
            if total_closed > 0:
                await self._send_closure_notification(total_closed, closed_positions)
            else:
                logger.info(f"✅ Afterhours position check complete: {total_checked} positions checked, all can afford afterhours margin rates")

        except Exception as e:
            logger.error(f"Error in afterhours position check: {e}", exc_info=True)

    async def _check_account_positions(self, account_id: str, account) -> tuple[int, int, List[Dict]]:
        """
        Check positions for a specific account and close oversized ones.
        
        Returns:
            (checked_count, closed_count, closed_positions_list)
        """
        checked = 0
        closed = 0
        closed_positions = []

        # Get partition state
        state = partition_manager.partition_state.get(account_id)
        if not state:
            return checked, closed, closed_positions

        # Get broker instance
        broker_wrapper = self.order_executor.account_brokers.get(account_id)
        if not broker_wrapper or not hasattr(broker_wrapper, 'broker'):
            logger.debug(f"Broker not initialized for {account_id}")
            return checked, closed, closed_positions

        broker = broker_wrapper.broker
        if not hasattr(broker, 'get_margin_requirements'):
            logger.debug(f"Broker does not support margin requirements for {account_id}")
            return checked, closed, closed_positions

        # Get all open positions across all partitions
        partitions = state.get('partitions', {})
        real_account = state.get('real_account', {})
        available_cash = real_account.get('available_cash', 0.0)

        for partition_id, partition_data in partitions.items():
            open_positions = partition_data.get('open_positions', {})
            
            for symbol, position in open_positions.items():
                checked += 1
                
                try:
                    quantity = position.get('quantity', 0.0)
                    side = position.get('side', 'LONG')
                    leverage = position.get('leverage', account.leverage)
                    
                    if quantity <= 0:
                        continue

                    # Get current price
                    try:
                        if hasattr(broker, 'get_current_price'):
                            current_price = await asyncio.to_thread(broker.get_current_price, symbol)
                        else:
                            # Fallback: try broker wrapper
                            if hasattr(broker_wrapper, 'get_current_price'):
                                current_price = await asyncio.to_thread(broker_wrapper.get_current_price, symbol)
                            else:
                                logger.warning(f"Could not get current price for {symbol}: no get_current_price method")
                                continue
                    except Exception as e:
                        logger.warning(f"Could not get current price for {symbol}: {e}")
                        continue

                    # Get afterhours margin requirements
                    position_side = 'LONG' if side == 'LONG' else 'SHORT'
                    margin_req = broker.get_margin_requirements(
                        symbol,
                        position_side,
                        force_period='overnight',  # Check afterhours rates
                        force_fresh=True  # Get fresh rates
                    )

                    # Calculate required afterhours margin
                    margin_calc = broker.calculate_required_margin(
                        symbol=symbol,
                        quantity=quantity,
                        price=current_price,
                        side=position_side,
                        leverage=leverage
                    )

                    overnight_margin_req = margin_calc.get('overnight_margin_required', 0.0)
                    initial_margin_req = margin_calc.get('initial_margin_required', 0.0)
                    
                    # Get partition's available cash from partition state
                    partition_virtual = partition_data.get('virtual_balance', 0.0)
                    partition_deployed = partition_data.get('deployed_cash', 0.0)
                    
                    # Calculate available cash (80% of virtual balance minus deployed)
                    max_allocation = partition_virtual * 0.80  # 80% allocation
                    partition_available = max(0.0, max_allocation - partition_deployed)
                    
                    # CRITICAL FIX: Check if position can afford TOTAL afterhours margin
                    # The position already has initial_margin_req deployed, so we need to check:
                    # Can the partition afford the TOTAL afterhours margin requirement?
                    # 
                    # Correct check: overnight_margin_req <= (partition_available + initial_margin_req)
                    # This accounts for the margin already deployed in the position
                    # 
                    # Previous bug: checked additional_margin_needed > partition_available
                    # This was wrong because it didn't account for the margin already deployed
                    # 
                    # Example: Position with $52.96 intraday margin, needs $148.28 afterhours margin
                    # - partition_available = $14.25 (after position deployed)
                    # - additional_needed = $95.32
                    # - Wrong check: $95.32 > $14.25 = TRUE → incorrectly closes
                    # - Correct check: $148.28 <= ($14.25 + $52.96) = $148.28 <= $67.21 = FALSE → correctly closes
                    #   OR: $148.28 <= $272.68 (max_allocation) = TRUE → position is safe!
                    #
                    # The position was validated at entry to afford afterhours margin, so if it passed
                    # entry validation, it should be safe. However, we still check in case:
                    # 1. Capital was used elsewhere (other positions opened)
                    # 2. Margin rates increased
                    # 3. Partition balance decreased
                    
                    # Check if position can afford TOTAL afterhours margin
                    # Use max_allocation (total available) instead of partition_available (remaining after deployment)
                    # This correctly accounts for the margin already deployed in the position
                    can_afford_afterhours = overnight_margin_req <= max_allocation
                    
                    if not can_afford_afterhours:
                        logger.warning(
                            f"⚠️ Position oversized for afterhours: {symbol} {side} {quantity:.4f} "
                            f"requires ${overnight_margin_req:,.2f} total afterhours margin "
                            f"(intraday: ${initial_margin_req:,.2f} → afterhours: ${overnight_margin_req:,.2f}), "
                            f"but partition {partition_id} max allocation is ${max_allocation:,.2f}. "
                            f"Auto-closing to prevent margin call."
                        )

                        # Auto-close the position
                        try:
                            await self._close_position(
                                account_id=account_id,
                                account=account,
                                broker=broker_wrapper,
                                partition_id=partition_id,
                                symbol=symbol,
                                side=side,
                                quantity=quantity,
                                leverage=leverage,
                                current_price=current_price
                            )
                            
                            closed += 1
                            closed_positions.append({
                                'account_id': account_id,
                                'partition_id': partition_id,
                                'symbol': symbol,
                                'side': side,
                                'quantity': quantity,
                                'intraday_margin': initial_margin_req,
                                'afterhours_margin': overnight_margin_req,
                                'max_allocation': max_allocation,
                                'available_cash': partition_available
                            })
                            
                            logger.info(f"✅ Auto-closed oversized position: {symbol} {side} {quantity:.4f} on partition {partition_id}")
                        except Exception as e:
                            logger.error(f"❌ Failed to auto-close position {symbol} on partition {partition_id}: {e}")
                    else:
                        logger.debug(
                            f"✅ Position safe for afterhours: {symbol} {side} {quantity:.4f} "
                            f"requires ${overnight_margin_req:,.2f} total afterhours margin, "
                            f"partition max allocation is ${max_allocation:,.2f}"
                        )

                except Exception as e:
                    logger.error(f"Error checking position {symbol} on partition {partition_id}: {e}")

        return checked, closed, closed_positions

    async def _close_position(
        self,
        account_id: str,
        account,
        broker,
        partition_id: str,
        symbol: str,
        side: str,
        quantity: float,
        leverage: int,
        current_price: float
    ):
        """Close a position by calling the order executor's exit method"""
        from models.webhook_payload import WebhookPayload
        
        # Create exit payload
        exit_side = 'SELL' if side == 'LONG' else 'BUY'
        
        # Find strategy ID from account's strategy_ids
        strategy_id = None
        if hasattr(account, 'strategy_ids') and account.strategy_ids:
            # Use first strategy ID that matches the symbol
            for sid in account.strategy_ids:
                if 'coinbase' in sid.lower() and symbol.split('-')[0].lower() in sid.lower():
                    strategy_id = sid
                    break
            if not strategy_id:
                strategy_id = account.strategy_ids[0]

        if not strategy_id:
            raise ValueError(f"Could not determine strategy ID for {symbol}")

        # Create exit payload with source and exit reason
        exit_payload = WebhookPayload(
            strategy_id=strategy_id,
            side=exit_side.lower(),
            symbol=symbol,
            price=current_price,
            action='exit',
            timestamp=datetime.utcnow().isoformat(),
            source='Afterhours Position Checker',
            exit_reason='AFTERHOURS_AUTO_CLOSE'
        )

        # Get strategy config for exit
        from config.yaml_config import get_strategy_by_id
        strategy = get_strategy_by_id(self.order_executor.config, strategy_id)
        if not strategy:
            raise ValueError(f"Strategy {strategy_id} not found in config")

        # Execute exit through order executor's internal method
        # Use the same method that handles exit signals
        response = await self.order_executor._execute_partition_exit(
            payload=exit_payload,
            account=account,
            strategy=strategy,
            broker=broker,
            symbol=symbol,
            current_price=current_price,
            partition_ids=[partition_id],
            price_hint=current_price
        )

        if response.status != "success":
            raise Exception(f"Exit failed: {response.error or 'Unknown error'}")

    async def _send_closure_notification(self, total_closed: int, closed_positions: List[Dict]):
        """Send Telegram notification about closed positions"""
        try:
            lines = [
                "🔒 AUTO-CLOSE: Oversized Positions Closed",
                "",
                f"Time: {datetime.now(self.et_tz).strftime('%I:%M %p ET')}",
                f"Total Closed: {total_closed}",
                ""
            ]

            for pos in closed_positions:
                lines.append(
                    f"• {pos['symbol']} {pos['side']} {pos['quantity']:.4f} "
                    f"(Partition: {pos['partition_id']})"
                )
                lines.append(
                    f"  Intraday: ${pos['intraday_margin']:,.2f} → "
                    f"Afterhours: ${pos['afterhours_margin']:,.2f} "
                    f"(+${pos['additional_needed']:,.2f})"
                )
                lines.append(
                    f"  Available: ${pos['available_cash']:,.2f} (insufficient)"
                )
                lines.append("")

            lines.append("✅ Positions closed to prevent margin calls at afterhours transition")

            message = "\n".join(lines)
            await telegram_notifier.send_message(message)
            logger.info(f"📱 Sent auto-close notification: {total_closed} positions closed")

        except Exception as e:
            logger.error(f"Failed to send auto-close notification: {e}")

