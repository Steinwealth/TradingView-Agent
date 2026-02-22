"""
Enhanced order execution with critical safety features:
- Retry logic with exponential backoff
- SL/TP verification
- Auto-close on SL/TP failure
- Position verification
- Telegram alerts
"""

import asyncio
import logging
from typing import Dict, Optional, Any
from utils.retry import retry_with_backoff, MaxRetriesExceeded
from services.telegram_notifier import telegram_notifier

logger = logging.getLogger(__name__)


class OrderExecutionSafety:
    """
    Provides safety wrappers around broker order execution.
    Implements retry, verification, and emergency protocols.
    """
    
    def __init__(self, broker):
        """
        Initialize safety wrapper.
        
        Args:
            broker: BinanceFuturesBroker instance
        """
        self.broker = broker
        logger.info("OrderExecutionSafety initialized")
    
    @retry_with_backoff(attempts=3, backoff_factor=2.0, initial_delay=2.0)
    async def place_entry_order_with_retry(
        self,
        symbol: str,
        side: str,
        quantity: float,
        leverage: int = 1,
        strategy_name: str = "Unknown"
    ) -> Dict[str, Any]:
        """
        Place entry order with automatic retry.
        
        Args:
            symbol: Trading symbol
            side: 'BUY' or 'SELL'
            quantity: Order quantity
            leverage: Leverage multiplier
            strategy_name: Strategy name for logging
        
        Returns:
            Order result dict
        
        Raises:
            MaxRetriesExceeded: If all retry attempts fail
        """
        logger.info(
            f"Placing {side} entry order: {quantity:.4f} {symbol} @ {leverage}x "
            f"(strategy: {strategy_name})"
        )
        
        # Place market order
        order_result = self.broker.place_market_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
            leverage=leverage,
            reduce_only=False
        )
        
        logger.info(f"✅ Entry order executed: {order_result.get('orderId')}")
        return order_result
    
    async def verify_position_opened(
        self,
        symbol: str,
        expected_side: str,
        expected_qty: float,
        strategy_name: str = "Unknown"
    ) -> Dict[str, Any]:
        """
        Verify position was opened correctly.
        
        Args:
            symbol: Trading symbol
            expected_side: Expected position side ('LONG' or 'SHORT')
            expected_qty: Expected quantity
            strategy_name: Strategy name
        
        Returns:
            Actual position dict from broker
        """
        logger.info(
            f"Verifying position opened: {expected_side} {expected_qty:.4f} {symbol}"
        )
        
        verification = self.broker.verify_position(
            symbol=symbol,
            expected_side=expected_side,
            expected_qty=expected_qty
        )
        
        if not verification['matches']:
            # Position mismatch - notify and use actual
            logger.warning(
                f"⚠️ Position mismatch: {verification['mismatch_details']}"
            )
            
            await telegram_notifier.notify_position_mismatch(
                symbol=symbol,
                expected_side=expected_side,
                actual_side=verification['actual_side'],
                expected_qty=expected_qty,
                actual_qty=verification['actual_qty']
            )
        else:
            logger.info(f"✅ Position verified: {expected_side} {expected_qty:.4f}")
        
        return verification['actual_position']
    
    async def set_sl_tp_with_verification(
        self,
        symbol: str,
        sl_price: Optional[float],
        tp_price: Optional[float],
        entry_price: float,
        strategy_name: str = "Unknown",
        max_attempts: int = 5
    ) -> Dict[str, Any]:
        """
        Set SL/TP with retry and verification. 
        If all attempts fail, EMERGENCY CLOSE POSITION.
        
        This is the MOST CRITICAL safety function!
        
        Args:
            symbol: Trading symbol
            sl_price: Stop loss price
            tp_price: Take profit price
            entry_price: Entry price (for calculating percentages)
            strategy_name: Strategy name
            max_attempts: Maximum retry attempts (default: 5)
        
        Returns:
            {
                'status': 'success' or 'emergency_closed',
                'sl_tp_result': dict,
                'verification': dict
            }
        
        Raises:
            Exception: Only if emergency close also fails
        """
        logger.info(
            f"Setting SL/TP for {symbol}: SL=${sl_price:.2f if sl_price else 0}, "
            f"TP=${tp_price:.2f if tp_price else 0}"
        )
        
        last_error = None
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Attempt to set SL/TP
                logger.info(f"SL/TP attempt {attempt}/{max_attempts}...")
                
                sl_tp_result = self.broker.set_stop_loss_take_profit(
                    symbol=symbol,
                    sl_price=sl_price,
                    tp_price=tp_price
                )
                
                # CRITICAL: Verify SL/TP orders exist on Binance
                await asyncio.sleep(0.5)  # Small delay to ensure orders are in system
                
                verification = self.broker.verify_sl_tp_exist(
                    symbol=symbol,
                    expected_sl=sl_price,
                    expected_tp=tp_price
                )
                
                # Check if both SL and TP exist (if they were requested)
                sl_ok = (sl_price is None) or verification['sl_exists']
                tp_ok = (tp_price is None) or verification['tp_exists']
                
                if sl_ok and tp_ok:
                    # SUCCESS!
                    logger.info(
                        f"✅ SL/TP set and verified on attempt {attempt}/{max_attempts}"
                    )
                    
                    # Send Telegram notification
                    await telegram_notifier.notify_sl_tp_set(
                        symbol=symbol,
                        sl_price=sl_price,
                        tp_price=tp_price,
                        entry_price=entry_price
                    )
                    
                    return {
                        'status': 'success',
                        'sl_tp_result': sl_tp_result,
                        'verification': verification
                    }
                else:
                    # Verification failed
                    missing = []
                    if not sl_ok:
                        missing.append("SL")
                    if not tp_ok:
                        missing.append("TP")
                    
                    error_msg = f"SL/TP verification failed: {', '.join(missing)} not found"
                    logger.warning(f"⚠️ {error_msg}")
                    last_error = error_msg
                    
                    # If not last attempt, wait and retry
                    if attempt < max_attempts:
                        delay = 2 ** attempt  # Exponential backoff: 2s, 4s, 8s, 16s, 32s
                        logger.info(f"Retrying in {delay}s...")
                        await asyncio.sleep(delay)
                        continue
                    
            except Exception as e:
                last_error = str(e)
                logger.error(f"❌ SL/TP attempt {attempt}/{max_attempts} failed: {e}")
                
                if attempt < max_attempts:
                    delay = 2 ** attempt
                    logger.info(f"Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    continue
        
        # 🚨 ALL ATTEMPTS FAILED - EMERGENCY PROTOCOL! 🚨
        logger.critical(
            f"🚨🚨 CRITICAL: SL/TP failed after {max_attempts} attempts for {symbol}! "
            f"INITIATING EMERGENCY CLOSE!"
        )
        
        # Notify immediately
        await telegram_notifier.notify_critical_failure(
            failure_type="SL/TP Placement Failure",
            details=f"Failed to set SL/TP after {max_attempts} attempts. Last error: {last_error}",
            symbol=symbol,
            action_taken="EMERGENCY POSITION CLOSE - Attempting now..."
        )
        
        # CLOSE POSITION IMMEDIATELY (can't leave unprotected!)
        try:
            logger.critical(f"Closing {symbol} position immediately (emergency)...")
            close_result = self.broker.close_position(symbol=symbol)
            
            # Verify position closed
            await asyncio.sleep(0.5)
            final_pos = self.broker.get_position(symbol)
            
            if final_pos['side'] == 'NONE':
                logger.critical(f"✅ Emergency close successful for {symbol}")
                
                await telegram_notifier.notify_critical_failure(
                    failure_type="SL/TP Failure - Position Closed",
                    details=f"Could not set SL/TP after {max_attempts} attempts. Position closed to prevent unprotected exposure.",
                    symbol=symbol,
                    action_taken="Position successfully closed at market"
                )
                
                return {
                    'status': 'emergency_closed',
                    'close_result': close_result,
                    'reason': last_error
                }
            else:
                # CRITICAL: Position still open!
                logger.critical(
                    f"🚨🚨 CRITICAL: Failed to close {symbol} position! "
                    f"Position: {final_pos['side']} {final_pos['quantity']}"
                )
                
                await telegram_notifier.notify_critical_failure(
                    failure_type="EMERGENCY CLOSE FAILED",
                    details=f"Failed to set SL/TP AND failed to close position! Position: {final_pos['side']} {final_pos['quantity']}",
                    symbol=symbol,
                    action_taken="NONE - MANUAL INTERVENTION REQUIRED IMMEDIATELY!"
                )
                
                raise Exception(
                    f"CRITICAL: Could not set SL/TP and could not close {symbol} position! "
                    f"Manual intervention required immediately!"
                )
                
        except Exception as close_error:
            logger.critical(f"🚨🚨 Emergency close failed: {close_error}")
            
            await telegram_notifier.notify_critical_failure(
                failure_type="EMERGENCY CLOSE FAILED",
                details=f"Failed to close position after SL/TP failure: {close_error}",
                symbol=symbol,
                action_taken="NONE - CRITICAL MANUAL INTERVENTION REQUIRED!"
            )
            
            raise Exception(
                f"CRITICAL: SL/TP failed and emergency close failed! {close_error}"
            )
    
    @retry_with_backoff(attempts=5, backoff_factor=2.0, initial_delay=2.0)
    async def close_position_with_retry(
        self,
        symbol: str,
        strategy_name: str = "Unknown"
    ) -> Dict[str, Any]:
        """
        Close position with automatic retry.
        
        Args:
            symbol: Trading symbol
            strategy_name: Strategy name
        
        Returns:
            Close order result
        
        Raises:
            MaxRetriesExceeded: If all retry attempts fail
        """
        logger.info(f"Closing position for {symbol} (strategy: {strategy_name})")
        
        close_result = self.broker.close_position(symbol=symbol)
        
        # Verify position closed
        await asyncio.sleep(0.5)
        final_pos = self.broker.get_position(symbol)
        
        if final_pos['side'] != 'NONE':
            raise Exception(
                f"Position not closed! Still have {final_pos['side']} "
                f"{final_pos['quantity']} {symbol}"
            )
        
        logger.info(f"✅ Position closed successfully for {symbol}")
        return close_result
    
    async def execute_entry_with_full_safety(
        self,
        symbol: str,
        side: str,
        quantity: float,
        leverage: int,
        sl_price: Optional[float],
        tp_price: Optional[float],
        strategy_name: str = "Unknown"
    ) -> Dict[str, Any]:
        """
        Execute complete entry flow with all safety features:
        1. Place entry order (with retry)
        2. Verify position opened
        3. Set SL/TP (with retry + verification + emergency close)
        4. Send Telegram notifications
        
        Args:
            symbol: Trading symbol
            side: 'BUY' or 'SELL'
            quantity: Order quantity
            leverage: Leverage multiplier
            sl_price: Stop loss price
            tp_price: Take profit price
            strategy_name: Strategy name
        
        Returns:
            Complete execution result
        """
        result = {
            'entry_order': None,
            'position_verification': None,
            'sl_tp_result': None,
            'status': 'unknown'
        }
        
        try:
            # Step 1: Place entry order (with retry)
            logger.info(f"=== EXECUTING ENTRY: {side} {quantity:.4f} {symbol} @ {leverage}x ===")
            
            entry_order = await self.place_entry_order_with_retry(
                symbol=symbol,
                side=side,
                quantity=quantity,
                leverage=leverage,
                strategy_name=strategy_name
            )
            result['entry_order'] = entry_order
            
            # Get fill price
            entry_price = float(entry_order.get('avgPrice', entry_order.get('price', 0)))
            
            # Notify order executed
            position_side = 'LONG' if side == 'BUY' else 'SHORT'
            await telegram_notifier.notify_order_executed(
                order_type="ENTRY",
                symbol=symbol,
                side=position_side,
                quantity=quantity,
                price=entry_price,
                leverage=leverage,
                strategy=strategy_name
            )
            
        except MaxRetriesExceeded as e:
            # Entry order failed after all retries
            logger.error(f"❌ Entry order failed after all retries: {e}")
            
            await telegram_notifier.notify_order_failed(
                order_type="ENTRY",
                symbol=symbol,
                attempts=3,
                final_error=str(e)
            )
            
            result['status'] = 'entry_failed'
            return result
        
        try:
            # Step 2: Verify position opened
            expected_side = 'LONG' if side == 'BUY' else 'SHORT'
            
            actual_position = await self.verify_position_opened(
                symbol=symbol,
                expected_side=expected_side,
                expected_qty=quantity,
                strategy_name=strategy_name
            )
            result['position_verification'] = actual_position
            
            # Use actual values from exchange
            actual_entry_price = actual_position.get('entry_price', entry_price)
            actual_quantity = actual_position.get('quantity', quantity)
            
        except Exception as e:
            logger.error(f"❌ Position verification failed: {e}")
            # Continue with expected values, but log warning
            actual_entry_price = entry_price
            actual_quantity = quantity
        
        try:
            # Step 3: Set SL/TP (MOST CRITICAL - with verification + emergency close)
            sl_tp_result = await self.set_sl_tp_with_verification(
                symbol=symbol,
                sl_price=sl_price,
                tp_price=tp_price,
                entry_price=actual_entry_price,
                strategy_name=strategy_name,
                max_attempts=5
            )
            result['sl_tp_result'] = sl_tp_result
            
            if sl_tp_result['status'] == 'success':
                result['status'] = 'success'
                logger.info(f"✅✅✅ ENTRY COMPLETE: {symbol} fully protected!")
            elif sl_tp_result['status'] == 'emergency_closed':
                result['status'] = 'emergency_closed'
                logger.warning(f"⚠️ Entry completed but position emergency closed (SL/TP failure)")
            
        except Exception as e:
            logger.critical(f"🚨 CRITICAL: SL/TP flow failed completely: {e}")
            result['status'] = 'sl_tp_failed'
            # This is already handled in set_sl_tp_with_verification
            # The position should have been emergency closed
        
        return result
    
    async def execute_exit_with_full_safety(
        self,
        symbol: str,
        strategy_name: str = "Unknown"
    ) -> Dict[str, Any]:
        """
        Execute exit with retry and verification.
        
        Args:
            symbol: Trading symbol
            strategy_name: Strategy name
        
        Returns:
            Exit result
        """
        result = {
            'close_order': None,
            'position_verification': None,
            'status': 'unknown'
        }
        
        try:
            logger.info(f"=== EXECUTING EXIT: {symbol} ===")
            
            # Get position before closing (for notification)
            pre_close_pos = self.broker.get_position(symbol)
            
            if pre_close_pos['side'] == 'NONE':
                logger.warning(f"No position to close for {symbol}")
                result['status'] = 'no_position'
                return result
            
            # Close position (with retry)
            close_order = await self.close_position_with_retry(
                symbol=symbol,
                strategy_name=strategy_name
            )
            result['close_order'] = close_order
            
            # Get exit price
            exit_price = float(close_order.get('avgPrice', close_order.get('price', 0)))
            
            # Notify order executed
            await telegram_notifier.notify_order_executed(
                order_type="EXIT",
                symbol=symbol,
                side=pre_close_pos['side'],
                quantity=pre_close_pos['quantity'],
                price=exit_price,
                leverage=pre_close_pos.get('leverage', 1),
                strategy=strategy_name
            )
            
            # Verify position closed
            final_pos = self.broker.get_position(symbol)
            
            if final_pos['side'] == 'NONE':
                logger.info(f"✅ Position verified closed for {symbol}")
                result['status'] = 'success'
            else:
                logger.warning(
                    f"⚠️ Position not fully closed: {final_pos['side']} "
                    f"{final_pos['quantity']} remaining"
                )
                result['status'] = 'partial_close'
            
            result['position_verification'] = final_pos
            
        except MaxRetriesExceeded as e:
            logger.error(f"❌ Exit order failed after all retries: {e}")
            
            await telegram_notifier.notify_order_failed(
                order_type="EXIT",
                symbol=symbol,
                attempts=5,
                final_error=str(e)
            )
            
            result['status'] = 'exit_failed'
        
        except Exception as e:
            logger.error(f"❌ Exit execution failed: {e}")
            result['status'] = 'error'
            result['error'] = str(e)
        
        return result

