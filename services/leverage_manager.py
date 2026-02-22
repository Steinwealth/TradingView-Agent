"""
TradingView Agent - Leverage Manager (V3.6)

Safe leverage management for multi-partition trading
Ensures each partition trades with correct leverage even when sharing one account

Key Features:
- Explicit leverage setting before every order
- Verification after setting
- Retry logic with exponential backoff
- Concurrency protection (asyncio lock)
- Full audit trail logging
"""

import logging
import asyncio
import inspect
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class LeverageManager:
    """
    Manages leverage settings for multi-partition trading
    Ensures each partition trades with correct leverage
    """
    
    def __init__(self):
        self.current_leverage: Dict[str, int] = {}  # Track current leverage per symbol
        self.leverage_lock = asyncio.Lock()  # Prevent race conditions
        logger.info("✅ Leverage Manager initialized")
    
    async def set_leverage_for_order(
        self,
        broker,
        account_id: str,
        partition_id: str,
        symbol: str,
        desired_leverage: int,
        max_attempts: int = 3
    ) -> bool:
        """
        Set leverage before placing order with retry logic
        
        Args:
            broker: Broker instance
            account_id: Account identifier
            partition_id: Partition identifier
            symbol: Trading symbol
            desired_leverage: Desired leverage (1-10)
            max_attempts: Maximum retry attempts
            
        Returns:
            True if successful, False otherwise
        """
        async with self.leverage_lock:
            cache_key = f"{account_id}_{symbol}"
            current = self.current_leverage.get(cache_key)
            
            logger.info(f"🔧 Setting leverage for {partition_id}:")
            logger.info(f"   Symbol: {symbol}")
            logger.info(f"   Current: {current}x (cached)" if current else "   Current: Unknown")
            logger.info(f"   Desired: {desired_leverage}x")
            
            # Retry loop
            for attempt in range(1, max_attempts + 1):
                try:
                    logger.info(f"   Attempt {attempt}/{max_attempts}")
                    
                    # Set leverage on broker (supports async or sync)
                    set_result = broker.set_leverage(symbol, desired_leverage)
                    success = await self._await_if_needed(set_result)
                    
                    if not success:
                        raise Exception("Broker returned False for set_leverage")
                    
                    # Verify leverage was set
                    await asyncio.sleep(0.5)  # Allow API to propagate
                    
                    actual_leverage = await self._await_if_needed(
                        broker.get_current_leverage(symbol)
                    )
                    
                    if actual_leverage == desired_leverage:
                        # Success!
                        self.current_leverage[cache_key] = desired_leverage
                        logger.info(f"✅ Leverage set and verified: {desired_leverage}x")
                        return True
                    else:
                        logger.warning(
                            f"⚠️ Leverage mismatch: "
                            f"Expected {desired_leverage}x, got {actual_leverage}x"
                        )
                        
                        if attempt < max_attempts:
                            wait_time = 2 ** attempt  # Exponential backoff
                            logger.info(f"   Retrying in {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"❌ Failed after {max_attempts} attempts")
                            return False
                
                except Exception as e:
                    logger.error(f"❌ Attempt {attempt} failed: {str(e)}")
                    
                    if attempt < max_attempts:
                        wait_time = 2 ** attempt
                        logger.info(f"   Retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"❌ All {max_attempts} attempts failed")
                        return False
            
            return False
    
    async def verify_leverage(
        self,
        broker,
        account_id: str,
        symbol: str,
        expected_leverage: int
    ) -> bool:
        """
        Verify current leverage matches expected
        
        Args:
            broker: Broker instance
            account_id: Account identifier
            symbol: Trading symbol
            expected_leverage: Expected leverage
            
        Returns:
            True if matches, False otherwise
        """
        try:
            actual_leverage = await self._await_if_needed(
                broker.get_current_leverage(symbol)
            )
            
            if actual_leverage != expected_leverage:
                logger.warning(f"⚠️ Leverage mismatch for {symbol}!")
                logger.warning(f"   Expected: {expected_leverage}x")
                logger.warning(f"   Actual: {actual_leverage}x")
                return False
            
            logger.info(f"✅ Leverage verified: {actual_leverage}x")
            return True
            
        except Exception as e:
            logger.error(f"❌ Leverage verification error: {str(e)}")
            return False
    
    def get_current_leverage(self, account_id: str, symbol: str) -> Optional[int]:
        """
        Get cached current leverage
        
        Args:
            account_id: Account identifier
            symbol: Trading symbol
            
        Returns:
            Current leverage or None if not cached
        """
        cache_key = f"{account_id}_{symbol}"
        return self.current_leverage.get(cache_key)
    
    def clear_cache(self, account_id: Optional[str] = None):
        """
        Clear leverage cache
        
        Args:
            account_id: If provided, clear only for this account
        """
        if account_id:
            keys_to_remove = [k for k in self.current_leverage.keys() if k.startswith(f"{account_id}_")]
            for key in keys_to_remove:
                del self.current_leverage[key]
            logger.info(f"🗑️ Leverage cache cleared for {account_id}")
        else:
            self.current_leverage.clear()
            logger.info(f"🗑️ Leverage cache cleared (all accounts)")

    async def _await_if_needed(self, maybe_coro: Any):
        """
        Await value if coroutine, otherwise return directly.
        """
        if inspect.isawaitable(maybe_coro):
            return await maybe_coro
        return maybe_coro


# Global instance
leverage_manager = LeverageManager()

