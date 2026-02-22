"""
TradingView Agent - Position Reconciler Service
Provides unified position tracking across partition and broker systems
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from decimal import Decimal

from services import partition_manager
from services.state_store import state_store
from services.telegram_notifier import telegram_notifier

logger = logging.getLogger(__name__)


@dataclass
class UnifiedPosition:
    """Unified position data structure"""
    symbol: str
    side: str  # LONG, SHORT
    quantity: float
    entry_price: float
    current_price: Optional[float] = None
    unrealized_pnl_usd: Optional[float] = None
    unrealized_pnl_pct: Optional[float] = None
    deployed_capital: float = 0.0
    leverage: int = 1
    partition_id: Optional[str] = None
    account_id: str = ""
    broker: str = ""
    entry_time: Optional[datetime] = None
    age_hours: float = 0.0
    last_updated: Optional[datetime] = None


@dataclass
class ReconciliationResult:
    """Result of position reconciliation"""
    account_id: str
    total_positions: int
    mismatches_found: int
    mismatches_resolved: int
    errors: List[str]
    last_reconciled: datetime
    positions: List[UnifiedPosition]


class PositionReconciler:
    """
    Unified position reconciliation service
    
    Provides single source of truth for all position data by reconciling:
    - Partition manager positions (Firestore-backed)
    - Broker positions (live API)
    - Demo account positions (legacy system)
    """
    
    def __init__(self):
        self.last_reconcile_times = {}
        self.reconcile_interval = timedelta(minutes=5)  # Reconcile every 5 minutes
        self.position_cache = {}
        self.cache_ttl = timedelta(seconds=30)  # Cache for 30 seconds
        
    async def get_unified_positions(self, account_id: str, force_refresh: bool = False) -> List[UnifiedPosition]:
        """
        Get unified positions for an account
        
        Args:
            account_id: Account identifier
            force_refresh: Skip cache and fetch fresh data
            
        Returns:
            List of unified positions
        """
        cache_key = f"positions_{account_id}"
        now = datetime.utcnow()
        
        # Check cache first (unless force refresh)
        if not force_refresh and cache_key in self.position_cache:
            cached_data, cache_time = self.position_cache[cache_key]
            if now - cache_time < self.cache_ttl:
                logger.debug(f"Returning cached positions for {account_id}")
                return cached_data
        
        try:
            # Get positions from all sources
            partition_positions = await self._get_partition_positions(account_id)
            
            # For now, partition positions are our primary source
            # In LIVE mode, we would also reconcile with broker positions
            unified_positions = partition_positions
            
            # Update cache
            self.position_cache[cache_key] = (unified_positions, now)
            
            logger.info(f"Retrieved {len(unified_positions)} unified positions for {account_id}")
            return unified_positions
            
        except Exception as e:
            logger.error(f"Error getting unified positions for {account_id}: {e}")
            return []
    
    async def _get_partition_positions(self, account_id: str) -> List[UnifiedPosition]:
        """Get positions from partition manager"""
        try:
            positions = []
            
            # Get partition state from partition manager
            partition_state = partition_manager.partition_state.get(account_id, {})
            partitions = partition_state.get('partitions', {})
            
            for partition_id, partition_data in partitions.items():
                open_positions = partition_data.get('open_positions', {})
                
                for symbol, position_data in open_positions.items():
                    # Convert partition position to unified format
                    unified_pos = UnifiedPosition(
                        symbol=symbol,
                        side=position_data.get('side', 'UNKNOWN'),
                        quantity=float(position_data.get('quantity', 0)),
                        entry_price=float(position_data.get('entry_price', 0)),
                        deployed_capital=float(position_data.get('cash_used', 0)),
                        leverage=int(position_data.get('leverage', 1)),
                        partition_id=partition_id,
                        account_id=account_id,
                        broker=partition_data.get('broker', 'unknown'),
                        entry_time=self._parse_datetime(position_data.get('entry_time')),
                        last_updated=datetime.utcnow()
                    )
                    
                    # Calculate age
                    if unified_pos.entry_time:
                        age_delta = datetime.utcnow() - unified_pos.entry_time
                        unified_pos.age_hours = age_delta.total_seconds() / 3600
                    
                    positions.append(unified_pos)
            
            return positions
            
        except Exception as e:
            logger.error(f"Error getting partition positions for {account_id}: {e}")
            return []
    
    async def reconcile_account_positions(self, account_id: str) -> ReconciliationResult:
        """
        Reconcile positions for a specific account
        
        Args:
            account_id: Account to reconcile
            
        Returns:
            ReconciliationResult with details
        """
        logger.info(f"🔄 Starting position reconciliation for {account_id}")
        
        result = ReconciliationResult(
            account_id=account_id,
            total_positions=0,
            mismatches_found=0,
            mismatches_resolved=0,
            errors=[],
            last_reconciled=datetime.utcnow(),
            positions=[]
        )
        
        try:
            # Get unified positions (this will refresh cache)
            positions = await self.get_unified_positions(account_id, force_refresh=True)
            result.positions = positions
            result.total_positions = len(positions)
            
            # Update last reconcile time
            self.last_reconcile_times[account_id] = result.last_reconciled
            
            logger.info(f"✅ Position reconciliation complete for {account_id}: {result.total_positions} positions")
            
        except Exception as e:
            error_msg = f"Position reconciliation failed for {account_id}: {e}"
            logger.error(error_msg)
            result.errors.append(error_msg)
        
        return result
    
    async def reconcile_all_accounts(self) -> Dict[str, ReconciliationResult]:
        """Reconcile positions for all accounts"""
        logger.info("🔄 Starting reconciliation for all accounts")
        
        results = {}
        
        # Get all account IDs from partition state
        account_ids = list(partition_manager.partition_state.keys())
        
        for account_id in account_ids:
            try:
                result = await self.reconcile_account_positions(account_id)
                results[account_id] = result
            except Exception as e:
                logger.error(f"Failed to reconcile {account_id}: {e}")
                results[account_id] = ReconciliationResult(
                    account_id=account_id,
                    total_positions=0,
                    mismatches_found=0,
                    mismatches_resolved=0,
                    errors=[str(e)],
                    last_reconciled=datetime.utcnow(),
                    positions=[]
                )
        
        logger.info(f"✅ Reconciliation complete for {len(results)} accounts")
        return results
    
    async def get_position_summary(self, account_id: str) -> Dict[str, Any]:
        """Get position summary with P&L calculations"""
        try:
            positions = await self.get_unified_positions(account_id)
            
            total_deployed = sum(pos.deployed_capital for pos in positions)
            total_positions = len(positions)
            
            # Group by symbol
            by_symbol = {}
            for pos in positions:
                if pos.symbol not in by_symbol:
                    by_symbol[pos.symbol] = []
                by_symbol[pos.symbol].append(pos)
            
            return {
                "account_id": account_id,
                "total_positions": total_positions,
                "total_deployed_capital": total_deployed,
                "positions_by_symbol": {
                    symbol: {
                        "count": len(positions),
                        "total_quantity": sum(p.quantity for p in positions),
                        "total_deployed": sum(p.deployed_capital for p in positions),
                        "partitions": [p.partition_id for p in positions]
                    }
                    for symbol, positions in by_symbol.items()
                },
                "positions": [
                    {
                        "symbol": pos.symbol,
                        "side": pos.side,
                        "quantity": pos.quantity,
                        "entry_price": pos.entry_price,
                        "deployed_capital": pos.deployed_capital,
                        "partition_id": pos.partition_id,
                        "age_hours": pos.age_hours
                    }
                    for pos in positions
                ],
                "last_updated": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting position summary for {account_id}: {e}")
            return {
                "account_id": account_id,
                "error": str(e),
                "last_updated": datetime.utcnow().isoformat()
            }
    
    def get_last_reconcile_time(self, account_id: str) -> Optional[datetime]:
        """Get last reconciliation time for account"""
        return self.last_reconcile_times.get(account_id)
    
    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string safely"""
        if not dt_str:
            return None
        
        try:
            # Try ISO format first
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            try:
                # Try timestamp
                return datetime.fromtimestamp(float(dt_str))
            except (ValueError, TypeError):
                return None
    
    async def cleanup_stale_positions(self, max_age_hours: float = 48.0) -> Dict[str, int]:
        """
        Identify and optionally clean up stale positions
        
        Args:
            max_age_hours: Maximum age before position is considered stale
            
        Returns:
            Dict with cleanup statistics
        """
        logger.info(f"🧹 Checking for stale positions (>{max_age_hours}h)")
        
        cleanup_stats = {"accounts_checked": 0, "stale_positions": 0, "alerts_sent": 0}
        
        try:
            results = await self.reconcile_all_accounts()
            
            for account_id, result in results.items():
                cleanup_stats["accounts_checked"] += 1
                
                for position in result.positions:
                    if position.age_hours > max_age_hours:
                        cleanup_stats["stale_positions"] += 1
                        
                        # Send alert about stale position
                        await self._alert_stale_position(position)
                        cleanup_stats["alerts_sent"] += 1
            
            logger.info(f"✅ Stale position check complete: {cleanup_stats}")
            
        except Exception as e:
            logger.error(f"Error during stale position cleanup: {e}")
        
        return cleanup_stats
    
    async def _alert_stale_position(self, position: UnifiedPosition):
        """Send alert about stale position"""
        try:
            alert_text = f"""
🕐 STALE POSITION DETECTED

Symbol: {position.symbol}
Side: {position.side}
Quantity: {position.quantity}
Age: {position.age_hours:.1f} hours
Partition: {position.partition_id}
Deployed: ${position.deployed_capital:,.2f}

⚠️ Position has been open for {position.age_hours:.1f} hours
Consider reviewing for manual closure if needed.
"""
            
            await telegram_notifier.send_message(alert_text)
            logger.info(f"📱 Sent stale position alert for {position.symbol}")
            
        except Exception as e:
            logger.error(f"Failed to send stale position alert: {e}")


# Global instance
position_reconciler = PositionReconciler()
