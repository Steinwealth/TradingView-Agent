"""
TradingView Agent - Partition Sync Scheduler (V3.6)

Background service for periodic balance synchronization
Syncs with broker every 10 trades OR every 24 hours (whichever comes first)

Why:
- Easy Ichimoku averages 0.89 trades/day
- 10 trades = ~11 days
- Need daily sync minimum to catch external changes
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
from zoneinfo import ZoneInfo

from config import settings

logger = logging.getLogger(__name__)


class PartitionSyncScheduler:
    """
    Manages periodic sync with broker for partition accounts
    
    Sync Triggers:
    1. Every 10 trades (manual trigger from partition_manager)
    2. Every 24 hours (background task)
    3. On-demand (API endpoint)
    
    Whichever comes first!
    """
    
    def __init__(self):
        self.last_sync = {}  # account_id -> datetime
        self.sync_interval_hours = 24  # Daily sync
        self.sync_every_n_trades = 10
        self.trade_counters = {}  # account_id -> count
        self.running = False
        self.background_task = None
        self.sync_time_hours = {1, 13}  # 1 AM ET and 1 PM ET
        self.last_time_slot = {}  # account_id -> timeslot string
        try:
            self.timezone = settings.LOCAL_TIMEZONE_INFO
        except AttributeError:
            self.timezone = ZoneInfo("UTC")
            logger.warning("LOCAL_TIMEZONE_INFO not set; partition sync scheduler defaulting to UTC.")
        
        logger.info("✅ Partition Sync Scheduler initialized")
        logger.info(
            f"   Sync triggers: Every {self.sync_every_n_trades} trades, every {self.sync_interval_hours}h, "
            f"and at 1:00 & 13:00 local time ({self.timezone.key})"
        )
    
    async def start(self):
        """Start background sync scheduler"""
        if self.running:
            logger.warning("Sync scheduler already running")
            return
        
        self.running = True
        self.background_task = asyncio.create_task(self._run_scheduler())
        logger.info("🔄 Partition sync scheduler started")
    
    async def stop(self):
        """Stop background sync scheduler"""
        self.running = False
        if self.background_task:
            self.background_task.cancel()
            try:
                await self.background_task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Partition sync scheduler stopped")
    
    async def _run_scheduler(self):
        """Background task - runs every hour and checks if sync needed"""
        while self.running:
            try:
                # Check every hour if daily sync is needed
                await asyncio.sleep(3600)  # 1 hour
                
                # Import here to avoid circular dependency
                from services.partition_manager import partition_state
                
                # Check each account
                for account_id in partition_state.keys():
                    await self._check_and_sync(account_id)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Sync scheduler error: {str(e)}")
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    async def _check_and_sync(self, account_id: str):
        """Check if sync needed and execute"""
        last_sync = self.last_sync.get(account_id)
        
        if last_sync is None:
            # First time - sync immediately
            logger.info(f"📊 First sync for {account_id}")
            await self.sync_account(account_id)
            return
        
        # Check if 24 hours passed
        time_since_sync = datetime.now() - last_sync
        hours_since_sync = time_since_sync.total_seconds() / 3600
        
        if hours_since_sync >= self.sync_interval_hours:
            logger.info(f"📊 Daily sync triggered for {account_id}")
            logger.info(f"   Last sync: {hours_since_sync:.1f}h ago")
            await self.sync_account(account_id)

        # Check fixed ET time slots
        now_local = datetime.now(self.timezone)
        if now_local.hour in self.sync_time_hours:
            slot = now_local.strftime("%Y-%m-%d-%H")
            last_slot = self.last_time_slot.get(account_id)
            if last_slot != slot:
                logger.info(f"🕒 Scheduled sync ({now_local.strftime('%Y-%m-%d %H:%M %Z')}) for {account_id}")
                await self.sync_account(account_id)
                self.last_time_slot[account_id] = slot
    
    async def sync_account(self, account_id: str, broker=None):
        """
        Sync account with broker
        
        Args:
            account_id: Account to sync
            broker: Broker instance (if None, will get from account config)
        """
        try:
            # Import here to avoid circular dependency
            from services.partition_manager import (
                partition_state,
                sync_real_account_balance
            )
            
            if account_id not in partition_state:
                logger.warning(f"Account {account_id} not initialized - skipping sync")
                return
            
            # Get broker if not provided
            if broker is None:
                from config.yaml_config import get_account_by_id, get_config
                config = get_config()
                account_config = get_account_by_id(config, account_id)
                
                if not account_config:
                    logger.error(f"Account {account_id} not found in config")
                    return
                
                # Create broker instance
                from brokers.coinbase_futures import CoinbaseFuturesBroker
                broker = CoinbaseFuturesBroker(
                    api_key=account_config.api_key,
                    api_secret=account_config.api_secret,
                    portfolio_id=getattr(account_config, 'portfolio_id', None)
                )
            
            # Perform sync
            logger.info(f"🔄 Syncing {account_id} with broker...")
            await sync_real_account_balance(account_id, broker)
            
            # Update last sync time
            self.last_sync[account_id] = datetime.now(self.timezone)
            now_local = datetime.now(self.timezone)
            self.last_time_slot[account_id] = now_local.strftime("%Y-%m-%d-%H")
            
            # Reset trade counter
            self.trade_counters[account_id] = 0
            
            logger.info(f"✅ Sync complete for {account_id}")
            
        except Exception as e:
            logger.error(f"❌ Sync failed for {account_id}: {str(e)}")
    
    def increment_trade_counter(self, account_id: str) -> bool:
        """
        Increment trade counter, return True if sync needed
        
        Returns:
            True if counter reached threshold (sync needed)
        """
        if account_id not in self.trade_counters:
            self.trade_counters[account_id] = 0
        
        self.trade_counters[account_id] += 1
        
        if self.trade_counters[account_id] >= self.sync_every_n_trades:
            logger.info(f"📊 Trade counter threshold reached for {account_id}")
            logger.info(f"   Trades since last sync: {self.trade_counters[account_id]}")
            return True
        
        return False
    
    def get_sync_status(self, account_id: str) -> Dict:
        """Get sync status for an account"""
        last_sync = self.last_sync.get(account_id)
        trade_count = self.trade_counters.get(account_id, 0)
        
        if last_sync:
            time_since_sync = datetime.now() - last_sync
            hours_since = time_since_sync.total_seconds() / 3600
            next_sync_in = max(0, self.sync_interval_hours - hours_since)
        else:
            hours_since = None
            next_sync_in = 0
        
        return {
            'account_id': account_id,
            'last_sync': last_sync.isoformat() if last_sync else None,
            'hours_since_sync': round(hours_since, 2) if hours_since else None,
            'trades_since_sync': trade_count,
            'next_sync_in_hours': round(next_sync_in, 2),
            'next_sync_trigger': 'daily' if trade_count < self.sync_every_n_trades else 'trade_count'
        }


# Global instance
sync_scheduler = PartitionSyncScheduler()

