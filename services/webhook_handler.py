"""
TradingView Agent - Webhook Handler Service
Processes incoming TradingView webhooks
"""

import logging
from datetime import datetime, timedelta
from typing import Dict

logger = logging.getLogger(__name__)


class WebhookHandler:
    """
    Handles webhook validation and routing
    """
    
    def __init__(self, settings):
        self.settings = settings
        self.last_heartbeats = {}  # strategy_name -> datetime
        logger.info("WebhookHandler initialized")
    
    async def check_broker_connections(self) -> Dict[str, bool]:
        """
        Check if all configured accounts are accessible
        
        Returns:
            {
                'account_1': True,
                'account_2': True,
                'total_enabled': 2
            }
        """
        status = {}
        enabled_count = 0
        
        # Check all configured accounts
        all_accounts = self.settings.get_all_configured_accounts()
        
        for account_num, account_config in all_accounts.items():
            account_key = f"account_{account_num}"
            
            try:
                # TODO: Ping broker API for each account
                # For now, assume OK if configured
                status[account_key] = True
                enabled_count += 1
                logger.debug(f"{account_config['label']}: ✅ OK")
            except Exception as e:
                status[account_key] = False
                logger.warning(f"{account_config['label']}: ❌ FAILED - {str(e)}")
        
        status['total_enabled'] = enabled_count
        status['total_configured'] = len(all_accounts)
        
        return status
    
    async def get_last_heartbeat(self) -> datetime:
        """Get last heartbeat timestamp"""
        if not self.last_heartbeats:
            return datetime.utcnow() - timedelta(minutes=10)
        
        # Return most recent heartbeat
        return max(self.last_heartbeats.values())
    
    async def update_heartbeat(self, strategy_name: str):
        """Update heartbeat for a strategy"""
        self.last_heartbeats[strategy_name] = datetime.utcnow()
        logger.info(f"Heartbeat updated for {strategy_name}")

