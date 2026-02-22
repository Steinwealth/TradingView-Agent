"""
Contract Expiry Monitoring Service

Runs daily to check contract expiry status and send alerts.

Features:
- Daily health check of all contracts
- Telegram alerts at 30, 10, and 3 days before expiry
- Emergency alerts for expired contracts
- Status reporting endpoint
"""

import logging
import asyncio
from datetime import datetime
from config.contract_expiry import (
    get_expiry_alerts,
    get_all_contract_status,
    CONTRACT_EXPIRY_DATES,
    WARNING_DAYS,
    PAUSE_ENTRIES_DAYS,
    CRITICAL_DAYS
)
from services.telegram_notifier import telegram_notifier

logger = logging.getLogger(__name__)


class ContractMonitor:
    """
    Monitors contract expiry dates and sends alerts
    """
    
    def __init__(self):
        self.last_check = None
        self.last_alerts = {}  # Track which alerts we've sent to avoid spam
    
    async def check_contracts(self) -> dict:
        """
        Check all contracts for expiry and send alerts if needed
        
        Returns:
            Dict with check results and alerts sent
        """
        try:
            self.last_check = datetime.now()
            
            # Get all contract status
            status_report = get_all_contract_status()
            
            # Get contracts that need alerts
            expiry_alerts = get_expiry_alerts()
            
            # Send new alerts (avoid repeating same alert)
            alerts_sent = []
            for symbol, alert_message in expiry_alerts.items():
                # Check if we've already sent this alert today
                alert_key = f"{symbol}_{alert_message[:50]}"  # Use first 50 chars as key
                
                if alert_key not in self.last_alerts:
                    # Send alert
                    await telegram_notifier.send_alert(
                        f"📅 CONTRACT EXPIRY ALERT\n\n{alert_message}"
                    )
                    
                    # Track that we sent this alert
                    self.last_alerts[alert_key] = datetime.now()
                    alerts_sent.append(alert_message)
                    
                    logger.info(f"Sent expiry alert: {alert_message}")
            
            # Generate summary
            contracts_ok = sum(1 for s in status_report.values() if s['status'] == 'ok')
            contracts_warning = sum(1 for s in status_report.values() if s['status'] == 'warning')
            contracts_paused = sum(1 for s in status_report.values() if s['status'] == 'pause_entries')
            contracts_expired = sum(1 for s in status_report.values() if s['status'] == 'expired')
            
            summary = {
                "timestamp": self.last_check.isoformat(),
                "total_contracts": len(status_report),
                "status_breakdown": {
                    "ok": contracts_ok,
                    "warning": contracts_warning,
                    "paused": contracts_paused,
                    "expired": contracts_expired
                },
                "alerts_sent": len(alerts_sent),
                "alert_messages": alerts_sent,
                "contract_status": status_report
            }
            
            logger.info(
                f"Contract health check complete: "
                f"{contracts_ok} OK, {contracts_warning} WARNING, "
                f"{contracts_paused} PAUSED, {contracts_expired} EXPIRED"
            )
            
            # Send daily health summary if there are any issues
            if contracts_warning > 0 or contracts_paused > 0 or contracts_expired > 0:
                health_msg = self._format_health_summary(status_report)
                await telegram_notifier.send_alert(
                    f"📊 DAILY CONTRACT HEALTH REPORT\n\n{health_msg}"
                )
            
            return summary
        
        except Exception as e:
            logger.error(f"Error checking contracts: {str(e)}")
            return {
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
    
    def _format_health_summary(self, status_report: dict) -> str:
        """
        Format contract status into readable health summary
        """
        lines = []
        
        # Group by status
        for status_type in ["expired", "pause_entries", "warning", "ok"]:
            contracts = [
                (symbol, info) 
                for symbol, info in status_report.items() 
                if info['status'] == status_type
            ]
            
            if not contracts:
                continue
            
            if status_type == "expired":
                lines.append("☠️ EXPIRED (DO NOT TRADE):")
                for symbol, info in contracts:
                    lines.append(f"  • {symbol}: {info['message']}")
            
            elif status_type == "pause_entries":
                lines.append("\n⛔ PAUSED (Exits Only):")
                for symbol, info in contracts:
                    lines.append(f"  • {symbol}: {info['message']}")
            
            elif status_type == "warning":
                lines.append("\n⚠️ WARNING (Prepare Rollover):")
                for symbol, info in contracts:
                    lines.append(f"  • {symbol}: Expires in {info['days_until_expiry']} days")
            
            elif status_type == "ok":
                # Only show perpetuals and far-dated contracts
                perpetuals = [s for s, i in contracts if i['days_until_expiry'] is None]
                if perpetuals:
                    lines.append("\n✅ PERPETUALS (No Expiry):")
                    for symbol in perpetuals:
                        lines.append(f"  • {symbol}")
        
        return "\n".join(lines) if lines else "All contracts OK! ✅"
    
    async def run_daily_check(self):
        """
        Run daily contract monitoring (call this on a schedule)
        """
        logger.info("Starting daily contract expiry check...")
        result = await self.check_contracts()
        logger.info(f"Daily check complete. Alerts sent: {result.get('alerts_sent', 0)}")
        return result
    
    def get_last_check_time(self) -> str:
        """
        Get timestamp of last check
        """
        if self.last_check:
            return self.last_check.isoformat()
        return "Never"
    
    def get_status(self) -> dict:
        """
        Get current monitoring status
        """
        status_report = get_all_contract_status()
        
        return {
            "last_check": self.get_last_check_time(),
            "monitoring_enabled": True,
            "total_contracts": len(CONTRACT_EXPIRY_DATES),
            "contract_status": status_report,
            "alert_thresholds": {
                "warning_days": WARNING_DAYS,
                "pause_entries_days": PAUSE_ENTRIES_DAYS,
                "critical_days": CRITICAL_DAYS
            }
        }


# Global monitor instance
contract_monitor = ContractMonitor()


# ============================================
# BACKGROUND TASK
# ============================================

async def start_contract_monitoring():
    """
    Start background task for daily contract monitoring
    Runs once per day at midnight UTC
    """
    logger.info("Contract monitoring service started")
    
    while True:
        try:
            # Run check
            await contract_monitor.run_daily_check()
            
            # Wait 24 hours before next check
            await asyncio.sleep(24 * 60 * 60)  # 24 hours
        
        except Exception as e:
            logger.error(f"Error in contract monitoring loop: {str(e)}")
            # Wait 1 hour before retrying on error
            await asyncio.sleep(60 * 60)


# ============================================
# EXAMPLE USAGE
# ============================================

if __name__ == "__main__":
    # Test the monitor
    import sys
    
    async def test_monitor():
        print("\n" + "="*70)
        print("CONTRACT MONITOR TEST")
        print("="*70 + "\n")
        
        monitor = ContractMonitor()
        result = await monitor.check_contracts()
        
        print("Check Results:")
        print(f"  Timestamp: {result['timestamp']}")
        print(f"  Total Contracts: {result['total_contracts']}")
        print(f"  Status Breakdown:")
        for status, count in result['status_breakdown'].items():
            print(f"    {status}: {count}")
        print(f"  Alerts Sent: {result['alerts_sent']}")
        
        if result['alert_messages']:
            print("\nAlerts:")
            for alert in result['alert_messages']:
                print(f"  • {alert}")
        
        print("\n" + "="*70)
        print("CURRENT CONTRACT STATUS")
        print("="*70 + "\n")
        
        status = monitor.get_status()
        for symbol, info in status['contract_status'].items():
            print(f"{symbol}:")
            print(f"  Status: {info['status']}")
            print(f"  Days until expiry: {info['days_until_expiry']}")
            print(f"  Entries allowed: {info['entries_allowed']}")
            print(f"  Exits allowed: {info['exits_allowed']}")
            print(f"  Message: {info['message']}")
            print()
    
    # Run test
    asyncio.run(test_monitor())

