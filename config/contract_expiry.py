"""
Contract Expiry Configuration and Monitoring

This module defines contract expiry dates and provides monitoring
to prevent opening new positions on expiring contracts.

Safety Features:
- Auto-pause new entries 10 days before expiry
- Allow exits to continue (close existing positions)
- Telegram alerts at 30, 10, and 3 days before expiry
- Reject new entries on expired contracts
"""

from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# ============================================
# CONTRACT EXPIRY DATES
# ============================================

# Add new contracts as they're released
# Format: "SYMBOL": "YYYY-MM-DD"
CONTRACT_EXPIRY_DATES = {
    # Coinbase nano Bitcoin Perpetual Futures
    "BIPZ2030": "2030-12-31",  # Current contract (Dec 2030)
    "BTCUSDC": "2030-12-31",   # Generic symbol for Coinbase BTC PERP (same contract)
    
    # Add future contracts here as they're released:
    # "BIPH2031": "2031-03-31",  # Example: March 2031 contract
    # "BIPZ2031": "2031-12-31",  # Example: Dec 2031 contract
    
    # Binance BTCUSDT Perpetual (no expiry - true perpetual)
    "BTCUSDT": None,  # No expiry
    "BTCUSDTPERP": None,  # No expiry
    
    # Kraken BTC Perpetual (no expiry - true perpetual)
    "BTCUSD": None,  # No expiry
    "PF_XBTUSD": None,  # No expiry
}

# ============================================
# EXPIRY MONITORING THRESHOLDS
# ============================================

# Days before expiry to start warning
WARNING_DAYS = 30  # First warning at 30 days

# Days before expiry to pause new entries (allow exits only)
PAUSE_ENTRIES_DAYS = 10  # Stop new trades 10 days before expiry

# Days before expiry for critical alerts
CRITICAL_DAYS = 3  # Final warning at 3 days


# ============================================
# CONTRACT EXPIRY CHECKER
# ============================================

def get_contract_expiry(symbol: str) -> Optional[datetime]:
    """
    Get the expiry date for a contract symbol
    
    Args:
        symbol: Trading symbol (e.g., "BIPZ2030", "BTCUSDC")
    
    Returns:
        datetime object if contract has expiry, None if perpetual or unknown
    """
    expiry_str = CONTRACT_EXPIRY_DATES.get(symbol)
    
    if expiry_str is None:
        # Check if it's a known symbol
        if symbol in CONTRACT_EXPIRY_DATES:
            logger.debug(f"Symbol {symbol} is a true perpetual (no expiry)")
            return None
        else:
            logger.warning(f"Unknown symbol {symbol} - assuming no expiry")
            return None
    
    try:
        return datetime.strptime(expiry_str, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid expiry date format for {symbol}: {expiry_str}")
        return None


def get_days_until_expiry(symbol: str) -> Optional[int]:
    """
    Get number of days until contract expiry
    
    Args:
        symbol: Trading symbol
    
    Returns:
        Days until expiry (can be negative if expired), or None if perpetual
    """
    expiry_date = get_contract_expiry(symbol)
    
    if expiry_date is None:
        return None  # Perpetual contract
    
    days_left = (expiry_date - datetime.now()).days
    return days_left


def check_contract_status(symbol: str) -> Tuple[str, Optional[int], str]:
    """
    Check contract expiry status
    
    Args:
        symbol: Trading symbol
    
    Returns:
        Tuple of (status, days_left, message):
        - status: "ok", "warning", "pause_entries", "expired"
        - days_left: Days until expiry or None if perpetual
        - message: Human-readable status message
    """
    days_left = get_days_until_expiry(symbol)
    
    # Perpetual contract (no expiry)
    if days_left is None:
        return "ok", None, f"✅ {symbol} is a perpetual contract (no expiry)"
    
    # Expired
    if days_left < 0:
        return "expired", days_left, f"🚨 {symbol} EXPIRED {abs(days_left)} days ago! DO NOT TRADE!"
    
    # Expiring very soon (within PAUSE_ENTRIES_DAYS)
    if days_left <= PAUSE_ENTRIES_DAYS:
        return "pause_entries", days_left, f"⛔ {symbol} expires in {days_left} days! NEW ENTRIES DISABLED. Exits allowed."
    
    # Warning period (within WARNING_DAYS)
    if days_left <= WARNING_DAYS:
        return "warning", days_left, f"⚠️ {symbol} expires in {days_left} days. Prepare for rollover soon."
    
    # All good
    return "ok", days_left, f"✅ {symbol} expires in {days_left} days. Trading normally."


def should_allow_entry(symbol: str) -> Tuple[bool, str]:
    """
    Check if new entry orders should be allowed for this contract
    
    Args:
        symbol: Trading symbol
    
    Returns:
        Tuple of (allow, reason):
        - allow: True if entry allowed, False if blocked
        - reason: Human-readable explanation
    """
    status, days_left, message = check_contract_status(symbol)
    
    if status == "expired":
        return False, f"Contract {symbol} has EXPIRED. Rejecting entry order."
    
    if status == "pause_entries":
        return False, f"Contract {symbol} expires in {days_left} days. New entries disabled. Please roll to new contract."
    
    if status == "warning":
        logger.warning(f"{symbol} expires in {days_left} days - consider rolling to new contract soon")
        return True, f"Entry allowed (expires in {days_left} days)"
    
    # "ok" status
    return True, "Entry allowed"


def should_allow_exit(symbol: str) -> Tuple[bool, str]:
    """
    Check if exit orders should be allowed for this contract
    
    Exit orders are ALWAYS allowed (even on expired contracts)
    to ensure positions can be closed properly.
    
    Args:
        symbol: Trading symbol
    
    Returns:
        Tuple of (allow, reason):
        - allow: Always True for exits
        - reason: Human-readable explanation
    """
    status, days_left, message = check_contract_status(symbol)
    
    if status == "expired":
        return True, f"Exit allowed (contract expired {abs(days_left)} days ago - CLOSE ASAP!)"
    
    if status == "pause_entries":
        return True, f"Exit allowed (contract expires in {days_left} days)"
    
    return True, "Exit allowed"


def get_expiry_alerts() -> Dict[str, str]:
    """
    Get all contracts that need expiry alerts
    
    Returns:
        Dict of {symbol: alert_message} for contracts needing alerts
    """
    alerts = {}
    
    for symbol in CONTRACT_EXPIRY_DATES.keys():
        status, days_left, message = check_contract_status(symbol)
        
        # Skip perpetuals
        if days_left is None:
            continue
        
        # Alert at specific thresholds
        if days_left == WARNING_DAYS:
            alerts[symbol] = f"⚠️ ROLLOVER REMINDER: {symbol} expires in {days_left} days. Start planning contract rollover."
        
        elif days_left == PAUSE_ENTRIES_DAYS:
            alerts[symbol] = f"🚨 TRADING PAUSED: {symbol} expires in {days_left} days. NEW ENTRIES DISABLED. Roll to new contract NOW!"
        
        elif days_left == CRITICAL_DAYS:
            alerts[symbol] = f"🔥 URGENT: {symbol} expires in {days_left} days! Close all positions and roll to new contract immediately!"
        
        elif days_left == 0:
            alerts[symbol] = f"💀 EXPIRY TODAY: {symbol} expires TODAY! Close all positions NOW!"
        
        elif days_left < 0:
            alerts[symbol] = f"☠️ EXPIRED: {symbol} expired {abs(days_left)} days ago! Check for stuck positions!"
    
    return alerts


# ============================================
# MONITORING STATUS
# ============================================

def get_all_contract_status() -> Dict[str, Dict]:
    """
    Get status of all configured contracts
    
    Returns:
        Dict of {symbol: {status, days_left, message}}
    """
    status_report = {}
    
    for symbol in CONTRACT_EXPIRY_DATES.keys():
        status, days_left, message = check_contract_status(symbol)
        status_report[symbol] = {
            "status": status,
            "days_until_expiry": days_left,
            "message": message,
            "entries_allowed": should_allow_entry(symbol)[0],
            "exits_allowed": should_allow_exit(symbol)[0],
        }
    
    return status_report


def add_contract(symbol: str, expiry_date: str):
    """
    Add a new contract to monitoring
    
    Args:
        symbol: Contract symbol
        expiry_date: Expiry date in YYYY-MM-DD format (or None for perpetual)
    """
    CONTRACT_EXPIRY_DATES[symbol] = expiry_date
    logger.info(f"Added contract {symbol} with expiry {expiry_date}")


def remove_contract(symbol: str):
    """
    Remove a contract from monitoring (after rollover)
    
    Args:
        symbol: Contract symbol to remove
    """
    if symbol in CONTRACT_EXPIRY_DATES:
        del CONTRACT_EXPIRY_DATES[symbol]
        logger.info(f"Removed contract {symbol} from monitoring")
    else:
        logger.warning(f"Cannot remove {symbol} - not in monitoring list")


# ============================================
# EXAMPLE USAGE
# ============================================

if __name__ == "__main__":
    # Test the expiry checker
    print("\n" + "="*70)
    print("CONTRACT EXPIRY STATUS REPORT")
    print("="*70 + "\n")
    
    status = get_all_contract_status()
    
    for symbol, info in status.items():
        print(f"{symbol}:")
        print(f"  Status: {info['status']}")
        print(f"  Days until expiry: {info['days_until_expiry']}")
        print(f"  Entries allowed: {info['entries_allowed']}")
        print(f"  Exits allowed: {info['exits_allowed']}")
        print(f"  Message: {info['message']}")
        print()
    
    # Test specific checks
    print("\n" + "="*70)
    print("ENTRY/EXIT PERMISSION CHECKS")
    print("="*70 + "\n")
    
    for symbol in ["BIPZ2030", "BTCUSDT", "BTCUSDTPERP"]:
        allow_entry, entry_reason = should_allow_entry(symbol)
        allow_exit, exit_reason = should_allow_exit(symbol)
        
        print(f"{symbol}:")
        print(f"  Entry: {'✅ ALLOWED' if allow_entry else '❌ BLOCKED'} - {entry_reason}")
        print(f"  Exit:  {'✅ ALLOWED' if allow_exit else '❌ BLOCKED'} - {exit_reason}")
        print()
    
    # Test alerts
    print("\n" + "="*70)
    print("EXPIRY ALERTS")
    print("="*70 + "\n")
    
    alerts = get_expiry_alerts()
    if alerts:
        for symbol, alert in alerts.items():
            print(f"{alert}\n")
    else:
        print("No expiry alerts at this time.\n")

