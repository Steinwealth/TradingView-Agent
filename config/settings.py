"""
TradingView Agent - Settings (V3.5 - YAML Config Only)

This module loads configuration from config.yaml and injects secrets from .env
All configuration is in config.yaml, only secrets in environment variables.

V3.5 Changes:
- Removed all ACCOUNT_N_*, STRATEGY_N_* environment variables
- Single declarative config.yaml file
- Secrets only in .env
- Cleaner, more maintainable architecture
"""

import os
import logging
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# Load environment variables (secrets only)
load_dotenv()

logger = logging.getLogger(__name__)

# ============================================
# YAML CONFIG (Primary Configuration)
# ============================================

# Import YAML config loader
from config.yaml_config import (
    get_config,
    get_strategy_by_id,
    get_account_by_id,
    get_accounts_for_strategy,
    get_enabled_accounts,
    get_enabled_strategies,
    AppConfig
)

# Load configuration at module level
try:
    CONFIG = get_config()
    logger.info("✅ V3.5 YAML configuration loaded successfully")
except Exception as e:
    logger.error(f"❌ Failed to load config.yaml: {str(e)}")
    raise


# ============================================
# HELPER FUNCTIONS (Simplified for V3.5)
# ============================================

def get_strategy_config(strategy_id: str):
    """Get strategy configuration by ID"""
    strategy = get_strategy_by_id(CONFIG, strategy_id)
    if not strategy:
        logger.warning(f"Strategy not found: {strategy_id}")
        return None
    
    return {
        'id': strategy.id,
        'name': strategy.name,
        'webhook_path': strategy.webhook_path,
        'data_symbol': strategy.data_symbol,
        'product_id': strategy.execution.product_id,
        'enabled': strategy.enabled
    }


def get_account_config(account_id: str):
    """Get account configuration by ID"""
    account = get_account_by_id(CONFIG, account_id)
    if not account:
        logger.warning(f"Account not found: {account_id}")
        return None
    
    return {
        'id': account.id,
        'broker': account.broker,
        'enabled': account.enabled,
        'mode': account.mode,
        'label': account.label,
        'strategy_ids': account.strategy_ids,
        'leverage': account.leverage,
        'api_key': account.api_key,
        'api_secret': account.api_secret,
        'api_passphrase': account.api_passphrase,
        'portfolio_id': account.portfolio_id,
        'partition_mode': account.partition_mode,
        'partition_split': account.partition_split,
        'partition_style': getattr(account, "partition_style", "isolated"),
        'partitions': [
            {
                'id': p.id,
                'enabled': p.enabled,
                'allocation_pct': p.allocation_pct,
                'leverage': p.leverage,
                'min_balance_usd': p.min_balance_usd,
                'strategy_ids': p.strategy_ids,
            }
            for p in account.partitions
        ]
    }


def get_all_configured_accounts():
    """Get all enabled accounts"""
    accounts = get_enabled_accounts(CONFIG)
    return {
        idx: {
            'account_id': account.id,
            'enabled': account.enabled,
            'mode': account.mode,
            'broker': account.broker,
            'api_key': account.api_key,
            'secret': account.api_secret,
            'leverage': account.leverage,
            'label': account.label,
            'strategy_ids': account.strategy_ids,
            'partition_mode': account.partition_mode,
            'partition_split': account.partition_split,
            'partition_style': getattr(account, "partition_style", "isolated"),
            'partitions': [
                {
                    'id': p.id,
                    'enabled': p.enabled,
                    'allocation_pct': p.allocation_pct,
                    'leverage': p.leverage,
                    'min_balance_usd': p.min_balance_usd,
                    'strategy_ids': p.strategy_ids,
                }
                for p in account.partitions
            ]
        }
        for idx, account in enumerate(accounts, start=1)
    }


# ============================================
# GLOBAL SETTINGS (From config.yaml)
# ============================================

# Risk Management
MAX_TOTAL_POSITIONS = CONFIG.risk_management.max_total_positions
MAX_LEVERAGE_GLOBAL = CONFIG.risk_management.max_leverage_global
ENABLE_TRADING = CONFIG.risk_management.enable_trading
MAX_COMMISSION_RATE_PCT = getattr(CONFIG.risk_management, "max_commission_rate_pct", 0.04)

# ADV Cap
ENABLE_ADV_CAP = CONFIG.risk_management.adv_cap.enabled
ADV_CAP_PCT = CONFIG.risk_management.adv_cap.cap_pct
ADV_LOOKBACK_DAYS = CONFIG.risk_management.adv_cap.lookback_days
ADV_MIN_VOLUME_USD = CONFIG.risk_management.adv_cap.min_volume_usd

# Position Sizing
USE_DYNAMIC_POSITION_SIZING = True  # Always true in V3.5
CAPITAL_ALLOCATION_PCT = CONFIG.general.capital_allocation_pct
if CAPITAL_ALLOCATION_PCT <= 1:
    CAPITAL_ALLOCATION_PCT *= 100.0
MAX_POSITION_ALLOCATION_PCT = getattr(CONFIG.general, "max_position_allocation_pct", 100.0)
if MAX_POSITION_ALLOCATION_PCT <= 1:
    MAX_POSITION_ALLOCATION_PCT *= 100.0
EQUITY_PERCENTAGE = CAPITAL_ALLOCATION_PCT  # default percent allocation (can be overridden per strategy)
MIN_POSITION_SIZE_USD = 10.0
ALLOW_ADV_CAP_BELOW_MINIMUM = False
MIN_CONTRACT_BUFFER_PCT = getattr(CONFIG.general, "min_contract_buffer_pct", 30.0)

# Daily Loss Limit (percentage-based)
MAX_DAILY_LOSS_PERCENT = 10.0  # Applied per account

# Retry Logic
MAX_RETRIES_ENTRY = 3
MAX_RETRIES_EXIT = 5
ENABLE_RETRY_EXITS = True
BROKER_RETRY_INITIAL_SECONDS = float(os.getenv("BROKER_RETRY_INITIAL_SECONDS", "30"))
BROKER_RETRY_MAX_SECONDS = float(os.getenv("BROKER_RETRY_MAX_SECONDS", "300"))
BROKER_RETRY_MAX_ATTEMPTS = int(os.getenv("BROKER_RETRY_MAX_ATTEMPTS", "0"))  # 0 = unlimited
BROKER_EXIT_VERIFY_INTERVAL_SECONDS = float(os.getenv("BROKER_EXIT_VERIFY_INTERVAL_SECONDS", "15"))
BROKER_RECONCILE_INTERVAL_SECONDS = int(os.getenv("BROKER_RECONCILE_INTERVAL_SECONDS", "900"))

# Telegram (from config.yaml)
TELEGRAM_ENABLED = CONFIG.telegram.enabled
TELEGRAM_BOT_TOKEN = CONFIG.telegram.bot_token or ""
TELEGRAM_CHAT_ID = CONFIG.telegram.chat_id or ""

# Deployment (from config.yaml)
ENVIRONMENT = CONFIG.deployment.environment
GCP_PROJECT_ID = CONFIG.deployment.gcp_project_id or ""
REGION = CONFIG.deployment.region
WEBHOOK_SECRET = CONFIG.deployment.webhook_secret
ADMIN_SECRET = CONFIG.deployment.admin_secret
ALLOWED_IPS = CONFIG.deployment.allowed_ips

# Calendar / Holiday Guard
CALENDAR_ENABLED = getattr(CONFIG, "calendar", None).enabled if getattr(CONFIG, "calendar", None) else False
CALENDAR_TZ = (getattr(getattr(CONFIG, "calendar", None), "timezone", None) or "America/New_York")
CALENDAR_PRE_HOURS = (getattr(getattr(CONFIG, "calendar", None), "pre_holiday_block_hours", None) or 24)
CALENDAR_POST_HOURS = (getattr(getattr(CONFIG, "calendar", None), "post_holiday_block_hours", None) or 24)
CALENDAR_HOLIDAYS = list(getattr(getattr(CONFIG, "calendar", None), "holidays", []) or [])
# Rules-based US market holidays (optional; defaults ON)
def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ["1", "true", "yes", "on"]

CALENDAR_RULES_US_MARKETS = _env_bool("CALENDAR_RULES_US_MARKETS", True)
CALENDAR_YEARS_BACK = int(os.getenv("CALENDAR_YEARS_BACK", "0"))
CALENDAR_YEARS_AHEAD = int(os.getenv("CALENDAR_YEARS_AHEAD", "3"))
CALENDAR_INCLUDE_LOW_VOLUME = _env_bool("CALENDAR_INCLUDE_LOW_VOLUME", True)

# Maintenance buffer threshold for EOD PASS/FAIL (percentage)
MIN_FREE_MARGIN_PCT_THRESHOLD = float(os.getenv("MIN_FREE_MARGIN_PCT_THRESHOLD", "30"))

# General / Timezone
LOCAL_TIMEZONE = CONFIG.general.local_timezone
try:
    LOCAL_TIMEZONE_INFO = ZoneInfo(LOCAL_TIMEZONE)
except Exception as exc:
    logger.error(f"Invalid local timezone '{LOCAL_TIMEZONE}': {exc}. Falling back to UTC.")
    LOCAL_TIMEZONE = "UTC"
    LOCAL_TIMEZONE_INFO = ZoneInfo("UTC")

# Demo Account Starting Balance (from config.yaml)
DEMO_STARTING_BALANCE = CONFIG.general.demo_starting_balance

# Monitoring
USE_FIRESTORE = CONFIG.monitoring.use_firestore
FIRESTORE_COLLECTION = CONFIG.monitoring.firestore_collection
LOG_LEVEL = CONFIG.monitoring.log_level


# ============================================
# BACKWARD COMPATIBILITY HELPERS (Minimal)
# ============================================

def get_accounts_for_strategy(strategy_id: str):
    """
    Get all enabled accounts for a strategy
    
    Returns list of dicts for compatibility with existing code
    """
    accounts = get_accounts_for_strategy(CONFIG, strategy_id)
    
    return [{
        'account_id': account.id,
        'enabled': account.enabled,
        'mode': account.mode,
        'broker': account.broker,
        'api_key': account.api_key,
        'secret': account.api_secret,
        'api_passphrase': account.api_passphrase,
        'leverage': account.leverage,
        'label': account.label
    } for account in accounts]


# ============================================
# VALIDATION
# ============================================

def validate_settings():
    """Validate all settings are correct"""
    errors = []
    
    # Check required secrets
    if not WEBHOOK_SECRET:
        errors.append("WEBHOOK_SECRET not set in .env")
    
    if not ADMIN_SECRET:
        errors.append("ADMIN_SECRET not set in .env")
    
    # Check at least one strategy enabled
    enabled_strategies = get_enabled_strategies(CONFIG)
    if not enabled_strategies:
        errors.append("No strategies enabled in config.yaml")
    
    # Check at least one account enabled
    enabled_accounts = get_enabled_accounts(CONFIG)
    if not enabled_accounts:
        errors.append("No accounts enabled in config.yaml")
    
    # Check all accounts have valid API keys
    for account in enabled_accounts:
        if not account.api_key or not account.api_secret:
            errors.append(f"Account '{account.id}' missing API credentials in .env")
    
    if errors:
        logger.error("❌ Configuration validation failed:")
        for error in errors:
            logger.error(f"   • {error}")
        raise ValueError(f"Configuration errors: {', '.join(errors)}")
    
    logger.info("✅ V3.5 configuration validated successfully")
    logger.info(f"   Strategies enabled: {len(enabled_strategies)}")
    logger.info(f"   Accounts enabled: {len(enabled_accounts)}")


# Run validation on import
try:
    validate_settings()
except Exception as e:
    logger.error(f"Settings validation failed: {str(e)}")
    # Don't raise during import, let application handle it


