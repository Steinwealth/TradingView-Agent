"""
TradingView Agent - YAML Configuration Loader (V3.5)
Loads declarative YAML config and injects secrets from environment variables
"""

import yaml
import os
import re
import logging
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field, validator
from pathlib import Path
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


# ============================================
# PYDANTIC MODELS FOR YAML STRUCTURE
# ============================================

class ExecutionConfig(BaseModel):
    """Strategy execution configuration"""
    product_id: str = Field(..., description="Broker product ID (e.g., BTC-PERP)")
    side_map: Dict[str, str] = Field(..., description="Action mapping (buy→BUY, sell→SELL)")


class SizingConfig(BaseModel):
    """Position sizing configuration"""
    capital_alloc_pct: float = Field(0.90, description="Capital allocation % (0.90 = 90%)")
    leverage: int = Field(1, ge=1, le=125, description="Default leverage")
    min_qty: float = Field(0.01, description="Minimum quantity")
    round_step: float = Field(0.01, description="Rounding step")


class StopPolicy(BaseModel):
    """Stop loss policy configuration"""
    type: str = Field(..., description="Type: fixed_pct or atr")
    value: Optional[float] = Field(None, description="Value (% for fixed_pct, multiplier for atr)")
    mult: Optional[float] = Field(None, description="ATR multiplier (if type=atr)")


class RiskConfig(BaseModel):
    """Risk management configuration"""
    max_margin_pct: float = Field(0.70, description="Max margin usage %")
    max_pos_contracts: Optional[int] = Field(None, description="Max position size in contracts")
    stop_policy: Optional[StopPolicy] = None
    tp_policy: Optional[StopPolicy] = None
    max_daily_loss_pct: float = Field(10.0, description="Max daily loss %")
    margin_mode: str = Field("ISOLATED", description="ISOLATED or CROSS")


class CoinbaseFuturesConfig(BaseModel):
    """Coinbase Futures position sizing and margin rate configuration"""
    all_trades_size_to_afterhours: bool = Field(
        False,
        description="If true, all trades use afterhours margin rates (conservative). If false, uses time-based optimization."
    )
    size_trades_with_intraday_rates_6p_10p: bool = Field(
        True,
        description="If true, uses time-based margin rate selection: 6:01PM-10:00PM ET = intraday rates, 10:01PM-6:00PM ET = afterhours rates"
    )
    auto_close_trades_oversized_for_afterhours_at_end_of_intraday: bool = Field(
        True,
        description="If true, checks all open positions 5 minutes before intraday ends (3:55PM ET) and auto-closes positions that cannot afford afterhours margin rates"
    )


class StrategyConfig(BaseModel):
    """TradingView strategy configuration"""
    id: str = Field(..., description="Unique strategy ID")
    name: str = Field(..., description="Display name")
    webhook_path: str = Field(..., description="Webhook endpoint path")
    data_symbol: str = Field(..., description="TradingView chart symbol")
    timeframe: Optional[str] = Field(None, description="Chart timeframe (5m, 15m, 1h, etc.)")
    enabled: bool = Field(True, description="Enable/disable strategy")
    execution: ExecutionConfig
    sizing: SizingConfig
    risk: Optional[RiskConfig] = None


class PartitionDefinition(BaseModel):
    """Virtual partition definition"""
    id: str = Field(..., description="Partition identifier")
    enabled: bool = Field(True, description="Enable/disable partition")
    allocation_pct: float = Field(..., gt=0, le=100, description="Allocation percentage of total balance")
    leverage: int = Field(..., ge=1, le=125, description="Partition leverage")
    min_balance_usd: float = Field(0.0, ge=0.0, description="Auto-disable threshold")
    strategy_ids: List[str] = Field(..., description="Strategies routed to this partition")


class AccountConfig(BaseModel):
    """Broker account configuration"""
    id: str = Field(..., description="Unique account ID")
    broker: str = Field(..., description="Broker type (coinbase_futures, binance_futures, etc.)")
    enabled: bool = Field(..., description="Enable/disable account")
    mode: str = Field(..., description="DEMO or LIVE")
    label: str = Field(..., description="Display label")
    strategy_ids: List[str] = Field(..., description="List of strategy IDs this account listens to")
    leverage: int = Field(1, ge=1, le=125, description="Account leverage")
    api_key: str = Field(..., description="API key (from env)")
    api_secret: str = Field(..., description="API secret (from env)")
    api_passphrase: Optional[str] = Field(None, description="API passphrase (Coinbase only)")
    portfolio_id: Optional[str] = Field(None, description="Broker portfolio UUID (Coinbase Advanced Trade)")
    partition_mode: str = Field("disabled", description="Partitioning mode (disabled/enabled)")
    partition_split: Optional[str] = Field(None, description="Partition split preset (50/50, 50/25/25, etc.)")
    partition_style: str = Field(
        "isolated",
        description="Capital accounting style for partitions (isolated/cooperative)"
    )
    partitions: List[PartitionDefinition] = Field(default_factory=list, description="Partition definitions")
    risk: Optional[RiskConfig] = None
    
    @validator('mode')
    def validate_mode(cls, v):
        if v not in ['DEMO', 'LIVE']:
            raise ValueError(f"Mode must be DEMO or LIVE, got: {v}")
        return v
    
    @validator('broker')
    def validate_broker(cls, v):
        valid = ['coinbase_futures', 'binance_futures', 'kraken_futures', 'oanda']
        if v not in valid:
            raise ValueError(f"Broker must be one of {valid}, got: {v}")
        return v

    @validator('partition_mode')
    def validate_partition_mode(cls, v):
        if v not in ['disabled', 'enabled']:
            raise ValueError(f"partition_mode must be 'disabled' or 'enabled', got: {v}")
        return v

    @validator('partition_style')
    def validate_partition_style(cls, v):
        value = v.lower()
        if value not in ['isolated', 'cooperative']:
            raise ValueError(f"partition_style must be 'isolated' or 'cooperative', got: {v}")
        return value

    @validator('partitions', always=True)
    def validate_partitions(cls, partitions, values):
        mode = values.get('partition_mode', 'disabled')
        split = values.get('partition_split')
        if mode == 'enabled':
            if not partitions:
                raise ValueError("Partitions must be defined when partition_mode is enabled")
            if not split:
                raise ValueError("partition_split must be provided when partition_mode is enabled")
        return partitions


class RoutingRule(BaseModel):
    """Routing rule configuration"""
    strategy_id: str
    to_accounts: List[str]
    allow_market: bool = True
    allow_limit: bool = True


class ADVCapConfig(BaseModel):
    """ADV Cap (Slip Guard) configuration"""
    enabled: bool = True
    cap_pct: float = 1.0
    lookback_days: int = 7
    min_volume_usd: float = 100000


class MACDFiltersConfig(BaseModel):
    """MACD Entry Filters configuration (Dec 2025)"""
    enabled: bool = True
    long_block_macd_below: float = 0.0  # Block LONG if MACD < this value
    short_block_macd_below: float = -20.0  # Block SHORT if MACD < this value


class RiskManagementConfig(BaseModel):
    """Global risk management configuration"""
    max_total_positions: int = 10
    max_leverage_global: int = 5
    enable_trading: bool = True
    max_commission_rate_pct: float = 0.04
    adv_cap: ADVCapConfig
    macd_filters: Optional[MACDFiltersConfig] = None


class TelegramConfig(BaseModel):
    """Telegram notifications configuration"""
    enabled: bool = False
    bot_token: Optional[str] = None
    chat_id: Optional[str] = None


class DeploymentConfig(BaseModel):
    """Deployment configuration"""
    environment: str = "production"
    gcp_project_id: Optional[str] = None
    region: str = "us-central1"
    webhook_secret: str = Field(..., description="Webhook authentication secret")
    admin_secret: str = Field(..., description="Admin endpoints secret")
    allowed_ips: List[str] = []


class ContractExpiryInfo(BaseModel):
    """Contract expiry information"""
    expiry_date: Optional[str] = None
    warning_days: List[int] = [30, 10, 3]
    block_entry_days: int = 10


class ContractExpiryConfig(BaseModel):
    """Contract expiry monitoring configuration"""
    contracts: Dict[str, ContractExpiryInfo]


class MonitoringConfig(BaseModel):
    """Monitoring and logging configuration"""
    use_firestore: bool = True
    firestore_collection: str = "tradingview_trades"
    log_level: str = "INFO"


class GeneralConfig(BaseModel):
    """General application configuration"""
    local_timezone: str = Field(
        "America/Los_Angeles",
        description="IANA timezone used for local display and scheduling"
    )
    capital_allocation_pct: float = Field(
        90.0,
        description="Percentage of equity allocated per trade (e.g., 90.0 = 90%)"
    )
    max_position_allocation_pct: float = Field(
        100.0,
        description="Maximum percentage of the capital allocation allowed per individual signal"
    )
    min_contract_buffer_pct: float = Field(
        30.0,
        description="Additional percentage buffer applied to minimum contract wallet calculations"
    )
    disable_entries_fri_sun: bool = Field(
        True,
        description="Disable new entries Friday-Sunday (exits always allowed). Enables scale-to-zero optimization."
    )
    demo_starting_balance: float = Field(
        2000.0,
        description="Starting balance for DEMO mode accounts (in USD). Used for initializing demo accounts and fallback values throughout the Agent."
    )
    # Holiday guard defaults (can be overridden in CalendarConfig below)
    # Kept here for backward compatibility; canonical shape is CalendarConfig.

    @validator('local_timezone')
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except Exception as exc:
            raise ValueError(f"Invalid timezone '{value}': {exc}") from exc
        return value

    @validator('capital_allocation_pct')
    def validate_capital_pct(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("capital_allocation_pct must be greater than 0")
        if value > 100 and value > 1:
            raise ValueError("capital_allocation_pct should be <= 100 when expressed in percent")
        return value

    @validator('max_position_allocation_pct')
    def validate_max_position_pct(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("max_position_allocation_pct must be greater than 0")
        if value > 100 and value > 1:
            raise ValueError("max_position_allocation_pct should be <= 100 when expressed in percent")
        return value

    @validator('min_contract_buffer_pct')
    def validate_min_contract_buffer(cls, value: float) -> float:
        if value < 0:
            raise ValueError("min_contract_buffer_pct must be zero or positive")
        return value


class AppConfig(BaseModel):
    """Complete application configuration"""
    version: str
    strategies: List[StrategyConfig]
    accounts: List[AccountConfig]
    routing: Optional[List[RoutingRule]] = []
    risk_management: RiskManagementConfig
    telegram: TelegramConfig
    deployment: DeploymentConfig
    monitoring: MonitoringConfig
    contract_expiry: Optional[ContractExpiryConfig] = None
    general: GeneralConfig = GeneralConfig()
    calendar: Optional["CalendarConfig"] = None
    coinbase_futures: Optional["CoinbaseFuturesConfig"] = None


class CalendarConfig(BaseModel):
    """Holiday and trading-window calendar configuration"""
    enabled: bool = True
    timezone: str = Field("America/New_York", description="IANA timezone for holiday window")
    pre_holiday_block_hours: int = Field(24, ge=0, le=72, description="Hours before holiday to block new entries")
    post_holiday_block_hours: int = Field(24, ge=0, le=72, description="Hours after holiday to continue blocking entries")
    holidays: List[str] = Field(default_factory=list, description="List of holiday dates in YYYY-MM-DD (local TZ)")


# ============================================
# YAML LOADER WITH SECRET INJECTION
# ============================================

def inject_env_vars(obj: Any) -> Any:
    """
    Recursively inject environment variables into config
    
    Replaces ${ENV_VAR} with os.getenv('ENV_VAR')
    """
    if isinstance(obj, dict):
        return {k: inject_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [inject_env_vars(item) for item in obj]
    elif isinstance(obj, str):
        # Match ${ENV_VAR} pattern
        pattern = r'\$\{([A-Z_][A-Z0-9_]*)\}'
        
        def replace_var(match):
            env_var = match.group(1)
            value = os.getenv(env_var)
            if value is None:
                logger.warning(f"Environment variable {env_var} not set!")
                return ""
            return value
        
        return re.sub(pattern, replace_var, obj)
    return obj


def load_yaml_config(config_path: str = "config.yaml") -> AppConfig:
    """
    Load YAML configuration and inject secrets from environment
    
    Args:
        config_path: Path to config.yaml file
    
    Returns:
        Validated AppConfig object
    
    Raises:
        FileNotFoundError: If config.yaml not found
        ValueError: If validation fails
    """
    # Find config file
    config_file = Path(config_path)
    
    if not config_file.exists():
        # Try relative to this file
        config_file = Path(__file__).parent.parent / config_path
    
    if not config_file.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            f"Looked in: {config_file}"
        )
    
    logger.info(f"Loading configuration from {config_file}")
    
    # Load YAML
    try:
        with open(config_file, 'r') as f:
            raw_config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML syntax: {str(e)}")
    
    # Inject environment variables
    config_with_secrets = inject_env_vars(raw_config)
    
    # Validate with Pydantic
    try:
        config = AppConfig(**config_with_secrets)
    except Exception as e:
        raise ValueError(f"Configuration validation failed: {str(e)}")
    
    # Log configuration summary
    logger.info("✅ Configuration loaded successfully!")
    logger.info(f"   Strategies: {len(config.strategies)}")
    logger.info(f"   Accounts: {len(config.accounts)} ({len([a for a in config.accounts if a.enabled])} enabled)")
    logger.info(f"   Environment: {config.deployment.environment}")
    
    # Log strategy → account mapping
    for strategy in config.strategies:
        if not strategy.enabled:
            continue
        
        listening_accounts = [
            a for a in config.accounts 
            if a.enabled and strategy.id in a.strategy_ids
        ]
        
        logger.info(f"   Strategy '{strategy.id}':")
        for account in listening_accounts:
            logger.info(f"     → {account.label} ({account.mode}, {account.leverage}x)")
    
    # Validate references
    _validate_config_references(config)
    
    return config


def _validate_config_references(config: AppConfig):
    """
    Validate that all references in config are correct
    
    - All strategy_ids in accounts exist in strategies
    - All routing rules reference valid strategies/accounts
    - No duplicate IDs
    """
    # Get all strategy IDs
    strategy_ids = {s.id for s in config.strategies}
    account_ids = {a.id for a in config.accounts}
    
    # Check for duplicates
    if len(strategy_ids) != len(config.strategies):
        raise ValueError("Duplicate strategy IDs found!")
    
    if len(account_ids) != len(config.accounts):
        raise ValueError("Duplicate account IDs found!")
    
    # Validate account strategy_ids
    for account in config.accounts:
        for strategy_id in account.strategy_ids:
            if strategy_id not in strategy_ids:
                raise ValueError(
                    f"Account '{account.id}' references unknown strategy '{strategy_id}'"
                )
    
    # Validate routing rules
    for rule in config.routing:
        if rule.strategy_id not in strategy_ids:
            raise ValueError(
                f"Routing rule references unknown strategy '{rule.strategy_id}'"
            )
        
        for account_id in rule.to_accounts:
            if account_id not in account_ids:
                raise ValueError(
                    f"Routing rule references unknown account '{account_id}'"
                )
    
    logger.info("✅ Configuration references validated")


# ============================================
# HELPER FUNCTIONS
# ============================================

def get_strategy_by_id(config: AppConfig, strategy_id: str) -> Optional[StrategyConfig]:
    """Get strategy configuration by ID"""
    for strategy in config.strategies:
        if strategy.id == strategy_id:
            return strategy
    return None


def get_account_by_id(config: AppConfig, account_id: str) -> Optional[AccountConfig]:
    """Get account configuration by ID"""
    for account in config.accounts:
        if account.id == account_id:
            return account
    return None


def get_accounts_for_strategy(config: AppConfig, strategy_id: str) -> List[AccountConfig]:
    """
    Get all enabled accounts listening to a specific strategy
    
    Args:
        config: AppConfig object
        strategy_id: Strategy ID
    
    Returns:
        List of enabled AccountConfig objects
    """
    accounts = []
    
    for account in config.accounts:
        # Skip disabled accounts
        if not account.enabled:
            continue
        
        # Check if account listens to this strategy
        if strategy_id in account.strategy_ids:
            accounts.append(account)
    
    # Apply routing rules (if any)
    routing_rule = get_routing_rule(config, strategy_id)
    if routing_rule:
        # Filter to only allowed accounts
        accounts = [a for a in accounts if a.id in routing_rule.to_accounts]
    
    return accounts


def get_routing_rule(config: AppConfig, strategy_id: str) -> Optional[RoutingRule]:
    """Get routing rule for a strategy (if any)"""
    for rule in config.routing:
        if rule.strategy_id == strategy_id:
            return rule
    return None


def get_enabled_strategies(config: AppConfig) -> List[StrategyConfig]:
    """Get all enabled strategies"""
    return [s for s in config.strategies if s.enabled]


def get_enabled_accounts(config: AppConfig) -> List[AccountConfig]:
    """Get all enabled accounts"""
    return [a for a in config.accounts if a.enabled]


# ============================================
# GLOBAL CONFIG INSTANCE
# ============================================

_config_instance: Optional[AppConfig] = None


def get_config(reload: bool = False) -> AppConfig:
    """
    Get global configuration instance (singleton pattern)
    
    Args:
        reload: Force reload from file
    
    Returns:
        AppConfig object
    """
    global _config_instance
    
    if _config_instance is None or reload:
        _config_instance = load_yaml_config()
    
    return _config_instance


# ============================================
# BACKWARD COMPATIBILITY
# ============================================

def get_all_configured_accounts() -> Dict[int, Dict]:
    """
    Get all configured accounts (backward compatible format)
    
    Returns dict keyed by account number (1-10)
    """
    config = get_config()
    result = {}
    
    for idx, account in enumerate(config.accounts, start=1):
        if not account.enabled:
            continue
        
        result[idx] = {
            'account_number': idx,
            'enabled': account.enabled,
            'mode': account.mode,
            'strategy': account.strategy_ids[0] if account.strategy_ids else None,  # First strategy
            'broker': account.broker,
            'api_key': account.api_key,
            'secret': account.api_secret,
            'leverage': account.leverage,
            'label': account.label
        }
    
    return result


def get_accounts_for_strategy_compat(strategy_number: int) -> List[Dict]:
    """
    Get accounts for strategy (backward compatible)
    
    Args:
        strategy_number: Strategy number (1-5) - maps to strategy by index
    
    Returns:
        List of account config dicts
    """
    config = get_config()
    
    # Get strategy by index (1-based)
    if strategy_number < 1 or strategy_number > len(config.strategies):
        logger.error(f"Invalid strategy_number: {strategy_number}")
        return []
    
    strategy = config.strategies[strategy_number - 1]
    accounts = get_accounts_for_strategy(config, strategy.id)
    
    # Convert to old format
    result = []
    for idx, account in enumerate(accounts, start=1):
        result.append({
            'account_number': idx,
            'enabled': account.enabled,
            'mode': account.mode,
            'broker': account.broker,
            'api_key': account.api_key,
            'secret': account.api_secret,
            'leverage': account.leverage,
            'label': account.label
        })
    
    return result

