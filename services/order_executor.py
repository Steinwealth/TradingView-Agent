"""
TradingView Agent - Order Executor Service (V3.5)
Routes TradingView strategy signals to configured broker accounts

V3.5 ARCHITECTURE:
- Declarative YAML configuration (config.yaml)
- Unlimited strategies (defined in YAML)
- Unlimited accounts (defined in YAML)
- Strategy ID → Account routing
- Each account: enabled, mode (LIVE/DEMO), broker, api_key, leverage
- Executes LONG/SHORT/EXIT signals only
- Optional SL/TP if provided in alert
"""

import logging
import asyncio
import math
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set, Any, Callable, Awaitable
from decimal import Decimal, ROUND_DOWN

from models.webhook_payload import WebhookPayload, OrderResponse, PositionInfo
from brokers.binance_futures import create_binance_futures_trader
from brokers.coinbase_futures import create_coinbase_futures_trader
from services.position_sizer import PositionSizer
from services.telegram_notifier import telegram_notifier
# format_price imported locally in functions to avoid circular import
from services import partition_manager
from services.leverage_manager import leverage_manager
from services.partition_sync_scheduler import sync_scheduler
from services.state_store import state_store
from config.contract_expiry import should_allow_entry, should_allow_exit, check_contract_status
from config.yaml_config import (
    get_config, 
    get_strategy_by_id,
    get_accounts_for_strategy,
    get_enabled_accounts
)

logger = logging.getLogger(__name__)


class BrokerRetryRequired(Exception):
    """Internal helper exception to trigger broker retry logic."""
    pass


class OrderExecutor:
    """
    Executes TradingView strategy signals on configured broker accounts
    
    Flow:
    1. Receive webhook with strategy_id
    2. Get all accounts listening to that strategy
    3. Execute signal on each ENABLED account
    4. Handle DEMO vs LIVE mode per account
    5. Retry failed orders
    6. Track results
    """
    
    def __init__(self, settings, risk_manager=None):
        self.settings = settings
        self.risk_manager = risk_manager
        self.position_sizer = PositionSizer(settings)
        
        # Load YAML config
        self.config = get_config()
        
        # Coinbase Futures settings (for position sizing optimization)
        self.coinbase_futures_config = getattr(self.config, 'coinbase_futures', None)
        self.account_configs: Dict[str, Any] = {}

        # Store initialized broker instances per account
        self.account_brokers = {}  # {account_id: broker_instance}
        
        # DEMO mode tracking (virtual positions for DEMO accounts)
        self.demo_positions = {}  # {account_id: {symbol: position_data}}
        
        # DEMO ACCOUNT SYSTEM - Complete tracking (V3.5)
        self.demo_accounts = {}  # {account_id: account state}
        self.demo_trade_history = {}  # {account_id: [list of completed trades]}

        # Partition management
        self.partition_initialized: Set[str] = set()
        self.partition_init_lock = asyncio.Lock()
        self._price_cache: Dict[str, Tuple[float, float]] = {}
        self._commission_cap_state: Dict[str, Dict[str, Any]] = {}
        
        logger.info("OrderExecutor initialized (V3.5 YAML Config)")
        
        # Initialize all configured accounts
        self._initialize_accounts()

    def _get_margin_rate_period(self, strategy_id: str, entry_time_et: Optional[datetime] = None) -> str:
        """
        Determine which margin rate period to use based on Coinbase Futures settings and entry time.
        
        Args:
            strategy_id: Strategy ID (e.g., 'ichimoku-coinbase-eth-5m')
            entry_time_et: Entry time in ET timezone (defaults to now if None)
        
        Returns:
            'intraday' or 'overnight' (afterhours)
        """
        from datetime import datetime
        from zoneinfo import ZoneInfo
        
        # Check if this is a Coinbase Futures strategy
        is_coinbase = 'coinbase' in strategy_id.lower()
        
        # If not Coinbase Futures or settings not configured, use afterhours (conservative)
        if not is_coinbase or not self.coinbase_futures_config:
            return 'overnight'
        
        # If all_trades_size_to_afterhours is true, always use afterhours rates
        if self.coinbase_futures_config.all_trades_size_to_afterhours:
            return 'overnight'
        
        # If size_trades_with_intraday_rates_6p_10p is true, use time-based selection
        if self.coinbase_futures_config.size_trades_with_intraday_rates_6p_10p:
            # Get entry time (ET)
            if entry_time_et is None:
                et_tz = ZoneInfo('America/New_York')
                entry_time_et = datetime.now(et_tz)
            
            entry_hour = entry_time_et.hour
            entry_minute = entry_time_et.minute
            
            # Intraday hours: 9:30 AM ET - 4:00 PM ET (market hours)
            # Afterhours: 4:00 PM ET - 9:30 AM ET next day
            # Note: 6:01PM-10:00PM ET is a special safe window for intraday rates
            # but we should use intraday rates for all market hours (9:30 AM - 4:00 PM ET)
            
            # Market hours: 9:30 AM ET (09:30) to 4:00 PM ET (16:00)
            if entry_hour == 9 and entry_minute >= 30:  # 9:30 AM - 9:59 AM ET
                return 'intraday'
            elif 10 <= entry_hour < 16:  # 10:00 AM - 3:59 PM ET
                return 'intraday'
            elif entry_hour == 16 and entry_minute == 0:  # 4:00 PM ET exactly
                return 'intraday'
            # Special safe window: 6:01PM-10:00PM ET (18:01-22:00) = intraday rates
            elif entry_hour == 18 and entry_minute >= 1:  # 6:01PM-6:59PM ET
                return 'intraday'
            elif 19 <= entry_hour < 22:  # 7:00PM-9:59PM ET
                return 'intraday'
            elif entry_hour == 22 and entry_minute == 0:  # 10:00PM ET exactly
                return 'intraday'
            else:
                # All other times (4:01 PM - 9:29 AM ET, 10:01 PM - 6:00 PM ET) = afterhours rates
                return 'overnight'
        
        # Default: use afterhours rates (conservative)
        return 'overnight'

    @staticmethod
    def _round_quantity_step(quantity: float, round_step: Optional[float]) -> float:
        """Round quantity down to the nearest valid increment."""
        if quantity is None:
            return 0.0
        if not round_step or round_step <= 0:
            return float(quantity)
        try:
            steps = math.floor(quantity / round_step)
            return round(steps * round_step, 8)
        except Exception:
            return float(quantity)

    @staticmethod
    def _compute_min_wallet_requirement(
        *,
        min_qty: float,
        price: float,
        leverage: int,
        capital_pct: float,
        allocation_pct: float,
        buffer_pct: float = 0.0,
        margin_rate: Optional[float] = None
    ) -> Optional[Dict[str, float]]:
        """
        Estimate the minimum wallet size needed to satisfy the broker's min contract.
        
        Args:
            margin_rate: Optional margin rate (as decimal, e.g., 0.10 for 10%).
                       If provided, uses this for accurate margin calculation.
                       If None, uses simplified calculation based on leverage.
        """
        if min_qty <= 0 or price <= 0:
            return None
        leverage = leverage or 1
        capital_fraction = (capital_pct / 100.0) if capital_pct else 0.0
        allocation_fraction = (allocation_pct / 100.0) if allocation_pct else 0.0
        if capital_fraction <= 0:
            return None
        if allocation_fraction <= 0:
            allocation_fraction = 1.0
        
        buffer_multiplier = 1.0 + (buffer_pct / 100.0) if buffer_pct and buffer_pct > 0 else 1.0

        required_notional = float(min_qty) * float(price)
        buffered_notional = required_notional * buffer_multiplier
        
        # Use provided margin rate if available (more accurate)
        # Otherwise use simplified calculation: margin = notional / leverage
        if margin_rate is not None and margin_rate > 0:
            # Accurate margin calculation using API margin rate
            # margin = (notional * margin_rate) / leverage
            required_margin = (buffered_notional * margin_rate) / leverage if leverage > 0 else buffered_notional * margin_rate
        else:
            # Simplified calculation (fallback)
            required_margin = buffered_notional / leverage if leverage > 0 else buffered_notional
        partition_balance = required_margin / capital_fraction
        wallet_balance = partition_balance / allocation_fraction

        return {
            "notional_usd": required_notional,
            "buffered_notional_usd": buffered_notional,
            "margin_usd": required_margin,
            "partition_balance_usd": partition_balance,
            "wallet_balance_usd": wallet_balance,
            "buffer_pct": buffer_pct
        }
    
    def _initialize_demo_account(self, account_id: str, label: str, reset: bool = False):
        """Initialize or restore DEMO account state."""
        if account_id in self.demo_accounts and not reset:
            return
        
        if not reset:
            restored = state_store.load_demo_account_sync(account_id)
            if restored and restored.get('account'):
                self.demo_accounts[account_id] = restored['account']
                self.demo_positions[account_id] = restored.get('positions', {})
                self.demo_trade_history[account_id] = restored.get('history', [])
                current_balance = self.demo_accounts[account_id].get('current_balance', 0.0)
                logger.info(f"    ♻️ DEMO Account restored: ${current_balance:,.2f}")
                return
        
        # Initialize demo account with partition system sync
        # Starting balance from config.yaml (configurable in one place)
        starting_balance = self.settings.DEMO_STARTING_BALANCE
        current_balance = self.settings.DEMO_STARTING_BALANCE
        total_pnl = 0.0
        
        # CRITICAL: Sync with partition system if it exists
        try:
            from services.partition_manager import partition_state
            
            state = partition_state.get(account_id)
            if state:
                # Calculate current balance from partition totals
                partitions = state.get('partitions', {})
                partition_total = sum(
                    partition_data.get('virtual_balance', 0.0) 
                    for partition_data in partitions.values()
                )
                
                if partition_total > 0:
                    current_balance = partition_total
                    # If syncing from partitions, preserve the starting_balance from demo account if available
                    # Otherwise, use current balance as starting (for fresh initialization)
                    # The starting_balance will be correct if restored from Firestore above
                    # For new accounts syncing from partitions, assume starting = current (no P&L yet)
                    if partition_total >= starting_balance:
                        # If partitions show higher balance, this is from accumulated P&L
                        # Keep default starting_balance, calculate P&L
                        total_pnl = current_balance - starting_balance
                    else:
                        # If partitions show lower balance, they might be using different starting
                        # Use partition total as both starting and current (fresh state)
                        starting_balance = partition_total
                        current_balance = partition_total
                        total_pnl = 0.0
                    logger.info(f"    🔄 Synced demo account with partition system: ${current_balance:,.2f} (starting: ${starting_balance:,.2f})")
        except Exception as e:
            logger.debug(f"Could not sync demo account with partitions during init: {e}")
        
        self.demo_accounts[account_id] = {
            'account_id': account_id,
            'label': label,
            'starting_balance': starting_balance,
            'current_balance': current_balance,
            'peak_balance': max(current_balance, starting_balance),
            'total_pnl': total_pnl,
            'total_pnl_pct': (total_pnl / starting_balance * 100) if starting_balance > 0 else 0.0,
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'largest_win': 0.0,
            'largest_loss': 0.0,
            'current_streak': 0,
            'max_win_streak': 0,
            'max_loss_streak': 0,
            'max_drawdown_usd': 0.0,
            'max_drawdown_pct': 0.0,
            'total_commission_paid': 0.0,
            'commission_rate': 0.0002,
            'created_at': datetime.utcnow().isoformat(),
            'last_updated': datetime.utcnow().isoformat()
        }
        self.demo_trade_history[account_id] = []
        self.demo_positions[account_id] = {}
        logger.info(f"    💰 DEMO Account initialized: ${self.demo_accounts[account_id]['current_balance']:,.2f}")
        logger.info(f"    📊 Commission rate: 0.02% (matches Coinbase LIVE)")
        state_store.save_demo_account_sync(
            account_id,
            self.demo_accounts[account_id],
            self.demo_positions[account_id],
            self.demo_trade_history[account_id]
        )
    
    def _initialize_accounts(self):
        """Initialize broker connections for all configured accounts"""
        enabled_accounts = get_enabled_accounts(self.config)
        
        if not enabled_accounts:
            logger.warning("❌ No accounts enabled! Check config.yaml")
            return
        
        logger.info(f"Initializing {len(enabled_accounts)} enabled accounts...")
        
        for account in enabled_accounts:
            try:
                account_id = account.id
                broker_type = account.broker
                mode = account.mode
                leverage = account.leverage
                label = account.label
                
                logger.info(f"  Account '{account_id}': {label}")
                logger.info(f"    Broker: {broker_type}")
                logger.info(f"    Strategies: {', '.join(account.strategy_ids)}")
                logger.info(f"    Leverage: {leverage}x")
                logger.info(f"    Mode: {mode}")

                self.account_configs[account_id] = account
                
                # Create broker instance based on type
                if broker_type == "coinbase_futures":
                    broker = create_coinbase_futures_trader(
                        api_key=account.api_key,
                        api_secret=account.api_secret,
                        leverage=leverage,
                        sandbox=False,
                        demo_mode=(mode == "DEMO"),
                        portfolio_id=getattr(account, 'portfolio_id', None)
                    )
                    self.account_brokers[account_id] = broker
                    logger.info(f"    ✅ Coinbase Futures initialized")
                
                elif broker_type == "binance_futures":
                    broker = create_binance_futures_trader(
                        api_key=account.api_key,
                        api_secret=account.api_secret,
                        leverage=leverage,
                        testnet=False,
                        demo_mode=(mode == "DEMO")
                    )
                    self.account_brokers[account_id] = broker
                    logger.info(f"    ✅ Binance Futures initialized")
                
                elif broker_type == "kraken_futures":
                    logger.warning(f"    ⚠️ Kraken not implemented yet")
                
                elif broker_type == "oanda":
                    logger.warning(f"    ⚠️ OANDA not implemented yet")
                
                else:
                    logger.error(f"    ❌ Unknown broker type: {broker_type}")
                
                # Initialize DEMO account tracking if in DEMO mode
                if mode == "DEMO":
                    self._initialize_demo_account(account_id, label)
                    logger.info(f"    📊 DEMO mode: Complete P&L tracking enabled")
            
            except Exception as e:
                logger.error(f"  Account '{account.id}': Failed to initialize - {str(e)}")
        
        logger.info(f"Account initialization complete: {len(self.account_brokers)} brokers ready")
    
    async def execute(self, payload: WebhookPayload) -> OrderResponse:
        """
        Execute TradingView signal on all accounts listening to this strategy
        
        Args:
            payload: Validated webhook payload (V3.5)
        
        Returns:
            OrderResponse with execution results
        """
        strategy_id = payload.strategy_id
        action = payload.side.upper()
        
        # Get strategy from config
        strategy = get_strategy_by_id(self.config, strategy_id)
        if not strategy:
            logger.error(f"❌ Unknown strategy: {strategy_id}")
            return OrderResponse(
                status="rejected",
                client_order_id=payload.get_client_order_id(),
                symbol="unknown",
                side=payload.get_side(),
                quantity=0,
                timestamp=datetime.utcnow().isoformat(),
                broker="none",
                strategy=strategy_id,
                error=f"Unknown strategy: {strategy_id}"
            )
        
        # Check if strategy is enabled
        if not strategy.enabled:
            logger.warning(f"⚠️ Strategy '{strategy_id}' is DISABLED in config.yaml")
            return OrderResponse(
                status="rejected",
                client_order_id=payload.get_client_order_id(),
                symbol=strategy.execution.product_id,
                side=payload.get_side(),
                quantity=0,
                timestamp=datetime.utcnow().isoformat(),
                broker="none",
                strategy=strategy.name,
                error=f"Strategy '{strategy_id}' is disabled"
            )
        
        # Get symbol from strategy config (more reliable than payload)
        symbol = strategy.execution.product_id
        
        logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info(f"📨 SIGNAL RECEIVED: {strategy.name} | {action} {symbol}")
        logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        # Get all accounts listening to this strategy
        accounts = get_accounts_for_strategy(self.config, strategy_id)
        
        if not accounts:
            logger.error(f"❌ No accounts configured for strategy '{strategy_id}'")
            return OrderResponse(
                status="rejected",
                client_order_id=payload.get_client_order_id(),
                symbol=symbol,
                side=payload.get_side(),
                quantity=0,
                timestamp=datetime.utcnow().isoformat(),
                broker="none",
                strategy=strategy.name,
                error=f"No accounts configured for strategy '{strategy_id}'"
            )
        
        logger.info(f"🎯 Routing to {len(accounts)} account(s):")
        for account in accounts:
            logger.info(f"  • {account.label} ({account.mode}, {account.leverage}x)")
        
        # Execute on all accounts
        results = []
        for account in accounts:
            try:
                result = await self._execute_on_account(payload, account, strategy)
                results.append(result)
            except Exception as e:
                error_str = str(e)
                logger.error(f"❌ {account.label} execution failed: {error_str}", exc_info=True)
                
                # Only send Telegram alert for unexpected/critical failures
                # Skip alerting for known/expected error patterns
                known_errors = [
                    "validation error",  # Pydantic validation (should be fixed now)
                    "No partition positions",
                    "insufficient available capital",
                    "holiday_block_window",
                ]
                is_known_error = any(known.lower() in error_str.lower() for known in known_errors)
                
                if not is_known_error:
                    try:
                        await telegram_notifier.send_critical_failure(
                            f"🚨 ACCOUNT EXECUTION FAILED\n\n"
                            f"Account: {account.label}\n"
                            f"Strategy: {strategy.name}\n"
                            f"Symbol: {symbol}\n"
                            f"Action: {payload.side.upper()}\n"
                            f"Error: {error_str}\n\n"
                            f"⚠️ Check Cloud Run logs for full details."
                        )
                    except Exception as telegram_error:
                        logger.error(f"Failed to send Telegram alert: {str(telegram_error)}")
                
                results.append(OrderResponse(
                    status="failed",
                    client_order_id=payload.get_client_order_id(),
                    symbol=symbol,
                    side=payload.get_side(),
                    quantity=0,
                    timestamp=datetime.utcnow().isoformat(),
                    broker=account.broker,
                    strategy=strategy.name,
                    error=error_str
                ))
        
        # Return first successful result (or first result if all failed)
        successful = [r for r in results if r.status == "success"]
        return successful[0] if successful else results[0]
    
    def _partition_mode_enabled(self, account) -> bool:
        return getattr(account, "partition_mode", "disabled") == "enabled"

    async def _ensure_partitions_initialized(self, account, broker) -> None:
        if not self._partition_mode_enabled(account):
            return

        # Check if partitions are already initialized
        if account.id in self.partition_initialized:
            # Check for mode mismatch - if mode changed, we need to reinitialize
            current_mode = getattr(account, 'mode', 'LIVE').upper()
            state = partition_manager.partition_state.get(account.id)
            if state:
                cached_mode = state.get('meta', {}).get('mode', '').upper()
                if cached_mode and cached_mode != current_mode:
                    logger.warning(
                        f"⚠️ Mode mismatch detected for {account.id}: "
                        f"cache is {cached_mode}, current is {current_mode}. "
                        f"Reinitializing partitions with correct balance source."
                    )
                    # Clear initialized flag to force reinitialization
                    self.partition_initialized.discard(account.id)
                else:
                    # Partitions already initialized and mode matches - skip
                    return
            else:
                # No state found - skip (shouldn't happen, but safe)
                return

        async with self.partition_init_lock:
            # Double-check after acquiring lock
            if account.id in self.partition_initialized:
                current_mode = getattr(account, 'mode', 'LIVE').upper()
                state = partition_manager.partition_state.get(account.id)
                if state:
                    cached_mode = state.get('meta', {}).get('mode', '').upper()
                    if cached_mode and cached_mode == current_mode:
                        return

            logger.info(f"🧩 Initializing virtual partitions for account {account.id}")
            await partition_manager.initialize_partitions(account.id, account, broker)
            self.partition_initialized.add(account.id)

    async def warm_partitions(self) -> None:
        """
        Ensure all partition-enabled accounts restore their state at startup.
        CRITICAL: Checks for mode mismatches and forces reinitialization if needed.
        """
        for account_id, account in self.account_configs.items():
            if not self._partition_mode_enabled(account):
                continue
            broker = self.account_brokers.get(account_id)
            if not broker:
                logger.warning(f"⚠️ No broker instance available for {account_id} during warm-up")
                continue
            try:
                # CRITICAL: Always check for mode mismatch on startup
                # Load state from Firestore to check cached mode
                current_mode = getattr(account, 'mode', 'LIVE').upper()
                from services.state_store import state_store
                restored_state = await state_store.load_partition_state(account_id)
                
                if restored_state:
                    cached_mode = restored_state.get('meta', {}).get('mode', '').upper()
                    if cached_mode and cached_mode != current_mode:
                        logger.warning(
                            f"⚠️ Mode mismatch detected on startup for {account_id}: "
                            f"Firestore cache is {cached_mode}, current mode is {current_mode}. "
                            f"Clearing initialized flag to force reinitialization with correct balance."
                        )
                        # Clear initialized flag to force reinitialization with correct balance
                        self.partition_initialized.discard(account_id)
                
                # Initialize partitions (will detect mode mismatch and use correct balance)
                await self._ensure_partitions_initialized(account, broker)
                
                # For LIVE mode, refresh balance from broker
                if getattr(account, "mode", "").upper() == "LIVE":
                    await self._refresh_live_account_balance(account_id, broker)
            except Exception as exc:
                logger.warning(f"⚠️ Failed to warm partitions for {account_id}: {exc}")

    async def _refresh_live_account_balance(self, account_id: str, broker) -> None:
        """
        Refresh real-account balances for live accounts so cooperative partitions
        reflect the actual wallet value at startup.
        """
        get_balance = getattr(broker, "get_account_balance", None)
        if not callable(get_balance):
            return
        try:
            balance = await asyncio.to_thread(get_balance)
        except Exception as exc:
            logger.debug(f"Unable to fetch live balance for {account_id}: {exc}")
            return

        total_wallet = balance.get("total_wallet_balance")
        available = balance.get("available_balance")
        margin_balance = balance.get("total_margin_balance")

        if total_wallet is None and margin_balance is not None:
            total_wallet = margin_balance
        if total_wallet is None:
            return

        cash_in_positions = total_wallet - (available or 0.0)
        await partition_manager.update_real_account_balances(
            account_id,
            total_balance=total_wallet,
            available_cash=available,
            cash_in_positions=cash_in_positions
        )

    def _get_partitions_for_strategy(self, account_id: str, strategy_id: str) -> List[str]:
        state = partition_manager.partition_state.get(account_id)
        if not state:
            return []

        style = partition_manager.get_partition_style(account_id)
        partitions = []
        for partition_id, partition in state['partitions'].items():
            if not partition.get('enabled', True):
                continue
            if style == "cooperative":
                partitions.append(partition_id)
            elif strategy_id in partition.get('strategy_ids', []):
                partitions.append(partition_id)
        return partitions

    def _get_cooperative_partition_order(self, account_id: str, partition_ids: List[str]) -> List[str]:
        """
        For cooperative style accounts, provide partitions in FIFO order filtered
        to ones without open positions.
        """
        state = partition_manager.partition_state.get(account_id)
        if not state:
            return partition_ids

        partitions = state.get('partitions', {})
        meta = state.setdefault('meta', {})
        rotation = meta.get('coop_rotation')

        if not rotation or set(rotation) != set(partitions.keys()):
            rotation = list(partitions.keys())
            meta['coop_rotation'] = rotation

        available: List[str] = []
        total = len(rotation)
        if total == 0:
            return available

        start_idx = meta.get('coop_next_index', 0) % total

        for offset in range(total):
            idx = (start_idx + offset) % total
            partition_id = rotation[idx]
            if partition_id not in partition_ids:
                continue
            partition = partitions.get(partition_id)
            if not partition or not partition.get('enabled', True):
                continue
            open_positions = partition.get('open_positions') or {}
            if open_positions:
                continue
            available.append(partition_id)

        return available

    def _find_opposite_positions(
        self, 
        account_id: str, 
        symbol: str, 
        new_direction: str
    ) -> List[Dict[str, Any]]:
        """
        Find all partitions with positions in the opposite direction for a given symbol.
        
        Args:
            account_id: The account to check
            symbol: The symbol to check (e.g., 'ETP-CBSE', 'ETH-PERP')
            new_direction: The direction of the new signal ('LONG' or 'SHORT')
        
        Returns:
            List of dicts with partition_id, position info for opposite positions
        """
        opposite_direction = 'SHORT' if new_direction == 'LONG' else 'LONG'
        opposite_positions = []
        
        state = partition_manager.partition_state.get(account_id)
        if not state:
            return opposite_positions
        
        partitions = state.get('partitions', {})
        
        # Normalize symbol for comparison (handle different formats)
        symbol_normalized = symbol.upper().replace('-PERP', '').replace('-CBSE', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
        
        for partition_id, partition in partitions.items():
            if not partition.get('enabled', True):
                continue
                
            open_positions = partition.get('open_positions') or {}
            
            for pos_symbol, position in open_positions.items():
                if not isinstance(position, dict):
                    continue
                
                # Normalize position symbol for comparison
                pos_symbol_normalized = pos_symbol.upper().replace('-PERP', '').replace('-CBSE', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
                
                # Check if it's the same underlying symbol
                if pos_symbol_normalized != symbol_normalized:
                    continue
                
                # Check if it's in the opposite direction
                position_side = position.get('side', '').upper()
                if position_side == opposite_direction:
                    opposite_positions.append({
                        'partition_id': partition_id,
                        'symbol': pos_symbol,
                        'side': position_side,
                        'quantity': position.get('quantity', 0),
                        'entry_price': position.get('entry_price', 0),
                        'cash_used': position.get('cash_used', 0)
                    })
                    from services.exit_monitor import format_price
                    logger.info(
                        f"🔄 Found opposite {position_side} position in {partition_id}: "
                        f"{position.get('quantity', 0)} {pos_symbol} @ {format_price(position.get('entry_price', 0), pos_symbol)}"
                    )
        
        return opposite_positions

    def _advance_cooperative_pointer(self, account_id: str, partition_id: str) -> None:
        """
        Advance the cooperative FIFO pointer after allocating a partition.
        """
        state = partition_manager.partition_state.get(account_id)
        if not state:
            return

        meta = state.setdefault('meta', {})
        rotation = meta.get('coop_rotation') or list(state.get('partitions', {}).keys())
        if not rotation:
            return

        if partition_id in rotation:
            idx = rotation.index(partition_id)
            meta['coop_next_index'] = (idx + 1) % len(rotation)

    def _commission_cap_allows_entry(self, account, *, symbol: str, strategy_name: Optional[str]) -> Tuple[bool, Optional[str]]:
        max_cap = getattr(self.settings, "MAX_COMMISSION_RATE_PCT", None)
        if not max_cap or max_cap <= 0:
            return True, None

        state = self._commission_cap_state.get(account.id)
        if not state or not state.get('blocked'):
            return True, None

        last_pct = state.get('last_rate_pct')
        if last_pct is None:
            return True, None

        reason = (
            f"Commission cap active ({last_pct:.4f}% > {max_cap:.4f}%) – "
            f"skipping new entries for {symbol}"
        )
        logger.warning(reason)
        return False, reason

    async def _process_commission_observation(
        self,
        *,
        account,
        strategy,
        symbol: str,
        partition_id: Optional[str],
        commission_usd: Optional[float],
        notional_usd: float
    ) -> None:
        max_cap = getattr(self.settings, "MAX_COMMISSION_RATE_PCT", None)
        if commission_usd is None or notional_usd <= 0:
            return

        observed_pct = (commission_usd / notional_usd) * 100.0
        state = self._commission_cap_state.setdefault(account.id, {})
        previous_block = state.get('blocked', False)
        state.update({
            'last_rate_pct': observed_pct,
            'last_commission_usd': commission_usd,
            'last_notional_usd': notional_usd,
            'blocked': False,
            'last_symbol': symbol,
            'last_partition': partition_id,
            'last_updated': datetime.utcnow().isoformat()
        })

        if max_cap and observed_pct > max_cap:
            state['blocked'] = True
            if not previous_block:
                logger.critical(
                    "Commission cap exceeded for %s (%.4f%% > %.4f%%). Blocking new entries.",
                    account.label,
                    observed_pct,
                    max_cap
                )
                try:
                    await telegram_notifier.send_commission_cap_alert(
                        account_label=account.label,
                        symbol=symbol,
                        strategy=strategy.name if strategy else None,
                        partition_id=partition_id,
                        mode=account.mode,
                        commission_usd=commission_usd,
                        notional_usd=notional_usd,
                        observed_rate_pct=observed_pct,
                        max_rate_pct=max_cap
                    )
                except Exception as alert_error:
                    logger.error(f"❌ Failed to send commission cap alert: {alert_error}", exc_info=True)
        elif previous_block and max_cap:
            logger.info(
                "Commission cap cleared for %s (%.4f%% <= %.4f%%).",
                account.label,
                observed_pct,
                max_cap
            )
            state['blocked'] = False

    async def _execute_partition_trade(self, payload: WebhookPayload, account, strategy, broker) -> OrderResponse:
        symbol = strategy.execution.product_id
        await self._ensure_partitions_initialized(account, broker)

        partition_ids = self._get_partitions_for_strategy(account.id, strategy.id)
        if not partition_ids:
            logger.warning(f"⚠️ No partitions enabled for strategy {strategy.id} on account {account.id}")
            
            # Send Telegram alert for missing partitions
            position_side = "LONG" if payload.is_entry() and payload.is_long() else ("SHORT" if payload.is_entry() else None)
            try:
                await telegram_notifier.send_signal_skipped_alert(
                    account_label=account.label,
                    action=payload.get_side(),
                    symbol=symbol,
                    mode=account.mode,
                    strategy=strategy.name,
                    position_side=position_side,
                    reason="No partitions enabled for this strategy",
                    skipped_partitions=None,
                    note="Check config.yaml to ensure partitions are configured for this strategy_id"
                )
            except Exception as alert_error:
                logger.error(f"❌ Failed to send signal skipped alert: {alert_error}", exc_info=True)
            
            return OrderResponse(
                status="failed",
                client_order_id=payload.get_client_order_id(),
                symbol=symbol,
                side=payload.get_side(),
                quantity=0,
                timestamp=datetime.utcnow().isoformat(),
                broker=account.broker,
                strategy=strategy.name,
                error="No partitions enabled for strategy"
            )

        partition_style = partition_manager.get_partition_style(account.id)
        is_entry_signal = payload.side.lower() in ("buy", "sell")

        if is_entry_signal:
            allowed, reason = self._commission_cap_allows_entry(
                account,
                symbol=symbol,
                strategy_name=strategy.name
            )
            if not allowed:
                position_side = "LONG" if payload.side.lower() == "buy" else "SHORT"
                try:
                    await telegram_notifier.send_signal_skipped_alert(
                        account_label=account.label,
                        action=payload.get_side(),
                        symbol=symbol,
                        mode=account.mode,
                        strategy=strategy.name,
                        position_side=position_side,
                        reason=reason,
                        skipped_partitions=None,
                        note="Commission cap active; exits still allowed."
                    )
                except Exception as alert_error:
                    logger.error(f"❌ Failed to send signal skipped alert (commission cap): {alert_error}", exc_info=True)
                except Exception as alert_error:
                    logger.error(f"❌ Failed to send signal skipped alert (commission cap): {alert_error}", exc_info=True)
                return OrderResponse(
                    status="skipped",
                    account_id=account.id,
                    client_order_id=payload.get_client_order_id(),
                    symbol=symbol,
                    side=payload.get_side(),
                    quantity=0,
                    timestamp=datetime.utcnow().isoformat(),
                    broker=account.broker,
                    strategy=strategy.name,
                    error="commission_cap_active"
                )

        if partition_style == "cooperative" and is_entry_signal:
            ordered = self._get_cooperative_partition_order(account.id, partition_ids)
            if not ordered:
                reason = "All cooperative partitions currently deployed"
                logger.info(f"⚠️ {reason} for account {account.id}")
                position_side = "LONG" if payload.side.lower() == "buy" else "SHORT"
                try:
                    await telegram_notifier.send_signal_skipped_alert(
                        account_label=account.label,
                        action=payload.get_side(),
                        symbol=symbol,
                        mode=account.mode,
                        strategy=strategy.name,
                        position_side=position_side,
                        reason=reason,
                        skipped_partitions=[
                            {"id": pid, "allocation_pct": partition_manager.partition_state.get(account.id, {}).get('partitions', {}).get(pid, {}).get('allocation_pct')}
                            for pid in partition_ids
                        ]
                    )
                except Exception as alert_error:
                    logger.error(f"❌ Failed to send signal skipped alert (cooperative partitions): {alert_error}", exc_info=True)
                
                return OrderResponse(
                    status="skipped",
                    account_id=account.id,
                    order_id=f"{payload.get_client_order_id()}-cooperative-busy",
                    client_order_id=payload.get_client_order_id(),
                    symbol=symbol,
                    side=payload.get_side(),
                    quantity=0,
                    price=None,
                    fill_price=None,
                    commission=None,
                    timestamp=datetime.utcnow().isoformat(),
                    broker=account.broker,
                    strategy=strategy.name,
                    error=reason
                )
            partition_ids = ordered
        # Holiday pre/post guard for entries
        if is_entry_signal and getattr(self.settings, "CALENDAR_ENABLED", False):
            try:
                from services.holiday_guard import get_current_holiday_info
                is_holiday_period, holiday_name, holiday_type = get_current_holiday_info()
                
                if is_holiday_period:
                    logger.info(f"🎭 Entry blocked due to holiday: {holiday_name} ({holiday_type})")
                    
                    # Try to send holiday alert, but don't let alert failure allow trade to proceed
                    try:
                        await telegram_notifier.send_holiday_alert(
                            holiday_name=holiday_name,
                            holiday_type=holiday_type,
                            account_label=account.label
                        )
                    except Exception as alert_err:
                        logger.error(f"Failed to send holiday alert (trade still blocked): {alert_err}")
                    
                    # CRITICAL: Always return skipped response when holiday detected
                    return OrderResponse(
                        status="skipped",
                        account_id=account.id,
                        client_order_id=payload.get_client_order_id(),
                        symbol=symbol,
                        side=payload.get_side(),
                        quantity=0,
                        timestamp=datetime.utcnow().isoformat(),
                        broker=account.broker,
                        strategy=strategy.name,
                        error="holiday_block_window"
                    )
            except Exception as _hg_err:
                logger.warning(f"Holiday guard check failed (allowing trade): {_hg_err}")

        # Determine current price (shared across partitions)
        price_hint = float(payload.price) if payload.price is not None else None

        try:
            from services.exit_monitor import format_price
            current_price = broker.get_current_price(symbol)
            logger.info(f"   Current Price: {format_price(current_price, symbol)}")
        except Exception as e:
            logger.error(f"❌ Failed to get current price: {str(e)}")
            current_price = payload.price or 0

        if account.mode == "DEMO" and price_hint:
            from services.exit_monitor import format_price
            current_price = price_hint
            logger.info(f"   Using payload price for DEMO execution: {format_price(current_price, symbol)}")

        price_hint = float(payload.price) if payload.price is not None else None

        if account.mode == "DEMO" and price_hint:
            from services.exit_monitor import format_price
            logger.info(f"   Using payload price for DEMO execution: {format_price(price_hint, symbol)}")

        if payload.is_entry():
            # ============================================
            # CLOSE OPPOSITE POSITIONS BEFORE NEW ENTRY
            # ============================================
            # If we're opening LONG and there are SHORT positions for this symbol, close them first
            # If we're opening SHORT and there are LONG positions for this symbol, close them first
            # This prevents hedged positions and works with brokers that don't allow hedging
            
            new_direction = 'LONG' if payload.is_long() else 'SHORT'
            opposite_direction = 'SHORT' if payload.is_long() else 'LONG'
            
            # Find all partitions with opposite positions for this symbol
            opposite_positions = self._find_opposite_positions(
                account_id=account.id,
                symbol=symbol,
                new_direction=new_direction
            )
            
            if opposite_positions:
                logger.info(f"🔄 Found {len(opposite_positions)} {opposite_direction} position(s) for {symbol} - closing before opening {new_direction}")
                
                # CRITICAL: Check if any opposite positions are currently being exited
                # If so, wait for them to complete (prevents race conditions)
                from services.partition_manager import is_position_exiting
                max_wait_seconds = 10  # Maximum wait time
                wait_interval = 0.5  # Check every 500ms
                waited_seconds = 0.0
                
                while waited_seconds < max_wait_seconds:
                    positions_still_exiting = [
                        p for p in opposite_positions
                        if is_position_exiting(account.id, p['partition_id'], symbol)
                    ]
                    if not positions_still_exiting:
                        break  # All exits completed
                    logger.debug(f"⏳ Waiting for {len(positions_still_exiting)} opposite position(s) to finish exiting... ({waited_seconds:.1f}s)")
                    await asyncio.sleep(wait_interval)
                    waited_seconds += wait_interval
                
                if waited_seconds >= max_wait_seconds:
                    logger.warning(f"⚠️ Timeout waiting for opposite positions to exit - proceeding anyway")
                
                # Re-check opposite positions (some may have been closed by Agent exits)
                opposite_positions = self._find_opposite_positions(
                    account_id=account.id,
                    symbol=symbol,
                    new_direction=new_direction
                )
                
                if opposite_positions:
                    # Create an exit payload to close remaining opposite positions
                    # This handles the case where a new opposite-direction signal arrives
                    # without an explicit exit signal (e.g., SELL Short -> BUY Long without Exit)
                    # This is a graceful exit to allow the new direction trade to execute
                    exit_reason = f"Direction Change ({opposite_direction} -> {new_direction})"
                    logger.info(f"🔄 Closing {opposite_direction} position(s) due to new {new_direction} signal - Exit Reason: {exit_reason}")
                    
                    exit_payload = WebhookPayload(
                        strategy_id=payload.strategy_id,
                        side="exit",
                        price=payload.price,
                        symbol=payload.symbol,
                        bar_time=payload.bar_time,
                        timestamp=payload.timestamp,
                        exit_reason=exit_reason,
                        source="Agent Direction Change"
                    )
                    
                    # Execute exit for opposite positions
                    exit_result = await self._execute_partition_exit(
                        payload=exit_payload,
                        account=account,
                        strategy=strategy,
                        broker=broker,
                        symbol=symbol,
                        current_price=current_price,
                        partition_ids=[p['partition_id'] for p in opposite_positions],
                        price_hint=price_hint
                    )
                    
                    logger.info(f"✅ Closed {len(opposite_positions)} {opposite_direction} position(s) before opening {new_direction}")
                    
                    # Small delay to ensure state is updated
                    await asyncio.sleep(0.1)
                else:
                    logger.info(f"✅ Opposite positions already closed (likely by Agent exit) - proceeding with {new_direction} entry")
                
                # Refresh partition_ids to get available partitions after closing
                # In cooperative mode, the closed partitions are now available
                if partition_style == "cooperative":
                    partition_ids = self._get_cooperative_partition_order(account.id, partition_ids)
                    if not partition_ids:
                        # All partitions still busy (shouldn't happen after closing)
                        logger.warning("⚠️ No partitions available after closing opposite positions")
            
            return await self._execute_partition_entry(
                payload=payload,
                account=account,
                strategy=strategy,
                broker=broker,
                symbol=symbol,
                current_price=current_price,
                partition_ids=partition_ids,
                price_hint=price_hint
            )
        elif payload.is_exit():
            return await self._execute_partition_exit(
                payload=payload,
                account=account,
                strategy=strategy,
                broker=broker,
                symbol=symbol,
                current_price=current_price,
                partition_ids=partition_ids,
                price_hint=price_hint
            )

        # Unsupported action for partitions
        logger.warning(f"⚠️ Unsupported partition action: {payload.side}")
        return OrderResponse(
            status="rejected",
            client_order_id=payload.get_client_order_id(),
            symbol=symbol,
            side=payload.get_side(),
            quantity=0,
            timestamp=datetime.utcnow().isoformat(),
            broker=account.broker,
            strategy=strategy.name,
            error=f"Unsupported action {payload.side}"
        )

    async def _execute_partition_entry(
        self,
        payload: WebhookPayload,
        account,
        strategy,
        broker,
        symbol: str,
        current_price: float,
        partition_ids: List[str],
        price_hint: Optional[float]
    ) -> OrderResponse:
        equity_pct = strategy.sizing.capital_alloc_pct or self.settings.EQUITY_PERCENTAGE
        if equity_pct <= 1:  # allow fractional inputs like 0.9 for 90%
            equity_pct *= 100.0
        total_quantity = 0.0
        weighted_price_sum = 0.0
        executed_partitions = []
        partition_notifications: List[Dict[str, Any]] = []
        total_actual_cash = 0.0
        total_virtual_capacity = 0.0

        state = partition_manager.partition_state.get(account.id, {})
        partition_style = partition_manager.get_partition_style(account.id)
        single_partition_only = partition_style == "cooperative"

        position_side = 'LONG' if payload.is_long() else 'SHORT'
        order_side = 'BUY' if payload.is_long() else 'SELL'
        current_exposure = partition_manager.get_total_notional_usd(
            account.id,
            symbol,
            current_price
        )
        adv_cap_notes: List[str] = []

        partition_plans: List[Dict[str, Any]] = []
        capital_skipped: List[Dict[str, Any]] = []
        skip_reason: Optional[str] = None
        partition_reconciled = False

        # Get strategy sizing parameters (needed for rounding checks)
        min_qty = getattr(strategy.sizing, "min_qty", None)
        round_step = getattr(strategy.sizing, "round_step", None)
        try:
            min_qty = float(min_qty) if min_qty is not None else None
        except (TypeError, ValueError):
            min_qty = None
        try:
            round_step = float(round_step) if round_step is not None else None
        except (TypeError, ValueError):
            round_step = None

        for partition_id in partition_ids:
            partition_meta = state.get('partitions', {}).get(partition_id, {})
            partition_meta.setdefault('deployed_cash', 0.0)
            partition_meta.setdefault('open_positions', {})
            allocation_pct = float(partition_meta.get('allocation_pct') or 100.0)
            partition_virtual = float(partition_meta.get('virtual_balance') or 0.0)

            # Reconcile deployed cash with actual open positions to avoid stale locks
            open_positions = partition_meta.get('open_positions') or {}
            calculated_deployed = 0.0
            for pos in open_positions.values():
                if not isinstance(pos, dict):
                    continue
                calculated_deployed += float(pos.get('total_cash_used', pos.get('cash_used', 0.0) or 0.0))

            stored_deployed = float(partition_meta.get('deployed_cash', 0.0) or 0.0)
            if abs(calculated_deployed - stored_deployed) > 0.01:
                logger.warning(
                    f"⚠️ Partition {partition_id} deployed cash mismatch detected: "
                    f"stored ${stored_deployed:,.2f} vs calculated ${calculated_deployed:,.2f}. "
                    f"Using calculated value."
                )
                stored_deployed = calculated_deployed
                partition_meta['deployed_cash'] = round(calculated_deployed, 2)
                partition_manager.partition_state[account.id]['partitions'][partition_id]['deployed_cash'] = round(calculated_deployed, 2)
                partition_reconciled = True

            try:
                sizing = partition_manager.calculate_partition_position_size(
                    partition_id=partition_id,
                    account_id=account.id,
                    equity_pct=equity_pct
                )
            except Exception as e:
                logger.error(f"❌ Failed sizing for partition {partition_id}: {str(e)}")
                continue

            actual_cash = sizing.get('actual_cash', 0.0)
            leverage_value = sizing.get('leverage', account.leverage)
            leverage_value = int(leverage_value) if leverage_value else 1
            notional = sizing.get('notional', 0.0)
            if actual_cash <= 0 or notional <= 0 or current_price <= 0:
                logger.warning(f"⚠️ Partition {partition_id} sizing invalid (cash={actual_cash}, notional={notional}, price={current_price})")
                continue

            quantity = notional / current_price
            if quantity <= 0:
                logger.warning(f"⚠️ Partition {partition_id} quantity <= 0, skipping")
                continue
            
            # Check if quantity will be sufficient after rounding BEFORE we round
            # This prevents "quantity collapsed" errors by catching insufficient size early
            # and provides better error messages with exact capital requirements
            if round_step and round_step > 0:
                # Calculate what the quantity will be after rounding
                steps_after_rounding = math.floor(quantity / round_step)
                quantity_after_rounding = steps_after_rounding * round_step
                
                # If rounding would result in 0, skip early with better message
                if quantity_after_rounding <= 0:
                    min_required_qty = round_step  # Need at least one full step
                    min_required_notional = min_required_qty * current_price
                    # Use simplified calculation (notional / leverage) for early check
                    # The margin rate check happens later in the validation flow
                    min_required_cash_estimate = min_required_notional / leverage_value if leverage_value > 0 else min_required_notional
                    
                    logger.warning(
                        f"⚠️ Partition {partition_id} insufficient size for {symbol}: "
                        f"calculated {quantity:.2f} {symbol.split('-')[0]}, but round_step={round_step} requires ≥{min_required_qty:.0f}. "
                        f"Estimated need: ${min_required_cash_estimate:,.2f} cash (have ${actual_cash:,.2f})"
                    )
                    capital_skipped.append({
                        "id": partition_id,
                        "allocation_pct": allocation_pct,
                        "leverage": leverage_value,
                        "detail": (
                            f"Insufficient size: calculated {quantity:.2f} {symbol.split('-')[0]}, "
                            f"but round_step={round_step} requires ≥{min_required_qty:.0f}. "
                            f"Estimated need: ${min_required_cash_estimate:,.2f} cash"
                        )
                    })
                    # Use normalized symbol for display consistency
                    from services.telegram_notifier import normalize_symbol_for_display
                    display_symbol = normalize_symbol_for_display(symbol)
                    skip_reason = f"Insufficient capital - need ${min_required_cash_estimate:,.2f}, have ${actual_cash:,.2f}"
                    continue
                
                # Also check if quantity after rounding will be below minimum
                if min_qty and quantity_after_rounding < min_qty:
                    min_required_qty = min_qty
                    min_required_notional = min_required_qty * current_price
                    # Use simplified calculation (notional / leverage) for early check
                    # The margin rate check happens later in the validation flow
                    min_required_cash_estimate = min_required_notional / leverage_value if leverage_value > 0 else min_required_notional
                    
                    logger.warning(
                        f"⚠️ Partition {partition_id} insufficient size for {symbol}: "
                        f"after rounding to {round_step}, quantity would be {quantity_after_rounding:.0f}, "
                        f"but min_qty={min_qty:.0f} requires ≥{min_required_qty:.0f}. "
                        f"Estimated need: ${min_required_cash_estimate:,.2f} cash (have ${actual_cash:,.2f})"
                    )
                    capital_skipped.append({
                        "id": partition_id,
                        "allocation_pct": allocation_pct,
                        "leverage": leverage_value,
                        "detail": (
                            f"Insufficient size: after rounding, quantity would be {quantity_after_rounding:.0f}, "
                            f"but min_qty={min_qty:.0f} requires ≥{min_required_qty:.0f}. "
                            f"Estimated need: ${min_required_cash_estimate:,.2f} cash"
                        )
                    })
                    # Use normalized symbol for display consistency
                    from services.telegram_notifier import normalize_symbol_for_display
                    display_symbol = normalize_symbol_for_display(symbol)
                    skip_reason = f"Insufficient capital - need ${min_required_cash_estimate:,.2f}, have ${actual_cash:,.2f}"
                    continue

            # ============================================
            # MARGIN REQUIREMENT VALIDATION (Coinbase Futures)
            # ============================================
            # Check margin requirements before allowing entry
            # Only allow positions that can either:
            # 1. Close before 4PM ET (overnight rollover), OR
            # 2. Afford the overnight margin rate
            if hasattr(broker, 'broker') and hasattr(broker.broker, 'get_margin_requirements'):
                try:
                    from zoneinfo import ZoneInfo
                    
                    # Determine which margin rate period to use based on Easy Ichimoku settings
                    et_tz = ZoneInfo('America/New_York')
                    entry_time_et = datetime.now(et_tz)
                    margin_period = self._get_margin_rate_period(strategy.id, entry_time_et)
                    
                    # Fetch fresh margin rates for the selected period and cache them during trade validation
                    # This ensures we use the most current rates when validating the trade
                    # The fresh rates will be cached and used for this trade execution
                    margin_req = broker.broker.get_margin_requirements(
                        symbol, 
                        position_side, 
                        force_period=margin_period,
                        force_fresh=True
                    )
                    
                    logger.info(
                        f"📊 Using {margin_period.upper()} margin rates for {strategy.id} "
                        f"(entry time: {entry_time_et.strftime('%H:%M')} ET)"
                    )
                    
                    # Also ensure the other period rate is cached (intraday if using overnight, overnight if using intraday)
                    # This ensures both rates are available for margin validation
                    if hasattr(broker.broker, 'ensure_daily_margin_rates'):
                        # Only fetch missing rate (don't force refresh - we just got fresh current rate)
                        broker.broker.ensure_daily_margin_rates(symbol, position_side, force_refresh=False)
                    
                    # Calculate required margin using the selected period rate
                    # For intraday period, we still need to check overnight margin affordability
                    # to ensure position can safely transition if it crosses into afterhours
                    margin_calc = broker.broker.calculate_required_margin(
                        symbol=symbol,
                        quantity=quantity,
                        price=current_price,
                        side=position_side,
                        leverage=leverage_value
                    )
                    
                    # Get the margin requirement for the selected period
                    if margin_period == 'intraday':
                        # Use intraday rate for initial margin, but still check overnight affordability
                        initial_margin_req = margin_calc.get('initial_margin_required', 0.0)
                        # For intraday positions, we still need to validate overnight margin
                        # in case the position crosses into afterhours (though it shouldn't for 6PM-10PM window)
                        overnight_margin_req = margin_calc.get('overnight_margin_required', 0.0)
                    else:  # overnight
                        # Use overnight rate for both initial and overnight margin
                        initial_margin_req = margin_calc.get('overnight_margin_required', 0.0)
                        overnight_margin_req = initial_margin_req
                    
                    # Check current time (ET) for overnight margin validation
                    hour = entry_time_et.hour
                    minutes = entry_time_et.minute
                    time_to_4pm = (16 * 60) - (hour * 60 + minutes)  # Minutes until 4PM ET
                    
                    # Check if we can afford initial margin
                    if initial_margin_req > actual_cash:
                        logger.warning(
                            f"⚠️ Partition {partition_id} insufficient margin: "
                            f"required ${initial_margin_req:,.2f}, available ${actual_cash:,.2f}"
                        )
                        capital_skipped.append({
                            "id": partition_id,
                            "detail": f"Insufficient margin: need ${initial_margin_req:,.2f}, have ${actual_cash:,.2f}"
                        })
                        # Use normalized symbol for display consistency
                        from services.telegram_notifier import normalize_symbol_for_display
                        display_symbol = normalize_symbol_for_display(symbol)
                        skip_reason = f"Insufficient margin for {display_symbol} ({position_side})"
                        continue
                    
                    # For intraday positions, validate they can afford afterhours margin if held past 4PM
                    # For afterhours positions, validate overnight margin affordability
                    if margin_period == 'intraday':
                        # Check if this is the special safe window (6:01PM-10:00PM ET) that closes before next day's 4PM
                        # Use 'hour' and 'minutes' variables defined above (from entry_time_et)
                        is_safe_window = (
                            (hour == 18 and minutes >= 1) or  # 6:01PM-6:59PM ET
                            (19 <= hour < 22) or  # 7:00PM-9:59PM ET
                            (hour == 22 and minutes == 0)  # 10:00PM ET exactly
                        )
                        
                        if is_safe_window:
                            # Special window: positions will close before next day's 4PM, so don't need afterhours margin
                            can_afford_overnight = True  # Not needed for this window
                            can_close_before_4pm = True
                            
                            logger.info(
                                f"✅ Partition {partition_id} margin check passed (INTRADAY - Safe Window): "
                                f"initial ${initial_margin_req:,.2f}, overnight ${overnight_margin_req:,.2f} (not needed), "
                                f"available ${actual_cash:,.2f}, "
                                f"entry time {entry_time_et.strftime('%H:%M')} ET (will close before next day's 4PM)"
                            )
                        else:
                            # Market hours (9:30 AM - 4:00 PM ET): Must afford afterhours margin if held past 4PM
                            can_afford_overnight = overnight_margin_req <= actual_cash
                            can_close_before_4pm = time_to_4pm > 0 and time_to_4pm >= 60  # At least 1 hour before 4PM
                            
                            # If position might be held past 4PM, must afford overnight margin
                            if not can_close_before_4pm and not can_afford_overnight:
                                logger.warning(
                                    f"⚠️ Partition {partition_id} cannot afford overnight margin for intraday position: "
                                    f"required ${overnight_margin_req:,.2f}, available ${actual_cash:,.2f}. "
                                    f"Position opened at {entry_time_et.strftime('%H:%M')} ET might be held past 4PM ET "
                                    f"(only {time_to_4pm} minutes remaining)."
                                )
                                capital_skipped.append({
                                    "id": partition_id,
                                    "detail": (
                                        f"Cannot afford overnight margin (${overnight_margin_req:,.2f}) "
                                        f"for intraday position that might be held past 4PM ET ({time_to_4pm} min remaining)"
                                    )
                                })
                                from services.telegram_notifier import normalize_symbol_for_display
                                display_symbol = normalize_symbol_for_display(symbol)
                                skip_reason = f"Cannot afford overnight margin - need ${overnight_margin_req:,.2f}, have ${actual_cash:,.2f}"
                                continue
                            
                            logger.info(
                                f"✅ Partition {partition_id} margin check passed (INTRADAY - Market Hours): "
                                f"initial ${initial_margin_req:,.2f}, overnight ${overnight_margin_req:,.2f} (validated), "
                                f"available ${actual_cash:,.2f}, "
                                f"entry time {entry_time_et.strftime('%H:%M')} ET "
                                f"({'can close before 4PM' if can_close_before_4pm else 'can afford overnight'})"
                            )
                    else:
                        # Afterhours positions: validate overnight margin affordability
                        can_afford_overnight = overnight_margin_req <= actual_cash
                        can_close_before_4pm = time_to_4pm > 0 and time_to_4pm >= 60  # At least 1 hour before 4PM
                        
                        # If position will be held past 4PM, must afford overnight margin
                        if not can_close_before_4pm and not can_afford_overnight:
                            logger.warning(
                                f"⚠️ Partition {partition_id} cannot afford overnight margin: "
                                f"required ${overnight_margin_req:,.2f}, available ${actual_cash:,.2f}. "
                                f"Position would be held past 4PM ET (only {time_to_4pm} minutes remaining)."
                            )
                            capital_skipped.append({
                                "id": partition_id,
                                "detail": (
                                    f"Cannot afford overnight margin (${overnight_margin_req:,.2f}) "
                                    f"and cannot close before 4PM ET ({time_to_4pm} min remaining)"
                                )
                            })
                            # Use normalized symbol for display consistency
                            from services.telegram_notifier import normalize_symbol_for_display
                            display_symbol = normalize_symbol_for_display(symbol)
                            skip_reason = f"Cannot afford overnight margin - need ${overnight_margin_req:,.2f}, have ${actual_cash:,.2f}"
                            continue
                        
                        # Log margin information for afterhours position
                        logger.info(
                            f"✅ Partition {partition_id} margin check passed (AFTERHOURS): "
                            f"initial ${initial_margin_req:,.2f}, overnight ${overnight_margin_req:,.2f}, "
                            f"available ${actual_cash:,.2f}, "
                            f"{'can close before 4PM' if can_close_before_4pm else 'can afford overnight'}"
                        )
                    
                except Exception as e:
                    logger.warning(f"⚠️ Margin check failed for partition {partition_id}: {e}")
                    # Continue with trade if margin check fails (graceful degradation)
                    # This allows trading even if margin API is unavailable

            requirement = None
            if min_qty and current_price > 0:
                buffer_pct = getattr(self.settings, "MIN_CONTRACT_BUFFER_PCT", 0.0)
                
                # CRITICAL: Use API margin rates for accurate minimum contract calculation
                # This ensures position sizing is based on real margin requirements
                margin_rate = None
                if hasattr(broker, 'broker') and hasattr(broker.broker, 'get_margin_requirements'):
                    try:
                        # Determine which margin rate period to use based on Easy Ichimoku settings
                        et_tz = ZoneInfo('America/New_York')
                        entry_time_et = datetime.now(et_tz)
                        margin_period = self._get_margin_rate_period(strategy.id, entry_time_et)
                        
                        # Fetch margin rate for the selected period (use force_fresh=False to use cached API rate if available)
                        # The rate was already fetched with force_fresh=True during validation above
                        margin_req = broker.broker.get_margin_requirements(
                            symbol, 
                            position_side, 
                            force_period=margin_period,
                            force_fresh=False
                        )
                        
                        # Get the rate for the selected period
                        if margin_period == 'intraday':
                            margin_rate = margin_req.get('daytime_rate') or margin_req.get('current_rate') or margin_req.get('initial_margin_pct')
                        else:  # overnight
                            margin_rate = margin_req.get('overnight_rate') or margin_req.get('current_rate') or margin_req.get('initial_margin_pct')
                        
                        if margin_rate:
                            logger.debug(
                                f"Using API {margin_period} margin rate {margin_rate*100:.4f}% for min contract calculation "
                                f"(entry time: {entry_time_et.strftime('%H:%M')} ET)"
                            )
                    except Exception as e:
                        logger.debug(f"Could not fetch margin rate for min contract calculation: {e}")
                        # Will use simplified calculation (notional / leverage)
                
                requirement = self._compute_min_wallet_requirement(
                    min_qty=min_qty,
                    price=current_price,
                    leverage=leverage_value,
                    capital_pct=equity_pct,
                    allocation_pct=allocation_pct,
                    buffer_pct=buffer_pct,
                    margin_rate=margin_rate  # Use API margin rate for accurate calculation
                )
                if requirement:
                    required_partition_balance = requirement.get("partition_balance_usd")
                    required_wallet = requirement.get("wallet_balance_usd")
                    if required_partition_balance and (partition_virtual + 1e-6) < required_partition_balance:
                        detail = (
                            f"Needs wallet ≥ ${required_wallet:,.2f} to trade {symbol} "
                            f"({allocation_pct:.0f}% slot, includes {buffer_pct:.0f}% buffer)."
                        )
                        capital_skipped.append({
                            "id": partition_id,
                            "allocation_pct": allocation_pct,
                            "leverage": leverage_value,
                            "detail": detail
                        })
                        # Use normalized symbol for display consistency
                        from services.telegram_notifier import normalize_symbol_for_display
                        display_symbol = normalize_symbol_for_display(symbol)
                        skip_reason = f"Insufficient capital - need ${required_wallet:,.2f}, have ${partition_virtual:,.2f}"
                        continue

            quantity = self._round_quantity_step(quantity, round_step)

            if quantity <= 0:
                logger.warning(
                    f"⚠️ Partition {partition_id} quantity collapsed after rounding "
                    f"(step={round_step}); skipping."
                )
                capital_skipped.append({
                    "id": partition_id,
                    "allocation_pct": allocation_pct,
                    "leverage": leverage_value,
                    "detail": "Quantity collapsed after applying broker round step"
                })
                skip_reason = skip_reason or "Order size below broker minimum"
                continue

            if min_qty and quantity < min_qty:
                required_wallet = requirement.get("wallet_balance_usd") if requirement else None
                detail_parts = [
                    f"Needs ≥ {min_qty:g} contracts",
                    f"({min_qty * current_price:,.2f} USD notional)"
                ]
                if required_wallet:
                    detail_parts.append(f"Wallet ≥ ${required_wallet:,.2f}")
                detail = " • ".join(detail_parts)

                capital_skipped.append({
                    "id": partition_id,
                    "allocation_pct": allocation_pct,
                    "leverage": leverage_value,
                    "quantity": quantity,
                    "price": current_price,
                    "capital": actual_cash,
                    "detail": detail
                })

                skip_reason = f"Position below broker minimum ({quantity:g} < {min_qty:g})"
                if required_wallet:
                    skip_reason = f"Insufficient capital - need ${required_wallet:,.2f}"
                continue

            # ============================================
            # OPTIMIZE POSITION SIZE: Maximize contracts within available cash
            # ============================================
            # Use API margin rate (intraday or afterhours) based on Easy Ichimoku settings
            # CRITICAL: Only use API rates (not indicative rates) to ensure accuracy
            # This ensures positions are sized correctly for the selected margin period
            # Only optimize if we have margin requirements available
            if hasattr(broker, 'broker') and hasattr(broker.broker, 'get_margin_requirements'):
                try:
                    # Determine which margin rate period to use based on Easy Ichimoku settings
                    et_tz = ZoneInfo('America/New_York')
                    entry_time_et = datetime.now(et_tz)
                    margin_period = self._get_margin_rate_period(strategy.id, entry_time_et)
                    
                    # Get margin rate from API for the selected period (use cached rate from validation above)
                    # force_fresh=False uses cached API rate if available (fetched during validation)
                    margin_req = broker.broker.get_margin_requirements(
                        symbol, 
                        position_side, 
                        force_period=margin_period,
                        force_fresh=False
                    )
                    
                    # CRITICAL: Only use API rates, not indicative rates
                    source = margin_req.get('source', 'unknown')
                    if source != 'api':
                        logger.warning(
                            f"⚠️ Skipping position optimization for {partition_id}: "
                            f"margin rate source is '{source}' (not 'api'). "
                            f"Will use capital allocation method instead."
                        )
                    else:
                        # Get the margin rate for the selected period
                        if margin_period == 'intraday':
                            selected_margin_rate = margin_req.get('daytime_rate', 0.0)
                            period_label = 'intraday'
                        else:  # overnight
                            selected_margin_rate = margin_req.get('overnight_rate', 0.0)
                            period_label = 'afterhours'
                        
                        if selected_margin_rate > 0 and min_qty and current_price > 0:
                            try:
                                contract_size = float(min_qty)
                                
                                # Calculate margin per contract at selected rate
                                # margin_per_contract = (contract_size × price × margin_rate) / leverage
                                contract_notional = contract_size * current_price
                                margin_per_contract = (contract_notional * selected_margin_rate) / leverage_value
                                
                                # Calculate maximum contracts we can afford with available cash
                                # Use actual_cash (70% allocation) as the limit
                                max_contracts = int(actual_cash / margin_per_contract) if margin_per_contract > 0 else 0
                                
                                if max_contracts > 0:
                                    # Recalculate quantity, notional, and actual_cash based on max contracts
                                    optimized_quantity = max_contracts * contract_size
                                    optimized_notional = optimized_quantity * current_price
                                    optimized_margin = (optimized_notional * selected_margin_rate) / leverage_value
                                    
                                    # Only optimize if we can fit more contracts than current calculation
                                    # and optimized margin is within available cash
                                    if optimized_quantity > quantity and optimized_margin <= actual_cash:
                                        logger.info(
                                            f"📈 Optimizing position size for {partition_id}: "
                                            f"{quantity:.4f} → {optimized_quantity:.4f} {symbol.split('-')[0]} "
                                            f"({int(quantity/contract_size)} → {max_contracts} contracts), "
                                            f"margin ${(notional * selected_margin_rate) / leverage_value:,.2f} → ${optimized_margin:,.2f} "
                                            f"(using API {period_label} rate {selected_margin_rate*100:.4f}%)"
                                        )
                                        quantity = optimized_quantity
                                        notional = optimized_notional
                                        actual_cash = optimized_margin  # Use actual margin required, not capital allocation
                                    else:
                                        logger.debug(
                                            f"Position size optimization skipped for {partition_id}: "
                                            f"optimized quantity ({optimized_quantity:.4f}) not greater than current ({quantity:.4f}) "
                                            f"or optimized margin (${optimized_margin:,.2f}) exceeds available (${actual_cash:,.2f})"
                                        )
                            except (TypeError, ValueError) as e:
                                logger.debug(f"Could not optimize position size: {e}")
                except Exception as e:
                    logger.debug(f"Could not fetch margin rate for optimization: {e}")

            max_allocation = (partition_meta.get('virtual_balance', 0.0) or 0.0) * (equity_pct / 100.0)
            deployed_cash = partition_meta.get('deployed_cash', 0.0) or 0.0
            available_cash = max(0.0, max_allocation - deployed_cash)
            per_trade_cap = max_allocation * (self.settings.MAX_POSITION_ALLOCATION_PCT / 100.0)
            per_trade_cap = max(0.0, per_trade_cap)

            if per_trade_cap <= 0:
                logger.info(
                    f"Partition {partition_id} per-trade cap is $0.00 "
                    f"(check max_position_allocation_pct setting). Skipping."
                )
                capital_skipped.append({
                    "id": partition_id,
                    "detail": "Per-trade cap is $0.00 (check max_position_allocation_pct setting)"
                })
                continue

            if available_cash <= 0:
                logger.info(
                    f"Partition {partition_id} has no available capital "
                    f"(allocated ${deployed_cash:,.2f} / ${max_allocation:,.2f}). Skipping."
                )
                capital_skipped.append({
                    "id": partition_id,
                    "detail": f"Allocated ${deployed_cash:,.2f} / ${max_allocation:,.2f} (no free capital)"
                })
                continue

            cap_limit = min(available_cash, per_trade_cap)

            if cap_limit <= 0:
                logger.info(
                    f"Partition {partition_id} per-trade cap exhausted "
                    f"(available ${available_cash:,.2f}, per-trade ${per_trade_cap:,.2f}). Skipping."
                )
                capital_skipped.append({
                    "id": partition_id,
                    "detail": f"Per-trade limit ${per_trade_cap:,.2f}, available ${available_cash:,.2f} (no usable capacity)"
                })
                continue

            if actual_cash > cap_limit:
                if cap_limit <= 0:
                    logger.info(
                        f"Partition {partition_id} insufficient capital for {symbol} "
                        f"(requested ${actual_cash:,.2f}, usable ${cap_limit:,.2f})."
                    )
                    capital_skipped.append({
                        "id": partition_id,
                        "detail": f"Requested ${actual_cash:,.2f}, usable ${cap_limit:,.2f} (no capital remaining)"
                    })
                    continue
                scale_ratio = cap_limit / actual_cash if actual_cash else 0
                adjusted_notional = notional * scale_ratio
                adjusted_quantity = adjusted_notional / current_price if current_price > 0 else 0.0

                if adjusted_notional <= 0 or adjusted_quantity <= 0:
                    logger.info(
                        f"Partition {partition_id} scaling resulted in zero quantity "
                        f"(usable ${cap_limit:,.2f}). Skipping."
                    )
                    capital_skipped.append({
                        "id": partition_id,
                        "detail": f"Scaling resulted in zero quantity (usable ${cap_limit:,.2f})"
                    })
                    continue

                limit_reasons = []
                if cap_limit < available_cash:
                    limit_reasons.append(f"per-trade cap ${per_trade_cap:,.2f}")
                if cap_limit < per_trade_cap:
                    limit_reasons.append(f"available capital ${available_cash:,.2f}")
                if not limit_reasons:
                    limit_reasons.append(f"cap ${cap_limit:,.2f}")
                logger.info(
                    f"{partition_id}: requested ${actual_cash:,.2f} scaled to ${cap_limit:,.2f} "
                    f"due to {', '.join(limit_reasons)}."
                )

                actual_cash = cap_limit
                notional = adjusted_notional
                quantity = adjusted_quantity

            partition_plans.append({
                'partition_id': partition_id,
                'actual_cash': actual_cash,
                'notional': notional,
                'leverage': leverage_value,
                'quantity': quantity,
                'max_allocation': max_allocation,
                'virtual_balance': partition_meta.get('virtual_balance')
            })

            if single_partition_only:
                self._advance_cooperative_pointer(account.id, partition_id)
                break

        if partition_reconciled:
            try:
                await state_store.save_partition_state(account.id, partition_manager.partition_state[account.id])
            except Exception as exc:
                logger.warning(f"⚠️ Unable to persist partition deployed cash reconciliation: {exc}")

        if not partition_plans:
            if capital_skipped:
                skipped_log = "; ".join(
                    f"{item['id']} ({item['detail']})" for item in capital_skipped
                )
                logger.info(
                    f"Signal skipped for {symbol}: insufficient available capital in partitions "
                    f"({skipped_log})."
                )
                try:
                    await telegram_notifier.send_signal_skipped_alert(
                        account_label=account.label,
                        action=order_side,
                        symbol=symbol,
                        mode=account.mode,
                        strategy=strategy.name,
                        position_side=position_side,
                        reason=skip_reason or "Position already executed with allocated capital",
                        skipped_partitions=capital_skipped,
                        note=None
                    )
                except Exception as alert_error:
                    logger.error(f"❌ Failed to send signal skipped alert (capital): {alert_error}", exc_info=True)
                return OrderResponse(
                    status="skipped",
                    account_id=account.id,
                    order_id=f"{payload.get_client_order_id()}-partition-skipped",
                    client_order_id=payload.get_client_order_id(),
                    symbol=symbol,
                    side=payload.get_side(),
                    quantity=0,
                    price=current_price,
                    fill_price=current_price,
                    commission=None,
                    timestamp=datetime.utcnow().isoformat(),
                    broker=account.broker,
                    strategy=strategy.name,
                    leverage=None,
                    error="Insufficient partition capital"
                )
            logger.error("❌ All partition executions failed (sizing)")
            return OrderResponse(
                status="failed",
                account_id=account.id,
                client_order_id=payload.get_client_order_id(),
                symbol=symbol,
                side=payload.get_side(),
                quantity=0,
                timestamp=datetime.utcnow().isoformat(),
                broker=account.broker,
                strategy=strategy.name,
                error="All partition executions failed"
            )

        # =====================================================
        # ADV CAP SLIP GUARD - Simple Position Size Capping
        # =====================================================
        # 1. Get max position size = ADV × cap_pct
        # 2. Cap each partition's notional to max if exceeded
        # 3. Execute ALL trades at their (possibly capped) sizes
        # =====================================================
        
        adv_cap_limit_pct: Optional[float] = None
        max_position_notional: Optional[float] = None  # Max notional per position based on ADV
        
        if self.settings.ENABLE_ADV_CAP and self.risk_manager:
            # Get ADV data for the symbol
            adv_result = await self.risk_manager.apply_adv_cap(
                symbol=symbol,
                broker=account.broker,
                requested_qty_usd=0,  # Just fetching ADV info
                leverage=1,
                current_exposure_usd=0
            )
            
            adv_usd = adv_result.get('adv_usd')
            cap_pct = self.settings.ADV_CAP_PCT / 100.0 if hasattr(self.settings, 'ADV_CAP_PCT') else 0.01
            
            if adv_usd and adv_usd > 0:
                max_position_notional = adv_usd * cap_pct
                adv_cap_limit_pct = cap_pct * 100.0
                logger.info(f"📊 ADV Cap: {symbol} ADV=${adv_usd:,.0f}, Cap={cap_pct*100:.2f}%, Max=${max_position_notional:,.0f}")
            else:
                logger.warning(f"📊 ADV data unavailable for {symbol} - executing without cap")
        
        # Apply ADV cap to each partition individually
        for plan in partition_plans:
            partition_id = plan['partition_id']
            original_notional = plan['notional']
            
            if max_position_notional and original_notional > max_position_notional:
                # Cap this partition's notional to ADV limit
                scale_factor = max_position_notional / original_notional
                
                plan['notional'] = max_position_notional
                plan['actual_cash'] = plan['actual_cash'] * scale_factor
                plan['quantity'] = max_position_notional / current_price if current_price > 0 else 0
                
                # Recalculate leverage to match new notional/cash ratio
                if plan['actual_cash'] > 0:
                    new_leverage = max_position_notional / plan['actual_cash']
                    plan['leverage'] = max(1, min(plan['leverage'], int(new_leverage)))
                
                adv_cap_notes.append(
                    f"{partition_id} capped: ${original_notional:,.0f} → ${max_position_notional:,.0f} (ADV {adv_cap_limit_pct:.2f}%)"
                )
                logger.info(f"📊 ADV Cap applied to {partition_id}: ${original_notional:,.0f} → ${max_position_notional:,.0f}")

        # Execute in order of increasing leverage (lowest leverage first)
        partition_plans.sort(key=lambda p: p['leverage'])
        
        # Track leverage for OrderResponse (use first executed partition's leverage)
        executed_leverage = None
        
        for plan in partition_plans:
            partition_id = plan['partition_id']
            actual_cash = plan['actual_cash']
            notional = plan['notional']
            leverage_value = plan['leverage']
            quantity = plan['quantity']

            # Skip only if quantity is truly zero (not due to ADV cap)
            if quantity <= 0 or actual_cash <= 0:
                logger.warning(f"⚠️ Partition {partition_id} has zero quantity/cash - skipping")
                continue

            success = await leverage_manager.set_leverage_for_order(
                broker=broker,
                account_id=account.id,
                partition_id=partition_id,
                symbol=symbol,
                desired_leverage=leverage_value
            )

            if not success:
                logger.error(f"❌ Failed to set leverage {leverage_value}x for partition {partition_id}")
                continue

            client_order_id = f"{payload.get_client_order_id()}-{partition_id}"

            price_override = price_hint if (account.mode == "DEMO" and price_hint) else None
            try:
                order, _ = await self._place_market_order_with_retry(
                    broker=broker,
                    symbol=symbol,
                    side=order_side,
                    quantity=quantity,
                    leverage=leverage_value,
                    client_order_id=client_order_id,
                    price_override=price_override,
                    account=account,
                    strategy=strategy,
                    partition_id=partition_id,
                    action_label="ENTRY"
                )
            except Exception as e:
                logger.error(f"❌ Partition {partition_id} order failed after retries: {str(e)}")
                continue

            fill_price = order.get('price') or current_price or price_hint or 0
            order_id = order.get('orderId') or order.get('order_id') or client_order_id

            broker_commission = None
            if account.mode == "LIVE":
                try:
                    if 'commission' in order:
                        broker_commission = float(order.get('commission', 0.0))
                    elif 'fee' in order:
                        broker_commission = float(order.get('fee', 0.0))
                except Exception as fee_error:
                    logger.debug(f"Commission parse failed for entry order: {fee_error}")

            notional_value = abs(quantity * fill_price) if fill_price else 0.0
            await self._process_commission_observation(
                account=account,
                strategy=strategy,
                symbol=symbol,
                partition_id=partition_id,
                commission_usd=broker_commission,
                notional_usd=notional_value
            )

            # Calculate TradingView stop loss price if provided in payload
            # This is used by exit monitor to match TradingView's stop loss level
            tradingview_stop_loss = None
            if payload.sl_pct:
                if payload.is_long():
                    tradingview_stop_loss = fill_price * (1 - abs(payload.sl_pct) / 100)
                else:
                    tradingview_stop_loss = fill_price * (1 + abs(payload.sl_pct) / 100)
            elif payload.sl_price:
                tradingview_stop_loss = payload.sl_price
            
            await partition_manager.open_partition_position(
                partition_id=partition_id,
                account_id=account.id,
                symbol=symbol,
                side=position_side,
                quantity=quantity,
                entry_price=fill_price,
                cash_used=actual_cash,
                leverage=leverage_value,
                order_id=order_id,
                strategy_id=strategy.id,
                strategy_name=strategy.name,
                tradingview_stop_loss=tradingview_stop_loss  # Pass TradingView stop loss for exit monitoring
            )
            
            # Update real-time margin telemetry from broker
            if account.mode == "LIVE":
                try:
                    await partition_manager.update_runtime_margin_telemetry(account.id, broker)
                except Exception as telemetry_error:
                    logger.debug(f"Telemetry update failed after entry: {telemetry_error}")

            if payload.tp_price or payload.sl_price or payload.tp_pct or payload.sl_pct:
                try:
                    await self._set_optional_sltp(
                        payload=payload,
                        broker=broker,
                        account=account,
                        quantity=quantity,
                        fill_price=fill_price,
                        symbol=symbol
                    )
                except Exception as sltp_error:
                    logger.error(f"⚠️ SL/TP setup failed for partition {partition_id}: {sltp_error}")

            total_quantity += quantity
            weighted_price_sum += fill_price * quantity
            executed_partitions.append(partition_id)
            current_exposure += notional

            partition_meta = state.get('partitions', {}).get(partition_id, {})
            total_actual_cash += actual_cash
            total_virtual_capacity += plan.get('virtual_balance') or plan.get('max_allocation') or partition_meta.get('virtual_balance', 0.0) or 0.0
            # Track leverage for OrderResponse (use first executed partition's leverage)
            if executed_leverage is None:
                executed_leverage = leverage_value
            partition_notifications.append({
                'id': partition_id,
                'quantity': quantity,
                'price': fill_price,
                'leverage': leverage_value,
                'allocation_pct': partition_meta.get('allocation_pct'),
                'position_side': position_side,
                'symbol': symbol,
                'capital_used': actual_cash,
                'virtual_balance_before': partition_meta.get('virtual_balance'),
                'max_allocation': plan.get('max_allocation'),
                'virtual_balance': plan.get('virtual_balance')
            })

        if not executed_partitions:
            # This should only happen if ALL partitions had zero quantity/cash
            # (e.g., insufficient capital) - NOT due to ADV cap which only reduces sizes
            logger.warning("⚠️ No partitions executed - all partitions had zero quantity or cash")

            skip_reason = "Insufficient capital in all partitions"
            if adv_cap_notes:
                skip_reason = "Insufficient capital after ADV cap adjustment"

            skipped_partitions = [
                {
                    "id": plan["partition_id"],
                    "detail": f"Requested ${plan['actual_cash']:,.2f} margin (${plan['notional']:,.2f} notional)",
                    "quantity": plan.get("quantity"),
                    "capital": plan.get("actual_cash"),
                    "price": current_price,
                    "leverage": plan.get("leverage"),
                    "allocation_pct": state.get('partitions', {}).get(plan["partition_id"], {}).get('allocation_pct')
                }
                for plan in partition_plans
            ]

            try:
                await telegram_notifier.send_signal_skipped_alert(
                    account_label=account.label,
                    action=order_side,
                    symbol=symbol,
                    mode=account.mode,
                    strategy=strategy.name,
                    position_side=position_side,
                    reason=skip_reason,
                    skipped_partitions=skipped_partitions,
                    note=skip_reason
                )
            except Exception as alert_error:
                logger.error(f"❌ Failed to send signal skipped alert (ADV cap): {alert_error}", exc_info=True)

            return OrderResponse(
                status="skipped",
                account_id=account.id,
                order_id=f"{payload.get_client_order_id()}-no-capital",
                client_order_id=payload.get_client_order_id(),
                symbol=symbol,
                side=payload.get_side(),
                quantity=0,
                price=current_price,
                fill_price=current_price,
                commission=None,
                timestamp=datetime.utcnow().isoformat(),
                broker=account.broker,
                strategy=strategy.name,
                leverage=None,
                error="ADV cap prevented execution"
            )

        average_price = weighted_price_sum / total_quantity if total_quantity > 0 else current_price

        logger.info(f"✅ Partition entry complete. Partitions executed: {executed_partitions}")

        adv_cap_note = None
        adv_cap_limit_pct: Optional[float] = None
        adv_result = None
        if adv_cap_notes:
            detail = "; ".join(adv_cap_notes)
            pct_text = f"{adv_cap_limit_pct:.2f}%" if adv_cap_limit_pct is not None else f"{self.settings.ADV_CAP_PCT:.2f}%"
            adv_cap_note = f"*Position orders capped at {pct_text} of ADV for {symbol}* ({detail})"

        # Determine source for entry alerts
        source = payload.source if payload.source else "TradingView webhook Entry"
        
        # Send execution alert with error handling
        try:
            alert_sent = await telegram_notifier.send_execution_alert(
                account_label=account.label,
                action=order_side,
                symbol=symbol,
                quantity=total_quantity,
                price=average_price,
                leverage=None,
                mode=account.mode,
                strategy=strategy.name,
                position_side=position_side,
                partition_summaries=partition_notifications,
                adv_cap_note=adv_cap_note,
                capital_deployed=total_actual_cash,
                capital_capacity=total_virtual_capacity if total_virtual_capacity > 0 else None,
                account_id=account.id,  # CRITICAL: Pass account_id for portfolio P&L section
                source=source
            )
            if not alert_sent:
                logger.warning(
                    f"⚠️ Execution alert NOT sent for {symbol} {order_side} "
                    f"(account: {account.label}) - Telegram may be disabled or failed"
                )
        except Exception as alert_error:
            logger.error(
                f"❌ Failed to send execution alert for {symbol} {order_side} "
                f"(account: {account.label}): {alert_error}",
                exc_info=True
            )
            # Don't fail execution if alert fails, but log it

        return OrderResponse(
            status="success",
            account_id=account.id,
            order_id=f"{payload.get_client_order_id()}-partition",
            client_order_id=payload.get_client_order_id(),
            symbol=symbol,
            side=payload.get_side(),
            quantity=total_quantity,
            price=average_price,
            fill_price=average_price,
            commission=None,
            timestamp=datetime.utcnow().isoformat(),
            broker=account.broker,
            strategy=strategy.name,
            leverage=executed_leverage if executed_leverage is not None else account.leverage,
            capital_deployed=total_actual_cash
        )

    async def _execute_partition_exit(
        self,
        payload: WebhookPayload,
        account,
        strategy,
        broker,
        symbol: str,
        current_price: float,
        partition_ids: List[str],
        price_hint: Optional[float]
    ) -> OrderResponse:
        state = partition_manager.partition_state.get(account.id, {})
        total_quantity = 0.0
        weighted_price_sum = 0.0
        aggregated_pnl = 0.0
        total_virtual_balance_before = 0.0
        aggregated_commission = 0.0
        total_capital_deployed = 0.0
        closed_partitions = []
        partition_notifications: List[Dict[str, Any]] = []
        executed_leverage = None  # Track leverage for OrderResponse

        if account.mode == "DEMO" and price_hint:
            from services.exit_monitor import format_price
            current_price = price_hint
            logger.info(f"   Using payload price for DEMO exit: {format_price(current_price, symbol)}")

        # CRITICAL: For exits, close ALL positions for this symbol across ALL partitions
        # This ensures stale positions from previous days are also closed
        all_partitions = list(state.get('partitions', {}).keys())
        partitions_to_check = partition_ids.copy()
        
        # Add any partitions that have open positions for this symbol (even if not in strategy list)
        for partition_id in all_partitions:
            if partition_id not in partitions_to_check:
                partition_info = state.get('partitions', {}).get(partition_id)
                if partition_info and symbol in partition_info.get('open_positions', {}):
                    partitions_to_check.append(partition_id)
                    logger.info(f"🔍 Found additional position for {symbol} in partition {partition_id} (not in strategy list) - will close")
        
        logger.info(f"📋 Exit signal for {symbol}: Checking {len(partitions_to_check)} partitions (strategy: {len(partition_ids)}, additional: {len(partitions_to_check) - len(partition_ids)})")
        
        # Log all positions that will be closed (for debugging)
        positions_to_close = []
        for pid in partitions_to_check:
            part_info = state.get('partitions', {}).get(pid)
            if part_info:
                pos = part_info.get('open_positions', {}).get(symbol)
                if pos:
                    positions_to_close.append({
                        'partition': pid,
                        'side': pos.get('side', 'UNKNOWN'),
                        'quantity': pos.get('quantity', 0.0),
                        'entry_price': pos.get('entry_price', 0.0)
                    })
        
        if positions_to_close:
            from services.exit_monitor import format_price
            logger.info(f"🔍 Exit signal for {symbol}: Will close {len(positions_to_close)} position(s):")
            for pos_info in positions_to_close:
                logger.info(f"   - {pos_info['partition']}: {pos_info['side']} {pos_info['quantity']} @ {format_price(pos_info['entry_price'], symbol)}")
        else:
            logger.warning(f"⚠️ Exit signal for {symbol}: No positions found to close")

        for partition_id in partitions_to_check:
            partition_info = state.get('partitions', {}).get(partition_id)
            if not partition_info:
                continue

            position = partition_info['open_positions'].get(symbol)
            if not position:
                continue

            quantity = position['quantity']
            if quantity <= 0:
                continue
            
            # CRITICAL: Check if position is already being exited (prevents duplicate exits)
            from services.partition_manager import is_position_exiting, mark_position_exiting, clear_position_exiting
            
            if is_position_exiting(account.id, partition_id, symbol):
                logger.warning(f"⚠️ Position {symbol} in {partition_id} is already being exited - skipping duplicate exit")
                continue
            
            # CRITICAL: Verify broker position exists before executing exit (prevents phantom positions)
            if account.mode == "LIVE":
                try:
                    broker_positions = await broker.get_all_positions()
                    broker_has_position = False
                    for bp in broker_positions:
                        bp_symbol = bp.get('symbol') or bp.get('product_id', '')
                        # Normalize symbols for comparison
                        bp_symbol_norm = bp_symbol.upper().replace('-PERP', '').replace('-CBSE', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
                        symbol_norm = symbol.upper().replace('-PERP', '').replace('-CBSE', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
                        
                        if bp_symbol_norm == symbol_norm:
                            broker_qty = abs(float(bp.get('position_amt', 0) or bp.get('quantity', 0) or bp.get('net_size', 0)))
                            if broker_qty > 0:
                                broker_has_position = True
                                break
                    
                    if not broker_has_position:
                        logger.warning(f"⚠️ Broker has no position for {symbol} in {partition_id} - position may already be closed, skipping exit")
                        # Remove stale position from partition state
                        if symbol in partition_info.get('open_positions', {}):
                            del partition_info['open_positions'][symbol]
                            logger.info(f"🧹 Removed stale position {symbol} from partition {partition_id}")
                        continue
                except Exception as broker_check_error:
                    logger.warning(f"⚠️ Could not verify broker position for {symbol}: {broker_check_error} - proceeding with exit")
            
            # CRITICAL: Verify broker position exists before executing exit (prevents phantom positions)
            if account.mode == "LIVE":
                try:
                    broker_positions = await broker.get_all_positions()
                    broker_has_position = False
                    for bp in broker_positions:
                        bp_symbol = bp.get('symbol') or bp.get('product_id', '')
                        # Normalize symbols for comparison
                        bp_symbol_norm = bp_symbol.upper().replace('-PERP', '').replace('-CBSE', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
                        symbol_norm = symbol.upper().replace('-PERP', '').replace('-CBSE', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
                        
                        if bp_symbol_norm == symbol_norm:
                            broker_qty = abs(float(bp.get('position_amt', 0) or bp.get('quantity', 0) or bp.get('net_size', 0)))
                            if broker_qty > 0:
                                broker_has_position = True
                                break
                    
                    if not broker_has_position:
                        logger.warning(f"⚠️ Broker has no position for {symbol} in {partition_id} - position may already be closed, skipping exit")
                        # Remove stale position from partition state
                        if symbol in partition_info.get('open_positions', {}):
                            del partition_info['open_positions'][symbol]
                            logger.info(f"🧹 Removed stale position {symbol} from partition {partition_id}")
                        continue
                except Exception as broker_check_error:
                    logger.warning(f"⚠️ Could not verify broker position for {symbol}: {broker_check_error} - proceeding with exit")
            
            # Mark position as exiting before executing
            mark_position_exiting(account.id, partition_id, symbol)
            
            try:
                side = position['side']
                order_side = 'SELL' if side == 'LONG' else 'BUY'
                leverage_value = position.get('leverage', account.leverage)

                await leverage_manager.set_leverage_for_order(
                    broker=broker,
                    account_id=account.id,
                    partition_id=partition_id,
                    symbol=symbol,
                    desired_leverage=leverage_value
                )

                client_order_id = f"{payload.get_client_order_id()}-{partition_id}-exit"

                price_override = price_hint if account.mode == "DEMO" else None
                exit_verify_delay = getattr(self.settings, "BROKER_EXIT_VERIFY_INTERVAL_SECONDS", 15)

                async def verify_callback():
                    # In DEMO mode, skip broker verification - we control position state directly
                    if account.mode == "DEMO":
                        # In demo mode, position is cleared immediately in place_market_order
                        # No need to verify with broker since we control the state
                        logger.debug(f"DEMO mode: Skipping broker position verification for {symbol}")
                        return True, None
                    
                    await asyncio.sleep(exit_verify_delay)
                    return await self._verify_broker_position_closed(broker, symbol)

                order, _ = await self._place_market_order_with_retry(
                    broker=broker,
                    symbol=symbol,
                    side=order_side,
                    quantity=quantity,
                    leverage=leverage_value,
                    client_order_id=client_order_id,
                    price_override=price_override,
                    account=account,
                    strategy=strategy,
                    partition_id=partition_id,
                    action_label="EXIT",
                    verify_callback=verify_callback
                )
            except Exception as e:
                logger.error(f"❌ Partition {partition_id} exit failed after retries: {str(e)}")
                # Clear exit marker on failure (allows retry)
                clear_position_exiting(account.id, partition_id, symbol)
                continue

            # Check if this was a manual closure (position already closed on broker)
            manual_closure = order.get('manual_closure_detected', False)
            if manual_closure:
                logger.info(f"🔄 Partition {partition_id}: Position manually closed - cleaning up internal state")
                
                # Trigger position reconciliation after manual closure cleanup
                try:
                    from services.position_reconciler import position_reconciler
                    await position_reconciler.reconcile_account_positions(account.id)
                    logger.info(f"✅ Position reconciliation completed after manual closure for {account.id}")
                except Exception as e:
                    logger.warning(f"⚠️ Position reconciliation failed after manual closure: {e}")

            fill_price = order.get('price') or current_price or price_hint

            # In Live mode, try to get actual broker data (P&L, commissions)
            broker_pnl_usd_net = None
            broker_commission = None
            
            if account.mode == "LIVE":
                try:
                    # Get position before close to calculate P&L
                    position_before = partition_info['open_positions'].get(symbol, {})
                    total_cash_used = position_before.get('total_cash_used', position_before.get('cash_used', 0.0))
                    
                    # Try to get actual commission from broker order response
                    # Most brokers include commission in the order response
                    if 'commission' in order:
                        broker_commission = float(order.get('commission', 0.0))
                        logger.info(f"💰 LIVE MODE: Broker commission from order: ${broker_commission:,.2f}")
                    elif 'fee' in order:
                        broker_commission = float(order.get('fee', 0.0))
                        logger.info(f"💰 LIVE MODE: Broker fee from order: ${broker_commission:,.2f}")
                    
                    # Try to get realized P&L from broker
                    # Some brokers provide this in the order response or position data
                    if 'realized_pnl' in order:
                        broker_pnl_usd_net = float(order.get('realized_pnl', 0.0))
                        logger.info(f"💰 LIVE MODE: Broker realized P&L from order: ${broker_pnl_usd_net:,.2f}")
                    elif 'pnl' in order:
                        broker_pnl_usd_net = float(order.get('pnl', 0.0))
                        logger.info(f"💰 LIVE MODE: Broker P&L from order: ${broker_pnl_usd_net:,.2f}")
                    
                    # If broker data not available, we'll calculate from price change
                    # The balance sync will reconcile any differences
                    if broker_pnl_usd_net is None or broker_commission is None:
                        logger.info(f"💰 LIVE MODE: Broker P&L/commission not in order response, will calculate from price change")
                        logger.info(f"   Balance sync will reconcile with broker's actual account balance")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to extract broker P&L/commission from order: {str(e)}")
                    logger.info(f"   Will calculate from price change; balance sync will reconcile")

            notional_value = abs(quantity * fill_price) if fill_price else 0.0
            await self._process_commission_observation(
                account=account,
                strategy=strategy,
                symbol=symbol,
                partition_id=partition_id,
                commission_usd=broker_commission,
                notional_usd=notional_value
            )

            # Extract peak profit and exit reason from payload's historical_enhancer data
            # Also check if we need to get peak profit from exit_monitor for this position
            peak_profit_pct = None
            exit_reason = getattr(payload, 'exit_reason', None)
            if payload.historical_enhancer:
                if isinstance(payload.historical_enhancer, dict):
                    peak_profit_pct = payload.historical_enhancer.get('peak_profit_pct')
                    # Exit reason might be in historical_enhancer or as payload attribute
                    if not exit_reason:
                        exit_reason = payload.historical_enhancer.get('exit_reason')
            
            # If peak profit not in payload, try to get it from exit_monitor for this position
            if peak_profit_pct is None:
                try:
                    from services.exit_monitor import exit_monitor
                    position_key = f"{account.id}_{partition_id}_{symbol}"
                    if position_key in exit_monitor.monitored_positions:
                        position = exit_monitor.monitored_positions[position_key]
                        if position.peak_profit_pct > 0:
                            peak_profit_pct = position.peak_profit_pct
                            logger.debug(f"📈 Retrieved peak profit from exit_monitor: {peak_profit_pct:.2f}% for {symbol}")
                except Exception as e:
                    logger.debug(f"Could not retrieve peak profit from exit_monitor: {e}")
            
            pnl_usd, trade_record = await partition_manager.close_partition_position(
                partition_id=partition_id,
                account_id=account.id,
                symbol=symbol,
                exit_price=fill_price,
                broker_pnl_usd_net=broker_pnl_usd_net,
                broker_commission=broker_commission,
                mode=account.mode,
                strategy_id=strategy.id,
                strategy_name=strategy.name,
                peak_profit_pct=peak_profit_pct,
                exit_reason=exit_reason
            )
            
            # Update real-time margin telemetry from broker
            if account.mode == "LIVE":
                try:
                    await partition_manager.update_runtime_margin_telemetry(account.id, broker)
                except Exception as telemetry_error:
                    logger.debug(f"Telemetry update failed after exit: {telemetry_error}")

            total_quantity += quantity
            weighted_price_sum += fill_price * quantity
            aggregated_pnl += pnl_usd
            aggregated_commission += trade_record.get('commission', 0.0)
            closed_partitions.append(partition_id)

            partition_meta = state.get('partitions', {}).get(partition_id, {})
            entry_time_iso = trade_record.get('entry_time')
            exit_time_iso = trade_record.get('exit_time')
            holding_seconds = None
            if entry_time_iso and exit_time_iso:
                try:
                    entry_dt = datetime.fromisoformat(entry_time_iso)
                    exit_dt = datetime.fromisoformat(exit_time_iso)
                    holding_seconds = max((exit_dt - entry_dt).total_seconds(), 0.0)
                except Exception:
                    holding_seconds = None

            total_virtual_balance_before += trade_record.get('virtual_balance_before', 0.0)
            
            # Track leverage and capital deployed for OrderResponse
            if executed_leverage is None:
                executed_leverage = leverage_value
            cash_used = trade_record.get('cash_used') or trade_record.get('total_cash_used', 0.0)
            total_capital_deployed += cash_used

            partition_notifications.append({
                'id': partition_id,
                'quantity': quantity,
                'price': fill_price,
                'leverage': leverage_value,
                'allocation_pct': partition_meta.get('allocation_pct'),
                'entry_price': trade_record.get('entry_price'),
                'exit_price': trade_record.get('exit_price'),
                'pnl_usd': pnl_usd,
                'pnl_pct': trade_record.get('pnl_pct_net'),
                'position_side': side,
                'symbol': symbol,
                'capital_used': cash_used,
                'virtual_balance_before': trade_record.get('virtual_balance_before'),
                'holding_seconds': holding_seconds
            })

        if not closed_partitions:
            logger.warning("⚠️ Partition exit requested but no positions found")
            
            # Send informational alert when exit signal received but no positions to close
            # This ensures user knows TradingView signal was received even if nothing to close
            try:
                from zoneinfo import ZoneInfo
                from datetime import timezone
                
                source = payload.source if payload.source else "TradingView webhook Exit"
                now_utc = datetime.now(timezone.utc)
                local_dt = now_utc.astimezone(telegram_notifier.local_timezone)
                eastern_dt = now_utc.astimezone(ZoneInfo("America/New_York"))
                local_label = local_dt.tzname() or "Local"
                eastern_label = eastern_dt.tzname() or "ET"
                time_line = (
                    f"{local_dt.strftime('%I:%M %p')} {local_label}"
                    f" ({eastern_dt.strftime('%I:%M %p')} {eastern_label})"
                )
                
                mode_text = f"{account.mode.upper()} Mode" if account.mode else "Mode Unknown"
                symbol_display = symbol  # Will be normalized by send_alert if needed
                
                alert_message = (
                    "====================================================================\n"
                    "\n"
                    f"📥 <b>EXIT SIGNAL RECEIVED</b> | {mode_text}\n"
                    f"          Time: {time_line}\n"
                    "\n"
                    f"Account: {account.label}\n"
                    f"Strategy: {strategy.name}\n"
                    f"Source: {source}\n"
                    f"Symbol: {symbol_display}\n"
                    "\n"
                    f"⚠️ <b>No positions found to close</b>\n"
                    "\n"
                    "The exit signal was received, but there are no open positions for this symbol.\n"
                    "\n"
                    "This may occur if:\n"
                    "• Position was already closed\n"
                    "• Position was closed manually\n"
                    "• State synchronization issue"
                )
                await telegram_notifier.send_alert(
                    alert_message,
                    title="Exit Signal - No Positions",
                    priority="info"
                )
                logger.info(f"✅ Sent informational alert for exit signal with no positions to close")
            except Exception as alert_error:
                logger.error(
                    f"❌ Failed to send exit signal alert for {symbol} "
                    f"(account: {account.label}): {alert_error}",
                    exc_info=True
                )
            
            return OrderResponse(
                status="rejected",
                account_id=account.id,
                client_order_id=payload.get_client_order_id(),
                symbol=symbol,
                side="CLOSE",
                quantity=0,
                timestamp=datetime.utcnow().isoformat(),
                broker=account.broker,
                strategy=strategy.name,
                error="No partition positions to close"
            )

        average_price = weighted_price_sum / total_quantity if total_quantity > 0 else current_price

        logger.info(f"✅ Partition exit complete. Partitions closed: {closed_partitions}. Aggregated P&L: ${aggregated_pnl:+.2f}")

        # In Live mode, sync with broker after each trade to ensure accuracy
        # This reconciles any differences between calculated P&L and actual broker balance
        if account.mode == "LIVE":
            logger.info(f"💰 LIVE MODE: Syncing with broker after trade close to reconcile P&L and commissions")
            await sync_scheduler.sync_account(account.id, broker=broker)
        elif partition_manager.account_is_flat(account.id):
            logger.info(f"🧮 Account {account.id} is flat. Triggering immediate broker sync.")
            await sync_scheduler.sync_account(account.id, broker=broker)

        overall_side = partition_notifications[0].get('position_side') if partition_notifications else None

        overall_pct = (aggregated_pnl / total_virtual_balance_before * 100) if total_virtual_balance_before > 0 else None
        
        # Get total account balance for accurate account-level P&L percentage
        total_account_balance = None
        try:
            account_state = partition_manager.partition_state.get(account.id, {})
            real_account = account_state.get('real_account', {})
            total_account_balance = real_account.get('total_balance', 0.0)
        except Exception as e:
            logger.debug(f"Could not get total account balance for P&L calculation: {e}")

        # Determine source for exit alerts
        source = payload.source if payload.source else "TradingView webhook Exit"
        
        # Extract exit reason from payload if available
        exit_reason = getattr(payload, 'exit_reason', None)
        
        # Send execution alert with error handling
        try:
            alert_sent = await telegram_notifier.send_execution_alert(
                account_label=account.label,
                action="EXIT",
                symbol=symbol,
                quantity=total_quantity,
                price=average_price,
                leverage=None,
                mode=account.mode,
                strategy=strategy.name,
                position_side=overall_side,
                partition_summaries=partition_notifications,
                pnl_usd=aggregated_pnl,
                pnl_pct=overall_pct,
                capital_capacity=total_virtual_balance_before if total_virtual_balance_before > 0 else None,
                total_account_balance=total_account_balance,
                source=source,
                exit_reason=exit_reason
            )
            if not alert_sent:
                logger.warning(
                    f"⚠️ Exit alert NOT sent for {symbol} "
                    f"(account: {account.label}) - Telegram may be disabled or failed"
                )
        except Exception as alert_error:
            logger.error(
                f"❌ Failed to send exit alert for {symbol} "
                f"(account: {account.label}): {alert_error}",
                exc_info=True
            )
            # Don't fail execution if alert fails, but log it

        # Calculate holding time from first closed position
        holding_time_seconds = None
        if partition_notifications:
            first_notification = partition_notifications[0]
            if first_notification.get('holding_seconds'):
                holding_time_seconds = first_notification['holding_seconds']
        
        # Get entry price from first closed position
        entry_price = None
        if partition_notifications:
            first_notification = partition_notifications[0]
            entry_price = first_notification.get('entry_price')
        
        # Ensure peak profit is included in OrderResponse for Historical Enhancer
        # This will be picked up by main.py's _append_historical_enhancer_event
        response_historical_enhancer = None
        if payload.historical_enhancer:
            response_historical_enhancer = payload.historical_enhancer.copy() if isinstance(payload.historical_enhancer, dict) else payload.historical_enhancer
        
        return OrderResponse(
            status="success",
            account_id=account.id,
            order_id=f"{payload.get_client_order_id()}-partition-exit",
            client_order_id=payload.get_client_order_id(),
            symbol=symbol,
            side="CLOSE",
            quantity=total_quantity,
            price=average_price,
            fill_price=average_price,
            commission=aggregated_commission or None,
            timestamp=datetime.utcnow().isoformat(),
            broker=account.broker,
            strategy=strategy.name,
            leverage=executed_leverage if executed_leverage is not None else account.leverage,
            capital_deployed=total_capital_deployed if total_capital_deployed > 0 else None,
            historical_enhancer=response_historical_enhancer,  # Include peak profit data for Historical Enhancer
            # P&L data for Historical Enhancer
            pnl_usd=aggregated_pnl,
            pnl_pct=overall_pct,
            entry_price=entry_price,
            holding_time_seconds=holding_time_seconds
        )

    async def _execute_on_account(self, payload: WebhookPayload, account, strategy) -> OrderResponse:
        """
        Execute signal on a single broker account
        
        Args:
            payload: Webhook payload
            account: AccountConfig from YAML
            strategy: StrategyConfig from YAML
        
        Returns:
            OrderResponse with execution result
        """
        account_id = account.id
        mode = account.mode
        broker_type = account.broker
        leverage = account.leverage
        label = account.label
        symbol = strategy.execution.product_id
        
        logger.info(f"")
        logger.info(f"{'═'*60}")
        logger.info(f"🔄 EXECUTING: {label}")
        logger.info(f"   Broker: {broker_type} | Mode: {mode} | Leverage: {leverage}x")
        logger.info(f"{'═'*60}")
        
        # Get broker instance
        broker = self.account_brokers.get(account_id)
        if not broker:
            raise Exception(f"Broker not initialized for Account '{account_id}'")
        
        # CONTRACT EXPIRY CHECK (for entry signals)
        if payload.is_entry():
            allowed, message = should_allow_entry(symbol)
            if not allowed:
                logger.error(f"❌ CONTRACT EXPIRY: {message}")
                await telegram_notifier.send_critical_failure(
                    f"🚨 ENTRY BLOCKED - CONTRACT EXPIRING!\n\n"
                    f"Account: {label}\n"
                    f"Symbol: {symbol}\n"
                    f"Reason: {message}\n\n"
                    f"⚠️ ACTION REQUIRED:\n"
                    f"1. Close open positions on this symbol\n"
                    f"2. Roll to new contract on TradingView\n"
                    f"3. Update alerts for new contract"
                )
                return OrderResponse(
                    status="rejected",
                    account_id=account_id,
                    client_order_id=payload.get_client_order_id(),
                    symbol=symbol,
                    side=payload.get_side(),
                    quantity=0,
                    timestamp=datetime.utcnow().isoformat(),
                    broker=broker_type,
                    strategy=strategy.name,
                    error=f"Contract expiry: {message}"
                )

        if self._partition_mode_enabled(account):
            return await self._execute_partition_trade(payload, account, strategy, broker)
        
        # DEMO MODE: Simulate execution internally
        if mode == "DEMO":
            return await self._execute_demo(payload, account, broker, symbol, strategy.name)
        
        # LIVE MODE: Execute on real broker
        return await self._execute_live(payload, account, broker, symbol, strategy.name)
    
    async def _execute_demo(self, payload: WebhookPayload, account, broker, symbol: str, strategy_name: str) -> OrderResponse:
        """
        Execute in DEMO mode (virtual P&L tracking with balance updates)
        
        V3.5 DEMO System:
        - Tracks balance per account (starts at $1000)
        - Updates balance after each trade (compounding)
        - Maintains trade history
        - Calculates cumulative P&L
        - Provides API endpoints for monitoring
        """
        account_id = account.id
        leverage = account.leverage
        label = account.label
        broker_type = account.broker
        
        # Ensure DEMO account is initialized
        if account_id not in self.demo_accounts:
            self._initialize_demo_account(account_id, label)
        
        demo_account = self.demo_accounts[account_id]
        account_positions = self.demo_positions[account_id]
        
        logger.info(f"📊 DEMO MODE: Simulating {payload.side} on {label}")
        logger.info(f"   Current Balance: ${demo_account['current_balance']:,.2f}")
        
        # Get current price from broker (or fallback to payload)
        try:
            from services.exit_monitor import format_price
            current_price = broker.get_current_price(symbol)
            logger.info(f"   Current Price: {format_price(current_price, symbol)}")
        except:
            from services.exit_monitor import format_price
            current_price = payload.price or 100000.0  # Fallback
            logger.info(f"   Current Price: {format_price(current_price, symbol)}")
        
        # Execute based on action
        if payload.is_entry():
            # Calculate position size using CURRENT BALANCE (not hardcoded!)
            current_balance = demo_account['current_balance']
            position_value = current_balance * 0.90  # 90% of equity
            quantity = position_value / current_price
            
            # Calculate commission (0.02% on entry)
            commission_rate = demo_account['commission_rate']
            entry_commission = position_value * commission_rate
            
            # Store DEMO position
            account_positions[symbol] = {
                'side': 'LONG' if payload.is_long() else 'SHORT',
                'quantity': quantity,
                'entry_price': current_price,
                'leverage': leverage,
                'entry_time': datetime.utcnow().isoformat(),
                'strategy': strategy_name,
                'balance_at_entry': current_balance,
                'entry_commission': entry_commission
            }
            
            from services.exit_monitor import format_price
            logger.info(f"✅ DEMO: Opened {payload.get_side()} position")
            logger.info(f"   Entry Price: {format_price(current_price, symbol)}")
            logger.info(f"   Quantity: {quantity:.6f} BTC")
            logger.info(f"   Position Value: ${position_value:,.2f}")
            logger.info(f"   Leverage: {leverage}x")
            logger.info(f"   Entry Commission: ${entry_commission:.2f} (0.02%)")
            
            await state_store.save_demo_account(
                account_id,
                self.demo_accounts[account_id],
                self.demo_positions[account_id],
                self.demo_trade_history[account_id]
            )
            
            return OrderResponse(
                status="success",
                account_id=account_id,
                order_id=f"demo_{int(time.time())}",
                client_order_id=payload.get_client_order_id(),
                symbol=symbol,
                side=payload.get_side(),
                quantity=quantity,
                price=current_price,
                fill_price=current_price,
                timestamp=datetime.utcnow().isoformat(),
                broker=f"{broker_type}_DEMO",
                strategy=strategy_name
            )
        
        elif payload.is_exit():
            # Close DEMO position and UPDATE BALANCE
            if symbol in account_positions:
                position = account_positions[symbol]
                
                # Calculate realized P&L
                entry_price = position['entry_price']
                quantity = position['quantity']
                balance_at_entry = position['balance_at_entry']
                entry_commission = position.get('entry_commission', 0.0)
                
                if position['side'] == 'LONG':
                    # LONG: P&L = (exit - entry) / entry
                    pnl_pct_gross = ((current_price - entry_price) / entry_price) * 100 * leverage
                else:  # SHORT
                    # SHORT: P&L = (entry - exit) / entry
                    pnl_pct_gross = ((entry_price - current_price) / entry_price) * 100 * leverage
                
                # Calculate gross P&L in USD (before commission)
                # Use balance_at_entry (partition balance) - this is what compounds after each trade
                # P&L % is already calculated with leverage, so apply to partition balance
                pnl_usd_gross = balance_at_entry * (pnl_pct_gross / 100)
                
                # Calculate exit commission (0.02% of position value at exit)
                commission_rate = demo_account['commission_rate']
                position_value_at_exit = quantity * current_price
                exit_commission = position_value_at_exit * commission_rate
                
                # Total commission (entry + exit)
                total_commission = entry_commission + exit_commission
                
                # NET P&L (after commissions)
                pnl_usd_net = pnl_usd_gross - total_commission
                # P&L % is calculated on partition balance (balance_at_entry)
                pnl_pct_net = (pnl_usd_net / balance_at_entry) * 100
                
                # UPDATE BALANCE (compounding with commission impact!)
                new_balance = balance_at_entry + pnl_usd_net
                old_balance = demo_account['current_balance']
                demo_account['current_balance'] = new_balance
                demo_account['total_pnl'] += pnl_usd_net
                demo_account['total_pnl_pct'] = ((new_balance - demo_account['starting_balance']) / demo_account['starting_balance']) * 100
                demo_account['total_trades'] += 1
                demo_account['total_commission_paid'] += total_commission
                demo_account['last_updated'] = datetime.utcnow().isoformat()
                
                # UPDATE PEAK BALANCE & CALCULATE DRAWDOWN
                if new_balance > demo_account['peak_balance']:
                    demo_account['peak_balance'] = new_balance
                
                # Calculate current drawdown from peak
                current_drawdown_usd = demo_account['peak_balance'] - new_balance
                current_drawdown_pct = (current_drawdown_usd / demo_account['peak_balance']) * 100 if demo_account['peak_balance'] > 0 else 0.0
                
                # Update max drawdown if current is larger
                if current_drawdown_usd > demo_account['max_drawdown_usd']:
                    demo_account['max_drawdown_usd'] = current_drawdown_usd
                    demo_account['max_drawdown_pct'] = current_drawdown_pct
                
                # Update win/loss stats (based on NET P&L)
                if pnl_usd_net > 0:
                    demo_account['winning_trades'] += 1
                    demo_account['current_streak'] = max(1, demo_account['current_streak'] + 1) if demo_account['current_streak'] >= 0 else 1
                    demo_account['largest_win'] = max(demo_account['largest_win'], pnl_usd_net)
                else:
                    demo_account['losing_trades'] += 1
                    demo_account['current_streak'] = min(-1, demo_account['current_streak'] - 1) if demo_account['current_streak'] <= 0 else -1
                    demo_account['largest_loss'] = min(demo_account['largest_loss'], pnl_usd_net)
                
                # Update max streaks
                if demo_account['current_streak'] > 0:
                    demo_account['max_win_streak'] = max(demo_account['max_win_streak'], demo_account['current_streak'])
                elif demo_account['current_streak'] < 0:
                    demo_account['max_loss_streak'] = max(demo_account['max_loss_streak'], abs(demo_account['current_streak']))
                
                # Add to trade history (with commission details)
                trade_record = {
                    'trade_id': len(self.demo_trade_history[account_id]) + 1,
                    'timestamp': datetime.utcnow().isoformat(),
                    'side': position['side'],
                    'symbol': symbol,
                    'entry_price': entry_price,
                    'exit_price': current_price,
                    'quantity': quantity,
                    'leverage': leverage,
                    'pnl_usd_gross': pnl_usd_gross,
                    'pnl_pct_gross': pnl_pct_gross,
                    'pnl_usd_net': pnl_usd_net,
                    'pnl_pct_net': pnl_pct_net,
                    'entry_commission': entry_commission,
                    'exit_commission': exit_commission,
                    'total_commission': total_commission,
                    'balance_before': balance_at_entry,
                    'balance_after': new_balance,
                    'strategy': strategy_name,
                    'entry_time': position['entry_time'],
                    'exit_time': datetime.utcnow().isoformat()
                }
                self.demo_trade_history[account_id].append(trade_record)
                
                # Remove from open positions
                del account_positions[symbol]
                
                from services.exit_monitor import format_price
                logger.info(f"✅ DEMO: Closed {position['side']} position")
                logger.info(f"   Entry: {format_price(entry_price, symbol)}")
                logger.info(f"   Exit: {format_price(current_price, symbol)}")
                logger.info(f"   Gross P&L: ${pnl_usd_gross:+,.2f} ({pnl_pct_gross:+.2f}%)")
                logger.info(f"   Commission: -${total_commission:.2f} (Entry: ${entry_commission:.2f} + Exit: ${exit_commission:.2f})")
                logger.info(f"   NET P&L: ${pnl_usd_net:+,.2f} ({pnl_pct_net:+.2f}%)")
                logger.info(f"   Balance: ${old_balance:,.2f} → ${new_balance:,.2f}")
                logger.info(f"   Total P&L: ${demo_account['total_pnl']:+,.2f} ({demo_account['total_pnl_pct']:+.2f}%)")
                logger.info(f"   Total Commission Paid: ${demo_account['total_commission_paid']:,.2f}")
                logger.info(f"   Max Drawdown: ${demo_account['max_drawdown_usd']:,.2f} ({demo_account['max_drawdown_pct']:.2f}%)")
                logger.info(f"   Win Rate: {demo_account['winning_trades']}/{demo_account['total_trades']} ({(demo_account['winning_trades']/demo_account['total_trades']*100):.1f}%)")
                
                await state_store.save_demo_account(
                    account_id,
                    self.demo_accounts[account_id],
                    self.demo_positions[account_id],
                    self.demo_trade_history[account_id]
                )
                
                return OrderResponse(
                    status="success",
                    account_id=account_id,
                    order_id=f"demo_close_{int(time.time())}",
                    client_order_id=payload.get_client_order_id(),
                    symbol=symbol,
                    side="CLOSE",
                    quantity=position['quantity'],
                    price=current_price,
                    fill_price=current_price,
                    timestamp=datetime.utcnow().isoformat(),
                    broker=f"{broker_type}_DEMO",
                    strategy=strategy_name
                )
            else:
                logger.warning(f"⚠️ DEMO: No position to close for {symbol}")
                return OrderResponse(
                    status="rejected",
                    account_id=account_id,
                    client_order_id=payload.get_client_order_id(),
                    symbol=symbol,
                    side="CLOSE",
                    quantity=0,
                    timestamp=datetime.utcnow().isoformat(),
                    broker=f"{broker_type}_DEMO",
                    strategy=strategy_name,
                    error="No position to close"
                )
    
    async def _execute_live(self, payload: WebhookPayload, account, broker, symbol: str, strategy_name: str) -> OrderResponse:
        """
        Execute in LIVE mode (real broker execution)
        
        Executes on real broker with retry logic
        """
        account_id = account.id
        leverage = account.leverage
        label = account.label
        broker_type = account.broker
        
        logger.info(f"💰 LIVE MODE: Executing {payload.side} on {label}")
        
        # Get current price
        try:
            current_price = broker.get_current_price(symbol)
            logger.info(f"   Current Price: {format_price(current_price, symbol)}")
        except Exception as e:
            logger.error(f"❌ Failed to get current price: {str(e)}")
            current_price = payload.price or 0
        
        existing_notional = 0.0
        if current_price > 0 and hasattr(broker, "get_position"):
            try:
                current_position = broker.get_position(symbol)
                if current_position:
                    existing_qty = abs(current_position.get('quantity', 0))
                    existing_notional = existing_qty * current_price
            except Exception as e:
                logger.debug(f"Unable to fetch existing position for ADV cap: {e}")
        
        adv_cap_note = None

        # Calculate position size
        try:
            account_balance = broker.get_account_balance()
            available = account_balance.get('available_balance', 0)
            
            # Use 90% of equity (matches backtest)
            position_value = available * (self.settings.EQUITY_PERCENTAGE / 100)
            requested_collateral = position_value

            # Apply ADV Cap (Slip Guard) using risk manager
            if self.settings.ENABLE_ADV_CAP and self.risk_manager:
                adv_result = await self.risk_manager.apply_adv_cap(
                    symbol=symbol,
                    broker=account.broker,
                    requested_qty_usd=position_value,
                    leverage=leverage,
                    current_exposure_usd=existing_notional
                )

                approved_notional = adv_result.get(
                    'approved_notional_usd',
                    position_value * max(leverage, 1)
                )
                max_allowed = adv_result.get('max_allowed_usd')
                adv_usd = adv_result.get('adv_usd')
                if max_allowed and adv_usd:
                    try:
                        adv_cap_limit_pct = (max_allowed / adv_usd) * 100.0
                    except ZeroDivisionError:
                        adv_cap_limit_pct = None

                effective_leverage = leverage if leverage else 1
                allowed_notional = max(0.0, approved_notional)

                if allowed_notional < requested_collateral * effective_leverage:
                    # Reduce leverage stepwise down toward 1x before cutting collateral
                    new_leverage = effective_leverage
                    if requested_collateral > 0:
                        desired_leverage = allowed_notional / requested_collateral if requested_collateral else effective_leverage
                    else:
                        desired_leverage = effective_leverage

                    if new_leverage > 1:
                        if desired_leverage < 1:
                            new_leverage = 1
                            position_value = allowed_notional
                        else:
                            new_leverage = max(1, min(new_leverage, int(math.floor(desired_leverage))))
                            position_value = allowed_notional / new_leverage if new_leverage else 0
                    else:
                        new_leverage = 1
                        position_value = min(requested_collateral, allowed_notional)

                    leverage = max(new_leverage, 1)

                    if position_value > requested_collateral:
                        position_value = requested_collateral

                    detail_reason = adv_result.get('reason', 'ADV cap applied')
                    pct_text = f"{adv_cap_limit_pct:.2f}%" if adv_cap_limit_pct is not None else f"{self.settings.ADV_CAP_PCT:.2f}%"
                    adv_cap_note = f"*Position orders capped at {pct_text} of ADV for {symbol}* ({detail_reason})"
                else:
                    position_value = allowed_notional / max(leverage, 1)
            elif self.settings.ENABLE_ADV_CAP and not self.risk_manager:
                logger.warning("ADV cap enabled but RiskManager not available – skipping cap")

            quantity = position_value / current_price if current_price > 0 else 0

            if quantity <= 0:
                logger.warning("⚠️ ADV cap left no executable size for this account")
                detail_reason = adv_result.get('reason', 'ADV cap reached') if self.settings.ENABLE_ADV_CAP and self.risk_manager else 'ADV cap reached'
                if adv_cap_note is None:
                    pct_text = f"{adv_cap_limit_pct:.2f}%" if adv_cap_limit_pct is not None else f"{self.settings.ADV_CAP_PCT:.2f}%"
                    adv_cap_note = f"*Position orders capped at {pct_text} of ADV for {symbol}* ({detail_reason})"

                # Determine source for entry alerts
                source = payload.source if payload.source else "TradingView webhook Entry"
                
                try:
                    alert_sent = await telegram_notifier.send_execution_alert(
                        account_label=label,
                        action="BUY" if payload.is_long() else "SELL",
                        symbol=symbol,
                        quantity=0,
                        price=current_price,
                        leverage=leverage,
                        mode=account.mode,
                        strategy=strategy_name,
                        position_side="LONG" if payload.is_long() else "SHORT",
                        partition_summaries=[],
                        adv_cap_note=adv_cap_note,
                        account_id=account_id,  # CRITICAL: Pass account_id for portfolio P&L section
                        source=source
                    )
                    if not alert_sent:
                        logger.warning(f"⚠️ Execution alert NOT sent for {symbol} (ADV capped) - Telegram may be disabled or failed")
                except Exception as alert_error:
                    logger.error(f"❌ Failed to send execution alert (ADV capped) for {symbol}: {alert_error}", exc_info=True)

                return OrderResponse(
                    status="success",
                    order_id=f"{payload.get_client_order_id()}-adv-capped",
                    client_order_id=payload.get_client_order_id(),
                    symbol=symbol,
                    side=payload.get_side(),
                    quantity=0,
                    price=current_price,
                    fill_price=current_price,
                    timestamp=datetime.utcnow().isoformat(),
                    broker=broker_type,
                    strategy=strategy_name,
                    leverage=leverage
                )

            # Check minimum position size
            min_size_usd = self.settings.MIN_POSITION_SIZE_USD
            if quantity * current_price < min_size_usd:
                logger.warning(f"⚠️ Position too small (${quantity * current_price:.2f} < ${min_size_usd})")
                if not self.settings.ALLOW_ADV_CAP_BELOW_MINIMUM:
                    return OrderResponse(
                        status="rejected",
                        account_id=account_id,
                        client_order_id=payload.get_client_order_id(),
                        symbol=symbol,
                        side=payload.get_side(),
                        quantity=0,
                        timestamp=datetime.utcnow().isoformat(),
                        broker=broker_type,
                        strategy=strategy_name,
                        error=f"Position too small (${quantity * current_price:.2f} < ${min_size_usd})"
                    )
            
            logger.info(f"   Position Size: {quantity:.6f} ({position_value:,.2f} USD)")
            logger.info(f"   Leverage: {leverage}x")
            
        except Exception as e:
            raise
        
        # Execute based on action with retry logic
        max_retries = self.settings.MAX_RETRIES_ENTRY if payload.is_entry() else self.settings.MAX_RETRIES_EXIT
        
        for attempt in range(1, max_retries + 1):
            try:
                if payload.is_entry():
                    # ENTRY: LONG or SHORT
                    side = "BUY" if payload.is_long() else "SELL"
                    
                    logger.info(f"🔵 Attempt {attempt}/{max_retries}: {side} {quantity:.6f} {symbol}")
                    
                    order = broker.place_market_order(
                        symbol=symbol,
                        side=side,
                        quantity=quantity,
                        client_order_id=payload.get_client_order_id(),
                        leverage=leverage
                    )
                    
                    from services.exit_monitor import format_price
                    fill_price = order.get('price') or current_price
                    
                    logger.info(f"✅ ENTRY FILLED: {side} {quantity:.6f} @ {format_price(fill_price, symbol)}")
                    
                    # Set SL/TP if provided in alert (optional)
                    if payload.tp_price or payload.sl_price or payload.tp_pct or payload.sl_pct:
                        await self._set_optional_sltp(payload, broker, account, quantity, fill_price, symbol)
                    else:
                        logger.info(f"ℹ️  No SL/TP provided in alert (normal - TradingView manages exits)")
                    
                    # Send Telegram notification
                    position_side = "LONG" if payload.is_long() else "SHORT"
                    single_partition_summary = [{
                        'id': label or account_id,
                        'allocation_pct': 100.0,
                        'quantity': quantity,
                        'price': fill_price,
                        'leverage': leverage,
                        'position_side': position_side
                    }]

                    # Determine source for entry alerts
                    source = payload.source if payload.source else "TradingView webhook Entry"
                    
                    try:
                        alert_sent = await telegram_notifier.send_execution_alert(
                            account_label=label,
                            action=side,
                            symbol=symbol,
                            quantity=quantity,
                            price=fill_price,
                            leverage=leverage,
                            mode=account.mode,
                            strategy=strategy_name,
                            position_side=position_side,
                            partition_summaries=single_partition_summary,
                            adv_cap_note=adv_cap_note,
                            account_id=account_id,  # CRITICAL: Pass account_id for portfolio P&L section
                            source=source
                        )
                        if not alert_sent:
                            logger.warning(f"⚠️ Execution alert NOT sent for {symbol} {side} (single partition) - Telegram may be disabled or failed")
                    except Exception as alert_error:
                        logger.error(f"❌ Failed to send execution alert (single partition) for {symbol} {side}: {alert_error}", exc_info=True)
                    
                    return OrderResponse(
                        status="success",
                        account_id=account_id,
                        order_id=order.get('orderId'),
                        client_order_id=payload.get_client_order_id(),
                        symbol=symbol,
                        side=side,
                        quantity=quantity,
                        price=fill_price,
                        fill_price=fill_price,
                        timestamp=datetime.utcnow().isoformat(),
                        broker=broker_type,
                        strategy=strategy_name
                    )
                
                elif payload.is_exit():
                    # EXIT: Close position
                    logger.info(f"🔴 Attempt {attempt}/{max_retries}: CLOSE {symbol}")
                    
                    # Get current position
                    position = broker.get_position(symbol)
                    if position.get('quantity', 0) == 0:
                        logger.warning(f"⚠️ No position to close for {symbol} - position manually closed or already flat")
                        
                        # Position was manually closed - we still need to clean up internal state
                        # Get current market price for P&L calculation
                        try:
                            current_price = broker.get_current_price(symbol) or payload.price or 0
                        except:
                            current_price = payload.price or 0
                        
                        # Return success status with manual closure flag
                        return OrderResponse(
                            status="completed",
                            account_id=account_id,
                            client_order_id=payload.get_client_order_id(),
                            symbol=symbol,
                            side="CLOSE",
                            quantity=0,
                            price=current_price,
                            timestamp=datetime.utcnow().isoformat(),
                            broker=broker_type,
                            strategy=strategy_name,
                            error=None,
                            execution={'manual_closure': True, 'cleanup_required': True}
                        )
                    
                    position_qty = position['quantity']
                    position_side = position['side']
                    
                    # Determine close side (opposite of position)
                    close_side = "SELL" if position_side == "LONG" else "BUY"
                    
                    # Close position
                    order = broker.close_position_market(
                        symbol=symbol,
                        client_order_id=payload.get_client_order_id()
                    )
                    
                    from services.exit_monitor import format_price
                    fill_price = order.get('price') or current_price
                    
                    logger.info(f"✅ EXIT FILLED: {close_side} {position_qty:.6f} @ {format_price(fill_price, symbol)}")
                    
                    # Calculate P&L
                    entry_price = position.get('entry_price', fill_price)
                    if position_side == "LONG":
                        pnl_pct = ((fill_price - entry_price) / entry_price) * 100 * leverage
                    else:
                        pnl_pct = ((entry_price - fill_price) / entry_price) * 100 * leverage
                    
                    if pnl_pct != 0:
                        from services.exit_monitor import format_price
                        logger.info(f"   Entry: {format_price(entry_price, symbol)}")
                        logger.info(f"   Exit: {format_price(fill_price, symbol)}")
                        logger.info(f"   P&L: {pnl_pct:+.2f}%")
                    
                    # Send Telegram notification
                    pnl_usd = None
                    if position_side == "LONG":
                        pnl_usd = (fill_price - entry_price) * position_qty * leverage
                    elif position_side == "SHORT":
                        pnl_usd = (entry_price - fill_price) * position_qty * leverage

                    single_partition_summary = [{
                        'id': label or account_id,
                        'allocation_pct': 100.0,
                        'quantity': position_qty,
                        'price': fill_price,
                        'leverage': leverage,
                        'position_side': position_side,
                        'entry_price': entry_price,
                        'exit_price': fill_price,
                        'pnl_usd': pnl_usd,
                        'pnl_pct': pnl_pct
                    }]

                    # Determine source for exit alerts
                    source = payload.source if payload.source else "TradingView webhook Exit"
                    
                    # Extract exit reason from payload if available
                    exit_reason = getattr(payload, 'exit_reason', None)
                    
                    try:
                        alert_sent = await telegram_notifier.send_execution_alert(
                            account_label=label,
                            action="EXIT",
                            symbol=symbol,
                            quantity=position_qty,
                            price=fill_price,
                            leverage=leverage,
                            mode=account.mode,
                            strategy=strategy_name,
                            position_side=position_side,
                            entry_price=entry_price,
                            exit_price=fill_price,
                            pnl_usd=pnl_usd,
                            pnl_pct=pnl_pct,
                            partition_summaries=single_partition_summary,
                            source=source,
                            exit_reason=exit_reason
                        )
                        if not alert_sent:
                            logger.warning(f"⚠️ Exit alert NOT sent for {symbol} (single partition) - Telegram may be disabled or failed")
                    except Exception as alert_error:
                        logger.error(f"❌ Failed to send exit alert (single partition) for {symbol}: {alert_error}", exc_info=True)
                    
                    return OrderResponse(
                        status="success",
                        account_id=account_id,
                        order_id=order.get('orderId'),
                        client_order_id=payload.get_client_order_id(),
                        symbol=symbol,
                        side="CLOSE",
                        quantity=position_qty,
                        price=fill_price,
                        fill_price=fill_price,
                        timestamp=datetime.utcnow().isoformat(),
                        broker=broker_type,
                        strategy=strategy_name
                    )
            
            except Exception as e:
                logger.error(f"❌ Attempt {attempt} failed: {str(e)}")
                
                if attempt < max_retries:
                    delay = 2 ** attempt  # Exponential backoff: 2s, 4s, 8s
                    logger.info(f"⏳ Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                else:
                    # All retries exhausted
                    logger.error(f"❌ All {max_retries} retries exhausted")
                    
                    # Send critical failure alert
                    await telegram_notifier.send_critical_failure(
                        f"🚨 ORDER FAILED - ALL RETRIES EXHAUSTED!\n\n"
                        f"Account: {label}\n"
                        f"Action: {payload.side.upper()}\n"
                        f"Symbol: {symbol}\n"
                        f"Retries: {max_retries}\n"
                        f"Error: {str(e)}\n\n"
                        f"⚠️ MANUAL INTERVENTION REQUIRED!"
                    )
                    
                    raise
    
    async def _set_optional_sltp(self, payload: WebhookPayload, broker, account, quantity: float, entry_price: float, symbol: str):
        """
        Set SL/TP on broker if provided in alert (optional feature)
        
        Most TradingView alerts won't include SL/TP levels.
        This is only used if the alert specifically includes them.
        """
        account_id = account.id
        label = account.label
        leverage = account.leverage
        
        tp_price = None
        sl_price = None
        
        # Calculate TP/SL prices if percentages provided
        if payload.tp_pct:
            if payload.is_long():
                tp_price = entry_price * (1 + abs(payload.tp_pct) / 100)
            else:
                tp_price = entry_price * (1 - abs(payload.tp_pct) / 100)
        elif payload.tp_price:
            tp_price = payload.tp_price
        
        if payload.sl_pct:
            if payload.is_long():
                sl_price = entry_price * (1 - abs(payload.sl_pct) / 100)
            else:
                sl_price = entry_price * (1 + abs(payload.sl_pct) / 100)
        elif payload.sl_price:
            sl_price = payload.sl_price
        
        
        # Set SL/TP if provided
        try:
            if tp_price:
                from services.exit_monitor import format_price
                side = "SELL" if payload.is_long() else "BUY"
                broker.place_take_profit(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    take_profit_price=tp_price,
                    client_order_id=f"{payload.get_client_order_id()}_TP",
                    leverage=leverage
                )
                logger.info(f"✅ Take Profit set @ {format_price(tp_price, symbol)}")
            
            if sl_price:
                from services.exit_monitor import format_price
                side = "SELL" if payload.is_long() else "BUY"
                broker.place_stop_loss(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    stop_price=sl_price,
                    client_order_id=f"{payload.get_client_order_id()}_SL",
                    leverage=leverage
                )
                logger.info(f"✅ Stop Loss set @ {format_price(sl_price, symbol)}")
        
        except Exception as e:
            logger.warning(f"⚠️ Failed to set SL/TP: {str(e)}")
            logger.info(f"   TradingView will manage exits via EXIT signals")
    
    # ============================================
    # DEMO ACCOUNT MANAGEMENT METHODS (V3.5)
    # ============================================
    
    def get_demo_account_balance(self, account_id: str) -> Dict:
        """
        Get current DEMO account balance and stats
        
        Enhanced to sync with partition system for accurate balance reporting.
        
        Returns:
            {
                'account_id': str,
                'label': str,
                'current_balance': float,
                'starting_balance': float,
                'total_pnl': float,
                'total_pnl_pct': float,
                'roi': float,
                'total_trades': int,
                'winning_trades': int,
                'losing_trades': int,
                'win_rate': float,
                'largest_win': float,
                'largest_loss': float
            }
        """
        if account_id not in self.demo_accounts:
            return {
                'error': f'DEMO account {account_id} not found',
                'account_id': account_id,
                'current_balance': 0.0
            }
        
        account = self.demo_accounts[account_id]
        
        # CRITICAL: Sync with partition system for accurate balance
        try:
            from services.partition_manager import partition_state
            
            # Get partition state for this account
            state = partition_state.get(account_id)
            if state:
                # Calculate total from partitions (most accurate)
                partitions = state.get('partitions', {})
                total_virtual_balance = sum(
                    partition_data.get('virtual_balance', 0.0) 
                    for partition_data in partitions.values()
                )
                
                if total_virtual_balance > 0:
                    # Update demo account balance to match partition total
                    account['current_balance'] = total_virtual_balance
                    logger.debug(f"📊 Synced demo balance with partitions: ${total_virtual_balance:,.2f}")
        except Exception as e:
            logger.debug(f"Could not sync demo balance with partitions: {e}")
        
        win_rate = (account['winning_trades'] / account['total_trades'] * 100) if account['total_trades'] > 0 else 0.0
        
        return {
            'account_id': account['account_id'],
            'label': account['label'],
            'current_balance': account['current_balance'],
            'starting_balance': account['starting_balance'],
            'peak_balance': account['peak_balance'],
            'total_pnl': account['total_pnl'],
            'total_pnl_pct': account['total_pnl_pct'],
            'roi': account['total_pnl_pct'],
            'total_trades': account['total_trades'],
            'winning_trades': account['winning_trades'],
            'losing_trades': account['losing_trades'],
            'win_rate': win_rate,
            'largest_win': account['largest_win'],
            'largest_loss': account['largest_loss'],
            'max_drawdown_usd': account['max_drawdown_usd'],
            'max_drawdown_pct': account['max_drawdown_pct'],
            'total_commission_paid': account['total_commission_paid'],
            'commission_rate': account['commission_rate'],
            'current_streak': account['current_streak'],
            'max_win_streak': account['max_win_streak'],
            'max_loss_streak': account['max_loss_streak'],
            'created_at': account['created_at'],
            'last_updated': account['last_updated']
        }
    
    def get_demo_positions(self, account_id: str) -> Dict:
        """
        Get open DEMO positions for an account
        
        Returns:
            {
                'account_id': str,
                'open_positions': [...],
                'total_unrealized_pnl': float
            }
        """
        if account_id not in self.demo_positions:
            return {
                'account_id': account_id,
                'open_positions': [],
                'total_positions': 0
            }
        
        positions = []
        total_unrealized = 0.0
        
        for symbol, pos in self.demo_positions[account_id].items():
            # Calculate unrealized P&L (would need current price - skip for now)
            positions.append({
                'symbol': symbol,
                'side': pos['side'],
                'quantity': pos['quantity'],
                'entry_price': pos['entry_price'],
                'leverage': pos['leverage'],
                'entry_time': pos['entry_time'],
                'strategy': pos['strategy']
            })
        
        return {
            'account_id': account_id,
            'open_positions': positions,
            'total_positions': len(positions)
        }
    
    def get_demo_trade_history(self, account_id: str, limit: int = 50) -> Dict:
        """
        Get DEMO trade history for an account
        
        Args:
            account_id: Account ID
            limit: Max trades to return (default 50)
        
        Returns:
            {
                'account_id': str,
                'total_trades': int,
                'winning_trades': int,
                'losing_trades': int,
                'win_rate': float,
                'trades': [...]
            }
        """
        if account_id not in self.demo_trade_history:
            return {
                'account_id': account_id,
                'total_trades': 0,
                'trades': []
            }
        
        trades = self.demo_trade_history[account_id]
        account = self.demo_accounts.get(account_id, {})
        
        win_rate = (account.get('winning_trades', 0) / account.get('total_trades', 1) * 100) if account.get('total_trades', 0) > 0 else 0.0
        
        return {
            'account_id': account_id,
            'total_trades': len(trades),
            'winning_trades': account.get('winning_trades', 0),
            'losing_trades': account.get('losing_trades', 0),
            'win_rate': win_rate,
            'trades': trades[-limit:]  # Return last N trades
        }
    
    def reset_demo_account(self, account_id: str) -> Dict:
        """
        Reset DEMO account to starting balance (from config.yaml)
        
        Clears all positions, trade history, and stats
        
        Returns:
            {
                'account_id': str,
                'balance': float,
                'status': str
            }
        """
        if account_id in self.demo_accounts:
            label = self.demo_accounts[account_id]['label']
            self._initialize_demo_account(account_id, label, reset=True)
            logger.info(f"🔄 DEMO Account '{account_id}' reset to $1,000")
            
            return {
                'account_id': account_id,
                'balance': self.settings.DEMO_STARTING_BALANCE,
                'status': 'reset',
                'timestamp': datetime.utcnow().isoformat()
            }
        else:
            return {
                'account_id': account_id,
                'error': 'Account not found',
                'status': 'failed'
            }
    
    def get_all_demo_accounts(self) -> List[Dict]:
        """
        Get all DEMO accounts with balances
        
        Returns:
            List of DEMO account summaries
        """
        return [
            {
                'account_id': acc['account_id'],
                'label': acc['label'],
                'current_balance': acc['current_balance'],
                'peak_balance': acc['peak_balance'],
                'total_pnl': acc['total_pnl'],
                'total_pnl_pct': acc['total_pnl_pct'],
                'total_trades': acc['total_trades'],
                'win_rate': (acc['winning_trades'] / acc['total_trades'] * 100) if acc['total_trades'] > 0 else 0.0,
                'max_drawdown_usd': acc['max_drawdown_usd'],
                'max_drawdown_pct': acc['max_drawdown_pct'],
                'total_commission_paid': acc['total_commission_paid']
            }
            for acc in self.demo_accounts.values()
        ]

    async def _place_market_order_with_retry(
        self,
        *,
        broker,
        symbol: str,
        side: str,
        quantity: float,
        leverage: int,
        client_order_id: str,
        price_override: Optional[float],
        account,
        strategy,
        partition_id: Optional[str],
        action_label: str,
        verify_callback: Optional[Callable[[], Awaitable[Tuple[bool, Optional[str]]]]] = None
    ) -> Tuple[Dict[str, Any], int]:
        """Place a broker order with limited retries, enhanced exit handling, and position reconciliation."""
        initial_delay = getattr(self.settings, "BROKER_RETRY_INITIAL_SECONDS", 30)
        max_delay = getattr(self.settings, "BROKER_RETRY_MAX_SECONDS", 300)
        max_attempts = getattr(self.settings, "BROKER_RETRY_MAX_ATTEMPTS", 3)  # Default to 3 attempts, not unlimited
        delay = max(1, float(initial_delay or 1))
        attempt = 0

        # Enhanced exit handling: Check for manual closure before attempting order
        if action_label == "EXIT" and account.mode == "LIVE":
            try:
                # Check if position still exists on broker
                broker_position = await asyncio.to_thread(broker.get_position, symbol)
                broker_quantity = broker_position.get('quantity', 0) if broker_position else 0
                
                if abs(broker_quantity) < 0.01:  # Position already closed on broker
                    logger.warning(f"🔄 Manual closure detected for {symbol} - position already closed on broker")
                    
                    # Get current market price for internal state cleanup
                    try:
                        current_price = await asyncio.to_thread(broker.get_current_price, symbol)
                        if not current_price:
                            current_price = price_override or 0.0
                    except Exception:
                        current_price = price_override or 0.0
                    
                    # Return simulated successful exit for internal state cleanup
                    return {
                        "status": "filled",
                        "account_id": account.id,
                        "client_order_id": client_order_id,
                        "symbol": symbol,
                        "side": "CLOSE",
                        "quantity": 0,  # No quantity actually traded
                        "price": current_price,
                        "timestamp": datetime.utcnow().isoformat(),
                        "broker": getattr(broker, 'broker_name', 'unknown'),
                        "strategy": strategy.name,
                        "message": "Position already closed on broker - internal state reconciled",
                        "manual_closure_detected": True
                    }, 0
                    
            except Exception as e:
                logger.debug(f"Could not check broker position for manual closure detection: {e}")
                # Continue with normal exit processing

        while max_attempts == 0 or attempt < max_attempts:
            attempt += 1
            try:
                order = broker.place_market_order(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    leverage=leverage,
                    client_order_id=client_order_id,
                    price_override=price_override
                )

                if verify_callback:
                    verified, verify_reason = await verify_callback()
                    if not verified:
                        raise BrokerRetryRequired(verify_reason or "Broker still reports open position")

                return order, attempt

            except BrokerRetryRequired as retry_error:
                reason = str(retry_error) or "Broker verification failed"
            except Exception as exc:
                reason = str(exc)

            try:
                await telegram_notifier.send_broker_blocked_alert(
                    account_label=account.label,
                    symbol=symbol,
                    action=action_label,
                    strategy=strategy.name,
                    mode=account.mode,
                    reason=reason,
                    attempt=attempt,
                    next_retry_seconds=delay,
                    partition_id=partition_id
                )
            except Exception as alert_error:
                logger.error(f"❌ Failed to send broker blocked alert: {alert_error}", exc_info=True)

            if max_attempts and attempt >= max_attempts:
                raise

            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay or delay)
        
        # If we get here, all retries failed
        raise Exception(f"Order failed after {max_attempts} attempts. Last error: {reason}")

    async def _verify_broker_position_closed(self, broker, symbol: str) -> Tuple[bool, Optional[str]]:
        """Check if the broker still reports an open position for the symbol."""
        # In DEMO mode, skip verification - we control position state directly
        if hasattr(broker, 'demo_mode') and broker.demo_mode:
            logger.debug(f"DEMO mode: Skipping broker position verification for {symbol}")
            return True, None
        
        # Also check broker.broker.demo_mode (for broker wrapper)
        if hasattr(broker, 'broker') and hasattr(broker.broker, 'demo_mode') and broker.broker.demo_mode:
            logger.debug(f"DEMO mode: Skipping broker position verification for {symbol}")
            return True, None
        
        state = await self._get_broker_open_position_state(broker, symbol)
        if state is None:
            return True, None
        if state:
            return False, "Broker still reports an open position"
        return True, None

    async def _get_broker_open_position_state(self, broker, symbol: str) -> Optional[bool]:
        """Return True if broker reports an open position, False if flat, None if unknown."""
        target_symbol = (symbol or "").upper()

        def _extract_quantity(data: Dict[str, Any]) -> float:
            for key in ("positionAmt", "position_amt", "quantity", "qty", "position_amt_abs"):
                value = data.get(key)
                if value is not None:
                    try:
                        return abs(float(value))
                    except (TypeError, ValueError):
                        continue
            return 0.0

        def _side_is_none(data: Dict[str, Any]) -> bool:
            side = (data.get("positionSide") or data.get("side") or "").upper()
            return side in ("", "NONE")

        get_position_fn = getattr(broker, "get_position", None)
        if callable(get_position_fn):
            try:
                position = await asyncio.to_thread(get_position_fn, symbol)
                if not position:
                    return False
                qty = _extract_quantity(position)
                if qty <= 0 or _side_is_none(position):
                    return False
                return True
            except Exception as exc:
                logger.debug(f"Unable to fetch broker position for {symbol}: {exc}")

        get_all_fn = getattr(broker, "get_all_positions", None)
        if callable(get_all_fn):
            try:
                positions = await asyncio.to_thread(get_all_fn)
                if not positions:
                    return False
                for pos in positions:
                    pos_symbol = (pos.get("symbol") or pos.get("product_id") or "").upper()
                    if pos_symbol != target_symbol:
                        continue
                    qty = _extract_quantity(pos)
                    if qty > 0:
                        return True
                return False
            except Exception as exc:
                logger.debug(f"Unable to fetch broker positions list: {exc}")

        return None

    def get_partition_overview(self, account_id: Optional[str] = None) -> List[Dict]:
        summaries = partition_manager.get_partition_summaries(account_id)
        for summary in summaries:
            try:
                summary['sync_status'] = sync_scheduler.get_sync_status(summary['account_id'])
            except Exception:
                summary['sync_status'] = None
        return summaries

    def _get_account_config(self, account_id: str) -> Optional[Any]:
        return self.account_configs.get(account_id)

    def _get_account_label(self, account_id: str) -> str:
        config = self._get_account_config(account_id)
        if config and getattr(config, "label", None):
            return config.label
        return account_id

    def clear_commission_cap_violation(self, account_id: Optional[str] = None) -> None:
        """
        Clear commission-cap blocks so new entries can resume after review.
        """
        if account_id:
            state = self._commission_cap_state.get(account_id)
            if state:
                state['blocked'] = False
            return

        for state in self._commission_cap_state.values():
            state['blocked'] = False

    async def get_account_balance_snapshot(self, account_id: str) -> Dict[str, Any]:
        """
        Fetch current broker balance information plus partition state for an account.
        """
        account = self._get_account_config(account_id)
        if not account:
            raise ValueError(f"Account '{account_id}' not found")

        broker = self.account_brokers.get(account_id)
        if not broker:
            raise ValueError(f"Broker for account '{account_id}' not initialized")

        # Ensure partitions are initialized so state reflects Firestore
        await self._ensure_partitions_initialized(account, broker)

        balance_info: Dict[str, Any] = {}
        if hasattr(broker, "get_account_balance"):
            try:
                balance_info = broker.get_account_balance()
            except Exception as exc:
                logger.error(f"Failed to fetch broker balance for {account_id}: {exc}")
                raise

        partition_state_snapshot = partition_manager.partition_state.get(account_id, {})
        partitions = partition_state_snapshot.get("partitions", {})
        partitions_summary: List[Dict[str, Any]] = []
        for pid, data in partitions.items():
            partitions_summary.append({
                "partition_id": pid,
                "virtual_balance": data.get("virtual_balance"),
                "current_balance": data.get("virtual_balance"),
                "allocation_pct": data.get("allocation_pct"),
                "deployed_cash": data.get("deployed_cash", 0.0),
                "open_positions": len(data.get("open_positions") or {})
            })

        return {
            "account_id": account_id,
            "account_label": self._get_account_label(account_id),
            "mode": account.mode,
            "broker_balance": balance_info,
            "partitions": partitions_summary
        }

    async def compute_unrealized_position_pnl(
        self,
        account_id: str,
        position: Dict[str, Any]
    ) -> Tuple[float, float]:
        """
        Compute unrealized P&L (USD and %) for an open position using live prices.
        """
        try:
            symbol = position.get('symbol')
            entry_price = float(position.get('entry_price') or 0.0)
            quantity = float(position.get('quantity') or 0.0)
            cash_used = float(position.get('cash_used') or position.get('total_cash_used') or 0.0)
            side = (position.get('side') or "").upper()

            if not symbol or entry_price <= 0 or quantity == 0:
                return 0.0, 0.0

            broker = self.account_brokers.get(account_id)
            if not broker:
                return 0.0, 0.0

            now = time.time()
            cached = self._price_cache.get(symbol)
            if cached and now - cached[1] < 5:
                current_price = cached[0]
            else:
                loop = asyncio.get_running_loop()
                current_price = 0.0
                for attempt in range(3):
                    try:
                        current_price = await loop.run_in_executor(None, broker.get_current_price, symbol)
                    except Exception as exc:
                        logger.debug(f"Price fetch attempt {attempt + 1} failed for {symbol}: {exc}")
                        current_price = 0.0
                    if current_price:
                        self._price_cache[symbol] = (current_price, now)
                        break
                    await asyncio.sleep(0.2 * (attempt + 1))

                if not current_price:
                    return 0.0, 0.0

            direction = 1 if side == 'LONG' else -1
            pnl_usd = (current_price - entry_price) * quantity * direction
            pnl_pct = (pnl_usd / cash_used * 100) if cash_used else 0.0

            return pnl_usd, pnl_pct
        except Exception as exc:
            logger.debug(f"Failed to compute unrealized P&L for {position.get('symbol')}: {exc}")
            return 0.0, 0.0

    def get_daily_account_stats(self) -> Dict[str, Dict[str, float]]:
        """
        Produce account snapshot for daily summary reporting.
        """
        account_stats: Dict[str, Dict[str, Any]] = {}
        account_id_to_label: Dict[str, str] = {}

        try:
            partition_overview = self.get_partition_overview()
        except Exception:
            partition_overview = []

        for entry in partition_overview:
            account_id = entry.get('account_id')
            if not account_id:
                continue

            label = self._get_account_label(account_id)
            real_account = entry.get('real_account', {})
            partitions = entry.get('partitions', [])
            strategy_summaries = entry.get('strategies', [])

            total_balance = real_account.get('total_balance', 0.0)
            realized_pnl = sum(p.get('total_pnl', 0.0) for p in partitions)
            
            # Get starting balance (needed for calculations later)
            starting_total = 0.0
            if account_id in self.demo_accounts:
                demo_account = self.demo_accounts[account_id]
                starting_total = demo_account.get('starting_balance', 0.0)
            if starting_total == 0.0:
                starting_total = sum(p.get('starting_balance', 0.0) for p in partitions) or 0.0
            
            # Get current account balance for percentage calculations
            # For DEMO accounts, use demo account's current_balance (most accurate)
            # For LIVE accounts, use total_balance from real_account
            current_account_balance = total_balance
            if account_id in self.demo_accounts:
                demo_account = self.demo_accounts[account_id]
                demo_balance = demo_account.get('current_balance', 0.0)
                demo_starting = demo_account.get('starting_balance', 0.0)
                # Verify demo_balance is valid (should be >= starting_balance)
                if demo_balance > 0 and demo_starting > 0 and demo_balance >= demo_starting:
                    current_account_balance = demo_balance
                    logger.debug(f"EOD: Using demo account current_balance ${current_account_balance:,.2f} for {account_id}")
                elif demo_starting > 0:
                    # Recalculate from starting + P&L
                    current_account_balance = demo_starting + realized_pnl
                    logger.debug(f"EOD: Recalculated balance from demo starting + P&L: ${current_account_balance:,.2f} for {account_id}")
                else:
                    current_account_balance = demo_balance if demo_balance > 0 else total_balance
            
            # Fallback: if current_balance is still 0 or invalid, calculate from starting + P&L
            if current_account_balance <= 0:
                current_account_balance = starting_total + realized_pnl
            
            # Calculate realized_pct against CURRENT account balance (not starting balance)
            realized_pct = (realized_pnl / current_account_balance * 100) if current_account_balance > 0 else 0.0

            # MaxDD calculation: maxDD should be calculated from peak balance, not starting balance
            # The max_drawdown_pct in partitions is already calculated from peak, so we need to find the maximum
            # However, for account-level maxDD, we should calculate it from the account's peak balance
            # For now, use the maximum of partition max_drawdown_pct values (they're already from peak)
            max_dd_pct_values = [p.get('max_drawdown_pct', 0.0) for p in partitions if p.get('max_drawdown_pct')]
            max_dd_pct = max(max_dd_pct_values) if max_dd_pct_values else 0.0
            
            # Also calculate max_dd_usd for reference (sum of partition drawdowns)
            drawdown_usd_values = [p.get('max_drawdown_usd', 0.0) for p in partitions]
            max_dd_usd = sum(drawdown_usd_values)
            avg_dd_usd = sum(drawdown_usd_values) / len(drawdown_usd_values) if drawdown_usd_values else 0.0
            avg_dd_pct = (avg_dd_usd / starting_total * 100) if starting_total else 0.0

            total_trades = sum(p.get('total_trades', 0) for p in partitions)
            winning_trades = sum(p.get('winning_trades', 0) for p in partitions)
            losing_trades = sum(p.get('losing_trades', 0) for p in partitions)
            account_win_rate = (winning_trades / total_trades * 100) if total_trades else 0.0

            account_stats[label] = {
                'account_id': account_id,
                'balance': total_balance,
                'realized_pnl': realized_pnl,
                'realized_pnl_pct': realized_pct,
                'max_drawdown_usd': max_dd_usd,
                'max_drawdown_pct': max_dd_pct,
                'avg_drawdown_usd': avg_dd_usd,
                'avg_drawdown_pct': avg_dd_pct,
                'partition_style': entry.get('partition_style', 'isolated'),
                'partitions': partitions,
                'strategies': strategy_summaries,
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': account_win_rate
            }
            account_id_to_label[account_id] = label

        # Include DEMO accounts that might not use partitions
        for account_id, demo_info in self.demo_accounts.items():
            label = demo_info.get('label', account_id)
            if account_id in account_id_to_label:
                existing_label = account_id_to_label[account_id]
                stats = account_stats.get(existing_label)
                if not stats:
                    continue

                stats.setdefault('balance', demo_info.get('current_balance', 0.0))
                stats.setdefault('partition_style', demo_info.get('partition_style', 'isolated'))
                stats.setdefault('partitions', demo_info.get('partitions', []))
                continue

            starting_balance = demo_info.get('starting_balance', 0.0)
            total_pnl = demo_info.get('total_pnl', 0.0)
            realized_pct = (total_pnl / starting_balance * 100) if starting_balance else 0.0
            max_dd_usd = demo_info.get('max_drawdown_usd', 0.0)
            max_dd_pct = demo_info.get('max_drawdown_pct', 0.0)
            avg_dd_usd = demo_info.get('largest_loss', max_dd_usd)
            avg_dd_pct = max_dd_pct

            total_trades = demo_info.get('total_trades', 0)
            winning_trades = demo_info.get('winning_trades', 0)
            losing_trades = demo_info.get('losing_trades', 0)
            win_rate = (winning_trades / total_trades * 100) if total_trades else 0.0

            account_stats[label] = {
                'account_id': account_id,
                'balance': demo_info.get('current_balance', 0.0),
                'realized_pnl': total_pnl,
                'realized_pnl_pct': realized_pct,
                'max_drawdown_usd': max_dd_usd,
                'max_drawdown_pct': max_dd_pct,
                'avg_drawdown_usd': avg_dd_usd,
                'avg_drawdown_pct': avg_dd_pct,
                'partition_style': demo_info.get('partition_style', 'isolated'),
                'partitions': demo_info.get('partitions', []),
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': win_rate
            }
            account_id_to_label[account_id] = label

        return account_stats
