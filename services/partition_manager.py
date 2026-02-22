"""
TradingView Agent - Partition Manager (V3.6)

Virtual Account Partitioning System
Allows splitting a single physical broker account into multiple virtual partitions
Each partition can run different strategies with different leverage settings

Key Features:
- Split account 50/50, 50/25/25, 33/33/33, or 25/25/25/25
- Independent leverage per partition (1x, 3x, 5x, etc.)
- Isolated P&L tracking
- Auto-disable on low balance
- Real-time cash management
- Over-allocation prevention
"""

import logging
import asyncio
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

from services.state_store import state_store
# format_price imported locally in functions to avoid circular import

# Import demo starting balance from config
try:
    from config import settings
    DEMO_STARTING_BALANCE = settings.DEMO_STARTING_BALANCE
except Exception:
    # Fallback if config not loaded (shouldn't happen in normal operation)
    DEMO_STARTING_BALANCE = 2000.0

logger = logging.getLogger(__name__)


# ============================================
# PARTITION STATE (In-Memory)
# ============================================

partition_state = {}

# Global lock for position operations (prevents race conditions)
position_lock = asyncio.Lock()

# Trade counter for periodic sync
trade_counter = 0
SYNC_EVERY_N_TRADES = 10  # Sync with broker every 10 trades

# Exit-in-progress tracking (prevents duplicate exits)
# Format: {account_id: {partition_id: {symbol: exit_timestamp}}}
exits_in_progress: Dict[str, Dict[str, Dict[str, datetime]]] = {}
EXIT_TIMEOUT_SECONDS = 60  # Consider exit stale after 60 seconds


def _create_strategy_entry(
    strategy_id: str,
    *,
    label: Optional[str] = None,
    allocation_pct: float = 0.0,
    starting_balance: float = 0.0
) -> Dict[str, Any]:
    starting_balance = round(starting_balance, 2)
    return {
        'strategy_id': strategy_id,
        'label': label or strategy_id,
        'allocation_pct': allocation_pct,
        'starting_balance': starting_balance,
        'virtual_balance': starting_balance,
        'peak_balance': starting_balance,
        'total_pnl': 0.0,
        'total_pnl_pct': 0.0,
        'max_drawdown_usd': 0.0,
        'max_drawdown_pct': 0.0,
        'max_drawdown_trade_id': None,  # Track which trade caused max drawdown
        'max_drawdown_trade_symbol': None,
        'max_drawdown_trade_pnl': None,
        'total_trades': 0,
        'winning_trades': 0,
        'losing_trades': 0,
        'largest_win': 0.0,
        'largest_loss': 0.0
    }


def _derive_strategies_from_partitions(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    derived: Dict[str, Dict[str, Any]] = {}
    partitions = state.get('partitions', {})
    for data in partitions.values():
        strategy_ids = data.get('strategy_ids') or []
        if len(strategy_ids) != 1:
            continue
        strategy_id = strategy_ids[0]
        if strategy_id in derived:
            continue
        entry = _create_strategy_entry(
            strategy_id,
            allocation_pct=data.get('allocation_pct', 0.0),
            starting_balance=data.get('starting_balance', 0.0)
        )
        entry.update({
            'virtual_balance': data.get('virtual_balance', entry['virtual_balance']),
            'peak_balance': data.get('peak_balance', entry['peak_balance']),
            'total_pnl': data.get('total_pnl', 0.0),
            'total_pnl_pct': data.get('total_pnl_pct', 0.0),
            'max_drawdown_usd': data.get('max_drawdown_usd', 0.0),
            'max_drawdown_pct': data.get('max_drawdown_pct', 0.0),
            'total_trades': data.get('total_trades', 0),
            'winning_trades': data.get('winning_trades', 0),
            'losing_trades': data.get('losing_trades', 0),
            'largest_win': data.get('largest_win', 0.0),
            'largest_loss': data.get('largest_loss', 0.0)
        })
        derived[strategy_id] = entry
    return derived


def _ensure_strategy_registry(account_id: str, account_config=None, total_balance: Optional[float] = None) -> None:
    state = partition_state.get(account_id)
    if not state:
        return

    strategies = state.setdefault('strategies', {})
    meta = state.setdefault('meta', {})
    symbol_map = meta.setdefault('symbol_to_strategy', {})
    meta.setdefault('strategy_labels', {})

    if not strategies:
        derived = _derive_strategies_from_partitions(state)
        if derived:
            strategies.update(derived)

    strategy_ids = getattr(account_config, 'strategy_ids', []) if account_config else []
    count = len(strategy_ids) if strategy_ids else max(len(strategies), 1)
    default_alloc = 100.0 / count if count else 100.0

    if not strategies and strategy_ids and total_balance is not None:
        for strategy_id in strategy_ids:
            starting_balance = total_balance * (default_alloc / 100.0)
            strategies[strategy_id] = _create_strategy_entry(
                strategy_id,
                allocation_pct=default_alloc,
                starting_balance=starting_balance
            )

    if strategy_ids:
        for strategy_id in strategy_ids:
            if strategy_id not in strategies:
                starting_balance = (total_balance * (default_alloc / 100.0)) if total_balance is not None else 0.0
                strategies[strategy_id] = _create_strategy_entry(
                    strategy_id,
                    allocation_pct=default_alloc,
                    starting_balance=starting_balance
                )


def _resolve_strategy_context(
    account_id: str,
    symbol: str,
    explicit_strategy_id: Optional[str],
    explicit_strategy_name: Optional[str],
    position: Optional[Dict[str, Any]] = None,
    partition_id: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    if explicit_strategy_id:
        return explicit_strategy_id, explicit_strategy_name

    if position:
        pos_strategy_id = position.get('strategy_id')
        pos_strategy_name = position.get('strategy_name')
        if pos_strategy_id:
            return pos_strategy_id, pos_strategy_name

    state = partition_state.get(account_id, {})
    meta = state.get('meta', {})
    symbol_map = meta.get('symbol_to_strategy', {})
    strategy_labels = meta.get('strategy_labels', {})

    inferred_strategy_id = symbol_map.get(symbol)
    inferred_strategy_name = strategy_labels.get(inferred_strategy_id) if inferred_strategy_id else None
    if not inferred_strategy_id and partition_id:
        partition = state.get('partitions', {}).get(partition_id, {})
        strat_ids = partition.get('strategy_ids') or []
        if len(strat_ids) == 1:
            inferred_strategy_id = strat_ids[0]
            inferred_strategy_name = strategy_labels.get(inferred_strategy_id)
    return inferred_strategy_id, inferred_strategy_name


def _update_strategy_stats(
    account_id: str,
    strategy_id: Optional[str],
    pnl_usd_net: float
) -> None:
    if not strategy_id:
        return

    state = partition_state.get(account_id)
    if not state:
        return

    strategies = state.setdefault('strategies', {})
    meta = state.setdefault('meta', {})
    labels = meta.setdefault('strategy_labels', {})

    strategy_entry = strategies.get(strategy_id)
    if not strategy_entry:
        total_balance = state.get('real_account', {}).get('total_balance')
        starting_balance = (total_balance / max(len(strategies) or 1, 1)) if total_balance else 0.0
        strategy_entry = _create_strategy_entry(
            strategy_id,
            allocation_pct=0.0,
            starting_balance=starting_balance
        )
        strategies[strategy_id] = strategy_entry

    if labels.get(strategy_id):
        strategy_entry['label'] = labels[strategy_id]

    strategy_entry['total_trades'] = strategy_entry.get('total_trades', 0) + 1
    if pnl_usd_net > 0:
        strategy_entry['winning_trades'] = strategy_entry.get('winning_trades', 0) + 1
        strategy_entry['largest_win'] = max(strategy_entry.get('largest_win', 0.0), pnl_usd_net)
    elif pnl_usd_net < 0:
        strategy_entry['losing_trades'] = strategy_entry.get('losing_trades', 0) + 1
        strategy_entry['largest_loss'] = min(strategy_entry.get('largest_loss', 0.0), pnl_usd_net)

    strategy_entry['total_pnl'] = round(strategy_entry.get('total_pnl', 0.0) + pnl_usd_net, 2)
    starting_balance = strategy_entry.get('starting_balance', 0.0)
    strategy_entry['total_pnl_pct'] = (
        (strategy_entry['total_pnl'] / starting_balance) * 100 if starting_balance else 0.0
    )

    virtual_balance = round(strategy_entry.get('virtual_balance', starting_balance) + pnl_usd_net, 2)
    if virtual_balance < 0:
        virtual_balance = 0.0
    strategy_entry['virtual_balance'] = virtual_balance

    peak_balance = strategy_entry.get('peak_balance', virtual_balance)
    if virtual_balance > peak_balance:
        strategy_entry['peak_balance'] = virtual_balance
    else:
        drawdown_usd = peak_balance - virtual_balance
        if drawdown_usd > strategy_entry.get('max_drawdown_usd', 0.0):
            strategy_entry['max_drawdown_usd'] = drawdown_usd
            strategy_entry['max_drawdown_pct'] = (drawdown_usd / peak_balance * 100) if peak_balance else 0.0


def _get_partition_style(account_id: str) -> str:
    state = partition_state.get(account_id)
    if not state:
        return "isolated"
    return state.get('meta', {}).get('style', 'isolated')


def get_partition_style(account_id: str) -> str:
    """
    Public accessor for the current partition style of an account.
    """
    return _get_partition_style(account_id)


def _rebalance_cooperative(account_id: str) -> Dict[str, float]:
    """
    Reallocate cooperative partitions so each keeps its configured share
    of the real account equity.
    """
    state = partition_state.get(account_id)
    if not state:
        return {}

    style = state.get('meta', {}).get('style', 'isolated')
    if style != 'cooperative':
        return {}

    real_account = state.get('real_account', {})

    total_decimal = Decimal(str(real_account.get('cash_in_positions', 0.0))) + Decimal(
        str(real_account.get('available_cash', 0.0))
    )
    if total_decimal < Decimal('0'):
        total_decimal = Decimal('0')

    partitions_items = list(state.get('partitions', {}).items())
    if not partitions_items:
        real_account['total_balance'] = float(total_decimal.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        return {}

    new_balances: Dict[str, float] = {}
    cumulative = Decimal('0')

    for idx, (partition_id, partition) in enumerate(partitions_items):
        allocation = Decimal(str(partition.get('allocation_pct', 0.0)))
        target = (total_decimal * allocation) / Decimal('100')

        if idx < len(partitions_items) - 1:
            target = target.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            cumulative += target
        else:
            target = (total_decimal - cumulative).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        target_float = float(target)
        partition['virtual_balance'] = target_float

        peak_balance = partition.get('peak_balance', 0.0)
        if target_float > peak_balance:
            partition['peak_balance'] = target_float
        else:
            peak = partition.get('peak_balance', target_float)
            if peak > 0:
                current_dd = peak - target_float
                current_dd_pct = (current_dd / peak) * 100
                if current_dd > partition.get('max_drawdown_usd', 0.0):
                    partition['max_drawdown_usd'] = current_dd
                    partition['max_drawdown_pct'] = current_dd_pct

        new_balances[partition_id] = target_float

    real_account['total_balance'] = float(total_decimal.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    return new_balances


# ============================================
# PARTITION SPLITS
# ============================================

PARTITION_SPLITS = {
    "50/50": {
        "allocations": [50.0, 50.0],
        "description": "2 partitions, equal risk"
    },
    "50/25/25": {
        "allocations": [50.0, 25.0, 25.0],
        "description": "3 partitions, conservative primary"
    },
    "33/33/33": {
        "allocations": [33.0, 33.0, 33.0],  # 99% total - OK!
        "description": "3 partitions, equal risk (99% allocated)"
    },
    "25/25/25/25": {
        "allocations": [25.0, 25.0, 25.0, 25.0],
        "description": "4 partitions, maximum diversification"
    },
    "100": {
        "allocations": [100.0],
        "description": "Single partition (full account allocation)"
    }
}


# ============================================
# INITIALIZATION
# ============================================

async def initialize_partitions(account_id: str, config, broker) -> Dict:
    """
    Initialize partition system for an account
    
    Args:
        account_id: Account identifier
        config: Account configuration from config.yaml
        broker: Broker instance (must implement BrokerInterface)
        
    Returns:
        Dict containing partition state
    """
    logger.info(f"🚀 Initializing partition system for {account_id}")
    logger.info(f"   Broker: {broker.get_broker_name()}")
    
    # Get real balance from broker (UNIFIED INTERFACE)
    # For DEMO mode: Use demo account balance (with P&L) from order_executor
    # For LIVE mode: Use actual broker wallet balance
    mode = getattr(config, 'mode', 'LIVE')
    real_total = None
    
    if mode == "DEMO":
        # In DEMO mode, get balance from order_executor's demo account (includes P&L)
        try:
            # Import order_executor from main (global instance)
            # Use try/except to handle circular import gracefully
            import sys
            if 'main' in sys.modules:
                from main import order_executor
                if hasattr(order_executor, 'demo_accounts') and account_id in order_executor.demo_accounts:
                    demo_account = order_executor.demo_accounts[account_id]
                    real_total = float(demo_account.get('current_balance', DEMO_STARTING_BALANCE))
                    logger.info(f"   💰 DEMO mode: Using demo account balance ${real_total:,.2f} (includes P&L)")
                else:
                    # Fallback to broker's demo balance if demo account not initialized yet
                    real_total = await broker.get_account_balance_usd()
                    logger.info(f"   💰 DEMO mode: Using broker demo balance ${real_total:,.2f} (demo account not initialized)")
            else:
                # main not loaded yet, use broker fallback
                real_total = await broker.get_account_balance_usd()
                logger.info(f"   💰 DEMO mode: Using broker demo balance ${real_total:,.2f} (main not loaded)")
        except Exception as e:
            logger.warning(f"⚠️ Failed to get demo account balance from order_executor: {e}")
            # Fallback to broker
            real_total = await broker.get_account_balance_usd()
            logger.info(f"   💰 DEMO mode: Using broker demo balance ${real_total:,.2f} (fallback after error)")
    else:
        # LIVE mode: Use actual broker wallet balance
        try:
            # Use unified method - works for Coinbase (USD/USDC), Binance (USDT/BNB), Kraken (USD/EUR)
            real_total = await broker.get_account_balance_usd()
            logger.info(f"   💵 LIVE mode: Real account balance ${real_total:.2f} USD equivalent")
        except Exception as e:
            logger.error(f"❌ Failed to get account balance: {str(e)}")
            raise
    
    if real_total is None:
        raise ValueError(f"Failed to get account balance for {account_id}")
    
    # Attempt to restore persisted state (if Firestore enabled)
    # CRITICAL: Firestore cache must track DEMO and LIVE modes separately
    # DEMO mode: Uses demo account balance (with P&L tracking)
    # LIVE mode: Uses broker wallet balance
    mode = getattr(config, 'mode', 'LIVE')
    restored_state = await state_store.load_partition_state(account_id)
    
    # Check if restored state is from a different mode (LIVE vs DEMO)
    # If modes don't match, we need to reinitialize with correct balance source
    mode_mismatch = False
    if restored_state:
        cached_mode = restored_state.get('meta', {}).get('mode')
        cached_balance = restored_state.get('real_account', {}).get('total_balance', 0)
        
        # If no mode tag in cache, check balance to infer mode
        # DEMO mode should use demo account balance, LIVE uses broker balance
        if not cached_mode:
            logger.warning(
                f"   ⚠️ Firestore cache has no mode tag. "
                f"Checking balance source to determine if reinitialization needed."
            )
            # If in DEMO mode and cached balance doesn't match demo account, reinitialize
            if mode == "DEMO":
                try:
                    import sys
                    if 'main' in sys.modules:
                        from main import order_executor
                        if hasattr(order_executor, 'demo_accounts') and account_id in order_executor.demo_accounts:
                            demo_account = order_executor.demo_accounts[account_id]
                            demo_balance = float(demo_account.get('current_balance', DEMO_STARTING_BALANCE))
                            if abs(cached_balance - demo_balance) > 0.01:
                                logger.warning(
                                    f"   ⚠️ Cached balance (${cached_balance:,.2f}) doesn't match "
                                    f"demo account (${demo_balance:,.2f}). Reinitializing."
                                )
                                mode_mismatch = True
                                restored_state = None
                except Exception as e:
                    logger.debug(f"   Could not check demo account balance: {e}")
        elif cached_mode.upper() != mode.upper():
            logger.warning(
                f"   ⚠️ Firestore cache is from {cached_mode} mode, but current mode is {mode}. "
                f"Reinitializing with correct balance source for {mode} mode."
            )
            mode_mismatch = True
            # Don't use cached state if mode doesn't match - treat as fresh init
            restored_state = None
    
    if restored_state and not mode_mismatch:
        expected_partitions = {p.id for p in config.partitions}
        restored_partitions = set(restored_state.get('partitions', {}).keys())
        if expected_partitions == restored_partitions:
            restored_style = getattr(config, 'partition_style', 'isolated')
            partition_state[account_id] = restored_state
            partition_state[account_id].setdefault('meta', {})
            partition_state[account_id]['meta']['style'] = restored_style
            partition_state[account_id]['meta']['mode'] = mode  # Store current mode
            real_account = partition_state[account_id].get('real_account', {})
            
            # CRITICAL: In DEMO mode, ALWAYS use demo account balance, never cached balance
            # In LIVE mode, use broker wallet balance
            if mode == "DEMO":
                # CRITICAL: In DEMO mode, ALWAYS get demo account balance from order_executor
                # NEVER use cached balance from Firestore - it might be from LIVE mode
                # This ensures DEMO mode always uses the correct demo account balance
                demo_balance_found = False
                demo_balance = DEMO_STARTING_BALANCE  # Default fallback from config
                
                try:
                    import sys
                    if 'main' in sys.modules:
                        from main import order_executor
                        if hasattr(order_executor, 'demo_accounts') and account_id in order_executor.demo_accounts:
                            demo_account = order_executor.demo_accounts[account_id]
                            demo_balance = float(demo_account.get('current_balance', DEMO_STARTING_BALANCE))
                            real_total = demo_balance  # ALWAYS use demo account balance
                            demo_balance_found = True
                            logger.info(f"   💰 DEMO mode: Using demo account balance ${real_total:,.2f} (ignoring any cached balance)")
                        else:
                            logger.warning(f"   ⚠️ Demo account not found in order_executor, using default ${DEMO_STARTING_BALANCE:,.2f}")
                            real_total = demo_balance
                    else:
                        logger.warning(f"   ⚠️ Main module not loaded, using default ${DEMO_STARTING_BALANCE:,.2f}")
                        real_total = demo_balance
                except Exception as e:
                    logger.warning(f"   ⚠️ Could not get demo account balance from order_executor: {e}, using default ${DEMO_STARTING_BALANCE:,.2f}")
                    real_total = demo_balance
                
                # CRITICAL: ALWAYS update partition balance to use demo account balance
                # This OVERWRITES any cached balance from Firestore (even if it was from LIVE mode)
                cached_balance = real_account.get('total_balance', 0)
                cached_cash_in_positions = real_account.get('cash_in_positions', 0.0)
                
                # Update total balance to demo account balance
                real_account['total_balance'] = real_total
                
                # Update available_cash to match new total (preserving cash_in_positions if any)
                # available_cash = total_balance - cash_in_positions
                real_account['available_cash'] = real_total - cached_cash_in_positions
                
                if abs(cached_balance - real_total) > 0.01:
                    logger.warning(
                        f"   ⚠️ Overwriting cached balance ${cached_balance:,.2f} with demo account balance ${real_total:,.2f}"
                    )
                    logger.info(f"   💰 Updated available_cash: ${real_account['available_cash']:,.2f} (total ${real_total:,.2f} - positions ${cached_cash_in_positions:,.2f})")
                
                logger.info(f"   💰 DEMO mode: Partition balance set to ${real_total:,.2f} (will split partitions from this)")
                
                # Rebalance partitions to match current demo balance
                # This splits the demo account balance across partitions
                # Also fixes starting_balance values to match demo account's starting_balance
                await rebalance_partition_virtual_balances(account_id)
                logger.info(f"   ✅ Partitions rebalanced to match demo account balance ${real_total:,.2f}")
                logger.info(f"   ✅ Partition starting_balance values corrected to use demo account starting: ${demo_balance:,.2f}")
                
                # Save updated state to Firestore with correct DEMO balance and mode
                # This ensures Firestore cache tracks DEMO mode values correctly
                await state_store.save_partition_state(account_id, partition_state[account_id])
                logger.info(f"   💾 Saved partition state to Firestore (DEMO mode, balance ${real_total:,.2f})")
            elif mode == "LIVE":
                # LIVE mode: Use actual broker wallet balance (from broker API)
                real_account['total_balance'] = real_total
                logger.info(f"   💵 LIVE mode: Partition balance set to ${real_total:,.2f} (from broker wallet)")
                # Rebalance partitions to match broker balance
                await rebalance_partition_virtual_balances(account_id)
                # Save updated state to Firestore with correct LIVE balance and mode
                # This ensures Firestore cache tracks LIVE mode values correctly
                await state_store.save_partition_state(account_id, partition_state[account_id])
                logger.info(f"   💾 Saved partition state to Firestore (LIVE mode, balance ${real_total:,.2f})")
            
            real_account.setdefault('last_sync', datetime.now().isoformat())
            logger.info(f"   ♻️ Restored partition state for {account_id} from persistence ({mode} mode)")
            
            # For DEMO accounts, ensure partition starting_balance values are correct
            if mode == "DEMO":
                await rebalance_partition_virtual_balances(account_id)
                logger.info(f"   ✅ Verified and corrected partition starting_balance values for DEMO account")
            
            if restored_style == "cooperative":
                # Cooperative partitions: ensure they're balanced correctly
                _rebalance_cooperative(account_id)
            _ensure_strategy_registry(account_id, config, real_total)
            for partition in partition_state[account_id].get('partitions', {}).values():
                open_positions = partition.get('open_positions', {})
                total_deployed = Decimal('0')
                for symbol, pos in open_positions.items():
                    if not isinstance(pos, dict):
                        continue
                    pos.setdefault('order_ids', [pos.get('order_id')])
                    pos.setdefault('legs', [{
                        'quantity': pos.get('quantity', 0.0),
                        'entry_price': pos.get('entry_price', 0.0),
                        'cash_used': pos.get('cash_used', 0.0),
                        'leverage': pos.get('leverage', partition.get('leverage', 1)),
                        'order_id': pos.get('order_id'),
                        'entry_time': pos.get('entry_time', datetime.now().isoformat()),
                        'virtual_balance_at_entry': pos.get('virtual_balance_at_entry', partition.get('virtual_balance', 0.0))
                    }])
                    total_deployed += Decimal(str(pos.get('cash_used', 0.0)))
                partition.setdefault('deployed_cash', float(total_deployed.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)))
            await state_store.save_partition_state(account_id, partition_state[account_id])
            return partition_state[account_id]
        else:
            logger.warning(
                f"   ♻️ Saved partition layout ({restored_partitions}) does not match "
                f"config ({expected_partitions}); reinitialising from scratch."
            )
    
    # Validate partition split
    split = config.partition_split
    if split not in PARTITION_SPLITS:
        raise ValueError(f"Invalid partition split: {split}")
    
    allocations = PARTITION_SPLITS[split]['allocations']
    
    if len(allocations) != len(config.partitions):
        raise ValueError(
            f"Partition split '{split}' expects {len(allocations)} partitions, "
            f"but {len(config.partitions)} are configured"
        )
    
    # Initialize account state
    # Store mode in meta so Firestore can track DEMO vs LIVE separately
    partition_state[account_id] = {
        'meta': {
            'style': getattr(config, 'partition_style', 'isolated'),
            'mode': mode  # Store current mode for Firestore tracking
        },
        'real_account': {
            'total_balance': real_total,
            'cash_in_positions': 0.00,
            'available_cash': real_total,
            'last_sync': datetime.now().isoformat(),
            'pending_orders': []
        },
        'partitions': {}
    }
    
    # Initialize each partition
    # For DEMO accounts, calculate starting_balance from demo account's starting_balance
    # For LIVE accounts, use virtual_balance (which equals real_total * allocation)
    demo_account_starting = None
    if mode == "DEMO":
        try:
            import sys
            if 'main' in sys.modules:
                from main import order_executor
                if hasattr(order_executor, 'demo_accounts') and account_id in order_executor.demo_accounts:
                    demo_account = order_executor.demo_accounts[account_id]
                    demo_account_starting = float(demo_account.get('starting_balance', DEMO_STARTING_BALANCE))
                    logger.info(f"   💰 DEMO mode: Using demo account starting_balance ${demo_account_starting:,.2f} for partition initialization")
        except Exception as e:
            logger.debug(f"Could not get demo account starting_balance during init: {e}")
            demo_account_starting = DEMO_STARTING_BALANCE
    
    for i, partition_config in enumerate(config.partitions):
        allocation_pct = allocations[i]
        virtual_balance = real_total * (allocation_pct / 100.0)
        
        # Calculate starting_balance: DEMO uses demo account starting, LIVE uses virtual_balance
        if mode == "DEMO" and demo_account_starting:
            partition_starting = round(demo_account_starting * (allocation_pct / 100.0), 2)
        else:
            partition_starting = virtual_balance
        
        partition_state[account_id]['partitions'][partition_config.id] = {
            'allocation_pct': allocation_pct,
            'virtual_balance': virtual_balance,
            'starting_balance': partition_starting,
            'peak_balance': virtual_balance,
            'deployed_cash': 0.00,
            'total_pnl': 0.00,
            'max_drawdown_usd': 0.00,
            'max_drawdown_pct': 0.00,
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'largest_win': 0.00,
            'largest_loss': 0.00,
            'enabled': partition_config.enabled,
            'leverage': partition_config.leverage,
            'min_balance_usd': partition_config.min_balance_usd,
            'strategy_ids': partition_config.strategy_ids,
            'open_positions': {},
            'trade_history': [],
            'current_streak': 0,
            'max_win_streak': 0,
            'max_loss_streak': 0
        }
        
        logger.info(
            f"   ✅ {partition_config.id}: ${virtual_balance:.2f} "
            f"({allocation_pct}%, {partition_config.leverage}x)"
        )
    
    logger.info(f"✅ Partition system initialized for {account_id}")
    logger.info(f"   Split: {split} - {PARTITION_SPLITS[split]['description']}")
    logger.info(f"   Partitions: {len(config.partitions)}")
    logger.info(f"   Total allocated: {sum(allocations)}%")
    if _get_partition_style(account_id) == "cooperative":
        _rebalance_cooperative(account_id)

    # For DEMO mode, ensure we're using demo account balance (with P&L)
    # For LIVE mode, ensure we're using broker wallet balance
    mode = getattr(config, 'mode', 'LIVE')
    if mode == "DEMO":
        # Double-check: In DEMO mode, always use demo account balance
        try:
            import sys
            if 'main' in sys.modules:
                from main import order_executor
                if hasattr(order_executor, 'demo_accounts') and account_id in order_executor.demo_accounts:
                    demo_account = order_executor.demo_accounts[account_id]
                    demo_balance = float(demo_account.get('current_balance', DEMO_STARTING_BALANCE))
                    if abs(demo_balance - real_total) > 0.01:  # If different, update
                        real_total = demo_balance
                        real_account = partition_state[account_id].get('real_account', {})
                        real_account['total_balance'] = real_total
                        logger.info(f"   💰 DEMO mode: Updated partition balance to ${real_total:,.2f} (demo account with P&L)")
                        # Rebalance partitions to match demo balance
                        await rebalance_partition_virtual_balances(account_id)
        except Exception as e:
            logger.debug(f"   Could not verify demo account balance: {e}")
    
    _ensure_strategy_registry(account_id, config, real_total)
    
    await state_store.save_partition_state(account_id, partition_state[account_id])
    
    return partition_state[account_id]


# ============================================
# POSITION SIZING
# ============================================

def calculate_partition_position_size(
    partition_id: str,
    account_id: str,
    equity_pct: float = 90.0
) -> Dict:
    """
    Calculate position size for a partition
    Uses partition's virtual balance and checks real available cash
    
    Args:
        partition_id: Partition identifier
        account_id: Account identifier
        equity_pct: Percentage of equity to use (default 90%)
        
    Returns:
        Dict with sizing information
    """
    if account_id not in partition_state:
        raise ValueError(f"Account {account_id} not initialized")
    
    state = partition_state[account_id]
    
    if partition_id not in state['partitions']:
        raise ValueError(f"Partition {partition_id} not found")
    
    style = _get_partition_style(account_id)

    if style == "cooperative":
        _rebalance_cooperative(account_id)

    partition = state['partitions'][partition_id]
    real_account = state['real_account']
    
    # Check if partition enabled
    if not partition['enabled']:
        raise ValueError(f"Partition {partition_id} is disabled")
    
    # Calculate desired position from virtual balance
    virtual_balance = partition['virtual_balance']
    desired_cash = virtual_balance * (equity_pct / 100.0)
    
    # Check available real cash
    available_cash = real_account['available_cash']
    
    # Use minimum of desired or available
    # NOTE: No buffer applied - we maximize contracts using margin-based sizing in order_executor
    # The 30% reserve (100% - 70% allocation) provides sufficient buffer for margin increases
    if desired_cash > available_cash:
        logger.warning(f"⚠️ Insufficient cash for {partition_id}")
        logger.warning(f"   Desired: ${desired_cash:.2f}")
        logger.warning(f"   Available: ${available_cash:.2f}")
        logger.warning(f"   Using available cash (no buffer - margin-based sizing will optimize)")
        actual_cash = available_cash  # No buffer - margin-based sizing will maximize contracts
        reduced = True
    else:
        actual_cash = desired_cash
        reduced = False
    
    # Apply leverage
    leverage = partition['leverage']
    notional = actual_cash * leverage
    
    return {
        'partition_id': partition_id,
        'virtual_balance': virtual_balance,
        'desired_cash': desired_cash,
        'actual_cash': actual_cash,
        'available_cash': available_cash,
        'notional': notional,
        'leverage': leverage,
        'reduced': reduced
    }


def get_total_notional_usd(
    account_id: str,
    symbol: str,
    current_price: float
) -> float:
    """
    Calculate the total notional exposure for a symbol across all partitions.
    """
    state = partition_state.get(account_id)
    if not state or current_price <= 0:
        return 0.0
    
    total_notional = 0.0
    for partition in state.get('partitions', {}).values():
        position = partition.get('open_positions', {}).get(symbol)
        if not position:
            continue
        quantity = abs(position.get('quantity', 0.0))
        if quantity <= 0:
            continue
        total_notional += quantity * current_price
    
    return total_notional


# ============================================
# OPEN POSITION
# ============================================

async def open_partition_position(
    partition_id: str,
    account_id: str,
    symbol: str,
    side: str,
    quantity: float,
    entry_price: float,
    cash_used: float,
    leverage: int,
    order_id: str,
    strategy_id: Optional[str] = None,
    strategy_name: Optional[str] = None,
    tradingview_stop_loss: Optional[float] = None  # TradingView stop loss price from alert (if provided)
) -> Dict:
    """
    Record position opening in partition state
    Reserves real cash and tracks virtual position
    
    CRITICAL: Uses asyncio lock to prevent race conditions
    
    Args:
        partition_id: Partition identifier
        account_id: Account identifier
        symbol: Trading symbol
        side: 'LONG' or 'SHORT'
        quantity: Position quantity
        entry_price: Entry price
        cash_used: Real cash used (collateral)
        leverage: Leverage used
        order_id: Order ID from broker
        strategy_id: Strategy identifier responsible for the trade
        strategy_name: Display label for the strategy
        
    Returns:
        Position dict
    """
    # CRITICAL: Acquire lock to prevent concurrent modifications
    async with position_lock:
        state = partition_state[account_id]
        partition = state['partitions'][partition_id]
        partition.setdefault('deployed_cash', 0.0)
        real_account = state['real_account']
        meta = state.setdefault('meta', {})
        symbol_map = meta.setdefault('symbol_to_strategy', {})
        strategy_labels = meta.setdefault('strategy_labels', {})
        if strategy_id and symbol:
            symbol_map.setdefault(symbol, strategy_id)
        if strategy_id and strategy_name:
            strategy_labels[strategy_id] = strategy_name
        
        # Final check: ensure cash still available
        if cash_used > real_account['available_cash']:
            raise ValueError(
                f"Insufficient cash after lock acquisition: "
                f"Need ${cash_used:.2f}, have ${real_account['available_cash']:.2f}"
            )
        
        # Create position record using Decimal for precision
        cash_used_decimal = Decimal(str(cash_used)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        now_iso = datetime.now().isoformat()

        leg = {
            'quantity': quantity,
            'entry_price': entry_price,
            'cash_used': float(cash_used_decimal),
            'leverage': leverage,
            'order_id': order_id,
            'entry_time': now_iso,
            'virtual_balance_at_entry': partition['virtual_balance'],
            'strategy_id': strategy_id,
            'strategy_name': strategy_name
        }
        
        position = {
            'side': side,
            'quantity': quantity,
            'entry_price': entry_price,
            'cash_used': float(cash_used_decimal),
            'virtual_balance_at_entry': partition['virtual_balance'],
            'entry_time': now_iso,
            'leverage': leverage,
            'order_id': order_id,
            'order_ids': [order_id],
            'legs': [leg],
            'strategy_id': strategy_id,
            'strategy_name': strategy_name
        }
        
        # Store TradingView stop loss price if provided (source of truth for stop loss)
        if tradingview_stop_loss is not None and tradingview_stop_loss > 0:
            from services.exit_monitor import format_price
            position['tradingview_stop_loss'] = float(tradingview_stop_loss)
            logger.debug(f"📌 Stored TradingView stop loss price for {symbol}: {format_price(tradingview_stop_loss, symbol)}")
        
        # Capture margin per contract when position is opened
        # This records the actual margin requirement at trade execution time
        if strategy_id:
            await _capture_margin_on_position_open(
                account_id, strategy_id, symbol, entry_price, 
                quantity, leverage, cash_used, state
            )
        
        # Store in partition
        if symbol in partition['open_positions']:
            existing = partition['open_positions'][symbol]
            if existing['side'] != side:
                logger.warning(
                    f"⚠️ Partition {partition_id} received opposing signal for {symbol}. "
                    f"Existing side: {existing['side']}, new side: {side}. Treating as scale-in."
                )
            existing.setdefault('legs', []).append(leg)
            existing.setdefault('order_ids', []).append(order_id)

            total_quantity = existing['quantity'] + quantity
            if total_quantity <= 0:
                raise ValueError(f"Invalid aggregated quantity for {symbol} in {partition_id}")

            total_cash_decimal = Decimal(str(existing['cash_used'])) + cash_used_decimal

            weighted_entry = Decimal('0')
            total_qty_decimal = Decimal('0')
            for leg_info in existing['legs']:
                qty_dec = Decimal(str(leg_info['quantity']))
                total_qty_decimal += qty_dec
                weighted_entry += Decimal(str(leg_info['entry_price'])) * qty_dec

            avg_entry = float((weighted_entry / total_qty_decimal).quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP))

            existing['quantity'] = float(total_qty_decimal)
            existing['cash_used'] = float(total_cash_decimal.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
            existing['entry_price'] = avg_entry
            existing['order_id'] = order_id  # latest
            existing['entry_time'] = min(existing.get('entry_time', now_iso), now_iso)
            if strategy_id and not existing.get('strategy_id'):
                existing['strategy_id'] = strategy_id
            if strategy_name and not existing.get('strategy_name'):
                existing['strategy_name'] = strategy_name
        else:
            partition['open_positions'][symbol] = position
        
        # Reserve real cash (using Decimal for precision)
        cash_in_positions = Decimal(str(real_account['cash_in_positions']))
        available_cash = Decimal(str(real_account['available_cash']))
        
        cash_in_positions += cash_used_decimal
        available_cash -= cash_used_decimal
        partition['deployed_cash'] = float(
            (Decimal(str(partition.get('deployed_cash', 0.0))) + cash_used_decimal).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
        )
        
        real_account['cash_in_positions'] = float(cash_in_positions)
        real_account['available_cash'] = float(available_cash)
    
    from services.exit_monitor import format_price
    logger.info(f"✅ {partition_id} opened {side} position:")
    logger.info(f"   Symbol: {symbol}")
    logger.info(f"   Entry Price: {format_price(entry_price, symbol)}")
    logger.info(f"   Quantity: {quantity}")
    logger.info(f"   Cash Used: ${cash_used:.2f}")
    logger.info(f"   Leverage: {leverage}x")
    logger.info(f"   Virtual Balance: ${partition['virtual_balance']:.2f}")
    logger.info(f"   ─────────────────────────────")
    logger.info(f"   Real Available Cash: ${real_account['available_cash']:.2f}")
    logger.info(f"   Real Cash in Positions: ${real_account['cash_in_positions']:.2f}")
    
    await state_store.save_partition_state(account_id, partition_state[account_id])
    
    return position


# ============================================
# CLOSE POSITION
# ============================================

def is_position_exiting(account_id: str, partition_id: str, symbol: str) -> bool:
    """
    Check if a position is currently being exited (prevents duplicate exits)
    
    Returns:
        True if position is marked as exiting, False otherwise
    """
    account_exits = exits_in_progress.get(account_id, {})
    partition_exits = account_exits.get(partition_id, {})
    exit_timestamp = partition_exits.get(symbol)
    
    if exit_timestamp is None:
        return False
    
    # Check if exit is stale (timeout)
    now = datetime.utcnow()
    if (now - exit_timestamp).total_seconds() > EXIT_TIMEOUT_SECONDS:
        # Remove stale exit marker
        del partition_exits[symbol]
        if not partition_exits:
            del account_exits[partition_id]
        if not account_exits:
            del exits_in_progress[account_id]
        return False
    
    return True


def mark_position_exiting(account_id: str, partition_id: str, symbol: str) -> None:
    """Mark a position as currently being exited"""
    if account_id not in exits_in_progress:
        exits_in_progress[account_id] = {}
    if partition_id not in exits_in_progress[account_id]:
        exits_in_progress[account_id][partition_id] = {}
    exits_in_progress[account_id][partition_id][symbol] = datetime.utcnow()


def clear_position_exiting(account_id: str, partition_id: str, symbol: str) -> None:
    """Clear exit-in-progress marker for a position"""
    if account_id not in exits_in_progress:
        return  # Nothing to clear
    
    account_exits = exits_in_progress[account_id]
    if partition_id not in account_exits:
        return  # Partition not in exits
    
    partition_exits = account_exits[partition_id]
    if symbol in partition_exits:
        del partition_exits[symbol]
    
    # Clean up empty partition dict
    if not partition_exits:
        del account_exits[partition_id]
    
    # Clean up empty account dict
    if not account_exits:
        del exits_in_progress[account_id]


async def close_partition_position(
    partition_id: str,
    account_id: str,
    symbol: str,
    exit_price: float,
    commission_rate: float = 0.0004,  # 0.04% total (0.02% entry + 0.02% exit)
    broker_pnl_usd_net: Optional[float] = None,  # Broker-provided P&L (Live mode)
    broker_commission: Optional[float] = None,  # Broker-provided commission (Live mode)
    mode: str = "DEMO",  # "DEMO" or "LIVE" - affects whether to use broker data
    strategy_id: Optional[str] = None,
    strategy_name: Optional[str] = None,
    peak_profit_pct: Optional[float] = None,  # NEW: Maximum profit % reached during trade (for optimization analysis)
    exit_reason: Optional[str] = None  # NEW: Exit reason (e.g., "BREAKEVEN_STOP", "RSI_EXIT_LONG")
) -> Tuple[float, Dict]:
    """
    Close position and update both virtual and real balances
    
    Args:
        partition_id: Partition identifier
        account_id: Account identifier
        symbol: Trading symbol
        exit_price: Exit price
        commission_rate: Commission rate (default 0.04% total) - used if broker_commission not provided
        broker_pnl_usd_net: Broker-provided net P&L in USD (Live mode only, optional)
        broker_commission: Broker-provided commission in USD (Live mode only, optional)
        mode: Account mode ("DEMO" or "LIVE") - determines whether to prefer broker data
        strategy_id: Strategy identifier responsible for the trade
        strategy_name: Display name for the strategy
        
    Returns:
        Tuple of (net_pnl, trade_record)
    """
    state = partition_state[account_id]
    partition = state['partitions'][partition_id]
    real_account = state['real_account']
    style = _get_partition_style(account_id)
    
    if symbol not in partition['open_positions']:
        raise ValueError(f"No open position for {symbol} in {partition_id}")
    
    # Clear exit-in-progress marker (position is being closed)
    clear_position_exiting(account_id, partition_id, symbol)
    
    position = partition['open_positions'][symbol]
    legs = position.get('legs') or [position]

    resolved_strategy_id, resolved_strategy_name = _resolve_strategy_context(
        account_id,
        symbol,
        strategy_id,
        strategy_name,
        position,
        partition_id=partition_id
    )
    if resolved_strategy_id and resolved_strategy_name:
        meta = state.setdefault('meta', {})
        strategy_labels = meta.setdefault('strategy_labels', {})
        strategy_labels[resolved_strategy_id] = resolved_strategy_name
    
    # Extract aggregated data
    entry_price = position['entry_price']
    quantity = position['quantity']
    cash_used = position['cash_used']
    leverage = position['leverage']
    
    # ============================================
    # CALCULATE P&L
    # ============================================
    
    # Use Decimal for precise calculations
    exit_decimal = Decimal(str(exit_price))
    commission_rate_decimal = Decimal(str(commission_rate))
    
    total_quantity_decimal = Decimal('0')
    weighted_entry_total = Decimal('0')
    total_cash_used_decimal = Decimal('0')
    total_pnl_gross_decimal = Decimal('0')
    total_commission_decimal = Decimal('0')
    
    for leg in legs:
        leg_entry_decimal = Decimal(str(leg['entry_price']))
        leg_quantity_decimal = Decimal(str(leg['quantity']))
        leg_cash_decimal = Decimal(str(leg['cash_used']))
        leg_leverage_decimal = Decimal(str(leg.get('leverage', leverage)))
        
        total_quantity_decimal += leg_quantity_decimal
        weighted_entry_total += leg_entry_decimal * leg_quantity_decimal
        total_cash_used_decimal += leg_cash_decimal
        
        if position['side'] == 'LONG':
            leg_price_change = (exit_decimal - leg_entry_decimal) / leg_entry_decimal
        else:
            leg_price_change = (leg_entry_decimal - exit_decimal) / leg_entry_decimal
        
        leg_pnl_pct_decimal = leg_price_change * Decimal('100') * leg_leverage_decimal
        leg_pnl_gross_decimal = leg_cash_decimal * (leg_pnl_pct_decimal / Decimal('100'))
        
        total_pnl_gross_decimal += leg_pnl_gross_decimal
        total_commission_decimal += leg_cash_decimal * commission_rate_decimal
    
    if total_quantity_decimal > 0:
        avg_entry_decimal = weighted_entry_total / total_quantity_decimal
    else:
        avg_entry_decimal = Decimal(str(entry_price))
    
    # ============================================
    # USE BROKER DATA IN LIVE MODE (if provided)
    # ============================================
    
    # In Live mode, prefer broker-provided values over calculated ones
    if mode == "LIVE" and broker_commission is not None:
        # Use broker-provided commission (source of truth)
        total_commission_decimal = Decimal(str(broker_commission))
        logger.info(f"💰 LIVE MODE: Using broker-provided commission: ${float(total_commission_decimal):,.2f}")
    
    if mode == "LIVE" and broker_pnl_usd_net is not None:
        # Use broker-provided P&L (source of truth)
        pnl_usd_net_decimal = Decimal(str(broker_pnl_usd_net))
        # Recalculate gross P&L from net + commission for consistency
        total_pnl_gross_decimal = pnl_usd_net_decimal + total_commission_decimal
        logger.info(f"💰 LIVE MODE: Using broker-provided P&L: ${float(pnl_usd_net_decimal):,.2f} (net)")
    else:
        # Calculate P&L from price change (Demo mode or fallback)
        pnl_usd_net_decimal = total_pnl_gross_decimal - total_commission_decimal
    
    total_cash_used_decimal = total_cash_used_decimal.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    total_pnl_gross_decimal = total_pnl_gross_decimal.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    total_commission_decimal = total_commission_decimal.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    pnl_usd_net_decimal = pnl_usd_net_decimal.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    
    # Convert to float for storage
    pnl_usd_gross = float(total_pnl_gross_decimal)
    commission = float(total_commission_decimal)
    pnl_usd_net = float(pnl_usd_net_decimal)
    
    # ============================================
    # UPDATE VIRTUAL LAYER (Partition Balance)
    # ============================================
    
    old_virtual_balance_decimal = Decimal(str(partition['virtual_balance']))
    new_virtual_balance_decimal = old_virtual_balance_decimal + pnl_usd_net_decimal
    new_virtual_balance = float(new_virtual_balance_decimal.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    # PROTECTION: Prevent negative balance
    if new_virtual_balance < 0:
        logger.error(f"⚠️ Negative balance detected for {partition_id}!")
        logger.error(f"   P&L NET: ${pnl_usd_net:+.2f}")
        logger.error(f"   Calculated balance: ${new_virtual_balance:.2f}")
        logger.error(f"   Setting to $0.00 and disabling partition")
        new_virtual_balance = 0.00
        partition['enabled'] = False
    
    partition['virtual_balance'] = new_virtual_balance
    partition['total_pnl'] += pnl_usd_net
    
    # Update peak and drawdown
    if style != "cooperative":
        if new_virtual_balance > partition['peak_balance']:
            partition['peak_balance'] = new_virtual_balance
        
        current_dd = partition['peak_balance'] - new_virtual_balance
        current_dd_pct = (current_dd / partition['peak_balance']) * 100
        
        if current_dd > partition['max_drawdown_usd']:
            partition['max_drawdown_usd'] = current_dd
            partition['max_drawdown_pct'] = current_dd_pct
            # Note: max_drawdown_trade_id will be set after trade_record is created below
            # Track which trade caused this max drawdown (set after trade_record is created)
            # This will be set below after trade_record is created
    
    # ============================================
    # UPDATE REAL LAYER (Actual Cash)
    # ============================================
    
    # Cash used is freed back to account plus/minus P&L (use Decimal)
    cash_returned_decimal = total_cash_used_decimal + pnl_usd_net_decimal
    cash_returned = float(cash_returned_decimal.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    # Update cash positions and available cash
    cash_in_positions = Decimal(str(real_account['cash_in_positions']))
    available_cash = Decimal(str(real_account['available_cash']))
    
    cash_in_positions -= total_cash_used_decimal
    available_cash += cash_returned_decimal
    
    deployed_cash_decimal = Decimal(str(partition.get('deployed_cash', 0.0)))
    deployed_cash_decimal -= total_cash_used_decimal
    if deployed_cash_decimal < Decimal('0'):
        deployed_cash_decimal = Decimal('0')
    partition['deployed_cash'] = float(deployed_cash_decimal.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    # PROTECTION: Prevent negative cash_in_positions (rounding errors)
    if cash_in_positions < Decimal('0'):
        logger.warning(f"⚠️ Negative cash_in_positions detected: ${float(cash_in_positions):.2f}")
        logger.warning(f"   Likely rounding error - adjusting to $0.00")
        # Transfer the difference to available_cash
        available_cash += cash_in_positions  # Add negative (subtract)
        cash_in_positions = Decimal('0')
    
    real_account['cash_in_positions'] = float(cash_in_positions.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    real_account['available_cash'] = float(available_cash.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    # Update total balance (sum of all virtual balances - use Decimal for precision)
    if style != "cooperative":
        total_virtual = Decimal('0')
        for p in state['partitions'].values():
            total_virtual += Decimal(str(p['virtual_balance']))
        
        real_account['total_balance'] = float(total_virtual.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        
        # CRITICAL: Verify cash balance matches total balance
        calculated_cash_total = Decimal(str(real_account['cash_in_positions'])) + Decimal(str(real_account['available_cash']))
        total_balance_decimal = Decimal(str(real_account['total_balance']))
        
        if abs(calculated_cash_total - total_balance_decimal) > Decimal('0.02'):  # Allow 2 cent tolerance
            logger.warning(f"⚠️ Cash balance mismatch detected!")
            logger.warning(f"   Cash total: ${float(calculated_cash_total):.2f}")
            logger.warning(f"   Virtual total: ${float(total_balance_decimal):.2f}")
            logger.warning(f"   Difference: ${float(abs(calculated_cash_total - total_balance_decimal)):.2f}")
            # Sync real account balance to match virtual
            real_account['total_balance'] = float(calculated_cash_total.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    else:
        rebalance_balances = _rebalance_cooperative(account_id)
        new_virtual_balance = rebalance_balances.get(partition_id, partition['virtual_balance'])
        calculated_cash_total = Decimal(str(real_account['cash_in_positions'])) + Decimal(str(real_account['available_cash']))
        real_account['total_balance'] = float(calculated_cash_total.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    # ============================================
    # UPDATE STATISTICS
    # ============================================
    
    partition['total_trades'] += 1
    
    if pnl_usd_net > 0:
        partition['winning_trades'] += 1
        partition['current_streak'] = max(1, partition['current_streak'] + 1)
        partition['max_win_streak'] = max(
            partition['max_win_streak'],
            partition['current_streak']
        )
        if pnl_usd_net > partition['largest_win']:
            partition['largest_win'] = pnl_usd_net
    else:
        partition['losing_trades'] += 1
        partition['current_streak'] = min(-1, partition['current_streak'] - 1)
        partition['max_loss_streak'] = max(
            partition['max_loss_streak'],
            abs(partition['current_streak'])
        )
        if pnl_usd_net < partition['largest_loss']:
            partition['largest_loss'] = pnl_usd_net
    
    # ============================================
    # CREATE TRADE RECORD
    # ============================================
    
    average_entry_price = float(avg_entry_decimal.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP))
    pnl_pct_gross = float(
        (total_pnl_gross_decimal / total_cash_used_decimal * Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    ) if total_cash_used_decimal > 0 else 0.0
    pnl_pct_net = float(
        (pnl_usd_net_decimal / total_cash_used_decimal * Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    ) if total_cash_used_decimal > 0 else 0.0
    
    trade_record = {
        'trade_id': partition['total_trades'],
        'symbol': symbol,
        'side': position['side'],
        'entry_price': average_entry_price,
        'exit_price': exit_price,
        'quantity': float(total_quantity_decimal),
        'leverage': leverage,
        'pnl_usd_gross': pnl_usd_gross,
        'pnl_pct_gross': pnl_pct_gross,
        'pnl_usd_net': pnl_usd_net,
        'pnl_pct_net': pnl_pct_net,
        'commission': commission,
        'virtual_balance_before': float(old_virtual_balance_decimal),
        'virtual_balance_after': new_virtual_balance,
        'cash_used': float(total_cash_used_decimal),
        'entry_time': position.get('entry_time'),
        'exit_time': datetime.now().isoformat(),
        'order_id': position.get('order_id'),
        'order_ids': position.get('order_ids', [position.get('order_id')]),
        'legs': legs,
        'strategy_id': resolved_strategy_id,
        'strategy_name': resolved_strategy_name,
        'peak_profit_pct': peak_profit_pct,  # NEW: Maximum profit % reached during trade (for optimization analysis)
        'exit_reason': exit_reason  # NEW: Exit reason (e.g., "BREAKEVEN_STOP", "RSI_EXIT_LONG")
    }
    
    partition['trade_history'].append(trade_record)
    
    # Update max drawdown trade tracking if this trade caused max drawdown
    if style != "cooperative":
        current_dd = partition.get('peak_balance', new_virtual_balance) - new_virtual_balance
        if current_dd > partition.get('max_drawdown_usd', 0.0):
            partition['max_drawdown_trade_id'] = trade_record.get('trade_id')
            partition['max_drawdown_trade_symbol'] = trade_record.get('symbol')
            partition['max_drawdown_trade_pnl'] = pnl_usd_net
    
    # Remove from open positions
    del partition['open_positions'][symbol]
    
    # ============================================
    # LOGGING
    # ============================================
    
    from services.exit_monitor import format_price
    logger.info(f"✅ {partition_id} closed {position['side']} position:")
    logger.info(f"   Symbol: {symbol}")
    logger.info(f"   Legs Closed: {len(legs)}")
    logger.info(f"   Entry (avg): {format_price(average_entry_price, symbol)} → Exit: {format_price(exit_price, symbol)}")
    logger.info(f"   Quantity (total): {float(total_quantity_decimal):.6f}")
    logger.info(f"   P&L (Gross): ${pnl_usd_gross:+.2f}")
    logger.info(f"   Commission: ${commission:.2f}")
    logger.info(f"   P&L (NET): ${pnl_usd_net:+.2f}")
    logger.info(f"   ─────────────────────────────")
    logger.info(f"   Virtual Balance: ${float(old_virtual_balance_decimal):.2f} → ${new_virtual_balance:.2f}")
    logger.info(f"   Total P&L: ${partition['total_pnl']:+.2f}")
    logger.info(f"   Win Rate: {partition['winning_trades']}/{partition['total_trades']}")
    logger.info(f"   ─────────────────────────────")
    logger.info(f"   Real Available Cash: ${real_account['available_cash']:.2f}")
    logger.info(f"   Real Total Balance: ${real_account['total_balance']:.2f}")
    
    # ============================================
    # SYNC DEMO ACCOUNT BALANCE (if DEMO mode)
    # ============================================
    # In DEMO mode, sync the demo account balance from partition manager's real_account
    # This ensures the demo account balance reflects all P&L from closed trades
    if mode == "DEMO":
        try:
            import sys
            if 'main' in sys.modules:
                from main import order_executor
                if hasattr(order_executor, 'demo_accounts') and account_id in order_executor.demo_accounts:
                    demo_account = order_executor.demo_accounts[account_id]
                    # Update demo account balance to match partition manager's real_account total_balance
                    # This ensures demo account reflects all P&L from closed trades
                    new_demo_balance = real_account['total_balance']
                    old_demo_balance = demo_account.get('current_balance', DEMO_STARTING_BALANCE)
                    starting_balance = demo_account.get('starting_balance', DEMO_STARTING_BALANCE)  # Use actual starting balance from account
                    if abs(new_demo_balance - old_demo_balance) > 0.01:
                        demo_account['current_balance'] = new_demo_balance
                        demo_account['total_pnl'] = new_demo_balance - starting_balance
                        demo_account['total_pnl_pct'] = ((new_demo_balance - starting_balance) / starting_balance * 100) if starting_balance > 0 else 0.0
                        logger.info(f"   💰 DEMO: Synced demo account balance ${old_demo_balance:,.2f} → ${new_demo_balance:,.2f}")
                        # Save demo account state
                        state_store.save_demo_account_sync(
                            account_id,
                            demo_account,
                            order_executor.demo_positions.get(account_id, {}),
                            order_executor.demo_trade_history.get(account_id, [])
                        )
        except Exception as sync_error:
            logger.debug(f"   Could not sync demo account balance: {sync_error}")
    
    # Check partition health
    await check_partition_health(partition_id, account_id)
    
    # Check if sync needed (every 10 trades OR daily, whichever first)
    try:
        from services.partition_sync_scheduler import sync_scheduler
        
        # Increment trade counter
        sync_needed = sync_scheduler.increment_trade_counter(account_id)
        
        if sync_needed:
            logger.info(f"📊 Sync triggered after {SYNC_EVERY_N_TRADES} trades")
            # Caller should await sync_scheduler.sync_account(account_id, broker)
    except ImportError:
        # Fallback to simple counter if scheduler not available
        global trade_counter
        trade_counter += 1
        
        if trade_counter % SYNC_EVERY_N_TRADES == 0:
            logger.info(f"📊 Trade #{trade_counter} - Sync recommended")
    
    _update_strategy_stats(account_id, resolved_strategy_id, pnl_usd_net)
    await state_store.save_partition_state(account_id, partition_state[account_id])
    
    return pnl_usd_net, trade_record


async def import_partition_position(
    partition_id: str,
    account_id: str,
    symbol: str,
    side: str,
    quantity: float,
    entry_price: float,
    cash_used: float,
    leverage: int,
    *,
    strategy_id: Optional[str] = None,
    strategy_name: Optional[str] = None,
    entry_time: Optional[str] = None,
    order_id: Optional[str] = None
) -> None:
    """
    Import an existing broker position into the ledger (used during reconciliation).
    """
    async with position_lock:
        state = partition_state.get(account_id)
        if not state:
            raise ValueError(f"Partition state missing for account '{account_id}'")
        partition = state.get('partitions', {}).get(partition_id)
        if not partition:
            raise ValueError(f"Partition '{partition_id}' not found for account '{account_id}'")

        partition.setdefault('open_positions', {})
        partition.setdefault('deployed_cash', 0.0)

        now_iso = entry_time or datetime.now().isoformat()
        order_identifier = order_id or f"reconcile-{symbol}-{now_iso}"
        cash_used_decimal = Decimal(str(cash_used)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        position = {
            'side': side,
            'quantity': quantity,
            'entry_price': entry_price,
            'cash_used': float(cash_used_decimal),
            'virtual_balance_at_entry': partition.get('virtual_balance', 0.0),
            'entry_time': now_iso,
            'leverage': leverage,
            'order_id': order_identifier,
            'order_ids': [order_identifier],
            'legs': [{
                'quantity': quantity,
                'entry_price': entry_price,
                'cash_used': float(cash_used_decimal),
                'leverage': leverage,
                'order_id': order_identifier,
                'entry_time': now_iso,
                'virtual_balance_at_entry': partition.get('virtual_balance', 0.0),
                'strategy_id': strategy_id,
                'strategy_name': strategy_name
            }],
            'strategy_id': strategy_id,
            'strategy_name': strategy_name
        }

        partition['open_positions'][symbol] = position

        deployed_cash = Decimal(str(partition.get('deployed_cash', 0.0)))
        deployed_cash += cash_used_decimal
        partition['deployed_cash'] = float(deployed_cash.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

        real_account = state.setdefault('real_account', {})
        available_cash = Decimal(str(real_account.get('available_cash', 0.0)))
        cash_in_positions = Decimal(str(real_account.get('cash_in_positions', 0.0)))

        cash_in_positions += cash_used_decimal
        available_cash = max(Decimal('0.0'), available_cash - cash_used_decimal)

        real_account['cash_in_positions'] = float(cash_in_positions.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        real_account['available_cash'] = float(available_cash.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

    await state_store.save_partition_state(account_id, partition_state[account_id])


async def force_close_partition_position(
    partition_id: str,
    account_id: str,
    symbol: str
) -> bool:
    """
    Remove a position from the ledger without executing a broker order (used during reconciliation).
    """
    async with position_lock:
        state = partition_state.get(account_id)
        if not state:
            return False
        partition = state.get('partitions', {}).get(partition_id)
        if not partition:
            return False
        open_positions = partition.get('open_positions') or {}
        position = open_positions.pop(symbol, None)
        if not position:
            return False

        def _sum_cash_used(pos: Dict[str, Any]) -> Decimal:
            total = Decimal('0.0')
            legs = pos.get('legs') or []
            if legs:
                for leg in legs:
                    total += Decimal(str(leg.get('cash_used', 0.0)))
            else:
                total += Decimal(str(pos.get('cash_used', 0.0)))
            return total

        cash_used_decimal = _sum_cash_used(position).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        deployed_cash = Decimal(str(partition.get('deployed_cash', 0.0)))
        deployed_cash = max(Decimal('0.0'), deployed_cash - cash_used_decimal)
        partition['deployed_cash'] = float(deployed_cash.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

        real_account = state.setdefault('real_account', {})
        available_cash = Decimal(str(real_account.get('available_cash', 0.0)))
        cash_in_positions = Decimal(str(real_account.get('cash_in_positions', 0.0)))

        cash_in_positions = max(Decimal('0.0'), cash_in_positions - cash_used_decimal)
        available_cash += cash_used_decimal

        real_account['cash_in_positions'] = float(cash_in_positions.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        real_account['available_cash'] = float(available_cash.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

    await state_store.save_partition_state(account_id, partition_state[account_id])
    logger.info(f"🧹 Reconciled and removed stale position {symbol} from {partition_id} ({account_id})")
    return True


async def update_real_account_balances(
    account_id: str,
    *,
    total_balance: Optional[float] = None,
    available_cash: Optional[float] = None,
    cash_in_positions: Optional[float] = None
) -> None:
    """Update cached broker balances for an account."""
    state = partition_state.get(account_id)
    if not state:
        return
    real_account = state.setdefault('real_account', {})
    if total_balance is not None:
        real_account['total_balance'] = float(total_balance)
    if available_cash is not None:
        real_account['available_cash'] = float(available_cash)
    if cash_in_positions is not None:
        real_account['cash_in_positions'] = float(cash_in_positions)
    await state_store.save_partition_state(account_id, state)
    await rebalance_partition_virtual_balances(account_id)


async def recalculate_real_account_from_partitions(account_id: str) -> None:
    """
    Recalculate real_account balances from partition totals.
    
    This fixes balance sync issues by ensuring real_account reflects
    the actual partition state, especially important for Demo mode.
    """
    state = partition_state.get(account_id)
    if not state:
        return
    
    real_account = state.setdefault('real_account', {})
    partitions = state.get('partitions', {})
    
    # Calculate totals from partitions
    total_virtual_balance = 0.0
    total_deployed_cash = 0.0
    
    for partition_data in partitions.values():
        virtual_balance = partition_data.get('virtual_balance', 0.0)
        deployed_cash = partition_data.get('deployed_cash', 0.0)
        
        total_virtual_balance += virtual_balance
        total_deployed_cash += deployed_cash
    
    # Update real_account with calculated values
    real_account['total_balance'] = round(total_virtual_balance, 2)
    real_account['cash_in_positions'] = round(total_deployed_cash, 2)
    real_account['available_cash'] = round(total_virtual_balance - total_deployed_cash, 2)
    real_account['last_sync'] = datetime.now().isoformat()
    
    logger.info(f"🔄 Recalculated real_account for {account_id}:")
    logger.info(f"   Total Balance: ${real_account['total_balance']:,.2f}")
    logger.info(f"   Available Cash: ${real_account['available_cash']:,.2f}")
    logger.info(f"   Deployed Cash: ${real_account['cash_in_positions']:,.2f}")
    
    # Save updated state
    await state_store.save_partition_state(account_id, state)


async def rebalance_partition_virtual_balances(account_id: str) -> None:
    """
    Rebalance cooperative partitions so each reflects its configured allocation
    of the real broker balance.
    
    For DEMO accounts, ensures partition starting_balance values are calculated
    from the demo account's starting_balance (from config), not from current balance.
    """
    state = partition_state.get(account_id)
    if not state:
        return
    style = state.get('meta', {}).get('style') or state.get('partition_style')
    if (style or "").lower() != "cooperative":
        return
    real_account = state.get('real_account', {})
    total_balance = float(real_account.get('total_balance') or 0.0)
    if total_balance <= 0:
        return

    # For DEMO accounts, get the demo account's starting_balance to calculate correct partition starting balances
    mode = state.get('meta', {}).get('mode', '').upper()
    demo_account_starting = None
    if mode == "DEMO":
        try:
            import sys
            if 'main' in sys.modules:
                from main import order_executor
                if hasattr(order_executor, 'demo_accounts') and account_id in order_executor.demo_accounts:
                    demo_account = order_executor.demo_accounts[account_id]
                    demo_account_starting = float(demo_account.get('starting_balance', DEMO_STARTING_BALANCE))
                    logger.debug(f"DEMO mode: Using demo account starting_balance ${demo_account_starting:,.2f} for partition calculations")
        except Exception as e:
            logger.debug(f"Could not get demo account starting_balance: {e}, using default")
            demo_account_starting = DEMO_STARTING_BALANCE

    partitions = state.get('partitions', {})
    for partition_id, partition in partitions.items():
        allocation_pct = float(partition.get('allocation_pct') or 0.0)
        if allocation_pct <= 0:
            continue
        target_balance = round(total_balance * (allocation_pct / 100.0), 2)
        partition['virtual_balance'] = target_balance
        partition['current_balance'] = target_balance
        
        # Calculate correct starting_balance for this partition
        # For DEMO accounts: use demo account's starting_balance * allocation_pct
        # For LIVE accounts or fresh partitions: use target_balance
        total_trades = int(partition.get('total_trades') or 0)
        open_positions = partition.get('open_positions') or {}
        
        if mode == "DEMO" and demo_account_starting:
            # DEMO mode: Calculate starting_balance from demo account's starting_balance
            correct_starting = round(demo_account_starting * (allocation_pct / 100.0), 2)
            current_starting = float(partition.get('starting_balance', 0.0))
            
            # If starting_balance is incorrect, update it (important for fixing existing partitions)
            if abs(current_starting - correct_starting) > 0.01:
                logger.info(
                    f"   🔧 Fixing partition {partition_id} starting_balance: "
                    f"${current_starting:,.2f} → ${correct_starting:,.2f} "
                    f"(DEMO account starting: ${demo_account_starting:,.2f})"
                )
                partition['starting_balance'] = correct_starting
            
            # If fresh partition (no trades), also reset peak and drawdown
            if total_trades == 0 and not open_positions:
                partition['peak_balance'] = correct_starting
                partition['max_drawdown_usd'] = 0.0
                partition['max_drawdown_pct'] = 0.0
        elif total_trades == 0 and not open_positions:
            # LIVE mode or no demo account: Fresh partition, use target_balance
            partition['starting_balance'] = target_balance
            partition['peak_balance'] = target_balance
            partition['max_drawdown_usd'] = 0.0
            partition['max_drawdown_pct'] = 0.0

    await state_store.save_partition_state(account_id, state)


# ============================================
# PARTITION HEALTH CHECK
# ============================================

async def check_partition_health(partition_id: str, account_id: str):
    """
    Check partition health and auto-disable if below minimum
    """
    partition = partition_state[account_id]['partitions'][partition_id]
    min_balance = partition['min_balance_usd']
    current_balance = partition['virtual_balance']
    
    if current_balance < min_balance and partition['enabled']:
        partition['enabled'] = False
        
        logger.warning(f"⚠️ PARTITION AUTO-DISABLED: {partition_id}")
        logger.warning(f"   Current Balance: ${current_balance:.2f}")
        logger.warning(f"   Minimum Required: ${min_balance:.2f}")
        logger.warning(f"   Other partitions continue trading")
        
        await state_store.save_partition_state(account_id, partition_state[account_id])
        
        return False
    
    return True


# ============================================
# BALANCE SYNC
# ============================================

async def sync_real_account_balance(account_id: str, broker):
    """
    FULL RECONCILIATION with broker (SOURCE OF TRUTH)
    Syncs balance, positions, and fees
    Uses Decimal for precision
    
    In LIVE mode, this function uses broker data as the source of truth:
    - Broker account balance is the authoritative balance
    - Broker positions and margins are used to reconcile virtual partitions
    - Any discrepancies between calculated P&L and broker balance are reconciled
    - Broker commissions/fees are accounted for from actual trade data
    
    Steps:
    1. Get actual balance from broker (SOURCE OF TRUTH)
    2. Get all open positions from broker (SOURCE OF TRUTH)
    3. Reconcile positions with partitions
    4. Calculate actual cash in positions (from broker data)
    5. Account for actual fees/commissions (from broker)
    6. Update available cash
    7. Verify accuracy and adjust partitions if needed
    
    RECOMMENDED: Call every 10 trades OR every 24 hours (whichever first)
    In LIVE mode, also call after each trade close to ensure accuracy
    """
    state = partition_state[account_id]
    real_account = state['real_account']
    
    logger.info(f"🔄 FULL SYNC starting for {account_id}...")
    
    # ============================================
    # STEP 1: Get Actual Balance from Broker (UNIFIED)
    # ============================================
    
    # Use unified interface - works for all brokers (Coinbase USD/USDC, Binance USDT/BNB, Kraken USD/EUR)
    actual_balance_raw = await broker.get_account_balance_usd()
    actual_balance = Decimal(str(actual_balance_raw))
    
    logger.info(f"   Broker Balance: ${float(actual_balance):.2f} USD equivalent")
    logger.info(f"   Broker: {broker.get_broker_name()}")
    
    # ============================================
    # STEP 2: Get All Open Positions from Broker (UNIFIED)
    # ============================================
    
    # Unified interface returns standardized position format across all brokers
    broker_positions = await broker.get_all_positions()
    
    logger.info(f"   Broker Positions: {len(broker_positions)}")
    
    # ============================================
    # STEP 3: Reconcile Positions with Partitions
    # ============================================
    
    # Calculate cash in positions from BROKER data (source of truth)
    actual_cash_in_positions = Decimal('0')
    reconciled_positions = {}
    
    for broker_pos in broker_positions:
        symbol = broker_pos.get('symbol') or broker_pos.get('product_id', '')
        if not symbol:
            logger.warning(f"⚠️ Broker position missing symbol, skipping: {broker_pos}")
            continue
        
        # Handle different broker position formats (position_amt, positionAmt, quantity, etc.)
        quantity = abs(float(broker_pos.get('position_amt') or broker_pos.get('positionAmt') or broker_pos.get('quantity', 0.0)))
        if quantity <= 0:
            continue  # Skip zero positions
        
        entry_price = float(broker_pos.get('entry_price') or broker_pos.get('avg_open_price') or broker_pos.get('average_open_price', 0.0))
        
        # Get margin from unified position format (already in USD equivalent!)
        # This works for ALL brokers:
        # - Coinbase: margin in USD/USDC
        # - Binance: margin in USDT (converted to USD 1:1)
        # - Kraken: margin in USD
        margin_usd = Decimal(str(broker_pos.get('margin_usd') or 0.0))
        
        actual_cash_in_positions += margin_usd
        
        # Try to match to partition using client_order_id or symbol
        matched_partition = None
        for pid, partition in state['partitions'].items():
            if symbol in partition['open_positions']:
                matched_partition = pid
                
                # Verify position matches
                our_pos = partition['open_positions'][symbol]
                our_quantity = our_pos['quantity']
                
                if abs(quantity - our_quantity) > 0.001:  # Allow small tolerance
                    logger.warning(f"   ⚠️ Position mismatch for {pid} {symbol}:")
                    logger.warning(f"      Our tracking: {our_quantity}")
                    logger.warning(f"      Broker actual: {quantity}")
                    logger.warning(f"      Using broker value (source of truth)")
                    
                    # Update our tracking to match broker
                    our_pos['quantity'] = quantity
                
                # Update cash_used to actual from broker (use margin_usd from unified format)
                old_cash = our_pos['cash_used']
                new_cash = float(margin_usd.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
                
                if abs(new_cash - old_cash) > 1.0:  # > $1 difference
                    logger.warning(f"   ⚠️ Cash used mismatch for {pid} {symbol}:")
                    logger.warning(f"      Our tracking: ${old_cash:.2f}")
                    logger.warning(f"      Broker actual: ${new_cash:.2f} USD")
                    logger.warning(f"      Difference: ${abs(new_cash - old_cash):.2f}")
                    logger.warning(f"      Likely due to:")
                    logger.warning(f"      - Actual commissions/fees deducted")
                    logger.warning(f"      - Broker rounding")
                    logger.warning(f"      Updating to broker value (source of truth)")
                    
                    # Update to actual
                    our_pos['cash_used'] = new_cash
                
                reconciled_positions[symbol] = pid
                break
        
        if not matched_partition:
            from services.exit_monitor import format_price
            logger.warning(f"   ⚠️ Unmatched position on broker: {symbol}")
            logger.warning(f"      Quantity: {quantity}")
            logger.warning(f"      Entry: {format_price(entry_price, symbol)}")
            logger.warning(f"      This may be an external trade!")
    
    # ============================================
    # STEP 4: Check for Orphaned Virtual Positions
    # ============================================
    
    # Find positions in our tracking that don't exist on broker
    for pid, partition in state['partitions'].items():
        for symbol in list(partition['open_positions'].keys()):
            if symbol not in reconciled_positions or reconciled_positions[symbol] != pid:
                logger.warning(f"   ⚠️ Orphaned position in {pid}: {symbol}")
                logger.warning(f"      Position exists in our tracking but not on broker")
                logger.warning(f"      Possible: Already closed on broker, or error")
                logger.warning(f"      Removing from tracking")
                
                # Free the cash
                orphaned_cash = Decimal(str(partition['open_positions'][symbol]['cash_used']))
                del partition['open_positions'][symbol]
    
    # ============================================
    # STEP 5: Calculate Expected Balance
    # ============================================
    
    expected_balance = Decimal('0')
    for p in state['partitions'].values():
        expected_balance += Decimal(str(p['virtual_balance']))
    
    # Check for discrepancy
    discrepancy = abs(actual_balance - expected_balance)
    partition_ids = list(state['partitions'].keys())
    raw_delta = actual_balance - expected_balance

    if partition_ids and abs(raw_delta) > Decimal('0.0001'):
        logger.info(f"   Adjusting partitions by delta ${float(raw_delta):.2f} to align with broker balance")
        per_partition = (raw_delta / len(partition_ids)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        cumulative = Decimal('0')

        for idx, pid in enumerate(partition_ids):
            adjust = per_partition
            if idx == len(partition_ids) - 1:
                adjust = raw_delta - cumulative
            cumulative += adjust
            partition = state['partitions'][pid]
            new_balance = Decimal(str(partition['virtual_balance'])) + adjust
            partition['virtual_balance'] = float(new_balance.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
            logger.info(f"     • {pid}: adjustment {float(adjust):+.2f} → ${partition['virtual_balance']:.2f}")

        expected_balance = Decimal('0')
        for p in state['partitions'].values():
            expected_balance += Decimal(str(p['virtual_balance']))
        discrepancy = abs(actual_balance - expected_balance)

    if discrepancy > Decimal('5.0'):  # $5 tolerance
        logger.warning(f"⚠️ Balance discrepancy detected!")
        logger.warning(f"   Actual (Broker): ${float(actual_balance):.2f}")
        logger.warning(f"   Expected (Virtual): ${float(expected_balance):.2f}")
        logger.warning(f"   Difference: ${float(discrepancy):.2f}")
        logger.warning(f"   Possible causes:")
        logger.warning(f"   - Broker commissions/fees (actual vs estimated)")
        logger.warning(f"   - Funding fees (futures contracts)")
        logger.warning(f"   - External deposits/withdrawals")
        logger.warning(f"   - External trades")
        
        # Send Telegram alert if large discrepancy
        if discrepancy > Decimal('50.0'):
            # Will be implemented when Telegram integration is added
            logger.critical(f"🚨 LARGE DISCREPANCY: ${float(discrepancy):.2f}")
    
    if _get_partition_style(account_id) == "cooperative":
        _rebalance_cooperative(account_id)
        expected_balance = Decimal('0')
        for p in state['partitions'].values():
            expected_balance += Decimal(str(p['virtual_balance']))
        discrepancy = abs(actual_balance - expected_balance)

    # ============================================
    # STEP 6: Update Real Account
    # ============================================
    
    # Update total balance to actual from broker
    real_account['total_balance'] = float(actual_balance.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    real_account['last_sync'] = datetime.now().isoformat()
    
    # Update cash in positions from reconciled positions
    real_account['cash_in_positions'] = float(actual_cash_in_positions.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    # Calculate available cash (broker balance - positions)
    available_cash = actual_balance - actual_cash_in_positions
    real_account['available_cash'] = float(available_cash.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    # ============================================
    # STEP 7: Verify Accuracy
    # ============================================
    
    # Verify: cash in positions + available = total
    calculated_total = actual_cash_in_positions + available_cash
    
    if abs(calculated_total - actual_balance) > Decimal('0.02'):  # 2 cent tolerance
        logger.error(f"❌ Cash calculation error!")
        logger.error(f"   In positions: ${float(actual_cash_in_positions):.2f}")
        logger.error(f"   Available: ${float(available_cash):.2f}")
        logger.error(f"   Sum: ${float(calculated_total):.2f}")
        logger.error(f"   Broker total: ${float(actual_balance):.2f}")
    
    # ============================================
    # STEP 8: Log Sync Summary
    # ============================================
    
    logger.info(f"✅ FULL SYNC complete for {account_id}:")
    logger.info(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info(f"   Total Balance: ${real_account['total_balance']:,.2f}")
    logger.info(f"   Cash in Positions: ${real_account['cash_in_positions']:,.2f}")
    logger.info(f"   Available Cash: ${real_account['available_cash']:,.2f}")
    logger.info(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info(f"   Virtual Total: ${float(expected_balance):,.2f}")
    logger.info(f"   Discrepancy: ${float(discrepancy):,.2f}")
    logger.info(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info(f"   Broker Positions: {len(broker_positions)}")
    logger.info(f"   Matched Positions: {len(reconciled_positions)}")
    
    # Log per-partition status
    for pid, partition in state['partitions'].items():
        open_count = len(partition['open_positions'])
        status = "ENABLED" if partition['enabled'] else "DISABLED"
        logger.info(f"   {pid}: ${partition['virtual_balance']:,.2f}, {open_count} pos, {status}")
    
    logger.info(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    await state_store.save_partition_state(account_id, state)
    
    # Return sync result
    return {
        'actual_balance': float(actual_balance),
        'expected_balance': float(expected_balance),
        'discrepancy': float(discrepancy),
        'cash_in_positions': float(actual_cash_in_positions),
        'available_cash': float(available_cash),
        'positions_reconciled': len(reconciled_positions),
        'positions_on_broker': len(broker_positions)
    }


# ============================================
# RUNTIME TELEMETRY - REAL-TIME MARGIN TRACKING
# ============================================

def _get_trading_period(now: datetime) -> str:
    """
    Determine current trading period for Coinbase futures.
    
    Coinbase derivatives trading day: 6 PM ET - 4 PM ET (next day)
    - Intraday: 6 PM ET - 4 PM ET (trading hours)
    - Overnight: 4 PM ET - 6 PM ET (after hours, higher margin)
    - Weekend: Friday 4 PM ET - Sunday 6 PM ET (or Monday 6 PM ET)
    
    Returns: 'intraday', 'overnight', or 'weekend'
    """
    from zoneinfo import ZoneInfo
    et_tz = ZoneInfo("America/New_York")
    now_et = now.astimezone(et_tz)
    
    weekday = now_et.weekday()  # 0=Monday, 6=Sunday
    hour = now_et.hour
    minute = now_et.minute
    time_minutes = hour * 60 + minute
    
    # Weekend: Friday 4 PM ET (16:00) through Sunday 6 PM ET (18:00)
    if weekday == 4:  # Friday
        if time_minutes >= 16 * 60:  # 4 PM ET or later
            return 'weekend'
    elif weekday == 5:  # Saturday
        return 'weekend'
    elif weekday == 6:  # Sunday
        if time_minutes < 18 * 60:  # Before 6 PM ET
            return 'weekend'
    
    # Trading day: 6 PM ET (18:00) - 4 PM ET (16:00) next day
    # Intraday: 6 PM ET - 4 PM ET
    # Overnight: 4 PM ET - 6 PM ET (next day)
    if time_minutes >= 18 * 60 or time_minutes < 16 * 60:
        # 6 PM ET - midnight or midnight - 4 PM ET = intraday
        return 'intraday'
    else:
        # 4 PM ET - 6 PM ET = overnight (higher margin period)
        return 'overnight'


async def update_runtime_margin_telemetry(account_id: str, broker) -> None:
    """
    Update real-time margin usage telemetry from broker positions.
    Tracks margin usage per strategy and account-level metrics across
    intraday, overnight, and weekend periods.
    
    This function:
    1. Fetches all positions from broker
    2. Calculates margin usage per strategy
    3. Updates worst margin usage with timestamps for each period
    4. Updates account-level margin metrics
    """
    state = partition_state.get(account_id)
    if not state:
        return
    
    try:
        # Get all positions from broker
        broker_positions = await asyncio.to_thread(broker.get_all_positions) if hasattr(broker, 'get_all_positions') else []
        if not broker_positions:
            return
        
        real_account = state.get('real_account', {})
        total_balance = float(real_account.get('total_balance') or 0.0)
        if total_balance <= 0:
            return
        
        # Initialize telemetry if not exists
        if 'runtime_telemetry' not in state:
            state['runtime_telemetry'] = {
                'account': {},
                'strategies': {}
            }
        telemetry = state['runtime_telemetry']
        
        # Track margin per strategy
        strategy_margin: Dict[str, float] = {}
        strategy_qty: Dict[str, float] = {}
        total_margin_used = 0.0
        
        # Map broker positions to strategies via partition open_positions
        for broker_pos in broker_positions:
            symbol = broker_pos.get('symbol', '').upper()
            margin_usd = float(broker_pos.get('margin_usd', 0.0))
            quantity = abs(float(broker_pos.get('position_amt', 0.0)))
            
            if margin_usd <= 0 or quantity <= 0:
                continue
            
            total_margin_used += margin_usd
            
            # Find which strategy owns this position
            strategy_id = None
            for partition_id, partition in state.get('partitions', {}).items():
                open_positions = partition.get('open_positions', {})
                if symbol in open_positions:
                    pos = open_positions[symbol]
                    strategy_id = pos.get('strategy_id')
                    if strategy_id:
                        strategy_margin[strategy_id] = strategy_margin.get(strategy_id, 0.0) + margin_usd
                        strategy_qty[strategy_id] = strategy_qty.get(strategy_id, 0.0) + quantity
                        break
        
        # Determine current trading period
        now = datetime.now()
        now_str = now.strftime("%H:%M")
        period = _get_trading_period(now)
        
        # Update overnight margin for positions held overnight
        # This ensures we capture margin requirements for positions transitioning to overnight
        await _update_overnight_margin_for_open_positions(account_id, state)
        
        # CRITICAL: All margin calculations use MAIN ACCOUNT WALLET BALANCE (total_balance)
        # This is the total_balance from real_account, which comes from broker.get_account_balance_usd()
        # For Coinbase: This is the total_wallet_balance (main account, not partition-specific)
        margin_usage_pct = (total_margin_used / total_balance * 100.0) if total_balance > 0 else 0.0
        free_margin_pct = max(0.0, 100.0 - margin_usage_pct)
        
        account_telemetry = telemetry.setdefault('account', {})
        account_telemetry['current_margin_usage_pct'] = margin_usage_pct
        account_telemetry['current_free_margin_pct'] = free_margin_pct
        account_telemetry['current_period'] = period
        
        # Track worst margin usage per period (intraday, overnight, weekend)
        period_key = f'worst_margin_usage_pct_{period}'
        period_time_key = f'worst_margin_usage_time_{period}'
        worst_margin_period = account_telemetry.get(period_key)
        if worst_margin_period is None or margin_usage_pct > worst_margin_period:
            account_telemetry[period_key] = margin_usage_pct
            account_telemetry[period_time_key] = now_str
        
        # Also track worst overall (today) for backward compatibility
        worst_margin = account_telemetry.get('worst_margin_usage_pct_today')
        if worst_margin is None or margin_usage_pct > worst_margin:
            account_telemetry['worst_margin_usage_pct_today'] = margin_usage_pct
            account_telemetry['worst_margin_usage_time_today'] = now_str
        
        # Track worst free margin per period
        worst_free_period_key = f'worst_free_margin_pct_{period}'
        worst_free_time_key = f'worst_free_margin_time_{period}'
        worst_free_period = account_telemetry.get(worst_free_period_key)
        if worst_free_period is None or free_margin_pct < worst_free_period:
            account_telemetry[worst_free_period_key] = free_margin_pct
            account_telemetry[worst_free_time_key] = now_str
        
        # Also track worst overall free margin (today)
        worst_free = account_telemetry.get('worst_free_margin_pct_today')
        if worst_free is None or free_margin_pct < worst_free:
            account_telemetry['worst_free_margin_pct_today'] = free_margin_pct
            account_telemetry['worst_free_margin_time_today'] = now_str
        
        account_telemetry['current_total_qty'] = sum(strategy_qty.values())
        
        # Update per-strategy telemetry
        # CRITICAL: Strategy margin % is calculated against MAIN ACCOUNT WALLET BALANCE
        # This ensures strategy metrics are relative to the total account, not partition-specific
        for strategy_id, margin_usd in strategy_margin.items():
            strategy_telemetry = telemetry['strategies'].setdefault(strategy_id, {})
            margin_pct = (margin_usd / total_balance * 100.0) if total_balance > 0 else 0.0
            
            strategy_telemetry['current_margin_usage_pct'] = margin_pct
            strategy_telemetry['current_total_qty'] = strategy_qty.get(strategy_id, 0.0)
            strategy_telemetry['current_period'] = period
            
            # Track worst margin usage per strategy per period
            strat_period_key = f'worst_margin_usage_pct_{period}'
            strat_period_time_key = f'worst_margin_usage_time_{period}'
            worst_strat_period = strategy_telemetry.get(strat_period_key)
            if worst_strat_period is None or margin_pct > worst_strat_period:
                strategy_telemetry[strat_period_key] = margin_pct
                strategy_telemetry[strat_period_time_key] = now_str
            
            # Also track worst overall per strategy (today)
            worst_strat_margin = strategy_telemetry.get('worst_margin_usage_pct_today')
            if worst_strat_margin is None or margin_pct > worst_strat_margin:
                strategy_telemetry['worst_margin_usage_pct_today'] = margin_pct
                strategy_telemetry['worst_margin_usage_time_today'] = now_str
        
        await state_store.save_partition_state(account_id, state)
        
    except Exception as exc:
        logger.debug(f"Error updating runtime margin telemetry for {account_id}: {exc}")


async def _capture_margin_on_position_open(
    account_id: str,
    strategy_id: str,
    symbol: str,
    entry_price: float,
    quantity: float,
    leverage: int,
    cash_used: float,
    state: Dict
) -> None:
    """
    Capture margin per contract when a position is opened.
    
    This records the actual margin requirement at trade execution time:
    - Intraday margin: Captured when position opens during trading hours
    - Overnight margin: Will be updated when position transitions to overnight
    
    Args:
        account_id: Account identifier
        strategy_id: Strategy identifier
        symbol: Trading symbol
        entry_price: Entry price
        quantity: Position quantity
        leverage: Leverage used
        cash_used: Cash used for margin
        state: Partition state dictionary
    """
    try:
        # Initialize telemetry if needed
        if 'runtime_telemetry' not in state:
            state['runtime_telemetry'] = {
                'account': {},
                'strategies': {}
            }
        telemetry = state['runtime_telemetry']
        strategy_telemetry = telemetry['strategies'].setdefault(strategy_id, {})
        
        # Calculate margin per 1 contract
        # Margin per contract = cash_used / quantity
        if quantity > 0:
            margin_per_1 = cash_used / quantity
            
            # Determine if this is intraday or overnight
            now = datetime.now()
            current_hour = now.hour
            is_overnight = current_hour >= 0 and current_hour < 8  # 12am-8am typically overnight
            
            if is_overnight:
                # Overnight margin requirement
                strategy_telemetry['margin_per_1_1am'] = margin_per_1
                logger.debug(f"Captured overnight margin for {strategy_id} ({symbol}): ${margin_per_1:,.2f}")
            else:
                # Intraday margin requirement
                strategy_telemetry['margin_per_1_1pm'] = margin_per_1
                logger.debug(f"Captured intraday margin for {strategy_id} ({symbol}): ${margin_per_1:,.2f}")
            
            # Also update the most recent margin (for current positions)
            strategy_telemetry['margin_per_1_current'] = margin_per_1
            strategy_telemetry['margin_per_1_last_capture'] = now.isoformat()
            
            await state_store.save_partition_state(account_id, state)
            
    except Exception as exc:
        logger.debug(f"Error capturing margin on position open for {strategy_id}: {exc}")


async def _update_overnight_margin_for_open_positions(account_id: str, state: Dict) -> None:
    """
    Update margin requirements for positions held overnight.
    
    This is called when positions transition from intraday to overnight holding.
    Captures the overnight margin requirement to ensure adequate safety buffer.
    
    Args:
        account_id: Account identifier
        state: Partition state dictionary
    """
    try:
        # Initialize telemetry if needed
        if 'runtime_telemetry' not in state:
            return
        
        telemetry = state['runtime_telemetry']
        now = datetime.now()
        current_hour = now.hour
        
        # Check if we're in overnight period (typically 4pm ET / 1pm PT onwards, or after market close)
        # For crypto futures, overnight is typically after 4pm ET
        is_overnight_period = current_hour >= 13 or current_hour < 8  # 1pm-8am
        
        if not is_overnight_period:
            return
        
        # Update margin for all open positions
        for partition_id, partition in state.get('partitions', {}).items():
            open_positions = partition.get('open_positions', {})
            
            for symbol, position in open_positions.items():
                strategy_id = position.get('strategy_id')
                if not strategy_id:
                    continue
                
                cash_used = position.get('cash_used', 0.0)
                quantity = position.get('quantity', 0.0)
                
                if quantity > 0 and cash_used > 0:
                    # Calculate current margin per contract
                    margin_per_1 = cash_used / quantity
                    
                    # Update overnight margin requirement
                    strategy_telemetry = telemetry['strategies'].setdefault(strategy_id, {})
                    strategy_telemetry['margin_per_1_1am'] = margin_per_1
                    strategy_telemetry['margin_per_1_overnight_updated'] = now.isoformat()
                    
                    logger.debug(f"Updated overnight margin for {strategy_id} ({symbol}): ${margin_per_1:,.2f}")
        
        await state_store.save_partition_state(account_id, state)
        
    except Exception as exc:
        logger.debug(f"Error updating overnight margin for {account_id}: {exc}")


# ============================================
# SAFETY CHECKS
# ============================================

def pre_trade_safety_check(
    partition_id: str,
    account_id: str,
    desired_cash: float
) -> Tuple[bool, List[Tuple[str, str]]]:
    """
    Comprehensive safety check before placing order
    
    Returns:
        Tuple of (passed, checks)
    """
    state = partition_state[account_id]
    partition = state['partitions'][partition_id]
    real_account = state['real_account']
    
    checks = []
    
    # 1. Partition enabled?
    if not partition['enabled']:
        checks.append(("FAIL", "Partition disabled"))
    
    # 2. Sufficient virtual balance?
    if desired_cash > partition['virtual_balance']:
        checks.append((
            "FAIL",
            f"Desired ${desired_cash:.2f} > Virtual ${partition['virtual_balance']:.2f}"
        ))
    
    # 3. Sufficient real cash available?
    if desired_cash > real_account['available_cash']:
        checks.append((
            "WARN",
            f"Desired ${desired_cash:.2f} > Available ${real_account['available_cash']:.2f}"
        ))
    
    # 4. Total allocation check (use Decimal for precision)
    total_virtual_decimal = Decimal('0')
    for p in state['partitions'].values():
        total_virtual_decimal += Decimal(str(p['virtual_balance']))
    
    real_total_decimal = Decimal(str(real_account['total_balance']))
    tolerance = real_total_decimal * Decimal('0.05')  # 5% tolerance
    
    if total_virtual_decimal > (real_total_decimal + tolerance):
        checks.append((
            "FAIL",
            f"Over-allocation: Virtual ${float(total_virtual_decimal):.2f} > Real ${float(real_total_decimal):.2f}"
        ))
    
    # 5. Cash + positions should equal total (use Decimal)
    cash_in_positions_decimal = Decimal(str(real_account['cash_in_positions']))
    available_cash_decimal = Decimal(str(real_account['available_cash']))
    calculated_total = cash_in_positions_decimal + available_cash_decimal
    
    if abs(calculated_total - real_total_decimal) > Decimal('1.00'):  # $1 tolerance
        checks.append((
            "WARN",
            f"Cash mismatch: ${float(calculated_total):.2f} != ${float(real_total_decimal):.2f}"
        ))
    
    # 6. Position limit (1 per partition)
    if len(partition['open_positions']) >= 1:
        checks.append(("FAIL", "Partition already has open position"))
    
    # Log results
    for level, msg in checks:
        if level == "FAIL":
            logger.error(f"❌ {msg}")
        elif level == "WARN":
            logger.warning(f"⚠️ {msg}")
    
    # Return pass/fail
    failures = [c for c in checks if c[0] == "FAIL"]
    return len(failures) == 0, checks


# ============================================
# GETTER FUNCTIONS
# ============================================

def get_partition_state(account_id: str, partition_id: str) -> Optional[Dict]:
    """Get partition state"""
    if account_id not in partition_state:
        return None
    if partition_id not in partition_state[account_id]['partitions']:
        return None
    return partition_state[account_id]['partitions'][partition_id]


def get_all_partitions(account_id: str) -> Optional[Dict]:
    """Get all partitions for an account"""
    if account_id not in partition_state:
        return None
    return partition_state[account_id]['partitions']


def get_account_state(account_id: str) -> Optional[Dict]:
    """Get complete account state"""
    return partition_state.get(account_id)


def account_is_flat(account_id: str) -> bool:
    """Return True if account has no open positions or reserved cash."""
    state = partition_state.get(account_id)
    if not state:
        return True

    if abs(state['real_account'].get('cash_in_positions', 0.0)) > 1e-6:
        return False

    for partition in state['partitions'].values():
        if partition.get('open_positions'):
            if len(partition['open_positions']) > 0:
                return False
    return True


def get_partition_summaries(account_id: Optional[str] = None) -> List[Dict]:
    """Provide partition summaries for health/reporting endpoints."""
    accounts = [account_id] if account_id else list(partition_state.keys())
    summaries: List[Dict] = []

    for acc_id in accounts:
        state = partition_state.get(acc_id)
        if not state:
            continue

        # CRITICAL: Recalculate real_account from partition totals to ensure accuracy
        real_account = state.setdefault('real_account', {})
        partitions = state.get('partitions', {})
        
        # Calculate accurate totals from partitions
        total_virtual_balance = 0.0
        total_deployed_cash = 0.0
        
        for partition_data in partitions.values():
            virtual_balance = partition_data.get('virtual_balance', 0.0)
            deployed_cash = partition_data.get('deployed_cash', 0.0)
            
            total_virtual_balance += virtual_balance
            total_deployed_cash += deployed_cash
        
        # Update real_account with accurate calculated values
        real_account['total_balance'] = round(total_virtual_balance, 2)
        real_account['cash_in_positions'] = round(total_deployed_cash, 2)
        real_account['available_cash'] = round(total_virtual_balance - total_deployed_cash, 2)
        partitions_summary = []
        for partition_id, data in state['partitions'].items():
            starting_balance = data.get('starting_balance', 0.0)
            current_balance = data.get('virtual_balance', 0.0)
            peak_balance = data.get('peak_balance', 0.0)
            # Calculate P&L from current state (virtual_balance - starting_balance)
            # This ensures accuracy for cooperative partitions after rebalancing
            # The stored total_pnl field is kept for audit/historical purposes but
            # display uses calculated value to reflect current state
            total_pnl = current_balance - starting_balance
            total_pnl_pct = (total_pnl / starting_balance * 100) if starting_balance else 0.0
            max_dd_usd = data.get('max_drawdown_usd', 0.0)
            max_dd_pct = data.get('max_drawdown_pct', 0.0)
            avg_dd_usd = data.get('avg_drawdown_usd', max_dd_usd)
            avg_dd_pct = data.get('avg_drawdown_pct', max_dd_pct)

            open_position_details: List[Dict[str, Any]] = []
            for symbol, pos in (data.get('open_positions') or {}).items():
                entry_time = pos.get('entry_time')
                if not entry_time and pos.get('legs'):
                    entry_time = pos['legs'][0].get('entry_time')
                open_position_details.append({
                    'symbol': symbol,
                    'side': pos.get('side'),
                    'quantity': round(pos.get('quantity', 0.0), 8),
                    'entry_price': round(pos.get('entry_price', 0.0), 2),
                    'cash_used': round(pos.get('cash_used', 0.0), 2),
                    'leverage': pos.get('leverage'),
                    'entry_time': entry_time,
                    'partition_id': partition_id
                })

            partitions_summary.append({
                'partition_id': partition_id,
                'enabled': data.get('enabled', True),
                'allocation_pct': data.get('allocation_pct', 0.0),
                'virtual_balance': round(current_balance, 2),
                'current_balance': round(current_balance, 2),
                'starting_balance': round(starting_balance, 2),
                'peak_balance': round(peak_balance, 2),
                'total_pnl': round(total_pnl, 2),
                'total_pnl_pct': round(total_pnl_pct, 2),
                'open_positions': len(data.get('open_positions', {})),
                'open_position_details': open_position_details,
                'leverage': data.get('leverage'),
                'min_balance_usd': data.get('min_balance_usd', 0.0),
                'strategy_ids': data.get('strategy_ids', []),
                'total_trades': data.get('total_trades', 0),
                'winning_trades': data.get('winning_trades', 0),
                'losing_trades': data.get('losing_trades', 0),
                'max_drawdown_usd': round(max_dd_usd, 2),
                'max_drawdown_pct': round(max_dd_pct, 2),
                'avg_drawdown_usd': round(avg_dd_usd, 2),
                'avg_drawdown_pct': round(avg_dd_pct, 2)
            })

        strategies_summary: List[Dict[str, Any]] = []
        strategy_labels = state.get('meta', {}).get('strategy_labels', {})
        for strategy_id, data in (state.get('strategies') or {}).items():
            label = data.get('label') or strategy_labels.get(strategy_id) or strategy_id
            strategies_summary.append({
                'strategy_id': strategy_id,
                'label': label,
                'allocation_pct': data.get('allocation_pct'),
                'virtual_balance': round(data.get('virtual_balance', 0.0), 2),
                'starting_balance': round(data.get('starting_balance', 0.0), 2),
                'total_pnl': round(data.get('total_pnl', 0.0), 2),
                'total_pnl_pct': round(data.get('total_pnl_pct', 0.0), 2),
                'max_drawdown_pct': round(data.get('max_drawdown_pct', 0.0), 2),
                'max_drawdown_usd': round(data.get('max_drawdown_usd', 0.0), 2),
                'total_trades': data.get('total_trades', 0),
                'winning_trades': data.get('winning_trades', 0),
                'losing_trades': data.get('losing_trades', 0)
            })

        summaries.append({
            'account_id': acc_id,
            'real_account': {
                'total_balance': round(real_account.get('total_balance', 0.0), 2),
                'available_cash': round(real_account.get('available_cash', 0.0), 2),
                'cash_in_positions': round(real_account.get('cash_in_positions', 0.0), 2),
                'last_sync': real_account.get('last_sync')
            },
            'partition_style': state.get('meta', {}).get('style', 'isolated'),
            'partitions': partitions_summary,
            'strategies': strategies_summary,
            'is_flat': account_is_flat(acc_id)
        })

    return summaries

