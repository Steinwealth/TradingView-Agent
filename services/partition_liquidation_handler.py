"""
TradingView Agent - Partition Liquidation Handler (V3.6)

Handles liquidation events and emergency exits for partitions
Ensures liquidated/failed partitions are disabled while others continue trading

Key Features:
- Detects liquidation events
- Handles emergency exits
- Auto-disables affected partition
- Protects other partitions
- Sends critical Telegram alerts
"""

import logging
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, Tuple

logger = logging.getLogger(__name__)


async def handle_partition_liquidation(
    partition_id: str,
    account_id: str,
    symbol: str,
    liquidation_price: float,
    reason: str = "liquidation"
) -> Dict:
    """
    Handle partition liquidation event
    
    Actions:
    1. Calculate total loss
    2. Update partition balance to $0
    3. Disable partition permanently
    4. Free up locked cash
    5. Send critical Telegram alert
    6. Log event for audit
    7. Other partitions continue trading
    
    Args:
        partition_id: Partition that was liquidated
        account_id: Account identifier
        symbol: Trading symbol
        liquidation_price: Price at which liquidation occurred
        reason: Reason for liquidation (default: "liquidation")
        
    Returns:
        Dict with liquidation details
    """
    from services.partition_manager import partition_state
    
    logger.critical(f"🚨 LIQUIDATION EVENT: {partition_id}")
    logger.critical(f"   Account: {account_id}")
    logger.critical(f"   Symbol: {symbol}")
    logger.critical(f"   Price: ${liquidation_price:,.2f}")
    logger.critical(f"   Reason: {reason}")
    
    state = partition_state[account_id]
    partition = state['partitions'][partition_id]
    real_account = state['real_account']
    
    # Get position if exists
    position = partition['open_positions'].get(symbol)
    
    if not position:
        logger.warning(f"⚠️ No open position found for {symbol} in {partition_id}")
        logger.warning(f"   Liquidation may have already been processed")
    
    # ============================================
    # CALCULATE TOTAL LOSS
    # ============================================
    
    if position:
        entry_price = position['entry_price']
        cash_used = position['cash_used']
        virtual_balance_at_entry = position['virtual_balance_at_entry']
        leverage = position['leverage']
        
        # On liquidation, lose the entire cash used (collateral)
        loss_usd = Decimal(str(cash_used))
        
        logger.critical(f"   Entry Price: ${entry_price:,.2f}")
        logger.critical(f"   Liquidation Price: ${liquidation_price:,.2f}")
        logger.critical(f"   Cash Lost: ${float(loss_usd):,.2f}")
        logger.critical(f"   Leverage: {leverage}x")
    else:
        loss_usd = Decimal('0')
    
    # ============================================
    # UPDATE PARTITION (Set to $0, DISABLE)
    # ============================================
    
    old_balance = partition['virtual_balance']
    
    # Set balance to $0 (total loss of collateral)
    partition['virtual_balance'] = 0.00
    partition['total_pnl'] -= float(loss_usd)  # Record the loss
    
    # CRITICAL: Disable partition permanently
    partition['enabled'] = False
    
    # Update max drawdown (will be 100%)
    if partition['peak_balance'] > 0:
        partition['max_drawdown_usd'] = partition['peak_balance']
        partition['max_drawdown_pct'] = 100.0
    
    # Add to stats
    partition['total_trades'] += 1
    partition['losing_trades'] += 1
    
    logger.critical(f"   Virtual Balance: ${old_balance:.2f} → $0.00")
    logger.critical(f"   Status: ENABLED → DISABLED ❌")
    
    # ============================================
    # UPDATE REAL ACCOUNT
    # ============================================
    
    if position:
        # Free up the cash (but it's lost)
        # Cash in positions reduced by cash_used
        # Available cash doesn't increase (money is gone)
        
        cash_in_positions = Decimal(str(real_account['cash_in_positions']))
        cash_used_decimal = Decimal(str(cash_used))
        
        # Cash is no longer "in positions" (it's gone)
        cash_in_positions -= cash_used_decimal
        
        # Ensure not negative
        if cash_in_positions < Decimal('0'):
            cash_in_positions = Decimal('0')
        
        deployed_cash = Decimal(str(partition.get('deployed_cash', 0.0)))
        deployed_cash -= cash_used_decimal
        if deployed_cash < Decimal('0'):
            deployed_cash = Decimal('0')
        partition['deployed_cash'] = float(deployed_cash.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        
        real_account['cash_in_positions'] = float(
            cash_in_positions.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        )
        
        # Remove position
        del partition['open_positions'][symbol]
    
    # Update total balance (sum virtual partitions)
    total_virtual = Decimal('0')
    for p in state['partitions'].values():
        total_virtual += Decimal(str(p['virtual_balance']))
    
    real_account['total_balance'] = float(
        total_virtual.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    )
    
    # ============================================
    # CHECK OTHER PARTITIONS
    # ============================================
    
    other_partitions = [
        p for pid, p in state['partitions'].items() 
        if pid != partition_id
    ]
    
    enabled_count = sum(1 for p in other_partitions if p['enabled'])
    
    logger.info(f"📊 Other Partitions Status:")
    logger.info(f"   Total partitions: {len(state['partitions'])}")
    logger.info(f"   Liquidated: 1 ({partition_id})")
    logger.info(f"   Still enabled: {enabled_count}")
    
    if enabled_count > 0:
        logger.info(f"   ✅ Other partitions continue trading")
        total_remaining = sum(p['virtual_balance'] for p in other_partitions)
        logger.info(f"   Remaining capital: ${total_remaining:,.2f}")
    else:
        logger.critical(f"   ⚠️ ALL PARTITIONS DISABLED - ACCOUNT STOPPED")
    
    # ============================================
    # CREATE LIQUIDATION RECORD
    # ============================================
    
    liquidation_record = {
        'partition_id': partition_id,
        'symbol': symbol,
        'liquidation_price': liquidation_price,
        'entry_price': position['entry_price'] if position else None,
        'leverage': position['leverage'] if position else None,
        'cash_lost': float(loss_usd),
        'virtual_balance_before': old_balance,
        'virtual_balance_after': 0.00,
        'timestamp': datetime.now().isoformat(),
        'reason': reason,
        'partitions_remaining': enabled_count,
        'account_balance_after': real_account['total_balance']
    }
    
    # Add to partition history
    if 'liquidation_events' not in partition:
        partition['liquidation_events'] = []
    partition['liquidation_events'].append(liquidation_record)
    
    logger.critical(f"🚨 LIQUIDATION RECORD CREATED")
    logger.critical(f"   Cash Lost: ${float(loss_usd):,.2f}")
    logger.critical(f"   Account Balance After: ${real_account['total_balance']:,.2f}")
    logger.critical(f"   {enabled_count} partition(s) still trading")
    
    return liquidation_record


async def handle_partition_emergency_exit(
    partition_id: str,
    account_id: str,
    symbol: str,
    exit_price: float,
    reason: str = "emergency_exit"
) -> Tuple[float, Dict]:
    """
    Handle emergency exit event for a partition
    
    Scenarios:
    - SL/TP placement failed (emergency close)
    - Position verification failed
    - Risk threshold exceeded
    - Manual emergency exit
    
    Actions:
    1. Close position at current price
    2. Calculate P&L (likely small loss)
    3. Update partition balance
    4. Disable partition (safety)
    5. Alert via Telegram
    6. Other partitions continue
    
    Args:
        partition_id: Partition with emergency
        account_id: Account identifier
        symbol: Trading symbol
        exit_price: Emergency exit price
        reason: Reason for emergency exit
        
    Returns:
        Tuple of (pnl, exit_record)
    """
    from services.partition_manager import partition_state, close_partition_position
    
    logger.error(f"🚨 EMERGENCY EXIT: {partition_id}")
    logger.error(f"   Account: {account_id}")
    logger.error(f"   Symbol: {symbol}")
    logger.error(f"   Exit Price: ${exit_price:,.2f}")
    logger.error(f"   Reason: {reason}")
    
    state = partition_state[account_id]
    partition = state['partitions'][partition_id]
    
    # Check if position exists
    if symbol not in partition['open_positions']:
        logger.warning(f"⚠️ No open position found for {symbol}")
        return 0.0, {}
    
    position = partition['open_positions'][symbol]
    
    logger.error(f"   Entry Price: ${position['entry_price']:,.2f}")
    logger.error(f"   Cash Used: ${position['cash_used']:,.2f}")
    logger.error(f"   Leverage: {position['leverage']}x")
    
    # ============================================
    # CLOSE POSITION (Calculate P&L)
    # ============================================
    
    # Use standard close logic
    pnl_net, trade_record = await close_partition_position(
        partition_id=partition_id,
        account_id=account_id,
        symbol=symbol,
        exit_price=exit_price
    )
    
    logger.error(f"   P&L: ${pnl_net:+.2f}")
    
    # ============================================
    # DISABLE PARTITION (Safety Measure)
    # ============================================
    
    # Auto-disable after emergency exit (safety)
    partition['enabled'] = False
    
    logger.error(f"   🛑 PARTITION DISABLED (emergency exit)")
    logger.error(f"   Other partitions continue trading")
    
    # ============================================
    # CREATE EMERGENCY RECORD
    # ============================================
    
    emergency_record = {
        'partition_id': partition_id,
        'symbol': symbol,
        'exit_price': exit_price,
        'entry_price': position['entry_price'],
        'pnl': pnl_net,
        'reason': reason,
        'timestamp': datetime.now().isoformat(),
        'partition_disabled': True,
        'balance_after': partition['virtual_balance']
    }
    
    # Add to partition history
    if 'emergency_exits' not in partition:
        partition['emergency_exits'] = []
    partition['emergency_exits'].append(emergency_record)
    
    # Check other partitions
    enabled_count = sum(
        1 for p in state['partitions'].values() 
        if p['enabled']
    )
    
    logger.error(f"📊 Partition Status After Emergency:")
    logger.error(f"   Disabled: {partition_id}")
    logger.error(f"   Still trading: {enabled_count} partition(s)")
    
    return pnl_net, emergency_record


def check_liquidation_risk(
    partition_id: str,
    account_id: str,
    current_price: float,
    symbol: str
) -> Tuple[bool, Optional[float], Optional[str]]:
    """
    Check if partition is at risk of liquidation
    
    Args:
        partition_id: Partition to check
        account_id: Account identifier
        current_price: Current market price
        symbol: Trading symbol
        
    Returns:
        Tuple of (at_risk, liquidation_price, risk_level)
    """
    from services.partition_manager import partition_state
    
    state = partition_state[account_id]
    partition = state['partitions'][partition_id]
    
    # Check if position exists
    if symbol not in partition['open_positions']:
        return False, None, None
    
    position = partition['open_positions'][symbol]
    entry_price = position['entry_price']
    leverage = position['leverage']
    side = position['side']
    
    # Calculate liquidation price
    # For LONG: liq_price = entry * (1 - 1/leverage) * safety_factor
    # For SHORT: liq_price = entry * (1 + 1/leverage) * safety_factor
    
    safety_margin = 0.95  # 5% safety margin before true liquidation
    
    if side == 'LONG':
        # Liquidation when price drops by (1/leverage * 100)%
        liq_distance_pct = (1 / leverage) * 100 * safety_margin
        liq_price = entry_price * (1 - liq_distance_pct / 100)
    else:  # SHORT
        liq_distance_pct = (1 / leverage) * 100 * safety_margin
        liq_price = entry_price * (1 + liq_distance_pct / 100)
    
    # Calculate current distance to liquidation
    if side == 'LONG':
        distance_pct = ((current_price - liq_price) / liq_price) * 100
    else:  # SHORT
        distance_pct = ((liq_price - current_price) / liq_price) * 100
    
    # Determine risk level
    if distance_pct < 2:
        risk_level = "CRITICAL"
        at_risk = True
    elif distance_pct < 5:
        risk_level = "HIGH"
        at_risk = True
    elif distance_pct < 10:
        risk_level = "MODERATE"
        at_risk = False
    else:
        risk_level = "LOW"
        at_risk = False
    
    if at_risk:
        logger.warning(f"⚠️ Liquidation risk for {partition_id}:")
        logger.warning(f"   Symbol: {symbol}")
        logger.warning(f"   Entry: ${entry_price:,.2f}")
        logger.warning(f"   Current: ${current_price:,.2f}")
        logger.warning(f"   Liquidation: ${liq_price:,.2f}")
        logger.warning(f"   Distance: {distance_pct:.2f}%")
        logger.warning(f"   Risk Level: {risk_level}")
    
    return at_risk, liq_price, risk_level


async def emergency_close_partition(
    partition_id: str,
    account_id: str,
    symbol: str,
    broker,
    reason: str
) -> bool:
    """
    Emergency close partition position
    
    Use cases:
    - SL/TP placement failed
    - Liquidation risk critical
    - Manual intervention needed
    
    Args:
        partition_id: Partition to close
        account_id: Account identifier
        symbol: Trading symbol
        broker: Broker instance
        reason: Reason for emergency close
        
    Returns:
        True if successful, False otherwise
    """
    logger.error(f"🚨 EMERGENCY CLOSE: {partition_id}")
    logger.error(f"   Reason: {reason}")
    
    try:
        # Get current price
        current_price = await broker.get_current_price(symbol)
        
        # Close position on broker
        close_order = await broker.close_position_market(symbol)
        
        logger.error(f"   ✅ Position closed on broker")
        logger.error(f"   Exit Price: ${current_price:,.2f}")
        
        # Handle in partition system
        pnl, record = await handle_partition_emergency_exit(
            partition_id=partition_id,
            account_id=account_id,
            symbol=symbol,
            exit_price=current_price,
            reason=reason
        )
        
        logger.error(f"   P&L: ${pnl:+.2f}")
        logger.error(f"   Partition disabled for safety")
        
        return True
        
    except Exception as e:
        logger.critical(f"❌ EMERGENCY CLOSE FAILED: {str(e)}")
        logger.critical(f"   MANUAL INTERVENTION REQUIRED!")
        return False


def get_partition_risk_summary(account_id: str) -> Dict:
    """
    Get risk summary for all partitions
    
    Returns:
        Dict with risk status for each partition
    """
    from services.partition_manager import partition_state
    
    state = partition_state[account_id]
    
    summary = {
        'account_id': account_id,
        'total_balance': state['real_account']['total_balance'],
        'partitions': [],
        'at_risk_count': 0,
        'disabled_count': 0,
        'enabled_count': 0
    }
    
    for pid, partition in state['partitions'].items():
        partition_summary = {
            'partition_id': pid,
            'enabled': partition['enabled'],
            'virtual_balance': partition['virtual_balance'],
            'allocation_pct': partition['allocation_pct'],
            'leverage': partition['leverage'],
            'total_pnl': partition['total_pnl'],
            'max_drawdown_pct': partition.get('max_drawdown_pct', 0.0),
            'open_positions': len(partition['open_positions']),
            'liquidation_events': len(partition.get('liquidation_events', [])),
            'emergency_exits': len(partition.get('emergency_exits', []))
        }
        
        summary['partitions'].append(partition_summary)
        
        if partition['enabled']:
            summary['enabled_count'] += 1
        else:
            summary['disabled_count'] += 1
    
    return summary

