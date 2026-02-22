"""
Broker ↔ Ledger reconciliation utilities.
Ensures Firestore partition state mirrors the broker account after restarts.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

from config import settings
from services import partition_manager


logger = logging.getLogger(__name__)


class BrokerReconciler:
    def __init__(self, order_executor):
        self.order_executor = order_executor
        self._watchdog_task: Optional[asyncio.Task] = None
        self._telemetry_task: Optional[asyncio.Task] = None

    async def reconcile_all_accounts(self, scope: str = "manual") -> None:
        account_ids = list(self.order_executor.account_configs.keys())
        for account_id in account_ids:
            try:
                await self.reconcile_account(account_id, scope=scope)
            except Exception as exc:
                logger.error(f"Reconciliation failed for {account_id}: {exc}")

    async def reconcile_account(self, account_id: str, scope: str = "manual") -> None:
        account = self.order_executor._get_account_config(account_id)
        if not account:
            return
        if getattr(account, "partition_mode", "disabled") != "enabled":
            return
        # Only reconcile LIVE accounts (DEMO accounts use order_executor's demo balance)
        if getattr(account, "mode", "").upper() != "LIVE":
            logger.debug(f"[Reconcile] Skipping {account_id} - not LIVE mode")
            return

        broker = self.order_executor.account_brokers.get(account_id)
        if not broker:
            logger.warning(f"[Reconcile] Broker not initialized for {account_id}")
            return

        await self.order_executor._ensure_partitions_initialized(account, broker)

        broker_positions = await self._fetch_broker_positions(broker)
        state = partition_manager.partition_state.get(account_id)
        if not state:
            logger.debug(f"[Reconcile] Partition state missing for {account_id}")
            return

        ledger_positions = self._collect_ledger_positions(account_id, state)

        matched_indices = set()
        unmatched_broker = []

        for broker_pos in broker_positions:
            match_idx = self._match_ledger_position(broker_pos, ledger_positions, matched_indices)
            if match_idx is None:
                unmatched_broker.append(broker_pos)

        unmatched_ledger = [
            ledger_positions[idx] for idx in range(len(ledger_positions)) if idx not in matched_indices
        ]

        adjustments = {
            "imported": [],
            "removed": []
        }

        # Remove ledger entries that no longer exist at the broker
        for entry in unmatched_ledger:
            removed = await partition_manager.force_close_partition_position(
                entry['partition_id'],
                account_id,
                entry['symbol']
            )
            if removed:
                adjustments["removed"].append(entry['symbol'])

        # Import broker positions that aren't tracked yet
        for broker_pos in unmatched_broker:
            partition_id = self._select_partition_for_import(account_id)
            if not partition_id:
                logger.error(f"[Reconcile] No available partition to import {broker_pos['symbol']} ({account_id})")
                continue

            cash_used = broker_pos.get('margin_usd')
            if cash_used is None:
                quantity = broker_pos.get('quantity', 0.0)
                entry_price = broker_pos.get('entry_price', 0.0)
                leverage = broker_pos.get('leverage') or 1
                cash_used = (quantity * entry_price) / leverage if leverage else quantity * entry_price

            await partition_manager.import_partition_position(
                partition_id=partition_id,
                account_id=account_id,
                symbol=broker_pos['symbol'],
                side=broker_pos['side'],
                quantity=broker_pos['quantity'],
                entry_price=broker_pos['entry_price'],
                cash_used=cash_used,
                leverage=broker_pos.get('leverage') or 1,
                strategy_id=None,
                strategy_name=None,
                entry_time=broker_pos.get('entry_time'),
                order_id=f"reconcile-{broker_pos['symbol']}"
            )
            adjustments["imported"].append(broker_pos['symbol'])

        await self._sync_real_balances(account_id, broker)
        
        # Update real-time margin telemetry from broker positions
        try:
            await partition_manager.update_runtime_margin_telemetry(account_id, broker)
        except Exception as telemetry_error:
            logger.debug(f"[Reconcile] Telemetry update failed: {telemetry_error}")

        # For isolated partitions, perform full balance sync to ensure partition
        # virtual balances match the real broker wallet (accounts for different
        # leverage rates and P&L accumulation across partitions)
        style = partition_manager.get_partition_style(account_id)
        if style == "isolated":
            try:
                from services.partition_manager import sync_real_account_balance
                logger.info(f"[Reconcile] Syncing isolated partition balances with broker wallet for {account_id}")
                await sync_real_account_balance(account_id, broker)
            except Exception as exc:
                logger.warning(f"[Reconcile] Failed to sync isolated balances: {exc}")

        if adjustments["imported"] or adjustments["removed"]:
            logger.info(
                f"[Reconcile] {account.label} ({scope}) imported={adjustments['imported']} "
                f"removed={adjustments['removed']}"
            )

    async def _fetch_broker_positions(self, broker) -> List[Dict[str, Any]]:
        try:
            positions = await asyncio.to_thread(broker.get_all_positions)
        except Exception as exc:
            logger.error(f"Failed to fetch broker positions: {exc}")
            return []

        normalized: List[Dict[str, Any]] = []
        for entry in positions or []:
            symbol = (entry.get('symbol') or entry.get('product_id') or "").upper()
            if not symbol:
                continue
            quantity = abs(float(entry.get('position_amt') or entry.get('positionAmt') or 0.0))
            if quantity <= 0:
                continue
            side = (entry.get('side') or entry.get('positionSide') or ("LONG" if entry.get('position_amt', 0) >= 0 else "SHORT")).upper()
            entry_price = float(entry.get('entry_price') or entry.get('avg_open_price') or entry.get('average_open_price') or 0.0)
            leverage = int(entry.get('leverage') or 1)
            normalized.append({
                'symbol': symbol,
                'quantity': quantity,
                'side': side,
                'entry_price': entry_price,
                'leverage': leverage,
                'margin_usd': entry.get('margin_usd'),
                'entry_time': entry.get('entry_time')
            })
        return normalized

    def _collect_ledger_positions(self, account_id: str, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        positions: List[Dict[str, Any]] = []
        for partition_id, partition in state.get('partitions', {}).items():
            open_positions = partition.get('open_positions') or {}
            for symbol, data in open_positions.items():
                quantity = abs(float(data.get('quantity', 0.0)))
                if quantity <= 0:
                    continue
                positions.append({
                    'account_id': account_id,
                    'partition_id': partition_id,
                    'symbol': symbol.upper(),
                    'side': (data.get('side') or "").upper(),
                    'quantity': quantity
                })
        return positions

    def _match_ledger_position(
        self,
        broker_position: Dict[str, Any],
        ledger_positions: List[Dict[str, Any]],
        matched_indices: set
    ) -> Optional[int]:
        symbol = broker_position['symbol'].upper()
        side = broker_position['side'].upper()
        for idx, ledger_pos in enumerate(ledger_positions):
            if idx in matched_indices:
                continue
            if ledger_pos['symbol'] == symbol and ledger_pos['side'] == side:
                matched_indices.add(idx)
                return idx
        return None

    def _select_partition_for_import(self, account_id: str) -> Optional[str]:
        state = partition_manager.partition_state.get(account_id)
        if not state:
            return None
        partitions = state.get('partitions', {})
        empty = [
            pid for pid, data in partitions.items()
            if not (data.get('open_positions') or {})
        ]
        if empty:
            return empty[0]
        # fallback: partition with fewest open positions
        best_pid = None
        best_count = None
        for pid, data in partitions.items():
            count = len(data.get('open_positions') or {})
            if best_pid is None or count < best_count:
                best_pid = pid
                best_count = count
        return best_pid

    async def _sync_real_balances(self, account_id: str, broker) -> None:
        """
        Sync real account balances from broker.
        
        CRITICAL: For Coinbase, we only use USD balance (ignore USDC).
        Available cash is calculated as: total_balance - cash_in_positions
        where cash_in_positions comes from partition state (actual position margins).
        
        NOTE: DEMO accounts use order_executor's demo account balance (with P&L),
        not the broker balance. This method only syncs LIVE accounts.
        """
        # Check if this is a DEMO account (should not sync broker balance for DEMO)
        account = self.order_executor._get_account_config(account_id)
        if account and getattr(account, "mode", "").upper() == "DEMO":
            logger.debug(f"[Reconcile] Skipping balance sync for {account_id} - DEMO mode uses order_executor balance")
            return
        
        get_balance = getattr(broker, "get_account_balance", None)
        if not callable(get_balance):
            return
        try:
            balance_info = await asyncio.to_thread(get_balance)
        except Exception as exc:
            logger.debug(f"Broker balance fetch failed for {account_id}: {exc}")
            return

        # Get total balance (USD-only wallet balance)
        total = balance_info.get('total_wallet_balance')
        if total is None:
            total = balance_info.get('total_balance') or balance_info.get('account_balance')

        if total is None:
            return

        # Get cash_in_positions from partition state (source of truth for deployed capital)
        # This is more accurate than using broker's available_balance which may include USDC
        state = partition_manager.partition_state.get(account_id)
        cash_in_positions = 0.0
        if state:
            real_account = state.get('real_account', {})
            cash_in_positions = real_account.get('cash_in_positions', 0.0)
            
            # If not set, calculate from partition deployed_cash
            if cash_in_positions == 0.0:
                partitions = state.get('partitions', {})
                for partition_data in partitions.values():
                    cash_in_positions += partition_data.get('deployed_cash', 0.0) or 0.0

        # Calculate available cash as: total - cash_in_positions
        # This ensures we only use USD and ignore USDC
        available = total - cash_in_positions

        await partition_manager.update_real_account_balances(
            account_id,
            total_balance=total,
            available_cash=available,
            cash_in_positions=cash_in_positions
        )

    async def start_watchdog(self) -> None:
        interval = getattr(settings, "BROKER_RECONCILE_INTERVAL_SECONDS", 0)
        if interval <= 0:
            logger.info("Broker reconciliation watchdog disabled (interval <= 0)")
            return
        if self._watchdog_task:
            return

        async def _loop():
            while True:
                try:
                    await self.reconcile_all_accounts(scope="watchdog")
                except Exception as exc:
                    logger.error(f"Watchdog reconciliation error: {exc}")
                await asyncio.sleep(interval)

        self._watchdog_task = asyncio.create_task(_loop())

    async def stop_watchdog(self) -> None:
        if self._watchdog_task:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            self._watchdog_task = None
    
    async def start_telemetry_watchdog(self) -> None:
        """
        Start periodic margin telemetry updates for all LIVE accounts.
        Updates every 15 minutes to track margin across intraday, overnight, and weekend.
        Runs immediately on startup, then every 15 minutes.
        """
        interval = 15 * 60  # 15 minutes
        if self._telemetry_task:
            return
        
        async def _update_all_accounts():
            """Update telemetry for all LIVE accounts"""
            for account_id in self.order_executor.account_configs.keys():
                account = self.order_executor._get_account_config(account_id)
                if not account or getattr(account, "mode", "").upper() != "LIVE":
                    continue
                broker = self.order_executor.account_brokers.get(account_id)
                if broker:
                    try:
                        await partition_manager.update_runtime_margin_telemetry(account_id, broker)
                    except Exception as exc:
                        logger.debug(f"Telemetry update failed for {account_id}: {exc}")
        
        async def _loop():
            # Run immediately on startup
            await _update_all_accounts()
            # Then run every 15 minutes
            while True:
                try:
                    await asyncio.sleep(interval)
                    await _update_all_accounts()
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.error(f"Telemetry watchdog error: {exc}")
                    await asyncio.sleep(60)  # Wait 1 minute before retrying
        
        self._telemetry_task = asyncio.create_task(_loop())
        logger.info("✅ Margin telemetry watchdog started (15 min interval, runs immediately)")
    
    async def stop_telemetry_watchdog(self) -> None:
        if self._telemetry_task:
            self._telemetry_task.cancel()
            try:
                await self._telemetry_task
            except asyncio.CancelledError:
                pass
            self._telemetry_task = None
        logger.info("🛑 Margin telemetry watchdog stopped")

