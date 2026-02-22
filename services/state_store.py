"""
State persistence layer for TradingView Agent.

Persists partition state and DEMO account data to Firestore so that
open positions survive container restarts and redeployments.
"""

import asyncio
import logging
from decimal import Decimal
from datetime import datetime, date
from typing import Any, Dict, Optional

from config import settings

try:
    from google.cloud import firestore
except Exception:  # pragma: no cover - handled gracefully at runtime
    firestore = None  # type: ignore


logger = logging.getLogger(__name__)


class StateStore:
    """
    Firestore-backed persistence helper.

    Provides both synchronous and asynchronous helpers so that the existing
    codebase (a mix of sync + async contexts) can persist state without major
    refactors.
    """

    def __init__(self) -> None:
        self.enabled = bool(settings.USE_FIRESTORE and firestore is not None)
        self._client = None
        self.partition_collection = None
        self.demo_collection = None

        if not self.enabled:
            logger.debug("StateStore disabled – Firestore integration unavailable.")
            return

        try:
            self._client = firestore.Client()
            self.partition_collection = self._client.collection("partition_state")
            self.demo_collection = self._client.collection("demo_accounts")
            logger.info("StateStore: Firestore client initialised.")
        except Exception as exc:  # pragma: no cover - runtime dependency
            logger.error(f"StateStore: Failed to initialise Firestore ({exc}).")
            self.enabled = False

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _to_serializable(self, value: Any) -> Any:
        """Recursively convert values to Firestore-compatible types."""
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, dict):
            return {k: self._to_serializable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._to_serializable(v) for v in value]
        return value

    # ------------------------------------------------------------------ #
    # Partition State (Sync)
    # ------------------------------------------------------------------ #

    def save_partition_state_sync(self, account_id: str, state: Dict) -> None:
        if not self.enabled or not self.partition_collection:
            return
        payload = self._to_serializable(state)
        self.partition_collection.document(account_id).set(payload)

    def load_partition_state_sync(self, account_id: str) -> Optional[Dict]:
        if not self.enabled or not self.partition_collection:
            return None
        doc = self.partition_collection.document(account_id).get()
        if doc.exists:
            return doc.to_dict()
        return None

    def clear_partition_state_sync(self, account_id: str) -> None:
        if not self.enabled or not self.partition_collection:
            return
        self.partition_collection.document(account_id).delete()

    # ------------------------------------------------------------------ #
    # Partition State (Async)
    # ------------------------------------------------------------------ #

    async def save_partition_state(self, account_id: str, state: Dict) -> None:
        if not self.enabled:
            return
        await asyncio.to_thread(self.save_partition_state_sync, account_id, state)

    async def load_partition_state(self, account_id: str) -> Optional[Dict]:
        if not self.enabled:
            return None
        return await asyncio.to_thread(self.load_partition_state_sync, account_id)

    async def clear_partition_state(self, account_id: str) -> None:
        if not self.enabled:
            return
        await asyncio.to_thread(self.clear_partition_state_sync, account_id)

    # ------------------------------------------------------------------ #
    # DEMO Account State (Sync)
    # ------------------------------------------------------------------ #

    def save_demo_account_sync(
        self,
        account_id: str,
        account_data: Dict,
        positions: Dict,
        trade_history: Any,
    ) -> None:
        if not self.enabled or not self.demo_collection:
            return
        payload = {
            "account": self._to_serializable(account_data),
            "positions": self._to_serializable(positions),
            "history": self._to_serializable(trade_history),
            "updated_at": datetime.utcnow().isoformat(),
        }
        self.demo_collection.document(account_id).set(payload)

    def load_demo_account_sync(self, account_id: str) -> Optional[Dict]:
        if not self.enabled or not self.demo_collection:
            return None
        doc = self.demo_collection.document(account_id).get()
        if doc.exists:
            return doc.to_dict()
        return None

    def clear_demo_account_sync(self, account_id: str) -> None:
        if not self.enabled or not self.demo_collection:
            return
        self.demo_collection.document(account_id).delete()

    # ------------------------------------------------------------------ #
    # DEMO Account State (Async)
    # ------------------------------------------------------------------ #

    async def save_demo_account(
        self,
        account_id: str,
        account_data: Dict,
        positions: Dict,
        trade_history: Any,
    ) -> None:
        if not self.enabled:
            return
        await asyncio.to_thread(
            self.save_demo_account_sync,
            account_id,
            account_data,
            positions,
            trade_history,
        )

    async def load_demo_account(self, account_id: str) -> Optional[Dict]:
        if not self.enabled:
            return None
        return await asyncio.to_thread(self.load_demo_account_sync, account_id)

    async def clear_demo_account(self, account_id: str) -> None:
        if not self.enabled:
            return
        await asyncio.to_thread(self.clear_demo_account_sync, account_id)


# Singleton instance imported by other modules
state_store = StateStore()


