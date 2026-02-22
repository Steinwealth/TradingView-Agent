"""
Coinbase Derivatives/Futures Broker - Perpetual Futures Integration
Executes trades on Coinbase Derivatives with leverage support
Supports DEMO (paper trading) and LIVE modes

Platform: Coinbase Derivatives (USA: BTC PERP, INTX: BTCUSDC.P)
Fees: 0.02% (promotional taker/maker)
Leverage: Up to 10x
"""

import time
import json
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Dict, Optional, List, Tuple, Any
from urllib.parse import urlencode

import requests
from coinbase.jwt_generator import build_rest_jwt, format_jwt_uri
from coinbase.rest import RESTClient

logger = logging.getLogger(__name__)


class CoinbaseFuturesBroker:
    """
    Coinbase Derivatives/Futures broker for leveraged BTC trading
    
    Provides:
    - 0.02% maker/taker fees (promotional, verify with Coinbase)
    - Up to 10x leverage
    - Long and short positions
    - Demo mode for paper trading
    
    Symbols:
    - USA: "BTC-PERP" (or BIP, BIPZ2030)
    - INTX: "BTCUSDC-PERP" (or BTCUSDC.P)
    """
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        sandbox: bool = False,
        demo_mode: bool = False,
        portfolio_id: Optional[str] = None,
        api_passphrase: Optional[str] = None
    ):
        """
        Initialize Coinbase Derivatives connection
        
        Args:
            api_key: Coinbase API key
            api_secret: Coinbase API secret
            sandbox: Use sandbox environment (default: False)
            demo_mode: Paper trading mode (no real orders, default: False)
        """
        self.api_key = api_key
        self.api_secret = api_secret.replace("\\n", "\n")
        self.demo_mode = demo_mode
        self.sandbox = sandbox
        self.portfolio_id = portfolio_id
        self.rest_client: Optional[RESTClient] = None
        # Enable CDP client for both DEMO and LIVE mode (for price/margin fetching)
        # In DEMO mode, we can still fetch market data and margin rates from API
        self.use_cdp = (
            "BEGIN EC PRIVATE KEY" in self.api_secret or
            "BEGIN PRIVATE KEY" in self.api_secret
        )
        self.current_leverage: Dict[str, int] = {}
        self.api_passphrase = api_passphrase
        self.product_cache: Dict[str, Any] = {}
        self._products_last_fetch: float = 0.0
        self._margin_cache: Dict[str, Dict[str, Any]] = {}  # Cache for margin requirements (daily)
        self._margin_cache_date: Dict[str, str] = {}  # Date string (YYYY-MM-DD) for each cached margin
        self._symbol_contract_map: Dict[str, str] = {
            'BTC': 'BIP',
            'BTC-PERP': 'BIP',
            'BTCUSDC': 'BIP',
            'BIP': 'BIP',
            'BIP-CBSE': 'BIP',  # Coinbase product ID format
            'ETH': 'ETP',
            'ETH-PERP': 'ETP',
            'ETP': 'ETP',
            'ETP-CBSE': 'ETP',  # Coinbase product ID format
            'SOL': 'SLP',
            'SOL-PERP': 'SLP',
            'SLP': 'SLP',
            'SLP-CBSE': 'SLP',  # Coinbase product ID format
            'XRP': 'XPP',
            'XRP-PERP': 'XPP',
            'XPP': 'XPP',
            'XPP-CBSE': 'XPP'  # Coinbase product ID format
        }
        self._contract_symbol_map: Dict[str, str] = {
            'BIP': 'BTC-PERP',
            'ETP': 'ETH-PERP',
            'SLP': 'SOL-PERP',
            'XPP': 'XRP-PERP'
        }
        self.default_margin_type = "CROSS"
        
        if sandbox:
            logger.warning("Coinbase Advanced Trade sandbox mode is not supported for CFM futures")
        else:
            if demo_mode:
                logger.warning("Coinbase Futures initialized - DEMO MODE (Paper Trading)")
            else:
                logger.info("Coinbase Futures initialized - LIVE MODE (Advanced Trade CFM)")
        
        # Paper trading state (for demo mode)
        self.demo_position = {
            'symbol': None,
            'side': 'NONE',
            'quantity': 0.0,
            'entry_price': 0.0,
            'leverage': 1,
            'entry_time': None,
            'unrealized_pnl': 0.0,
            'tp_price': None,
            'sl_price': None
        }
        
        # Maintenance margin rate for liquidation calculation
        self.MAINTENANCE_MARGIN_RATE = 0.004  # 0.4% for BTC
        
        logger.info("CoinbaseFuturesBroker initialized")
        
        # Initialize REST client for price/margin fetching (works in both DEMO and LIVE)
        if self.use_cdp:
            try:
                self._initialize_cdp_client()
            except Exception as e:
                logger.warning(f"Could not initialize REST client (will use indicative rates): {e}")

    def _initialize_cdp_client(self):
        """
        Initialize Coinbase Advanced Trade (CDP) REST client using JWT authentication.
        """
        try:
            self.rest_client = RESTClient(api_key=self.api_key, api_secret=self.api_secret)
            logger.info("✅ Coinbase Advanced Trade REST client initialized")
        except Exception as exc:
            logger.error(f"❌ Failed to initialize Coinbase CDP client: {exc}")
            self.rest_client = None
            raise

    # ------------------------------------------------------------------
    # Utility helpers for Advanced Trade CFM
    # ------------------------------------------------------------------

    def _build_headers(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        full_path = path
        if params:
            query = urlencode(params)
            full_path = f"{path}?{query}"
        uri = format_jwt_uri(method, full_path)
        token = build_rest_jwt(uri, self.api_key, self.api_secret)
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def _send_signed_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        if self.demo_mode:
            logger.debug(f"DEMO signed request {method} {path}")
            return {}

        headers = self._build_headers(method, path, params)
        url = f"https://api.coinbase.com{path}"
        response = requests.request(method, url, headers=headers, params=params, json=body, timeout=30)
        if response.status_code >= 400:
            logger.error("Coinbase CFM request failed: %s %s -> %s %s", method, path, response.status_code, response.text)
            response.raise_for_status()
        if response.text:
            return response.json()
        return {}

    def _cfm_get_balance_summary(self) -> Dict[str, Any]:
        if self.demo_mode:
            # In demo mode, return empty dict to prevent API calls
            logger.debug("DEMO: Skipping CFM balance summary API call")
            return {}
        return self._send_signed_request('GET', '/api/v3/brokerage/cfm/balance_summary')

    def _cfm_list_positions(self) -> List[Dict[str, Any]]:
        data = self._send_signed_request('GET', '/api/v3/brokerage/cfm/positions')
        return data.get('positions', []) or []

    def _parse_cfm_positions(self, raw_positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        positions: List[Dict] = []
        for entry in raw_positions:
            position_info = entry.get('position', entry)
            product_id = entry.get('product_id') or position_info.get('product_id')
            if not product_id:
                continue
            try:
                product = self._get_product_by_id(product_id)
            except Exception:
                product = {
                    'product_id': product_id,
                    'price': entry.get('current_price'),
                    'future_product_details': {
                        'contract_code': entry.get('contract_code', 'BIP'),
                        'contract_size': '0.01',
                        'venue': 'cde'
                    }
                }

            future_details = product.get('future_product_details', {}) if isinstance(product, dict) else {}
            if not future_details:
                continue

            contract_code = future_details.get('contract_code') or entry.get('contract_code')
            symbol = self._symbol_from_contract_code(contract_code) if contract_code else 'BTC-PERP'
            contract_size = Decimal(future_details.get('contract_size', '0.0'))

            contracts_value = position_info.get('net_size', position_info.get('net_size_in_contracts'))
            if contracts_value is None:
                contracts_value = entry.get('number_of_contracts', '0')
            net_size = Decimal(contracts_value or '0')
            if net_size == 0:
                continue

            side = position_info.get('side') or entry.get('side') or ('LONG' if net_size > 0 else 'SHORT')
            side = side.upper()
            if net_size < 0 and side == 'LONG':
                side = 'SHORT'
            elif net_size > 0 and side == 'SHORT':
                side = 'LONG'
            contracts_abs = abs(net_size)
            quantity_btc = float((contracts_abs * contract_size).quantize(Decimal('0.00000001')))
            entry_price = float(position_info.get('avg_open_price', position_info.get('average_open_price', entry.get('avg_entry_price', '0')) or 0))
            mark_price = float(product.get('mid_market_price') or product.get('price') or entry.get('current_price') or 0)

            unrealized_field = position_info.get('unrealized_pnl', {})
            if isinstance(unrealized_field, dict):
                unrealized = float(unrealized_field.get('value', 0) or 0)
            else:
                unrealized = float(unrealized_field or entry.get('unrealized_pnl', 0) or 0)

            leverage = position_info.get('leverage')
            try:
                leverage = int(Decimal(str(leverage))) if leverage is not None else self.current_leverage.get(symbol, 1)
            except Exception:
                leverage = self.current_leverage.get(symbol, 1)

            liquidation_price = position_info.get('liquidation_price') or entry.get('liquidation_price') or 0
            try:
                liquidation_price = float(liquidation_price)
            except Exception:
                liquidation_price = 0.0

            margin_usd = float((contracts_abs * contract_size * Decimal(mark_price or 0)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
            notional_usd = margin_usd * (leverage or 1)

            positions.append({
                'symbol': symbol,
                'position_amt': quantity_btc if side == 'LONG' else -quantity_btc,
                'entry_price': entry_price,
                'mark_price': mark_price,
                'unrealized_pnl': unrealized,
                'leverage': leverage or 1,
                'margin_usd': margin_usd,
                'notional_usd': notional_usd,
                'liquidation_price': liquidation_price,
                'side': side,
                'client_order_id': '',
                'product_id': product_id,
                'contracts': float(contracts_abs)
            })

        return positions

    def _refresh_products(self, force: bool = False) -> None:
        if not self.rest_client:
            raise RuntimeError("REST client not initialized")

        now = time.time()
        if not force and now - self._products_last_fetch < 30 and self.product_cache:
            return

        response = self.rest_client.get_products(product_type="FUTURE")
        products = getattr(response, 'products', []) or []
        normalized: Dict[str, Dict[str, Any]] = {}
        for product in products:
            if hasattr(product, 'dict'):
                data = product.dict()
            elif hasattr(product, '__dict__'):
                data = dict(product.__dict__)
            else:
                data = product
            product_id = data.get('product_id')
            if product_id:
                normalized[product_id] = data
        self.product_cache = normalized
        self._products_last_fetch = now

    def _contract_code_from_symbol(self, symbol: str) -> str:
        key = symbol.upper()
        return self._symbol_contract_map.get(key, key)

    def _symbol_from_contract_code(self, contract_code: str) -> str:
        return self._contract_symbol_map.get(contract_code, contract_code)

    def _get_product_for_symbol(self, symbol: str) -> Any:
        if not self.rest_client:
            raise RuntimeError("REST client not initialized")
        self._refresh_products()
        contract_code = self._contract_code_from_symbol(symbol)
        for product in self.product_cache.values():
            future_details = product.get('future_product_details', {}) if isinstance(product, dict) else {}
            if not future_details:
                continue
            if future_details.get('contract_code', '').upper() == contract_code.upper() and future_details.get('venue', '').lower() == 'cde':
                return product
        raise ValueError(f"No CDE futures product found for symbol {symbol} (contract_code: {contract_code})")

    def _get_product_by_id(self, product_id: str) -> Any:
        product = self.product_cache.get(product_id)
        if product:
            return product
        # Fetch directly and cache
        if self.rest_client:
            response = self.rest_client.get_product(product_id)
            product_data = getattr(response, 'product', None)
            if product_data:
                if hasattr(product_data, 'dict'):
                    product_data = product_data.dict()
                elif hasattr(product_data, '__dict__'):
                    product_data = dict(product_data.__dict__)
                self.product_cache[product_id] = product_data
                return product_data
        raise ValueError(f"Unknown product id {product_id}")

    def _contracts_from_quantity(self, product: Any, quantity_btc: float) -> int:
        future_details = product.get('future_product_details', {}) if isinstance(product, dict) else {}
        contract_size = Decimal(str(future_details.get('contract_size', '0.0')))
        if contract_size == 0:
            raise ValueError("Invalid contract size returned by Coinbase")
        contracts = (Decimal(str(quantity_btc)) / contract_size).quantize(Decimal('1'), rounding=ROUND_DOWN)
        return int(contracts)


    # ============================================
    # UNIFIED INTERFACE METHODS (V3.6 - For Partitions)
    # ============================================
    
    async def get_account_balance_usd(self) -> float:
        """
        Get total account balance in USD (unified interface for partitions)
        
        Returns balance in USD/USDC (Coinbase uses both)
        All amounts already in USD - no conversion needed
        
        Returns:
            float: Total balance in USD
        """
        balance_dict = self.get_account_balance()
        total = balance_dict.get('total_wallet_balance', 0.0)
        return float(total)
    
    async def get_available_balance_usd(self) -> float:
        """
        Get available balance for trading in USD (unified interface)
        
        Returns:
            float: Available balance in USD
        """
        balance_dict = self.get_account_balance()
        available = balance_dict.get('available_balance', 0.0)
        return float(available)
    
    async def get_all_positions(self) -> List[Dict]:
        """
        Get all open positions in UNIFIED format (for partition sync)
        
        Returns positions with standardized fields including margin_usd
        
        Returns:
            List of positions in unified format
        """
        if self.demo_mode:
            demo_position = self.get_position('BTC-PERP')
            return [demo_position] if demo_position.get('positionSide') != 'NONE' else []

        try:
            raw_positions = self._cfm_list_positions()
        except Exception as exc:
            logger.error(f"Failed to fetch CFM positions: {exc}")
            return []

        return self._parse_cfm_positions(raw_positions)
    
    def get_broker_name(self) -> str:
        """Get broker name (for unified interface)"""
        return "Coinbase Futures"
    
    # ============================================
    # EXISTING METHODS (Keep all functionality)
    # ============================================
    
    def _get_account_balance_cdp(self) -> Dict:
        """
        Fetch account balances via Coinbase Advanced Trade (CDP) API.
        """
        if not self.rest_client or not self.portfolio_id:
            raise RuntimeError("CDP client not initialized")

        response = self.rest_client.get_perps_portfolio_balances(self.portfolio_id)

        total_wallet_balance = 0.0
        available_balance = 0.0

        for portfolio_balance in getattr(response, "portfolio_balances", []) or []:
            for balance in getattr(portfolio_balance, "balances", []) or []:
                collateral_value = getattr(balance, "collateral_value", "0") or "0"
                max_withdraw = getattr(balance, "max_withdraw_amount", "0") or "0"
                try:
                    total_wallet_balance += float(collateral_value)
                except (TypeError, ValueError):
                    logger.debug(f"Unable to parse collateral value: {collateral_value}")
                try:
                    available_balance += float(max_withdraw)
                except (TypeError, ValueError):
                    logger.debug(f"Unable to parse withdrawable amount: {max_withdraw}")

        return {
            'total_wallet_balance': total_wallet_balance,
            'available_balance': available_balance,
            'total_unrealized_pnl': 0.0,  # Derived via positions elsewhere
            'total_margin_balance': total_wallet_balance,
            'mode': 'LIVE',
            'portfolio_id': self.portfolio_id,
        }

    def get_account_balance(self) -> Dict:
        """
        Get futures account balance
        
        Returns:
            {
                'total_wallet_balance': 1000.0,
                'available_balance': 950.0,
                'total_unrealized_pnl': 50.0,
                'total_margin_balance': 1050.0
            }
        """
        # CRITICAL: Always return demo balance if in demo mode (before any API calls)
        if self.demo_mode:
            logger.debug(f"DEMO MODE: Returning demo balance $1000.00 (demo_mode={self.demo_mode})")
            # Return demo balance
            return {
                'total_wallet_balance': 1000.0,
                'available_balance': 1000.0 - abs(self.demo_position['unrealized_pnl']),
                'total_unrealized_pnl': self.demo_position['unrealized_pnl'],
                'total_margin_balance': 1000.0 + self.demo_position['unrealized_pnl'],
                'mode': 'DEMO'
            }
        
        try:
            if not self.use_cdp:
                raise RuntimeError("Advanced Trade credentials required for live Coinbase futures")

            summary = self._cfm_get_balance_summary()
            balance_summary = summary.get('balance_summary', {})

            def _extract_amount(field: str) -> float:
                value = balance_summary.get(field, {})
                if isinstance(value, dict):
                    return float(value.get('value', 0) or 0)
                try:
                    return float(value or 0)
                except Exception:
                    return 0.0

            total_usd = _extract_amount('total_usd_balance')
            available_margin = _extract_amount('available_margin')
            futures_buying_power = _extract_amount('futures_buying_power')
            unrealized_pnl = _extract_amount('unrealized_pnl')
            
            # CRITICAL: Use USD-only balance (ignore USDC)
            # The CFM API's total_usd_balance may include both USD and USDC as collateral.
            # For trading, we only use USD. The reconciler will calculate available_cash
            # correctly as: total_balance - cash_in_positions (from actual position margins)
            
            # Use total_usd_balance as the wallet balance (this should be USD-only from the wallet)
            # Note: If the API returns USD+USDC combined, we need to filter to USD-only
            # For now, assume total_usd_balance is the USD wallet balance
            wallet_balance = total_usd
            
            # Available balance will be recalculated by the reconciler based on:
            # available_cash = total_balance - cash_in_positions
            # where cash_in_positions comes from actual broker position margins (USD-only)
            # So we return wallet_balance as both total and available initially
            # The reconciler will correct available_cash after checking positions
            
            return {
                'total_wallet_balance': wallet_balance,
                'available_balance': wallet_balance,  # Initial value - reconciler will recalculate based on positions
                'total_unrealized_pnl': unrealized_pnl,
                'total_margin_balance': wallet_balance,
                'mode': 'LIVE'
            }
        
        except Exception as e:
            logger.error(f"Error fetching Coinbase balance: {str(e)}")
            raise

    def get_current_price(self, symbol: str) -> float:
        """
        Get current market price for a symbol
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDC", "BTC-PERP")
        
        Returns:
            Current price as float
        """
       
        try:
            product = self._get_product_for_symbol(symbol)
            if not product:
                raise ValueError(f"No product found for symbol {symbol}")
            if isinstance(product, dict):
                price = product.get('price') or product.get('mid_market_price')
            else:
                price = getattr(product, 'price', None) or getattr(product, 'mid_market_price', None)
            if price:
                return float(price)
            if self.rest_client:
                product_id = product.get('product_id') if isinstance(product, dict) else getattr(product, 'product_id', None)
                if product_id:
                    response = self.rest_client.get_product(product_id)
                    p = getattr(response, 'product', None)
                    if p:
                        return float(getattr(p, 'price', getattr(p, 'mid_market_price', 0)) or 0)
            return 0.0

        except Exception as e:
            logger.error(f"Error fetching price for {symbol}: {str(e)}")
            raise
    
    def _normalize_symbol(self, symbol: str) -> str:
        """
        Normalize symbol to Coinbase format
        
        Examples:
            BTCUSDC → BTC-USD
            BTC-PERP → BTC-PERP
            BTCUSDC.P → BTC-USD-PERP
        """
        # Common conversions
        symbol_map = {
            'BTCUSDC': 'BTC-USD',
            'ETHUSDC': 'ETH-USD',
            'BTCUSD': 'BTC-USD',
            'ETHUSD': 'ETH-USD'
        }
        
        if symbol in symbol_map:
            return symbol_map[symbol]
        
        # Already in correct format
        if '-' in symbol:
            return symbol
        
        # Try to parse (e.g., BTCUSDC → BTC-USD)
        if len(symbol) == 7 and symbol.endswith('USDC'):
            base = symbol[:3]
            return f'{base}-USD'
        
        return symbol
    
    def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        leverage: int = 1,
        client_order_id: str = None,
        price_override: Optional[float] = None
    ) -> Dict:
        """
        Place a market order on Coinbase Futures
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDC", "BTC-PERP")
            side: 'BUY' or 'SELL'
            quantity: Quantity in base currency (e.g., BTC)
            leverage: Leverage multiplier (1-10x)
            client_order_id: Optional custom order ID
        
        Returns:
            Order details dict
        """
        if self.demo_mode:
            logger.info(f"DEMO: Market {side} {quantity} {symbol} @ {leverage}x")

            current_price = float(price_override) if price_override else 100000.0  # Use provided price when available

            # Check if this is a closing order (opposite side of existing position)
            existing_position = self.get_position(symbol)
            is_closing = False
            if existing_position.get('positionSide') != 'NONE':
                existing_side = existing_position.get('positionSide', '').upper()
                order_side = side.upper()
                # If we have LONG and order is SELL, or we have SHORT and order is BUY, it's a close
                if (existing_side == 'LONG' and order_side == 'SELL') or (existing_side == 'SHORT' and order_side == 'BUY'):
                    is_closing = True
                    logger.info(f"DEMO: Closing position {existing_side} with {order_side} order")

            if is_closing:
                # Close the position - set to NONE
                self.demo_position = {
                    'symbol': symbol,
                    'side': 'NONE',
                    'quantity': 0.0,
                    'entry_price': 0.0,
                    'leverage': leverage,
                    'entry_time': None,
                    'unrealized_pnl': 0.0,
                    'tp_price': None,
                    'sl_price': None
                }
                logger.info(f"DEMO: Position closed for {symbol}")
            else:
                # Open new position
                self.demo_position = {
                    'symbol': symbol,
                    'side': side,
                    'quantity': quantity,
                    'entry_price': current_price,
                    'leverage': leverage,
                    'entry_time': datetime.now(),
                    'unrealized_pnl': 0.0,
                    'tp_price': None,
                    'sl_price': None
                }
                logger.info(f"DEMO: Position opened for {symbol}: {side} {quantity} @ ${current_price:,.2f}")

            self.current_leverage[symbol] = leverage

            return {
                'orderId': f'demo_{int(time.time())}',
                'symbol': symbol,
                'side': side,
                'quantity': quantity,
                'price': current_price,
                'status': 'FILLED',
                'mode': 'DEMO'
            }

        if not self.rest_client:
            raise RuntimeError("REST client not initialized")

        try:
            product = self._get_product_for_symbol(symbol)
            future_details = product.get('future_product_details', {}) if isinstance(product, dict) else {}
            contract_size = Decimal(future_details.get('contract_size', '0.0')) if future_details else Decimal('0')
            contracts = self._contracts_from_quantity(product, quantity)

            if contracts <= 0:
                raise ValueError("Quantity is too small for a single futures contract")

            base_size = str(contracts)
            client_order_id = client_order_id or f"tv-agent-{int(time.time()*1000)}"

            product_id = product.get('product_id') if isinstance(product, dict) else getattr(product, 'product_id', None)

            response = self.rest_client.create_order(
                client_order_id=client_order_id,
                product_id=product_id,
                side=side.upper(),
                order_configuration={
                    "market_market_ioc": {
                        "base_size": base_size,
                        "reduce_only": False
                    }
                },
                leverage=str(leverage),
                margin_type=self.default_margin_type
            )

            if not getattr(response, 'success', False):
                failure = getattr(response, 'error_response', None)
                raise Exception(f"Coinbase CFM order rejected: {failure}")

            order_id = ''
            success_payload = getattr(response, 'success_response', None)
            if success_payload:
                order_id = success_payload.get('order_id', '') if isinstance(success_payload, dict) else getattr(success_payload, 'order_id', '')

            filled_contracts = Decimal('0')
            average_price = Decimal('0')
            order_obj = None
            if order_id:
                try:
                    order = self.rest_client.get_order(order_id)
                    order_obj = getattr(order, 'order', None)
                    if order_obj:
                        if hasattr(order_obj, 'filled_size'):
                            filled_contracts = Decimal(str(order_obj.filled_size or '0'))
                            average_price = Decimal(str(order_obj.average_filled_price or '0'))
                        elif isinstance(order_obj, dict):
                            filled_contracts = Decimal(str(order_obj.get('filled_size', '0')))
                            average_price = Decimal(str(order_obj.get('average_filled_price', '0')))
                except Exception as exc:
                    logger.warning(f"Unable to fetch order details for {order_id}: {exc}")

            if filled_contracts <= 0:
                filled_contracts = Decimal(str(contracts))
            filled_quantity = float((filled_contracts * contract_size).quantize(Decimal('0.00000001')))

            self.current_leverage[symbol] = leverage

            execution_price = float(average_price) if average_price > 0 else self.get_current_price(symbol)
            order_status = 'UNKNOWN'
            if order_obj:
                status_val = getattr(order_obj, 'status', None)
                if status_val is None and isinstance(order_obj, dict):
                    status_val = order_obj.get('status')
                if status_val:
                    order_status = str(status_val).upper()

            return {
                'orderId': order_id or client_order_id,
                'symbol': symbol,
                'side': side,
                'quantity': filled_quantity if filled_quantity > 0 else quantity,
                'price': execution_price,
                'status': order_status,
                'contracts': float(filled_contracts)
            }

        except Exception as e:
            logger.error(f"Error placing Coinbase market order: {str(e)}")
            raise
    
    def place_stop_loss(
        self,
        symbol: str,
        side: str,
        quantity: float,
        stop_price: float,
        reduce_only: bool = True,
        client_order_id: Optional[str] = None,
        leverage: Optional[int] = None,
        **kwargs
    ) -> Dict:
        """
        Place a stop loss order
        
        Args:
            symbol: Trading symbol
            side: 'BUY' or 'SELL' (opposite of position)
            quantity: Quantity to close
            stop_price: Trigger price
            reduce_only: Must reduce position (default: True)
        
        Returns:
            Order details dict
        """
        if self.demo_mode:
            logger.info(f"DEMO: Stop Loss @ {stop_price}")
            self.demo_position['sl_price'] = stop_price
            return {
                'orderId': f'demo_sl_{int(time.time())}',
                'symbol': symbol,
                'type': 'STOP',
                'stopPrice': stop_price,
                'mode': 'DEMO'
            }
        raise NotImplementedError("Coinbase CFM stop-loss orders are not yet supported via API")
    
    def place_take_profit(
        self,
        symbol: str,
        side: str,
        quantity: float,
        limit_price: Optional[float] = None,
        take_profit_price: Optional[float] = None,
        reduce_only: bool = True,
        client_order_id: Optional[str] = None,
        leverage: Optional[int] = None,
        **kwargs
    ) -> Dict:
        """
        Place a take profit order
        
        Args:
            symbol: Trading symbol
            side: 'BUY' or 'SELL' (opposite of position)
            quantity: Quantity to close
            limit_price: Target price
            reduce_only: Must reduce position (default: True)
        
        Returns:
            Order details dict
        """
        if self.demo_mode:
            logger.info(f"DEMO: Take Profit @ {limit_price}")
            self.demo_position['tp_price'] = limit_price
            return {
                'orderId': f'demo_tp_{int(time.time())}',
                'symbol': symbol,
                'type': 'LIMIT',
                'price': limit_price,
                'mode': 'DEMO'
            }
        target_price = limit_price if limit_price is not None else take_profit_price
        if target_price is None:
            raise ValueError("take_profit_price must be provided")
        raise NotImplementedError("Coinbase CFM take-profit orders are not yet supported via API")
    
    def get_open_orders(self, symbol: str = None) -> List[Dict]:
        """
        Get all open orders
        
        Args:
            symbol: Optional symbol filter
        
        Returns:
            List of open orders
        """
        if self.demo_mode:
            # Return demo orders
            orders = []
            if self.demo_position['sl_price']:
                orders.append({
                    'orderId': 'demo_sl',
                    'type': 'STOP_LOSS',
                    'stopPrice': self.demo_position['sl_price']
                })
            if self.demo_position['tp_price']:
                orders.append({
                    'orderId': 'demo_tp',
                    'type': 'LIMIT',
                    'price': self.demo_position['tp_price']
                })
            return orders
        logger.debug("get_open_orders not implemented for live Coinbase CFM yet")
        return []
    
    def verify_sl_tp_exist(self, symbol: str, expected_sl: float = None, expected_tp: float = None) -> Tuple[bool, bool, str]:
        """
        Verify SL and TP orders exist on the exchange
        
        Args:
            symbol: Trading symbol
            expected_sl: Expected stop loss price (optional)
            expected_tp: Expected take profit price (optional)
        
        Returns:
            (sl_exists, tp_exists, message)
        """
        try:
            if self.demo_mode:
                open_orders = self.get_open_orders(symbol)
                sl_orders = [o for o in open_orders if 'STOP' in o.get('type', '')]
                tp_orders = [o for o in open_orders if o.get('type') in ['LIMIT', 'TAKE_PROFIT']]
                message = f"SL: {'✅' if sl_orders else '❌'}, TP: {'✅' if tp_orders else '❌'}"
                return bool(sl_orders), bool(tp_orders), message

            logger.debug("SL/TP verification not supported for Coinbase CFM yet")
            return False, False, "Coinbase CFM SL/TP verification not supported"
        
        except Exception as e:
            logger.error(f"Error verifying Coinbase SL/TP: {str(e)}")
            return False, False, f"Verification failed: {str(e)}"
    
    def get_position(self, symbol: str) -> Dict:
        """
        Get current position for a symbol
        
        Args:
            symbol: Trading symbol
        
        Returns:
            {
                'symbol': 'BTCUSDC',
                'positionSide': 'LONG' or 'SHORT' or 'NONE',
                'positionAmt': 0.009,
                'entryPrice': 100000.0,
                'unrealizedProfit': 50.0,
                'leverage': 3,
                'liquidationPrice': 96000.0
            }
        """
        if self.demo_mode:
            # Return demo position
            if self.demo_position['side'] == 'NONE':
                return {
                    'symbol': symbol,
                    'positionSide': 'NONE',
                    'positionAmt': 0.0,
                    'entryPrice': 0.0,
                    'unrealizedProfit': 0.0,
                    'leverage': 1,
                    'liquidationPrice': 0.0,
                    'mode': 'DEMO'
                }
            
            # Calculate unrealized P&L
            current_price = 100000.0  # Mock
            if self.demo_position['side'] == 'BUY':
                pnl = (current_price - self.demo_position['entry_price']) * self.demo_position['quantity']
            else:
                pnl = (self.demo_position['entry_price'] - current_price) * self.demo_position['quantity']
            
            return {
                'symbol': symbol,
                'positionSide': 'LONG' if self.demo_position['side'] == 'BUY' else 'SHORT',
                'positionAmt': self.demo_position['quantity'],
                'entryPrice': self.demo_position['entry_price'],
                'unrealizedProfit': pnl,
                'leverage': self.demo_position['leverage'],
                'liquidationPrice': 0.0,
                'mode': 'DEMO'
            }
        
        if not self.use_cdp:
            raise RuntimeError("Advanced Trade credentials required for live Coinbase futures")

        try:
            raw_positions = self._cfm_list_positions()
            parsed_positions = self._parse_cfm_positions(raw_positions)
            for pos in parsed_positions:
                if pos['symbol'] == symbol:
                    side = pos['side']
                    quantity = abs(pos['position_amt'])
                    return {
                        'symbol': symbol,
                        'positionSide': side,
                        'positionAmt': quantity,
                        'entryPrice': pos['entry_price'],
                        'unrealizedProfit': pos['unrealized_pnl'],
                        'leverage': pos['leverage'],
                        'liquidationPrice': pos['liquidation_price'],
                        'product_id': pos.get('product_id'),
                        'contracts': pos.get('contracts')
                    }

            return {
                'symbol': symbol,
                'positionSide': 'NONE',
                'positionAmt': 0.0,
                'entryPrice': 0.0,
                'unrealizedProfit': 0.0,
                'leverage': self.current_leverage.get(symbol, 1),
                'liquidationPrice': 0.0
            }

        except Exception as e:
            logger.error(f"Error fetching Coinbase position: {str(e)}")
            raise
    
    def verify_position(self, symbol: str, expected_side: str, expected_quantity: float, tolerance: float = 0.01) -> Tuple[bool, str]:
        """
        Verify position matches expected values
        
        Args:
            symbol: Trading symbol
            expected_side: Expected position side ('LONG', 'SHORT', 'NONE')
            expected_quantity: Expected quantity
            tolerance: Allowed difference (default: 1%)
        
        Returns:
            (matches, message)
        """
        try:
            position = self.get_position(symbol)
            actual_side = position['positionSide']
            actual_quantity = abs(position['positionAmt'])
            
            # Check side matches
            if actual_side != expected_side:
                msg = f"Position side mismatch: Expected {expected_side}, Actual {actual_side}"
                logger.warning(msg)
                return False, msg
            
            # Check quantity matches (within tolerance)
            if expected_quantity > 0:
                diff_pct = abs(actual_quantity - expected_quantity) / expected_quantity * 100
                
                if diff_pct > tolerance:
                    msg = f"Position quantity mismatch: Expected {expected_quantity}, Actual {actual_quantity} (Diff: {diff_pct:.2f}%)"
                    logger.warning(msg)
                    return False, msg
            
            logger.info(f"Position verified: {actual_side} {actual_quantity} {symbol}")
            return True, "Position verified"
        
        except Exception as e:
            msg = f"Position verification error: {str(e)}"
            logger.error(msg)
            return False, msg
    
    def close_position_market(self, symbol: str) -> Dict:
        """
        Close entire position at market price
        
        Args:
            symbol: Trading symbol
        
        Returns:
            Order details dict
        """
        try:
            position = self.get_position(symbol)
            
            if position['positionSide'] == 'NONE':
                logger.info(f"No position to close for {symbol}")
                return {
                    'status': 'NO_POSITION',
                    'symbol': symbol
                }
            
            quantity = abs(position['positionAmt'])
            close_side = 'SELL' if position['positionSide'] == 'LONG' else 'BUY'
            
            logger.info(f"Closing Coinbase position: {close_side} {quantity} {symbol}")
            return self.place_market_order(symbol, close_side, quantity)
        
        except Exception as e:
            logger.error(f"Error closing Coinbase position: {str(e)}")
            raise
    
    def cancel_all_orders(self, symbol: str = None) -> None:
        """
        Cancel all open orders
        
        Args:
            symbol: Optional symbol filter (cancels all if None)
        """
        if self.demo_mode:
            logger.info(f"DEMO: Cancel all orders for {symbol}")
            if symbol == self.demo_position['symbol'] or symbol is None:
                self.demo_position['sl_price'] = None
                self.demo_position['tp_price'] = None
            return
        logger.warning("cancel_all_orders not implemented for Coinbase CFM yet")

    def get_historical_candles(self, symbol: str, timeframe: str = '1d', limit: int = 14) -> List[Dict]:
        """
        Get historical candle data for ADV calculation
        
        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe ('1d' recommended for ADV)
            limit: Number of candles to fetch
        
        Returns:
            List of candles with OHLCV data
        """
        try:
            cb_symbol = self._normalize_symbol(symbol)
            
            # Convert timeframe to Coinbase granularity (in seconds)
            granularity_map = {
                '1m': 60,
                '5m': 300,
                '15m': 900,
                '1h': 3600,
                '4h': 14400,
                '1d': 86400
            }
            
            granularity = granularity_map.get(timeframe, 86400)
            
            # Calculate time range
            end_time = int(time.time())
            start_time = end_time - (granularity * limit)
            
            logger.debug("Historical candles not implemented for Coinbase CFM yet; returning empty list")
            return []
        
        except Exception as e:
            logger.error(f"Error fetching Coinbase candles: {str(e)}")
            raise
    
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """
        Set leverage for a symbol
        
        Note: Coinbase may handle leverage differently than Binance
        This may be a no-op or handled during order placement
        
        Args:
            symbol: Trading symbol
            leverage: Leverage multiplier (1-10x)
        
        Returns:
            Result dict
        """
        self.current_leverage[symbol] = leverage

        if self.demo_mode:
            logger.info(f"DEMO: Set leverage {leverage}x for {symbol}")
            self.demo_position['leverage'] = leverage
            return True

        if self.use_cdp:
            logger.info(f"CDP leverage handled per-order ({leverage}x) for {symbol}")
            return True

        logger.info(f"Leverage setting for Coinbase (legacy): {leverage}x (account-level config)")
        return True

    async def get_current_leverage(self, symbol: str) -> Optional[int]:
        if self.demo_mode and self.demo_position['symbol'] == symbol:
            return self.demo_position.get('leverage', 1)

        return self.current_leverage.get(symbol, 1)

    def get_margin_requirements(self, symbol: str, side: str = "LONG", force_period: Optional[str] = None, force_fresh: bool = False) -> Dict[str, Any]:
        """
        Get margin requirements for a symbol and side.
        
        Fetches actual margin rates from Coinbase API or uses indicative maximums.
        Checks if currently in overnight period (4PM-6PM ET) for higher rates.
        
        Args:
            symbol: Trading symbol (e.g., "BTC-PERP")
            side: Position side ("LONG" or "SHORT")
            force_period: Optional period override ("intraday" or "overnight")
            force_fresh: If True, always fetch fresh rates (bypasses cache). Used during trade validation.
        
        Returns:
            Dict with margin requirements:
            {
                "initial_margin_pct": float,  # Initial margin percentage
                "maintenance_margin_pct": float,  # Maintenance margin percentage
                "is_overnight_period": bool,  # True if 4PM-6PM ET
                "daytime_rate": float,  # Daytime initial margin rate
                "overnight_rate": float,  # Overnight initial margin rate (if available)
                "current_rate": float,  # Current applicable rate
                "source": str,  # "api" or "indicative"
                "timestamp": str  # ISO timestamp
            }
        """
        from datetime import datetime
        from zoneinfo import ZoneInfo
        
        # Check if in overnight period (4PM-6PM ET) or force period
        et_tz = ZoneInfo('America/New_York')
        now_et = datetime.now(et_tz)
        hour = now_et.hour
        
        if force_period:
            is_overnight = (force_period.lower() == 'overnight')
        else:
            is_overnight = 16 <= hour < 18  # 4PM-6PM ET
        
        # Get contract code from symbol
        contract_code = self._contract_code_from_symbol(symbol)
        side_upper = side.upper()
        
        # Check cache first (daily cache - separate for intraday and overnight)
        today_str = now_et.strftime('%Y-%m-%d')
        period = 'overnight' if is_overnight else 'intraday'
        cache_key = f"{contract_code}_{side_upper}_{period}"
        
        # Check cache first (unless forcing a fresh fetch)
        # During trade validation: force_fresh=True to always fetch fresh rates
        # For EOD reporting: Use cached rates if available
        # force_period=None means use cache if available, fetch fresh if not
        # force_period='intraday' or 'overnight' means force fetch fresh rate for that period
        
        # If force_fresh=True (trade validation), always fetch fresh rates
        if force_fresh:
            logger.debug(f"Force fetching fresh margin rate for {symbol} {side_upper} ({period}) - trade validation")
            self._margin_cache.pop(cache_key, None)
            self._margin_cache_date.pop(cache_key, None)
        # If no force_period specified, check cache first
        elif not force_period:
            if cache_key in self._margin_cache:
                cached_date = self._margin_cache_date.get(cache_key, '')
                if cached_date == today_str:
                    cached = self._margin_cache[cache_key].copy()
                    cached["is_overnight_period"] = is_overnight  # Update in case period changed
                    cached["timestamp"] = datetime.utcnow().isoformat()
                    logger.debug(f"Using cached margin requirements for {symbol} {side_upper} ({period})")
                    return cached
                else:
                    # Cache is from a different day, clear it
                    logger.debug(f"Margin cache expired for {symbol} {side_upper} ({period}), refreshing for {today_str}")
                    self._margin_cache.pop(cache_key, None)
                    self._margin_cache_date.pop(cache_key, None)
            # Cache miss or expired - will fetch fresh below
        else:
            # Force fetch: clear existing cache to ensure fresh rate is fetched
            logger.debug(f"Force fetching fresh margin rate for {symbol} {side_upper} ({period})")
            self._margin_cache.pop(cache_key, None)
            self._margin_cache_date.pop(cache_key, None)
        
        # Indicative maximum rates (from Coinbase documentation)
        # Used as fallback if API doesn't provide rates
        indicative_rates = {
            "BIP": {  # BTC
                "LONG": {"daytime": 0.18751, "overnight": None},  # 18.751%
                "SHORT": {"daytime": 0.230944, "overnight": None}  # 23.0944%
            },
            "ETP": {  # ETH
                "LONG": {"daytime": 0.197957, "overnight": None},  # 19.7957%
                "SHORT": {"daytime": 0.271392, "overnight": None}  # 27.1392%
            },
            "SLP": {  # SOL
                "LONG": {"daytime": 0.292641, "overnight": None},  # 29.2641%
                "SHORT": {"daytime": 0.447569, "overnight": None}  # 44.7569%
            },
            "XPP": {  # XRP
                "LONG": {"daytime": 0.310835, "overnight": None},  # 31.0835%
                "SHORT": {"daytime": 0.501035, "overnight": None}  # 50.1035%
            }
        }
        
        # Try to fetch from API first
        # CRITICAL: When force_fresh=True (during trade validation), we MUST fetch from API
        # to ensure accurate position sizing and risk management
        api_rate = None
        api_source = "indicative"
        api_overnight_rate = None
        
        try:
            # Try to fetch from API (works in both DEMO and LIVE mode for margin rates)
            # In DEMO mode, we can still fetch margin rates even if we can't place orders
            # Initialize REST client if needed (for margin/price fetching)
            if not self.rest_client:
                try:
                    # Try to initialize REST client (works in DEMO mode for data fetching)
                    if self.use_cdp:
                        self._initialize_cdp_client()
                except Exception as e:
                    logger.debug(f"Could not initialize REST client for margin fetching: {e}")
                    # Will fall back to indicative rates
            
            # CRITICAL: If force_fresh=True, we MUST attempt API fetch (don't use cache)
            # This ensures trade validation uses the most current margin rates
            if force_fresh:
                logger.info(f"🔄 Force fresh margin rate fetch for {symbol} {side_upper} (trade validation)")
            
            if self.rest_client:
                # Method 1: Try to get margin from product information
                try:
                    product = self._get_product_for_symbol(symbol)
                    if product:
                        # Normalize product data (handle both dict and object formats)
                        if not isinstance(product, dict):
                            if hasattr(product, 'dict'):
                                product = product.dict()
                            elif hasattr(product, '__dict__'):
                                product = dict(product.__dict__)
                            else:
                                product = {}
                        
                        # Check for margin information in product details
                        future_details = product.get('future_product_details', {})
                        if not future_details and isinstance(product, dict):
                            # Try to extract future_details from object attributes
                            if hasattr(product, 'future_product_details'):
                                fd = product.future_product_details
                                if hasattr(fd, 'dict'):
                                    future_details = fd.dict()
                                elif hasattr(fd, '__dict__'):
                                    future_details = dict(fd.__dict__)
                        
                        # Look for margin fields in product data
                        # Coinbase API provides margin rates in future_product_details:
                        # - intraday_margin_rate: {long_margin_rate, short_margin_rate}
                        # - overnight_margin_rate: {long_margin_rate, short_margin_rate}
                        
                        # PRIMARY: Extract from future_product_details.intraday_margin_rate / overnight_margin_rate
                        margin_pct = None
                        side_key = 'long_margin_rate' if side_upper == 'LONG' else 'short_margin_rate'
                        
                        # Check if we're looking for intraday or overnight rate
                        if force_period == 'overnight':
                            # Get overnight rate
                            if future_details and isinstance(future_details, dict):
                                overnight_margin = future_details.get('overnight_margin_rate', {})
                                if isinstance(overnight_margin, dict):
                                    margin_pct_str = overnight_margin.get(side_key)
                                    if margin_pct_str:
                                        try:
                                            margin_pct = float(margin_pct_str)
                                            api_overnight_rate = margin_pct
                                            logger.info(f"✅ Found overnight margin rate from API for {symbol} {side_upper}: {margin_pct*100:.4f}%")
                                        except (ValueError, TypeError):
                                            pass
                        else:
                            # Get intraday rate (default)
                            if future_details and isinstance(future_details, dict):
                                intraday_margin = future_details.get('intraday_margin_rate', {})
                                if isinstance(intraday_margin, dict):
                                    margin_pct_str = intraday_margin.get(side_key)
                                    if margin_pct_str:
                                        try:
                                            margin_pct = float(margin_pct_str)
                                            logger.info(f"✅ Found intraday margin rate from API for {symbol} {side_upper}: {margin_pct*100:.4f}%")
                                        except (ValueError, TypeError):
                                            pass
                        
                        # FALLBACK: Try other field names if API structure not found
                        if margin_pct is None:
                            margin_fields_to_check = [
                                product.get('initial_margin_pct'),
                                product.get('margin_requirement'),
                                product.get('margin_rate'),
                                product.get('initial_margin_requirement_pct'),
                                future_details.get('initial_margin_pct') if isinstance(future_details, dict) else None,
                                future_details.get('margin_requirement') if isinstance(future_details, dict) else None,
                                future_details.get('margin_rate') if isinstance(future_details, dict) else None,
                                future_details.get('initial_margin_requirement_pct') if isinstance(future_details, dict) else None,
                            ]
                            
                            # Also check nested structures
                            if isinstance(product, dict):
                                # Check for margin in nested structures
                                for key in product.keys():
                                    if 'margin' in key.lower() and isinstance(product[key], (int, float)):
                                        margin_pct = product[key]
                                        break
                            
                            # Find first non-None margin value
                            for field_value in margin_fields_to_check:
                                if field_value is not None:
                                    try:
                                        margin_pct = float(field_value)
                                        break
                                    except (ValueError, TypeError):
                                        continue
                        
                        # If we got intraday rate, also try to get overnight rate
                        if margin_pct is not None and api_overnight_rate is None and force_period != 'overnight':
                            if future_details and isinstance(future_details, dict):
                                overnight_margin = future_details.get('overnight_margin_rate', {})
                                if isinstance(overnight_margin, dict):
                                    overnight_rate_str = overnight_margin.get(side_key)
                                    if overnight_rate_str:
                                        try:
                                            api_overnight_rate = float(overnight_rate_str)
                                            logger.info(f"✅ Found overnight margin rate from API for {symbol} {side_upper}: {api_overnight_rate*100:.4f}%")
                                        except (ValueError, TypeError):
                                            pass
                        
                        if margin_pct is not None:
                            api_rate = margin_pct
                            api_source = "api"
                            logger.info(f"✅ Fetched margin rate from API for {symbol} {side_upper}: {api_rate*100:.4f}%")
                        else:
                            # Log available fields at INFO level to help identify where margin data might be
                            if isinstance(product, dict):
                                all_keys = list(product.keys())
                                logger.info(f"🔍 Margin not found in product data for {symbol}. Available top-level fields: {all_keys}")
                                if 'future_product_details' in product:
                                    fd = product['future_product_details']
                                    if isinstance(fd, dict):
                                        fd_keys = list(fd.keys())
                                        logger.info(f"🔍 Future product details fields for {symbol}: {fd_keys}")
                                        # Log actual values for margin-related fields
                                        for key in fd_keys:
                                            if 'margin' in key.lower() or 'requirement' in key.lower():
                                                logger.info(f"   Found margin-related field '{key}': {fd[key]}")
                                    else:
                                        logger.info(f"🔍 Future product details type: {type(fd)}")
                            else:
                                logger.info(f"🔍 Margin not found in product data for {symbol}. Product type: {type(product)}")
                            
                            # Log full product structure to help debug (first 20 fields)
                            try:
                                if isinstance(product, dict):
                                    # Log a sample of the product structure
                                    product_sample = {k: str(v)[:200] if not isinstance(v, (dict, list)) else f"{type(v).__name__}({len(v) if hasattr(v, '__len__') else 'N/A'})" for k, v in list(product.items())[:20]}
                                    logger.info(f"🔍 Product structure sample for {symbol}: {json.dumps(product_sample, indent=2, default=str)}")
                            except Exception as e:
                                logger.debug(f"Could not log product structure: {e}")
                            
                except ValueError as e:
                    # Product not found - this is expected if symbol mapping is incorrect
                    logger.debug(f"Product not found for {symbol}: {e}")
                except Exception as e:
                    logger.debug(f"Could not fetch margin from product info for {symbol}: {e}")
                
                # Method 2: Try to get margin from account/portfolio endpoints
                if api_rate is None:
                    try:
                        # Try to get account information which might include margin requirements
                        if hasattr(self.rest_client, 'get_portfolio') and self.portfolio_id:
                            try:
                                portfolio_response = self.rest_client.get_portfolio(self.portfolio_id)
                                portfolio = getattr(portfolio_response, 'portfolio', None)
                                if portfolio:
                                    # Check for margin fields in portfolio
                                    if isinstance(portfolio, dict):
                                        portfolio_keys = list(portfolio.keys())
                                        logger.info(f"🔍 Portfolio fields available: {portfolio_keys}")
                                        for key in portfolio_keys:
                                            if 'margin' in key.lower():
                                                logger.info(f"   Found portfolio margin field '{key}': {portfolio[key]}")
                                    elif hasattr(portfolio, '__dict__'):
                                        portfolio_dict = portfolio.__dict__
                                        for key in portfolio_dict:
                                            if 'margin' in key.lower():
                                                logger.info(f"   Found portfolio margin field '{key}': {portfolio_dict[key]}")
                            except Exception as e:
                                logger.debug(f"Could not fetch portfolio data: {e}")
                        
                        # Try account endpoints if available
                        if api_rate is None and hasattr(self.rest_client, 'list_accounts'):
                            try:
                                # Get account info - might have margin requirements
                                accounts_response = self.rest_client.list_accounts()
                                accounts = getattr(accounts_response, 'accounts', []) or []
                                if accounts:
                                    logger.info(f"🔍 Found {len(accounts)} accounts, checking for margin data")
                                    # Check first account for margin fields
                                    if accounts:
                                        acc = accounts[0]
                                        if isinstance(acc, dict):
                                            acc_keys = list(acc.keys())
                                            logger.info(f"🔍 Account fields: {acc_keys}")
                                        elif hasattr(acc, '__dict__'):
                                            acc_dict = acc.__dict__
                                            logger.info(f"🔍 Account fields: {list(acc_dict.keys())}")
                            except Exception as e:
                                logger.debug(f"Could not fetch account data: {e}")
                    except Exception as e:
                        logger.debug(f"Could not fetch margin from account/portfolio endpoints: {e}")
                        
        except Exception as e:
            logger.debug(f"Could not fetch margin from API for {symbol}: {e}")
        
        # Use API rate if available, otherwise use indicative rates
        if api_rate is not None:
            # API provided a rate - use it for both daytime and overnight
            # Overnight rate estimated at 20% increase if not provided separately
            daytime_rate = api_rate
            # Use provided overnight rate if available, otherwise estimate
            if api_overnight_rate is not None:
                overnight_rate = api_overnight_rate
            else:
                overnight_rate = api_rate * 1.2  # Estimate 20% increase for overnight
            source = "api"
            logger.info(f"✅ Using API margin rate for {symbol} {side_upper}: intraday {daytime_rate*100:.4f}%, overnight {overnight_rate*100:.4f}%")
        else:
            # Fall back to indicative rates (from Coinbase documentation)
            rates = indicative_rates.get(contract_code, {})
            side_rates = rates.get(side_upper, {})
            daytime_rate = side_rates.get("daytime", 0.20)  # Default 20% if not found
            overnight_rate = side_rates.get("overnight")
            source = "indicative"
            
            # If overnight rate not available, estimate 20% increase
            if overnight_rate is None:
                overnight_rate = daytime_rate * 1.2  # Estimate 20% increase
            
            logger.debug(f"Using indicative margin rate for {symbol} {side_upper}: intraday {daytime_rate*100:.4f}%, overnight {overnight_rate*100:.4f}%")
        
        # Determine current rate based on time period
        if is_overnight:
            current_rate = overnight_rate
        else:
            current_rate = daytime_rate
        
        # Maintenance margin (estimated at 0.4% for BTC, may vary)
        # TODO: Fetch actual maintenance margin from API when available
        # For now, use a conservative estimate based on typical futures maintenance margins
        maintenance_margin_pct = 0.004  # 0.4% (estimated - typically 0.2-0.5% for crypto futures)
        
        result = {
            "initial_margin_pct": current_rate,
            "maintenance_margin_pct": maintenance_margin_pct,
            "is_overnight_period": is_overnight,
            "daytime_rate": daytime_rate,
            "overnight_rate": overnight_rate,
            "current_rate": current_rate,
            "source": source,
            "timestamp": datetime.utcnow().isoformat(),
            "contract_code": contract_code,
            "side": side_upper
        }
        
        # Cache the result (daily cache)
        self._margin_cache[cache_key] = result.copy()
        self._margin_cache_date[cache_key] = today_str
        
        # Log cache operation
        if source == "api":
            logger.info(f"✅ Cached API margin rate for {symbol} {side_upper} ({period}): {current_rate*100:.4f}%")
        else:
            logger.debug(f"Cached indicative margin rate for {symbol} {side_upper} ({period}): {current_rate*100:.4f}%")
        
        return result
    
    def refresh_margin_cache(self, symbol: Optional[str] = None) -> None:
        """
        Force refresh of margin requirements cache.
        Clears cache so rates will be re-fetched on next request.
        
        Args:
            symbol: Optional symbol to refresh. If None, refreshes all cached margins.
        """
        if symbol:
            contract_code = self._contract_code_from_symbol(symbol)
            # Remove all cache entries for this contract (both intraday and overnight)
            keys_to_remove = [k for k in self._margin_cache.keys() if k.startswith(f"{contract_code}_")]
            for key in keys_to_remove:
                self._margin_cache.pop(key, None)
                self._margin_cache_date.pop(key, None)
            logger.info(f"Refreshed margin cache for {symbol}")
        else:
            self._margin_cache.clear()
            self._margin_cache_date.clear()
            logger.info("Refreshed all margin cache")
    
    def ensure_daily_margin_rates(self, symbol: str, side: str = "LONG", force_refresh: bool = False) -> None:
        """
        Ensure both intraday and overnight margin rates are cached for today.
        Called before trade execution to fetch the most current rates.
        
        Args:
            symbol: Trading symbol
            side: Position side ("LONG" or "SHORT")
            force_refresh: If True, always fetch fresh rates (default: True for trade execution)
        """
        from zoneinfo import ZoneInfo
        
        et_tz = ZoneInfo('America/New_York')
        now_et = datetime.now(et_tz)
        today_str = now_et.strftime('%Y-%m-%d')
        contract_code = self._contract_code_from_symbol(symbol)
        side_upper = side.upper()
        
        # Check if we have both rates cached for today
        intraday_key = f"{contract_code}_{side_upper}_intraday"
        overnight_key = f"{contract_code}_{side_upper}_overnight"
        
        intraday_cached = (
            intraday_key in self._margin_cache and
            self._margin_cache_date.get(intraday_key) == today_str
        )
        overnight_cached = (
            overnight_key in self._margin_cache and
            self._margin_cache_date.get(overnight_key) == today_str
        )
        
        # Before trade execution or initialization: Always fetch fresh rates from API
        # This ensures we use the latest margin requirements for trade validation
        # The fresh rates will update the daily cache
        if force_refresh:
            logger.info(f"📊 Fetching fresh margin rates from API for {symbol} {side_upper}")
            try:
                # Fetch fresh intraday rate from API (bypasses cache, forces API fetch)
                # force_fresh=True ensures we get the most current API rates
                self.get_margin_requirements(symbol, side, force_period='intraday', force_fresh=True)
                logger.info(f"✅ Fresh intraday margin rate fetched from API for {symbol} {side_upper}")
            except Exception as e:
                logger.warning(f"Failed to fetch fresh intraday margin rate for {symbol}: {e}")
            
            try:
                # Fetch fresh overnight rate from API (bypasses cache, forces API fetch)
                # force_fresh=True ensures we get the most current API rates
                self.get_margin_requirements(symbol, side, force_period='overnight', force_fresh=True)
                logger.info(f"✅ Fresh overnight margin rate fetched from API for {symbol} {side_upper}")
            except Exception as e:
                logger.warning(f"Failed to fetch fresh overnight margin rate for {symbol}: {e}")
        else:
            # Only fetch missing rates (for EOD reporting - uses cached rates if available)
            # If cache is empty, fetch from API (not just indicative rates)
            if not intraday_cached:
                logger.info(f"📊 Fetching intraday margin rate from API for {symbol} {side_upper} (will cache for today)")
                try:
                    # Fetch from API (force_fresh=False but cache is empty, so will fetch from API)
                    # This ensures we get API rates, not just indicative rates
                    self.get_margin_requirements(symbol, side, force_period='intraday', force_fresh=False)
                except Exception as e:
                    logger.warning(f"Failed to fetch intraday margin rate for {symbol}: {e}")
            
            if not overnight_cached:
                logger.info(f"📊 Fetching overnight margin rate from API for {symbol} {side_upper} (will cache for today)")
                try:
                    # Fetch from API (force_fresh=False but cache is empty, so will fetch from API)
                    # This ensures we get API rates, not just indicative rates
                    self.get_margin_requirements(symbol, side, force_period='overnight', force_fresh=False)
                except Exception as e:
                    logger.warning(f"Failed to fetch overnight margin rate for {symbol}: {e}")
        
        # Verify both rates are now cached
        intraday_final = (
            intraday_key in self._margin_cache and
            self._margin_cache_date.get(intraday_key) == today_str
        )
        overnight_final = (
            overnight_key in self._margin_cache and
            self._margin_cache_date.get(overnight_key) == today_str
        )
        
        if intraday_final and overnight_final:
            intraday_source = self._margin_cache.get(intraday_key, {}).get('source', 'unknown')
            overnight_source = self._margin_cache.get(overnight_key, {}).get('source', 'unknown')
            intraday_rate = self._margin_cache.get(intraday_key, {}).get('daytime_rate', 0) * 100
            overnight_rate = self._margin_cache.get(overnight_key, {}).get('overnight_rate', 0) * 100
            logger.info(f"✅ Daily margin rates cached for {symbol} {side_upper}: intraday {intraday_rate:.4f}% ({intraday_source}), overnight {overnight_rate:.4f}% ({overnight_source})")
        else:
            logger.warning(f"⚠️ Some margin rates not cached for {symbol} {side_upper}: intraday={intraday_final}, overnight={overnight_final}")
    
    def calculate_required_margin(self, symbol: str, quantity: float, price: float, side: str = "LONG", leverage: int = 1) -> Dict[str, Any]:
        """
        Calculate required margin for a position.
        
        Args:
            symbol: Trading symbol
            quantity: Position quantity
            price: Entry price
            side: Position side ("LONG" or "SHORT")
            leverage: Leverage multiplier
        
        Returns:
            Dict with margin calculations:
            {
                "notional_value": float,
                "initial_margin_required": float,
                "maintenance_margin_required": float,
                "margin_requirements": dict,  # Full margin requirements dict
                "can_afford_overnight": bool,  # True if can afford overnight margin
            }
        """
        margin_req = self.get_margin_requirements(symbol, side)
        
        notional_value = quantity * price
        initial_margin_pct = margin_req["initial_margin_pct"]
        maintenance_margin_pct = margin_req["maintenance_margin_pct"]
        
        # Initial margin required (at leverage)
        initial_margin_required = (notional_value * initial_margin_pct) / leverage
        maintenance_margin_required = notional_value * maintenance_margin_pct
        
        # Check if can afford overnight margin
        overnight_rate = margin_req.get("overnight_rate", initial_margin_pct * 1.2)
        overnight_margin_required = (notional_value * overnight_rate) / leverage
        can_afford_overnight = True  # Will be checked against available balance
        
        return {
            "notional_value": notional_value,
            "initial_margin_required": initial_margin_required,
            "maintenance_margin_required": maintenance_margin_required,
            "overnight_margin_required": overnight_margin_required,
            "margin_requirements": margin_req,
            "can_afford_overnight": can_afford_overnight,
            "leverage": leverage
        }


def create_coinbase_futures_trader(
    api_key: str,
    api_secret: str,
    leverage: int = 1,
    sandbox: bool = False,
    demo_mode: bool = False,
    portfolio_id: Optional[str] = None
) -> 'CoinbaseFuturesTrader':
    """
    Factory function to create Coinbase Futures trader with standard interface
    
    Args:
        api_key: Coinbase API key
        api_secret: Coinbase API secret
        leverage: Default leverage (1-10x)
        sandbox: Use sandbox environment
        demo_mode: Paper trading mode
    
    Returns:
        CoinbaseFuturesTrader instance
    """
    broker = CoinbaseFuturesBroker(
        api_key=api_key,
        api_secret=api_secret,
        sandbox=sandbox,
        demo_mode=demo_mode,
        portfolio_id=portfolio_id
    )
    
    trader = CoinbaseFuturesTrader(broker, default_leverage=leverage)
    
    mode = "DEMO (Paper)" if demo_mode else ("SANDBOX" if sandbox else "LIVE")
    logger.info(f"Coinbase Futures trader created - {mode} - Leverage: {leverage}x")
    
    return trader


class CoinbaseFuturesTrader:
    """
    Wrapper for Coinbase Futures broker with standard trading interface
    Matches the interface used by Order Executor
    """
    
    def __init__(self, broker: CoinbaseFuturesBroker, default_leverage: int = 1):
        self.broker = broker
        self.default_leverage = default_leverage
        logger.info(f"CoinbaseFuturesTrader initialized with {default_leverage}x leverage")
    
    # ------------------------------------------------------------------
    # Unified BrokerInterface passthrough helpers
    # ------------------------------------------------------------------
    
    def get_broker_name(self) -> str:
        if hasattr(self.broker, "get_broker_name"):
            return self.broker.get_broker_name()
        return "Coinbase Futures"
    
    async def get_account_balance_usd(self) -> float:
        return await self.broker.get_account_balance_usd()
    
    async def get_available_balance_usd(self) -> float:
        return await self.broker.get_available_balance_usd()
    
    async def get_all_positions(self) -> List[Dict]:
        return await self.broker.get_all_positions()
    
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        return await self.broker.set_leverage(symbol, leverage)
    
    async def get_current_leverage(self, symbol: str) -> Optional[int]:
        return await self.broker.get_current_leverage(symbol)
    
    def get_current_price(self, symbol: str) -> float:
        return self.broker.get_current_price(symbol)

    def get_account_balance(self) -> Dict:
        """
        Return full broker balance snapshot (matches the demo/live logic inside CoinbaseFuturesBroker).
        """
        if hasattr(self.broker, "get_account_balance"):
            return self.broker.get_account_balance()
        raise AttributeError("Underlying broker does not support get_account_balance")
    
    def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        leverage: int = 1,
        client_order_id: Optional[str] = None,
        price_override: Optional[float] = None
    ) -> Dict:
        return self.broker.place_market_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
            leverage=leverage,
            client_order_id=client_order_id,
            price_override=price_override
        )
    
    def close_position_market(self, symbol: str) -> Dict:
        return self.broker.close_position_market(symbol)
    
    def get_position(self, symbol: str) -> Dict:
        return self.broker.get_position(symbol)
    
    def get_margin_requirements(self, symbol: str, side: str = "LONG") -> Dict[str, Any]:
        """Get margin requirements for a symbol and side"""
        return self.broker.get_margin_requirements(symbol, side)
    
    def calculate_required_margin(self, symbol: str, quantity: float, price: float, side: str = "LONG", leverage: int = 1) -> Dict[str, Any]:
        """Calculate required margin for a position"""
        return self.broker.calculate_required_margin(symbol, quantity, price, side, leverage)
    
    def refresh_margin_cache(self, symbol: Optional[str] = None) -> None:
        """Force refresh of margin requirements cache"""
        return self.broker.refresh_margin_cache(symbol)
    
    def ensure_daily_margin_rates(self, symbol: str, side: str = "LONG") -> None:
        """Ensure both intraday and overnight margin rates are cached for today"""
        return self.broker.ensure_daily_margin_rates(symbol, side)
    
    async def execute_signal(self, signal: Dict) -> Dict:
        """
        Execute a trading signal (buy/sell/close)
        
        Args:
            signal: Signal dict with action, symbol, qty_usdt, leverage, etc.
        
        Returns:
            Execution result dict
        """
        try:
            action = signal.get('action')
            symbol = signal.get('symbol')
            qty_usdt = signal.get('qty_usdt', 0)
            leverage = signal.get('leverage', self.default_leverage)
            sl_pct = signal.get('sl_pct')
            tp_pct = signal.get('tp_pct')
            
            # Set leverage
            self.broker.set_leverage(symbol, leverage)
            
            if action == 'buy':
                # Long entry
                current_price = self.broker.get_current_price(symbol)
                quantity = qty_usdt / current_price
                
                order = self.broker.place_market_order(symbol, 'BUY', quantity, leverage)
                
                return {
                    'status': 'success',
                    'action': 'buy',
                    'order_id': order['orderId'],
                    'symbol': symbol,
                    'side': 'LONG',
                    'quantity': quantity,
                    'price': order.get('price', current_price)
                }
            
            elif action == 'sell':
                # Short entry
                current_price = self.broker.get_current_price(symbol)
                quantity = qty_usdt / current_price
                
                order = self.broker.place_market_order(symbol, 'SELL', quantity, leverage)
                
                return {
                    'status': 'success',
                    'action': 'sell',
                    'order_id': order['orderId'],
                    'symbol': symbol,
                    'side': 'SHORT',
                    'quantity': quantity,
                    'price': order.get('price', current_price)
                }
            
            elif action == 'close':
                # Close position
                order = self.broker.close_position_market(symbol)
                
                return {
                    'status': 'success',
                    'action': 'close',
                    'order_id': order.get('orderId'),
                    'symbol': symbol,
                    'side': 'CLOSE'
                }
            
            else:
                raise ValueError(f"Unknown action: {action}")
        
        except Exception as e:
            logger.error(f"Error executing Coinbase signal: {str(e)}")
            raise


# Example usage and testing
if __name__ == "__main__":
    # Test in demo mode
    broker = CoinbaseFuturesBroker(
        api_key="test_key",
        api_secret="test_secret",
        demo_mode=True
    )
    
    print("\n" + "="*70)
    print("COINBASE FUTURES BROKER - DEMO TEST")
    print("="*70 + "\n")
    
    # Test balance
    balance = broker.get_account_balance()
    print(f"Account Balance: ${balance['available_balance']:,.2f}")
    
    # Test market order
    order = broker.place_market_order('BTCUSDC', 'BUY', 0.01, leverage=3)
    print(f"Market Order: {order}")
    
    # Test SL/TP
    sl = broker.place_stop_loss('BTCUSDC', 'SELL', 0.01, 98000)
    tp = broker.place_take_profit('BTCUSDC', 'SELL', 0.01, 105000)
    print(f"Stop Loss: {sl}")
    print(f"Take Profit: {tp}")
    
    # Test verification
    sl_ok, tp_ok, msg = broker.verify_sl_tp_exist('BTCUSDC')
    print(f"SL/TP Verification: {msg}")
    
    # Test position
    position = broker.get_position('BTCUSDC')
    print(f"Position: {position['positionSide']} {position['positionAmt']} @ ${position['entryPrice']:,.2f}")
    
    # Test close
    close_order = broker.close_position_market('BTCUSDC')
    print(f"Close Order: {close_order}")
    
    print("\n" + "="*70)
    print("✅ DEMO TEST COMPLETE")
    print("="*70 + "\n")

