"""
TradingView Agent - Order Executor Service
Routes orders to appropriate broker and manages execution

Enhanced with critical safety features:
- Retry logic with exponential backoff
- SL/TP verification
- Auto-close on SL/TP failure
- Position verification
- Telegram alerts
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional
from models.webhook_payload import WebhookPayload, OrderResponse, PositionInfo
from brokers.base_dex import BaseDEXBroker, BaseDEXTrader
from brokers.binance_futures import create_binance_futures_trader
from brokers.coinbase_futures import create_coinbase_futures_trader
from services.position_sizer import PositionSizer
from services.order_execution_safety import OrderExecutionSafety
from services.telegram_notifier import telegram_notifier
from config.contract_expiry import should_allow_entry, should_allow_exit, check_contract_status

logger = logging.getLogger(__name__)


class OrderExecutor:
    """
    Routes and executes orders across multiple brokers
    Applies ADV Cap (Slip Guard) before execution
    """
    
    def __init__(self, settings, risk_manager=None):
        self.settings = settings
        self.risk_manager = risk_manager
        self.brokers = {}
        
        # Multi-account: Store separate brokers by account suffix
        # Will be initialized on-demand when signals arrive
        self.binance_accounts = {}  # Will store brokers keyed by "_1x", "_3x", "_5x"
        self.coinbase_accounts = {}  # Will store Coinbase brokers keyed by "_1x", "_3x", "_5x"
        self.kraken_accounts = {}  # Will store Kraken brokers keyed by "_1x", "_2x", "_3x"
        self.safety_wrappers = {}  # Store safety wrappers for each broker
        
        # Initialize default Binance Futures broker (if configured) - for backward compatibility
        if settings.BINANCE_API_KEY and settings.BINANCE_SECRET:
            try:
                # Check for demo mode
                demo_mode = getattr(settings, 'DEMO_MODE', False)
                
                # Get default leverage (1x or 3x)
                default_leverage = getattr(settings, 'BINANCE_FUTURES_LEVERAGE', 1)
                
                broker = create_binance_futures_trader(
                    api_key=settings.BINANCE_API_KEY,
                    api_secret=settings.BINANCE_SECRET,
                    leverage=default_leverage,
                    demo_mode=demo_mode
                )
                
                self.brokers['binance_futures'] = broker
                
                # Create safety wrapper for this broker
                self.safety_wrappers['binance_futures'] = OrderExecutionSafety(broker.broker)
                
                mode = "DEMO (Paper Trading)" if demo_mode else "LIVE"
                logger.info(f"Binance Futures broker initialized - {mode} - Leverage: {default_leverage}x")
                logger.info(f"✅ Safety features enabled for binance_futures (retry, verification, auto-close)")
            except Exception as e:
                logger.error(f"Failed to initialize Binance Futures: {str(e)}")
        
        # Check for multi-account configuration
        multi_account_configured = any([
            settings.BINANCE_API_KEY_1X,
            settings.BINANCE_API_KEY_3X,
            settings.BINANCE_API_KEY_5X
        ])
        
        if multi_account_configured:
            logger.info("Multi-account mode detected - brokers will be initialized on-demand")
            logger.info(f"  1x account: {'✅ Configured' if settings.BINANCE_API_KEY_1X else '❌ Not configured'}")
            logger.info(f"  3x account: {'✅ Configured' if settings.BINANCE_API_KEY_3X else '❌ Not configured'}")
            logger.info(f"  5x account: {'✅ Configured' if settings.BINANCE_API_KEY_5X else '❌ Not configured'}")
        
        # Initialize Coinbase Futures broker (if configured and enabled)
        if settings.ENABLE_COINBASE and settings.COINBASE_API_KEY and settings.COINBASE_API_SECRET:
            try:
                demo_mode = getattr(settings, 'DEMO_MODE', False)
                sandbox = getattr(settings, 'COINBASE_SANDBOX', False)
                default_leverage = getattr(settings, 'COINBASE_FUTURES_LEVERAGE', 1)
                
                broker = create_coinbase_futures_trader(
                    api_key=settings.COINBASE_API_KEY,
                    api_secret=settings.COINBASE_API_SECRET,
                    leverage=default_leverage,
                    sandbox=sandbox,
                    demo_mode=demo_mode
                )
                
                self.brokers['coinbase_futures'] = broker
                
                # Create safety wrapper for this broker
                self.safety_wrappers['coinbase_futures'] = OrderExecutionSafety(broker.broker)
                
                mode = "DEMO (Paper Trading)" if demo_mode else ("SANDBOX" if sandbox else "LIVE")
                logger.info(f"Coinbase Futures broker initialized - {mode} - Leverage: {default_leverage}x")
                logger.info(f"✅ Safety features enabled for coinbase_futures (retry, verification, auto-close)")
            except Exception as e:
                logger.error(f"Failed to initialize Coinbase Futures: {str(e)}")
        
        # Check for multi-account Coinbase configuration
        coinbase_multi_configured = any([
            settings.COINBASE_API_KEY_1X,
            settings.COINBASE_API_KEY_3X,
            settings.COINBASE_API_KEY_5X
        ])
        
        if coinbase_multi_configured:
            logger.info("Coinbase multi-account mode detected - brokers will be initialized on-demand")
            logger.info(f"  Coinbase 1x: {'✅ Configured' if settings.COINBASE_API_KEY_1X else '❌ Not configured'}")
            logger.info(f"  Coinbase 3x: {'✅ Configured' if settings.COINBASE_API_KEY_3X else '❌ Not configured'}")
            logger.info(f"  Coinbase 5x: {'✅ Configured' if settings.COINBASE_API_KEY_5X else '❌ Not configured'}")
        
        # Initialize Base DEX broker (if configured and enabled)
        if settings.ENABLE_BASE_DEX and settings.BASE_WALLET_PRIVATE_KEY:
            try:
                base_broker = BaseDEXBroker(settings)
                self.brokers['base_dex'] = BaseDEXTrader(base_broker)
                logger.info("Base DEX broker initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Base DEX: {str(e)}")
        
        # Note: Kraken Futures broker not yet implemented (Month 2+)
        # Will add when ready: if settings.ENABLE_KRAKEN and settings.KRAKEN_API_KEY...
        
        logger.info(f"OrderExecutor initialized with brokers: {list(self.brokers.keys())}")
        
        # Initialize Position Sizer for dynamic position sizing (90% of equity)
        # Note: For multi-account, brokers dict will be updated on-demand
        self.position_sizer = PositionSizer(settings, self.brokers)
        logger.info("OrderExecutor: Position Sizer integrated (Dynamic 90% equity sizing)")
        
        if risk_manager:
            logger.info("OrderExecutor: Risk manager integrated (ADV Cap enabled)")
    
    def _get_or_create_binance_account(self, strategy_name: str):
        """
        Get or create Binance broker for specific account based on strategy name
        
        For multi-account support: strategy_name ending in _1x/_3x/_5x routes to
        different Binance accounts with different API keys and leverage.
        
        Args:
            strategy_name: Strategy name from webhook (e.g., "IchimokuBinance5m_3x")
        
        Returns:
            Broker instance for that account
        """
        strategy_lower = strategy_name.lower()
        
        # Determine account key
        if strategy_lower.endswith('_1x'):
            account_key = '1x'
        elif strategy_lower.endswith('_3x'):
            account_key = '3x'
        elif strategy_lower.endswith('_5x'):
            account_key = '5x'
        else:
            # Use default broker (single account)
            return self.brokers.get('binance_futures')
        
        # Check if already initialized
        if account_key in self.binance_accounts:
            return self.binance_accounts[account_key]
        
        # Initialize new broker for this account
        api_key, api_secret, demo_mode, leverage = self.settings.get_binance_credentials(strategy_name)
        
        if not api_key or not api_secret:
            logger.error(f"No credentials configured for {account_key} account (strategy: {strategy_name})")
            return None
        
        try:
            broker = create_binance_futures_trader(
                api_key=api_key,
                api_secret=api_secret,
                leverage=leverage,
                demo_mode=demo_mode
            )
            
            self.binance_accounts[account_key] = broker
            
            # Create safety wrapper for this broker
            safety_key = f'binance_futures_{account_key}'
            self.safety_wrappers[safety_key] = OrderExecutionSafety(broker.broker)
            
            mode = "DEMO" if demo_mode else "LIVE"
            logger.info(f"Initialized Binance Futures {account_key} account - {mode} @ {leverage}x leverage")
            logger.info(f"✅ Safety features enabled for {account_key} account (retry, verification, auto-close)")
            
            # Update position sizer's broker dict so it can fetch balances
            self.position_sizer.brokers[f'binance_futures_{account_key}'] = broker
            
            return broker
            
        except Exception as e:
            logger.error(f"Failed to initialize Binance account {account_key}: {str(e)}")
            return None
    
    def _get_or_create_coinbase_account(self, strategy_name: str):
        """
        Get or create Coinbase broker for specific account based on strategy name
        
        For multi-account support: strategy_name ending in _1x/_3x/_5x routes to
        different Coinbase accounts with different API keys and leverage.
        
        Args:
            strategy_name: Strategy name from webhook (e.g., "IchimokuCoinbaseUSA5m_3x")
        
        Returns:
            Broker instance for that account
        """
        strategy_lower = strategy_name.lower()
        
        # Determine account key
        if strategy_lower.endswith('_1x'):
            account_key = '1x'
        elif strategy_lower.endswith('_3x'):
            account_key = '3x'
        elif strategy_lower.endswith('_5x'):
            account_key = '5x'
        else:
            # Use default broker (single account)
            return self.brokers.get('coinbase_futures')
        
        # Check if already initialized
        if account_key in self.coinbase_accounts:
            return self.coinbase_accounts[account_key]
        
        # Get API keys for this account
        if account_key == '1x':
            api_key = self.settings.COINBASE_API_KEY_1X
            api_secret = self.settings.COINBASE_SECRET_1X
            leverage = 1
        elif account_key == '3x':
            api_key = self.settings.COINBASE_API_KEY_3X
            api_secret = self.settings.COINBASE_SECRET_3X
            leverage = 3
        elif account_key == '5x':
            api_key = self.settings.COINBASE_API_KEY_5X
            api_secret = self.settings.COINBASE_SECRET_5X
            leverage = 5
        else:
            logger.error(f"Unknown account key: {account_key}")
            return None
        
        if not api_key or not api_secret:
            logger.error(f"No Coinbase credentials configured for {account_key} account (strategy: {strategy_name})")
            return None
        
        try:
            demo_mode = getattr(self.settings, 'DEMO_MODE', False)
            sandbox = getattr(self.settings, 'COINBASE_SANDBOX', False)
            
            broker = create_coinbase_futures_trader(
                api_key=api_key,
                api_secret=api_secret,
                leverage=leverage,
                sandbox=sandbox,
                demo_mode=demo_mode
            )
            
            self.coinbase_accounts[account_key] = broker
            
            # Create safety wrapper for this broker
            safety_key = f'coinbase_futures_{account_key}'
            self.safety_wrappers[safety_key] = OrderExecutionSafety(broker.broker)
            
            mode = "DEMO" if demo_mode else ("SANDBOX" if sandbox else "LIVE")
            logger.info(f"Initialized Coinbase Futures {account_key} account - {mode} @ {leverage}x leverage")
            logger.info(f"✅ Safety features enabled for Coinbase {account_key} account (retry, verification, auto-close)")
            
            # Update position sizer's broker dict so it can fetch balances
            self.position_sizer.brokers[f'coinbase_futures_{account_key}'] = broker
            
            return broker
            
        except Exception as e:
            logger.error(f"Failed to initialize Coinbase account {account_key}: {str(e)}")
            return None
    
    async def _execute_binance_with_safety(
        self,
        payload: WebhookPayload,
        broker_key: str,
        broker
    ) -> OrderResponse:
        """
        Execute Binance Futures order with full safety features.
        Uses OrderExecutionSafety wrapper for retry, verification, and auto-close.
        
        Args:
            payload: Webhook payload
            broker_key: Broker key for safety wrapper lookup
            broker: Binance broker instance
        
        Returns:
            OrderResponse
        """
        # Get safety wrapper
        safety_wrapper = self.safety_wrappers.get(broker_key)
        
        if not safety_wrapper:
            # No safety wrapper - fall back to direct execution
            logger.warning(f"No safety wrapper found for {broker_key}, using direct execution")
            signal = {
                'action': payload.action,
                'symbol': payload.symbol,
                'qty_pct': payload.qty_pct,
                'qty_usdt': payload.qty_usdt,
                'qty_units': payload.qty_units,
                'tp_pct': payload.tp_pct,
                'sl_pct': payload.sl_pct,
                'price': payload.price
            }
            result = await broker.execute_signal(signal)
            
            return OrderResponse(
                status="success",
                order_id=result.get('order_id'),
                client_order_id=payload.get_client_order_id(),
                symbol=payload.symbol,
                side=result.get('side', payload.get_side()),
                quantity=result.get('quantity', 0),
                fill_price=result.get('price'),
                timestamp=datetime.utcnow().isoformat(),
                broker='binance_futures',
                strategy=payload.strategy
            )
        
        # Use safety wrapper
        try:
            if payload.action in ['buy', 'sell']:
                # Entry order with full safety
                logger.info(f"Using safety-wrapped entry execution for {payload.symbol}")
                
                # Get current price for SL/TP calculation
                current_price = broker.broker.get_current_price(payload.symbol)
                
                # Calculate SL/TP prices from percentages
                sl_price = None
                tp_price = None
                
                if payload.sl_pct:
                    if payload.action == 'buy':
                        sl_price = current_price * (1 - payload.sl_pct / 100)
                    else:  # sell/short
                        sl_price = current_price * (1 + payload.sl_pct / 100)
                
                if payload.tp_pct:
                    if payload.action == 'buy':
                        tp_price = current_price * (1 + payload.tp_pct / 100)
                    else:  # sell/short
                        tp_price = current_price * (1 - payload.tp_pct / 100)
                
                # Calculate quantity in base currency (e.g., BTC)
                quantity = payload.qty_usdt / current_price
                
                # Determine side
                side = 'BUY' if payload.action == 'buy' else 'SELL'
                
                # Execute with full safety
                safety_result = await safety_wrapper.execute_entry_with_full_safety(
                    symbol=payload.symbol,
                    side=side,
                    quantity=quantity,
                    leverage=payload.leverage or 1,
                    sl_price=sl_price,
                    tp_price=tp_price,
                    strategy_name=payload.strategy
                )
                
                if safety_result['status'] == 'success':
                    entry_order = safety_result['entry_order']
                    return OrderResponse(
                        status="success",
                        order_id=entry_order.get('orderId'),
                        client_order_id=payload.get_client_order_id(),
                        symbol=payload.symbol,
                        side=side,
                        quantity=quantity,
                        fill_price=float(entry_order.get('avgPrice', current_price)),
                        timestamp=datetime.utcnow().isoformat(),
                        broker='binance_futures',
                        strategy=payload.strategy
                    )
                elif safety_result['status'] == 'emergency_closed':
                    return OrderResponse(
                        status="warning",
                        order_id="emergency_close",
                        client_order_id=payload.get_client_order_id(),
                        symbol=payload.symbol,
                        side=side,
                        quantity=0,
                        timestamp=datetime.utcnow().isoformat(),
                        broker='binance_futures',
                        strategy=payload.strategy,
                        error="SL/TP failed, position emergency closed"
                    )
                else:
                    return OrderResponse(
                        status="failed",
                        client_order_id=payload.get_client_order_id(),
                        symbol=payload.symbol,
                        side=side,
                        quantity=0,
                        timestamp=datetime.utcnow().isoformat(),
                        broker='binance_futures',
                        strategy=payload.strategy,
                        error=f"Entry failed: {safety_result.get('status')}"
                    )
            
            elif payload.action == 'close':
                # Exit order with safety
                logger.info(f"Using safety-wrapped exit execution for {payload.symbol}")
                
                safety_result = await safety_wrapper.execute_exit_with_full_safety(
                    symbol=payload.symbol,
                    strategy_name=payload.strategy
                )
                
                if safety_result['status'] == 'success':
                    close_order = safety_result['close_order']
                    return OrderResponse(
                        status="success",
                        order_id=close_order.get('orderId', 'close'),
                        client_order_id=payload.get_client_order_id(),
                        symbol=payload.symbol,
                        side="CLOSE",
                        quantity=0,
                        timestamp=datetime.utcnow().isoformat(),
                        broker='binance_futures',
                        strategy=payload.strategy
                    )
                else:
                    return OrderResponse(
                        status="failed",
                        client_order_id=payload.get_client_order_id(),
                        symbol=payload.symbol,
                        side="CLOSE",
                        quantity=0,
                        timestamp=datetime.utcnow().isoformat(),
                        broker='binance_futures',
                        strategy=payload.strategy,
                        error=f"Exit failed: {safety_result.get('status')}"
                    )
            
        except Exception as e:
            logger.error(f"Safety-wrapped execution failed: {str(e)}", exc_info=True)
            return OrderResponse(
                status="failed",
                client_order_id=payload.get_client_order_id(),
                symbol=payload.symbol,
                side=payload.get_side(),
                quantity=0,
                timestamp=datetime.utcnow().isoformat(),
                broker='binance_futures',
                strategy=payload.strategy,
                error=str(e)
            )
    
    async def execute(self, payload: WebhookPayload) -> OrderResponse:
        """
        Execute order based on payload
        Applies ADV Cap (Slip Guard) before execution to minimize slippage
        
        Args:
            payload: Validated webhook payload from TradingView
        
        Returns:
            OrderResponse with execution details
        """
        try:
            broker_name = payload.broker
            strategy_name = payload.strategy
            
            # ============================================
            # CRITICAL: CONTRACT EXPIRY CHECK
            # ============================================
            # Block new entries on expiring contracts (within 10 days)
            # Allow exits to continue (must close positions!)
            
            if payload.action in ['buy', 'sell']:
                # Check if entry is allowed on this contract
                allow_entry, reason = should_allow_entry(payload.symbol)
                
                if not allow_entry:
                    # Get full status for logging
                    status, days_left, message = check_contract_status(payload.symbol)
                    
                    error_msg = f"❌ NEW ENTRY BLOCKED: {reason}"
                    logger.error(error_msg)
                    logger.error(f"Contract Status: {message}")
                    
                    # Send urgent Telegram alert
                    await telegram_notifier.send_critical_failure(
                        f"🚨 ENTRY BLOCKED - CONTRACT EXPIRING!\n\n"
                        f"Symbol: {payload.symbol}\n"
                        f"Status: {message}\n"
                        f"Action: {payload.action.upper()}\n"
                        f"Strategy: {strategy_name}\n\n"
                        f"⚠️ New entries disabled on this contract.\n"
                        f"✅ Exits still allowed to close positions.\n\n"
                        f"🔧 ACTION REQUIRED:\n"
                        f"1. Close any open positions on {payload.symbol}\n"
                        f"2. Roll to new contract on TradingView\n"
                        f"3. Update alerts for new contract\n"
                        f"4. Resume trading\n\n"
                        f"Contract expires in {days_left} days!" if days_left is not None else ""
                    )
                    
                    return OrderResponse(
                        status="rejected",
                        client_order_id=payload.get_client_order_id(),
                        symbol=payload.symbol,
                        side=payload.get_side(),
                        quantity=0,
                        timestamp=datetime.utcnow().isoformat(),
                        broker=broker_name,
                        strategy=strategy_name,
                        error=error_msg
                    )
                
                # Log if contract is in warning period
                status, days_left, message = check_contract_status(payload.symbol)
                if status == "warning" and days_left is not None:
                    logger.warning(f"⚠️ Contract Warning: {payload.symbol} expires in {days_left} days")
            
            elif payload.action == 'close':
                # Always allow exits (even on expired contracts!)
                allow_exit, reason = should_allow_exit(payload.symbol)
                
                status, days_left, message = check_contract_status(payload.symbol)
                if status in ["pause_entries", "expired"]:
                    logger.warning(f"⚠️ Allowing exit on expiring contract: {payload.symbol}")
                    logger.warning(f"Status: {message}")
            
            # Contract check passed - continue with execution
            # ============================================
            
            # Multi-account routing: Check if strategy name indicates specific account
            is_multi_account = any(
                strategy_name.lower().endswith(suffix) for suffix in ['_1x', '_2x', '_3x', '_5x']
            )
            
            if broker_name == 'binance_futures' and is_multi_account:
                # Use multi-account Binance broker
                broker = self._get_or_create_binance_account(strategy_name)
                
                if not broker:
                    raise Exception(f"Failed to initialize Binance broker for strategy: {strategy_name}")
                
                # Log which account we're using
                if strategy_name.lower().endswith('_1x'):
                    account_label = "1x (Safe)"
                elif strategy_name.lower().endswith('_2x'):
                    account_label = "2x (Balanced)"
                elif strategy_name.lower().endswith('_3x'):
                    account_label = "3x (Moderate)"
                elif strategy_name.lower().endswith('_5x'):
                    account_label = "5x (Aggressive)"
                
                logger.info(f"Multi-Account Binance: Routing {strategy_name} to Account {account_label}")
            
            elif broker_name == 'coinbase_futures' and is_multi_account:
                # Use multi-account Coinbase broker
                broker = self._get_or_create_coinbase_account(strategy_name)
                
                if not broker:
                    raise Exception(f"Failed to initialize Coinbase broker for strategy: {strategy_name}")
                
                # Log which account we're using
                if strategy_name.lower().endswith('_1x'):
                    account_label = "1x (Safe)"
                elif strategy_name.lower().endswith('_2x'):
                    account_label = "2x (Balanced)"
                elif strategy_name.lower().endswith('_3x'):
                    account_label = "3x (Moderate)"
                elif strategy_name.lower().endswith('_5x'):
                    account_label = "5x (Aggressive)"
                
                logger.info(f"Multi-Account Coinbase: Routing {strategy_name} to Account {account_label}")
            
            else:
                # Single account or other broker
                if broker_name not in self.brokers:
                    raise Exception(f"Broker {broker_name} not initialized. Available: {list(self.brokers.keys())}")
                
                broker = self.brokers[broker_name]
            
            logger.info(f"Routing to {broker_name}: {payload.action} {payload.symbol}")
            
            # Step 1: Calculate position size from account equity (90% of balance)
            # Get leverage from payload or default
            leverage = payload.leverage or getattr(self.settings, 'BINANCE_FUTURES_LEVERAGE', 1)
            
            # Calculate position size if not provided or if dynamic sizing is enabled
            if payload.action in ['buy', 'sell']:
                position_size_result = await self.position_sizer.calculate_position_size(
                    broker_name=broker_name,
                    symbol=payload.symbol,
                    leverage=leverage,
                    requested_qty_usd=payload.qty_usdt,
                    qty_pct=payload.qty_pct
                )
                
                if not position_size_result['can_trade']:
                    error_msg = f"Position sizing rejected: {position_size_result['reason']}"
                    logger.error(error_msg)
                    
                    return OrderResponse(
                        status="rejected",
                        client_order_id=payload.get_client_order_id(),
                        symbol=payload.symbol,
                        side=payload.get_side(),
                        quantity=0,
                        timestamp=datetime.utcnow().isoformat(),
                        broker=broker_name,
                        strategy=payload.strategy,
                        error=error_msg
                    )
                
                # Update payload with calculated position size
                calculated_qty = position_size_result['collateral_usd']
                
                if payload.qty_usdt != calculated_qty:
                    logger.info(
                        f"Position sized: {payload.symbol} - "
                        f"Method: {position_size_result['sizing_method']}, "
                        f"Original: ${payload.qty_usdt or 0:,.2f}, "
                        f"Calculated: ${calculated_qty:,.2f} @ {leverage}x, "
                        f"Reason: {position_size_result['reason']}"
                    )
                    payload.qty_usdt = calculated_qty
                
                # Update leverage in payload
                payload.leverage = leverage
            
            # Step 2: Apply ADV Cap (Slip Guard) if risk manager is available
            original_qty_usdt = payload.qty_usdt
            adv_cap_result = None
            
            if self.risk_manager and payload.qty_usdt and payload.action in ['buy', 'sell']:
                # Get leverage from payload or default
                leverage = payload.leverage or getattr(self.settings, 'BINANCE_FUTURES_LEVERAGE', 1)
                
                # Apply ADV cap
                adv_cap_result = await self.risk_manager.apply_adv_cap(
                    symbol=payload.symbol,
                    broker=broker_name,
                    requested_qty_usd=payload.qty_usdt,
                    leverage=leverage
                )
                
                # Update payload with capped quantity
                if adv_cap_result['was_capped']:
                    capped_qty = adv_cap_result['approved_qty_usd']
                    
                    logger.warning(
                        f"ADV CAP APPLIED: {payload.symbol} - "
                        f"Original: ${original_qty_usdt:,.0f} → Capped: ${capped_qty:,.0f} | "
                        f"{adv_cap_result['reason']}"
                    )
                    
                    # Check if capped amount is below minimum
                    min_position_size = getattr(self.settings, 'MIN_POSITION_SIZE_USD', 10.0)
                    allow_below_min = getattr(self.settings, 'ALLOW_ADV_CAP_BELOW_MINIMUM', False)
                    
                    if capped_qty < min_position_size:
                        if not allow_below_min:
                            # Reject trade if below minimum and not allowed
                            error_msg = (
                                f"ADV Cap reduced position below minimum: "
                                f"${capped_qty:.2f} < ${min_position_size} minimum. "
                                f"Trade rejected. (Set ALLOW_ADV_CAP_BELOW_MINIMUM=true to allow)"
                            )
                            logger.error(error_msg)
                            
                            return OrderResponse(
                                status="rejected",
                                client_order_id=payload.get_client_order_id(),
                                symbol=payload.symbol,
                                side=payload.get_side(),
                                quantity=0,
                                timestamp=datetime.utcnow().isoformat(),
                                broker=broker_name,
                                strategy=payload.strategy,
                                error=error_msg
                            )
                        else:
                            # Allow trade below minimum (user explicitly enabled this)
                            logger.warning(
                                f"ADV Cap below minimum but ALLOW_ADV_CAP_BELOW_MINIMUM=true: "
                                f"Executing ${capped_qty:.2f} (min: ${min_position_size})"
                            )
                    
                    payload.qty_usdt = capped_qty
                else:
                    logger.info(f"ADV Check: {payload.symbol} - {adv_cap_result['reason']}")
            
            # Check if this is Binance or Coinbase Futures - use safety wrapper
            if broker_name in ['binance_futures', 'coinbase_futures'] or is_multi_account:
                # Determine broker key for safety wrapper lookup
                if broker_name == 'coinbase_futures':
                    if strategy_name.lower().endswith('_1x'):
                        broker_key = 'coinbase_futures_1x'
                    elif strategy_name.lower().endswith('_3x'):
                        broker_key = 'coinbase_futures_3x'
                    elif strategy_name.lower().endswith('_5x'):
                        broker_key = 'coinbase_futures_5x'
                    else:
                        broker_key = 'coinbase_futures'
                else:  # binance_futures
                    if strategy_name.lower().endswith('_1x'):
                        broker_key = 'binance_futures_1x'
                    elif strategy_name.lower().endswith('_2x'):
                        broker_key = 'binance_futures_2x'
                    elif strategy_name.lower().endswith('_3x'):
                        broker_key = 'binance_futures_3x'
                    elif strategy_name.lower().endswith('_5x'):
                        broker_key = 'binance_futures_5x'
                    else:
                        broker_key = 'binance_futures'
                
                # Use safety-wrapped execution for Binance/Coinbase Futures
                logger.info(f"🛡️ Using safety-wrapped execution (retry, verify, auto-close)")
                response = await self._execute_binance_with_safety(
                    payload=payload,
                    broker_key=broker_key,
                    broker=broker
                )
                
                logger.info(f"Order executed with safety features: {response.status}")
                return response
            
            else:
                # For other brokers (Base DEX, etc.), use direct execution
                logger.info(f"Using direct execution for {broker_name}")
                
                # Build signal dict for broker
                signal = {
                    'action': payload.action,
                    'symbol': payload.symbol,
                    'qty_pct': payload.qty_pct,
                    'qty_usdt': payload.qty_usdt,
                    'qty_units': payload.qty_units,
                    'tp_pct': payload.tp_pct,
                    'sl_pct': payload.sl_pct,
                    'price': payload.price
                }
                
                # Execute on broker
                result = await broker.execute_signal(signal)
                
                # Build response
                response = OrderResponse(
                    status="success",
                    order_id=result.get('tx_hash') or result.get('order_id'),
                    client_order_id=payload.get_client_order_id(),
                    symbol=payload.symbol,
                    side=result.get('side', payload.get_side()),
                    quantity=result.get('amount_out', result.get('quantity', 0)),
                    fill_price=result.get('price'),
                    commission=result.get('gas_cost_usd', 0),
                    timestamp=datetime.utcnow().isoformat(),
                    broker=broker_name,
                    strategy=payload.strategy
                )
                
                logger.info(f"Order executed successfully: {response.order_id}")
                
                # TODO: Save to database
                
                return response
            
        except Exception as e:
            logger.error(f"Order execution failed: {str(e)}", exc_info=True)
            
            # Return failed response
            return OrderResponse(
                status="failed",
                client_order_id=payload.get_client_order_id(),
                symbol=payload.symbol,
                side=payload.get_side(),
                quantity=0,
                timestamp=datetime.utcnow().isoformat(),
                broker=payload.broker,
                strategy=payload.strategy,
                error=str(e)
            )
    
    async def get_all_positions(self) -> List[Dict]:
        """
        Get all open positions across all brokers
        
        Returns:
            List of position dicts
        """
        positions = []
        
        for broker_name, broker in self.brokers.items():
            try:
                if hasattr(broker, 'broker') and hasattr(broker.broker, 'get_position'):
                    pos = await broker.broker.get_position()
                    pos['broker'] = broker_name
                    positions.append(pos)
            except Exception as e:
                logger.error(f"Error getting position from {broker_name}: {str(e)}")
        
        return positions
    
    async def get_statistics(self) -> Dict:
        """
        Get trading statistics
        
        Returns:
            Stats dict with daily/weekly/monthly metrics
        """
        # TODO: Implement statistics tracking
        return {
            'today': {'trades': 0, 'pnl': 0},
            'week': {'trades': 0, 'pnl': 0},
            'month': {'trades': 0, 'pnl': 0}
        }
    
    async def close_all_positions(self) -> List[Dict]:
        """
        Close all open positions across all brokers
        
        Returns:
            List of close results
        """
        results = []
        
        for broker_name, broker in self.brokers.items():
            try:
                # Get current position
                if hasattr(broker, 'broker') and hasattr(broker.broker, 'get_position'):
                    pos = await broker.broker.get_position()
                    
                    # If has position, close it
                    if pos.get('side') != 'NONE' and pos.get('quantity', 0) > 0:
                        close_signal = {
                            'action': 'close',
                            'symbol': pos['symbol']
                        }
                        result = await broker.execute_signal(close_signal)
                        results.append({
                            'broker': broker_name,
                            'result': result
                        })
            except Exception as e:
                logger.error(f"Error closing {broker_name} position: {str(e)}")
                results.append({
                    'broker': broker_name,
                    'error': str(e)
                })
        
        return results

