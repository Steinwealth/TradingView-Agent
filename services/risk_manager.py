"""
TradingView Agent - Risk Manager Service
Enforces risk limits and safety checks
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
from models.webhook_payload import WebhookPayload
from services.volume_fetcher import VolumeFetcher
from config.risk_limits import get_adv_limits

# Import config for MACD filter settings
try:
    from config.settings import CONFIG
except ImportError:
    CONFIG = None

logger = logging.getLogger(__name__)


class RiskManager:
    """
    Enforces risk limits and safety checks before trade execution
    Includes ADV Cap (Slip Guard) to minimize slippage
    """
    
    def __init__(self, risk_limits: Dict, settings=None, pnl_tracker=None):
        self.risk_limits = risk_limits
        self.settings = settings
        self.pnl_tracker = pnl_tracker
        initial_flag = True
        if settings and hasattr(settings, "ENABLE_TRADING"):
            initial_flag = bool(settings.ENABLE_TRADING)
        self.trading_enabled = initial_flag
        self.daily_losses = {}
        self.trade_count_today = 0
        self.last_reset = datetime.utcnow().date()
        
        # Initialize volume fetcher for ADV cap
        self.volume_fetcher = VolumeFetcher(settings) if settings else None
        
        # ADV cap tracking
        self.adv_cap_adjustments = 0  # Count how many orders were capped
        
        # Liquidation risk tracking
        self.auto_halt_on_high_risk = True  # Auto-halt if liquidation risk is critical
        
        logger.info(f"RiskManager initialized with limits: {risk_limits}")
        
        if settings:
            adv_enabled = getattr(settings, 'ENABLE_ADV_CAP', False)
            adv_pct = getattr(settings, 'ADV_CAP_PCT', 1.0)
            logger.info(f"ADV Cap (Slip Guard): {'ENABLED' if adv_enabled else 'DISABLED'} - Max {adv_pct}% of ADV")
        
        # Log MACD filter status
        if CONFIG and hasattr(CONFIG, 'risk_management') and CONFIG.risk_management:
            rm = CONFIG.risk_management
            if hasattr(rm, 'macd_filters') and rm.macd_filters:
                logger.info(
                    f"MACD Entry Filters: {'ENABLED' if rm.macd_filters.enabled else 'DISABLED'} - "
                    f"LONG block < {rm.macd_filters.long_block_macd_below:.2f}, "
                    f"SHORT block < {rm.macd_filters.short_block_macd_below:.2f}"
                )
        
        if pnl_tracker:
            logger.info("RiskManager: P&L tracker integrated for liquidation monitoring")
    
    async def validate_trade(self, payload: WebhookPayload) -> Dict:
        """
        Validate trade against risk limits
        
        Args:
            payload: Webhook payload to validate
        
        Returns:
            {'allowed': True/False, 'reason': 'string'}
        """
        try:
            # Reset daily counters if new day
            await self._check_daily_reset()
            
            # CRITICAL: Exit signals should ALWAYS be allowed
            # This ensures open positions can be closed regardless of risk limits
            if payload.is_exit():
                logger.info(f"✅ Exit signal - bypassing all risk checks")
                return {'allowed': True, 'reason': 'Exit signal - always allowed'}
            
            # 2. Check heartbeat (if action is heartbeat, always allow)
            if payload.is_heartbeat():
                return {'allowed': True, 'reason': 'Heartbeat'}
            
            # 1. Check if trading is enabled (only for entries)
            if not self.trading_enabled:
                return {
                    'allowed': False,
                    'reason': 'Trading disabled by kill switch'
                }
            
            # 3. Check daily loss limit
            daily_loss = sum(self.daily_losses.values())
            max_daily_loss = self.risk_limits.get('max_daily_loss_usd', 500)
            
            if abs(daily_loss) > max_daily_loss:
                return {
                    'allowed': False,
                    'reason': f'Daily loss limit exceeded: ${abs(daily_loss):.2f} > ${max_daily_loss}'
                }
            
            # 4. Check max trades per day
            max_trades_per_day = self.risk_limits.get('max_trades_per_day', 100)
            
            if self.trade_count_today >= max_trades_per_day:
                return {
                    'allowed': False,
                    'reason': f'Max trades per day exceeded: {self.trade_count_today} >= {max_trades_per_day}'
                }
            
            # 5. Check leverage limit
            max_leverage = self.risk_limits.get('max_leverage', 5)
            
            if payload.leverage and payload.leverage > max_leverage:
                return {
                    'allowed': False,
                    'reason': f'Leverage too high: {payload.leverage}x > {max_leverage}x'
                }
            
            # 6. Check minimum order size
            min_order_usd = self.risk_limits.get('min_order_usd', 10)
            
            if payload.qty_usdt and payload.qty_usdt < min_order_usd:
                return {
                    'allowed': False,
                    'reason': f'Order too small: ${payload.qty_usdt} < ${min_order_usd}'
                }
            
            # 7. Check liquidation risk if pnl_tracker available
            if self.pnl_tracker and self.auto_halt_on_high_risk and payload.side in ['buy', 'sell']:
                try:
                    # Get current prices (simplified - just use entry price as estimate)
                    current_prices = {}
                    should_halt, halt_reason = await self.pnl_tracker.should_halt_trading(current_prices)
                    
                    if should_halt:
                        logger.critical(f"AUTO-HALT TRIGGERED: {halt_reason}")
                        await self.disable_trading()
                        return {
                            'allowed': False,
                            'reason': f'Auto-halted for safety: {halt_reason}'
                        }
                except Exception as e:
                    logger.error(f"Liquidation risk check error: {str(e)}")
                    # Don't block trade on error
            
            # 8. Check MACD Entry Filters (Dec 2025)
            # Based on analysis: MACD Histogram is the biggest differentiator for entry quality
            # Winners average +0.82 vs Losers -4.40 (difference +5.22)
            if payload.side in ['buy', 'sell']:  # Only check entries, not exits
                macd_check = await self._check_macd_filters(payload)
                if not macd_check['allowed']:
                    return macd_check
            
            # All checks passed
            self.trade_count_today += 1
            
            return {
                'allowed': True,
                'reason': 'All risk checks passed'
            }
            
        except Exception as e:
            logger.error(f"Risk validation error: {str(e)}")
            return {
                'allowed': False,
                'reason': f'Risk check error: {str(e)}'
            }
    
    async def record_trade_result(self, pnl: float):
        """
        Record trade result for daily loss tracking
        
        Args:
            pnl: Profit/loss in USD
        """
        today = datetime.utcnow().date()
        
        if today not in self.daily_losses:
            self.daily_losses[today] = 0
        
        self.daily_losses[today] += pnl
        
        logger.info(f"Trade result recorded: ${pnl:.2f}, Daily total: ${self.daily_losses[today]:.2f}")
    
    async def disable_trading(self, *, reason: Optional[str] = None, critical: bool = False):
        """Disable all trading (kill switch or manual pause)"""
        self.trading_enabled = False
        message = reason or "Trading disabled"
        if critical:
            logger.critical(message)
        else:
            logger.info(message)
    
    async def enable_trading(self):
        """Enable trading (resume after kill switch)"""
        self.trading_enabled = True
        logger.info("TRADING ENABLED - Trading resumed")
    
    async def _check_daily_reset(self):
        """Reset daily counters if new day"""
        today = datetime.utcnow().date()
        
        if today != self.last_reset:
            logger.info(f"New day - resetting daily counters. Previous: {self.trade_count_today} trades, ${sum(self.daily_losses.values()):.2f} PnL, ADV caps: {self.adv_cap_adjustments}")
            self.trade_count_today = 0
            self.adv_cap_adjustments = 0
            self.last_reset = today
            
            # Keep only last 7 days of history
            cutoff = today - timedelta(days=7)
            self.daily_losses = {
                date: pnl for date, pnl in self.daily_losses.items()
                if date >= cutoff
            }
    
    async def apply_adv_cap(
        self,
        symbol: str,
        broker: str,
        requested_qty_usd: float,
        leverage: int = 1,
        current_exposure_usd: float = 0.0
    ) -> Dict:
        """
        Apply ADV (Average Daily Volume) cap to position size (Slip Guard)
        
        Limits position size to a percentage of Average Daily Volume
        to minimize slippage and ensure orders can be filled.
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            broker: Broker name (e.g., "binance_futures")
            requested_qty_usd: Requested position size in USD
            leverage: Leverage multiplier (default 1)
        
        Returns:
            {
                'approved_qty_usd': float,  # Final approved quantity
                'was_capped': bool,         # Whether order was reduced
                'original_qty_usd': float,  # Original requested quantity
                'adv_usd': float,           # Average Daily Volume
                'max_allowed_usd': float,   # Max allowed based on ADV
                'reason': str               # Explanation
            }
        """
        try:
            # Check if ADV cap is enabled
            if not self.settings or not getattr(self.settings, 'ENABLE_ADV_CAP', False):
                return {
                    'approved_qty_usd': requested_qty_usd,
                    'was_capped': False,
                    'original_qty_usd': requested_qty_usd,
                    'adv_usd': None,
                    'max_allowed_usd': None,
                    'approved_notional_usd': requested_qty_usd * max(leverage, 1),
                    'reason': 'ADV cap disabled'
                }
            
            # Get ADV cap settings
            adv_cap_pct = getattr(self.settings, 'ADV_CAP_PCT', 1.0)
            adv_lookback_days = getattr(self.settings, 'ADV_LOOKBACK_DAYS', 7)
            adv_min_volume = getattr(self.settings, 'ADV_MIN_VOLUME_USD', 100000)
            
            # Get symbol-specific ADV limits
            adv_limits = get_adv_limits(symbol)
            symbol_max_pct = adv_limits.get('max_position_pct_of_adv', adv_cap_pct)
            
            # Fetch ADV from volume fetcher
            if not self.volume_fetcher:
                logger.warning("Volume fetcher not initialized, skipping ADV cap")
                return {
                    'approved_qty_usd': requested_qty_usd,
                    'was_capped': False,
                    'original_qty_usd': requested_qty_usd,
                    'adv_usd': None,
                    'max_allowed_usd': None,
                    'reason': 'Volume fetcher unavailable'
                }
            
            adv_usd = await self.volume_fetcher.get_adv_usd(
                symbol=symbol,
                broker=broker,
                lookback_days=adv_lookback_days
            )
            
            if adv_usd is None:
                logger.warning(f"Could not fetch ADV for {symbol}, allowing trade without ADV cap")
                return {
                    'approved_qty_usd': requested_qty_usd,
                    'was_capped': False,
                    'original_qty_usd': requested_qty_usd,
                    'adv_usd': None,
                    'max_allowed_usd': None,
                    'approved_notional_usd': requested_qty_usd * max(leverage, 1),
                    'reason': 'ADV data unavailable'
                }
            
            # Check minimum volume requirement
            if adv_usd < adv_min_volume:
                logger.warning(f"ADV for {symbol} (${adv_usd:,.0f}) below minimum (${adv_min_volume:,.0f})")
                return {
                    'approved_qty_usd': 0,
                    'was_capped': True,
                    'original_qty_usd': requested_qty_usd,
                    'adv_usd': adv_usd,
                    'max_allowed_usd': 0,
                    'approved_notional_usd': 0,
                    'reason': f'Insufficient volume: ${adv_usd:,.0f} < ${adv_min_volume:,.0f} min'
                }
            
            # Calculate max allowed position size
            # Use the more conservative of global or symbol-specific cap
            effective_cap_pct = min(adv_cap_pct, symbol_max_pct)
            max_allowed_usd = adv_usd * (effective_cap_pct / 100.0)
            
            # Account for leverage (notional value vs collateral)
            # If 3x leverage, $1000 collateral = $3000 notional position
            # So we check notional position against ADV
            effective_leverage = leverage if leverage else 1
            requested_notional = requested_qty_usd * effective_leverage
            current_exposure_usd = max(current_exposure_usd, 0.0)
            remaining_capacity = max_allowed_usd - current_exposure_usd
            
            if remaining_capacity <= 0:
                reason = (
                    f"ADV cap reached: current exposure ${current_exposure_usd:,.0f} "
                    f"meets {effective_cap_pct:.2f}% of ${adv_usd:,.0f} ADV"
                )
                logger.warning(reason)
                return {
                    'approved_qty_usd': 0,
                    'approved_notional_usd': 0,
                    'was_capped': True,
                    'original_qty_usd': requested_qty_usd,
                    'adv_usd': adv_usd,
                    'max_allowed_usd': max_allowed_usd,
                    'reason': reason
                }
            
            allowed_notional = min(requested_notional, remaining_capacity)
            was_capped = allowed_notional < requested_notional
            approved_qty_usd = allowed_notional / effective_leverage
            
            # Check if order needs to be capped
            if was_capped:
                self.adv_cap_adjustments += 1
                logger.warning(
                    "ADV CAP APPLIED: %s - Requested: $%s @ %sx "
                    "(notional $%s) | Remaining capacity: $%s | ADV: $%s (%s%% cap)",
                    symbol,
                    f"{requested_qty_usd:,.0f}",
                    effective_leverage,
                    f"{requested_notional:,.0f}",
                    f"{remaining_capacity:,.0f}",
                    f"{adv_usd:,.0f}",
                    f"{effective_cap_pct:.2f}"
                )
                reason = (
                    f"Capped to remaining ADV capacity ${allowed_notional:,.0f} "
                    f"({effective_cap_pct:.2f}% of ${adv_usd:,.0f} ADV)"
                )
            else:
                reason = (
                    f"Within ADV limit: exposure after trade ${current_exposure_usd + allowed_notional:,.0f} "
                    f"<= ${max_allowed_usd:,.0f} ({effective_cap_pct:.2f}% of ${adv_usd:,.0f} ADV)"
                )
            
            return {
                'approved_qty_usd': approved_qty_usd,
                'approved_notional_usd': allowed_notional,
                'was_capped': was_capped,
                'original_qty_usd': requested_qty_usd,
                'adv_usd': adv_usd,
                'max_allowed_usd': max_allowed_usd,
                'reason': reason
            }
                
        except Exception as e:
            logger.error(f"Error applying ADV cap: {str(e)}")
            # On error, allow the trade but log the issue
            return {
                'approved_qty_usd': requested_qty_usd,
                'was_capped': False,
                'original_qty_usd': requested_qty_usd,
                'adv_usd': None,
                'max_allowed_usd': None,
                'approved_notional_usd': requested_qty_usd * max(leverage, 1),
                'reason': f'ADV cap error: {str(e)}'
            }
    
    async def _check_macd_filters(self, payload: WebhookPayload) -> Dict:
        """
        Check MACD entry filters based on Historical Enhancer data
        
        Filters:
        - LONG: Block if MACD Histogram < 0 (negative)
        - SHORT: Block if MACD Histogram < -20 (overextended)
        
        Args:
            payload: Webhook payload with potential historical_enhancer data
        
        Returns:
            {'allowed': True/False, 'reason': 'string'}
        """
        try:
            # Get MACD filter config from CONFIG
            macd_filters_enabled = False
            long_threshold = 0.0
            short_threshold = -20.0
            
            # Check if config has risk_management.macd_filters
            if CONFIG and hasattr(CONFIG, 'risk_management') and CONFIG.risk_management:
                rm = CONFIG.risk_management
                if hasattr(rm, 'macd_filters') and rm.macd_filters:
                    macd_filters_enabled = rm.macd_filters.enabled
                    long_threshold = rm.macd_filters.long_block_macd_below
                    short_threshold = rm.macd_filters.short_block_macd_below
            
            # If MACD filters disabled, allow trade
            if not macd_filters_enabled:
                return {'allowed': True, 'reason': 'MACD filters disabled'}
            
            # Check if historical_enhancer data is present
            if not payload.historical_enhancer:
                # No MACD data available - allow trade but log
                logger.debug(f"MACD filter check skipped: no historical_enhancer data for {payload.strategy_id}")
                return {'allowed': True, 'reason': 'MACD data not available (allowing trade)'}
            
            # Extract MACD Histogram value
            # Historical Enhancer may use different field names, try common ones
            macd_hist = None
            if isinstance(payload.historical_enhancer, dict):
                # Try common field names for MACD Histogram
                macd_hist = payload.historical_enhancer.get('macd_histogram') or \
                           payload.historical_enhancer.get('macd_hist') or \
                           payload.historical_enhancer.get('macdHistogram') or \
                           payload.historical_enhancer.get('macd')
            
            # If MACD value not available, allow trade
            if macd_hist is None:
                logger.debug(f"MACD filter check skipped: MACD value not found in historical_enhancer for {payload.strategy_id}")
                return {'allowed': True, 'reason': 'MACD value not available (allowing trade)'}
            
            # Validate MACD value is numeric
            try:
                macd_hist = float(macd_hist)
            except (ValueError, TypeError):
                logger.warning(f"MACD filter check skipped: invalid MACD value '{macd_hist}' for {payload.strategy_id}")
                return {'allowed': True, 'reason': 'Invalid MACD value (allowing trade)'}
            
            # Apply filters based on entry side
            if payload.side == 'buy':  # LONG entry
                if macd_hist < long_threshold:
                    logger.info(
                        f"⛔ MACD Filter BLOCKED LONG entry: {payload.strategy_id} - "
                        f"MACD {macd_hist:.2f} < {long_threshold:.2f} threshold"
                    )
                    return {
                        'allowed': False,
                        'reason': f'MACD filter blocked LONG: MACD {macd_hist:.2f} < {long_threshold:.2f} (negative/weak momentum)'
                    }
                else:
                    logger.debug(f"✅ MACD Filter PASSED LONG entry: {payload.strategy_id} - MACD {macd_hist:.2f} >= {long_threshold:.2f}")
            
            elif payload.side == 'sell':  # SHORT entry
                if macd_hist < short_threshold:
                    logger.info(
                        f"⛔ MACD Filter BLOCKED SHORT entry: {payload.strategy_id} - "
                        f"MACD {macd_hist:.2f} < {short_threshold:.2f} threshold (overextended)"
                    )
                    return {
                        'allowed': False,
                        'reason': f'MACD filter blocked SHORT: MACD {macd_hist:.2f} < {short_threshold:.2f} (overextended)'
                    }
                else:
                    logger.debug(f"✅ MACD Filter PASSED SHORT entry: {payload.strategy_id} - MACD {macd_hist:.2f} >= {short_threshold:.2f}")
            
            # Filter passed
            return {'allowed': True, 'reason': f'MACD filter passed: MACD {macd_hist:.2f}'}
            
        except Exception as e:
            logger.error(f"MACD filter check error: {str(e)}", exc_info=True)
            # On error, allow trade but log warning
            return {'allowed': True, 'reason': f'MACD filter error (allowing trade): {str(e)}'}
    
    def get_adv_stats(self) -> Dict:
        """Get ADV cap statistics for monitoring"""
        return {
            'adv_cap_enabled': getattr(self.settings, 'ENABLE_ADV_CAP', False) if self.settings else False,
            'adv_cap_pct': getattr(self.settings, 'ADV_CAP_PCT', 1.0) if self.settings else None,
            'adjustments_today': self.adv_cap_adjustments,
            'volume_cache': self.volume_fetcher.get_cache_status() if self.volume_fetcher else None
        }

