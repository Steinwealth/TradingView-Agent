"""
Telegram notification service for real-time alerts and daily P&L summaries.

Provides instant notifications for:
- Order executions (entries, exits)
- Critical failures (SL/TP failures, position mismatches)
- Daily P&L summaries
- Liquidation risk warnings
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from zoneinfo import ZoneInfo
import aiohttp
from config import settings

logger = logging.getLogger(__name__)


def normalize_strategy_label(strategy_id: str) -> str:
    """
    Normalize strategy IDs to user-friendly labels for display.
    
    Converts:
    - ichimoku-coinbase-btc-1h -> Easy Ichimoku Coinbase BTC 1h
    - ichimoku-coinbase-eth-1h -> Easy Ichimoku Coinbase ETH 1h
    - ichimoku-coinbase-sol-1h -> Easy Ichimoku Coinbase SOL 1h
    - ichimoku-coinbase-xrp-1h -> Easy Ichimoku Coinbase XRP 1h
    - ichimoku-coinbase-btc-5m -> Easy Ichimoku Coinbase BTC 5m
    - ichimoku-coinbase-eth-5m -> Easy Ichimoku Coinbase ETH 5m
    - ichimoku-coinbase-sol-5m -> Easy Ichimoku Coinbase SOL 5m
    - ichimoku-coinbase-xrp-5m -> Easy Ichimoku Coinbase XRP 5m
    """
    if not strategy_id:
        return strategy_id
    
    strategy_lower = strategy_id.lower()
    
    # Map strategy IDs to user-friendly labels
    label_map = {
        'ichimoku-coinbase-btc-1h': 'Easy Ichimoku Coinbase BTC 1h',
        'ichimoku-coinbase-eth-1h': 'Easy Ichimoku Coinbase ETH 1h',
        'ichimoku-coinbase-sol-1h': 'Easy Ichimoku Coinbase SOL 1h',
        'ichimoku-coinbase-xrp-1h': 'Easy Ichimoku Coinbase XRP 1h',
        'ichimoku-coinbase-btc-5m': 'Easy Ichimoku Coinbase BTC 5m',
        'ichimoku-coinbase-eth-5m': 'Easy Ichimoku Coinbase ETH 5m',
        'ichimoku-coinbase-sol-5m': 'Easy Ichimoku Coinbase SOL 5m',
        'ichimoku-coinbase-xrp-5m': 'Easy Ichimoku Coinbase XRP 5m',
    }
    
    return label_map.get(strategy_lower, strategy_id)


def normalize_symbol_for_display(symbol: str) -> str:
    """
    Normalize Coinbase product IDs to standard symbol format for display.
    
    Converts:
    - ETP-CBSE -> ETH-PERP
    - BIP-CBSE -> BTC-PERP
    - SLP-CBSE -> SOL-PERP
    - XPP-CBSE -> XRP-PERP
    
    Also handles other variations:
    - ETP -> ETH-PERP
    - BIP -> BTC-PERP
    - SLP -> SOL-PERP
    - XPP -> XRP-PERP
    """
    if not symbol:
        return symbol
    
    symbol_upper = symbol.upper()
    
    # Map Coinbase product IDs to standard format
    symbol_map = {
        'ETP-CBSE': 'ETH-PERP',
        'BIP-CBSE': 'BTC-PERP',
        'SLP-CBSE': 'SOL-PERP',
        'XPP-CBSE': 'XRP-PERP',
        'ETP': 'ETH-PERP',
        'BIP': 'BTC-PERP',
        'SLP': 'SOL-PERP',
        'XPP': 'XRP-PERP',
    }
    
    # Check exact match first
    if symbol_upper in symbol_map:
        return symbol_map[symbol_upper]
    
    # Check if it contains Coinbase product ID
    for coinbase_id, standard_symbol in symbol_map.items():
        if coinbase_id in symbol_upper:
            return standard_symbol
    
    # Return original if no mapping found
    return symbol


def get_price_precision(symbol: str) -> int:
    """
    Get price precision for symbol (4 decimals for XRP, 2 for others).
    
    Args:
        symbol: Trading symbol (e.g., "XRP-PERP", "BTC-PERP", "XPP-CBSE")
    
    Returns:
        Number of decimal places (2 or 4)
    """
    symbol_upper = symbol.upper()
    if "XRP" in symbol_upper or "XPP" in symbol_upper:
        return 4
    return 2


def format_price(price: float, symbol: str) -> str:
    """
    Format price with symbol-specific precision.
    
    Args:
        price: Price value
        symbol: Trading symbol
    
    Returns:
        Formatted price string (e.g., "$2.0524" for XRP, "$91,234.56" for BTC)
    """
    precision = get_price_precision(symbol)
    return f"${price:,.{precision}f}"


def format_exit_reason(reason: str) -> str:
    """
    Format exit reason for display in alerts.
    
    Converts:
    - RSI_EXIT_LONG -> RSI Exit (Long)
    - RSI_EXIT_SHORT -> RSI Exit (Short)
    - BREAKEVEN_STOP -> Breakeven Stop
    - TRAILING_STOP -> Trailing Stop
    - STOP_LOSS -> Stop Loss
    - TAKE_PROFIT -> Take Profit
    - EOD_AUTO_CLOSE -> EOD Auto-Close
    """
    if not reason:
        return reason
    
    reason_upper = reason.upper()
    
    # Map exit reasons to user-friendly format
    reason_map = {
        'RSI_EXIT_LONG': 'RSI Exit (Long)',
        'RSI_EXIT_SHORT': 'RSI Exit (Short)',
        'BREAKEVEN_STOP': 'Breakeven Stop',
        'BREAKEVEN_3H': '3-Hour Breakeven Exit',
        'BREAKEVEN_6H': '6-Hour Breakeven Exit',
        'TRAILING_STOP': 'Trailing Stop',
        'STOP_LOSS': 'Stop Loss',
        'TAKE_PROFIT': 'Take Profit',
        'EOD_AUTO_CLOSE': 'EOD Auto-Close',
        'AFTERHOURS_AUTO_CLOSE': 'Afterhours Auto-Close',
    }
    
    return reason_map.get(reason_upper, reason.replace('_', ' ').title())


class TelegramNotifier:
    """
    Telegram notification service using Bot API.
    
    Setup:
        1. Create Telegram bot via @BotFather
        2. Get bot token
        3. Start chat with your bot
        4. Get your chat ID (use @userinfobot)
        5. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env
    """
    
    def __init__(self):
        self.bot_token = settings.TELEGRAM_BOT_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.enabled = bool(self.bot_token and self.chat_id)
        
        try:
            self.local_timezone = settings.LOCAL_TIMEZONE_INFO
        except AttributeError:
            self.local_timezone = timezone.utc
            logger.warning("LOCAL_TIMEZONE_INFO not set in settings; defaulting to UTC for Telegram alerts.")

        if not self.enabled:
            logger.warning(
                "Telegram notifications disabled. Set TELEGRAM_BOT_TOKEN and "
                "TELEGRAM_CHAT_ID to enable."
            )
        else:
            logger.info("✅ Telegram notifications enabled")

    @staticmethod
    def _timestamp() -> str:
        """Return a standardized UTC timestamp string."""
        return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

    @staticmethod
    def _format_retry_eta(seconds: Optional[float]) -> str:
        if seconds is None or seconds <= 0:
            return "immediately"
        total_seconds = int(round(seconds))
        minutes, secs = divmod(total_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        parts: List[str] = []
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}m")
        if not parts or secs:
            parts.append(f"{secs}s")
        return " ".join(parts)

    async def send_alert(
        self,
        message: str,
        *,
        title: Optional[str] = None,
        priority: str = "info"
    ) -> bool:
        """Generic helper for sending informational alerts."""
        header = f"<b>{title}</b>\n\n" if title else ""
        body = f"{header}{message.strip()}" if message else header.rstrip()
        return await self.send_message(body, priority=priority, auto_prefix=False)

    async def send_signal_skipped_alert(
        self,
        *,
        account_label: str,
        action: str,
        symbol: str,
        mode: Optional[str],
        strategy: Optional[str] = None,
        position_side: Optional[str] = None,
        reason: Optional[str] = None,
        skipped_partitions: Optional[List[Dict[str, Any]]] = None,
        note: Optional[str] = None
    ) -> bool:
        """Notify when a signal is skipped (e.g., capital exhausted, ADV cap, etc.)."""
        # Normalize symbol for display
        symbol = normalize_symbol_for_display(symbol)
        
        if not self.enabled:
            return False

        action_upper = action.upper()
        now_utc = datetime.now(timezone.utc)
        local_dt = now_utc.astimezone(self.local_timezone)
        eastern_dt = now_utc.astimezone(ZoneInfo("America/New_York"))
        local_label = local_dt.tzname() or "Local"
        eastern_label = eastern_dt.tzname() or "ET"
        time_line = (
            f"{local_dt.strftime('%I:%M %p')} {local_label}"
            f" ({eastern_dt.strftime('%I:%M %p')} {eastern_label})"
        )

        mode_text = f"{mode.upper()} Mode" if mode else "Mode Unknown"

        icon = "🟢" if action_upper == "BUY" else "🔴" if action_upper == "SELL" else "⚪️"
        side_text = position_side.title() if position_side else ""
        order_parts: List[str] = [icon, action_upper]
        if side_text:
            order_parts.append(side_text)
        if symbol:
            order_parts.append(f"<b>{symbol}</b>")

        lines = [
            "====================================================================",
            "",
            f"⏭️ <b>SIGNAL SKIPPED</b> | {mode_text}",
            f"          Time: {time_line}",
            "",
            f"Account: {account_label}",
        ]

        if strategy:
            lines.append(f"Strategy: {strategy}")

        lines.append("")
        lines.append(f"Signal: {' '.join(order_parts)}")
        lines.append("")
        
        # Show reason (clean, no partition details)
        if reason:
            lines.append(f"Reason: {reason}")
        lines.append("")
        lines.append("Trade Skipped!")

        return await self.send_message("\n".join(lines), priority="warning", auto_prefix=False)

    async def send_critical_failure(self, message: str, *, title: Optional[str] = None) -> bool:
        """Send a critical alert to Telegram."""
        lines = [
            "====================================================================",
            "",
            f"🚨 <b>{title or 'CRITICAL ALERT'}</b>",
            "",
            message.strip(),
            "",
            f"⏰ {self._timestamp()}",
        ]
        return await self.send_message("\n".join(lines), priority="critical", auto_prefix=False)

    async def send_execution_alert(
        self,
        *,
        account_label: str,
        action: str,
        symbol: str,
        quantity: float,
        price: float,
        leverage: Optional[int],
        mode: Optional[str] = None,
        strategy: Optional[str] = None,
        position_side: Optional[str] = None,
        partition_id: Optional[str] = None,
        partition_allocation: Optional[float] = None,
        partition_summaries: Optional[List[Dict[str, Any]]] = None,
        entry_price: Optional[float] = None,
        exit_price: Optional[float] = None,
        pnl_usd: Optional[float] = None,
        pnl_pct: Optional[float] = None,
        adv_cap_note: Optional[str] = None,
        capital_deployed: Optional[float] = None,
        capital_capacity: Optional[float] = None,
        account_id: Optional[str] = None,
        include_portfolio_pnl: bool = True,
        total_account_balance: Optional[float] = None,
        source: Optional[str] = None,
        exit_reason: Optional[str] = None
    ) -> bool:
        """Format and send execution alerts (entries/exits)."""
        # Log alert attempt for debugging
        price_precision = get_price_precision(symbol)
        logger.info(f"📱 Attempting to send execution alert: {action} {symbol} (quantity: {quantity}, price: ${price:,.{price_precision}f})")
        
        if not self.enabled:
            logger.warning(
                f"⚠️ Telegram notifications disabled - execution alert NOT sent "
                f"for {action} {symbol} (check TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)"
            )
            return False

        try:
            action_upper = action.upper()
            is_exit = action_upper == "EXIT"
            now_utc = datetime.now(timezone.utc)
            local_dt = now_utc.astimezone(self.local_timezone)
            eastern_dt = now_utc.astimezone(ZoneInfo("America/New_York"))

            # Normalize symbol for display
            symbol = normalize_symbol_for_display(symbol)
            
            summaries = partition_summaries or []
            if not summaries and partition_id:
                summaries = [{
                    'id': partition_id,
                    'allocation_pct': partition_allocation,
                    'quantity': quantity,
                    'price': price,
                    'leverage': leverage,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'pnl_usd': pnl_usd,
                    'pnl_pct': pnl_pct,
                    'position_side': position_side
                }]
            
            # Normalize symbols in partition summaries
            for summary in summaries:
                if 'symbol' in summary:
                    summary['symbol'] = normalize_symbol_for_display(summary['symbol'])

            # Use module-level format_price function for symbol-specific precision
            def fmt_price(price: float, symbol: str) -> str:
                """Format price with symbol-specific precision"""
                return format_price(price, symbol)
            
            def fmt_signed_currency(value: Optional[float]) -> Optional[str]:
                if value is None:
                    return None
                # Format as +$7.06 or -$1.15 (sign before dollar sign)
                sign = "+" if value >= 0 else "-"
                abs_value = abs(value)
                return f"{sign}${abs_value:,.2f}"

            def fmt_signed_pct(value: Optional[float], *, bold: bool = False) -> Optional[str]:
                if value is None:
                    return None
                text = f"{value:+.2f}%"
                return f"<b>{text}</b>" if bold else text

            def format_quantity(value: Optional[float]) -> Optional[str]:
                if value is None:
                    return None
                text = f"{value:.6f}".rstrip('0').rstrip('.')
                return text or f"{value:.6f}"

            def format_duration(seconds: Optional[float]) -> Optional[str]:
                if seconds is None:
                    return None
                minutes = int(seconds // 60)
                hours = minutes // 60
                minutes = minutes % 60
                if hours > 0:
                    return f"{hours}h {minutes}m"
                return f"{minutes}m"

            mode_text = f"{mode.upper()} Mode" if mode else "Mode Unknown"

            if is_exit:
                local_label = local_dt.tzname() or "Local"
                eastern_label = eastern_dt.tzname() or "ET"
                time_line = (
                    f"{local_dt.strftime('%I:%M %p')} {local_label}"
                    f" ({eastern_dt.strftime('%I:%M %p')} {eastern_label})"
                )

                lines: List[str] = [
                    "====================================================================",
                    "",
                    f"📉 <b>POSITION CLOSED</b> | {mode_text}",
                    f"          Time: {time_line}",
                    "",
                    f"Account: {account_label}",
                ]

                if strategy:
                    lines.append(f"Strategy: {strategy}")
                
                # Add Source line
                if source:
                    lines.append(f"Source: {source}")
                else:
                    lines.append(f"Source: TradingView webhook Exit")
                
                # Add Exit Reason line if provided
                if exit_reason:
                    # Format exit reason nicely
                    reason_display = format_exit_reason(exit_reason)
                    lines.append(f"Exit Reason: {reason_display}")

                # Calculate account-level P&L percentage (impact on total account)
                # This shows the true portfolio impact, not just partition-level returns
                account_pnl_pct = None
                if pnl_usd is not None and total_account_balance and total_account_balance > 0:
                    account_pnl_pct = (pnl_usd / total_account_balance) * 100
                
                total_line_parts: List[str] = []
                # Use account-level P&L % if available, otherwise fall back to partition P&L %
                display_pct = account_pnl_pct if account_pnl_pct is not None else pnl_pct
                pct_text = fmt_signed_pct(display_pct, bold=True)
                if pct_text:
                    total_line_parts.append(pct_text)
                currency_text = fmt_signed_currency(pnl_usd)
                if currency_text:
                    total_line_parts.append(currency_text)

                if total_line_parts:
                    lines.extend(["", "Total P&L to Account:", f"  • {' '.join(total_line_parts)}"])
                
                # Count positions by direction for clarity
                long_count = sum(1 for s in summaries if (s.get('position_side') or '').upper() == 'LONG')
                short_count = sum(1 for s in summaries if (s.get('position_side') or '').upper() == 'SHORT')
                total_positions = len(summaries)
                
                # Add summary line if multiple positions or both directions
                if total_positions > 1 or (long_count > 0 and short_count > 0):
                    position_summary = []
                    if long_count > 0:
                        position_summary.append(f"{long_count} Long")
                    if short_count > 0:
                        position_summary.append(f"{short_count} Short")
                    if position_summary:
                        lines.extend(["", f"Positions Closed: {', '.join(position_summary)}"])

                def format_exit_partition(summary: Dict[str, Any]) -> List[str]:
                    pid_display = (summary.get('id') or "Partition").replace('_', '-')
                    partition_lines = [f"{pid_display}:"]

                    pnl_pct_val = summary.get('pnl_pct')
                    pnl_usd_val = summary.get('pnl_usd')
                    pnl_segments: List[str] = []
                    pct_summary = fmt_signed_pct(pnl_pct_val, bold=True) if pnl_pct_val is not None else None
                    if pct_summary:
                        pnl_segments.append(pct_summary)
                    currency_summary = fmt_signed_currency(pnl_usd_val)
                    if currency_summary:
                        pnl_segments.append(currency_summary)
                    if pnl_segments:
                        partition_lines.append(f"  • 🔰 {' '.join(pnl_segments)}")

                    partition_symbol = normalize_symbol_for_display(summary.get('symbol') or symbol)
                    exit_side = summary.get('position_side') or position_side or ""
                    partition_lines.append(f"          {exit_side.title()} Exit • <b>{partition_symbol}</b>")

                    qty_text = format_quantity(summary.get('quantity'))
                    exit_price_val = summary.get('exit_price') or summary.get('price')
                    if qty_text:
                        if exit_price_val is not None:
                            partition_lines.append(f"          {qty_text} @ {fmt_price(exit_price_val, partition_symbol)}")
                        else:
                            partition_lines.append(f"          {qty_text}")

                    capital_used = summary.get('capital_used') or summary.get('virtual_balance_before')
                    if capital_used is not None:
                        partition_lines.append(f"          ${capital_used:,.2f}")

                    entry_val = summary.get('entry_price')
                    if entry_val is not None:
                        partition_lines.append(f"          Entry Price: {fmt_price(entry_val, partition_symbol)}")
                    if exit_price_val is not None:
                        partition_lines.append(f"          Exit Price: {fmt_price(exit_price_val, partition_symbol)}")

                    duration_text = format_duration(summary.get('holding_seconds'))
                    if duration_text:
                        partition_lines.append(f"          Holding Time: {duration_text}")

                    return partition_lines

                for summary in summaries:
                    lines.append("")
                    lines.extend(format_exit_partition(summary))

                if adv_cap_note:
                    lines.extend(["", f"ADV Cap: {adv_cap_note}"])

                message = "\n".join(lines)
                logger.debug(f"📱 Sending exit alert for {symbol} ({len(lines)} lines)")
                result = await self.send_message(message, priority="info", auto_prefix=False)
                if result:
                    logger.info(f"✅ Exit alert sent successfully for {symbol}")
                else:
                    logger.warning(f"⚠️ Exit alert send returned False for {symbol}")
                return result

            # ENTRY (or other non-exit) formatting
            local_label = local_dt.tzname() or "Local"
            eastern_label = eastern_dt.tzname() or "ET"
            time_line = (
                f"{local_dt.strftime('%I:%M %p')} {local_label}"
                f" ({eastern_dt.strftime('%I:%M %p')} {eastern_label})"
            )

            lines = [
                "====================================================================",
                "",
                f"✅ <b>ORDER EXECUTED</b> | {mode_text}",
                f"          Time: {time_line}",
                "",
                f"Account: {account_label}",
            ]

            if strategy:
                lines.append(f"Strategy: {strategy}")
            
            # Add Source line for entries
            if source:
                lines.append(f"Source: {source}")
            else:
                # Determine source based on action
                if action_upper in ["BUY", "SELL"]:
                    lines.append(f"Source: TradingView webhook Entry")
                else:
                    lines.append(f"Source: TradingView webhook")

            deployed = capital_deployed if capital_deployed is not None else sum(
                summary.get('capital_used') or 0.0 for summary in summaries
            )

            def _capacity_from_summary(summary: Dict[str, Any]) -> float:
                for key in ('virtual_balance', 'virtual_balance_before', 'max_allocation', 'capital_capacity'):
                    value = summary.get(key)
                    if value:
                        return float(value)
                return 0.0

            capacity = (
                capital_capacity if capital_capacity is not None
                else sum(_capacity_from_summary(summary) for summary in summaries)
            )

            capital_block: List[str] = []
            if deployed and capacity:
                percent = (deployed / capacity * 100) if capacity > 0 else 0.0
                capital_block = [
                    "",
                    "💼 Capital Deployment:",
                    f"   • Deployed: ${deployed:,.2f}",
                    f"   • / ${capacity:,.2f} ({percent:.0f}%)"
                ]

            icon = "🟢" if action_upper == "BUY" else "🔴"

            def format_entry_partition(summary: Dict[str, Any]) -> List[str]:
                pid_display = (summary.get('id') or partition_id or "Partition").replace('_', '-')
                partition_lines = [f"{pid_display}:"]

                partition_symbol = normalize_symbol_for_display(summary.get('symbol') or symbol)
                partition_side = summary.get('position_side', position_side) or ("LONG" if action_upper == "BUY" else "SHORT")
                partition_lines.append(f"  • {icon} {action_upper} {partition_side.title()} <b>{partition_symbol}</b>")

                qty_text = format_quantity(summary.get('quantity'))
                if qty_text:
                    entry_price_val = summary.get('entry_price') or summary.get('price', price)
                    if entry_price_val is not None:
                        partition_lines.append(f"          {qty_text} @ {fmt_price(entry_price_val, partition_symbol)}")
                    else:
                        partition_lines.append(f"          {qty_text}")

                capital_used = summary.get('capital_used')
                if capital_used is not None:
                    partition_lines.append(f"          ${capital_used:,.2f}")

                return partition_lines

            if summaries:
                for summary in summaries:
                    lines.append("")
                    lines.extend(format_entry_partition(summary))

            if capital_block:
                lines.extend(capital_block)

            if adv_cap_note:
                lines.append("")
                lines.append(f"ADV Cap: {adv_cap_note}")

            # Add portfolio P&L section for entry alerts
            # CRITICAL: If this fails, we should still send the alert without P&L section
            # Portfolio P&L is optional - don't let it block the alert
            # TEMPORARILY DISABLED: Portfolio P&L section may be causing entry alerts to fail
            # Re-enable once the hanging issue is resolved
            portfolio_pnl_enabled = False  # Set to True to re-enable portfolio P&L section
            
            if include_portfolio_pnl and account_id and not is_exit and portfolio_pnl_enabled:
                try:
                    # Add timeout to prevent hanging - portfolio P&L is optional
                    # Use a short timeout to ensure alerts are sent quickly
                    logger.debug(f"📊 Fetching portfolio P&L for entry alert: {symbol} (account: {account_id})")
                    portfolio_pnl_section = await asyncio.wait_for(
                        self._get_portfolio_pnl_section(account_id),
                        timeout=1.5  # 1.5 second timeout - very short to prevent delays
                    )
                    if portfolio_pnl_section:
                        lines.extend(portfolio_pnl_section)
                        logger.debug(f"✅ Added portfolio P&L section to entry alert for {symbol}")
                except asyncio.TimeoutError:
                    logger.warning(f"⚠️ Portfolio P&L section timed out for {account_id} (symbol: {symbol}) - sending alert without it")
                except Exception as e:
                    logger.warning(f"⚠️ Could not add portfolio P&L to alert for {account_id} (symbol: {symbol}): {e} - sending alert without it")
            elif not account_id and not is_exit:
                logger.debug(f"⚠️ account_id not provided for entry alert {symbol} - skipping portfolio P&L section")
            elif not portfolio_pnl_enabled and not is_exit:
                logger.debug(f"ℹ️ Portfolio P&L section disabled for entry alert {symbol} - sending alert without it")

            message = "\n".join(lines)
            logger.debug(f"📱 Sending entry alert for {symbol} ({len(lines)} lines)")
            result = await self.send_message(message, priority="success", auto_prefix=False)
            if result:
                logger.info(f"✅ Entry alert sent successfully for {symbol}")
            else:
                logger.warning(f"⚠️ Entry alert send returned False for {symbol}")
            return result
        
        except Exception as e:
            logger.error(
                f"❌ Failed to format/send execution alert for {action} {symbol}: {e}",
                exc_info=True
            )
            # Try to send a simplified alert as fallback
            try:
                # Helper for fallback (defined outside format_entry_partition scope)
                def get_price_precision_fallback(symbol: str) -> int:
                    symbol_upper = symbol.upper()
                    if "XRP" in symbol_upper or "XPP" in symbol_upper:
                        return 4
                    return 2
                
                price_precision = get_price_precision_fallback(symbol)
                fallback_message = (
                    f"⚠️ <b>Execution Alert (Simplified)</b>\n\n"
                    f"Action: {action}\n"
                    f"Symbol: {symbol}\n"
                    f"Quantity: {quantity}\n"
                    f"Price: ${price:,.{price_precision}f}\n"
                    f"Mode: {mode or 'Unknown'}\n"
                    f"Strategy: {strategy or 'Unknown'}\n\n"
                    f"<i>Full alert formatting failed. See logs for details.</i>"
                )
                return await self.send_message(fallback_message, priority="warning", auto_prefix=False)
            except Exception as fallback_error:
                logger.error(f"❌ Failed to send fallback alert: {fallback_error}")
                return False

    async def send_broker_blocked_alert(
        self,
        *,
        account_label: str,
        symbol: str,
        action: str,
        strategy: Optional[str],
        mode: Optional[str],
        reason: str,
        attempt: int,
        next_retry_seconds: float,
        partition_id: Optional[str] = None
    ) -> bool:
        """
        Notify when the broker blocks an entry/exit order and the agent will retry.
        """
        # Normalize symbol for display
        symbol = normalize_symbol_for_display(symbol)
        
        if not self.enabled:
            return False

        action_upper = action.upper()
        icon = "🚫" if action_upper == "ENTRY" else "🚨"
        now_utc = datetime.now(timezone.utc)
        local_dt = now_utc.astimezone(self.local_timezone)
        eastern_dt = now_utc.astimezone(ZoneInfo("America/New_York"))
        local_label = local_dt.tzname() or "Local"
        eastern_label = eastern_dt.tzname() or "ET"
        time_line = (
            f"{local_dt.strftime('%I:%M %p')} {local_label}"
            f" ({eastern_dt.strftime('%I:%M %p')} {eastern_label})"
        )
        mode_text = f"{mode.upper()} Mode" if mode else "Mode Unknown"
        retry_text = self._format_retry_eta(next_retry_seconds)

        lines = [
            "====================================================================",
            "",
            f"{icon} <b>{action_upper} BLOCKED</b> | {mode_text}",
            f"          Time: {time_line}",
            "",
            f"Account: {account_label}",
        ]

        if strategy:
            lines.append(f"Strategy: {strategy}")
        if partition_id:
            lines.append(f"Partition: {partition_id}")

        lines.extend([
            "",
            f"Symbol: <b>{symbol}</b>",
            f"Reason: {reason.strip() or 'Broker rejected the order'}",
            f"Attempt: {attempt}",
            f"Next Retry: {retry_text}",
            "",
            "Agent will keep retrying automatically until the broker accepts the order.",
            "",
            f"⏰ {self._timestamp()}",
        ])

        priority = "warning" if action_upper == "ENTRY" else "critical"
        return await self.send_message("\n".join(lines), priority=priority, auto_prefix=False)

    async def send_commission_cap_alert(
        self,
        *,
        account_label: str,
        symbol: str,
        strategy: Optional[str],
        partition_id: Optional[str],
        mode: Optional[str],
        commission_usd: float,
        notional_usd: float,
        observed_rate_pct: float,
        max_rate_pct: float
    ) -> bool:
        """
        Notify when actual broker commissions exceed the configured cap and trading is halted.
        """
        # Normalize symbol for display
        symbol = normalize_symbol_for_display(symbol)
        
        if not self.enabled:
            return False

        now_utc = datetime.now(timezone.utc)
        local_dt = now_utc.astimezone(self.local_timezone)
        eastern_dt = now_utc.astimezone(ZoneInfo("America/New_York"))
        local_label = local_dt.tzname() or "Local"
        eastern_label = eastern_dt.tzname() or "ET"
        time_line = (
            f"{local_dt.strftime('%I:%M %p')} {local_label}"
            f" ({eastern_dt.strftime('%I:%M %p')} {eastern_label})"
        )
        mode_text = f"{(mode or 'UNKNOWN').upper()} Mode"

        lines = [
            "====================================================================",
            "",
            "🧾 <b>COMMISSION CAP TRIGGERED</b>",
            f"          Time: {time_line}",
            "",
            f"Account: {account_label}",
        ]

        if strategy:
            lines.append(f"Strategy: {strategy}")
        if partition_id:
            lines.append(f"Partition: {partition_id}")

        lines.extend([
            "",
            f"Symbol: <b>{symbol}</b>",
            f"Mode: {mode_text}",
            "",
            f"Observed Fee: {observed_rate_pct:.4f}% on ${notional_usd:,.2f}",
            f"Commission Paid: ${commission_usd:,.2f}",
            f"Max Allowed: {max_rate_pct:.4f}%",
            "",
            "Action: Trading disabled until reviewed.",
            "",
            "Use /trading/enable (with admin secret) after confirming broker fees.",
            "",
            f"⏰ {self._timestamp()}",
        ])

        return await self.send_message("\n".join(lines), priority="critical", auto_prefix=False)
    
    async def send_message(
        self,
        message: str,
        priority: str = "info",
        parse_mode: str = "HTML",
        *,
        auto_prefix: bool = True
    ) -> bool:
        """
        Send message to Telegram.
        
        Args:
            message: Message text (supports HTML formatting)
            priority: Message priority ("info", "warning", "critical")
            parse_mode: Telegram parse mode ("HTML" or "Markdown")
        
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("Telegram notifier disabled; skipping message send.")
            return False
        
        formatted_message = message
        if auto_prefix:
            emoji_map = {
                "info": "ℹ️",
                "success": "✅",
                "warning": "⚠️",
                "critical": "🚨",
                "error": "❌"
            }
            emoji = emoji_map.get(priority, "ℹ️")
            formatted_message = f"{emoji} {message}"
        
        # Telegram API endpoint
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        
        payload = {
            "chat_id": self.chat_id,
            "text": formatted_message,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }
        
        # Retry logic for Telegram API calls
        max_retries = 3
        retry_delays = [1.0, 2.0, 4.0]  # Exponential backoff: 1s, 2s, 4s
        
        for attempt in range(max_retries):
            try:
                # Create timeout with increased duration (30 seconds)
                timeout = aiohttp.ClientTimeout(total=30.0, connect=10.0)
                
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(url, json=payload) as response:
                        if response.status == 200:
                            if attempt > 0:
                                logger.info(f"✅ Telegram message sent successfully on attempt {attempt + 1}: {message[:100]}")
                            else:
                                logger.debug(f"✅ Telegram message sent: {message[:100]}")
                            return True
                        else:
                            error_text = await response.text()
                            # Don't retry on 4xx errors (client errors like bad request, unauthorized)
                            if 400 <= response.status < 500:
                                logger.error(
                                    f"❌ Telegram API client error (no retry): {response.status} - {error_text[:200]}"
                                )
                                return False
                            
                            # Retry on 5xx errors (server errors) or other errors
                            if attempt < max_retries - 1:
                                delay = retry_delays[attempt]
                                logger.warning(
                                    f"⚠️ Telegram API error {response.status} on attempt {attempt + 1}/{max_retries}, "
                                    f"retrying in {delay}s: {error_text[:200]}"
                                )
                                await asyncio.sleep(delay)
                                continue
                            else:
                                logger.error(
                                    f"❌ Telegram API error after {max_retries} attempts: "
                                    f"{response.status} - {error_text[:200]}"
                                )
                                return False
            
            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    delay = retry_delays[attempt]
                    logger.warning(
                        f"⚠️ Telegram API timeout on attempt {attempt + 1}/{max_retries}, "
                        f"retrying in {delay}s: {message[:100]}"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(
                        f"❌ Telegram API timeout after {max_retries} attempts - message not sent: {message[:100]}"
                    )
                    return False
            except Exception as e:
                if attempt < max_retries - 1:
                    delay = retry_delays[attempt]
                    logger.warning(
                        f"⚠️ Telegram send error on attempt {attempt + 1}/{max_retries}, "
                        f"retrying in {delay}s: {str(e)}"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(
                        f"❌ Failed to send Telegram message after {max_retries} attempts: {str(e)}",
                        exc_info=True
                    )
                    return False
        
        # Should never reach here, but just in case
        return False
    
    async def notify_order_executed(
        self,
        order_type: str,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        leverage: int = 1,
        strategy: str = "Unknown"
    ):
        """Compatibility wrapper for legacy safety wrapper alerts."""
        # Normalize symbol for display
        symbol = normalize_symbol_for_display(symbol)
        
        action = "EXIT" if order_type.upper() == "EXIT" else ("BUY" if side.upper() == "LONG" else "SELL")
        await self.send_execution_alert(
            account_label="Order Execution Safety",
            action=action,
            symbol=symbol,
            quantity=quantity,
            price=price,
            leverage=leverage,
            strategy=strategy,
            position_side=side
        )
    
    async def notify_sl_tp_set(
        self,
        symbol: str,
        sl_price: Optional[float],
        tp_price: Optional[float],
        entry_price: float
    ):
        """Notify that SL/TP orders have been set."""
        # Normalize symbol for display
        symbol = normalize_symbol_for_display(symbol)
        
        sl_pct = ((sl_price - entry_price) / entry_price * 100) if sl_price else 0
        tp_pct = ((tp_price - entry_price) / entry_price * 100) if tp_price else 0
        
        lines = [
            "====================================================================",
            "",
            "🛡️ <b>PROTECTION SET</b>",
            "",
            f"Symbol: {symbol}",
        ]
        if sl_price:
            lines.append(f"Stop Loss: {format_price(sl_price, symbol)} ({sl_pct:+.2f}%)")
        if tp_price:
            lines.append(f"Take Profit: {format_price(tp_price, symbol)} ({tp_pct:+.2f}%)")
        lines.append("")
        lines.append(f"⏰ {self._timestamp()}")
        await self.send_message("\n".join(lines), priority="info", auto_prefix=False)
    
    async def notify_critical_failure(
        self,
        failure_type: str,
        details: str,
        symbol: str,
        action_taken: str
    ):
        """
        Notify about critical failures requiring attention.
        
        Args:
            failure_type: Type of failure (e.g., "SL/TP Failure", "Position Mismatch")
            details: Detailed error message
            symbol: Affected symbol
            action_taken: What the agent did (e.g., "Position closed")
        """
        # Normalize symbol for display
        symbol = normalize_symbol_for_display(symbol)
        
        lines = [
            "====================================================================",
            "",
            "🚨 <b>CRITICAL FAILURE</b>",
            "",
            f"Type: {failure_type}",
            f"Symbol: {symbol}",
            f"Details: {details}",
            f"Action Taken: {action_taken}",
            "",
            "⚠️ MANUAL REVIEW REQUIRED!",
            "",
            f"⏰ {self._timestamp()}",
        ]
        await self.send_message("\n".join(lines), priority="critical", auto_prefix=False)
    
    async def notify_position_mismatch(
        self,
        symbol: str,
        expected_side: str,
        actual_side: str,
        expected_qty: float,
        actual_qty: float
    ):
        """Notify about position verification mismatch."""
        # Normalize symbol for display
        symbol = normalize_symbol_for_display(symbol)
        
        lines = [
            "====================================================================",
            "",
            "⚠️ <b>POSITION MISMATCH DETECTED</b>",
            "",
            f"Symbol: {symbol}",
            f"Expected: {expected_side} {expected_qty:.4f}",
            f"Actual: {actual_side} {actual_qty:.4f}",
            "",
            "Broker position used as source of truth.",
            "",
            f"⏰ {self._timestamp()}",
        ]
        await self.send_message("\n".join(lines), priority="warning", auto_prefix=False)
    
    async def notify_liquidation_risk(
        self,
        symbol: str,
        distance_to_liquidation: float,
        current_price: float,
        liquidation_price: float,
        risk_level: str
    ):
        """Notify about liquidation risk."""
        # Normalize symbol for display
        symbol = normalize_symbol_for_display(symbol)
        
        extra = ""
        priority = "info"
        if risk_level == "CRITICAL":
            extra = "\n🚨 <b>IMMEDIATE ACTION REQUIRED!</b>"
            priority = "critical"
        elif risk_level == "HIGH":
            extra = "\n⚠️ <b>Monitor closely!</b>"
            priority = "warning"

        lines = [
            "====================================================================",
            "",
            "🛑 <b>LIQUIDATION RISK</b>",
            "",
            f"Symbol: {symbol}",
            f"Risk Level: {risk_level}",
            f"Current Price: {format_price(current_price, symbol)}",
            f"Liquidation Price: {format_price(liquidation_price, symbol)}",
            f"Distance: {distance_to_liquidation:.2f}%",
        ]

        if extra:
            lines.extend(["", extra.strip()])

        lines.extend(["", f"⏰ {self._timestamp()}"])

        await self.send_message("\n".join(lines), priority=priority, auto_prefix=False)
    
    async def send_daily_summary(
        self,
        account_stats: Dict[str, Any],
        open_positions: list,
        daily_pnl: float,
        *,
        daily_pnl_pct: Optional[float] = None,
        win_rate: Optional[float] = None,
        total_trades: Optional[int] = None,
        avg_win: Optional[float] = None,
        avg_loss: Optional[float] = None,
        account_daily_stats: Optional[Dict[str, Dict[str, float]]] = None
    ):
        """
        Send daily P&L summary.
        
        Args:
            account_stats: Account + partition statistics from P&L tracker
            open_positions: List of currently open positions
            daily_pnl: Today's aggregate P&L
        """
        report_time_local = datetime.now(self.local_timezone)
        account_daily_stats = account_daily_stats or {}

        def fmt_pct(value: float, bold: bool = False) -> str:
            text = f"{value:+.2f}%"
            return f"<b>{text}</b>" if bold else text

        def fmt_currency(value: float, show_plus: bool = True, bold: bool = False) -> str:
            sign = "+" if value >= 0 else "-"
            amount = f"${abs(value):,.2f}"
            if not show_plus and value >= 0:
                text = amount
            else:
                text = f"{sign}{amount}" if show_plus or value < 0 else amount
            return f"<b>{text}</b>" if bold else text

        def fmt_plain_number(value: float) -> str:
            sign = "+" if value >= 0 else "-"
            return f"{sign}{abs(value):,.2f}"

        lines = [
            "====================================================================",
            "",
            "🛃 <b>END-OF-DAY REPORT</b> | 🏝️🎖️",
        ]

        for account_name, stats in account_stats.items():
            lines.extend(["", f"  • {account_name}:"])
            style = stats.get('partition_style', 'isolated')
            style_label = "Cooperative" if style == "cooperative" else "Isolated"
            lines.append(f"     Partition Style: <b>{style_label}</b>")
            lines.append("")
            lines.append("📈 P&L (Today):")
            account_id = stats.get('account_id')
            account_daily = account_daily_stats.get(account_id, {})
            account_daily_pct = account_daily.get('daily_pnl_pct')
            if account_daily_pct is None:
                account_daily_pct = daily_pnl_pct if daily_pnl_pct is not None else 0.0
            account_daily_pnl = account_daily.get('daily_pnl', daily_pnl)
            lines.append(f"          {fmt_pct(account_daily_pct, bold=True)} {fmt_currency(account_daily_pnl, show_plus=True)}")
            
            # Today's Risk Metrics
            max_dd_pct = stats.get('max_drawdown_pct', 0.0)
            fm_today = account_daily.get('free_margin_pct')
            mu_worst_today = account_daily.get('margin_usage_worst_pct')
            mu_time_today = account_daily.get('margin_usage_worst_time') or "--:--"
            fm_worst_today = account_daily.get('free_margin_worst_pct')
            comm_today = account_daily.get('commissions_today', 0.0)
            comm_mtd = account_daily.get('commissions_mtd', 0.0)
            
            lines.append("")
            lines.append(f"          maxDD: {fmt_pct(-abs(max_dd_pct))}")
            # Use margin_usage_worst_pct from stats (from get_daily_account_stats) for Today
            # This is more accurate than account_daily_stats which may not be updated
            worst_margin_today = stats.get('margin_usage_worst_pct')
            worst_margin_time_today = stats.get('margin_usage_worst_time', '--:--')
            if worst_margin_today is not None and worst_margin_today > 0:
                lines.append(f"          Peak Margin: {int(round(worst_margin_today))}% @ {worst_margin_time_today}")
            elif mu_worst_today is not None and mu_worst_today > 0:
                # Fallback to account_daily_stats if stats doesn't have it
                lines.append(f"          Peak Margin: {int(round(mu_worst_today))}% @ {mu_time_today}")
            # Use global settings from config import (not local settings variable)
            # Import here to avoid any potential shadowing issues
            from config import settings as global_settings
            maint_threshold = int(getattr(global_settings, 'MIN_FREE_MARGIN_PCT_THRESHOLD', 30))
            # Use today's worst free margin to determine if it stayed ≥ 30% today
            if fm_worst_today is not None:
                maint_pass = fm_worst_today >= maint_threshold
                lines.append(f"          Free Margin ≥{maint_threshold}%: {'PASS' if maint_pass else 'FAIL'}")
            # If no worst value available, don't show (can't determine PASS/FAIL without historical data)
            if comm_today > 0 or comm_mtd > 0:
                lines.append(f"          Commissions: ${comm_today:,.2f} (MTD: ${comm_mtd:,.2f})")
            
            lines.append("")
            lines.append("🎖️ Account Balances (All Time):")
            realized_pct = stats.get('realized_pnl_pct', 0.0)
            realized_pnl = stats.get('realized_pnl', 0.0)
            max_dd_pct = stats.get('max_drawdown_pct', 0.0)
            
            # Calculate current account balance
            # For DEMO: Use demo account current_balance (which includes starting balance + P&L) or calculate from starting_balance + realized_pnl
            # For LIVE: use actual broker balance
            current_balance = None
            starting_balance = stats.get('starting_total', 0.0)
            
            if account_id:
                try:
                    from services.partition_manager import partition_manager
                    state = partition_manager.partition_state.get(account_id, {})
                    real_account = state.get('real_account', {}) or {}
                    account_mode = real_account.get('mode', 'DEMO')
                    total_balance = float(real_account.get('total_balance') or 0.0)
                    
                    if account_mode == 'DEMO':
                        # DEMO: Prioritize demo account's current_balance (includes starting balance + P&L)
                        # This is the most accurate source for total account balance
                        demo_balance = None
                        demo_starting_balance = None
                        try:
                            import sys
                            if 'main' in sys.modules:
                                from main import order_executor
                                if hasattr(order_executor, 'demo_accounts') and account_id in order_executor.demo_accounts:
                                    demo_account = order_executor.demo_accounts[account_id]
                                    demo_balance = float(demo_account.get('current_balance', 0.0))
                                    demo_starting_balance = float(demo_account.get('starting_balance', 0.0))
                                    
                                    # CRITICAL: Verify demo_balance is actually the total (not just P&L)
                                    # If demo_balance is less than starting_balance, it might be wrong
                                    # If demo_balance looks like just P&L (small value), recalculate
                                    if demo_balance > 0:
                                        if demo_starting_balance > 0 and demo_balance >= demo_starting_balance:
                                            # Valid total balance (includes starting + P&L)
                                            # Double-check: verify demo_balance makes sense (should be close to starting + P&L)
                                            expected_total = demo_starting_balance + realized_pnl
                                            if abs(demo_balance - expected_total) > 1.0:  # Allow $1 tolerance for rounding
                                                # demo_balance doesn't match expected total, recalculate
                                                logger.warning(f"EOD: demo_balance (${demo_balance:,.2f}) doesn't match expected (${expected_total:,.2f}), recalculating")
                                                current_balance = expected_total
                                                starting_balance = demo_starting_balance
                                                logger.debug(f"EOD: Recalculated balance from demo starting + P&L: ${current_balance:,.2f} (starting: ${starting_balance:,.2f}, P&L: ${realized_pnl:,.2f})")
                                            else:
                                                # demo_balance is valid
                                                current_balance = demo_balance
                                                starting_balance = demo_starting_balance  # Update starting_balance from demo account
                                                logger.debug(f"EOD: Using demo account current_balance: ${current_balance:,.2f} (starting: ${starting_balance:,.2f})")
                                        elif demo_starting_balance > 0:
                                            # demo_balance might be wrong, recalculate from starting + P&L
                                            current_balance = demo_starting_balance + realized_pnl
                                            starting_balance = demo_starting_balance
                                            logger.debug(f"EOD: Recalculated balance from demo starting + P&L: ${current_balance:,.2f} (starting: ${starting_balance:,.2f}, P&L: ${realized_pnl:,.2f})")
                                        else:
                                            # No starting_balance in demo account - don't trust demo_balance as-is
                                            # It might be just P&L. Recalculate from stats starting_balance + realized_pnl
                                            # First try to get starting_balance from stats
                                            if starting_balance > 0:
                                                current_balance = starting_balance + realized_pnl
                                                logger.debug(f"EOD: Recalculated from stats starting + P&L (no demo starting_balance): ${current_balance:,.2f} (starting: ${starting_balance:,.2f}, P&L: ${realized_pnl:,.2f})")
                                            else:
                                                # Last resort: use config default starting balance
                                                from services.settings import Settings
                                                local_settings = Settings()
                                                default_starting = getattr(local_settings, 'DEMO_STARTING_BALANCE', 2000.0)
                                                starting_balance = default_starting
                                                current_balance = starting_balance + realized_pnl
                                                logger.debug(f"EOD: Using default starting balance + P&L (no demo starting_balance): ${current_balance:,.2f} (default starting: ${default_starting:,.2f}, P&L: ${realized_pnl:,.2f})")
                                            # Also check if demo_balance looks suspiciously like just P&L
                                            # If demo_balance is much smaller than expected total, it's probably wrong
                                            if current_balance > 0 and abs(demo_balance - current_balance) > 100:
                                                logger.warning(f"EOD: demo_balance (${demo_balance:,.2f}) differs significantly from calculated balance (${current_balance:,.2f}), using calculated")
                        except Exception as e:
                            logger.debug(f"EOD: Could not get demo account current_balance: {e}")
                        
                        # Fallback 1: Use real_account.total_balance if demo account not available
                        # But verify it's reasonable (should be >= starting_balance)
                        if current_balance is None or current_balance <= 0:
                            if total_balance > 0:
                                # Verify total_balance is reasonable
                                if starting_balance > 0 and total_balance >= starting_balance:
                                    current_balance = total_balance
                                    logger.debug(f"EOD: Using real_account.total_balance: ${current_balance:,.2f}")
                                elif starting_balance > 0:
                                    # total_balance might be wrong, recalculate
                                    current_balance = starting_balance + realized_pnl
                                    logger.debug(f"EOD: Recalculated from starting + P&L (total_balance looked wrong): ${current_balance:,.2f}")
                                else:
                                    # No starting_balance, use total_balance as-is
                                    current_balance = total_balance
                                    logger.debug(f"EOD: Using real_account.total_balance (no starting_balance): ${current_balance:,.2f}")
                        
                        # Fallback 2: Calculate from starting_balance + realized_pnl
                        if current_balance is None or current_balance <= 0:
                            if starting_balance > 0:
                                current_balance = starting_balance + realized_pnl
                                logger.debug(f"EOD: Using calculated balance (starting + P&L): ${current_balance:,.2f} (starting: ${starting_balance:,.2f}, P&L: ${realized_pnl:,.2f})")
                            else:
                                # Last resort: If no starting_balance, try to infer from stats or use default
                                # Check if we can get starting_balance from demo account
                                if demo_starting_balance and demo_starting_balance > 0:
                                    starting_balance = demo_starting_balance
                                    current_balance = starting_balance + realized_pnl
                                    logger.debug(f"EOD: Using demo starting_balance + P&L: ${current_balance:,.2f}")
                                else:
                                    # Use default starting balance (should be $2000 based on config)
                                    from services.settings import Settings
                                    local_settings = Settings()
                                    default_starting = getattr(local_settings, 'DEMO_STARTING_BALANCE', 2000.0)
                                    starting_balance = default_starting
                                    current_balance = starting_balance + realized_pnl
                                    logger.debug(f"EOD: Using default starting balance + P&L: ${current_balance:,.2f} (default starting: ${default_starting:,.2f})")
                    else:
                        # LIVE: use actual broker balance
                        current_balance = total_balance if total_balance > 0 else (starting_balance + realized_pnl)
                except Exception as e:
                    logger.debug(f"EOD: Error getting account balance: {e}")
                    # Fallback: calculate from starting + P&L
                    if starting_balance > 0:
                        current_balance = starting_balance + realized_pnl
                    else:
                        # Try to get default starting balance
                        try:
                            from services.settings import Settings
                            local_settings = Settings()
                            default_starting = getattr(local_settings, 'DEMO_STARTING_BALANCE', 2000.0)
                            starting_balance = default_starting
                            current_balance = starting_balance + realized_pnl
                        except:
                            current_balance = 2000.0 + realized_pnl  # Hardcoded fallback
            else:
                # Fallback: calculate from starting + P&L
                if starting_balance > 0:
                    current_balance = starting_balance + realized_pnl
                else:
                    current_balance = 2000.0 + realized_pnl  # Hardcoded fallback
            
            # Display: P&L % and amount (first line)
            lines.append(f"          {fmt_pct(realized_pct, bold=True)} {fmt_currency(realized_pnl, show_plus=True)}")
            
            # Display: Total account balance (second line, separate)
            # CRITICAL: Ensure we always show total balance (starting + P&L), not just P&L
            # If current_balance looks suspiciously like just P&L (smaller than starting_balance), recalculate
            if current_balance and current_balance > 0:
                # Validate: current_balance should be >= starting_balance (for positive P&L) or close to it
                # If current_balance is much smaller than starting_balance, it's probably just P&L
                if starting_balance > 0:
                    if current_balance < starting_balance * 0.5:  # If balance is less than 50% of starting, it's probably wrong
                        logger.warning(f"EOD: current_balance (${current_balance:,.2f}) looks too small compared to starting_balance (${starting_balance:,.2f}), recalculating")
                        current_balance = starting_balance + realized_pnl
                        logger.debug(f"EOD: Recalculated total balance: ${current_balance:,.2f} (starting: ${starting_balance:,.2f}, P&L: ${realized_pnl:,.2f})")
                    elif abs(current_balance - (starting_balance + realized_pnl)) > 10.0:  # More than $10 difference, recalculate
                        logger.warning(f"EOD: current_balance (${current_balance:,.2f}) doesn't match expected (${starting_balance + realized_pnl:,.2f}), recalculating")
                        current_balance = starting_balance + realized_pnl
                        logger.debug(f"EOD: Recalculated total balance: ${current_balance:,.2f} (starting: ${starting_balance:,.2f}, P&L: ${realized_pnl:,.2f})")
                else:
                    # No starting_balance available, but if current_balance is suspiciously small, try to infer
                    # If realized_pnl is positive and current_balance is close to realized_pnl, it's probably wrong
                    if realized_pnl > 0 and abs(current_balance - realized_pnl) < 10.0:
                        logger.warning(f"EOD: current_balance (${current_balance:,.2f}) looks like just P&L (${realized_pnl:,.2f}), attempting to infer starting balance")
                        # Try to infer starting balance: if P&L is +$125.72 and it's 6.29%, starting was ~$2000
                        if realized_pct != 0:
                            inferred_starting = (realized_pnl / realized_pct) * 100
                            if inferred_starting > 1000 and inferred_starting < 10000:  # Reasonable range
                                current_balance = inferred_starting + realized_pnl
                                logger.debug(f"EOD: Inferred total balance: ${current_balance:,.2f} (inferred starting: ${inferred_starting:,.2f}, P&L: ${realized_pnl:,.2f})")
                            else:
                                # Use default starting balance
                                from services.settings import Settings
                                local_settings = Settings()
                                default_starting = getattr(local_settings, 'DEMO_STARTING_BALANCE', 2000.0)
                                current_balance = default_starting + realized_pnl
                                logger.debug(f"EOD: Using default starting balance + P&L: ${current_balance:,.2f} (default starting: ${default_starting:,.2f}, P&L: ${realized_pnl:,.2f})")
                
                # Final validation: If still suspicious, force recalculation
                if starting_balance > 0 and current_balance < starting_balance:
                    logger.warning(f"EOD: Final validation failed - current_balance (${current_balance:,.2f}) < starting_balance (${starting_balance:,.2f}), forcing recalculation")
                    current_balance = starting_balance + realized_pnl
                
                # Format balance as $X,XXX.XX (without sign prefix, as it's a total)
                balance_text = f"${current_balance:,.2f}"
                lines.append(f"          {balance_text}")
            else:
                # If balance unavailable, try to calculate it
                calculated_balance = starting_balance + realized_pnl
                if calculated_balance > 0:
                    balance_text = f"${calculated_balance:,.2f}"
                    lines.append(f"          {balance_text}")
            lines.append("")
            lines.append(f"          maxDD: {fmt_pct(-abs(max_dd_pct))}")
            account_win_rate = stats.get('win_rate')
            account_total_trades = stats.get('total_trades')
            if account_win_rate is None:
                account_win_rate = win_rate if win_rate is not None else 0.0
            if account_total_trades is None:
                account_total_trades = total_trades if total_trades is not None else 0
            lines.append(f"          Win Rate: {account_win_rate:.1f}% • Total Trades: {account_total_trades}")
            wins_val = stats.get('winning_trades')
            losses_val = stats.get('losing_trades')
            if wins_val is not None or losses_val is not None:
                lines.append(f"          Wins: {wins_val or 0} • Losses: {losses_val or 0}")
            # All-time peak margin and lowest free margin
            # Use all_time_peak_margin_pct from stats (from get_daily_account_stats) for All Time
            # This is more accurate and properly tracked
            mu_all_time = stats.get('all_time_peak_margin_pct')
            mu_all_time_time = stats.get('all_time_peak_margin_time', '--:--')
            # Fallback to account_daily_stats if stats doesn't have it
            if mu_all_time is None:
                mu_all_time = account_daily.get('all_time_peak_margin_pct')
                mu_all_time_time = account_daily.get('all_time_peak_margin_time') or "--:--"
            fm_all_time = account_daily.get('all_time_lowest_free_margin_pct')
            maint_threshold = int(getattr(settings, 'MIN_FREE_MARGIN_PCT_THRESHOLD', 30))
            if mu_all_time is not None and mu_all_time > 0:
                lines.append(f"          Peak Margin: {int(round(mu_all_time))}% @ {mu_all_time_time}")
            # Use all-time lowest free margin to determine if it has ALWAYS stayed ≥ 30%
            # PASS = always stayed above threshold, FAIL = ever broke below threshold
            if fm_all_time is not None:
                maint_pass = fm_all_time >= maint_threshold
                lines.append(f"          Free Margin ≥{maint_threshold}%: {'PASS' if maint_pass else 'FAIL'}")
            # If no all-time value available, don't show (can't determine PASS/FAIL without historical data)
            if comm_mtd > 0:
                lines.append(f"          Commissions (MTD): ${comm_mtd:,.2f}")
            
            # This Week P&L (Monday to Sunday)
            weekly_pnl = account_daily.get('weekly_pnl', 0.0)
            weekly_pnl_pct = account_daily.get('weekly_pnl_pct', 0.0)
            lines.append("")
            lines.append("📚 P&L (This Week):")
            lines.append(f"          {fmt_pct(weekly_pnl_pct, bold=True)} {fmt_currency(weekly_pnl, show_plus=True)}")
            
            # This Month P&L (1st to last day of month)
            monthly_pnl = account_daily.get('monthly_pnl', 0.0)
            monthly_pnl_pct = account_daily.get('monthly_pnl_pct', 0.0)
            lines.append("")
            lines.append("📆 P&L (This Month):")
            lines.append(f"          {fmt_pct(monthly_pnl_pct, bold=True)} {fmt_currency(monthly_pnl, show_plus=True)}")

            partition_style = (stats.get('partition_style') or "isolated").lower()
            strategy_entries = stats.get('strategies', [])
            # (Account Risk section removed in favor of inline Today/All Time blocks)

            if partition_style == "cooperative" and strategy_entries:
                lines.append("")
                # Reorder strategies to a canonical sequence for cooperative EOD
                # Order: 1H strategies first (BTC, ETH, SOL, XRP), then 5m strategies (BTC, ETH, SOL, XRP)
                desired_order = [
                    "ichimoku-coinbase-btc-1h",
                    "ichimoku-coinbase-eth-1h",
                    "ichimoku-coinbase-sol-1h",
                    "ichimoku-coinbase-xrp-1h",
                    "ichimoku-coinbase-btc-5m",
                    "ichimoku-coinbase-eth-5m",
                    "ichimoku-coinbase-sol-5m",
                    "ichimoku-coinbase-xrp-5m",
                ]
                def _strategy_order_key(entry: Dict[str, Any]) -> int:
                    sid = (entry.get('strategy_id') or "").lower()
                    if sid in desired_order:
                        return desired_order.index(sid)
                    # Fallback by label prefix if id missing/mismatched
                    # Prioritize 1H over 5m, then match by coin (BTC, ETH, SOL, XRP)
                    label = (entry.get('label') or "").lower()
                    is_1h = "1h" in label or "1h" in sid
                    coin_order = ["btc", "eth", "sol", "xrp"]
                    for i, token in enumerate(coin_order):
                        if token in label:
                            # 1H strategies get positions 0-3, 5m strategies get positions 4-7
                            base_index = i if is_1h else (i + 4)
                            return base_index
                    return len(desired_order) + 1
                strategy_entries = sorted(strategy_entries, key=_strategy_order_key)
                
                # Filter strategies: Only show strategies with activity (trades > 0 or non-zero P&L)
                # This prevents showing all strategies after a reset when they all have $0.00
                strategy_entries = [
                    s for s in strategy_entries
                    if s.get('total_trades', 0) > 0 or abs(s.get('total_pnl', 0.0)) > 0.01
                ]
                
                # Use current account balance for strategy percentages
                # Strategy P&L percentages should be calculated against current account balance, not starting balance
                strategy_balance_base = current_balance if current_balance and current_balance > 0 else (starting_balance + realized_pnl)
                if strategy_balance_base <= 0:
                    # Last resort fallback
                    strategy_balance_base = sum(
                        s.get('starting_balance', 0.0) for s in strategy_entries
                    ) or 1.0
                
                for idx, strategy_entry in enumerate(strategy_entries):
                    if idx > 0:
                        lines.append("")
                    strategy_id = strategy_entry.get('strategy_id')
                    # Use normalized label if available, otherwise fall back to label or strategy_id
                    strategy_label = normalize_strategy_label(strategy_id) if strategy_id else (strategy_entry.get('label') or 'Strategy')
                    strategy_pnl = strategy_entry.get('total_pnl', 0.0)
                    # Calculate strategy P&L as % of CURRENT total account balance
                    strategy_pct = (strategy_pnl / strategy_balance_base * 100) if strategy_balance_base > 0 else 0.0
                    strategy_max_dd = strategy_entry.get('max_drawdown_pct', 0.0)
                    lines.append(f"  • {strategy_label}")
                    lines.append(f"          {fmt_pct(strategy_pct, bold=True)} {fmt_currency(strategy_pnl, show_plus=True)}")
                    lines.append(f"          maxDD: {fmt_pct(-abs(strategy_max_dd))}")
                    
                    # Per-strategy margin data
                    mu_w = None
                    mu_w_time = "--:--"
                    qty_cur = 0.0
                    margin_per_1_1pm = 0.0
                    margin_per_1_1am = 0.0
                    
                    if strategy_id and account_id in account_daily_stats:
                        strat_risk = (account_daily_stats.get(account_id, {}).get('strategy_risk') or {}).get(strategy_id, {})
                        if strat_risk:
                            mu_w = strat_risk.get('margin_usage_worst_pct')
                            mu_w_time = strat_risk.get('margin_usage_worst_time') or "--:--"
                            qty_cur = strat_risk.get('qty_current', 0.0)
                            margin_per_1_1pm = strat_risk.get('margin_per_1_1pm', 0.0)
                            margin_per_1_1am = strat_risk.get('margin_per_1_1am', 0.0)
                    
                    lines.append(f"          Margin Per 1 (Intraday): ${margin_per_1_1pm:,.2f}")
                    lines.append(f"          Margin Per 1 (Afterhours): ${margin_per_1_1am:,.2f}")
                    # Only show Peak Margin if it's > 0
                    if mu_w is not None and mu_w > 0:
                        lines.append(f"          Peak Margin: {int(round(mu_w))}% @ {mu_w_time}")
            else:
                partitions = stats.get('partitions', [])
                # Use current account balance for partition percentages
                # Partition P&L percentages should be calculated against current account balance, not starting balance
                partition_balance_base = current_balance if current_balance and current_balance > 0 else (starting_balance + realized_pnl)
                if partition_balance_base <= 0:
                    # Last resort fallback
                    partition_balance_base = sum(
                        p.get('starting_balance', 0.0) for p in partitions
                    ) or 1.0
                
                for partition in partitions:
                    lines.append("")
                    partition_id = partition.get('partition_id', 'Partition')
                    partition_pnl = partition.get('total_pnl', 0.0)
                    # Calculate partition P&L as % of CURRENT total account balance
                    partition_pnl_pct = (partition_pnl / partition_balance_base * 100) if partition_balance_base > 0 else 0.0
                    partition_max_dd = partition.get('max_drawdown_pct', 0.0)
                    lines.append(f"  • {partition_id}")
                    lines.append(f"          {fmt_pct(partition_pnl_pct, bold=True)} {fmt_currency(partition_pnl, show_plus=True)}")
                    lines.append(f"          maxDD: {fmt_pct(-abs(partition_max_dd))}")

        if isinstance(open_positions, dict):
            total_open_positions = sum(len(positions) for positions in open_positions.values())
        else:
            total_open_positions = len(open_positions)

        lines.extend([
            "",
            f"📍 Open Positions ({total_open_positions}):",
        ])

        if isinstance(open_positions, dict):
            for account_name, positions in open_positions.items():
                if not positions:
                    lines.append(f"  • {account_name}: No open positions")
                    continue
                lines.append(f"  • {account_name}:")
                for pos in positions:
                    symbol = normalize_symbol_for_display(pos.get('symbol', 'UNKNOWN'))
                    side = pos.get('side', 'UNKNOWN').upper()
                    unrealized_pct = pos.get('unrealized_pnl_pct', 0.0)
                    unrealized_usd = pos.get('unrealized_pnl', 0.0)
                    lines.append(f"      - {symbol} {side}")
                    lines.append(f"          {fmt_pct(unrealized_pct, bold=True)} {fmt_plain_number(unrealized_usd)}")
        elif open_positions:
            for pos in open_positions:
                symbol = normalize_symbol_for_display(pos.get('symbol', 'UNKNOWN'))
                side = pos.get('side', 'UNKNOWN').upper()
                unrealized_pct = pos.get('unrealized_pnl_pct', 0.0)
                unrealized_usd = pos.get('unrealized_pnl', 0.0)
                lines.append(f"  • {symbol} {side}:")
                lines.append(f"          {fmt_pct(unrealized_pct, bold=True)} {fmt_plain_number(unrealized_usd)}")
        else:
            lines.append("  • No open positions")

        lines.extend([
            "",
            # Show Local and ET for consistency
            f"📅 Date: {report_time_local.strftime('%Y-%m-%d %I:%M %p %Z')} ({datetime.now(timezone.utc).astimezone(ZoneInfo('America/New_York')).strftime('%I:%M %p')} ET)",
        ])
        # Safety flags rendering (only when present in stats)
        # Expect account_daily_stats[account_id]['flags'] to be a list of strings
        for account_name, stats in account_stats.items():
            account_id = stats.get('account_id')
            flags = (account_daily_stats.get(account_id, {}) or {}).get('flags')
            if flags:
                lines.extend(["", "Safety Flags:"])
                for flag in flags:
                    lines.append(f"  • {flag}")

        priority = "success" if daily_pnl > 0 else ("warning" if daily_pnl < 0 else "info")
        return await self.send_message("\n".join(lines), priority=priority, auto_prefix=False)
    
    async def notify_order_retry(
        self,
        order_type: str,
        symbol: str,
        attempt: int,
        max_attempts: int,
        error: str
    ):
        """Notify about order retry attempts."""
        lines = [
            "====================================================================",
            "",
            "⚠️ <b>ORDER RETRY</b>",
            "",
            f"Type: {order_type}",
            f"Symbol: {symbol}",
            f"Attempt: {attempt}/{max_attempts}",
            f"Error: {error}",
            "",
            f"⏰ {self._timestamp()}",
        ]
        await self.send_message("\n".join(lines), priority="warning", auto_prefix=False)
    
    async def notify_order_failed(
        self,
        order_type: str,
        symbol: str,
        attempts: int,
        final_error: str
    ):
        """Notify that order failed after all retries."""
        lines = [
            "====================================================================",
            "",
            "🚨 <b>ORDER FAILED - ALL RETRIES EXHAUSTED</b>",
            "",
            f"Type: {order_type}",
            f"Symbol: {symbol}",
            f"Attempts: {attempts}",
            f"Error: {final_error}",
            "",
            "⚠️ Order not executed. Review logs.",
            "",
            f"⏰ {self._timestamp()}",
        ]
        await self.send_message("\n".join(lines), priority="critical", auto_prefix=False)
    
    async def _get_portfolio_pnl_section(self, account_id: str) -> Optional[List[str]]:
        """
        Get portfolio P&L section for alerts
        
        Returns formatted lines showing current portfolio P&L status
        """
        try:
            # Import here to avoid circular imports
            from services.realtime_pnl import realtime_pnl
            
            # Get current portfolio P&L
            account_pnl = await realtime_pnl.calculate_account_pnl(account_id, force_refresh=False)
            
            if account_pnl.total_positions == 0:
                # No positions, no P&L to show
                return None
            
            # Format P&L section
            lines = [
                "",
                "📊 Current Portfolio P&L:"
            ]
            
            # Total P&L
            pnl_sign = "+" if account_pnl.total_unrealized_pnl_usd >= 0 else ""
            pnl_pct_sign = "+" if account_pnl.portfolio_pnl_pct >= 0 else ""
            
            lines.append(f"   • {pnl_sign}${account_pnl.total_unrealized_pnl_usd:,.2f} ({pnl_pct_sign}{account_pnl.portfolio_pnl_pct:.2f}%)")
            
            # Position count and win rate
            lines.append(f"   • {account_pnl.total_positions} positions ({account_pnl.win_rate:.0f}% winning)")
            
            # Deployed capital
            lines.append(f"   • ${account_pnl.total_deployed_capital:,.2f} deployed")
            
            # Show top 3 positions if more than 3
            if len(account_pnl.positions) > 3:
                # Sort by absolute P&L (largest impact first)
                sorted_positions = sorted(account_pnl.positions, 
                                        key=lambda p: abs(p.unrealized_pnl_usd), 
                                        reverse=True)
                
                lines.append("   • Top positions:")
                for i, pos in enumerate(sorted_positions[:3]):
                    pnl_sign = "+" if pos.unrealized_pnl_usd >= 0 else ""
                    symbol_display = normalize_symbol_for_display(pos.symbol)
                    lines.append(f"     {i+1}. {symbol_display}: {pnl_sign}${pos.unrealized_pnl_usd:,.2f}")
            
            return lines
            
        except Exception as e:
            logger.debug(f"Error getting portfolio P&L section: {e}")
            return None
    
    async def send_holiday_alert(
        self,
        holiday_name: str,
        holiday_type: str = "MARKET_CLOSED",
        account_label: str = "TradingView Agent"
    ) -> bool:
        """
        Send holiday alert when trading is disabled due to holidays.
        
        Args:
            holiday_name: Name of the holiday (e.g., "Thanksgiving Day", "Christmas Day")
            holiday_type: "MARKET_CLOSED" or "LOW_VOLUME"
            account_label: Account label for display
            
        Returns:
            True if alert sent successfully
        """
        try:
            # Get current time in ET (market timezone)
            et_tz = ZoneInfo('America/New_York')
            pt_tz = ZoneInfo('America/Los_Angeles')
            now_et = datetime.now(et_tz)
            now_pt = datetime.now(pt_tz)
            
            day_name = now_et.strftime('%A, %B %d, %Y')
            
            # Determine emoji and message based on holiday type
            if holiday_type == "MARKET_CLOSED":
                # Bank holiday - market is closed
                emoji = "🏖️"
                reason_msg = "U.S. crypto futures market follows traditional market holidays. Trading disabled to preserve capital quality."
            else:
                # Low-volume holiday - market is open but we skip trading
                emoji = "🎃"
                reason_msg = "Market is open, but volume is typically low on this holiday. Trading disabled to preserve capital quality."
            
            # Build holiday alert message (similar to ETrade Strategy format)
            message = f"""====================================================================

{emoji} <b>Holiday! - {holiday_name}</b>
          {day_name}

🎭 <b>No Trading Today!</b> ☁️🏖️🏝️⛱️🌤️☁️☁️

🚫 <b>Status:</b>
          Trading DISABLED today.

💡 <b>Why:</b>
          {reason_msg}

✅ <b>System Status:</b> 
          Normal - Agent monitoring continues

🔍 <b>Next Trading:</b> 
          System will resume at next normal trading day

📱 <b>Account:</b> 
          {account_label}

⏰ {now_pt.strftime('%I:%M %p')} PT ({now_et.strftime('%I:%M %p')} ET)

"""
            
            success = await self.send_message(message, auto_prefix=False)
            
            if success:
                logger.info(f"🎭 Holiday alert sent: {holiday_name} ({holiday_type})")
            else:
                logger.error(f"❌ Failed to send holiday alert: {holiday_name}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending holiday alert: {e}")
            return False


# Global instance
telegram_notifier = TelegramNotifier()

