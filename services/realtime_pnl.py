"""
TradingView Agent - Real-Time P&L Calculator
Provides live P&L calculations for open positions
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from decimal import Decimal

from services.position_reconciler import position_reconciler, UnifiedPosition
from services.telegram_notifier import telegram_notifier

logger = logging.getLogger(__name__)


@dataclass
class PositionPnL:
    """P&L data for a single position"""
    symbol: str
    side: str
    quantity: float
    entry_price: float
    current_price: float
    unrealized_pnl_usd: float
    unrealized_pnl_pct: float
    deployed_capital: float
    leverage: int
    partition_id: str
    entry_time: Optional[datetime] = None
    age_hours: float = 0.0
    
    @property
    def is_profitable(self) -> bool:
        return self.unrealized_pnl_usd > 0
    
    @property
    def loss_pct(self) -> float:
        return abs(self.unrealized_pnl_pct) if self.unrealized_pnl_usd < 0 else 0.0


@dataclass
class AccountPnL:
    """Aggregated P&L data for an account"""
    account_id: str
    total_unrealized_pnl_usd: float
    total_deployed_capital: float
    total_positions: int
    profitable_positions: int
    losing_positions: int
    largest_winner_usd: float
    largest_loser_usd: float
    positions: List[PositionPnL]
    last_updated: datetime
    
    @property
    def win_rate(self) -> float:
        if self.total_positions == 0:
            return 0.0
        return (self.profitable_positions / self.total_positions) * 100
    
    @property
    def portfolio_pnl_pct(self) -> float:
        """Portfolio P&L percentage based on deployed capital (position-level impact)"""
        if self.total_deployed_capital == 0:
            return 0.0
        return (self.total_unrealized_pnl_usd / self.total_deployed_capital) * 100
    
    def get_account_portfolio_pnl_pct(self, total_account_balance: float) -> float:
        """Portfolio P&L percentage based on total account balance (true portfolio impact)"""
        if total_account_balance == 0:
            return 0.0
        return (self.total_unrealized_pnl_usd / total_account_balance) * 100


class RealtimePnLCalculator:
    """
    Real-time P&L calculator for open positions
    
    Calculates live P&L by fetching current market prices and comparing
    with position entry prices, taking into account leverage and deployed capital.
    """
    
    def __init__(self):
        self.price_cache = {}
        self.price_cache_ttl = timedelta(seconds=15)  # Cache prices for 15 seconds
        self.pnl_cache = {}
        self.pnl_cache_ttl = timedelta(seconds=30)  # Cache P&L for 30 seconds
        
    async def calculate_account_pnl(self, account_id: str, force_refresh: bool = False) -> AccountPnL:
        """
        Calculate real-time P&L for all positions in an account
        
        Args:
            account_id: Account to calculate P&L for
            force_refresh: Skip cache and fetch fresh data
            
        Returns:
            AccountPnL with current P&L calculations
        """
        cache_key = f"pnl_{account_id}"
        now = datetime.utcnow()
        
        # Check cache first (unless force refresh)
        if not force_refresh and cache_key in self.pnl_cache:
            cached_data, cache_time = self.pnl_cache[cache_key]
            if now - cache_time < self.pnl_cache_ttl:
                logger.debug(f"Returning cached P&L for {account_id}")
                return cached_data
        
        try:
            # Get unified positions
            positions = await position_reconciler.get_unified_positions(account_id, force_refresh=True)
            
            if not positions:
                # No positions - return empty P&L
                account_pnl = AccountPnL(
                    account_id=account_id,
                    total_unrealized_pnl_usd=0.0,
                    total_deployed_capital=0.0,
                    total_positions=0,
                    profitable_positions=0,
                    losing_positions=0,
                    largest_winner_usd=0.0,
                    largest_loser_usd=0.0,
                    positions=[],
                    last_updated=now
                )
                
                # Cache result
                self.pnl_cache[cache_key] = (account_pnl, now)
                return account_pnl
            
            # Get current prices for all symbols
            symbols = list(set(pos.symbol for pos in positions))
            current_prices = await self._get_current_prices(symbols)
            
            # Calculate P&L for each position
            position_pnls = []
            total_unrealized_pnl = 0.0
            total_deployed_capital = 0.0
            profitable_count = 0
            losing_count = 0
            largest_winner = 0.0
            largest_loser = 0.0
            
            for position in positions:
                current_price = current_prices.get(position.symbol)
                if current_price is None:
                    logger.warning(f"No current price available for {position.symbol}")
                    continue
                
                # Calculate P&L
                pnl_data = self._calculate_position_pnl(position, current_price)
                position_pnls.append(pnl_data)
                
                # Aggregate totals
                total_unrealized_pnl += pnl_data.unrealized_pnl_usd
                total_deployed_capital += pnl_data.deployed_capital
                
                if pnl_data.is_profitable:
                    profitable_count += 1
                    largest_winner = max(largest_winner, pnl_data.unrealized_pnl_usd)
                else:
                    losing_count += 1
                    largest_loser = min(largest_loser, pnl_data.unrealized_pnl_usd)
            
            # Create account P&L summary
            account_pnl = AccountPnL(
                account_id=account_id,
                total_unrealized_pnl_usd=total_unrealized_pnl,
                total_deployed_capital=total_deployed_capital,
                total_positions=len(position_pnls),
                profitable_positions=profitable_count,
                losing_positions=losing_count,
                largest_winner_usd=largest_winner,
                largest_loser_usd=largest_loser,
                positions=position_pnls,
                last_updated=now
            )
            
            # Cache result
            self.pnl_cache[cache_key] = (account_pnl, now)
            
            logger.info(f"📊 Calculated P&L for {account_id}: {len(position_pnls)} positions, ${total_unrealized_pnl:+.2f} unrealized")
            return account_pnl
            
        except Exception as e:
            logger.error(f"Error calculating P&L for {account_id}: {e}")
            # Return empty P&L on error
            return AccountPnL(
                account_id=account_id,
                total_unrealized_pnl_usd=0.0,
                total_deployed_capital=0.0,
                total_positions=0,
                profitable_positions=0,
                losing_positions=0,
                largest_winner_usd=0.0,
                largest_loser_usd=0.0,
                positions=[],
                last_updated=now
            )
    
    def _calculate_position_pnl(self, position: UnifiedPosition, current_price: float) -> PositionPnL:
        """Calculate P&L for a single position
        
        Uses the same calculation method as close_partition_position() to ensure consistency:
        - Calculate price change percentage
        - Apply leverage
        - Calculate P&L based on deployed capital (margin)
        """
        
        # Calculate price change percentage
        if position.side.upper() == "LONG":
            price_change_pct = ((current_price - position.entry_price) / position.entry_price) * 100
        else:  # SHORT
            price_change_pct = ((position.entry_price - current_price) / position.entry_price) * 100
        
        # Apply leverage to get P&L percentage
        leverage = position.leverage if position.leverage > 0 else 1
        pnl_pct = price_change_pct * leverage
        
        # Calculate P&L in USD based on deployed capital (margin)
        # This matches the calculation in close_partition_position()
        unrealized_pnl_usd = 0.0
        if position.deployed_capital > 0:
            unrealized_pnl_usd = position.deployed_capital * (pnl_pct / 100)
        
        # P&L percentage (already includes leverage)
        unrealized_pnl_pct = pnl_pct
        
        return PositionPnL(
            symbol=position.symbol,
            side=position.side,
            quantity=position.quantity,
            entry_price=position.entry_price,
            current_price=current_price,
            unrealized_pnl_usd=unrealized_pnl_usd,
            unrealized_pnl_pct=unrealized_pnl_pct,
            deployed_capital=position.deployed_capital,
            leverage=position.leverage,
            partition_id=position.partition_id or "unknown",
            entry_time=position.entry_time,
            age_hours=position.age_hours
        )
    
    async def _get_current_prices(self, symbols: List[str]) -> Dict[str, float]:
        """Get current prices for multiple symbols with caching"""
        now = datetime.utcnow()
        prices = {}
        symbols_to_fetch = []
        
        # Check cache for each symbol
        for symbol in symbols:
            cache_key = f"price_{symbol}"
            if cache_key in self.price_cache:
                cached_price, cache_time = self.price_cache[cache_key]
                if now - cache_time < self.price_cache_ttl:
                    prices[symbol] = cached_price
                    continue
            symbols_to_fetch.append(symbol)
        
        # Fetch prices for symbols not in cache
        if symbols_to_fetch:
            try:
                # For now, we'll use a simple price fetching approach
                # In production, this would connect to the appropriate broker APIs
                fetched_prices = await self._fetch_prices_from_broker(symbols_to_fetch)
                
                for symbol, price in fetched_prices.items():
                    if price is not None:
                        prices[symbol] = price
                        # Cache the price
                        cache_key = f"price_{symbol}"
                        self.price_cache[cache_key] = (price, now)
                        
            except Exception as e:
                logger.error(f"Error fetching current prices: {e}")
        
        return prices
    
    async def _fetch_prices_from_broker(self, symbols: List[str]) -> Dict[str, Optional[float]]:
        """Fetch current prices from Coinbase (preferred) with yfinance fallback"""
        prices = {}
        
        # Try Coinbase broker first (preferred for real-time data)
        try:
            coinbase_prices = await self._fetch_prices_from_coinbase(symbols)
            for symbol, price in coinbase_prices.items():
                if price is not None:
                    prices[symbol] = price
                    logger.debug(f"✅ Coinbase broker price for {symbol}: ${price:,.2f}")
        except Exception as e:
            logger.warning(f"Coinbase broker price fetch failed: {e}")
        
        # Try Coinbase public API for missing symbols
        missing_symbols = [s for s in symbols if s not in prices or prices[s] is None]
        if missing_symbols:
            try:
                coinbase_public_prices = await self._fetch_prices_from_coinbase_public(missing_symbols)
                for symbol, price in coinbase_public_prices.items():
                    if price is not None:
                        prices[symbol] = price
                        logger.debug(f"✅ Coinbase public API price for {symbol}: ${price:,.2f}")
            except Exception as e:
                logger.warning(f"Coinbase public API price fetch failed: {e}")
        
        # Use yfinance fallback for symbols that failed
        missing_symbols = [s for s in symbols if s not in prices or prices[s] is None]
        if missing_symbols:
            try:
                yfinance_prices = await self._fetch_prices_from_yfinance(missing_symbols)
                for symbol, price in yfinance_prices.items():
                    if price is not None:
                        prices[symbol] = price
                        logger.debug(f"📈 yfinance fallback for {symbol}: ${price:,.2f}")
            except Exception as e:
                logger.warning(f"yfinance fallback failed: {e}")
        
        # Fill in None for any remaining symbols
        for symbol in symbols:
            if symbol not in prices:
                prices[symbol] = None
                logger.warning(f"❌ No price available for {symbol}")
        
        return prices
    
    async def _fetch_prices_from_coinbase(self, symbols: List[str]) -> Dict[str, Optional[float]]:
        """Fetch prices from Coinbase Advanced Trade API"""
        prices = {}
        
        try:
            # Import broker here to avoid circular imports
            from brokers.coinbase_futures import create_coinbase_futures_trader
            from config.yaml_config import get_enabled_accounts
            from config.settings import CONFIG
            
            # Get first enabled account to fetch prices
            accounts = get_enabled_accounts(CONFIG)
            if not accounts:
                logger.warning("No enabled accounts found for Coinbase price fetching")
                return {symbol: None for symbol in symbols}
            
            account = accounts[0]  # Use first account for price fetching
            logger.debug(f"Using account {account.id} for Coinbase price fetching")
            
            # Create broker instance
            broker = create_coinbase_futures_trader(
                api_key=account.api_key,
                api_secret=account.api_secret,
                leverage=1,  # Default leverage for price fetching
                sandbox=False,
                demo_mode=(account.mode == "DEMO"),
                portfolio_id=getattr(account, 'portfolio_id', None)
            )
            
            # Fetch prices from Coinbase
            for symbol in symbols:
                try:
                    logger.debug(f"Fetching Coinbase price for {symbol}")
                    price = await asyncio.to_thread(broker.get_current_price, symbol)
                    if price and price > 0:
                        prices[symbol] = price
                        logger.debug(f"✅ Coinbase price for {symbol}: ${price:,.2f}")
                    else:
                        logger.warning(f"Coinbase returned invalid price for {symbol}: {price}")
                        prices[symbol] = None
                except Exception as e:
                    logger.warning(f"Coinbase price fetch failed for {symbol}: {e}")
                    prices[symbol] = None
                    
        except Exception as e:
            logger.error(f"Error setting up Coinbase broker for price fetching: {e}")
            return {symbol: None for symbol in symbols}
        
        return prices
    
    async def _fetch_prices_from_yfinance(self, symbols: List[str]) -> Dict[str, Optional[float]]:
        """Fetch prices from yfinance as fallback"""
        prices = {}
        
        try:
            import yfinance as yf
            
            # Map broker symbols to yfinance tickers
            symbol_map = {
                'ETP-CBSE': 'ETH-USD',
                'BIP-CBSE': 'BTC-USD', 
                'SLP-CBSE': 'SOL-USD',
                'XPP-CBSE': 'XRP-USD',
                'ETH-PERP': 'ETH-USD',
                'BTC-PERP': 'BTC-USD',
                'SOL-PERP': 'SOL-USD', 
                'XRP-PERP': 'XRP-USD'
            }
            
            for symbol in symbols:
                try:
                    # Map to yfinance ticker
                    yf_ticker = symbol_map.get(symbol, symbol)
                    
                    # Fetch current price
                    ticker = yf.Ticker(yf_ticker)
                    info = await asyncio.to_thread(ticker.history, period="1d", interval="1m")
                    
                    if not info.empty:
                        current_price = float(info['Close'].iloc[-1])
                        prices[symbol] = current_price
                    else:
                        prices[symbol] = None
                        
                except Exception as e:
                    logger.warning(f"yfinance price fetch failed for {symbol}: {e}")
                    prices[symbol] = None
                    
        except ImportError:
            logger.warning("yfinance not available - install with: pip install yfinance")
            return {symbol: None for symbol in symbols}
        except Exception as e:
            logger.error(f"Error fetching prices from yfinance: {e}")
            return {symbol: None for symbol in symbols}
        
        return prices
    
    async def _fetch_prices_from_coinbase_public(self, symbols: List[str]) -> Dict[str, Optional[float]]:
        """Fetch prices from Coinbase public API as fallback"""
        prices = {}
        
        try:
            import aiohttp
            
            # Map broker symbols to Coinbase public product IDs
            symbol_map = {
                'ETP-CBSE': 'ETH-USD',
                'BIP-CBSE': 'BTC-USD', 
                'SLP-CBSE': 'SOL-USD',
                'XPP-CBSE': 'XRP-USD',
                'ETH-PERP': 'ETH-USD',
                'BTC-PERP': 'BTC-USD',
                'SOL-PERP': 'SOL-USD', 
                'XRP-PERP': 'XRP-USD'
            }
            
            async with aiohttp.ClientSession() as session:
                for symbol in symbols:
                    try:
                        # Map to Coinbase product ID
                        product_id = symbol_map.get(symbol, symbol)
                        
                        # Fetch from Coinbase public API
                        url = f"https://api.exchange.coinbase.com/products/{product_id}/ticker"
                        async with session.get(url, timeout=5) as response:
                            if response.status == 200:
                                data = await response.json()
                                current_price = float(data.get('price', 0))
                                if current_price > 0:
                                    prices[symbol] = current_price
                                else:
                                    prices[symbol] = None
                            else:
                                logger.warning(f"Coinbase public API returned {response.status} for {symbol}")
                                prices[symbol] = None
                                
                    except Exception as e:
                        logger.warning(f"Coinbase public API price fetch failed for {symbol}: {e}")
                        prices[symbol] = None
                        
        except Exception as e:
            logger.error(f"Error fetching prices from Coinbase public API: {e}")
            return {symbol: None for symbol in symbols}
        
        return prices
    
    async def get_position_alerts(self, account_id: str, loss_threshold_pct: float = 10.0) -> List[Dict[str, Any]]:
        """
        Get positions that need attention (large losses, stale positions, etc.)
        
        Args:
            account_id: Account to check
            loss_threshold_pct: Alert if position loss exceeds this percentage
            
        Returns:
            List of alert dictionaries
        """
        try:
            account_pnl = await self.calculate_account_pnl(account_id)
            alerts = []
            
            for position in account_pnl.positions:
                # Large loss alert
                if position.loss_pct > loss_threshold_pct:
                    alerts.append({
                        "type": "large_loss",
                        "symbol": position.symbol,
                        "partition_id": position.partition_id,
                        "loss_pct": position.loss_pct,
                        "loss_usd": position.unrealized_pnl_usd,
                        "message": f"Position {position.symbol} down {position.loss_pct:.1f}% (${position.unrealized_pnl_usd:,.2f})"
                    })
                
                # Stale position alert (>24 hours)
                if position.age_hours > 24:
                    alerts.append({
                        "type": "stale_position",
                        "symbol": position.symbol,
                        "partition_id": position.partition_id,
                        "age_hours": position.age_hours,
                        "message": f"Position {position.symbol} open for {position.age_hours:.1f} hours"
                    })
            
            return alerts
            
        except Exception as e:
            logger.error(f"Error getting position alerts for {account_id}: {e}")
            return []
    
    def clear_cache(self):
        """Clear all cached data"""
        self.price_cache.clear()
        self.pnl_cache.clear()
        logger.info("🧹 Cleared P&L and price caches")


# Global instance
realtime_pnl = RealtimePnLCalculator()
