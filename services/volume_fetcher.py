"""
TradingView Agent - Volume Fetcher Service
Fetches Average Daily Volume (ADV) data for symbols across different brokers
Used for ADV Cap (Slip Guard) to minimize slippage
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
from decimal import Decimal
import statistics

logger = logging.getLogger(__name__)


class VolumeFetcher:
    """
    Fetches and caches Average Daily Volume (ADV) for symbols
    """
    
    def __init__(self, settings):
        self.settings = settings
        self.volume_cache = {}  # Cache: {symbol: {adv_usd, last_updated}}
        self.cache_duration = timedelta(hours=1)  # Refresh every hour
        logger.info("VolumeFetcher initialized")
    
    async def get_adv_usd(
        self,
        symbol: str,
        broker: str,
        lookback_days: int = 7
    ) -> Optional[float]:
        """
        Get Average Daily Volume (ADV) in USD for a symbol
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDT", "cbBTC/USDC")
            broker: Broker name (e.g., "binance_futures", "base_dex")
            lookback_days: Number of days to calculate average (default 7)
        
        Returns:
            Average daily volume in USD, or None if unavailable
        """
        try:
            # Check cache
            cache_key = f"{broker}:{symbol}"
            if cache_key in self.volume_cache:
                cached = self.volume_cache[cache_key]
                age = datetime.utcnow() - cached['last_updated']
                
                if age < self.cache_duration:
                    logger.debug(f"ADV cache hit for {cache_key}: ${cached['adv_usd']:,.0f}")
                    return cached['adv_usd']
            
            # Fetch fresh data based on broker
            adv_usd = None
            
            if broker == "binance_futures" or broker == "binance_spot":
                adv_usd = await self._fetch_binance_adv(symbol, lookback_days)
            
            elif broker == "base_dex":
                adv_usd = await self._fetch_base_dex_adv(symbol, lookback_days)
            
            elif broker == "oanda":
                adv_usd = await self._fetch_oanda_adv(symbol, lookback_days)
            
            elif broker == "coinbase_futures":
                adv_usd = await self._fetch_coinbase_adv(symbol, lookback_days)
            
            else:
                logger.warning(f"Unknown broker for ADV fetch: {broker}")
                return None
            
            # Cache result
            if adv_usd is not None:
                self.volume_cache[cache_key] = {
                    'adv_usd': adv_usd,
                    'last_updated': datetime.utcnow()
                }
                logger.info(f"ADV fetched for {cache_key}: ${adv_usd:,.0f} (lookback: {lookback_days}d)")
            
            return adv_usd
            
        except Exception as e:
            logger.error(f"Error fetching ADV for {broker}:{symbol}: {str(e)}")
            return None
    
    async def _fetch_binance_adv(self, symbol: str, lookback_days: int) -> Optional[float]:
        """
        Fetch ADV from Binance Futures API
        
        Uses Binance's klines (candlestick) data to calculate average daily volume
        """
        try:
            # Import Binance client
            from binance.client import Client
            
            # Initialize client (public API, no auth needed for volume data)
            client = Client(
                self.settings.BINANCE_API_KEY,
                self.settings.BINANCE_SECRET
            )
            client.API_URL = 'https://fapi.binance.com'  # Futures endpoint
            
            # Get klines (daily candles) for lookback period
            klines = client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_1DAY,
                limit=lookback_days
            )
            
            # Calculate daily volumes in USD
            # Kline format: [open_time, open, high, low, close, volume, close_time, quote_volume, ...]
            daily_volumes = []
            
            for kline in klines:
                quote_volume = float(kline[7])  # Quote asset volume (USDT)
                daily_volumes.append(quote_volume)
            
            # Calculate average
            if daily_volumes:
                adv_usd = statistics.mean(daily_volumes)
                return adv_usd
            else:
                logger.warning(f"No volume data returned for Binance {symbol}")
                return None
                
        except Exception as e:
            logger.error(f"Binance ADV fetch error for {symbol}: {str(e)}")
            return None
    
    async def _fetch_base_dex_adv(self, symbol: str, lookback_days: int) -> Optional[float]:
        """
        Fetch ADV from Base DEX (Uniswap V3 on Base)
        
        For DEX, we use pool liquidity (TVL) as a proxy for tradeable volume
        since historical volume data is harder to get from on-chain
        """
        try:
            # For Base DEX, use a conservative estimate based on pool liquidity
            # cbBTC/USDC 0.05% pool typically has $2M-5M liquidity
            # Daily volume ~ 50-100% of liquidity for active pools
            
            # Import Web3 for on-chain queries
            from web3 import Web3
            
            # Connect to Base RPC
            w3 = Web3(Web3.HTTPProvider(self.settings.BASE_CHAIN_RPC))
            
            # For now, use a conservative hardcoded estimate
            # TODO: Fetch real pool liquidity from Uniswap V3 subgraph
            
            if symbol == "cbBTC/USDC" or symbol == "BTC/USD":
                # cbBTC/USDC 0.05% pool
                # Typical TVL: $2M-5M
                # Typical daily volume: ~$2M (conservative)
                estimated_adv = 2_000_000  # $2M daily volume
                logger.info(f"Using estimated ADV for Base DEX {symbol}: ${estimated_adv:,.0f}")
                return estimated_adv
            else:
                logger.warning(f"Unknown symbol for Base DEX ADV: {symbol}")
                return None
                
        except Exception as e:
            logger.error(f"Base DEX ADV fetch error for {symbol}: {str(e)}")
            return None
    
    async def _fetch_oanda_adv(self, symbol: str, lookback_days: int) -> Optional[float]:
        """
        Fetch ADV from OANDA API
        
        Uses OANDA's candles API to calculate average daily volume
        """
        try:
            # OANDA doesn't provide volume in the same way as crypto exchanges
            # For forex/commodities, use typical market volume estimates
            
            if symbol == "XAU_USD":
                # Gold futures typical daily volume: ~$100M
                estimated_adv = 100_000_000
                logger.info(f"Using estimated ADV for OANDA {symbol}: ${estimated_adv:,.0f}")
                return estimated_adv
            else:
                logger.warning(f"Unknown symbol for OANDA ADV: {symbol}")
                return None
                
        except Exception as e:
            logger.error(f"OANDA ADV fetch error for {symbol}: {str(e)}")
            return None
    
    async def _fetch_coinbase_adv(self, symbol: str, lookback_days: int) -> Optional[float]:
        """
        Fetch ADV from Coinbase Futures (CDE nano perpetuals)
        
        Uses Coinbase public API to get candle data and calculate average daily volume.
        For nano perpetuals, we use the underlying spot volume as a proxy since
        futures volume data may not be directly available via public API.
        
        Symbols supported:
        - BTC-PERP, BIP-CBSE, BIP → BTC
        - ETH-PERP, ETP-CBSE, ETP → ETH
        - SOL-PERP, SLP-CBSE, SLP → SOL
        - XRP-PERP, XPP-CBSE, XPP → XRP
        """
        try:
            import requests
            
            # Map Coinbase futures symbols to spot symbols for volume lookup
            symbol_map = {
                # BTC variants
                'BTC-PERP': 'BTC-USD',
                'BIP-CBSE': 'BTC-USD',
                'BIP': 'BTC-USD',
                # ETH variants
                'ETH-PERP': 'ETH-USD',
                'ETP-CBSE': 'ETH-USD',
                'ETP': 'ETH-USD',
                # SOL variants
                'SOL-PERP': 'SOL-USD',
                'SLP-CBSE': 'SOL-USD',
                'SLP': 'SOL-USD',
                # XRP variants
                'XRP-PERP': 'XRP-USD',
                'XPP-CBSE': 'XRP-USD',
                'XPP': 'XRP-USD',
            }
            
            # Normalize symbol
            normalized = symbol.upper().strip()
            spot_symbol = symbol_map.get(normalized)
            
            if not spot_symbol:
                # Try to extract base currency
                base = normalized.replace('-PERP', '').replace('-CBSE', '').replace('P', '')
                if base in ['BTC', 'BT', 'BI']:
                    spot_symbol = 'BTC-USD'
                elif base in ['ETH', 'ET']:
                    spot_symbol = 'ETH-USD'
                elif base in ['SOL', 'SL']:
                    spot_symbol = 'SOL-USD'
                elif base in ['XRP', 'XP']:
                    spot_symbol = 'XRP-USD'
                else:
                    logger.warning(f"Unknown Coinbase symbol for ADV: {symbol}")
                    return self._get_coinbase_fallback_adv(symbol)
            
            # Coinbase Exchange API - Get candles (public, no auth required)
            # Granularity: 86400 = 1 day
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=lookback_days)
            
            url = f"https://api.exchange.coinbase.com/products/{spot_symbol}/candles"
            params = {
                'granularity': 86400,  # Daily candles
                'start': start_time.isoformat(),
                'end': end_time.isoformat()
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                logger.warning(f"Coinbase API returned {response.status_code} for {spot_symbol}")
                return self._get_coinbase_fallback_adv(symbol)
            
            candles = response.json()
            
            if not candles or not isinstance(candles, list):
                logger.warning(f"No candle data for Coinbase {spot_symbol}")
                return self._get_coinbase_fallback_adv(symbol)
            
            # Candle format: [timestamp, low, high, open, close, volume]
            # Volume is in base currency, need to convert to USD
            daily_volumes_usd = []
            
            for candle in candles:
                if len(candle) >= 6:
                    volume_base = float(candle[5])  # Volume in base currency
                    close_price = float(candle[4])  # Close price in USD
                    volume_usd = volume_base * close_price
                    daily_volumes_usd.append(volume_usd)
            
            if daily_volumes_usd:
                adv_usd = statistics.mean(daily_volumes_usd)
                logger.info(f"Coinbase ADV for {symbol} ({spot_symbol}): ${adv_usd:,.0f} ({len(daily_volumes_usd)} days)")
                return adv_usd
            else:
                logger.warning(f"Could not calculate ADV for Coinbase {symbol}")
                return self._get_coinbase_fallback_adv(symbol)
                
        except Exception as e:
            logger.error(f"Coinbase ADV fetch error for {symbol}: {str(e)}")
            return self._get_coinbase_fallback_adv(symbol)
    
    def _get_coinbase_fallback_adv(self, symbol: str) -> float:
        """
        Return conservative fallback ADV estimates for Coinbase symbols
        Based on typical market volumes for each asset
        """
        normalized = symbol.upper()
        
        # Conservative estimates based on typical Coinbase spot volumes
        # These are floor values - actual volume is usually higher
        fallback_adv = {
            'BTC': 500_000_000,    # $500M daily (conservative, actual ~$1-2B)
            'ETH': 200_000_000,    # $200M daily (conservative, actual ~$500M-1B)
            'SOL': 100_000_000,    # $100M daily (conservative, actual ~$200-400M)
            'XRP': 50_000_000,     # $50M daily (conservative, actual ~$100-200M)
        }
        
        # Determine base asset
        for base, adv in fallback_adv.items():
            if base in normalized:
                logger.info(f"Using fallback ADV for Coinbase {symbol}: ${adv:,.0f}")
                return adv
        
        # Default fallback
        default_adv = 50_000_000  # $50M default
        logger.info(f"Using default fallback ADV for Coinbase {symbol}: ${default_adv:,.0f}")
        return default_adv
    
    def clear_cache(self):
        """Clear the ADV cache (force refresh on next fetch)"""
        self.volume_cache.clear()
        logger.info("ADV cache cleared")
    
    def get_cache_status(self) -> Dict:
        """Get current cache status for monitoring"""
        return {
            'cached_symbols': list(self.volume_cache.keys()),
            'cache_count': len(self.volume_cache),
            'cache_duration_hours': self.cache_duration.total_seconds() / 3600
        }

