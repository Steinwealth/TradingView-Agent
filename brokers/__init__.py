"""
Broker integration package (V3.6)

All brokers now implement unified interface for partition support:
- get_account_balance_usd() → Returns USD equivalent
- get_available_balance_usd() → Returns available in USD
- get_all_positions() → Returns positions with margin_usd field

Works seamlessly across Coinbase (USD/USDC), Binance (USDT/BNB), Kraken (USD/EUR)
"""

from .broker_interface import BrokerInterface
from .coinbase_futures import CoinbaseFuturesBroker, CoinbaseFuturesTrader

__all__ = [
    'BrokerInterface',
    'CoinbaseFuturesBroker',
    'CoinbaseFuturesTrader'
]

try:
    from .binance_futures import (
        BinanceFuturesBroker,
        BinanceFuturesTrader,
        create_binance_futures_trader
    )

    __all__.extend([
        'BinanceFuturesBroker',
        'BinanceFuturesTrader',
        'create_binance_futures_trader'
    ])
except ImportError:
    print("Binance Futures broker not available. Skipping import.")

try:
    from .base_dex import BaseDEXBroker, BaseDEXTrader

    __all__.extend([
        'BaseDEXBroker',
        'BaseDEXTrader'
    ])
except ImportError:
    print("Base DEX broker not available. Skipping import.")

