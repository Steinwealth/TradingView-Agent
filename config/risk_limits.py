"""
TradingView Agent - Risk Limits Configuration
Strategy-specific risk parameters and global limits
"""

# Strategy-specific risk limits
RISK_LIMITS = {
    "MegaCryptoBot": {
        "description": "Easy MegaCryptoBot V4.10 Optimized",
        "broker": "binance_futures",
        "symbol": "BTCUSDT",
        "max_position_usdt": 10000,
        "max_leverage": 3,  # Recommended for 14.37% max DD
        "max_daily_loss_pct": 15,
        "max_drawdown_pct": 20,
        "expected_trades_per_month": 41.5,
        "expected_monthly_return_pct": 15.93,  # 1x leverage
        "expected_monthly_return_pct_3x": 41.9,  # 3x leverage
        "verified_max_dd_pct": 4.79,  # 1x
        "verified_max_dd_pct_3x": 14.37,  # 3x
    },
    
    "SuperTrendline": {
        "description": "Easy Bitcoin SuperTrendline",
        "broker": "binance_futures",
        "symbol": "BTCUSDT",
        "max_position_usdt": 25000,
        "max_leverage": 2,
        "max_daily_loss_pct": 12,
        "max_drawdown_pct": 18,
        "expected_trades_per_month": 7,
        "expected_monthly_return_pct": 12.5,
        "verified_max_dd_pct": 10,
    },
    
    "IchimokuBinance": {
        "description": "€£$¥ Ichimoku - Binance 5m/15m Presets",
        "broker": "binance_futures",  # Binance Futures for 3x leverage
        "symbol": "BTCUSDT",
        "max_position_usdt": 25000,
        "max_leverage": 3,  # 3x recommended for max returns
        "max_daily_loss_pct": 10,
        "max_drawdown_pct": 15,
        # Binance 5m preset (CHAMPION!)
        "expected_trades_per_month_5m": 23,  # 14 trades in 18 days
        "expected_monthly_return_pct_5m_1x": 55,  # +55% monthly @ 1x
        "expected_monthly_return_pct_5m_3x": 169,  # +169% monthly @ 3x
        "verified_max_dd_pct_5m": 1.65,  # 1.65% @ 1x
        "verified_max_dd_pct_5m_3x": 4.95,  # ~5% @ 3x (estimated)
        # Binance 15m preset (pending testing)
        "expected_trades_per_month_15m": 36,  # Estimated
        "expected_monthly_return_pct_15m_1x": 28,  # Estimated (similar to Coinbase)
        "expected_monthly_return_pct_15m_3x": 84,  # Estimated
    },
    
    "IchimokuCoinbase": {
        "description": "€£$¥ Ichimoku - Coinbase 5m/15m Presets",
        "broker": "base_dex",  # Execute on Base chain for lowest fees
        "symbol": "cbBTC/USDC",
        "max_position_usdt": 25000,
        "max_leverage": 1,  # Spot only (no leverage on DEX)
        "max_daily_loss_pct": 10,
        "max_drawdown_pct": 15,
        # Coinbase 5m preset
        "expected_trades_per_month_5m": 67,  # 47 trades in 21 days
        "expected_monthly_return_pct_5m": 17.6,  # +17.6% monthly @ 1x
        "verified_max_dd_pct_5m": 4.85,
        # Coinbase 15m preset
        "expected_trades_per_month_15m": 64,  # ~64 trades in 54 days
        "expected_monthly_return_pct_15m": 13.7,  # +13.7% monthly @ 1x
        "verified_max_dd_pct_15m": 5.0,  # Estimated
    },
    
    "EasyIchimoku": {
        "description": "Easy Ichimoku (Gold)",
        "broker": "oanda",
        "symbol": "XAU_USD",
        "max_position_units": 1000,  # Gold units
        "max_leverage": 1,  # No leverage on gold
        "max_daily_loss_pct": 10,
        "max_drawdown_pct": 15,
        "expected_trades_per_month": 8,
        "expected_monthly_return_pct": 4.5,
        "verified_max_dd_pct": 8,
    },
    
    # Default limits for new/unknown strategies
    "DEFAULT": {
        "max_position_usdt": 1000,
        "max_leverage": 1,
        "max_daily_loss_pct": 10,
        "max_drawdown_pct": 15,
    }
}

# Global risk limits (apply to all strategies)
GLOBAL_LIMITS = {
    "max_daily_loss_usd": 1000,  # Total across all strategies
    "max_total_positions": 5,  # Max simultaneous positions
    "max_leverage": 5,  # Never exceed this
    "max_position_size_usd": 50000,  # Single position limit
    "max_drawdown_pct": 25,  # Global kill switch
}

# Broker-specific limits
BROKER_LIMITS = {
    "binance_futures": {
        "max_leverage": 5,  # Conservative (Binance allows 125x!)
        "min_order_usdt": 5,  # Binance minimum
        "max_order_usdt": 1000000,
        "max_positions": 10,
    },
    
    "binance_spot": {
        "max_leverage": 3,  # Spot margin max for BTC
        "min_order_usdt": 10,
        "max_order_usdt": 1000000,
        "max_positions": 10,
    },
    
    "oanda": {
        "max_leverage": 50,  # OANDA allows high leverage
        "recommended_leverage": 1,  # But we stay conservative
        "min_order_units": 1,
        "max_order_units": 10000,
        "max_positions": 20,
    },
    
    "coinbase": {
        "max_leverage": 1,  # Coinbase is spot only
        "min_order_usd": 10,
        "max_order_usd": 100000,
        "max_positions": 10,
    },
    
    "base_dex": {
        "max_leverage": 1,  # Base DEX is spot only (no leverage)
        "min_order_usd": 10,
        "max_order_usd": 50000,  # Based on liquidity
        "max_positions": 1,  # One position at a time (wallet holds cbBTC or USDC)
        "max_slippage_pct": 1.0,  # Max 1% slippage
        "min_gas_balance_eth": 0.001,  # Min ETH for gas
    }
}

# Circuit breaker thresholds
CIRCUIT_BREAKER = {
    "consecutive_losses": 5,  # Pause after 5 losses in a row
    "daily_loss_pct": 15,  # Pause if daily loss > 15%
    "drawdown_pct": 20,  # Kill switch if DD > 20%
    "failed_orders": 10,  # Pause after 10 failed orders
}

# ADV Cap (Slip Guard) - Symbol-specific volume data
# Used to limit position sizes to minimize slippage
ADV_LIMITS = {
    # ==========================================
    # BINANCE FUTURES
    # ==========================================
    "BTCUSDT": {
        "min_daily_volume_usd": 10_000_000_000,  # BTC has ~$10B+ daily volume
        "max_position_pct_of_adv": 1.0,  # Max 1% of ADV (still huge for BTC)
        "typical_adv_usd": 15_000_000_000,  # Typical BTC daily volume
    },
    "ETHUSDT": {
        "min_daily_volume_usd": 3_000_000_000,  # ETH has ~$3B+ daily volume
        "max_position_pct_of_adv": 1.0,
        "typical_adv_usd": 5_000_000_000,
    },
    
    # ==========================================
    # COINBASE FUTURES (CDE Nano Perpetuals)
    # Volume data fetched via Coinbase Exchange API
    # ==========================================
    # BTC variants
    "BTC-PERP": {
        "min_daily_volume_usd": 500_000_000,   # Coinbase BTC spot ~$500M-2B daily
        "max_position_pct_of_adv": 1.0,        # 1% of ADV
        "typical_adv_usd": 1_000_000_000,      # $1B typical
    },
    "BIP-CBSE": {
        "min_daily_volume_usd": 500_000_000,
        "max_position_pct_of_adv": 1.0,
        "typical_adv_usd": 1_000_000_000,
    },
    "BIP": {
        "min_daily_volume_usd": 500_000_000,
        "max_position_pct_of_adv": 1.0,
        "typical_adv_usd": 1_000_000_000,
    },
    # ETH variants
    "ETH-PERP": {
        "min_daily_volume_usd": 200_000_000,   # Coinbase ETH spot ~$200M-1B daily
        "max_position_pct_of_adv": 1.0,
        "typical_adv_usd": 500_000_000,
    },
    "ETP-CBSE": {
        "min_daily_volume_usd": 200_000_000,
        "max_position_pct_of_adv": 1.0,
        "typical_adv_usd": 500_000_000,
    },
    "ETP": {
        "min_daily_volume_usd": 200_000_000,
        "max_position_pct_of_adv": 1.0,
        "typical_adv_usd": 500_000_000,
    },
    # SOL variants
    "SOL-PERP": {
        "min_daily_volume_usd": 100_000_000,   # Coinbase SOL spot ~$100-400M daily
        "max_position_pct_of_adv": 1.0,
        "typical_adv_usd": 200_000_000,
    },
    "SLP-CBSE": {
        "min_daily_volume_usd": 100_000_000,
        "max_position_pct_of_adv": 1.0,
        "typical_adv_usd": 200_000_000,
    },
    "SLP": {
        "min_daily_volume_usd": 100_000_000,
        "max_position_pct_of_adv": 1.0,
        "typical_adv_usd": 200_000_000,
    },
    # XRP variants
    "XRP-PERP": {
        "min_daily_volume_usd": 50_000_000,    # Coinbase XRP spot ~$50-200M daily
        "max_position_pct_of_adv": 1.0,
        "typical_adv_usd": 100_000_000,
    },
    "XPP-CBSE": {
        "min_daily_volume_usd": 50_000_000,
        "max_position_pct_of_adv": 1.0,
        "typical_adv_usd": 100_000_000,
    },
    "XPP": {
        "min_daily_volume_usd": 50_000_000,
        "max_position_pct_of_adv": 1.0,
        "typical_adv_usd": 100_000_000,
    },
    
    # ==========================================
    # BASE DEX
    # ==========================================
    "cbBTC/USDC": {
        "min_daily_volume_usd": 500_000,  # Base DEX cbBTC/USDC pool
        "max_position_pct_of_adv": 0.5,  # More conservative on DEX (0.5%)
        "typical_adv_usd": 2_000_000,  # Typical pool volume
    },
    
    # ==========================================
    # OANDA (Forex/Commodities)
    # ==========================================
    "XAU_USD": {
        "min_daily_volume_usd": 50_000_000,  # Gold futures volume
        "max_position_pct_of_adv": 0.5,
        "typical_adv_usd": 100_000_000,
    },
    
    # ==========================================
    # DEFAULT (Unknown symbols)
    # ==========================================
    "DEFAULT": {
        "min_daily_volume_usd": 100_000,
        "max_position_pct_of_adv": 0.5,  # Conservative default
        "typical_adv_usd": 1_000_000,
    }
}


def get_strategy_limits(strategy_name: str) -> dict:
    """
    Get risk limits for a strategy
    Returns DEFAULT if strategy not found
    """
    return RISK_LIMITS.get(strategy_name, RISK_LIMITS["DEFAULT"])


def get_broker_limits(broker_name: str) -> dict:
    """Get broker-specific limits"""
    return BROKER_LIMITS.get(broker_name, {})


def get_adv_limits(symbol: str) -> dict:
    """
    Get ADV (Average Daily Volume) limits for a symbol
    Returns DEFAULT if symbol not found
    """
    return ADV_LIMITS.get(symbol, ADV_LIMITS["DEFAULT"])

