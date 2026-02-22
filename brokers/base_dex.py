"""
Base Chain DEX Broker - Uniswap V3 Integration
Executes trades on Base chain with cbBTC/USDC at 0.05% fees
"""

from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account
from decimal import Decimal
import logging
import time
from typing import Dict, Optional, Tuple
import json

logger = logging.getLogger(__name__)


class BaseDEXBroker:
    """
    Base Chain Uniswap V3 broker for cbBTC/USDC trading
    
    Provides 0.05% swap fees + minimal gas costs
    Non-custodial execution via user wallet
    """
    
    def __init__(self, config):
        """
        Initialize Base chain connection
        
        Args:
            config: Settings object with BASE_CHAIN_RPC, wallet keys, contract addresses
        """
        self.config = config
        
        # Connect to Base chain
        self.w3 = Web3(Web3.HTTPProvider(config.BASE_CHAIN_RPC))
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)  # For Base (PoA chain)
        
        # Load wallet
        if config.BASE_WALLET_PRIVATE_KEY:
            self.account = Account.from_key(config.BASE_WALLET_PRIVATE_KEY)
            self.wallet_address = self.account.address
            logger.info(f"Base wallet loaded: {self.wallet_address}")
        else:
            logger.warning("No wallet private key provided - read-only mode")
            self.account = None
            self.wallet_address = config.BASE_WALLET_ADDRESS
        
        # Contract addresses
        self.cbbtc_address = Web3.to_checksum_address(config.CBBTC_ADDRESS_BASE)
        self.usdc_address = Web3.to_checksum_address(config.USDC_ADDRESS_BASE)
        self.weth_address = Web3.to_checksum_address(config.WETH_ADDRESS_BASE)
        self.router_address = Web3.to_checksum_address(config.UNISWAP_V3_ROUTER_BASE)
        self.quoter_address = Web3.to_checksum_address(config.UNISWAP_V3_QUOTER_BASE)
        
        # Load ABIs
        self.erc20_abi = self._get_erc20_abi()
        self.router_abi = self._get_uniswap_router_abi()
        self.quoter_abi = self._get_uniswap_quoter_abi()
        
        # Initialize contracts
        self.cbbtc_contract = self.w3.eth.contract(address=self.cbbtc_address, abi=self.erc20_abi)
        self.usdc_contract = self.w3.eth.contract(address=self.usdc_address, abi=self.erc20_abi)
        self.router_contract = self.w3.eth.contract(address=self.router_address, abi=self.router_abi)
        self.quoter_contract = self.w3.eth.contract(address=self.quoter_address, abi=self.quoter_abi)
        
        # Constants
        self.CBBTC_DECIMALS = 8  # cbBTC uses 8 decimals like BTC
        self.USDC_DECIMALS = 6   # USDC uses 6 decimals
        self.FEE_TIER = 500      # 0.05% = 500 (Uniswap uses basis points)
        
        logger.info("BaseDEXBroker initialized successfully")
    
    async def get_balances(self) -> Dict[str, Decimal]:
        """
        Get wallet balances for cbBTC and USDC
        
        Returns:
            {
                'cbBTC': Decimal('0.5'),
                'USDC': Decimal('10000.50'),
                'ETH': Decimal('0.1')  # For gas
            }
        """
        try:
            if not self.wallet_address:
                return {}
            
            # Get ETH balance (for gas)
            eth_balance_wei = self.w3.eth.get_balance(self.wallet_address)
            eth_balance = Decimal(eth_balance_wei) / Decimal(10**18)
            
            # Get cbBTC balance
            cbbtc_balance_raw = self.cbbtc_contract.functions.balanceOf(self.wallet_address).call()
            cbbtc_balance = Decimal(cbbtc_balance_raw) / Decimal(10**self.CBBTC_DECIMALS)
            
            # Get USDC balance
            usdc_balance_raw = self.usdc_contract.functions.balanceOf(self.wallet_address).call()
            usdc_balance = Decimal(usdc_balance_raw) / Decimal(10**self.USDC_DECIMALS)
            
            balances = {
                'cbBTC': cbbtc_balance,
                'USDC': usdc_balance,
                'ETH': eth_balance
            }
            
            logger.info(f"Base balances: {balances}")
            return balances
            
        except Exception as e:
            logger.error(f"Error getting balances: {str(e)}")
            raise
    
    async def get_quote(self, token_in: str, token_out: str, amount_in: Decimal) -> Tuple[Decimal, Decimal]:
        """
        Get quote from Uniswap V3 Quoter
        
        Args:
            token_in: "USDC" or "cbBTC"
            token_out: "cbBTC" or "USDC"
            amount_in: Amount to swap
        
        Returns:
            (amount_out, price)
        """
        try:
            # Map token names to addresses
            token_map = {
                'USDC': self.usdc_address,
                'cbBTC': self.cbbtc_address
            }
            
            token_in_address = token_map[token_in]
            token_out_address = token_map[token_out]
            
            # Convert to raw amount (wei)
            decimals_in = self.USDC_DECIMALS if token_in == 'USDC' else self.CBBTC_DECIMALS
            amount_in_raw = int(amount_in * Decimal(10**decimals_in))
            
            # Get quote from Quoter V2
            quote_result = self.quoter_contract.functions.quoteExactInputSingle((
                token_in_address,    # tokenIn
                token_out_address,   # tokenOut
                amount_in_raw,       # amountIn
                self.FEE_TIER,       # fee (500 = 0.05%)
                0                    # sqrtPriceLimitX96 (0 = no limit)
            )).call()
            
            # Parse result (quoter returns tuple)
            amount_out_raw = quote_result[0] if isinstance(quote_result, tuple) else quote_result
            
            # Convert back to human-readable
            decimals_out = self.CBBTC_DECIMALS if token_out == 'cbBTC' else self.USDC_DECIMALS
            amount_out = Decimal(amount_out_raw) / Decimal(10**decimals_out)
            
            # Calculate price
            price = amount_out / amount_in if amount_in > 0 else Decimal(0)
            
            logger.info(f"Quote: {amount_in} {token_in} → {amount_out} {token_out} (price: {price})")
            
            return (amount_out, price)
            
        except Exception as e:
            logger.error(f"Error getting quote: {str(e)}")
            raise
    
    async def execute_swap(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        min_amount_out: Decimal,
        deadline_seconds: int = 60
    ) -> Dict:
        """
        Execute swap on Uniswap V3
        
        Args:
            token_in: "USDC" or "cbBTC"
            token_out: "cbBTC" or "USDC"
            amount_in: Amount to swap
            min_amount_out: Minimum acceptable output (slippage protection)
            deadline_seconds: Transaction deadline
        
        Returns:
            {
                'tx_hash': '0x...',
                'amount_in': Decimal('1000'),
                'amount_out': Decimal('0.015'),
                'price': Decimal('66666.67'),
                'gas_used': 150000,
                'gas_cost_eth': Decimal('0.0001'),
                'status': 'success'
            }
        """
        if not self.account:
            raise Exception("No wallet private key - cannot execute trades")
        
        try:
            # Map tokens to addresses
            token_map = {
                'USDC': self.usdc_address,
                'cbBTC': self.cbbtc_address
            }
            
            token_in_address = token_map[token_in]
            token_out_address = token_map[token_out]
            
            # Convert amounts to raw (wei)
            decimals_in = self.USDC_DECIMALS if token_in == 'USDC' else self.CBBTC_DECIMALS
            decimals_out = self.CBBTC_DECIMALS if token_out == 'cbBTC' else self.USDC_DECIMALS
            
            amount_in_raw = int(amount_in * Decimal(10**decimals_in))
            min_amount_out_raw = int(min_amount_out * Decimal(10**decimals_out))
            
            # Check and approve token if needed
            await self._ensure_approval(token_in, token_in_address, amount_in_raw)
            
            # Build swap transaction
            deadline = int(time.time()) + deadline_seconds
            
            # Use exactInputSingle for single-hop swap
            swap_params = (
                token_in_address,     # tokenIn
                token_out_address,    # tokenOut
                self.FEE_TIER,        # fee (500 = 0.05%)
                self.wallet_address,  # recipient
                amount_in_raw,        # amountIn
                min_amount_out_raw,   # amountOutMinimum
                0                     # sqrtPriceLimitX96 (0 = no limit)
            )
            
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(self.wallet_address)
            gas_price = int(self.w3.eth.gas_price * self.config.GAS_PRICE_MULTIPLIER)
            
            tx = self.router_contract.functions.exactInputSingle(swap_params).build_transaction({
                'from': self.wallet_address,
                'gas': 200000,  # Estimate, will be adjusted
                'gasPrice': gas_price,
                'nonce': nonce,
                'deadline': deadline
            })
            
            # Estimate gas more precisely
            gas_estimate = self.w3.eth.estimate_gas(tx)
            tx['gas'] = int(gas_estimate * 1.2)  # 20% buffer
            
            # Sign transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx, self.account.key)
            
            # Send transaction
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            logger.info(f"Swap submitted: {tx_hash.hex()}")
            
            # Wait for confirmation
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            
            if tx_receipt['status'] != 1:
                raise Exception(f"Transaction failed: {tx_hash.hex()}")
            
            # Calculate gas cost
            gas_used = tx_receipt['gasUsed']
            gas_cost_wei = gas_used * gas_price
            gas_cost_eth = Decimal(gas_cost_wei) / Decimal(10**18)
            
            # Get actual amount out from logs (TODO: parse Transfer events)
            # For now, use min_amount_out as approximation
            actual_amount_out = min_amount_out
            actual_price = actual_amount_out / amount_in if amount_in > 0 else Decimal(0)
            
            result = {
                'tx_hash': tx_hash.hex(),
                'amount_in': float(amount_in),
                'amount_out': float(actual_amount_out),
                'price': float(actual_price),
                'gas_used': gas_used,
                'gas_cost_eth': float(gas_cost_eth),
                'gas_cost_usd': float(gas_cost_eth * Decimal(3000)),  # Assume $3000 ETH
                'fee_tier': '0.05%',
                'status': 'success',
                'block': tx_receipt['blockNumber']
            }
            
            logger.info(f"Swap successful: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Swap failed: {str(e)}")
            raise
    
    async def _ensure_approval(self, token_name: str, token_address: str, amount: int):
        """
        Ensure token is approved for Uniswap router to spend
        
        Args:
            token_name: "USDC" or "cbBTC"
            token_address: Token contract address
            amount: Amount to approve (raw, in wei)
        """
        if not self.account:
            return
        
        try:
            # Get token contract
            token_contract = self.w3.eth.contract(address=token_address, abi=self.erc20_abi)
            
            # Check current allowance
            current_allowance = token_contract.functions.allowance(
                self.wallet_address,
                self.router_address
            ).call()
            
            # If allowance sufficient, skip
            if current_allowance >= amount:
                logger.info(f"{token_name} already approved")
                return
            
            # Approve unlimited (standard practice to save gas on future approvals)
            max_approval = 2**256 - 1
            
            logger.info(f"Approving {token_name} for Uniswap router...")
            
            nonce = self.w3.eth.get_transaction_count(self.wallet_address)
            gas_price = self.w3.eth.gas_price
            
            approve_tx = token_contract.functions.approve(
                self.router_address,
                max_approval
            ).build_transaction({
                'from': self.wallet_address,
                'gas': 50000,
                'gasPrice': gas_price,
                'nonce': nonce
            })
            
            # Sign and send
            signed_approve = self.w3.eth.account.sign_transaction(approve_tx, self.account.key)
            approve_hash = self.w3.eth.send_raw_transaction(signed_approve.rawTransaction)
            
            # Wait for confirmation
            receipt = self.w3.eth.wait_for_transaction_receipt(approve_hash, timeout=120)
            
            if receipt['status'] != 1:
                raise Exception(f"Approval failed: {approve_hash.hex()}")
            
            logger.info(f"{token_name} approved successfully")
            
        except Exception as e:
            logger.error(f"Approval error: {str(e)}")
            raise
    
    async def buy_btc(self, usdc_amount: Decimal, max_slippage_pct: float = 1.0) -> Dict:
        """
        Buy cbBTC with USDC
        
        Args:
            usdc_amount: Amount of USDC to spend
            max_slippage_pct: Maximum acceptable slippage (default 1%)
        
        Returns:
            Swap result dict
        """
        try:
            logger.info(f"Buying cbBTC with {usdc_amount} USDC")
            
            # Get quote
            cbbtc_out, price = await self.get_quote('USDC', 'cbBTC', usdc_amount)
            
            # Calculate minimum with slippage
            min_cbbtc_out = cbbtc_out * Decimal(1 - max_slippage_pct / 100)
            
            logger.info(f"Expected: {cbbtc_out} cbBTC, Min: {min_cbbtc_out} cbBTC (price: ${1/price:.2f}/BTC)")
            
            # Execute swap
            result = await self.execute_swap(
                token_in='USDC',
                token_out='cbBTC',
                amount_in=usdc_amount,
                min_amount_out=min_cbbtc_out
            )
            
            result['side'] = 'BUY'
            result['symbol'] = 'BTC/USD'
            return result
            
        except Exception as e:
            logger.error(f"Buy BTC failed: {str(e)}")
            raise
    
    async def sell_btc(self, cbbtc_amount: Decimal, max_slippage_pct: float = 1.0) -> Dict:
        """
        Sell cbBTC for USDC
        
        Args:
            cbbtc_amount: Amount of cbBTC to sell
            max_slippage_pct: Maximum acceptable slippage (default 1%)
        
        Returns:
            Swap result dict
        """
        try:
            logger.info(f"Selling {cbbtc_amount} cbBTC for USDC")
            
            # Get quote
            usdc_out, price = await self.get_quote('cbBTC', 'USDC', cbbtc_amount)
            
            # Calculate minimum with slippage
            min_usdc_out = usdc_out * Decimal(1 - max_slippage_pct / 100)
            
            logger.info(f"Expected: {usdc_out} USDC, Min: {min_usdc_out} USDC (price: ${price:.2f}/BTC)")
            
            # Execute swap
            result = await self.execute_swap(
                token_in='cbBTC',
                token_out='USDC',
                amount_in=cbbtc_amount,
                min_amount_out=min_usdc_out
            )
            
            result['side'] = 'SELL'
            result['symbol'] = 'BTC/USD'
            return result
            
        except Exception as e:
            logger.error(f"Sell BTC failed: {str(e)}")
            raise
    
    async def get_position(self) -> Dict:
        """
        Get current position (based on wallet holdings)
        
        Returns:
            {
                'symbol': 'BTC/USD',
                'side': 'LONG' | 'SHORT' | 'NONE',
                'quantity': Decimal('0.5'),  # cbBTC amount
                'value_usd': Decimal('34000'),
                'cash_usd': Decimal('10000')  # USDC balance
            }
        """
        try:
            balances = await self.get_balances()
            
            cbbtc_balance = balances.get('cbBTC', Decimal(0))
            usdc_balance = balances.get('USDC', Decimal(0))
            
            # Estimate BTC price (get quote for 1 USDC)
            if cbbtc_balance > 0:
                _, btc_price_per_usdc = await self.get_quote('USDC', 'cbBTC', Decimal(1000))
                btc_price_usd = Decimal(1) / btc_price_per_usdc if btc_price_per_usdc > 0 else Decimal(0)
            else:
                btc_price_usd = Decimal(0)
            
            position = {
                'symbol': 'BTC/USD',
                'side': 'LONG' if cbbtc_balance > Decimal('0.0001') else 'NONE',
                'quantity': float(cbbtc_balance),
                'value_usd': float(cbbtc_balance * btc_price_usd),
                'cash_usd': float(usdc_balance),
                'total_value_usd': float(cbbtc_balance * btc_price_usd + usdc_balance),
                'current_price': float(btc_price_usd)
            }
            
            return position
            
        except Exception as e:
            logger.error(f"Error getting position: {str(e)}")
            raise
    
    def _get_erc20_abi(self) -> list:
        """Standard ERC20 ABI"""
        return [
            {
                "constant": True,
                "inputs": [],
                "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [{"name": "_owner", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "balance", "type": "uint256"}],
                "type": "function"
            },
            {
                "constant": False,
                "inputs": [
                    {"name": "_spender", "type": "address"},
                    {"name": "_value", "type": "uint256"}
                ],
                "name": "approve",
                "outputs": [{"name": "", "type": "bool"}],
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [
                    {"name": "_owner", "type": "address"},
                    {"name": "_spender", "type": "address"}
                ],
                "name": "allowance",
                "outputs": [{"name": "", "type": "uint256"}],
                "type": "function"
            }
        ]
    
    def _get_uniswap_router_abi(self) -> list:
        """Uniswap V3 SwapRouter02 ABI (simplified)"""
        return [
            {
                "inputs": [
                    {
                        "components": [
                            {"internalType": "address", "name": "tokenIn", "type": "address"},
                            {"internalType": "address", "name": "tokenOut", "type": "address"},
                            {"internalType": "uint24", "name": "fee", "type": "uint24"},
                            {"internalType": "address", "name": "recipient", "type": "address"},
                            {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
                            {"internalType": "uint256", "name": "amountOutMinimum", "type": "uint256"},
                            {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"}
                        ],
                        "internalType": "struct ISwapRouter.ExactInputSingleParams",
                        "name": "params",
                        "type": "tuple"
                    }
                ],
                "name": "exactInputSingle",
                "outputs": [{"internalType": "uint256", "name": "amountOut", "type": "uint256"}],
                "stateMutability": "payable",
                "type": "function"
            }
        ]
    
    def _get_uniswap_quoter_abi(self) -> list:
        """Uniswap V3 QuoterV2 ABI (simplified)"""
        return [
            {
                "inputs": [
                    {
                        "components": [
                            {"internalType": "address", "name": "tokenIn", "type": "address"},
                            {"internalType": "address", "name": "tokenOut", "type": "address"},
                            {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
                            {"internalType": "uint24", "name": "fee", "type": "uint24"},
                            {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"}
                        ],
                        "internalType": "struct IQuoterV2.QuoteExactInputSingleParams",
                        "name": "params",
                        "type": "tuple"
                    }
                ],
                "name": "quoteExactInputSingle",
                "outputs": [
                    {"internalType": "uint256", "name": "amountOut", "type": "uint256"},
                    {"internalType": "uint160", "name": "sqrtPriceX96After", "type": "uint160"},
                    {"internalType": "uint32", "name": "initializedTicksCrossed", "type": "uint32"},
                    {"internalType": "uint256", "name": "gasEstimate", "type": "uint256"}
                ],
                "stateMutability": "nonpayable",
                "type": "function"
            }
        ]


# Convenience wrapper class
class BaseDEXTrader:
    """
    High-level trading interface for TradingView signals
    
    Handles position management, sizing, and execution
    """
    
    def __init__(self, broker: BaseDEXBroker):
        self.broker = broker
        self.logger = logging.getLogger(__name__)
    
    async def execute_signal(self, signal: Dict) -> Dict:
        """
        Execute TradingView signal on Base DEX
        
        Args:
            signal: {
                'action': 'buy' | 'sell' | 'close',
                'symbol': 'BTC/USD',
                'qty_pct': 90,  # % of equity
                'qty_usdt': 900  # Or fixed USDT amount
            }
        
        Returns:
            Execution result dict
        """
        try:
            action = signal['action'].lower()
            
            # Get current position and balances
            position = await self.broker.get_position()
            balances = await self.broker.get_balances()
            
            usdc_balance = balances['USDC']
            cbbtc_balance = balances['cbBTC']
            
            self.logger.info(f"Executing {action}, USDC: {usdc_balance}, cbBTC: {cbbtc_balance}")
            
            # Handle BUY (LONG entry)
            if action in ['buy', 'long']:
                # Calculate USDC amount to spend
                if signal.get('qty_usdt'):
                    usdc_to_spend = Decimal(str(signal['qty_usdt']))
                elif signal.get('qty_pct'):
                    usdc_to_spend = usdc_balance * Decimal(signal['qty_pct']) / Decimal(100)
                else:
                    usdc_to_spend = usdc_balance * Decimal(0.9)  # Default 90%
                
                # Safety check
                if usdc_to_spend > usdc_balance:
                    raise Exception(f"Insufficient USDC: Need {usdc_to_spend}, have {usdc_balance}")
                
                if usdc_to_spend < Decimal('10'):
                    raise Exception(f"Order too small: ${usdc_to_spend}")
                
                # Execute buy
                result = await self.broker.buy_btc(usdc_to_spend, max_slippage_pct=1.0)
                result['action'] = 'BUY'
                return result
            
            # Handle SELL (SHORT entry or CLOSE)
            elif action in ['sell', 'short', 'close', 'exit']:
                # Sell all cbBTC
                if cbbtc_balance < Decimal('0.00001'):
                    raise Exception(f"No cbBTC to sell: Balance {cbbtc_balance}")
                
                # Execute sell
                result = await self.broker.sell_btc(cbbtc_balance, max_slippage_pct=1.0)
                result['action'] = 'SELL'
                return result
            
            else:
                raise Exception(f"Unknown action: {action}")
                
        except Exception as e:
            self.logger.error(f"Signal execution failed: {str(e)}")
            raise


# Export
__all__ = ['BaseDEXBroker', 'BaseDEXTrader']

