from web3 import Web3
from eth_account import Account
import json
from datetime import datetime, timedelta

class BlockchainManager:
    def __init__(self):
        # Connect to local Ethereum node (use Infura in production)
        self.w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))
        
        # Smart contract details
        self.contract_address = None
        self.contract_abi = self._load_contract_abi()
        self.contract = None
        
        # Initialize contract
        self._initialize_contract()
        
        # Token tracking
        self.token_operations = []

    def _load_contract_abi(self):
        """Load the ABI for the token storage smart contract"""
        # This would load the actual ABI in production
        return [
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "token",
                        "type": "string"
                    }
                ],
                "name": "storeToken",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "getTokenCount",
                "outputs": [
                    {
                        "internalType": "uint256",
                        "name": "",
                        "type": "uint256"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            }
        ]

    def _initialize_contract(self):
        """Initialize the smart contract"""
        if self.contract_address and self.contract_abi:
            self.contract = self.w3.eth.contract(
                address=self.contract_address,
                abi=self.contract_abi
            )

    def store_token(self, token: str):
        """Store a token on the blockchain"""
        try:
            # In production, this would actually store on blockchain
            # For demo, we'll just track it locally
            self.token_operations.append({
                'token': token,
                'timestamp': datetime.now(),
                'operation': 'store'
            })
            return True
        except Exception as e:
            print(f"Error storing token: {e}")
            return False

    def validate_token(self, token: str) -> bool:
        """Validate a token exists on the blockchain"""
        try:
            # In production, this would check the blockchain
            return any(op['token'] == token for op in self.token_operations)
        except Exception as e:
            print(f"Error validating token: {e}")
            return False

    def get_total_tokens(self) -> int:
        """Get total number of tokens stored"""
        return len(self.token_operations)

    def get_active_tokens(self) -> int:
        """Get number of active tokens"""
        # In production, this would check token validity on blockchain
        return len([op for op in self.token_operations if not op.get('revoked')])

    def get_recent_operations(self) -> int:
        """Get number of token operations in last 24 hours"""
        yesterday = datetime.now() - timedelta(days=1)
        return len([op for op in self.token_operations 
                   if op['timestamp'] > yesterday])

    def revoke_token(self, token: str):
        """Revoke a token"""
        try:
            # Mark token as revoked
            for op in self.token_operations:
                if op['token'] == token:
                    op['revoked'] = True
                    op['revoke_timestamp'] = datetime.now()
            return True
        except Exception as e:
            print(f"Error revoking token: {e}")
            return False
