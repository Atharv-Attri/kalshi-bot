from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

# - - CONFIG - - #

RPC_URLS = [
    "https://polygon-rpc.com",
    "https://1rpc.io/matic",
    "https://rpc.ankr.com/polygon",
]

CHAIN_ID = 137

# YOUR wallet
priv_key = ""  # hex string from MetaMask export
pub_key = ""  # 0x... same address you funded

# USDC.e and CTF contracts on Polygon
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e
CTF_ADDRESS  = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # CTF (ERC1155)

# Spenders that need approval
SPENDERS = {
    "ctf_exchange":      "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "negrisk_exchange":  "0xC5d563A36AE78145C45a50134d48A1215220f80a",
    "negrisk_adapter":   "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",
}

# Allowance we want to set: max uint256
MAX_ALLOWANCE = 2**256 - 1

# Minimal ABIs
erc20_approve_abi = [
    {
        "constant": False,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_value",   "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

erc1155_set_approval_abi = [
    {
        "inputs": [
            {"internalType": "address", "name": "operator", "type": "address"},
            {"internalType": "bool",    "name": "approved", "type": "bool"},
        ],
        "name": "setApprovalForAll",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# - - CONNECT TO POLYGON - - #

def get_web3():
    last_error = None
    for url in RPC_URLS:
        print(f"Trying RPC: {url}")
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 60}))
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        try:
            if w3.is_connected():
                block_number = w3.eth.block_number
                print(f"Connected to {url}, block {block_number}")
                return w3
        except Exception as e:
            print(f"Failed on {url}: {e}")
            last_error = e
    raise SystemExit(f"Could not connect to any Polygon RPC. Last error: {last_error}")

web3 = get_web3()

acct = web3.to_checksum_address(pub_key)
print("Account:", acct)

usdc = web3.eth.contract(
    address=web3.to_checksum_address(USDC_ADDRESS),
    abi=erc20_approve_abi,
)
ctf = web3.eth.contract(
    address=web3.to_checksum_address(CTF_ADDRESS),
    abi=erc1155_set_approval_abi,
)

gas_price = web3.eth.gas_price
print("Gas price (wei):", gas_price)

# - - TX SENDER - - #

def send_tx(tx_dict, label):
    signed = web3.eth.account.sign_transaction(tx_dict, private_key=priv_key)
    tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"[{label}] sent:", tx_hash.hex())
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=600)
    status = "SUCCESS" if receipt.status == 1 else "FAILED"
    print(f"[{label}] status: {status}, gas used: {receipt.gasUsed}")
    print(f"  Explorer: https://polygonscan.com/tx/{tx_hash.hex()}")
    if receipt.status != 1:
        raise SystemExit(f"Transaction {label} failed")
    return receipt

# - - MAIN - - #

def main():
    nonce = web3.eth.get_transaction_count(acct)
    print("Starting nonce:", nonce)

    for name, spender_raw in SPENDERS.items():
        spender = web3.to_checksum_address(spender_raw)

        # 1) Approve USDC.e for this spender
        print(f"\nApproving USDC.e for {name} ({spender})")
        approve_tx = usdc.functions.approve(
            spender,
            MAX_ALLOWANCE,
        ).build_transaction(
            {
                "chainId": CHAIN_ID,
                "from": acct,
                "nonce": nonce,
                "gasPrice": gas_price,
            }
        )
        send_tx(approve_tx, f"USDC.approve -> {name}")
        nonce += 1

        # 2) setApprovalForAll on CTF for this spender
        print(f"Setting CTF setApprovalForAll for {name} ({spender})")
        ctf_tx = ctf.functions.setApprovalForAll(
            spender,
            True,
        ).build_transaction(
            {
                "chainId": CHAIN_ID,
                "from": acct,
                "nonce": nonce,
                "gasPrice": gas_price,
            }
        )
        send_tx(ctf_tx, f"CTF.setApprovalForAll -> {name}")
        nonce += 1

    print("\nAll approvals completed successfully.")

if __name__ == "__main__":
    main()