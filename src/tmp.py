from web3 import Web3
from dotenv import load_dotenv
import os

load_dotenv()

RPC = "https://polygon.drpc.org"

PRIVATE_KEY = os.getenv("POLY_PRIV_KEY")
WALLET_RAW = os.getenv("WALLET_ADDRESS")

if not PRIVATE_KEY:
    raise RuntimeError("POLY_PRIV_KEY is not set")
if not WALLET_RAW:
    raise RuntimeError("WALLET_ADDRESS is not set")

w3 = Web3(Web3.HTTPProvider(RPC))
WALLET = Web3.to_checksum_address(WALLET_RAW)

print("connected:", w3.is_connected())
print("chain:", w3.eth.chain_id)

# ERC1155 ABI fragment for setApprovalForAll and isApprovedForAll
ERC1155_ABI = [
    {
        "name": "setApprovalForAll",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"},
        ],
        "outputs": [],
    },
    {
        "name": "isApprovedForAll",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
]

CONDITIONAL = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")

SPENDERS = [
    Web3.to_checksum_address("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"),
    Web3.to_checksum_address("0xC5d563A36AE78145C45a50134d48A1215220f80a"),
    Web3.to_checksum_address("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"),
]

conditional = w3.eth.contract(address=CONDITIONAL, abi=ERC1155_ABI)


def is_approved(operator):
    return conditional.functions.isApprovedForAll(WALLET, operator).call()


def approve_for_all(operator, nonce):
    tx = conditional.functions.setApprovalForAll(
        operator,
        True
    ).build_transaction(
        {
            "from": WALLET,
            "nonce": nonce,
            "gas": 100000,
            "gasPrice": w3.to_wei("50", "gwei"),
            "chainId": 137,
        }
    )

    signed = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)

    print(f"SetApprovalForAll true for {operator}")
    print("tx:", w3.to_hex(tx_hash))

    return nonce + 1


def main():
    nonce = w3.eth.get_transaction_count(WALLET, "pending")

    print("\nCurrent Conditional Tokens approvals:\n")
    for op in SPENDERS:
        approved = is_approved(op)
        print(f"Operator {op}: {approved}")

    print("\nSending approvals where needed:\n")
    for op in SPENDERS:
        if is_approved(op):
            print(f"Already approved for {op}, skipping")
            continue
        nonce = approve_for_all(op, nonce)


if __name__ == "__main__":
    main()