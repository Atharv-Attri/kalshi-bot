from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from dotenv import load_dotenv
from os import getenv


def redeem(condition_id_hex):
    load_dotenv(".env")
    RPC_URL = "https://1rpc.io/matic"
    CHAIN_ID = 137

    CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

    ctf_abi = [
        {
            "name": "redeemPositions",
            "type": "function",
            "stateMutability": "nonpayable",
            "inputs": [
                {"name": "collateralToken", "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId", "type": "bytes32"},
                {"name": "indexSets", "type": "uint256[]"},
            ],
            "outputs": [],
        }
    ]

    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    acct = w3.to_checksum_address(getenv("WALLET_ADDRESS"))
    priv = getenv("PRIVATE_KEY")

    ctf = w3.eth.contract(address=w3.to_checksum_address(CTF_ADDRESS), abi=ctf_abi)

    condition_id = bytes.fromhex(condition_id_hex[2:])

    tx = ctf.functions.redeemPositions(
        w3.to_checksum_address(USDC_E_ADDRESS),
        b"\x00" * 32,
        condition_id,
        [1, 2]        # redeem both Up (1) and Down (2) if you hold any
    ).build_transaction({
        "from": acct,
        "nonce": w3.eth.get_transaction_count(acct),
        "chainId": CHAIN_ID,
        "gasPrice": w3.eth.gas_price,
    })

    signed = w3.eth.account.sign_transaction(tx, private_key=priv)
    txh = w3.eth.send_raw_transaction(signed.raw_transaction)
    print("Redeem tx:", txh.hex())
    return txh.hex()