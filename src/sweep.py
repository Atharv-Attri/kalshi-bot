import os
import time
import requests
from dotenv import load_dotenv

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from web3.exceptions import TransactionNotFound
from eth_account import Account


CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CHAIN_ID = 137

CTF_ABI = [
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


def wait_until_visible(w3: Web3, tx_hash: str, timeout_sec: int = 45) -> None:
    start = time.time()
    while True:
        try:
            w3.eth.get_transaction(tx_hash)
            return
        except TransactionNotFound:
            pass

        if time.time() - start > timeout_sec:
            raise RuntimeError(f"Sender RPC does not see tx after broadcast: {tx_hash}")

        time.sleep(1)


def wait_for_receipt(w3: Web3, tx_hash: str, timeout_sec: int = 240) -> dict:
    start = time.time()
    while True:
        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            if receipt is not None:
                return receipt
        except TransactionNotFound:
            pass

        if time.time() - start > timeout_sec:
            raise TimeoutError(f"Timed out waiting for receipt: {tx_hash}")

        time.sleep(2)


def redeem_one(w3: Web3, acct: str, priv: str, condition_id_hex: str) -> str:
    if not condition_id_hex.startswith("0x") or len(condition_id_hex) != 66:
        raise ValueError(f"Bad conditionId: {condition_id_hex}")

    ctf = w3.eth.contract(address=w3.to_checksum_address(CTF_ADDRESS), abi=CTF_ABI)
    condition_id = bytes.fromhex(condition_id_hex[2:])

    nonce = w3.eth.get_transaction_count(acct, "pending")

    base = int(w3.eth.gas_price)
    max_priority = max(int(base * 0.20), w3.to_wei(30, "gwei"))
    max_fee = max(int(base * 2.0), max_priority + w3.to_wei(30, "gwei"))

    fn = ctf.functions.redeemPositions(
        w3.to_checksum_address(USDC_ADDRESS),
        b"\x00" * 32,
        condition_id,
        [1, 2],
    )

    tx = fn.build_transaction(
        {
            "from": acct,
            "nonce": nonce,
            "chainId": CHAIN_ID,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": max_priority,
            "value": 0,
        }
    )

    gas_est = w3.eth.estimate_gas(tx)
    tx["gas"] = int(gas_est * 1.20)

    signed = w3.eth.account.sign_transaction(tx, private_key=priv)

    txh = w3.eth.send_raw_transaction(signed.raw_transaction).hex()
    if not txh.startswith("0x"):
        txh = "0x" + txh

    return txh


def sweep_unpaid_tokens():
    load_dotenv(".env")

    rpc_url = os.getenv("POLY_RPC_URL", "https://polygon.drpc.org")
    wallet_address = os.getenv("WALLET_ADDRESS")
    priv = os.getenv("POLY_PRIV_KEY")

    if not wallet_address or not priv:
        print("[red]Error: WALLET_ADDRESS or POLY_PRIV_KEY missing in .env[/red]")
        return

    wallet_address = Web3.to_checksum_address(wallet_address)

    pk_addr = Web3.to_checksum_address(Account.from_key(priv).address)
    if pk_addr != wallet_address:
        print("[red]Mismatch: WALLET_ADDRESS does not match POLY_PRIV_KEY address[/red]")
        print("WALLET_ADDRESS:", wallet_address)
        print("PRIVKEY_ADDR  :", pk_addr)
        return

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    print(f"[cyan]Scanning Polymarket for unpaid winning tickets in {wallet_address}...[/cyan]")

    url = "https://data-api.polymarket.com/positions"
    params = {
        "user": wallet_address,
        "redeemable": "true",
        "sizeThreshold": "0.1",
        "limit": 100,
    }

    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        positions = r.json()
    except Exception as e:
        print(f"[red]Failed to fetch positions from API: {e}[/red]")
        return

    if not positions:
        print("[green]Your wallet is clean. No unredeemed winning tokens found![/green]")
        return

    print(f"[bold yellow]Found {len(positions)} unredeemed position(s). Starting payout...[/bold yellow]\n")

    for pos in positions:
        title = pos.get("title", "Unknown Market")
        condition_id = pos.get("conditionId")
        size = pos.get("size", 0)

        print(f"Market: {title}")
        print(f"Condition ID: {condition_id}")
        print(f"Shares to cash out: {size}")

        if not condition_id:
            print("[red]Skipping: missing conditionId[/red]\n")
            continue

        try:
            print("Executing redemption transaction...")
            tx_hash = redeem_one(w3, wallet_address, priv, condition_id)
            print(f"Broadcasted: {tx_hash}")

            wait_until_visible(w3, tx_hash, timeout_sec=45)
            receipt = wait_for_receipt(w3, tx_hash, timeout_sec=240)

            if receipt.get("status", 0) != 1:
                raise RuntimeError(f"Reverted on-chain. tx={tx_hash}")

            print(f"[green]Confirmed[/green] in block {receipt.get('blockNumber')}: {tx_hash}\n")

            time.sleep(2)
        except Exception as e:
            print(f"[red]Failed to redeem condition {condition_id}: {e}[/red]\n")


if __name__ == "__main__":
    sweep_unpaid_tokens()