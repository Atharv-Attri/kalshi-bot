import os
import requests
from dotenv import load_dotenv, find_dotenv
from rich import print

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import SELL
from py_clob_client.exceptions import PolyApiException

POSITIONS_API = "https://data-api.polymarket.com/positions"
CLOB_API = "https://clob.polymarket.com"

class PolyFOKSeller:
    def __init__(self):
        env_path = find_dotenv()
        if env_path:
            os.chdir(os.path.dirname(env_path))

        load_dotenv(".env")

        self.wallet_address = os.getenv("WALLET_ADDRESS")
        self.private_key = os.getenv("POLY_PRIV_KEY")
        self.signature_type = 0
        self.limit_price = 0.47

        self.client = ClobClient(
            CLOB_API,
            key=self.private_key,
            chain_id=137,
            signature_type=self.signature_type,
            funder=self.wallet_address,
        )

        creds = self.client.derive_api_key()
        self.client.set_api_creds(creds)

    def fetch_positions(self):
        resp = requests.get(
            POSITIONS_API,
            params={
                "user": self.wallet_address,
                "sizeThreshold": 0.0,
                "limit": 500,
                "offset": 0,
            },
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()

        positions = []
        for pos in data:
            try:
                size = float(pos.get("size", 0))
            except Exception:
                size = 0.0

            asset_id = pos.get("asset")
            if not asset_id or size <= 0:
                continue

            positions.append({
                "asset_id": asset_id,
                "size": size,
                "title": pos.get("title", "Unknown Market"),
                "outcome": pos.get("outcome", "Unknown Outcome"),
            })

        return positions

    def sell_fok(self, asset_id, size, title, outcome):
        print(f"[cyan]Trying FOK sell:[/cyan] {title} | {outcome} | size={size} | px={self.limit_price}")

        try:
            order_args = MarketOrderArgs(
                token_id=asset_id,
                price=self.limit_price,
                amount=size,
                side=SELL,
                order_type=OrderType.FOK,
            )

            signed = self.client.create_market_order(order_args)
            resp = self.client.post_order(signed, OrderType.FOK)

            print(f"[green]Response:[/green] {resp}")
            return True, resp

        except PolyApiException as e:
            print(e.error_msg)
            err_payload = getattr(e, "error_message", None)
            print(f"[yellow]PolyApiException:[/yellow] {err_payload or e}")
            return False, err_payload or str(e)

        except Exception as e:
            print(f"[red]Unexpected error:[/red] {e}")
            return False, str(e)

    def run(self):
        positions = self.fetch_positions()

        if not positions:
            print("[yellow]No positions found.[/yellow]")
            return

        print(f"[bold white]Found {len(positions)} positions[/bold white]")
        for i, pos in enumerate(positions, start=1):
            print(f"{i}. {pos['title']} | {pos['outcome']} | size={pos['size']}")

        confirm = input("\nType 'sell' to FOK sell all at 0.52: ").strip().lower()
        if confirm != "sell":
            print("[yellow]Aborted.[/yellow]")
            return

        for pos in positions:
            self.sell_fok(
                pos["asset_id"],
                pos["size"],
                pos["title"],
                pos["outcome"],
            )

if __name__ == "__main__":
    PolyFOKSeller().run()