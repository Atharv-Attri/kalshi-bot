import asyncio
import pmxt
from dotenv import load_dotenv
from os import getenv
from rich import print
import json
from dateutil import parser
from pykalshi import MarketStatus, KalshiClient, Action, Side, TimeInForce
from time import sleep, time as utime
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs,
    OrderType,
    OpenOrderParams,
    BalanceAllowanceParams,
    AssetType,
)
from py_clob_client.order_builder.constants import BUY, SELL
from eth_account import Account
from redeem import redeem


class Arb:
    def __init__(self):
        # detection threshold for gross edge
        self.threshold = 0.13      # 13 percent gross edge to trigger
        self.min_edge = 0.04       # 4 percent minimum net edge you want to keep
        self.qty = 5
        self.pad = 0.01            # price padding on each venue

        load_dotenv(".env")
        CLOB_API = "https://clob.polymarket.com"
        SIGNATURE_TYPE = 0

        self.auth_client = ClobClient(
            CLOB_API,
            key=getenv("POLY_PRIV_KEY"),
            chain_id=137,
            signature_type= SIGNATURE_TYPE,
            funder=getenv("WALLET_ADDRESS"),
        )

        creds = self.auth_client.derive_api_key()
        self.auth_client.set_api_creds(creds)

        balance = self.auth_client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        usdc_balance = int(balance["balance"]) / 1e6
        print(f"[green]USDC Balance: ${usdc_balance:.2f}[/green]")

        self.poly = pmxt.Polymarket()
        self.kalshi = KalshiClient.from_env(demo=False)

        self.positions = {}

    # --------------- Kalshi and Poly helpers (sync) ---------------

    def buy_kalshi(self, ticker, side, max_base_price):
        """
        Place a FOK buy on Kalshi at (max_base_price + pad).
        max_base_price is in dollars.
        """
        limit_cents = int((max_base_price + self.pad) * 100)

        try:
            if side == Side.NO:
                order = self.kalshi.portfolio.place_order(
                    ticker,
                    Action.BUY,
                    side,
                    count=self.qty,
                    no_price=limit_cents,
                    time_in_force=TimeInForce.FOK,
                )
            else:
                order = self.kalshi.portfolio.place_order(
                    ticker,
                    Action.BUY,
                    side,
                    count=self.qty,
                    yes_price=limit_cents,
                    time_in_force=TimeInForce.FOK,
                )
        except Exception as e:
            print(f"[red]Kalshi order exception: {e}[/red]")
            return None

        # FOK should either execute or not, but keep legacy cancel logic
        if order is None:
            print("[red]Kalshi order is None[/red]")
            return None

        if getattr(order, "status", None) == "executed":
            return order

        sleep(1)
        try:
            order_after_cancel = self.kalshi.portfolio.cancel_order(order_id=order.order_id)
        except Exception as e:
            print(f"[red]Kalshi cancel exception: {e}[/red]")
            return order

        if getattr(order_after_cancel, "status", None) == "executed":
            return order_after_cancel

        # one more cancel attempt
        try:
            self.kalshi.portfolio.cancel_order(order_id=order.order_id)
        except Exception:
            pass

        return order_after_cancel

    def buy_poly(self, token_id, base_price, condition_id):
        """
        Place a GTC buy on Polymarket.
        base_price is the price BEFORE pad. Final limit = base_price + pad.
        """
        final_price = base_price + self.pad
        limit_order = OrderArgs(
            token_id=token_id,
            price=final_price,   # final price per share
            size=self.qty,
            side=BUY,
        )

        try:
            signed_order = self.auth_client.create_order(limit_order)
        except Exception as e:
            print(f"[red]Poly create_order exception: {e}[/red]")
            return None

        print(f"[yellow]Poly order signed at {final_price:.4f}[/yellow]")

        try:
            response = self.auth_client.post_order(signed_order, OrderType.GTC)
        except Exception as e:
            print(f"[red]Poly post_order exception: {e}[/red]")
            return None

        print("[cyan]Poly order placed[/cyan]")
        print(response)

        try:
            with open("IDS.txt", "a") as f:
                f.write(f"{condition_id}\n")
        except Exception as e:
            print(f"[red]Error writing IDS.txt: {e}[/red]")

        return response

    # --------------- Async arb execution ---------------

    async def execute_arb_pair(
        self,
        leg_name,
        ticker,
        kalshi_side,
        kalshi_max_base,
        poly_token_id,
        poly_base_price,
        condition_id,
    ):
        """
        Execute Kalshi and Poly legs concurrently using asyncio.
        kalshi_max_base and poly_base_price are precomputed base prices (without pad).
        """
        loop = asyncio.get_running_loop()

        print(
            f"[magenta]Executing arb {leg_name}[/magenta]\n"
            f"  Kalshi side: {kalshi_side.name}, max_base={kalshi_max_base:.4f}, "
            f"Poly base={poly_base_price:.4f} (final={poly_base_price + self.pad:.4f})\n"
            f"  ticker={ticker}, condition_id={condition_id}"
        )

        kalshi_future = loop.run_in_executor(
            None, self.buy_kalshi, ticker, kalshi_side, kalshi_max_base
        )
        poly_future = loop.run_in_executor(
            None, self.buy_poly, poly_token_id, poly_base_price, condition_id
        )

        kalshi_order, poly_response = await asyncio.gather(
            kalshi_future, poly_future
        )

        # Evaluate fills
        kalshi_status = getattr(kalshi_order, "status", None) if kalshi_order is not None else None
        kalshi_filled = kalshi_status == "executed"

        poly_filled = False
        poly_status_str = "unknown"
        if poly_response is None:
            poly_status_str = "no_response"
        else:
            # Try to infer fill from response fields
            try:
                ta = poly_response.get("takingAmount")
                poly_filled = bool(ta and float(ta) > 0)
                poly_status_str = f"takingAmount={ta}"
            except Exception:
                poly_status_str = "no_takingAmount_field"

        print(
            f"[blue]{leg_name} result:[/blue]\n"
            f"  Kalshi status: {kalshi_status}, filled={kalshi_filled}\n"
            f"  Poly status: {poly_status_str}, filled={poly_filled}"
        )

        if kalshi_filled and poly_filled:
            print("[green]Both legs filled successfully[/green]")
            return True, True

        if kalshi_filled and not poly_filled:
            print("[red]WARNING: Kalshi filled, Poly did NOT fill. Naked on Kalshi.[/red]")

        if not kalshi_filled and poly_filled:
            print("[red]WARNING: Poly filled, Kalshi did NOT fill. Naked on Poly.[/red]")

        if not kalshi_filled and not poly_filled:
            print("[yellow]Both legs failed or did not fill[/yellow]")

        return kalshi_filled, poly_filled

    # --------------- Main loop ---------------

    async def run(self):
        market = self.kalshi.get_markets(
            limit=100,
            mve_filter="exclude",
            status=MarketStatus.OPEN,
            series_ticker="KXBTC15M",
        )[0]
        ticker = market.ticker
        close = int(parser.isoparse(market.close_time).timestamp())
        entered = False
        condition_id = None

        print(f"[cyan]Starting arb loop on ticker {ticker}[/cyan]")

        while True:
            now = utime()

            if now > close:
                print("[yellow]Market closed, resetting...[/yellow]")
                print("[yellow]Sleeping for 90 seconds...[/yellow]")
                await asyncio.sleep(90)

                market = self.kalshi.get_markets(
                    limit=100,
                    mve_filter="exclude",
                    status=MarketStatus.OPEN,
                    series_ticker="KXBTC15M",
                )[0]
                ticker = market.ticker
                close = int(parser.isoparse(market.close_time).timestamp())
                entered = False

                print(f"[cyan]New market detected: {ticker}[/cyan]")

                # Try redeem previous condition if available
                if condition_id is not None:
                    try:
                        print("[yellow]TRYING TO REDEEM PAST MARKET...[/yellow]")
                        txn = redeem(condition_id)
                        with open("reciept.txt", "a") as f:
                            f.write(f"{ticker} - {condition_id} - {txn}\n")
                    except Exception as e:
                        print(f"[red]Redeem failed: {e}[/red]")
                condition_id = None

            if entered:
                print("[yellow]Already entered in this market, skipping...[/yellow]")
                await asyncio.sleep(10)
                continue

            # get Kalshi quotes
            k = self.kalshi.get_market(ticker)
            ky = float(k.yes_ask / 100) if k.yes_ask is not None else None
            kn = float(k.no_ask / 100) if k.no_ask is not None else None

            if ky is None or kn is None:
                print("[red]Missing Kalshi quotes, retrying...[/red]")
                await asyncio.sleep(1)
                continue

            # get Polymarket quotes
            p = self.poly.call_api(
                "getMarketBySlug", {"slug": f"btc-updown-15m-{close - 900}"}
            )
            yes_id, no_id = json.loads(p["clobTokenIds"])
            condition_id = p["conditionId"]

            best_ask = p.get("bestAsk")
            best_bid = p.get("bestBid")

            if best_ask is None or best_bid is None:
                print("[red]Missing Polymarket bestAsk or bestBid, retrying...[/red]")
                await asyncio.sleep(1)
                continue

            py = float(best_ask)           # YES on Poly
            pn = 1.0 - py                  # NO synthetic price

            gross_edge_yes = 1.0 - (ky + pn)   # YES Kalshi, NO Poly
            gross_edge_no = 1.0 - (kn + py)    # NO Kalshi, YES Poly

            print(
                f"[white]Ky: {ky:.4f}, Kn: {kn:.4f}, Py(bestAsk): {py:.4f}, "
                f"Pn: {pn:.4f}, TD: {close - now:.2f}[/white]"
            )
            print(
                f"[white]Gross edge YES-Kalshi/NO-Poly: {gross_edge_yes:.4f}, "
                f"NO-Kalshi/YES-Poly: {gross_edge_no:.4f}[/white]"
            )

            # strat 1: buy YES Kalshi, buy NO Poly
            if gross_edge_yes >= self.threshold:
                kalshi_est = ky + self.pad
                max_pn_poly = 1.0 - self.min_edge - kalshi_est

                # base price is before pad
                poly_base_price = max_pn_poly - self.pad

                print(
                    f"[green]Arb YES-Kalshi / NO-Poly found[/green]\n"
                    f"  gross_edge={gross_edge_yes:.4f} (threshold={self.threshold:.4f})\n"
                    f"  kalshi_est={kalshi_est:.4f}, max_pn_poly={max_pn_poly:.4f}, "
                    f"poly_base_price={poly_base_price:.4f}"
                )

                if poly_base_price <= 0:
                    print("[red]Computed Poly base price non positive, skipping[/red]")
                else:
                    kalshi_filled, poly_filled = await self.execute_arb_pair(
                        "YES-Kalshi_NO-Poly",
                        ticker,
                        Side.YES,
                        ky,               # base price for Kalshi
                        no_id,
                        poly_base_price,
                        condition_id,
                    )

                    if kalshi_filled and poly_filled:
                        entered = True

            # strat 2: buy NO Kalshi, buy YES Poly
            elif gross_edge_no >= self.threshold:
                kalshi_est = kn + self.pad
                max_py_poly = 1.0 - self.min_edge - kalshi_est
                poly_base_price = max_py_poly - self.pad

                print(
                    f"[green]Arb NO-Kalshi / YES-Poly found[/green]\n"
                    f"  gross_edge={gross_edge_no:.4f} (threshold={self.threshold:.4f})\n"
                    f"  kalshi_est={kalshi_est:.4f}, max_py_poly={max_py_poly:.4f}, "
                    f"poly_base_price={poly_base_price:.4f}"
                )

                if poly_base_price <= 0:
                    print("[red]Computed Poly base price non positive, skipping[/red]")
                else:
                    kalshi_filled, poly_filled = await self.execute_arb_pair(
                        "NO-Kalshi_YES-Poly",
                        ticker,
                        Side.NO,
                        kn,               # base price for Kalshi
                        yes_id,
                        poly_base_price,
                        condition_id,
                    )

                    if kalshi_filled and poly_filled:
                        entered = True

            await asyncio.sleep(1)


if __name__ == "__main__":
    arb = Arb()
    asyncio.run(arb.run())