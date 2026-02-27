import pmxt
from dotenv import load_dotenv
from os import getenv
from rich import print
import json
from dateutil import parser
from pykalshi import MarketStatus, CandlestickPeriod, KalshiClient, Action, Side, TimeInForce
from time import sleep, time as utime
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, MarketOrderArgs, OrderType, OpenOrderParams, BalanceAllowanceParams, AssetType
from py_clob_client.order_builder.constants import BUY, SELL
from eth_account import Account
from redeem import redeem
class Arb:
    def __init__(self):
        
        self.threshold = 0.1
        self.qty = 5


        load_dotenv(".env")
        CLOB_API = "https://clob.polymarket.com"
        SIGNATURE_TYPE = 0


        self.auth_client = ClobClient(
            CLOB_API,
            key=getenv("POLY_PRIV_KEY"),
            chain_id=137,
            signature_type=SIGNATURE_TYPE,
            funder=getenv("WALLET_ADDRESS")
)

        creds = self.auth_client.derive_api_key()
        
        self.auth_client.set_api_creds(creds)
        balance = self.auth_client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        usdc_balance = int(balance['balance']) / 1e6
        print(f"USDC Balance: ${usdc_balance:.2f}")
        self.poly = pmxt.Polymarket()
        self.kalshi = KalshiClient.from_env(demo = False)

        self.positions = {}

    def buy_kalshi(self, ticker, side, max):
        if side == Side.NO:
            order = self.kalshi.portfolio.place_order(ticker, Action.BUY, side, count=1, no_price=int(max*100), time_in_force=TimeInForce.GTC)
        else:
            order = self.kalshi.portfolio.place_order(ticker, Action.BUY, side, count=1, yes_price=int(max*100), time_in_force=TimeInForce.GTC)

        if order.status == "executed":
            return order

        sleep(1)
        order = self.kalshi.portfolio.cancel_order(order_id=order.order_id)
        if order.status == "executed":
            return order

        order = self.kalshi.portfolio.cancel_order(order_id=order.order_id)
        return order

    def buy_poly(self, token_id, price, id):
        limit_order = OrderArgs(
            token_id=token_id,
            price=price, # Price per share
            size=self.qty, # Number of shares
            side=BUY
        )

        # Sign the order
        signed_order = self.auth_client.create_order(limit_order)
        print("Order signed!")

        response = self.auth_client.post_order(signed_order, OrderType.GTC)
        print(f"Order placed!")
        print(response)

        with open("IDS.txt", "a") as f:
            f.write(f"{id}\n")


    def run(self):
        market = self.kalshi.get_markets(
            limit=100,
            mve_filter="exclude",
            status=MarketStatus.OPEN,
            series_ticker="KXBTC15M",
        )[0]
        ticker = market.ticker
        close = int(parser.isoparse(market.close_time).timestamp())
        entered = False
        while True:
            if utime() > close:
                print("Market closed, resetting...")
                print("Sleeping for 90 seconds...")
                sleep(90)
                market = self.kalshi.get_markets(
                    limit=100,
                    mve_filter="exclude",
                    status=MarketStatus.OPEN,
                    series_ticker="KXBTC15M",
                )[0]
                ticker = market.ticker
                close = int(parser.isoparse(market.close_time).timestamp())
                entered = False

                print("TRYING TO REDEEM PAST MARKET...")
                txn = redeem(condition_id)
                with open("reciept.txt", "a") as f:
                    f.write(f"{ticker} - {condition_id} - {txn}\n")

                

            if entered:
                print("Already entered, skipping...")
                sleep(5)
                continue

            k = self.kalshi.get_market(ticker)
            print(k)
            ky, kn = 0,0
            ky = float(k.yes_ask_dollars)
            kn = float(k.no_ask_dollars)
            p = self.poly.call_api("getMarketBySlug", {"slug": f"btc-updown-15m-{close - 900}"})
            yes_id, no_id = json.loads(p["clobTokenIds"])
            condition_id = p["conditionId"]
            if p["bestAsk"] is None:
                sleep(1)
                continue
            py =p["bestAsk"]
            pn = 1 - py
            
            # strat 1: buy YES kalshi, buy NO poly
            if 1- (ky + pn) > self.threshold:
                print("Arbitrage opportunity found! Buying YES on Kalshi and NO on Poly")
                kalshi_order = self.buy_kalshi(ticker, Side.YES, ky+0.01)
                if kalshi_order.status == "executed":
                    print("Kalshi order executed!")
                    self.buy_poly(no_id, pn+0.01, condition_id)
                    entered = True
                else:
                    print("Kalshi order failed or partially filled, skipping...")

            # strat 2: buy NO kalshi, buy YES poly
            elif 1- (kn + py) > self.threshold:
                print("Arbitrage opportunity found! Buying NO on Kalshi and YES on Poly")
                kalshi_order = self.buy_kalshi(ticker, Side.NO, kn+0.01)
                if kalshi_order.status == "executed":
                    print("Kalshi order executed!")
                    self.buy_poly(yes_id, py+0.01, condition_id)
                    entered = True
                else:
                    print("Kalshi order failed or partially filled, skipping...")

            print(f"Ky: {ky}, Kn: {kn}, Py: {py}, Pn: {pn}, TD: {close - utime()}")
            sleep(1)

if __name__ == "__main__":
    arb = Arb()
    arb.run()