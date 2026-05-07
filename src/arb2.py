import os
import asyncio
import pmxt
import requests
import pytz
from dotenv import load_dotenv, find_dotenv
from rich import print
import json
from dateutil import parser
from time import time as utime
from datetime import datetime

from pykalshi import (
    MarketStatus,
    KalshiClient,
    Action,
    Side,
    TimeInForce,
    Feed,
    TickerMessage,
)

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    BalanceAllowanceParams,
    AssetType,
    MarketOrderArgs,
    OrderType,
)
from py_clob_client.order_builder.constants import BUY
from py_clob_client.exceptions import PolyApiException

import websockets
from redeem import redeem
from time import sleep

POLYMARKET_MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLY_GAMMA_API = "https://gamma-api.polymarket.com/events" 

class ArbNBA:
    def __init__(self, auto_refresh: bool = True):
        self.threshold = 0.07   
        self.qty = 7            
        self.pad = 0.01         
        self.auto_refresh = auto_refresh
        self.TODAY = ""

        # SLEDGEHAMMER PATH FIX
        env_path = find_dotenv()
        if env_path:
            root_dir = os.path.dirname(env_path)
            os.chdir(root_dir)
            
        load_dotenv(".env")
        CLOB_API = "https://clob.polymarket.com"
        SIGNATURE_TYPE = 0

        # Polymarket Auth Setup
        self.auth_client = ClobClient(
            CLOB_API,
            key=os.getenv("POLY_PRIV_KEY"),
            chain_id=137,
            signature_type=SIGNATURE_TYPE,
            funder=os.getenv("WALLET_ADDRESS"),
        )
        creds = self.auth_client.derive_api_key()
        self.auth_client.set_api_creds(creds)

        balance = self.auth_client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        usdc_balance = int(balance["balance"]) / 1e6
        print(f"[green]Poly USDC Balance: ${usdc_balance:.2f}[/green]")

        self.poly = pmxt.Polymarket()
        self.kalshi = KalshiClient.from_env(demo=False)
        self.pt = pytz.timezone("America/Los_Angeles")
        
        self._bal_cache = None
        self._bal_cache_ts = 0.0
        self._bal_cache_ttl = 10.0

        # MULTI-GAME STATE MANAGEMENT
        self.monitored_tickers = set() 
        self.books = {}                
        self.game_meta = {}            

        self.team_abbrs = {
            "hawks": "ATL", "celtics": "BOS", "nets": "BKN", "hornets": "CHA",
            "bulls": "CHI", "cavaliers": "CLE", "mavericks": "DAL", "nuggets": "DEN",
            "pistons": "DET", "warriors": "GSW", "rockets": "HOU", "pacers": "IND",
            "clippers": "LAC", "lakers": "LAL", "grizzlies": "MEM", "heat": "MIA",
            "bucks": "MIL", "timberwolves": "MIN", "pelicans": "NOP", "knicks": "NYK",
            "thunder": "OKC", "magic": "ORL", "76ers": "PHI", "suns": "PHX",
            "trail blazers": "POR", "blazers": "POR", "kings": "SAC", "spurs": "SAS",
            "raptors": "TOR", "jazz": "UTA", "wizards": "WAS"
        }

    def get_balance_cached(self) -> float:
        now = utime()
        if self._bal_cache is None or (now - self._bal_cache_ts) >= self._bal_cache_ttl:
            try:
                bal = self.kalshi.portfolio.get_balance()
                self._bal_cache = bal.portfolio_value + bal.balance
                self._bal_cache_ts = now
            except Exception as e:
                print(f"[red]Error fetching Kalshi balance: {e}[/red]")
                if self._bal_cache is None:
                    return 0.0
        return float(self._bal_cache)

    def _log_event(self, msg: str):
        ts = datetime.fromtimestamp(utime()).isoformat()
        line = f"[{ts}] {msg}\n"
        try:
            with open("nba_arb.log", "a") as f:
                f.write(line)
        except Exception as e:
            print(f"[red]Failed to write nba_arb.log: {e}[/red]")

    def _poly_place_fok(self, token_id, base_price, condition_id):
        final_price = base_price + self.pad
        amount = self.qty * final_price

        if amount < 1.0:
            msg = f"Poly amount below 1 dollar min (amount={amount:.4f}) for token_id={token_id}. Skipping order."
            print(f"[yellow]{msg}[/yellow]")
            self._log_event(msg)
            return False, None, {"success": False, "status": "skip"}

        msg = f"trying to buy on Poly token_id={token_id} base_price={base_price:.4f} final_price={final_price:.4f} amount={amount:.4f}"
        print(f"[bold green]{msg}[/bold green]")
        self._log_event(msg)

        try:
            mo = MarketOrderArgs(
                token_id=token_id, price=final_price, amount=amount,
                side=BUY, order_type=OrderType.FOK,
            )
            signed = self.auth_client.create_market_order(mo)
            resp = self.auth_client.post_order(signed, OrderType.FOK)
        except PolyApiException as e:
            err_payload = getattr(e, "error_message", None)
            msg = f"PolyApiException in FOK order: {err_payload or e}"
            print(f"[yellow]{msg}[/yellow]")
            self._log_event(msg)
            return False, None, {"success": False, "status": "killed", "error": err_payload or str(e)}
        except Exception as e:
            msg = f"Unexpected Poly error in FOK order: {e}"
            print(f"[red]{msg}[/red]")
            self._log_event(msg)
            return False, None, {"success": False, "status": "error", "error": str(e)}

        self._log_event(f"Poly response: {resp}")
        
        success = resp.get("success", False)
        status = resp.get("status")
        order_id = resp.get("orderID")
        taking_raw = resp.get("takingAmount") or "0"
        
        try: taking_amt = float(taking_raw)
        except Exception: taking_amt = 0.0

        filled = bool(success and status in ("matched", "delayed") and taking_amt > 0)

        if filled:
            msg = f"Poly limit FOK filled (status={status}, takingAmount={taking_amt}) order_id={order_id}"
            print(f"[green]{msg}[/green]")
            self._log_event(msg)
            try:
                with open("IDS.txt", "a") as f:
                    f.write(f"{condition_id}\n")
            except Exception as e:
                print(f"[red]Error writing IDS.txt: {e}[/red]")
        else:
            msg = f"Poly limit FOK did NOT fill (success={success}, status={status}, takingAmount={taking_amt})"
            print(f"[yellow]{msg}[/yellow]")
            self._log_event(msg)

        return filled, order_id, resp

    async def _poly_place_fok_async(self, token_id, base_price, condition_id):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._poly_place_fok, token_id, base_price, condition_id)

    def _kalshi_buy_fok(self, ticker, side, max_base_price):
        limit_cents = int((max_base_price + self.pad) * 100)
        try:
            if side == Side.NO:
                order = self.kalshi.portfolio.place_order(
                    ticker, Action.BUY, side, count=self.qty,
                    no_price=limit_cents, time_in_force=TimeInForce.FOK,
                )
            else:
                order = self.kalshi.portfolio.place_order(
                    ticker, Action.BUY, side, count=self.qty,
                    yes_price=limit_cents, time_in_force=TimeInForce.FOK,
                )
        except Exception as e:
            msg = f"Kalshi FOK exception: {e}"
            print(f"[red]{msg}[/red]")
            self._log_event(msg)
            return None
        return order

    async def _kalshi_buy_fok_async(self, ticker, side, max_base_price):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._kalshi_buy_fok, ticker, side, max_base_price)

    def fetch_and_match_nba_markets(self):
        print("[cyan]Fetching NBA Markets from Kalshi and Polymarket...[/cyan]")
        matches = []
        
        try:
            poly_resp = requests.get(POLY_GAMMA_API, params={"limit": 100000, "closed": "false", "tag_id": "745", "negRisk": "false"}) 
            poly_events = poly_resp.json()
        except Exception as e:
            print(f"[red]Failed to fetch Poly markets: {e}[/red]")
            return []

        try:
            k_markets = self.kalshi.get_markets(
                limit=100, 
                mve_filter="exclude", 
                status=MarketStatus.OPEN, 
                series_ticker="KXNBAGAME"
            )
        except Exception as e:
            print(f"[red]Failed to fetch Kalshi markets: {e}[/red]")
            return []

        exclude_keywords = [
            "spread", "over/under", "o/u", "points", "rebounds", 
            "assists", "series", "championship", "finals", "draft", "mvp", "+", "-", "moneyline"
        ]

        for p_event in poly_events:
            if self.TODAY not in p_event["slug"]: continue
            p_title = p_event["title"]
            title_lower = p_title.lower()
            
            if " vs " not in title_lower and " vs. " not in title_lower: continue
            if any(kw in title_lower for kw in exclude_keywords): continue
            if not p_event.get("markets"): continue
            
            out = []
            for market in p_event["markets"]:
                #print(market["question"])
                if " vs " not in market["question"] and " vs. " not in market["question"]: continue
                if any(kw in market["question"].lower() for kw in exclude_keywords): continue
                out.append(market)

            #print(out)


            p_market = out[0]
            outcomes = p_market.get("outcomes", [])
            if len(outcomes) < 2: continue

            yes_outcome = str(outcomes[0]).lower()
            poly_yes_abbr = None
            
            for mascot, abbr in self.team_abbrs.items():
                if mascot in yes_outcome:
                    poly_yes_abbr = abbr
                    break
            
            if not poly_yes_abbr:
                first_team_str = title_lower.split(" vs")[0]
                for mascot, abbr in self.team_abbrs.items():
                    if mascot in first_team_str:
                        poly_yes_abbr = abbr
                        break
                        
            if not poly_yes_abbr: continue

            teams_in_game = []
            for mascot, abbr in self.team_abbrs.items():
                if mascot in title_lower:
                    teams_in_game.append(abbr)

            if len(teams_in_game) < 2: continue

            for k_market in k_markets:
                k_ticker = k_market.ticker
                
                if teams_in_game[0] in k_ticker and teams_in_game[1] in k_ticker:
                    if k_ticker.endswith(f"-{poly_yes_abbr}"):
                        try:
                            yes_id, no_id = json.loads(p_market["clobTokenIds"])
                            matches.append({
                                "kalshi_ticker": k_ticker,
                                "poly_yes_id": yes_id,
                                "poly_no_id": no_id,
                                "poly_condition_id": p_market.get("conditionId"),
                                "close_ts": int(parser.isoparse(p_market["endDate"]).timestamp()),
                                "title": p_title,
                                "poly_slug": p_event.get("slug", "unknown-slug")
                            })
                            print(p_event.get("slug"), k_ticker, p_market['id'])

                        except Exception as e:
                            print(f"[yellow]Failed to parse token IDs for {p_title}: {e}[/yellow]")
                            continue
        return matches

    async def _kalshi_ws_task(self, ticker: str, close_ts: int):
        def handle_msg(msg: TickerMessage):
            if msg.market_ticker != ticker:
                return
            if msg.yes_bid is None or msg.yes_ask is None:
                return

            yes_bid = msg.yes_bid / 100.0
            yes_ask = msg.yes_ask / 100.0
            no_bid = 1.0 - yes_ask
            no_ask = 1.0 - yes_bid

            self.books[ticker]["kalshi"]["yes_bid"] = yes_bid
            self.books[ticker]["kalshi"]["yes_ask"] = yes_ask
            self.books[ticker]["kalshi"]["no_bid"] = no_bid
            self.books[ticker]["kalshi"]["no_ask"] = no_ask

        with Feed(self.kalshi) as feed:
            @feed.on("ticker")
            def on_ticker(msg: TickerMessage):
                try: handle_msg(msg)
                except Exception as e: pass

            feed.subscribe("ticker", market_tickers=[ticker])

            while True:
                if utime() > close_ts + 60:
                    msg = f"Kalshi ws task exit for {ticker}"
                    print(f"[yellow]{msg}[/yellow]")
                    self._log_event(msg)
                    break
                await asyncio.sleep(1.0)

    async def _poly_ws_task(self, ticker: str, asset_ids: list, close_ts: int):
        while True:
            now = utime()
            if now > close_ts + 60:
                msg = f"Poly ws task exit (past close) for {ticker}"
                print(f"[yellow]{msg}[/yellow]")
                self._log_event(msg)
                return

            try:
                async with websockets.connect(POLYMARKET_MARKET_WS, ping_interval=20, ping_timeout=20) as ws:
                    sub_msg = {"assets_ids": asset_ids, "type": "market", "custom_feature_enabled": True}
                    await ws.send(json.dumps(sub_msg))
                    
                    msg_sub = f"POLY subscribed {asset_ids}"
                    print(f"[cyan]{msg_sub}[/cyan]")
                    #self._log_event(msg_sub)

                    async for raw in ws:
                        if utime() > close_ts + 60:
                            msg = f"Poly ws task exit (past close, inner loop) for {ticker}"
                            print(f"[yellow]{msg}[/yellow]")
                            self._log_event(msg)
                            return
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        if isinstance(msg, list):
                            for item in msg: self._handle_poly_event(ticker, item)
                        else:
                            self._handle_poly_event(ticker, msg)

            except Exception as e:
                await asyncio.sleep(3)

    def _handle_poly_event(self, ticker: str, data: dict):
        if not isinstance(data, dict) or data.get("event_type") != "best_bid_ask":
            return

        asset_id = data.get("asset_id")
        bb = float(data.get("best_bid")) if data.get("best_bid") is not None else None
        ba = float(data.get("best_ask")) if data.get("best_ask") is not None else None

        poly_yes_id = self.game_meta[ticker]["poly_yes_id"]
        poly_no_id = self.game_meta[ticker]["poly_no_id"]

        if asset_id == poly_yes_id:
            self.books[ticker]["poly"]["yes_bid"] = bb
            self.books[ticker]["poly"]["yes_ask"] = ba
        elif asset_id == poly_no_id:
            self.books[ticker]["poly"]["no_bid"] = bb
            self.books[ticker]["poly"]["no_ask"] = ba

    async def _arb_monitor_task(self, ticker: str, close_ts: int):
        last_print = 0
        entered_poly = False
        kalshi_hedged = False
        poly_side = None
        kalshi_side = None
        last_poly_attempt = 0.0
        last_kalshi_attempt = 0.0

        POLY_RETRY_COOLDOWN = 0.5
        KALSHI_RETRY_COOLDOWN = 0.5

        poly_condition_id = self.game_meta[ticker]["poly_condition_id"]
        poly_yes_id = self.game_meta[ticker]["poly_yes_id"]
        poly_no_id = self.game_meta[ticker]["poly_no_id"]

        while True:
            now = utime()
            if now > close_ts + 60:
                msg = f"Arb monitor exit for {ticker}"
                print(f"[yellow]{msg}[/yellow]")
                self._log_event(msg)
                break

            kb = self.books[ticker]["kalshi"]
            pb = self.books[ticker]["poly"]

            Ky, Kn = kb.get("yes_ask"), kb.get("no_ask")
            Py, Pn = pb.get("yes_ask"), pb.get("no_ask")

            if None not in (Ky, Kn, Py, Pn):
                gross_edge_yes = 1.0 - (Ky + Pn)  
                gross_edge_no = 1.0 - (Kn + Py)   

                if now - last_print >= 5.0:  
                    last_print = now
                    print(
                        f"[ARB-STATUS] {ticker} "
                        f"Ky={Ky:.4f} Kn={Kn:.4f} Py={Py:.4f} Pn={Pn:.4f} | "
                        f"edge_yes={gross_edge_yes:.4f} edge_no={gross_edge_no:.4f}"
                    )

                if not entered_poly and poly_condition_id is not None:
                    
                    if max(gross_edge_yes, gross_edge_no) >= self.threshold:
                         if self.get_balance_cached() < (self.qty * 100): 
                             print(f"[red]Insufficient Kalshi balance to complete hedge for {ticker}! Skipping.[/red]")
                             await asyncio.sleep(5)
                             continue

                    if gross_edge_yes >= self.threshold and now - last_poly_attempt >= POLY_RETRY_COOLDOWN:
                        last_poly_attempt = now
                        poly_base_price = Pn
                        
                        msg = f"Trigger: YES-Kalshi / NO-Poly edge={gross_edge_yes:.4f} Pn={Pn:.4f} final_exec_hint={poly_base_price + self.pad:.4f}"
                        print(f"[green]{msg}[/green]")
                        self._log_event(msg)
                        
                        poly_side, kalshi_side = "NO", "YES"
                        filled, order_id, resp = await self._poly_place_fok_async(poly_no_id, poly_base_price, poly_condition_id)

                        if filled:
                            entered_poly = True
                            msg_entered = f"Entered Poly NO first for strat YES-Kalshi / NO-Poly (token_id={poly_no_id}, order_id={order_id})"
                            print(f"[bold green]{msg_entered}[/bold green]")
                            self._log_event(msg_entered)
                        else:
                            poly_side, kalshi_side = None, None
                            msg_fail = "Poly NO FOK did not fill. Position not entered."
                            print(f"[yellow]{msg_fail}[/yellow]")
                            self._log_event(msg_fail)

                    elif gross_edge_no >= self.threshold and now - last_poly_attempt >= POLY_RETRY_COOLDOWN:
                        last_poly_attempt = now
                        poly_base_price = Py
                        
                        msg = f"Trigger: NO-Kalshi / YES-Poly edge={gross_edge_no:.4f} Py={Py:.4f} final_exec_hint={poly_base_price + self.pad:.4f}"
                        print(f"[green]{msg}[/green]")
                        self._log_event(msg)
                        
                        poly_side, kalshi_side = "YES", "NO"
                        filled, order_id, resp = await self._poly_place_fok_async(poly_yes_id, poly_base_price, poly_condition_id)

                        if filled:
                            entered_poly = True
                            msg_entered = f"Entered Poly YES first for strat NO-Kalshi / YES-Poly (token_id={poly_yes_id}, order_id={order_id})"
                            print(f"[bold green]{msg_entered}[/bold green]")
                            self._log_event(msg_entered)
                        else:
                            poly_side, kalshi_side = None, None
                            msg_fail = "Poly YES FOK did not fill. Position not entered."
                            print(f"[yellow]{msg_fail}[/yellow]")
                            self._log_event(msg_fail)


                if entered_poly and not kalshi_hedged and kalshi_side is not None:
                    if now - last_kalshi_attempt >= KALSHI_RETRY_COOLDOWN:
                        last_kalshi_attempt = now
                        max_px = Ky if kalshi_side == "YES" else Kn
                        order = await self._kalshi_buy_fok_async(ticker, Side.YES if kalshi_side == "YES" else Side.NO, max_px)

                        status = getattr(order, "status", None) if order is not None else None
                        if status == "executed":
                            kalshi_hedged = True
                            msg_hedge = f"Kalshi hedge filled (side={kalshi_side}, px<={max_px:.4f}). Fully hedged now."
                            print(f"[bold green]{msg_hedge}[/bold green]")
                            self._log_event(msg_hedge)
                        else:
                            msg_fail = f"Kalshi hedge FAILED for {ticker}. Retrying..."
                            print(f"[bold red]{msg_fail}[/bold red]")
                            self._log_event(msg_fail)

            await asyncio.sleep(0.2)
            
        # Redeem polymarket condition after the game concludes
        if poly_condition_id is not None:
            msg_start = f"Redeem start condition_id={poly_condition_id}"
            print(f"[yellow]{msg_start}[/yellow]")
            self._log_event(msg_start)
            try:
                txn = redeem(poly_condition_id)
                msg_succ = f"Redeem success condition_id={poly_condition_id} tx={txn}"
                print(f"[green]{msg_succ}[/green]")
                self._log_event(msg_succ)
                try:
                    with open("reciept.txt", "a") as f:
                        f.write(f"{ticker} - {poly_condition_id} - {txn}\n")
                except Exception as e:
                    self._log_event(f"Failed to write reciept.txt: {e}")
            except Exception as e:
                msg_fail = f"Redeem failed for {poly_condition_id}: {e}"
                print(f"[red]{msg_fail}[/red]")
                self._log_event(msg_fail)

    async def nba_arb_loop(self):
        self._log_event("\n[OVERNIGHT / NBA ARB RESTART]")
        while True:
            print(f"[magenta]Kalshi Balance: ${self.get_balance_cached() / 100:.2f}[/magenta]")
            matched_games = self.fetch_and_match_nba_markets()
            
            new_games_found = 0
            
            for game in matched_games:
                ticker = game["kalshi_ticker"]
                
                # Skip if we are already monitoring this specific game
                if ticker in self.monitored_tickers:
                    continue
                    
                new_games_found += 1
                close_ts = game["close_ts"]
                poly_yes_id = game["poly_yes_id"]
                poly_no_id = game["poly_no_id"]
                poly_condition_id = game["poly_condition_id"]

                # Log identical to your required format
                msg_new = f"\nNew Kalshi market {ticker}, close_ts={close_ts}"
                print(f"[bold white]{msg_new}[/bold white]")
                self._log_event(msg_new)
                
                self._log_event(f"Poly market {game['poly_slug']} id=unknown") # Added to match your log style
                self._log_event(f"Poly ids YES={poly_yes_id} NO={poly_no_id} conditionId={poly_condition_id}")
                
                # Initialize the isolated state for this game
                self.monitored_tickers.add(ticker)
                self.books[ticker] = {
                    "kalshi": {"yes_bid": None, "yes_ask": None, "no_bid": None, "no_ask": None},
                    "poly": {"yes_bid": None, "yes_ask": None, "no_bid": None, "no_ask": None}
                }
                self.game_meta[ticker] = {
                    "poly_yes_id": poly_yes_id,
                    "poly_no_id": poly_no_id,
                    "poly_condition_id": poly_condition_id
                }

                # Spin up the monitoring tasks in the background
                asyncio.create_task(self._kalshi_ws_task(ticker, close_ts))
                asyncio.create_task(self._poly_ws_task(ticker, [poly_yes_id, poly_no_id], close_ts))
                asyncio.create_task(self._arb_monitor_task(ticker, close_ts))

            if new_games_found == 0:
                print("[yellow]No new NBA matches to subscribe to. Waiting...[/yellow]")
            else:
                self._log_event("Refreshing to next market search interval")
                
            # Sleep for 5 minutes before pinging the APIs again for newly added games
            await asyncio.sleep(300)

if __name__ == "__main__":
    arb = ArbNBA(auto_refresh=True)
    asyncio.run(arb.nba_arb_loop())