import asyncio
import pmxt
from dotenv import load_dotenv
from os import getenv
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


POLYMARKET_MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class Arb:
    def __init__(self, auto_refresh: bool = True):
        # Detection and execution parameters
        self.threshold = 0.09   # gross edge needed to trigger
        self.qty = 7
        self.pad = 0.02         # padding added to both venues
        self.auto_refresh = auto_refresh

        load_dotenv(".env")
        CLOB_API = "https://clob.polymarket.com"
        SIGNATURE_TYPE = 0

        self.auth_client = ClobClient(
            CLOB_API,
            key=getenv("POLY_PRIV_KEY"),
            chain_id=137,
            signature_type=SIGNATURE_TYPE,
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

        # Live books for v2 (websocket driven)
        self.kalshi_book = {
            "yes_bid": None,
            "yes_ask": None,
            "no_bid": None,
            "no_ask": None,
        }
        self.poly_book = {
            "yes_bid": None,
            "yes_ask": None,
            "no_bid": None,
            "no_ask": None,
        }

        # Polymarket YES/NO token ids and condition id for current market (set in v2)
        self.poly_yes_id = None
        self.poly_no_id = None
        self.poly_condition_id = None

    # -------------------- Logging helper --------------------

    def _log_event(self, msg: str):
        """
        Append a simple timestamped line to arb.log.
        Does not change existing rich print behavior, just adds disk logging.
        """
        ts = datetime.fromtimestamp(utime()).isoformat()
        line = f"[{ts}] {msg}\n"
        try:
            with open("arb.log", "a") as f:
                f.write(line)
        except Exception as e:
            print(f"[red]Failed to write arb.log: {e}[/red]")

    # -------------------- Redeem worker --------------------

    async def _redeem_after_delay(self, ticker: str, condition_id: str, delay_sec: int = 7 * 60):
        """
        Sleeps delay_sec, then tries to redeem condition_id.
        Runs redeem() in a thread so it doesn't block the event loop.
        """
        if not condition_id:
            return

        try:
            msg = f"Redeem task scheduled for {condition_id} (ticker={ticker}) in {delay_sec}s"
            print(f"[cyan]{msg}[/cyan]")
            self._log_event(msg)

            await asyncio.sleep(delay_sec)

            print("[yellow]Trying to redeem past Polymarket condition...[/yellow]")
            self._log_event(f"Redeem start condition_id={condition_id}")

            loop = asyncio.get_running_loop()
            txn = await loop.run_in_executor(None, redeem, condition_id)

            print(f"[green]Redeem tx: {txn}[/green]")
            self._log_event(f"Redeem success condition_id={condition_id} tx={txn}")

            try:
                with open("reciept.txt", "a") as f:
                    f.write(f"{ticker} - {condition_id} - {txn}\n")
            except Exception as e:
                print(f"[red]Failed to write reciept.txt: {e}[/red]")
                self._log_event(f"Failed to write reciept.txt: {e}")

        except Exception as e:
            print(f"[red]Redeem failed for {condition_id}: {e}[/red]")
            self._log_event(f"Redeem failed for {condition_id}: {e}")

    # -------------------- Helpers --------------------

    @staticmethod
    def _safe_float(x):
        try:
            return float(x)
        except Exception:
            return None

    def _poly_place_fok(self, token_id, base_price, condition_id):
        """
        Place what is effectively a limit FOK order on Polymarket.

        - base_price is the observed current price (YES or NO)
        - final_price = base_price + pad (limit price ceiling)
        - amount is in USDC, not shares. For BUY this is how many dollars
          you are willing to spend.
        - If the order cannot be fully filled server side, we catch the
          PolyApiException and treat it as a non fill.
        """
        final_price = base_price + self.pad

        # amount is USDC to spend
        amount = self.qty * final_price

        # Enforce Poly min marketable size: 1 dollar
        if amount < 1.0:
            msg = (
                f"Poly amount below 1 dollar min (amount={amount:.4f}) for "
                f"token_id={token_id}, base_price={base_price:.4f}. Skipping order."
            )
            print(f"[yellow]{msg}[/yellow]")
            self._log_event(msg)
            return False, None, {"success": False, "status": "skip", "error": "amount_below_min", "amount": amount}

        msg = (
            f"trying to buy on Poly token_id={token_id} "
            f"base_price={base_price:.4f} final_price={final_price:.4f} "
            f"amount={amount:.4f}"
        )
        print(f"[bold green]{msg}[/bold green]")
        self._log_event(msg)

        try:
            # Step 1: sign limit FOK order
            mo = MarketOrderArgs(
                token_id=token_id,
                price=final_price,
                amount=amount,
                side=BUY,
                order_type=OrderType.FOK,
            )
            signed = self.auth_client.create_market_order(mo)

            # Step 2: post it as FOK
            resp = self.auth_client.post_order(signed, OrderType.FOK)

        except PolyApiException as e:
            # Typical case: "order couldn't be fully filled. FOK orders are fully filled or killed."
            err_payload = getattr(e, "error_message", None)
            msg = f"PolyApiException in FOK order: {err_payload or e}"
            print(f"[yellow]{msg}[/yellow]")
            self._log_event(msg)

            resp = {
                "success": False,
                "status": "killed",
                "error": err_payload or str(e),
            }
            return False, None, resp

        except Exception as e:
            # Any other unexpected error, also treat as non fill but log loudly
            msg = f"Unexpected Poly error in FOK order: {e}"
            print(f"[red]{msg}[/red]")
            self._log_event(msg)
            resp = {
                "success": False,
                "status": "error",
                "error": str(e),
            }
            return False, None, resp

        # Normal path, no exception
        print("[cyan]Poly limit FOK response[/cyan]")
        print(resp)
        self._log_event(f"Poly response: {resp}")

        success = resp.get("success", False)
        status = resp.get("status")
        order_id = resp.get("orderID")

        taking_raw = resp.get("takingAmount") or "0"
        try:
            taking_amt = float(taking_raw)
        except Exception:
            taking_amt = 0.0

        filled = bool(success and status in ("matched", "delayed") and taking_amt > 0)

        if filled:
            msg = (
                f"Poly limit FOK filled (status={status}, "
                f"takingAmount={taking_amt}) order_id={order_id}"
            )
            print(f"[green]{msg}[/green]")
            self._log_event(msg)
        else:
            msg = (
                f"Poly limit FOK did NOT fill "
                f"(success={success}, status={status}, takingAmount={taking_amt})"
            )
            print(f"[yellow]{msg}[/yellow]")
            self._log_event(msg)

        try:
            with open("IDS.txt", "a") as f:
                f.write(f"{condition_id}\n")
        except Exception as e:
            print(f"[red]Error writing IDS.txt: {e}[/red]")
            self._log_event(f"Error writing IDS.txt: {e}")

        return filled, order_id, resp

    async def _poly_place_fok_async(self, token_id, base_price, condition_id):
        """
        Async wrapper for _poly_place_fok so we do not block the event loop.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._poly_place_fok, token_id, base_price, condition_id
        )

    def _kalshi_buy_fok(self, ticker, side, max_base_price):
        """
        Place FOK buy on Kalshi at max_base_price plus pad.
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
            print(f"[red]Kalshi FOK exception: {e}[/red]")
            self._log_event(f"Kalshi FOK exception: {e}")
            return None

        if order is None:
            print("[red]Kalshi order is None[/red]")
            self._log_event("Kalshi order is None")
            return None

        return order

    async def _kalshi_buy_fok_async(self, ticker, side, max_base_price):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._kalshi_buy_fok, ticker, side, max_base_price
        )

    # -------------------- Websocket tasks for v2 --------------------

    async def _kalshi_ws_task(self, ticker: str, close_ts: int):
        """
        Kalshi websocket listener that keeps self.kalshi_book updated.

        NO side is synthesized as:
            no_bid = 1 - yes_ask
            no_ask = 1 - yes_bid

        Stops shortly after close_ts.
        """
        last_tick_print = {}

        def handle_msg(msg: TickerMessage):
            if msg.market_ticker != ticker:
                return

            # Need YES to synthesize NO
            if msg.yes_bid is None or msg.yes_ask is None:
                return

            yes_bid = msg.yes_bid / 100.0
            yes_ask = msg.yes_ask / 100.0

            # synthetic NO
            no_bid = 1.0 - yes_ask
            no_ask = 1.0 - yes_bid

            # update shared book
            self.kalshi_book["yes_bid"] = yes_bid
            self.kalshi_book["yes_ask"] = yes_ask
            self.kalshi_book["no_bid"] = no_bid
            self.kalshi_book["no_ask"] = no_ask

            now = utime()
            if now - last_tick_print.get(ticker, 0) < 1.0:
                return
            last_tick_print[ticker] = now

            # Uncomment if you want printing
            # def fmt(v):
            #     return f"{v:.4f}" if v is not None else "None"
            # print(
            #     f"[KALSHI] {ticker} "
            #     f"YES bid/ask={fmt(yes_bid)}/{fmt(yes_ask)} "
            #     f"NO bid/ask={fmt(no_bid)}/{fmt(no_ask)}"
            # )

        with Feed(self.kalshi) as feed:

            @feed.on("ticker")
            def on_ticker(msg: TickerMessage):
                try:
                    handle_msg(msg)
                except Exception as e:
                    print(f"[ERR][kalshi_ws] {type(e).__name__}: {e}")
                    self._log_event(f"kalshi_ws error: {e}")

            feed.subscribe("ticker", market_tickers=[ticker])

            while True:
                if utime() > close_ts + 60:
                    print(f"[yellow]Kalshi ws task exiting for {ticker} (past close).[/yellow]")
                    self._log_event(f"Kalshi ws task exit for {ticker}")
                    break
                await asyncio.sleep(1.0)

    async def _poly_ws_task(self, asset_ids, close_ts: int):
        """
        Polymarket websocket listener that keeps self.poly_book updated.

        clobTokenIds[0] -> YES
        clobTokenIds[1] -> NO

        Runs until a bit after close_ts, with reconnection on ws errors.
        """
        if isinstance(asset_ids, str):
            asset_ids = [asset_ids]

        # Reconnect loop until after market close
        while True:
            now = utime()
            if now > close_ts + 60:
                print("[yellow]Poly ws task exiting (past close).[/yellow]")
                self._log_event("Poly ws task exit (past close)")
                return

            try:
                async with websockets.connect(
                    POLYMARKET_MARKET_WS,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    sub_msg = {
                        "assets_ids": asset_ids,
                        "type": "market",
                        "custom_feature_enabled": True,
                    }
                    await ws.send(json.dumps(sub_msg))
                    print(f"[POLY] Subscribed to assets_ids={asset_ids}")
                    self._log_event(f"POLY subscribed {asset_ids}")

                    async for raw in ws:
                        now = utime()
                        if now > close_ts + 60:
                            print("[yellow]Poly ws task exiting (past close) inside loop.[/yellow]")
                            self._log_event("Poly ws task exit (past close, inner loop)")
                            return

                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            print("[POLY] non json message:", raw)
                            self._log_event(f"POLY non json: {raw}")
                            continue

                        if isinstance(msg, list):
                            for item in msg:
                                self._handle_poly_event(item)
                        else:
                            self._handle_poly_event(msg)

                print("[yellow]Poly ws connection closed cleanly, reconnecting if before close.[/yellow]")
                self._log_event("Poly ws closed cleanly, will reconnect")

            except websockets.exceptions.ConnectionClosedError as e:
                print(f"[red]Poly ws ConnectionClosedError: {e}. Reconnecting...[/red]")
                self._log_event(f"Poly ws ConnectionClosedError: {e}")
                await asyncio.sleep(1)

            except TimeoutError as e:
                print(f"[red]Poly ws TimeoutError: {e}. Reconnecting...[/red]")
                self._log_event(f"Poly ws TimeoutError: {e}")
                await asyncio.sleep(1)

            except Exception as e:
                print(f"[red]Poly ws unexpected error: {e}. Reconnecting...[/red]")
                self._log_event(f"Poly ws unexpected error: {e}")
                await asyncio.sleep(3)

    def _handle_poly_event(self, data):
        """
        Internal handler for Polymarket websocket messages.

        Only cares about event_type equal best_bid_ask.
        Uses self.poly_yes_id and self.poly_no_id to classify YES vs NO.
        """
        if not isinstance(data, dict):
            return

        if data.get("event_type") != "best_bid_ask":
            return

        asset_id = data.get("asset_id")
        best_bid = data.get("best_bid")
        best_ask = data.get("best_ask")

        try:
            bb = float(best_bid) if best_bid is not None else None
        except Exception:
            bb = None
        try:
            ba = float(best_ask) if best_ask is not None else None
        except Exception:
            ba = None

        if asset_id == self.poly_yes_id:
            self.poly_book["yes_bid"] = bb
            self.poly_book["yes_ask"] = ba
        elif asset_id == self.poly_no_id:
            self.poly_book["no_bid"] = bb
            self.poly_book["no_ask"] = ba
        else:
            return

        # Uncomment if you want printing
        # side = "YES" if asset_id == self.poly_yes_id else "NO"
        # def fmt(v):
        #     return f"{v:.4f}" if v is not None else "None"
        # print(
        #     f"[POLY] side={side} asset_id={asset_id} "
        #     f"best_bid={fmt(bb)} best_ask={fmt(ba)}"
        # )

    async def _arb_monitor_task(self, ticker: str, close_ts: int):
        """
        Continuously checks self.kalshi_book and self.poly_book for an arb
        and both prints alerts and executes trades according to:

        1. Always buy Polymarket first.
        2. If Polymarket order is not filled, do not consider position entered.
        3. If Polymarket filled but Kalshi hedge fails, keep retrying.

        Stops shortly after close_ts.
        """
        last_print = 0
        last_alert_yes = 0
        last_alert_no = 0

        entered_poly = False
        kalshi_hedged = False
        poly_side = None
        kalshi_side = None
        last_poly_attempt = 0.0
        last_kalshi_attempt = 0.0

        POLY_RETRY_COOLDOWN = 0.5
        KALSHI_RETRY_COOLDOWN = 0.5

        while True:
            now = utime()
            if now > close_ts + 60:
                print(f"[yellow]Arb monitor exiting for {ticker} (past close).[/yellow]")
                self._log_event(f"Arb monitor exit for {ticker}")
                break

            kb = self.kalshi_book
            pb = self.poly_book

            Ky = kb.get("yes_ask")
            Kn = kb.get("no_ask")
            Py = pb.get("yes_ask")
            Pn = pb.get("no_ask")

            if None not in (Ky, Kn, Py, Pn):
                gross_edge_yes = 1.0 - (Ky + Pn)  # YES on Kalshi / NO on Poly
                gross_edge_no = 1.0 - (Kn + Py)   # NO on Kalshi / YES on Poly

                if now - last_print >= 2.0:
                    last_print = now
                    print(
                        f"[ARB-STATUS] {ticker} "
                        f"Ky={Ky:.4f} Kn={Kn:.4f} Py={Py:.4f} Pn={Pn:.4f} | "
                        f"edge_yes={gross_edge_yes:.4f} edge_no={gross_edge_no:.4f} "
                        f"(threshold={self.threshold:.4f})"
                    )

                if not entered_poly and self.poly_condition_id is not None:
                    # Strat 1 (FAVORED): NO Kalshi / YES Poly
                    if gross_edge_no >= self.threshold and now - last_poly_attempt >= POLY_RETRY_COOLDOWN:
                        poly_base_price = Py

                        msg = (
                            f"Trigger: NO-Kalshi / YES-Poly "
                            f"edge={gross_edge_no:.4f} "
                            f"Py={Py:.4f} final_exec_hint={poly_base_price + self.pad:.4f}"
                        )
                        print(f"[green]{msg}[/green]")
                        self._log_event(msg)

                        if poly_base_price and poly_base_price > 0:
                            last_poly_attempt = now
                            poly_side = "YES"
                            kalshi_side = "NO"
                            token_id = self.poly_yes_id

                            filled, order_id, resp = await self._poly_place_fok_async(
                                token_id, poly_base_price, self.poly_condition_id
                            )

                            if filled:
                                entered_poly = True
                                msg = (
                                    f"Entered Poly YES first for strat NO-Kalshi / YES-Poly "
                                    f"(token_id={token_id}, order_id={order_id})"
                                )
                                print(f"[bold green]{msg}[/bold green]")
                                self._log_event(msg)
                            else:
                                poly_side = None
                                kalshi_side = None
                                msg = "Poly YES FOK did not fill. Position not entered."
                                print(f"[yellow]{msg}[/yellow]")
                                self._log_event(msg)

                    # Strat 2: YES Kalshi / NO Poly
                    elif gross_edge_yes >= self.threshold and now - last_poly_attempt >= POLY_RETRY_COOLDOWN:
                        poly_base_price = Pn

                        msg = (
                            f"Trigger: YES-Kalshi / NO-Poly "
                            f"edge={gross_edge_yes:.4f} "
                            f"Pn={Pn:.4f} final_exec_hint={poly_base_price + self.pad:.4f}"
                        )
                        print(f"[green]{msg}[/green]")
                        self._log_event(msg)

                        if poly_base_price and poly_base_price > 0:
                            last_poly_attempt = now
                            poly_side = "NO"
                            kalshi_side = "YES"
                            token_id = self.poly_no_id

                            filled, order_id, resp = await self._poly_place_fok_async(
                                token_id, poly_base_price, self.poly_condition_id
                            )

                            if filled:
                                entered_poly = True
                                msg = (
                                    f"Entered Poly NO first for strat YES-Kalshi / NO-Poly "
                                    f"(token_id={token_id}, order_id={order_id})"
                                )
                                print(f"[bold green]{msg}[/bold green]")
                                self._log_event(msg)
                            else:
                                poly_side = None
                                kalshi_side = None
                                msg = "Poly NO FOK did not fill. Position not entered."
                                print(f"[yellow]{msg}[/yellow]")
                                self._log_event(msg)

                if entered_poly and not kalshi_hedged and kalshi_side is not None:
                    if now - last_kalshi_attempt >= KALSHI_RETRY_COOLDOWN:
                        last_kalshi_attempt = now

                        if kalshi_side == "YES":
                            max_px = Ky
                            order = await self._kalshi_buy_fok_async(ticker, Side.YES, max_px)
                        else:
                            max_px = Kn
                            order = await self._kalshi_buy_fok_async(ticker, Side.NO, max_px)

                        status = getattr(order, "status", None) if order is not None else None

                        if status == "executed":
                            kalshi_hedged = True
                            msg = (
                                f"Kalshi hedge filled (side={kalshi_side}, "
                                f"px<={max_px:.4f}). Fully hedged now."
                            )
                            print(f"[bold green]{msg}[/bold green]")
                            self._log_event(msg)
                        else:
                            msg = (
                                f"Kalshi hedge FAILED (side={kalshi_side}, "
                                f"px<={max_px:.4f}). Will keep retrying."
                            )
                            print(f"[bold red]{msg}[/bold red]")
                            self._log_event(msg)

                if gross_edge_yes >= self.threshold and now - last_alert_yes >= 5.0:
                    last_alert_yes = now
                    msg = (
                        f"[ARB-ALERT] YES-Kalshi / NO-Poly "
                        f"edge={gross_edge_yes:.4f} "
                        f"| Ky={Ky:.4f}, Pn={Pn:.4f}"
                    )
                    print(f"[bold yellow]{msg}[/bold yellow]")

                if gross_edge_no >= self.threshold and now - last_alert_no >= 5.0:
                    last_alert_no = now
                    msg = (
                        f"[ARB-ALERT] NO-Kalshi / YES-Poly "
                        f"edge={gross_edge_no:.4f} "
                        f"| Kn={Kn:.4f}, Py={Py:.4f}"
                    )
                    print(msg)

            await asyncio.sleep(0.2)

    async def v2(self):
        """
        v2 main loop:

        - While auto_refresh:
            - Find current open KXBTC15M Kalshi market
            - Find corresponding Polymarket BTC 15m slug using close_time
            - Get clobTokenIds and wire them as YES and NO
            - Start:
                * Kalshi websocket
                * Polymarket websocket
                * Arb monitor that prints alerts and runs trading logic
            - After market close, schedule redemption of the condition
              (sleep 7 minutes, then redeem) and then refresh to next market.

        If auto_refresh is False, it runs for a single market then exits.
        """
        while True:
            mkts = self.kalshi.get_markets(
                limit=1,
                mve_filter="exclude",
                status=MarketStatus.OPEN,
                series_ticker="KXBTC15M",
            )
            if not mkts:
                print("[yellow]No open KXBTC15M markets found, retrying in 10 seconds.[/yellow]")
                self._log_event("No open KXBTC15M markets. Sleep 10.")
                await asyncio.sleep(10)
                continue

            market = mkts[0]
            ticker = market.ticker
            close_ts = int(parser.isoparse(market.close_time).timestamp())

            print(f"[cyan]v2: using Kalshi BTC market {ticker}, close_ts={close_ts}[/cyan]")
            self._log_event(f"New Kalshi market {ticker}, close_ts={close_ts}")

            slug = f"btc-updown-15m-{close_ts - 900}"
            p = self.poly.call_api("getMarketBySlug", {"slug": slug})

            print(
                f"[white]Poly slug={p.get('slug')}, endDate={p.get('endDate')}, id={p.get('id')}[/white]"
            )
            self._log_event(f"Poly market {p.get('slug')} id={p.get('id')}")

            yes_id, no_id = json.loads(p["clobTokenIds"])
            self.poly_yes_id = yes_id
            self.poly_no_id = no_id
            self.poly_condition_id = p.get("conditionId")

            print(
                f"[cyan]v2: Polymarket YES={yes_id} NO={no_id} "
                f"conditionId={self.poly_condition_id}[/cyan]"
            )
            self._log_event(
                f"Poly ids YES={yes_id} NO={no_id} conditionId={self.poly_condition_id}"
            )

            await asyncio.gather(
                self._kalshi_ws_task(ticker, close_ts),
                self._poly_ws_task([yes_id, no_id], close_ts),
                self._arb_monitor_task(ticker, close_ts),
            )

            last_condition_id = self.poly_condition_id

            if last_condition_id is not None:
                asyncio.create_task(self._redeem_after_delay(ticker, last_condition_id, delay_sec=7 * 60))

            if not self.auto_refresh:
                print("[yellow]auto_refresh is False. Exiting after one market.[/yellow]")
                self._log_event("Exit v2 after one market (auto_refresh False)")
                break

            print("[cyan]Refreshing to next open BTC market...[/cyan]")
            self._log_event("Refreshing to next market")
            await asyncio.sleep(5)


if __name__ == "__main__":
    arb = Arb(auto_refresh=True)
    asyncio.run(arb.v2())