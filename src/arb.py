import asyncio
import pmxt
from dotenv import load_dotenv
from os import getenv
from rich import print
import json
from dateutil import parser
from time import time as utime
from datetime import datetime

import time
import uuid

from pykalshi import (
    MarketStatus,
    KalshiClient,
    Action,
    Side,
    TimeInForce,
    Feed,
    TickerMessage,
    KalshiAPIError,
)

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    BalanceAllowanceParams,
    AssetType,
    MarketOrderArgs,
    OrderType,
)
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.exceptions import PolyApiException

import websockets

from redeem import redeem
import os
import base64
from urllib.parse import urlparse
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

POLYMARKET_MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class Arb:
    STATUS_PRINT_INTERVAL = 5.0
    STATUS_STALE_AFTER = 15.0
    STATUS_ORDER = ("BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE")
    _status_snapshot = {}
    _status_last_print = 0.0

    def __init__(self, auto_refresh: bool = True, crypto="btc", paper_trading=None):
        # Detection and execution parameters
        self.threshold = 0.04   # gross edge needed to trigger
        self.qty = 15
        self.pad = 0.01         # padding added to both venues
        self.auto_refresh = auto_refresh
        self.crypto = crypto
        self.paper_trading = (
            self._env_flag("ARB_PAPER_TRADING", True)
            if paper_trading is None
            else bool(paper_trading)
        )
        self.execution_mode = "PAPER" if self.paper_trading else "LIVE"
        self.min_entry_side_price = 0.50
        self.force_exit_side_price = 0.40
        self.session_pnl_log_path = "arb_session_pnl.log"
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

        self.poly = pmxt.Polymarket()
        self.kalshi = KalshiClient.from_env(demo=False)

        self.balance()

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

        print(f"[cyan]Execution mode: {self.execution_mode}[/cyan]")
        self._log_event(f"Execution mode: {self.execution_mode}")

    # -------------------- Logging helper --------------------
    @staticmethod
    def _env_flag(name: str, default: bool = False) -> bool:
        raw = getenv(name)
        if raw is None:
            return default
        return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}

    def balance(self):
        balance = self.auth_client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        poly = int(balance["balance"]) / 1e6
        print(f"[green]USDC Balance: ${poly:.2f}[/green]")

        kal_m = self.kalshi.portfolio.get_balance()
        kal = (kal_m.balance + kal_m.portfolio_value)/100
        print(f"[green]Kalshi Balance: ${kal:.2f}[/green]")

        # FIX 1: Never exit in paper trading mode regardless of balance.
        if not self.paper_trading and (poly < self.qty or kal < self.qty):
            exit()

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

    @staticmethod
    def _fmt_price(price) -> str:
        return "NA" if price is None else f"{float(price):.4f}"

    def _print_entry_attempt(
        self,
        strategy: str,
        expected_edge: float,
        poly_side: str,
        poly_target: float,
        kalshi_side: str,
        kalshi_target: float,
    ):
        msg = (
            f"[ARB] {strategy} target "
            f"Poly {poly_side}<={self._fmt_price(poly_target)} + "
            f"Kalshi {kalshi_side}<={self._fmt_price(kalshi_target)} | "
            f"expected_edge={expected_edge:.4f}"
        )
        print(f"[bold green]{msg}[/bold green]")
        self._log_event(msg)

    def _print_leg_fill(self, venue: str, side: str, price: float):
        msg = f"[ARB] {venue} {side} filled @{self._fmt_price(price)}"
        print(f"[green]{msg}[/green]")
        self._log_event(msg)

    def _print_attempt_fail(self, venue: str, side: str, price: float, result: dict):
        resp = result.get("resp") or {}
        if isinstance(resp, dict):
            status = resp.get("status", "unknown")
            error = resp.get("error")
        else:
            status = getattr(resp, "status", "unknown")
            error = getattr(resp, "error", None)
        detail = f" status={status}"
        if error:
            detail += f" error={error}"
        msg = f"[ARB] {venue} {side} FAILED @{self._fmt_price(price)}.{detail}"
        print(f"[yellow]{msg}[/yellow]")
        self._log_event(msg)

    def _print_hedged_summary(self, session: dict):
        poly_price = session["poly"]["entry_price"]
        kalshi_price = session["kalshi"]["entry_price"]
        expected_edge = session.get("expected_edge")
        actual_edge = None
        if poly_price is not None and kalshi_price is not None:
            actual_edge = 1.0 - poly_price - kalshi_price

        msg = (
            f"[ARB] Hedged {session['strategy']} | "
            f"Poly {session['poly']['side']}={self._fmt_price(poly_price)} + "
            f"Kalshi {session['kalshi']['side']}={self._fmt_price(kalshi_price)} | "
            f"expected_edge={self._fmt_price(expected_edge)} "
            f"actual_edge={self._fmt_price(actual_edge)}"
        )
        print(f"[bold green]{msg}[/bold green]")
        self._log_event(msg)

    def _record_status(
        self,
        ticker: str,
        ky: float,
        kn: float,
        py: float,
        pn: float,
        edge_yes: float,
        edge_no: float,
        now: float,
        session: dict = None,
        force: bool = False,
    ):
        position = {"status": "idle"}
        if session is not None:
            poly = session["poly"]
            kalshi = session["kalshi"]
            actual_edge = None
            if poly["entry_price"] is not None and kalshi["entry_price"] is not None:
                actual_edge = 1.0 - poly["entry_price"] - kalshi["entry_price"]
            poly_bid, kalshi_bid = self._session_current_pair_bids(session)
            position = {
                "status": self._session_status(session),
                "closing_reason": session.get("closing_reason"),
                "strategy": session["strategy"],
                "poly_side": poly["side"],
                "poly_entry": poly["entry_price"],
                "poly_bid": poly_bid,
                "kalshi_side": kalshi["side"],
                "kalshi_entry": kalshi["entry_price"],
                "kalshi_bid": kalshi_bid,
                "expected_edge": session.get("expected_edge"),
                "actual_edge": actual_edge,
            }

        cls = type(self)
        cls._status_snapshot[self.crypto.upper()] = {
            "ticker": ticker,
            "ky": ky,
            "kn": kn,
            "py": py,
            "pn": pn,
            "edge_yes": edge_yes,
            "edge_no": edge_no,
            "threshold": self.threshold,
            "updated_at": now,
            "position": position,
        }

        if not force and now - cls._status_last_print < cls.STATUS_PRINT_INTERVAL:
            return

        cls._status_last_print = now
        cls._print_status_dashboard(now)

    def _clear_status(self):
        type(self)._status_snapshot.pop(self.crypto.upper(), None)

    @classmethod
    def _print_status_dashboard(cls, now: float):
        rows = [
            (symbol, data)
            for symbol, data in cls._status_snapshot.items()
            if now - data["updated_at"] <= cls.STATUS_STALE_AFTER
        ]
        if not rows:
            return

        order = {symbol: idx for idx, symbol in enumerate(cls.STATUS_ORDER)}
        rows.sort(key=lambda item: (order.get(item[0], len(order)), item[0]))

        lines = [
            f"[bold cyan][ARB-STATUS {datetime.fromtimestamp(now).strftime('%H:%M:%S')}][/bold cyan]",
            "[dim]sym      Ky     Kn     Py     Pn   edgeY  edgeN  best      pos[/dim]",
        ]

        for symbol, data in rows:
            edge_yes = data["edge_yes"]
            edge_no = data["edge_no"]
            best_side = "YES" if edge_yes >= edge_no else "NO"
            best_edge = max(edge_yes, edge_no)
            threshold = data["threshold"]
            position = data.get("position") or {"status": "idle"}
            pos_status = position.get("status", "idle")
            pos_text = "idle"

            if pos_status == "hedged":
                actual_edge = position.get("actual_edge")
                expected_edge = position.get("expected_edge")
                style = "blue"
                pos_text = (
                    f"HEDGED "
                    f"P{position['poly_side']}@{cls._fmt_price(position['poly_entry'])} "
                    f"K{position['kalshi_side']}@{cls._fmt_price(position['kalshi_entry'])} "
                    f"exp={cls._fmt_price(expected_edge)} act={cls._fmt_price(actual_edge)}"
                )
            elif pos_status == "unhedged":
                expected_edge = position.get("expected_edge")
                style = "yellow"
                pos_text = (
                    f"UNHEDGED "
                    f"P{position['poly_side']}@{cls._fmt_price(position['poly_entry'])} "
                    f"need K{position['kalshi_side']} "
                    f"exp={cls._fmt_price(expected_edge)}"
                )
            elif pos_status.startswith("closing:"):
                actual_edge = position.get("actual_edge")
                reason = position.get("closing_reason") or pos_status.removeprefix("closing:")
                style = "red"
                pos_text = (
                    f"CLOSING({reason}) "
                    f"P{position['poly_side']}@{cls._fmt_price(position['poly_entry'])}/bid={cls._fmt_price(position['poly_bid'])} "
                    f"K{position['kalshi_side']}@{cls._fmt_price(position['kalshi_entry'])}/bid={cls._fmt_price(position['kalshi_bid'])} "
                    f"act={cls._fmt_price(actual_edge)}"
                )
            else:
                style = "green" if best_edge >= threshold else "yellow" if best_edge >= 0 else "dim"

            lines.append(
                f"[{style}]"
                f"{symbol:<5} "
                f"{data['ky']:>5.3f} "
                f"{data['kn']:>5.3f} "
                f"{data['py']:>5.3f} "
                f"{data['pn']:>5.3f} "
                f"{edge_yes:>7.4f} "
                f"{edge_no:>7.4f} "
                f"{best_side}:{best_edge:>7.4f} "
                f"{pos_text}"
                f"[/{style}]"
            )

        print("\n".join(lines))

    # -------------------- Kalshi client_order_id helper --------------------

    def _new_client_order_id(self, ticker: str, side_tag: str) -> str:
        # Unique across rapid retries and across runs
        return f"arb-{ticker}-{side_tag}-{time.time_ns()}-{uuid.uuid4().hex[:8]}"

    def _new_session_id(self, ticker: str) -> str:
        return f"{ticker}-{time.time_ns()}-{uuid.uuid4().hex[:6]}"

    # -------------------- Redeem worker --------------------

    async def _redeem_after_delay(self, ticker: str, condition_id: str, delay_sec: int = 7 * 60):
        """
        Sleeps delay_sec, then tries to redeem condition_id.
        Runs redeem() in a thread so it doesn't block the event loop.
        """
        if self.paper_trading:
            msg = f"Skipping redeem for {condition_id} (ticker={ticker}) in paper mode"
            print(f"[cyan]{msg}[/cyan]")
            self._log_event(msg)
            return

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

    @staticmethod
    def _side_name(side) -> str:
        if isinstance(side, Side):
            return side.name
        return str(side).upper()

    def _price_with_pad(self, action: str, base_price: float) -> float:
        if action == "BUY":
            return min(0.99, base_price + self.pad)
        return max(0.01, base_price - self.pad)

    def _book_price(self, venue: str, side: str, kind: str):
        book = self.poly_book if venue == "poly" else self.kalshi_book
        return book.get(f"{side.lower()}_{kind}")

    def _new_session(
        self,
        ticker: str,
        strategy: str,
        poly_side: str,
        kalshi_side: str,
        token_id: str,
        expected_edge: float = None,
        poly_target_price: float = None,
        kalshi_target_price: float = None,
    ):
        return {
            "id": self._new_session_id(ticker),
            "ticker": ticker,
            "strategy": strategy,
            "condition_id": self.poly_condition_id,
            "opened_at": datetime.fromtimestamp(utime()).isoformat(),
            "closing_reason": None,
            "expected_edge": expected_edge,
            "poly_target_price": poly_target_price,
            "kalshi_target_price": kalshi_target_price,
            "last_pnl_log_ts": 0.0,
            "poly": {
                "side": poly_side,
                "token_id": token_id,
                "entry_price": None,
                "entry_order_id": None,
                "exit_price": None,
                "exit_order_id": None,
                "entered": False,
                "exited": False,
            },
            "kalshi": {
                "side": kalshi_side,
                "entry_price": None,
                "entry_order_id": None,
                "exit_price": None,
                "exit_order_id": None,
                "entered": False,
                "exited": False,
            },
        }

    def _session_status(self, session: dict) -> str:
        poly = session["poly"]
        kalshi = session["kalshi"]
        if session.get("closing_reason"):
            return f"closing:{session['closing_reason']}"
        if poly["entered"] and kalshi["entered"]:
            return "hedged"
        if poly["entered"]:
            return "unhedged"
        return "idle"

    def _session_open_legs(self, session: dict):
        legs = []
        for venue in ("poly", "kalshi"):
            leg = session[venue]
            if leg["entered"] and not leg["exited"]:
                legs.append((venue, leg))
        return legs

    def _session_is_closed(self, session: dict) -> bool:
        return len(self._session_open_legs(session)) == 0

    def _session_current_pair_bids(self, session: dict):
        return (
            self._book_price("poly", session["poly"]["side"], "bid"),
            self._book_price("kalshi", session["kalshi"]["side"], "bid"),
        )

    def _session_has_hedged_open_pair(self, session: dict) -> bool:
        poly = session["poly"]
        kalshi = session["kalshi"]
        return (
            poly["entered"]
            and kalshi["entered"]
            and not poly["exited"]
            and not kalshi["exited"]
        )

    def _compute_session_pnl(self, session: dict):
        realized = 0.0
        for venue in ("poly", "kalshi"):
            leg = session[venue]
            if leg["entered"] and leg["exited"] and leg["exit_price"] is not None:
                realized += (leg["exit_price"] - leg["entry_price"]) * self.qty

        poly = session["poly"]
        kalshi = session["kalshi"]
        if (
            poly["entered"]
            and kalshi["entered"]
            and not poly["exited"]
            and not kalshi["exited"]
            and poly["entry_price"] is not None
            and kalshi["entry_price"] is not None
        ):
            locked_in = (1.0 - poly["entry_price"] - kalshi["entry_price"]) * self.qty
            return {
                "pnl": locked_in,
                "realized": realized,
                "mode": "locked_in",
            }

        mtm = realized
        open_legs = 0
        for venue in ("poly", "kalshi"):
            leg = session[venue]
            if leg["entered"] and not leg["exited"] and leg["entry_price"] is not None:
                current_bid = self._book_price(venue, leg["side"], "bid")
                if current_bid is None:
                    continue
                mtm += (current_bid - leg["entry_price"]) * self.qty
                open_legs += 1

        mode = "realized" if open_legs == 0 else "realized+mtm"
        return {
            "pnl": mtm,
            "realized": realized,
            "mode": mode,
        }

    def _log_session_pnl(self, session: dict, reason: str, force: bool = False):
        now = utime()
        if not force and now - session.get("last_pnl_log_ts", 0.0) < 2.0:
            return

        snapshot = self._compute_session_pnl(session)
        poly_bid, kalshi_bid = self._session_current_pair_bids(session)
        line = (
            f"session={session['id']} ticker={session['ticker']} "
            f"status={self._session_status(session)} reason={reason} "
            f"mode={snapshot['mode']} pnl={snapshot['pnl']:.4f} "
            f"realized={snapshot['realized']:.4f} "
            f"poly_bid={poly_bid if poly_bid is not None else 'NA'} "
            f"kalshi_bid={kalshi_bid if kalshi_bid is not None else 'NA'}"
        )

        ts = datetime.fromtimestamp(now).isoformat()
        try:
            with open(self.session_pnl_log_path, "a") as f:
                f.write(f"[{ts}] {line}\n")
        except Exception as e:
            print(f"[red]Failed to write {self.session_pnl_log_path}: {e}[/red]")
            self._log_event(f"Failed to write {self.session_pnl_log_path}: {e}")

        session["last_pnl_log_ts"] = now

    def _poly_order_fok(self, token_id, side, action, base_price, condition_id=None):
        """
        Place a Polymarket FOK order, or simulate it in paper mode.
        """
        side_name = self._side_name(side)
        action = action.upper()
        final_price = self._price_with_pad(action, base_price)
        amount = self.qty * final_price if action == "BUY" else float(self.qty)
        side_const = BUY if action == "BUY" else SELL
        order_label = f"{action} {side_name}"

        if action == "BUY" and amount < 1.0:
            msg = (
                f"Poly amount below 1 dollar min (amount={amount:.4f}) for "
                f"token_id={token_id}, action={action}, base_price={base_price:.4f}. Skipping order."
            )
            print(f"[yellow]{msg}[/yellow]")
            self._log_event(msg)
            return {
                "filled": False,
                "order_id": None,
                "resp": {
                    "success": False,
                    "status": "skip",
                    "error": "amount_below_min",
                    "amount": amount,
                },
                "price": final_price,
                "action": action,
                "side": side_name,
            }

        self._log_event(
            f"Poly attempt action={action} side={side_name} token_id={token_id} "
            f"base_price={base_price:.4f} limit_price={final_price:.4f} amount={amount:.4f} "
            f"mode={self.execution_mode}"
        )

        if self.paper_trading:
            # FIX 2: Capture order_id here and reuse it — don't re-fetch from resp later.
            order_id = self._new_client_order_id(f"poly-{token_id}", f"{action}-{side_name}")
            resp = {
                "success": True,
                "status": "matched",
                "orderID": order_id,
                "takingAmount": str(amount),
                "mock": True,
                "action": action,
                "side": side_name,
                "price": final_price,
            }
            self._log_event(f"Poly response: {resp}")
            filled = True
        else:
            try:
                mo = MarketOrderArgs(
                    token_id=token_id,
                    price=final_price,
                    amount=amount,
                    side=side_const,
                    order_type=OrderType.FOK,
                )
                signed = self.auth_client.create_market_order(mo)
                resp = self.auth_client.post_order(signed, OrderType.FOK)
            except PolyApiException as e:
                err_payload = getattr(e, "error_message", None)
                msg = f"PolyApiException in FOK order: {err_payload or e}"
                self._log_event(msg)
                resp = {
                    "success": False,
                    "status": "killed",
                    "error": err_payload or str(e),
                }
                return {
                    "filled": False,
                    "order_id": None,
                    "resp": resp,
                    "price": final_price,
                    "action": action,
                    "side": side_name,
                }
            except Exception as e:
                msg = f"Unexpected Poly error in FOK order: {e}"
                self._log_event(msg)
                resp = {
                    "success": False,
                    "status": "error",
                    "error": str(e),
                }
                return {
                    "filled": False,
                    "order_id": None,
                    "resp": resp,
                    "price": final_price,
                    "action": action,
                    "side": side_name,
                }

            self._log_event(f"Poly response: {resp}")
            success = resp.get("success", False)
            status = resp.get("status")
            filled = bool(success and status in ("matched", "delayed", "executed"))
            # Only set order_id from resp in the live branch
            order_id = resp.get("orderID")

        status = resp.get("status")
        taking_raw = resp.get("takingAmount") or resp.get("makingAmount") or "0"
        try:
            taking_amt = float(taking_raw)
        except Exception:
            taking_amt = 0.0

        if filled:
            self._log_event(
                f"Poly {order_label} FOK filled status={status} "
                f"fillAmount={taking_amt} order_id={order_id}"
            )
        else:
            msg = (
                f"Poly {order_label} limit FOK did NOT fill "
                f"(status={status}, fillAmount={taking_amt})"
            )
            self._log_event(msg)

        if filled and action == "BUY" and condition_id and not self.paper_trading:
            try:
                with open("IDS.txt", "a") as f:
                    f.write(f"{condition_id}\n")
            except Exception as e:
                print(f"[red]Error writing IDS.txt: {e}[/red]")
                self._log_event(f"Error writing IDS.txt: {e}")

        return {
            "filled": filled,
            "order_id": order_id,
            "resp": resp,
            "price": final_price,
            "action": action,
            "side": side_name,
        }

    async def _poly_order_fok_async(self, token_id, side, action, base_price, condition_id=None):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._poly_order_fok, token_id, side, action, base_price, condition_id
        )

    def _kalshi_order_fok(self, ticker, side, action, base_price):
        """
        Place a Kalshi FOK order, or simulate it in paper mode.
        """
        side_name = self._side_name(side)
        side_enum = side if isinstance(side, Side) else Side[side_name]
        action = action.upper()
        final_price = self._price_with_pad(action, base_price)
        limit_cents = int(round(final_price * 100))
        client_order_id = self._new_client_order_id(ticker, f"{action}-{side_name}")

        self._log_event(
            f"Kalshi attempt action={action} side={side_name} ticker={ticker} "
            f"base_price={base_price:.4f} limit_price={final_price:.4f} qty={self.qty} "
            f"mode={self.execution_mode}"
        )

        if self.paper_trading:
            # FIX 3 & 4: Paper mock is always a plain dict with a consistent
            # structure. "status" and "order_id" are top-level keys, so the
            # live getattr/get dual-path is never needed for paper orders.
            order = {
                "status": "executed",
                "order_id": client_order_id,
                "client_order_id": client_order_id,
                "mock": True,
                "action": action,
                "side": side_name,
                "price": final_price,
            }
            filled = True
            order_id = client_order_id
        else:
            try:
                if side_enum == Side.NO:
                    order = self.kalshi.portfolio.place_order(
                        ticker,
                        Action.BUY if action == "BUY" else Action.SELL,
                        side_enum,
                        count=self.qty,
                        no_price=limit_cents,
                        time_in_force=TimeInForce.FOK,
                        client_order_id=client_order_id,
                    )
                else:
                    order = self.kalshi.portfolio.place_order(
                        ticker,
                        Action.BUY if action == "BUY" else Action.SELL,
                        side_enum,
                        count=self.qty,
                        yes_price=limit_cents,
                        time_in_force=TimeInForce.FOK,
                        client_order_id=client_order_id,
                    )
            except KalshiAPIError as e:
                err = getattr(e, "error_code", str(e))
                self._log_event(f"Kalshi FOK exception: {err} client_order_id={client_order_id}")
                return {
                    "filled": False,
                    "order_id": None,
                    "resp": {"status": "error", "error": err},
                    "price": final_price,
                    "action": action,
                    "side": side_name,
                }

            if order is None:
                self._log_event("Kalshi order is None")
                return {
                    "filled": False,
                    "order_id": None,
                    "resp": {"status": "none"},
                    "price": final_price,
                    "action": action,
                    "side": side_name,
                }

            # Live orders may be objects or dicts depending on pykalshi version
            status = getattr(order, "status", None) if not isinstance(order, dict) else order.get("status")
            order_id = getattr(order, "order_id", None) if not isinstance(order, dict) else order.get("order_id")
            filled = status == "executed"

        self._log_event(f"Kalshi response: {order}")

        if filled:
            self._log_event(f"Kalshi {action} {side_name} FOK filled order_id={order_id}")
        else:
            status = getattr(order, "status", None) if not isinstance(order, dict) else order.get("status")
            msg = f"Kalshi {action} {side_name} FOK did NOT fill order_id={order_id} status={status}"
            self._log_event(msg)

        return {
            "filled": filled,
            "order_id": order_id,
            "resp": order,
            "price": final_price,
            "action": action,
            "side": side_name,
        }

    async def _kalshi_order_fok_async(self, ticker, side, action, base_price):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._kalshi_order_fok, ticker, side, action, base_price
        )

    async def _close_session_positions(self, session: dict):
        close_reason = session.get("closing_reason") or "manual_close"
        ticker = session["ticker"]

        for venue in ("poly", "kalshi"):
            leg = session[venue]
            if not leg["entered"] or leg["exited"]:
                continue

            exit_price = self._book_price(venue, leg["side"], "bid")
            if exit_price is None or exit_price <= 0:
                msg = (
                    f"Unable to close {venue} {leg['side']} for session={session['id']} "
                    f"because bid is unavailable."
                )
                print(f"[yellow]{msg}[/yellow]")
                self._log_event(msg)
                continue

            if venue == "poly":
                result = await self._poly_order_fok_async(
                    leg["token_id"],
                    leg["side"],
                    "SELL",
                    exit_price,
                    session.get("condition_id"),
                )
            else:
                result = await self._kalshi_order_fok_async(
                    ticker,
                    leg["side"],
                    "SELL",
                    exit_price,
                )

            if result["filled"]:
                leg["exit_price"] = result["price"]
                leg["exit_order_id"] = result["order_id"]
                leg["exited"] = True
                msg = (
                    f"Closed {venue} leg for session={session['id']} side={leg['side']} "
                    f"price={result['price']:.4f} reason={close_reason}"
                )
                print(f"[bold green]{msg}[/bold green]")
                self._log_event(msg)
            else:
                msg = (
                    f"Failed to close {venue} leg for session={session['id']} side={leg['side']} "
                    f"reason={close_reason}. Will retry."
                )
                print(f"[bold red]{msg}[/bold red]")
                self._log_event(msg)

        if self._session_is_closed(session):
            snapshot = self._compute_session_pnl(session)
            self._log_session_pnl(session, "session_closed", force=True)
            msg = (
                f"Arb session closed session={session['id']} reason={close_reason} "
                f"realized_pnl={snapshot['pnl']:.4f}"
            )
            print(f"[bold cyan]{msg}[/bold cyan]")
            self._log_event(msg)
            return True

        self._log_session_pnl(session, "close_retry", force=True)
        return False

    # -------------------- Websocket tasks for v2 --------------------

    def _kalshi_ws_headers(self):
        key_id = getenv("KALSHI_API_KEY_ID")
        private_key_path = getenv("KALSHI_PRIVATE_KEY_PATH")

        if not key_id or not private_key_path:
            raise RuntimeError("Missing KALSHI_API_KEY_ID or KALSHI_PRIVATE_KEY_PATH")

        with open(private_key_path, "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(),
                password=None,
            )

        ws_url = "wss://api.elections.kalshi.com/trade-api/ws/v2"
        ws_path = urlparse(ws_url).path

        ts = str(int(time.time() * 1000))
        msg = f"{ts}GET{ws_path}".encode()

        sig = private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )

        return {
            "KALSHI-ACCESS-KEY": key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
        }


    async def _kalshi_ws_task(self, ticker: str, close_ts: int):
        """
        Kalshi websocket listener using orderbook snapshots/deltas.

        This replaces ticker-based bid/ask handling because ticker
        messages frequently return yes_bid=None and yes_ask=None.
        """

        yes_book = {}
        no_book = {}

        last_print = 0.0

        def recompute_book():
            nonlocal last_print

            yes_bid = max(yes_book.keys()) / 100.0 if yes_book else None
            no_bid = max(no_book.keys()) / 100.0 if no_book else None

            yes_ask = (100 - max(no_book.keys())) / 100.0 if no_book else None
            no_ask = (100 - max(yes_book.keys())) / 100.0 if yes_book else None

            self.kalshi_book["yes_bid"] = yes_bid
            self.kalshi_book["yes_ask"] = yes_ask
            self.kalshi_book["no_bid"] = no_bid
            self.kalshi_book["no_ask"] = no_ask

            now = utime()



        def apply_snapshot(inner: dict):
            yes_book.clear()
            no_book.clear()

            for price_raw, qty_raw in inner.get("yes_dollars_fp", []):
                price = int(round(float(price_raw) * 100))
                qty = float(qty_raw)

                if qty > 0:
                    yes_book[price] = qty

            for price_raw, qty_raw in inner.get("no_dollars_fp", []):
                price = int(round(float(price_raw) * 100))
                qty = float(qty_raw)

                if qty > 0:
                    no_book[price] = qty

            recompute_book()

        def apply_delta(inner: dict):
            side = inner.get("side")

            if side not in ("yes", "no"):
                return

            price = int(round(float(inner["price_dollars"]) * 100))
            delta = float(inner["delta_fp"])

            book = yes_book if side == "yes" else no_book

            new_qty = book.get(price, 0.0) + delta

            if new_qty <= 1e-9:
                book.pop(price, None)
            else:
                book[price] = new_qty

            recompute_book()

        headers_key = (
            "additional_headers"
            if int(websockets.__version__.split(".")[0]) >= 14
            else "extra_headers"
        )

        while True:
            if utime() > close_ts + 60:
                print(f"[yellow]Kalshi ws task exiting for {ticker}[/yellow]")
                self._log_event(f"Kalshi ws exit for {ticker}")
                return

            try:
                conn_kw = {
                    headers_key: self._kalshi_ws_headers(),
                    "ping_interval": 20,
                    "ping_timeout": 20,
                    "close_timeout": 5,
                }

                async with websockets.connect(
                    "wss://api.elections.kalshi.com/trade-api/ws/v2",
                    **conn_kw,
                ) as ws:

                    subscribe_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["orderbook_delta"],
                            "market_tickers": [ticker],
                        },
                    }

                    await ws.send(json.dumps(subscribe_msg))

                    print(f"[cyan]Subscribed to Kalshi orderbook: {ticker}[/cyan]")
                    self._log_event(f"Subscribed Kalshi orderbook {ticker}")

                    while utime() <= close_ts + 60:
                        raw = await asyncio.wait_for(
                            ws.recv(),
                            timeout=10.0,
                        )

                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="ignore")

                        msg = json.loads(raw)

                        typ = msg.get("type")
                        inner = msg.get("msg")

                        if typ == "subscribed":
                            print(f"[green]Kalshi subscription confirmed[/green]")
                            continue

                        if typ == "error":
                            print(f"[red][kalshi_ws] error: {inner}[/red]")
                            self._log_event(f"Kalshi ws error: {inner}")
                            continue

                        if not isinstance(inner, dict):
                            continue

                        market_ticker = inner.get("market_ticker")

                        if market_ticker != ticker:
                            continue

                        if typ == "orderbook_snapshot":
                            apply_snapshot(inner)

                        elif typ == "orderbook_delta":
                            apply_delta(inner)

            except asyncio.TimeoutError:
                print(f"[yellow][kalshi_ws] timeout reconnect {ticker}[/yellow]")
                self._log_event(f"Kalshi ws timeout reconnect {ticker}")

            except Exception as e:
                print(
                    f"[red][kalshi_ws] reconnecting after "
                    f"{type(e).__name__}: {e}[/red]"
                )

                self._log_event(
                    f"Kalshi ws reconnect after "
                    f"{type(e).__name__}: {e}"
                )

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
            print(f"[debug][poly] non-dict message: {data!r}")
            return

        event_type = data.get("event_type")
        if event_type != "best_bid_ask":
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

    async def _arb_monitor_task(self, ticker: str, close_ts: int):
        """
        Continuously checks self.kalshi_book and self.poly_book for an arb
        and both prints alerts and executes trades according to:

        1. Always buy Polymarket first.
        2. If Polymarket order is not filled, do not consider position entered.
        3. If Polymarket filled but Kalshi hedge fails, keep retrying.

        Stops shortly after close_ts.
        """
        last_alert_yes = 0
        last_alert_no = 0
        last_block_log_yes = 0
        last_block_log_no = 0

        session = None
        last_poly_attempt = 0.0
        last_kalshi_attempt = 0.0
        last_close_attempt = 0.0

        POLY_RETRY_COOLDOWN = 0.5
        KALSHI_RETRY_COOLDOWN = 0.5
        CLOSE_RETRY_COOLDOWN = 0.5

        while True:
            now = utime()
            if now > close_ts + 60:
                if session is not None:
                    self._log_session_pnl(session, "monitor_exit", force=True)
                self._clear_status()
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

                self._record_status(ticker, Ky, Kn, Py, Pn, gross_edge_yes, gross_edge_no, now, session)

                if session is not None:
                    self._log_session_pnl(session, "heartbeat")
                    pair_poly_bid, pair_kalshi_bid = self._session_current_pair_bids(session)
                    if (
                        self._session_has_hedged_open_pair(session)
                        and session.get("closing_reason") is None
                        and pair_poly_bid is not None
                        and pair_kalshi_bid is not None
                        and pair_poly_bid < self.force_exit_side_price
                        and pair_kalshi_bid < self.force_exit_side_price
                    ):
                        session["closing_reason"] = "held_sides_below_40"
                        msg = (
                            f"Force exit triggered for session={session['id']} "
                            f"because held sides are both below {self.force_exit_side_price:.2f} "
                            f"(poly_{session['poly']['side'].lower()}_bid={pair_poly_bid:.4f}, "
                            f"kalshi_{session['kalshi']['side'].lower()}_bid={pair_kalshi_bid:.4f})"
                        )
                        print(f"[bold red]{msg}[/bold red]")
                        self._log_event(msg)
                        self._log_session_pnl(session, "force_exit_trigger", force=True)
                        self._record_status(
                            ticker,
                            Ky,
                            Kn,
                            Py,
                            Pn,
                            gross_edge_yes,
                            gross_edge_no,
                            now,
                            session,
                            force=True,
                        )

                if session is not None and session.get("closing_reason"):
                    if now - last_close_attempt >= CLOSE_RETRY_COOLDOWN:
                        last_close_attempt = now
                        closed = await self._close_session_positions(session)
                        if closed:
                            session = None
                            last_close_attempt = 0.0
                    await asyncio.sleep(0.05)
                    continue

                if session is None and self.poly_condition_id is not None:
                    # Strat 1 (FAVORED): YES Poly / NO Kalshi
                    if gross_edge_no >= self.threshold and now - last_poly_attempt >= POLY_RETRY_COOLDOWN:
                        poly_base_price = Py
                        kalshi_base_price = Kn

                        if (
                            poly_base_price is not None
                            and kalshi_base_price is not None
                            and poly_base_price < self.min_entry_side_price
                            and kalshi_base_price < self.min_entry_side_price
                        ):
                            if now - last_block_log_no >= 5.0:
                                last_block_log_no = now
                                msg = (
                                    f"Skipped NO-Kalshi / YES-Poly entry because both sides are below "
                                    f"{self.min_entry_side_price:.2f} (Py={poly_base_price:.4f}, Kn={kalshi_base_price:.4f})"
                                )
                                print(f"[yellow]{msg}[/yellow]")
                                self._log_event(msg)
                        else:
                            poly_target = self._price_with_pad("BUY", poly_base_price)
                            kalshi_target = self._price_with_pad("BUY", kalshi_base_price)
                            self._print_entry_attempt(
                                "NO-Kalshi / YES-Poly",
                                gross_edge_no,
                                "YES",
                                poly_target,
                                "NO",
                                kalshi_target,
                            )

                            if poly_base_price and poly_base_price > 0:
                                last_poly_attempt = now
                                token_id = self.poly_yes_id

                                result = await self._poly_order_fok_async(
                                    token_id,
                                    "YES",
                                    "BUY",
                                    poly_base_price,
                                    self.poly_condition_id,
                                )

                                if result["filled"]:
                                    session = self._new_session(
                                        ticker,
                                        "NO-Kalshi / YES-Poly",
                                        "YES",
                                        "NO",
                                        token_id,
                                        gross_edge_no,
                                        poly_target,
                                        kalshi_target,
                                    )
                                    session["poly"]["entered"] = True
                                    session["poly"]["entry_price"] = result["price"]
                                    session["poly"]["entry_order_id"] = result["order_id"]
                                    self._print_leg_fill("Poly", "YES", result["price"])
                                    self._log_session_pnl(session, "poly_entry", force=True)
                                else:
                                    self._print_attempt_fail("Poly", "YES", result["price"], result)

                    # Strat 2: NO Poly / YES Kalshi
                    elif gross_edge_yes >= self.threshold and now - last_poly_attempt >= POLY_RETRY_COOLDOWN:
                        poly_base_price = Pn
                        kalshi_base_price = Ky

                        if (
                            poly_base_price is not None
                            and kalshi_base_price is not None
                            and poly_base_price < self.min_entry_side_price
                            and kalshi_base_price < self.min_entry_side_price
                        ):
                            if now - last_block_log_yes >= 5.0:
                                last_block_log_yes = now
                                msg = (
                                    f"Skipped YES-Kalshi / NO-Poly entry because both sides are below "
                                    f"{self.min_entry_side_price:.2f} (Pn={poly_base_price:.4f}, Ky={kalshi_base_price:.4f})"
                                )
                                print(f"[yellow]{msg}[/yellow]")
                                self._log_event(msg)
                        else:
                            poly_target = self._price_with_pad("BUY", poly_base_price)
                            kalshi_target = self._price_with_pad("BUY", kalshi_base_price)
                            self._print_entry_attempt(
                                "YES-Kalshi / NO-Poly",
                                gross_edge_yes,
                                "NO",
                                poly_target,
                                "YES",
                                kalshi_target,
                            )

                            if poly_base_price and poly_base_price > 0:
                                last_poly_attempt = now
                                token_id = self.poly_no_id

                                result = await self._poly_order_fok_async(
                                    token_id,
                                    "NO",
                                    "BUY",
                                    poly_base_price,
                                    self.poly_condition_id,
                                )

                                if result["filled"]:
                                    session = self._new_session(
                                        ticker,
                                        "YES-Kalshi / NO-Poly",
                                        "NO",
                                        "YES",
                                        token_id,
                                        gross_edge_yes,
                                        poly_target,
                                        kalshi_target,
                                    )
                                    session["poly"]["entered"] = True
                                    session["poly"]["entry_price"] = result["price"]
                                    session["poly"]["entry_order_id"] = result["order_id"]
                                    self._print_leg_fill("Poly", "NO", result["price"])
                                    self._log_session_pnl(session, "poly_entry", force=True)
                                else:
                                    self._print_attempt_fail("Poly", "NO", result["price"], result)

                if session is not None and session["poly"]["entered"] and not session["kalshi"]["entered"]:
                    if now - last_kalshi_attempt >= KALSHI_RETRY_COOLDOWN:
                        last_kalshi_attempt = now
                        kalshi_side = session["kalshi"]["side"]

                        if kalshi_side == "YES":
                            max_px = Ky
                            result = await self._kalshi_order_fok_async(ticker, "YES", "BUY", max_px)
                        else:
                            max_px = Kn
                            result = await self._kalshi_order_fok_async(ticker, "NO", "BUY", max_px)

                        if result["filled"]:
                            session["kalshi"]["entered"] = True
                            session["kalshi"]["entry_price"] = result["price"]
                            session["kalshi"]["entry_order_id"] = result["order_id"]
                            self._print_leg_fill("Kalshi", kalshi_side, result["price"])
                            self._print_hedged_summary(session)
                            self._log_session_pnl(session, "hedged", force=True)
                        else:
                            self._print_attempt_fail("Kalshi", kalshi_side, result["price"], result)

                if gross_edge_yes >= self.threshold and now - last_alert_yes >= 5.0:
                    last_alert_yes = now
                    msg = (
                        f"[ARB-ALERT] YES-Kalshi / NO-Poly "
                        f"edge={gross_edge_yes:.4f} "
                        f"| Ky={Ky:.4f}, Pn={Pn:.4f}"
                    )
                    self._log_event(msg)

                if gross_edge_no >= self.threshold and now - last_alert_no >= 5.0:
                    last_alert_no = now
                    msg = (
                        f"[ARB-ALERT] NO-Kalshi / YES-Poly "
                        f"edge={gross_edge_no:.4f} "
                        f"| Kn={Kn:.4f}, Py={Py:.4f}"
                    )
                    self._log_event(msg)

            await asyncio.sleep(0.05)

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
            series_ticker = f"KX{self.crypto.upper()}15M"
            mkts = await asyncio.to_thread(
                self.kalshi.get_markets,
                limit=1,
                mve_filter="exclude",
                status=MarketStatus.OPEN,
                series_ticker=series_ticker,
            )
            if not mkts:
                print(f"[yellow]No open {series_ticker} markets found, retrying in 10 seconds.[/yellow]")
                self._log_event(f"No open {series_ticker} markets. Sleep 10.")
                await asyncio.sleep(10)
                continue

            market = mkts[0]
            ticker = market.ticker
            close_ts = int(parser.isoparse(market.close_time).timestamp())

            print(f"[cyan]v2: using Kalshi {self.crypto.upper()} market {ticker}, close_ts={close_ts}[/cyan]")
            self._log_event(f"\n\n\nNew Kalshi market {ticker}, close_ts={close_ts}")

            slug = f"{self.crypto.lower()}-updown-15m-{close_ts - 900}"
            p = await asyncio.to_thread(self.poly.call_api, "getMarketBySlug", {"slug": slug})

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

            if last_condition_id is not None and not self.paper_trading:
                asyncio.create_task(self._redeem_after_delay(ticker, last_condition_id, delay_sec=7 * 60))

            if not self.auto_refresh:
                print("[yellow]auto_refresh is False. Exiting after one market.[/yellow]")
                self._log_event("Exit v2 after one market (auto_refresh False)")
                break

            await asyncio.to_thread(self.balance)
            print(f"[cyan]Refreshing to next open {self.crypto.upper()} market...[/cyan]")
            self._log_event("Refreshing to next market")
            await asyncio.sleep(5)

async def run():
    cryptos = ("eth", "xrp", "btc", "sol", "doge", "bnb", "hype")
    arbs = await asyncio.gather(*(asyncio.to_thread(Arb, True, crypto) for crypto in cryptos))
    await asyncio.gather(*(arb.v2() for arb in arbs))

if __name__ == "__main__":
    asyncio.run(run())
