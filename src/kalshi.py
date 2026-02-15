from pykalshi import MarketStatus, CandlestickPeriod, KalshiClient
from rich import print
from dotenv import load_dotenv
from os import getenv
import requests
import json
from types import SimpleNamespace
from csv import writer
from pandas import read_csv
from time import sleep
from time import time as utime
from datetime import datetime, time
from dateutil import parser
import pytz
from pykalshi import Feed, TickerMessage, Action, Side, OrderType, TimeInForce
import asyncio
import logging
from collections import deque



class Kalshi:
    def __init__(self, config):
        load_dotenv(".env")
        self.client = KalshiClient.from_env(demo=False)

        self.events = None
        self.positions = {}
        self.load_positions()
        self.CONFIG = config

        self.pt = pytz.timezone("America/Los_Angeles")

        self._bal_cache = None
        self._bal_cache_ts = 0.0
        self._bal_cache_ttl = 10.0  # seconds, adjust if you want

        self._px_hist = {}  # ticker -> deque[(ts, yes_ask)]
        self._px_hist_secs = 12  # window length
        self._min_ticks = 6      # minimum samples before decisions



    def get_balance_cached(self) -> float:
        now = utime()
        if self._bal_cache is None or (now - self._bal_cache_ts) >= self._bal_cache_ttl:
            bal = self.client.portfolio.get_balance()
            self._bal_cache = bal.portfolio_value + bal.balance
            self._bal_cache_ts = now
        return float(self._bal_cache)

    def get_markets(self, limit=1, series= "KXNBAGAME", mve_filter = "exclude"):
        self.series = self.client.get_markets(limit=limit, mve_filter=mve_filter, status=MarketStatus.OPEN, series_ticker=series)
        print(f"Got {len(self.series)} markets from {series}")
        return self.series
    
                    

    def get_mulitple_markets(self, limit=1, series=["KXNBAGAME"], mve_filter="exclude"):
        merged = []
        for s in series:
            merged.extend(self.get_markets(limit, s, mve_filter))
        return merged

    def get_unique_events(self, markets, save = False):
        tickers = set()
        for market in markets:
                tickers.add(market.ticker)

        print(f"Got {len(tickers)} tickers")
        if save:
            json.dump(list(tickers),open("./../data/events.json","w"))
            self.events = tickers
        return tickers
    
    def filter_by_today(self, markets, save=False):
        pt = self.pt  # Pacific timezone object
        tmp = set()

        today_1159 = pt.localize(datetime.combine(datetime.now(pt).date(), time(23, 59)))
        t_time = int(today_1159.timestamp())

        for market in markets:
            exp_str = market.expected_expiration_time

            # fully timezone aware, handles Z, +00:00, +05:30, whatever
            dt_utc = parser.isoparse(exp_str)

            # convert to Pacific
            dt_pt = dt_utc.astimezone(pt)
            c_time = int(dt_pt.timestamp())

            #print(event, c_time, t_time)

            if c_time < t_time and market.status == "active":
                print(market.ticker, market.status)
                tmp.add(market)

        print(f"Filtered down to {len(tmp)} events")

        return tmp

    
    def load_events(self):
        with open("./../data/events.json", "r") as f:
            self.events = json.load(f)
        print(f"Loaded {len(self.events)} events")
        return self.events
    
    def get_quote(self, event_ticker):
        url = f"https://api.elections.kalshi.com/trade-api/v2/events/{event_ticker}"

        r = json.loads(requests.get(url).text)
        return {
            "title": r['event']["sub_title"],
            "event_ticker": r['markets'][0]["event_ticker"],
            "expected_expiration_time": r['markets'][0]["expected_expiration_time"],
            "expiration_time": r['markets'][0]["expiration_time"],
            "no_ask_dollars": float(r['markets'][0]["no_ask_dollars"]),
            "no_bid_dollars": float(r['markets'][0]["no_bid_dollars"]),
            "result": r['markets'][0]["result"],
            "status": r['markets'][0]["status"],
            "ticker": r['markets'][0]["ticker"],
            "yes_ask_dollars": float(r['markets'][0]["yes_ask_dollars"]),
            "yes_bid_dollars": float(r['markets'][0]["yes_bid_dollars"]),
        }
    

    def load_positions(self):
        with open("./../data/positions.json", "r") as f:
            self.positions = json.load(f)
        print(f"Loaded {len(self.positions)} positions")
        return self.positions
    
    def dump_positions(self):
        json.dump(self.positions, open("./../data/positions.json", "w"))
    
    def open_position(self, msg, direction, price):
        self.positions[msg.market_ticker] = {
            "dir": "yes" if direction == Side.YES else "no",
            "price": price
        }
        mmsg = [msg.market_ticker, "YES" if direction == Side.YES else "NO", "open", price, 0]

        self.logger(mmsg)

        print(f"[green]New position:\n\t{msg.market_ticker} is a {direction.upper()} @ ${price}")

    def close_position(self, msg, price, dir):
        pos = self.positions.pop(msg.market_ticker)
        diff = round(price - float(pos["price"]),4)
        self.logger([
            msg.market_ticker,
            dir,
            "close",
            price,
            diff
        ])
        print(f"[green]Closed position:\n\t{msg.market_ticker} is a {pos['dir'].upper()} @ ${price}\t=>\t${diff} P&L")


    def logger(self, message):
        writer(open("./../data/log.csv", "a")).writerow(message)

    def gen_financials(self):
        pnl = read_csv("./../data/log.csv")["effect"].astype(float).sum()
        print(f"Profit/Loss assuming {self.CONFIG.QTY} contracts were brought for each event: ${pnl * self.CONFIG.QTY}")
    
    def checkpoint(self):
        json.dump(self.positions, open("./../data/checkpoint.json", "w"), indent=1)
        json.dump(list(self.events), open("./../data/checkpoint.json", "a"), indent=1)
    
    
    def strategy_high(self):
        self.events = list(self.events)
        while len(self.events) > 0:
            for event in self.events:
                quote = self.get_quote(event)
                ticker = quote["ticker"]
                #print(ticker, quote["yes_ask_dollars"], quote["no_ask_dollars"])
                
                if ticker not in self.positions:
                    print(f"ticker: {ticker}\t positions: {self.positions}")
                    if quote['yes_ask_dollars'] >= self.CONFIG.L_LIMIT and quote['yes_ask_dollars'] <= self.CONFIG.U_LIMIT:
                        self.open_position(quote, "yes")
                        pass
                    elif quote['no_ask_dollars'] >= self.CONFIG.L_LIMIT and quote['no_ask_dollars'] <= self.CONFIG.U_LIMIT:
                        self.open_position(quote, "no")
                        pass
                else:
                    pos = self.positions[ticker]["dir"]
                    if quote["status"] != "active":
                        self.events.remove(event)
                        if quote["result"] == self.positions[ticker]["dir"]:
                            self.close_position(ticker, 1)
                        else:
                            self.close_position(ticker, 0)
                    elif quote[f"{pos}_bid_dollars"] < self.CONFIG.SL:
                        if (quote[f"{pos}_bid_dollars"] + quote[f"{pos}_ask_dollars"]) < self.CONFIG.SL:
                            self.close_position(ticker, quote[f"{pos}_bid_dollars"])

            if round(utime()) % 60 == 0:
                self.checkpoint()

    def buy(self, ticker, side, max):
        if side == Side.NO:
            order = self.client.portfolio.place_order(ticker, Action.BUY, side, count=1, no_price=int(max*100), time_in_force=TimeInForce.GTC)
        else:
            order = self.client.portfolio.place_order(ticker, Action.BUY, side, count=1, yes_price=int(max*100), time_in_force=TimeInForce.GTC)
        
        if order.status == "executed":
            return order
        
        sleep(1)
        order = self.client.portfolio.cancel_order(order_id=order.order_id)
        if order.status == "executed":
            return order
        
        order = self.client.portfolio.cancel_order(order_id=order.order_id)
        return order

    def sell(self, ticker, side, max):
        if side == Side.NO:
            order = self.client.portfolio.place_order(ticker, Action.SELL, side, count = 1,no_price=int(max*100), time_in_force=TimeInForce.GTC)
        else: 
            order = self.client.portfolio.place_order(ticker, Action.SELL, side, count = 1, yes_price=int(max*100), time_in_force=TimeInForce.GTC)

        if order.status == "executed":
            return order
        
        sleep(1)
        order = self.client.portfolio.get_order(order_id=order.order_id)
        if order.status == "executed":
            return order
        
        order = self.client.portfolio.cancel_order(order_id=order.order_id)
        return order

    def test(self):
        bal = self.client.portfolio.get_balance()
        return bal.portfolio_value + bal.balance
    
    async def strategy_high_trade(self):
        logging.basicConfig(level=logging.WARNING)

        self.events = list(self.events)
        self.seen = set(self.positions.keys())

        print(self.seen)
        for s in self.seen:
            if s in self.events:
                self.events.remove(s)
        print(self.events)
        
        print(f"[START] strategy_high_trade | events={len(self.events)}")

        last_tick_print = {}  # per-ticker throttling

        def log_tick(ticker, yes_bid, yes_ask):
            # print at most once per second per ticker
            now = utime()
            if now - last_tick_print.get(ticker, 0) >= 1.0:
                last_tick_print[ticker] = now
                print(f"[TICK] {ticker} | YES bid/ask={yes_bid:.2f}/{yes_ask:.2f}")

        with Feed(self.client) as feed:

            @feed.on("ticker")
            def handle_ticker(msg: TickerMessage):
                #print(msg)
                if self.test() < 800:
                    print(f"[red bold]{self.test()}  -  BALANCE ERROR EXITING....")
                    exit()
                try:
                    ticker = msg.market_ticker

                    # Ignore incomplete quotes
                    if msg.yes_bid is None or msg.yes_ask is None:
                        return

                    yes_bid = msg.yes_bid / 100
                    yes_ask = msg.yes_ask / 100
                    no_bid = 1 - yes_ask
                    no_ask = 1 - yes_bid

                    log_tick(ticker, yes_bid, yes_ask)

                    if abs(yes_ask - yes_bid) >0.1:
                        return

                    # ENTRY
                    if ticker not in self.positions and ticker not in self.seen:
                        if self.CONFIG.L_LIMIT <= yes_ask <= self.CONFIG.U_LIMIT:
                            px = yes_ask + 0.01
                            print(f"[ENTRY] BUY YES {ticker} @ {px:.2f}")
                            order = self.buy(ticker, Side.YES, px)
                            if getattr(order, "status", None) == "executed":
                                fill_px = float(order.yes_price / 100)
                                print(f"[FILL] YES {ticker} @ {fill_px:.2f}")
                                self.open_position(msg, Side.YES, fill_px)
                                self.seen.add(ticker)

                        elif self.CONFIG.L_LIMIT <= no_ask <= self.CONFIG.U_LIMIT:
                            px = no_ask + 0.01
                            print(f"[ENTRY] BUY NO  {ticker} @ {px:.2f}")
                            order = self.buy(ticker, Side.NO, px)
                            if getattr(order, "status", None) == "executed":
                                fill_px = float(order.no_price / 100)
                                print(f"[FILL] NO  {ticker} @ {fill_px:.2f}")
                                self.open_position(msg, Side.NO, fill_px)
                                self.seen.add(ticker)


                        return

                    # POSITION MANAGEMENT
                    pos = self.positions[ticker]
                    dir_str = pos["dir"]  # "yes" or "no"

                    if dir_str == "yes":
                        if yes_bid < self.CONFIG.SL:
                            print(f"[SL] SELL YES {ticker} @ {yes_bid:.2f}")
                            self.sell(ticker, Side.YES, yes_bid)
                            self.close_position(msg, yes_bid, "YES")
                            if ticker in self.events:
                                self.events.remove(ticker)
                            feed.unsubscribe("ticker", market_ticker=ticker)


                        
                    try:
                        if yes_bid == 0:
                            ticker = msg.market_ticker
                            market = self.client.get_market(ticker)
                            if market.result == "yes":
                                print(f"[LIFE] {ticker} | RESOLVED result=NO")
                                if ticker in self.events:
                                    if ticker in self.events:
                                        self.events.remove(ticker)
                                    self.close_position(msg,1,'NO')
                                    feed.unsubscribe("ticker", market_ticker=ticker)

                    except Exception as e:
                        print(f"[ERR][lifecycle] {type(e).__name__}: {e}")

                except Exception as e:
                    print(f"[ERR][ticker] {type(e).__name__}: {e}")

                
            feed.subscribe("ticker", market_tickers=self.events)
            # Wait for connect
            for _ in range(20):
                if feed.is_connected:
                    break
                await asyncio.sleep(0.5)

            print(f"[WS] connected={feed.is_connected} reconnects={feed.reconnect_count}")

            # Heartbeat every 5s
            while self.events:
                if round(utime()) % 60 == 0:
                    print(
                        f"[HB] connected={feed.is_connected} msgs={feed.messages_received} "
                        f"last={feed.seconds_since_last_message}"
                    )
                    self.checkpoint()
                await asyncio.sleep(1)

        print("[EXIT] strategy_high_trade")


    def open_position_yes(self, msg: TickerMessage, price: float):
        # store positions keyed by market_ticker
        self.positions[msg.market_ticker] = {
            "dir": "yes",
            "price": float(price),
        }
        self.logger([msg.market_ticker, "YES", "open", float(price), 0])
        print(f"[green]New position:\n\t{msg.market_ticker} is a YES @ ${price:.2f}")

    def close_position_yes(self, msg: TickerMessage, price: float, reason: str = "close"):
        pos = self.positions.pop(msg.market_ticker, None)
        if pos is None:
            return
        diff = round(float(price) - float(pos["price"]), 4)
        self.logger([msg.market_ticker, "YES", reason, float(price), diff])
        print(
            f"[green]Closed position:\n\t{msg.market_ticker} YES @ ${price:.2f}\t=>\t${diff} P&L"
        )

    def _maybe_remove_event(self, ticker: str):
        # self.events is a list here, so guard removal
        if ticker in self.events:
            self.events.remove(ticker)
    

    def _push_px(self, ticker: str, yes_ask: float):
        now = utime()
        dq = self._px_hist.get(ticker)
        if dq is None:
            dq = deque()
            self._px_hist[ticker] = dq
        dq.append((now, float(yes_ask)))

        # drop old
        cutoff = now - self._px_hist_secs
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def _approaching_from_below(self, ticker: str, lower: float) -> bool:
        dq = self._px_hist.get(ticker)
        if not dq or len(dq) < self._min_ticks:
            return False

        # must have been below lower recently
        was_below = any(px < lower for _, px in dq)
        if not was_below:
            return False

        # slope: compare earliest and latest in the deque
        t0, p0 = dq[0]
        t1, p1 = dq[-1]
        if t1 <= t0:
            return False

        slope = (p1 - p0) / (t1 - t0)  # dollars per second
        # require a small positive slope so we avoid catching a knife
        return slope >= 0.002  # tune this, see notes below


    async def strategy_yes_only(self):
        logging.basicConfig(level=logging.WARNING)

        # ensure we have a list of market tickers
        self.events = list(self.events)

        # "seen" prevents re-entry after fills or restarts
        self.seen = set(self.positions.keys())

        # if we already have a position, don't subscribe that ticker
        for t in list(self.seen):
            if t in self.events:
                self.events.remove(t)

        print(f"[START] strategy_yes_only | events={len(self.events)} open_pos={len(self.positions)}")

        last_tick_print = {}

        def log_tick(ticker: str, yes_bid: float, yes_ask: float):
            now = utime()
            if now - last_tick_print.get(ticker, 0) >= 1.0:
                last_tick_print[ticker] = now
                print(f"[TICK] {ticker} | YES bid/ask={yes_bid:.2f}/{yes_ask:.2f}")

        with Feed(self.client) as feed:

            @feed.on("ticker")
            def handle_ticker(msg: TickerMessage):
                try:
                    ticker = msg.market_ticker

                    # quick balance check, but do not call twice
                    bal = self.get_balance_cached()
                    if bal < 800:
                        print(f"[red bold]{bal}  -  BALANCE ERROR EXITING....")
                        raise SystemExit

                    if msg.yes_bid is None or msg.yes_ask is None:
                        return

                    yes_bid = msg.yes_bid / 100
                    yes_ask = msg.yes_ask / 100
                    self._push_px(ticker, yes_ask)


                    log_tick(ticker, yes_bid, yes_ask)

                    # skip wide spreads
                    if (yes_ask - yes_bid) > 0.10:
                        return

                    # ENTRY: YES only
                    if ticker not in self.positions and ticker not in self.seen:
                        if self.CONFIG.L_LIMIT <= yes_ask <= self.CONFIG.U_LIMIT:
                            if not self._approaching_from_below(ticker, self.CONFIG.L_LIMIT):
                                return

                            px = min(1.00, max(0.01, round(yes_ask + 0.01, 2)))
                            print(f"[ENTRY] BUY YES {ticker} @ {px:.2f}")
                            order = self.buy(ticker, Side.YES, px)

                            if getattr(order, "status", None) == "executed":
                                fill_px = float(order.yes_price / 100)
                                print(f"[FILL] YES {ticker} @ {fill_px:.2f}")
                                self.open_position_yes(msg, fill_px)
                                self.seen.add(ticker)
                            return


                    # POSITION MANAGEMENT: YES only
                    pos = self.positions.get(ticker)
                    if not pos:
                        return

                    # stop loss on bid
                    if yes_bid < self.CONFIG.SL:
                        px = round(yes_bid, 2)
                        print(f"[SL] SELL YES {ticker} @ {px:.2f}")
                        order = self.sell(ticker, Side.YES, px)

                        if getattr(order, "status", None) == "executed":
                            # Prefer actual fill price from the order if present
                            fill_px = px
                            if getattr(order, "yes_price", None) is not None:
                                fill_px = float(order.yes_price / 100)

                            self.close_position_yes(msg, fill_px, reason="sl")
                            self._maybe_remove_event(ticker)
                            feed.unsubscribe("ticker", market_ticker=ticker)
                        else:
                            print(f"[WARN] SL sell not executed for {ticker} (status={getattr(order,'status',None)}). Keeping position open.")

                        return


                    # lifecycle: use market.result instead of guessing from bid==0
                    # poll only when it looks dead to avoid hammering API
                    if yes_bid == 1 or yes_ask == 1:
                        market = self.client.get_market(ticker)
                        if getattr(market, "status", None) != "active":
                            # if it resolved YES, payout is 1.00 else 0.00
                            payout = 1.0 if getattr(market, "result", None) == "yes" else 0.0
                            print(f"[LIFE] {ticker} | RESOLVED result={getattr(market,'result',None)} payout={payout:.2f}")
                            self.close_position_yes(msg, payout, reason="resolved")
                            self._maybe_remove_event(ticker)
                            feed.unsubscribe("ticker", market_ticker=ticker)

                except SystemExit:
                    raise
                except Exception as e:
                    print(f"[ERR][ticker] {type(e).__name__}: {e}")

            feed.subscribe("ticker", market_tickers=self.events)

            # wait for connect
            for _ in range(20):
                if feed.is_connected:
                    break
                await asyncio.sleep(0.5)

            print(f"[WS] connected={feed.is_connected} reconnects={feed.reconnect_count}")

            while self.events:
                if round(utime()) % 60 == 0:
                    print(
                        f"[HB] connected={feed.is_connected} msgs={feed.messages_received} "
                        f"last={feed.seconds_since_last_message}"
                    )
                    self.checkpoint()
                await asyncio.sleep(1)

        print("[EXIT] strategy_yes_only")
