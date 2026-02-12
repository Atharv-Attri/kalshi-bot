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



class Kalshi:
    def __init__(self, config):
        load_dotenv(".env")
        self.client = KalshiClient.from_env(demo=True)

        self.events = None
        self.positions = {}
        self.load_positions()
        self.CONFIG = config

        self.pt = pytz.timezone("America/Los_Angeles")
        

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
        events = set()
        tickers = set()
        for market in markets:
            if market.event_ticker not in events:
                events.add(market.event_ticker)
                tickers.add(market.ticker)

        print(f"Got {len(tickers)} tickers")
        if save:
            json.dump(list(events),open("./../data/events.json","w"))
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
        mmsg = [
            msg.market_ticker,
            direction,
            "open",
            price,
            0
        ]
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

    
    async def strategy_high_trade(self):
        logging.basicConfig(level=logging.WARNING)

        self.events = list(self.events)
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

                    if abs(yes_ask - yes_bid) >0.1 and yes_ask > 0.85:
                        return

                    # ENTRY
                    if ticker not in self.positions:
                        if self.CONFIG.L_LIMIT <= yes_ask <= self.CONFIG.U_LIMIT:
                            px = yes_ask + 0.01
                            print(f"[ENTRY] BUY YES {ticker} @ {px:.2f}")
                            order = self.buy(ticker, Side.YES, px)
                            if getattr(order, "status", None) == "executed":
                                fill_px = float(order.yes_price / 100)
                                print(f"[FILL] YES {ticker} @ {fill_px:.2f}")
                                self.open_position(msg, "yes", fill_px)

                        elif self.CONFIG.L_LIMIT <= no_ask <= self.CONFIG.U_LIMIT:
                            px = no_ask + 0.01
                            print(f"[ENTRY] BUY NO  {ticker} @ {px:.2f}")
                            order = self.buy(ticker, Side.NO, px)
                            if getattr(order, "status", None) == "executed":
                                fill_px = float(order.no_price / 100)
                                print(f"[FILL] NO  {ticker} @ {fill_px:.2f}")
                                self.open_position(msg, "no", fill_px)

                        return

                    # POSITION MANAGEMENT
                    pos = self.positions[ticker]
                    dir_str = pos["dir"]  # "yes" or "no"

                    if dir_str == "yes":
                        if yes_bid < self.CONFIG.SL:
                            print(f"[SL] SELL YES {ticker} @ {yes_bid:.2f}")
                            self.sell(ticker, Side.YES, yes_bid)
                            self.close_position(msg, yes_bid, "YES")

                    else:  # "no"
                        if no_bid < self.CONFIG.SL:
                            print(f"[SL] SELL NO  {ticker} @ {no_bid:.2f}")
                            self.sell(ticker, Side.NO, no_bid)
                            self.close_position(msg, no_bid, "NO")

                except Exception as e:
                    print(f"[ERR][ticker] {type(e).__name__}: {e}")

            @feed.on("market_lifecycle")
            def handle_lifecycle(msg):
                try:
                    ticker = msg.market_ticker
                    status = msg.status
                    result = msg.result
                    if status and status != "active":
                        print(f"[LIFE] {ticker} | status={status} result={result}")
                        if ticker in self.events:
                            self.events.remove(ticker)
                            self.close_position(msg,1,result)
                except Exception as e:
                    print(f"[ERR][lifecycle] {type(e).__name__}: {e}")

            feed.subscribe("ticker", market_tickers=self.events)
            feed.subscribe("market_lifecycle", market_tickers=self.events)

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
