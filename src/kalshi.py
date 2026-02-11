from kalshi_python_sync import Configuration, KalshiClient
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

class Kalshi:
    def __init__(self, config):
        load_dotenv()
        self.configuration = Configuration(
            host = "https://api.elections.kalshi.com/trade-api/v2"
        )   

        with open('./private_key.pem', 'r') as f:
            self.private_key = f.read()

        self.configuration.api_key_id = getenv("API_KEY")
        self.configuration.private_key_pem = self.private_key

        self.client = KalshiClient(self.configuration)
        self.events = None
        self.positions = {}
        self.load_positions()
        self.CONFIG = config

        self.pt = pytz.timezone("America/Los_Angeles")
        

    def get_markets(self, limit=1, series= "KXNBAGAME", status = "open", mve_filter = "exclude"):
        url = f"https://api.elections.kalshi.com/trade-api/v2/markets?limit={limit}&status={status}&series_ticker={series}&mve_filter={mve_filter}"
        response = json.loads(requests.get(url).text)
        print(f"Got {len(response['markets'])} markets from {series}")
        return response["markets"]
    
    def get_markets_timed(self, max_close_ts, limit=1, series= "KXNBAGAME", status = "open", mve_filter = "exclude" ):
        url = f"https://api.elections.kalshi.com/trade-api/v2/markets?limit={limit}&status={status}&series_ticker={series}&mve_filter={mve_filter}&max_close_ts={max_close_ts}"
        print(url)
        response = json.loads(requests.get(url).text)
        print(f"Got {len(response['markets'])} markets")
        return response["markets"]
    
    def get_mulitple_markets(self, limit=1, series=["KXNBAGAME"], status="open", mve_filter="exclude"):
        merged = []
        for s in series:
            merged.extend(self.get_markets(limit, s, status, mve_filter))
        #print(merged)
        return merged

    def get_unique_events(self, markets, save = False):
        events = set()
        for market in markets:
            events.add(market["event_ticker"])

        print(f"Got {len(events)} events")
        if save:
            json.dump(list(events),open("./../data/events.json","w"))
            self.events = events
        return events
    
    def filter_by_today(self, save=False):
        pt = self.pt  # Pacific timezone object
        tmp = set()

        today_1159 = pt.localize(datetime.combine(datetime.now(pt).date(), time(23, 59)))
        t_time = int(today_1159.timestamp())

        for event in self.events:
            exp_str = self.get_quote(event)["expected_expiration_time"]

            # fully timezone aware, handles Z, +00:00, +05:30, whatever
            dt_utc = parser.isoparse(exp_str)

            # convert to Pacific
            dt_pt = dt_utc.astimezone(pt)
            c_time = int(dt_pt.timestamp())

            #print(event, c_time, t_time)

            if c_time < t_time:
                tmp.add(event)

        print(f"Filtered down to {len(tmp)} events")

        if save:
            with open("./../data/events.json", "w") as f:
                json.dump(list(tmp), f)
            self.events = tmp

        return self.events

    
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
    
    def open_position(self, quote, direction):
        self.positions[quote["ticker"]] = {
            "dir": direction,
            "price": quote[f"{direction}_ask_dollars"]
        }
        msg = [
            quote["ticker"],
            direction,
            "open",
            quote[f"{direction}_ask_dollars"],
            0
        ]
        self.logger(msg)

        print(f"[green]New position:\n\t{quote['ticker']} is a {direction.upper()} @ ${quote[f'{direction}_ask_dollars']}")

    def close_position(self, ticker, price):
        pos = self.positions.pop(ticker)
        diff = round(price - float(pos["price"]),4)
        self.logger([
            ticker,
            pos["dir"],
            "close",
            pos["price"],
            diff
        ])
        print(f"[green]Closed position:\n\t{ticker} is a {pos['dir'].upper()} @ ${price}\t=>\t${diff} P&L")


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
            


                
            
            
