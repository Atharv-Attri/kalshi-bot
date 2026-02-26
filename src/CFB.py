import asyncio
import json
import time
import math

import websockets
import aiohttp
from statistics import median
from rich import print

COINBASE_URL = "wss://ws-feed.exchange.coinbase.com"
KRAKEN_URL = "wss://ws.kraken.com"
BITSTAMP_URL = "wss://ws.bitstamp.net"

CRYPTOCOM_TICKER_URL = (
    "https://api.crypto.com/exchange/v1/public/get-tickers"
    "?instrument_name=BTC_USD"
)

GEMINI_TICKER_URL = "https://api.gemini.com/v2/ticker/BTCUSD"


class CFB:
    """
    Cross venue BTCUSD aggregator using only free public APIs.

    Venues:
        - Coinbase  BTC-USD  (websocket)
        - Kraken    XBT/USD  (websocket)
        - Bitstamp  btcusd   (websocket)
        - Crypto.com BTC_USD (REST polling)
        - Gemini    BTCUSD   (REST polling)

    Public API:
        get_btc() -> float | None

    Synthetic price:
        - Only venues with quotes newer than STALE_SEC are used
        - Spread sanity check per venue
        - Outliers beyond OUTLIER_PCT from cross median are dropped
        - Remaining mids aggregated with trimmed mean
    """

    STALE_SEC = 2.0        # max age for a quote in seconds
    OUTLIER_PCT = 0.005    # 0.5 percent deviation from median to drop a venue
    MAX_SPREAD_PCT = 0.005 # 0.5 percent max spread allowed for a venue

    def __init__(self):
        # last mid, spread, timestamp per venue
        self.latest = {
            "coinbase": {"mid": None, "spread": None, "ts": 0.0},
            "kraken": {"mid": None, "spread": None, "ts": 0.0},
            "bitstamp": {"mid": None, "spread": None, "ts": 0.0},
            "cryptocom": {"mid": None, "spread": None, "ts": 0.0},
            "gemini": {"mid": None, "spread": None, "ts": 0.0},
        }

        self._tasks: list[asyncio.Task] = []
        self._stopped = False

    # ------------- public API -------------

    async def run(self, log_sampler: bool = False):
        """
        Start all venue readers.

        If log_sampler is True, also starts a sampler that prints once per second.
        """
        if self._tasks:
            return

        self._stopped = False

        self._tasks.append(asyncio.create_task(self._coinbase_reader()))
        self._tasks.append(asyncio.create_task(self._kraken_reader()))
        self._tasks.append(asyncio.create_task(self._bitstamp_reader()))
        self._tasks.append(asyncio.create_task(self._cryptocom_reader()))
        self._tasks.append(asyncio.create_task(self._gemini_reader()))

        if log_sampler:
            self._tasks.append(asyncio.create_task(self._simple_sampler()))

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass

    async def stop(self):
        self._stopped = True
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def get_btc(self):
        """
        Return structured BTC snapshot dictionary.
        """
        return self._build_snapshot()

    def _build_snapshot(self):
        now = time.time()

        # Collect fresh mids
        fresh = {}
        for venue, rec in self.latest.items():
            mid = rec["mid"]
            ts = rec["ts"]
            spread = rec["spread"]

            if mid is None or ts == 0.0:
                continue

            age = now - ts
            if age > self.STALE_SEC:
                continue

            if spread is not None:
                if spread <= 0:
                    continue
                if (spread / mid) > self.MAX_SPREAD_PCT:
                    continue

            fresh[venue] = mid

        # Compute synthetic
        synth = None
        if fresh:
            mids = list(fresh.values())

            if len(mids) == 1:
                synth = mids[0]
            else:
                med = median(mids)
                allowed = [
                    m for m in mids
                    if abs(m - med) / med <= self.OUTLIER_PCT
                ]

                if not allowed:
                    allowed = mids

                n = len(allowed)
                if n == 1:
                    synth = allowed[0]
                elif n == 2:
                    synth = sum(allowed) / 2.0
                else:
                    vals = sorted(allowed)[1:-1]
                    synth = sum(vals) / len(vals) if vals else median(allowed)

        # Per venue prices
        price_cb = self.latest["coinbase"]["mid"]
        price_kr = self.latest["kraken"]["mid"]
        price_bs = self.latest["bitstamp"]["mid"]
        price_cc = self.latest["cryptocom"]["mid"]
        price_gm = self.latest["gemini"]["mid"]

        # Spread helper
        def spread(a, b):
            if a is None or b is None:
                return None
            return a - b

        return {
            "timestamp": now,

            "price_coinbase": price_cb,
            "price_kraken": price_kr,
            "price_bitstamp": price_bs,
            "price_cryptocom": price_cc,
            "price_gemini": price_gm,

            "price_synth": synth,

            # spreads
            "spread_cb_bs": spread(price_cb, price_bs),
            "spread_cb_kr": spread(price_cb, price_kr),
            "spread_cb_cc": spread(price_cb, price_cc),
            "spread_cb_gm": spread(price_cb, price_gm),

            "spread_kr_bs": spread(price_kr, price_bs),
            "spread_kr_cc": spread(price_kr, price_cc),
            "spread_kr_gm": spread(price_kr, price_gm),

            "spread_bs_cc": spread(price_bs, price_cc),
            "spread_bs_gm": spread(price_bs, price_gm),

            "spread_cc_gm": spread(price_cc, price_gm),
        }

    # ------------- core aggregation -------------

    def _get_synth(self):
        now = time.time()

        # 1. collect fresh venues with sane spreads
        fresh = {}
        for venue, rec in self.latest.items():
            mid = rec["mid"]
            ts = rec["ts"]
            spread = rec["spread"]

            if mid is None or ts == 0.0:
                continue

            age = now - ts
            if age > self.STALE_SEC:
                continue

            if spread is not None:
                if spread <= 0:
                    continue
                if (spread / mid) > self.MAX_SPREAD_PCT:
                    # insane spread, treat as bad quote
                    continue

            fresh[venue] = rec

        if not fresh:
            return None

        mids = [rec["mid"] for rec in fresh.values()]

        if len(mids) == 1:
            return mids[0]

        # 2. outlier filter relative to cross median
        med = median(mids)
        if med <= 0:
            # degenerate case, fall back to simple average
            return sum(mids) / len(mids)

        allowed = []
        for rec in fresh.values():
            m = rec["mid"]
            rel_dev = abs(m - med) / med
            if rel_dev <= self.OUTLIER_PCT:
                allowed.append(m)

        if not allowed:
            # everything got filtered, fall back to all mids
            allowed = mids

        n = len(allowed)
        if n == 1:
            return allowed[0]
        if n == 2:
            return (allowed[0] + allowed[1]) / 2.0

        # 3. trimmed mean across remaining venues
        vals = sorted(allowed)
        if n > 2:
            vals = vals[1:-1]  # drop min and max
            if not vals:
                # n == 3 case where min and max got removed
                return allowed[n // 2]

        return sum(vals) / len(vals)

    # ------------- helpers -------------

    def _set_mid(self, venue: str, bid: float, ask: float):
        """
        Validate bid and ask then update mid, spread and timestamp for venue.
        """
        if not (math.isfinite(bid) and math.isfinite(ask)):
            return
        if bid <= 0 or ask <= 0 or bid > ask:
            return

        mid = (bid + ask) / 2.0
        spread = ask - bid

        rec = self.latest.get(venue)
        if rec is None:
            return

        rec["mid"] = mid
        rec["spread"] = spread
        rec["ts"] = time.time()

    # ------------- websocket readers -------------

    async def _coinbase_reader(self):
        sub = {
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channels": ["ticker"],
        }

        while not self._stopped:
            try:
                async with websockets.connect(
                    COINBASE_URL, ping_interval=20, ping_timeout=20
                ) as ws:
                    await ws.send(json.dumps(sub))
                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("type") != "ticker":
                            continue
                        if msg.get("product_id") != "BTC-USD":
                            continue

                        best_bid = msg.get("best_bid")
                        best_ask = msg.get("best_ask")
                        if best_bid is None or best_ask is None:
                            continue

                        try:
                            bid = float(best_bid)
                            ask = float(best_ask)
                        except Exception:
                            continue

                        self._set_mid("coinbase", bid, ask)
            except Exception as e:
                print("coinbase reconnect:", e)
                await asyncio.sleep(1.0)

    async def _kraken_reader(self):
        sub = {
            "event": "subscribe",
            "pair": ["XBT/USD"],
            "subscription": {"name": "ticker"},
        }

        while not self._stopped:
            try:
                async with websockets.connect(
                    KRAKEN_URL, ping_interval=20, ping_timeout=20
                ) as ws:
                    await ws.send(json.dumps(sub))
                    async for raw in ws:
                        msg = json.loads(raw)

                        # public ticker messages are lists:
                        # [channel_id, data, "ticker", "XBT/USD"]
                        if (
                            isinstance(msg, list)
                            and len(msg) >= 4
                            and msg[2] == "ticker"
                        ):
                            pair = msg[3]
                            if pair != "XBT/USD":
                                continue

                            data = msg[1]
                            try:
                                bid = float(data["b"][0])
                                ask = float(data["a"][0])
                            except Exception:
                                continue

                            self._set_mid("kraken", bid, ask)
            except Exception as e:
                print("kraken reconnect:", e)
                await asyncio.sleep(1.0)

    async def _bitstamp_reader(self):
        channel = "order_book_btcusd"

        while not self._stopped:
            try:
                async with websockets.connect(
                    BITSTAMP_URL, ping_interval=20, ping_timeout=20
                ) as ws:
                    await ws.send(
                        json.dumps(
                            {
                                "event": "bts:subscribe",
                                "data": {"channel": channel},
                            }
                        )
                    )

                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("event") != "data":
                            continue

                        ch = msg.get("channel", "")
                        if ch != channel:
                            continue

                        data = msg.get("data", {})
                        bids = data.get("bids") or []
                        asks = data.get("asks") or []

                        if not (bids and asks):
                            continue

                        try:
                            bid = float(bids[0][0])
                            ask = float(asks[0][0])
                        except Exception:
                            continue

                        self._set_mid("bitstamp", bid, ask)
            except Exception as e:
                print("bitstamp reconnect:", e)
                await asyncio.sleep(1.0)

    # ------------- REST pollers -------------

    async def _cryptocom_reader(self):
        """
        Poll Crypto.com BTC_USD ticker for best bid and ask.

        Endpoint:
          GET /exchange/v1/public/get-tickers?instrument_name=BTC_USD

        Response structure:
          {
            "code": 0,
            "result": {
              "data": [
                {
                  "b": "bid_price",
                  "k": "ask_price",
                  ...
                }
              ]
            }
          }
        """
        while not self._stopped:
            try:
                async with aiohttp.ClientSession() as session:
                    while not self._stopped:
                        try:
                            async with session.get(
                                CRYPTOCOM_TICKER_URL, timeout=2
                            ) as resp:
                                if resp.status != 200:
                                    await asyncio.sleep(0.4)
                                    continue

                                payload = await resp.json()
                                if payload.get("code") != 0:
                                    await asyncio.sleep(0.4)
                                    continue

                                result = payload.get("result", {})
                                data = result.get("data") or []
                                if not data:
                                    await asyncio.sleep(0.4)
                                    continue

                                d0 = data[0]
                                b = d0.get("b")
                                k = d0.get("k")
                                if b is None or k is None:
                                    await asyncio.sleep(0.4)
                                    continue

                                try:
                                    bid = float(b)
                                    ask = float(k)
                                except Exception:
                                    await asyncio.sleep(0.4)
                                    continue

                                self._set_mid("cryptocom", bid, ask)

                            await asyncio.sleep(0.4)
                        except Exception as inner:
                            print("cryptocom poll error:", inner)
                            await asyncio.sleep(1.0)
            except Exception as e:
                print("cryptocom reconnect:", e)
                await asyncio.sleep(2.0)

    async def _gemini_reader(self):
        """
        Poll Gemini BTCUSD ticker for best bid and ask.

        Endpoint:
          GET https://api.gemini.com/v2/ticker/BTCUSD

        Response structure:
          {
            "symbol": "BTCUSD",
            "bid": "123.45",
            "ask": "123.46",
            ...
          }
        """
        while not self._stopped:
            try:
                async with aiohttp.ClientSession() as session:
                    while not self._stopped:
                        try:
                            async with session.get(
                                GEMINI_TICKER_URL, timeout=2
                            ) as resp:
                                if resp.status != 200:
                                    await asyncio.sleep(0.4)
                                    continue

                                payload = await resp.json()
                                b = payload.get("bid")
                                a = payload.get("ask")
                                if b is None or a is None:
                                    await asyncio.sleep(0.4)
                                    continue

                                try:
                                    bid = float(b)
                                    ask = float(a)
                                except Exception:
                                    await asyncio.sleep(0.4)
                                    continue

                                self._set_mid("gemini", bid, ask)

                            await asyncio.sleep(0.4)
                        except Exception as inner:
                            print("gemini poll error:", inner)
                            await asyncio.sleep(1.0)
            except Exception as e:
                print("gemini reconnect:", e)
                await asyncio.sleep(2.0)

    # ------------- sampler -------------

    async def _simple_sampler(self):
        """
        Once per second print synthetic BTC and per venue state.

        Example line:
        BTC synth=66750.12 (5)  coinbase=66751.00 spr=0.50 age=0.12s  ...
        """
        while not self._stopped:
            await asyncio.sleep(1.0)

            synth = self.get_btc()
            if synth is None:
                print("no fresh BTC sources")
                continue

            now = time.time()
            parts = []
            for venue, rec in self.latest.items():
                mid = rec["mid"]
                ts = rec["ts"]
                spread = rec["spread"]
                if mid is None or ts == 0.0:
                    continue
                age = now - ts
                if spread is None:
                    parts.append(
                        f"{venue}={mid:.2f} age={age:.2f}s"
                    )
                else:
                    parts.append(
                        f"{venue}={mid:.2f} spr={spread:.2f} age={age:.2f}s"
                    )

            joined = "  ".join(parts)
            #print(f"BTC synth={synth:.2f} ({len(parts)})  {joined}")


# example runner
if __name__ == "__main__":
    async def main():
        cfb = CFB()
        asyncio.create_task(cfb.run(log_sampler=True))

        # let feeds warm up a bit
        await asyncio.sleep(3.0)

        while True:
            print("BTC:", cfb.get_btc())
            await asyncio.sleep(1.0)

    asyncio.run(main())