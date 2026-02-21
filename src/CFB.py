import asyncio, json, time, websockets, math

COINBASE_URL = "wss://ws-feed.exchange.coinbase.com"
KRAKEN_URL = "wss://ws.kraken.com"
BITSTAMP_URL = "wss://ws.bitstamp.net"


class CFB:
    """
    Coinbase + Kraken + Bitstamp aggregator for BTCUSD, ETHUSD, SOLUSD, XRPUSD.

    Provides:
        get_btc()
        get_eth()
        get_sol()
        get_xrp()

    Each returns a synthetic trimmed mean of fresh mids (age <= STALE_SEC).
    """

    STALE_SEC = 2.0  # seconds

    def __init__(self):
        assets = ["BTC", "ETH", "SOL", "XRP"]

        # last mid + timestamp per venue * per asset
        self.latest = {
            asset: {
                "coinbase": {"mid": None, "ts": 0.0},
                "kraken": {"mid": None, "ts": 0.0},
                "bitstamp": {"mid": None, "ts": 0.0},
            }
            for asset in assets
        }

        # control
        self._tasks: list[asyncio.Task] = []
        self._stopped = False

    # -------- public API --------

    async def run(self, log_sampler=False):
        """
        Start websocket readers.

        If log_sampler=True, print this each second:
            BTC synth=xxxxx coinbase=... kraken=... bitstamp=...
        (no EMA, just raw synth)
        """
        if self._tasks:
            return

        self._stopped = False

        self._tasks.append(asyncio.create_task(self._coinbase_reader()))
        self._tasks.append(asyncio.create_task(self._kraken_reader()))
        self._tasks.append(asyncio.create_task(self._bitstamp_reader()))

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

    # ----- synthetic getters -----

    def get_btc(self): return self._get_synth("BTC")
    def get_eth(self): return self._get_synth("ETH")
    def get_sol(self): return self._get_synth("SOL")
    def get_xrp(self): return self._get_synth("XRP")

    def _get_synth(self, asset: str):
        now = time.time()
        mids = []

        for rec in self.latest[asset].values():
            mid = rec["mid"]
            if mid is None:
                continue
            if now - rec["ts"] <= self.STALE_SEC:
                mids.append(mid)

        if not mids:
            return None

        return self._trimmed_mean(mids)

    # -------- helpers --------

    @staticmethod
    def _trimmed_mean(values):
        vals = sorted(values)
        if len(vals) == 0:
            return None
        if len(vals) <= 2:
            return sum(vals) / len(vals)
        vals = vals[1:-1]
        return sum(vals) / len(vals)

    def _set_mid(self, asset, venue, bid, ask):
        if not (math.isfinite(bid) and math.isfinite(ask)):
            return
        if bid <= 0 or ask <= 0 or bid > ask:
            return
        mid = (bid + ask) / 2.0
        self.latest[asset][venue]["mid"] = mid
        self.latest[asset][venue]["ts"] = time.time()

    # -------- websocket readers --------

    async def _coinbase_reader(self):
        sub = {
            "type": "subscribe",
            "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"],
            "channels": ["ticker"],
        }

        def map_pid(pid):
            if pid == "BTC-USD": return "BTC"
            if pid == "ETH-USD": return "ETH"
            if pid == "SOL-USD": return "SOL"
            if pid == "XRP-USD": return "XRP"
            return None

        while not self._stopped:
            try:
                async with websockets.connect(
                    COINBASE_URL, ping_interval=20, ping_timeout=20
                ) as ws:
                    await ws.send(json.dumps(sub))
                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("type") == "ticker" and "best_bid" in msg:
                            asset = map_pid(msg.get("product_id", ""))
                            if not asset:
                                continue
                            try:
                                bid = float(msg["best_bid"])
                                ask = float(msg["best_ask"])
                            except:
                                continue
                            self._set_mid(asset, "coinbase", bid, ask)
            except Exception as e:
                print("coinbase reconnect:", e)
                await asyncio.sleep(1)

    async def _kraken_reader(self):
        sub = {
            "event": "subscribe",
            "pair": ["XBT/USD", "ETH/USD", "SOL/USD", "XRP/USD"],
            "subscription": {"name": "ticker"},
        }

        def map_pair(p):
            if p == "XBT/USD": return "BTC"
            if p == "ETH/USD": return "ETH"
            if p == "SOL/USD": return "SOL"
            if p == "XRP/USD": return "XRP"
            return None

        while not self._stopped:
            try:
                async with websockets.connect(
                    KRAKEN_URL, ping_interval=20, ping_timeout=20
                ) as ws:
                    await ws.send(json.dumps(sub))
                    async for raw in ws:
                        msg = json.loads(raw)

                        if isinstance(msg, list) and len(msg) >= 4 and msg[2] == "ticker":
                            pair = msg[3]
                            asset = map_pair(pair)
                            if not asset:
                                continue

                            data = msg[1]
                            try:
                                bid = float(data["b"][0])
                                ask = float(data["a"][0])
                            except:
                                continue

                            self._set_mid(asset, "kraken", bid, ask)
            except Exception as e:
                print("kraken reconnect:", e)
                await asyncio.sleep(1)

    async def _bitstamp_reader(self):
        channels = [
            "order_book_btcusd",
            "order_book_ethusd",
            "order_book_solusd",
            "order_book_xrpusd",
        ]

        def map_ch(ch):
            if ch.endswith("btcusd"): return "BTC"
            if ch.endswith("ethusd"): return "ETH"
            if ch.endswith("solusd"): return "SOL"
            if ch.endswith("xrpusd"): return "XRP"
            return None

        while not self._stopped:
            try:
                async with websockets.connect(
                    BITSTAMP_URL, ping_interval=20, ping_timeout=20
                ) as ws:
                    for ch in channels:
                        await ws.send(json.dumps({"event": "bts:subscribe", "data": {"channel": ch}}))

                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("event") != "data":
                            continue

                        ch = msg.get("channel", "")
                        asset = map_ch(ch)
                        if not asset:
                            continue

                        data = msg.get("data", {})
                        bids = data.get("bids") or []
                        asks = data.get("asks") or []

                        if not (bids and asks):
                            continue

                        try:
                            bid = float(bids[0][0])
                            ask = float(asks[0][0])
                        except:
                            continue

                        self._set_mid(asset, "bitstamp", bid, ask)
            except Exception as e:
                print("bitstamp reconnect:", e)
                await asyncio.sleep(1)

    # -------- simple sampler (no EMA) --------

    async def _simple_sampler(self):
        """
        If enabled, prints BTC synth + raw venue mids once per second.
        No smoothing, no EMA.
        """
        while not self._stopped:
            await asyncio.sleep(1)

            synth = self.get_btc()
            mids = self.latest["BTC"]

            if synth is None:
                print("no fresh BTC sources")
                continue

            parts = " ".join([f"{v}={m['mid']:.2f}" for v, m in mids.items() if m["mid"]])
            #print(f"BTC synth={synth:.2f}  {parts}")


# example runner
if __name__ == "__main__":
    async def main():
        cfb = CFB()
        asyncio.create_task(cfb.run(log_sampler=True))

        await asyncio.sleep(3)

        while True:
            print(
                "BTC:", cfb.get_btc(),
                "ETH:", cfb.get_eth(),
                "SOL:", cfb.get_sol(),
                "XRP:", cfb.get_xrp()
            )
            await asyncio.sleep(1)

    asyncio.run(main())