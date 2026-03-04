import asyncio
import json
import websockets

POLYMARKET_MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class PolyBestBidAskClient:
    def __init__(self, asset_ids):
        # Accept a single string or a list
        if isinstance(asset_ids, str):
            asset_ids = [asset_ids]
        self.asset_ids = asset_ids

    async def run(self):
        async with websockets.connect(POLYMARKET_MARKET_WS) as ws:
            await self.subscribe(ws)
            await self.listen(ws)

    async def subscribe(self, ws):
        msg = {
            "assets_ids": self.asset_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }
        await ws.send(json.dumps(msg))
        print(f"Subscribed to assets_ids={self.asset_ids}")

    async def listen(self, ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                print("Received non json message:", raw)
                continue

            # Polymarket sometimes sends a list of events
            if isinstance(msg, list):
                for data in msg:
                    self.handle_event(data)
            else:
                self.handle_event(msg)

    def handle_event(self, data):
        if not isinstance(data, dict):
            return

        event_type = data.get("event_type")

        if event_type == "book":
            self.handle_book(data)
        elif event_type == "best_bid_ask":
            self.handle_best_bid_ask(data)
        # you can add more here if you care about other events

    def handle_book(self, data):
        return
        asset_id = data.get("asset_id")
        bids = data.get("bids", [])
        asks = data.get("asks", [])

        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None

        print(
            f"[book]best_bid={best_bid} best_ask={best_ask}"
        )

    def handle_best_bid_ask(self, data):
        asset_id = data.get("asset_id")
        best_bid = data.get("best_bid")
        best_ask = data.get("best_ask")

        print(
            f"[best_bid_ask] best_bid={best_bid} best_ask={best_ask}"
        )


async def main():
    # replace this with your actual asset id
    token_id = "39264093635999014397909458418499296415218517843552106886620575421382271729353"

    client = PolyBestBidAskClient(token_id)
    await client.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")