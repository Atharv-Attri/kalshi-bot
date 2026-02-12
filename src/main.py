import asyncio
from kalshi import Kalshi
from utils import series
from rich import print
from types import SimpleNamespace
from pykalshi import Side

async def main(CONFIG):
    kalshi = Kalshi(CONFIG)

    markets = kalshi.get_mulitple_markets(500, series=[series.ATP, series.ATO])
#
    #print(markets[0])
    #markets = kalshi.filter_by_today(markets, True)
    #events = kalshi.get_unique_events(markets, save=True)
    #print(events)
    #kalshi.buy("KXNCAAMBGAME-26FEB11MICHNW-NW", Side.NO, 0.80)
    #kalshi.buy("KXNCAAMBGAME-26FEB11LIBNMSU-LIB", Side.NO, 0.45)

    #await kalshi.test()
    await kalshi.strategy_high_trade()
    # kalshi.gen_financials()


if __name__ == "__main__":
    CONFIG = SimpleNamespace(**{
        "L_LIMIT": 0.80,
        "U_LIMIT": 0.95,
        "SL": 0.50,
        "QTY": 25,
    })

    asyncio.run(main(CONFIG))
