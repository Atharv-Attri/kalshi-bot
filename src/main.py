from kalshi import Kalshi
from utils import series
from rich import print
from types import SimpleNamespace


def main(CONFIG):
    kalshi = Kalshi(CONFIG)

    markets = kalshi.get_mulitple_markets(500, series=[series.NCAA_BB_W])
    events = kalshi.get_unique_events(markets, save=True)
    events = kalshi.filter_by_today(True)
    
    kalshi.strategy_high()
    kalshi.gen_financials()

if __name__ == "__main__":
    CONFIG = SimpleNamespace(**{
        "L_LIMIT": 0.91,
        "U_LIMIT": 0.97,
        "SL": 0.50,
        "QTY": 25,
    })    
    main(CONFIG)
    
