from concurrent.futures import ProcessPoolExecutor

from strategies import *
from utils import *
import pandas as pd


def run_single_simulation(args):
    ih, i, df = args

    print("ih: ", ih, "i: ", i)
    start, end = random_timeframe(df, ih)
    fifth = FixedDateInvest(df, start=start, end=end, day=5).run()
    fifteen = FixedDateInvest(df, start=start, end=end, day=15).run()
    twenty_fifth = FixedDateInvest(df, start=start, end=end, day=25).run()
    rsi_35 = RSI(df, start=start, end=end, low=35, window=14).run()
    rsi_40 = RSI(df, start=start, end=end, low=40, window=14).run()
    rsi_45 = RSI(df, start=start, end=end, low=45, window=14).run()
    macd = MACD(df, start=start, end=end, slow=26, fast=12, sign=9).run()
    best = InvestOnLowest(df, start=start, end=end).run()

    return {
        "fifth_irr": fifth[0],
        "fifteen_irr": fifteen[0],
        "twenty_fifth_irr": twenty_fifth[0],
        "rsi_35_irr": rsi_35[0],
        "rsi_40_irr": rsi_40[0],
        "rsi_45_irr": rsi_45[0],
        "macd_irr": macd[0],
        "best_irr": best[0],
        "fifth_excess": fifth[1] / fifth[1] - 1,
        "fifteen_excess": fifteen[1] / fifth[1] - 1,
        "twenty_fifth_excess": twenty_fifth[1] / fifth[1] - 1,
        "rsi_35_excess": rsi_35[1] / fifth[1] - 1,
        "rsi_40_excess": rsi_40[1] / fifth[1] - 1,
        "rsi_45_excess": rsi_45[1] / fifth[1] - 1,
        "macd_excess": macd[1] / fifth[1] - 1,
        "best_excess": best[1] / fifth[1] - 1,
    }


def run_sim(ih: int, df: pd.DataFrame):
    with ProcessPoolExecutor(max_workers=32) as executor:
        results = list(executor.map(run_single_simulation, [(ih, i, df) for i in range(1000)]))

    res = pd.DataFrame(results).mean()
    res.to_csv(f'result/{ih}.csv')


def main():
    df = pd.read_csv('data/NIFTY_TRI.csv')
    df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')
    df['Price'] = df['Price'].str.replace(',', '').astype(float)
    df = df.sort_values('Date')
    df = df.rename(columns={'Date': 'date', 'Price': 'market'})[['date', 'market']]

    for ih in [1, 3, 5, 10, 15]:
        run_sim(ih, df)


if __name__ == '__main__':
    main()
