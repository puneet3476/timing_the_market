from concurrent.futures import ProcessPoolExecutor

from strategies import *
from utils import *
import pandas as pd


def run_single_simulation(args):
    ih, i, df = args

    print("ih: ", ih, "i: ", i)
    start, end = random_timeframe(df, ih)
    fifth = FixedDateInvest(df, start=start, end=end, day=5).irr()
    fifteen = FixedDateInvest(df, start=start, end=end, day=15).irr()
    twenty_fifth = FixedDateInvest(df, start=start, end=end, day=25).irr()
    rsi_35 = RSI(df, start=start, end=end, low=35, window=14).irr()
    rsi_40 = RSI(df, start=start, end=end, low=40, window=14).irr()
    rsi_45 = RSI(df, start=start, end=end, low=45, window=14).irr()
    macd = MACD(df, start=start, end=end, slow=26, fast=12, sign=9).irr()
    best = InvestOnLowest(df, start=start, end=end).irr()

    return {
        "fifth": fifth,
        "fifteen": fifteen,
        "twenty_fifth": twenty_fifth,
        "rsi_35": rsi_35,
        "rsi_40": rsi_40,
        "rsi_45": rsi_45,
        "macd": macd,
        "best": best,
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
