from random import choice

from pyxirr import xirr
import pandas as pd


def random_timeframe(df: pd.DataFrame, years: int) -> list[pd.Timestamp]:
    dates = df['date'].tolist()
    mx = max(dates)
    mn = min(dates)

    start_candidates = [
        dates[i] for i in range(0, len(dates))
        if (dates[i] + pd.DateOffset(years=years)).to_period('M').to_timestamp() < mx
           and dates[i].to_period('M').to_timestamp() > mn
    ]

    # start of the month
    start = choice(start_candidates).to_period('M').to_timestamp()

    # start of the last month - 1
    end = (start + pd.DateOffset(years=years)).to_period('M').to_timestamp() - pd.Timedelta(days=1)

    return [start, end]


def returns(df: pd.DataFrame, end: pd.Timestamp = None) -> pd.DataFrame:
    df = df[df['date'] <= end]
    df = df.sort_values('date')
    end_price = df['market'].iloc[-1]
    df['r'] = end_price / df['market']

    return df


def sip_irr(mkt: pd.DataFrame, sip: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, amt: int = 100) -> float:
    """
    :param amt: amount of monthly sip
    :param mkt: [date, market]
    :param sip: [date, cashflow]
    :param end:
    :return:
    """
    mkt = returns(mkt, end)

    sip = pd.merge(sip, mkt, on='date', how='inner')
    sip_portfolio_value = (sip['cashflow'] * sip['r']).sum()

    # Cash inflows to the bank
    dates = []
    cashflows = []

    # TODO: Interest on monthly balance

    for i in pd.date_range(start=start, end=end, freq='MS'):
        dates.append(i)
        cashflows.append(-1 * amt)

    cashflows.append(sip_portfolio_value)
    dates.append(end)

    irr = xirr(dates, cashflows)

    # print("*" * 100)
    # print("SIPS: ", len(sip))
    # print("CASHFLOWS: ", len(cashflows))
    # print("Dates: ", dates)
    # print("Cashflows: ", cashflows)
    # print("START: ", start)
    # print("END: ", end)
    # print("IRR: ", irr)
    return irr
