import pandas as pd
from ta import momentum
from timing_market.utils import sip_irr


class BaseStrategy:

    def __init__(self, mkt: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, amt: int = 100):
        self.mkt = mkt
        self.start = start
        self.end = end
        self.amt = amt

    def run(self) -> pd.DataFrame:
        pass

    def irr(self):
        sip = self.run()
        return sip_irr(start=self.start, end=self.end, amt=self.amt, mkt=self.mkt, sip=sip)


class InvestOnFifth(BaseStrategy):
    def __init__(self, market, **kwargs):
        super().__init__(market, **kwargs)

    def run(self):
        self.mkt = self.mkt[self.mkt['date'] >= self.start]
        self.mkt = self.mkt[self.mkt['date'] <= self.end]

        dates = [i + pd.DateOffset(days=4) for i in pd.date_range(start=self.start, end=self.end, freq='MS')]
        dates = [self.mkt['date'][(self.mkt['date'] >= dt) & (self.mkt['date'] <= self.end)].min() for dt in dates]
        sip = pd.DataFrame({'date': dates, 'cashflow': [self.amt] * len(dates)})
        return sip


class InvestOnLowest(BaseStrategy):
    def __init__(self, market, **kwargs):
        super().__init__(market, **kwargs)

    def run(self):
        self.mkt = self.mkt[self.mkt['date'] >= self.start]
        self.mkt = self.mkt[self.mkt['date'] <= self.end]
        self.mkt['ym'] = self.mkt['date'].dt.to_period('M')

        result = self.mkt.loc[self.mkt.groupby('ym')['market'].idxmin()]

        result['cashflow'] = 100

        return result


class RSI(BaseStrategy):
    def __init__(self, market, **kwargs):
        super().__init__(market, **kwargs)

    def run(self):
        self.mkt = self.mkt[self.mkt['date'] >= self.start]
        self.mkt = self.mkt[self.mkt['date'] <= self.end]
        self.mkt['ym'] = self.mkt['date'].dt.to_period('M')
        self.mkt['rsi'] = momentum.rsi(self.mkt['market'], window=14)

        days = []
        for month, group in self.mkt.groupby('ym'):
            group = group.sort_values('date')

            rsi_30 = group[group['rsi'] < 44]
            if not rsi_30.empty:
                days.append(rsi_30.iloc[0]['date'])
            else:
                days.append(group.iloc[-1]['date'])
            # print("month", month, "rsi", days[-1])
            # print("group", group.head())

        sip = pd.DataFrame(days, columns=['date'])
        sip['cashflow'] = 100

        return sip
