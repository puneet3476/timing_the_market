import pandas as pd
from PIL.ImageChops import offset
from ta import momentum, trend
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


class FixedDateInvest(BaseStrategy):
    def __init__(self, market, day: int, **kwargs):
        super().__init__(market, **kwargs)
        self.day = day

    def run(self):
        self.mkt = self.mkt[self.mkt['date'] >= self.start]
        self.mkt = self.mkt[self.mkt['date'] <= self.end]

        dates = [i + pd.DateOffset(days=self.day) for i in pd.date_range(start=self.start, end=self.end, freq='MS')]
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

        result['cashflow'] = self.amt

        return result


class RSI(BaseStrategy):
    def __init__(self, market, window: int, low: int, **kwargs):
        super().__init__(market, **kwargs)
        self.window = window
        self.low = low

    def run(self):
        self.mkt = self.mkt[self.mkt['date'] >= self.start]
        self.mkt = self.mkt[self.mkt['date'] <= self.end]
        self.mkt['ym'] = self.mkt['date'].dt.to_period('M')
        self.mkt['rsi'] = momentum.rsi(self.mkt['market'], window=self.window, )

        days = []
        for month, group in self.mkt.groupby('ym'):
            group = group.sort_values('date')

            rsi_threshold = group[group['rsi'] < self.low]
            if not rsi_threshold.empty:
                days.append(rsi_threshold.iloc[0]['date'])
            else:
                days.append(group.iloc[-1]['date'])

        sip = pd.DataFrame(days, columns=['date'])
        sip['cashflow'] = self.amt
        sip = sip.sort_values(by='date')

        return sip


class MACD(BaseStrategy):
    def __init__(self, market, fast: int = 12, slow: int = 26, sign: int = 9, **kwargs):
        super().__init__(market, **kwargs)
        self.fast = fast
        self.slow = slow
        self.sign = sign

    def run(self):
        self.mkt = self.mkt[self.mkt['date'] >= self.start]
        self.mkt = self.mkt[self.mkt['date'] <= self.end]
        self.mkt['ym'] = self.mkt['date'].dt.to_period('M')
        self.mkt['macd'] = trend.macd_signal(
            self.mkt['market'],
            window_slow=self.slow,
            window_fast=self.fast,
            window_sign=self.sign,
            fillna=True,
        )

        days = []
        for month, group in self.mkt.groupby('ym'):
            group = group.sort_values('date')

            macd_cross = group[group['macd'] > 0]
            if not macd_cross.empty:
                days.append(macd_cross.iloc[0]['date'])
            else:
                days.append(group.iloc[-1]['date'])

        sip = pd.DataFrame(days, columns=['date'])
        sip['cashflow'] = self.amt
        sip = sip.sort_values(by='date')

        return sip
