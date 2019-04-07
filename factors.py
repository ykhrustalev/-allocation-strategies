from quantopian.pipeline import CustomFactor
from quantopian.pipeline.data.builtin import USEquityPricing


class F13612A(CustomFactor):
    inputs = [USEquityPricing.close]
    window_length = 12 * 21

    @staticmethod
    def rate(series, days):
        p = -days
        return (series[-1] - series[p]) / series[p]

    def compute(self, today, assets, out, close):
        r1 = self.rate(close, 21)
        r3 = self.rate(close, 21 * 3)
        r6 = self.rate(close, 21 * 6)
        r12 = self.rate(close, 21 * 12)
        out[:] = (r1 + r3 + r6 + r12) / 4


class F13612W(CustomFactor):
    inputs = [USEquityPricing.close]
    window_length = 12 * 21

    @staticmethod
    def rate(series, days):
        p = -days
        return (series[-1] - series[p]) / series[p]

    def compute(self, today, assets, out, close):
        r1 = self.rate(close, 21)
        r3 = self.rate(close, 21 * 3)
        r6 = self.rate(close, 21 * 6)
        r12 = self.rate(close, 21 * 12)
        out[:] = (12 * r1 + 4 * r3 + 2 * r6 + 1 * r12) / 4
