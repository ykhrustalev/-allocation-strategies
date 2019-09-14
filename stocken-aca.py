from collections import defaultdict

import quantopian.algorithm as algo
from numpy import nanmax, nanmin
from quantopian.pipeline import CustomFactor
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import StaticSids


class Slice:
    def __init__(self, risk, stress, up_bound, down_bound):
        self.risk = risk
        self.stress = stress
        self.is_risk = None
        self.up_bound = up_bound
        self.down_bound = down_bound
        self.year = None

    def symbols(self):
        return [self.risk, self.stress]

    def current(self):
        return self.risk if self.is_risk else self.stress

    def calculate(self, price, max_price, min_price):
        """
        Go long the risk asset at today's close when the risk asset will end the
        day above its upper channel (highest close of previous n-days).
        Switch to the defensive asset at today's close when the risk asset will
        end the day below its lower channel (lowest close of previous n-days).
        Hold positions until a change in signal.
        Rebalance the entire portfolio either on a change in signal or on
        the last trading day of the calendar year.
        """
        if self.is_risk is None:
            is_risk = price > min_price
        elif self.is_risk:
            is_risk = price > min_price
        else:
            is_risk = price > max_price

        year = get_datetime().date().year

        signaled = is_risk != self.is_risk
        if self.year != year:
            signaled = True

        self.is_risk = is_risk
        if signaled:
            self.year = year

        return signaled


risky1 = Slice(symbol('SPY'), symbol('IEF'), 'up6', 'down12')
risky2 = Slice(symbol('VNQ'), symbol('IEF'), 'up6', 'down12')
risky3 = Slice(symbol('VWO'), symbol('VWOB'), 'up6', 'down12')
stress1 = Slice(symbol('GLD'), symbol('TLT'), 'up12', 'down6')

all_slices = (
    risky1,
    risky2,
    risky3,
    stress1,
)

all_symbols = []
for _slice in all_slices:
    all_symbols.extend(_slice.symbols())

to_hold = defaultdict(float)


class TrendFactor(CustomFactor):
    inputs = [USEquityPricing.close]
    func = None

    def compute(self, today, assets, out, close):
        if self.func == nanmax:
            v = nanmax(close, axis=0)
        else:
            v = nanmin(close, axis=0)

        out[:] = v


class Up6(TrendFactor):
    window_length = 6 * 21
    func = nanmax


class Up12(TrendFactor):
    window_length = 12 * 21
    func = nanmax


class Down6(TrendFactor):
    window_length = 6 * 21
    func = nanmin


class Down12(TrendFactor):
    window_length = 12 * 21
    func = nanmin


def initialize(context):
    algo.schedule_function(
        rebalance,
        algo.date_rules.month_start(),
        algo.time_rules.market_open(),
    )

    algo.schedule_function(
        record_vars,
        algo.date_rules.every_day(),
        algo.time_rules.market_close(),
    )

    algo.attach_pipeline(make_pipeline(), 'pipeline')


def make_pipeline():
    return Pipeline(
        columns={
            'close': USEquityPricing.close.latest,
            'up6': Up6(),
            'up12': Up12(),
            'down6': Down6(),
            'down12': Down12(),
        },
        screen=StaticSids(all_symbols),
    )


def record_vars(context, data):
    record(
        positions_cnt=len(context.portfolio.positions),
        risky=len(set(x for x in all_slices if x.is_risk)),
    )


def before_trading_start(context, data):
    context.share_for_allocation = 0.99
    context.out = algo.pipeline_output('pipeline')


def check_slice(context, slice):
    out = context.out

    price = out.loc[slice.risk, 'close']
    max_price = out.loc[slice.risk, slice.up_bound]
    min_price = out.loc[slice.risk, slice.down_bound]

    return slice.calculate(price, max_price, min_price)


def rebalance(context, data):
    signaled = [check_slice(context, sl) for sl in all_slices]
    if not any(signaled):
        return

    log.info('signaled {}'.format(signaled))

    global to_hold
    to_hold = defaultdict(float)

    share = 1.0 / len(all_slices)

    for sl in all_slices:
        to_hold[sl.current()] += share * context.share_for_allocation

    allocation = sorted(["{}={}".format(k.symbol, v)
                         for k, v in to_hold.items() if v > 0])
    log.info('allocation {}'.format(' '.join(allocation)))

    for stock, weight in to_hold.items():
        log.info("target {} {}", stock, weight)
        order_target_percent(stock, weight)
