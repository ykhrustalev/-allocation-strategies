import quantopian.algorithm as algo
from quantopian.pipeline import CustomFactor
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import StaticSids

universe = symbols('LQD', 'HYG', 'BIL')


class Momentum(CustomFactor):
    inputs = [USEquityPricing.close]
    window_length = 6 * 21

    def compute(self, today, assets, out, close):
        out[:] = (close[-1] - close[0]) / close[0]


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
            'momentum': Momentum(),
        },
        screen=StaticSids(universe),
    )


def record_vars(context, data):
    counters = {x.symbol: 0 for x in universe}
    for s in context.portfolio.positions.keys():
        counters[s.symbol] = 1
    record(**counters)


def before_trading_start(context, data):
    context.share_for_allocation = 0.99
    context.out = algo.pipeline_output('pipeline')
    context.to_hold = {x: 0.0 for x in universe}


def check_universe(context, universe):
    out = context.out

    candidates = out.loc[universe]
    candidates = candidates.sort_values(by='momentum', ascending=False)

    if len(candidates) == 0:
        log.error('empty candidates %s', out)
        return

    top = candidates.index[0]
    context.to_hold[top] = context.share_for_allocation


def rebalance(context, data):
    check_universe(context, universe)

    allocation = sorted(["{}={}".format(k.symbol, v)
                         for k, v in context.to_hold.items() if v > 0])
    print(' '.join(allocation))

    for stock, weight in context.to_hold.items():
        order_target_percent(stock, weight)
