import quantopian.algorithm as algo
from quantopian.pipeline import CustomFactor
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import StaticSids

equity_universe = symbols('SPY', 'EFA')
bonds_universe = symbols('LQD', 'HYG')
reits_universe = symbols('VNQ', 'REM')
stress_universe = symbols('GLD', 'TLT')


class Momentum(CustomFactor):
    inputs = [USEquityPricing.close]
    window_length = 6 * 21

    def compute(self, today, assets, out, close):
        out[:] = close[-1] / close[0] - 1


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
        screen=StaticSids(equity_universe +
                          bonds_universe +
                          reits_universe +
                          stress_universe),
    )


def record_vars(context, data):
    record(universe_cnt=len(context.portfolio.positions))


def before_trading_start(context, data):
    context.share_for_allocation = 0.99
    context.out = algo.pipeline_output('pipeline')
    context.to_hold = {x: 0.0 for x in (equity_universe +
                                        bonds_universe +
                                        reits_universe +
                                        stress_universe)}


def check_universe(context, universe):
    out = context.out

    candidates = out.loc[universe]
    candidates = candidates[candidates.momentum > 0]
    candidates = candidates.sort_values(by='momentum', ascending=False)

    if len(candidates) == 0:
        return

    top = candidates.index[0]
    context.to_hold[top] = 0.25 * context.share_for_allocation


def rebalance(context, data):
    check_universe(context, equity_universe)
    check_universe(context, bonds_universe)
    check_universe(context, reits_universe)
    check_universe(context, stress_universe)

    allocation = sorted(["{}={}".format(k.symbol, v)
                         for k, v in context.to_hold.items() if v > 0])
    print(' '.join(allocation))

    for stock, weight in context.to_hold.items():
        order_target_percent(stock, weight)
