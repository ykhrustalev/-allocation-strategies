import quantopian.algorithm as algo
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import StaticSids

universe = symbols('SPY', 'IEF')


def initialize(context):
    algo.schedule_function(
        rebalance,
        algo.date_rules.month_start(),
        algo.time_rules.market_open(),
    )

    algo.attach_pipeline(make_pipeline(), 'pipeline')


def make_pipeline():
    return Pipeline(
        columns={
            'close': USEquityPricing.close.latest,
        },
        screen=StaticSids(universe)
    )


def before_trading_start(context, data):
    context.share_for_allocation = 0.99


def rebalance(context, data):
    order_target_percent(symbol('SPY'), 0.6 * context.share_for_allocation)
    order_target_percent(symbol('IEF'), 0.4 * context.share_for_allocation)
