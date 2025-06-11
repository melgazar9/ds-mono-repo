# import os

# import pandas as pd
# import polars as pl

from bt_engine.central_loaders import PolygonBarLoader
from bt_engine.engine import BacktestEngine

# import os; os.chdir('projects/financial-strategies/strategies/gap_strategy')
from gap_backtest_utils import (
    GapStrategyRiskManager,
    GapPositionManager,
    GapStrategyEvaluator,
)

# Rules
# 1. If gap from previous day close to either pre-market open or market open is >= X% (adjusted) then trigger trade entry
# 2. Iterate over different stop and take loss percentages
# 3. Do not enter trade after 13:45 CST
# 4. All positions must be flattened by 14:55 CST if
# 5. Segment by groups --> HTB / short interest/volume buckets, market cap bucket, industry, VIX range, etc.


df_prev = PolygonBarLoader(
    cur_day_file="~/polygon_data/bars_1min/bars_1m_2025-05-14.csv.gz"
).load_raw_intraday_bars()

gbt = BacktestEngine(
    data_loader=PolygonBarLoader(
        load_method="pandas",
        cur_day_file="~/polygon_data/bars_1min/bars_1m_2025-05-15.csv.gz",
        df_prev=df_prev,
    ),
    risk_manager=GapStrategyRiskManager(),
    position_manager=GapPositionManager(
        overnight_gap=0.1, stop_loss_pct=0.01, take_profit_pct=0.01
    ),
    strategy_evaluator=GapStrategyEvaluator(),
)


gbt.load_and_clean_data()

gbt.run_backtest()
