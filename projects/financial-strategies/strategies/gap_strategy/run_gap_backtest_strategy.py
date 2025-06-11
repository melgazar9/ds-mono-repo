import pandas as pd
import polars as pl

from bt_engine.central_loaders import PolygonBarLoader
from bt_engine.engine import BacktestEngine
from gap_backtest_utils import (
    GapStrategyRiskManager,
    GapPositionManager,
    GapStrategyEvaluator,
)


class GapBacktestingEngine(BacktestEngine):
    def load_and_clean_data(self, cur_day_file) -> (pd.DataFrame, pl.DataFrame):
        self.data_loader.load_and_clean_data(cur_day_file)


gbt = GapBacktestingEngine(
    data_loader=PolygonBarLoader(load_method="pandas"),
    risk_manager=GapStrategyRiskManager(),
    position_manager=GapPositionManager(),
    strategy_evaluator=GapStrategyEvaluator(),
)

gbt.data_loader.df_prev = gbt.data_loader.load_raw_intraday_bars(
    "~/polygon_data/bars_1min/bars_1m_2025-05-15.csv.gz"
)
gbt.load_and_clean_data(
    cur_day_file="~/polygon_data/bars_1min/bars_1m_2025-05-15.csv.gz"
)

gbt.data_loader.df.head(2)
