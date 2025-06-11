import pandas as pd
import polars as pl
from bt_engine.engine import (
    RiskManager,
    PositionManager,
    StrategyEvaluator,
)

# data is loaded with DataLoader --> PolygonDataLoader from backtest-engine helpers.


class GapPositionManager(PositionManager):
    def __init__(self, overnight_gap: float):
        self.overnight_gap = overnight_gap

    def detect_trade(
        self,
        df: [pd.DataFrame, pl.DataFrame],
    ) -> (pd.DataFrame, pl.DataFrame):
        df["trigger_trade_entry_mkt"] = False
        df.loc[
            df["adj_mkt_pct_chg"] >= self.overnight_gap, "trigger_trade_entry_mkt"
        ] = True

        df["trigger_trade_entry_pmkt"] = False
        df.loc[
            df["adj_pmkt_pct_chg"] >= self.overnight_gap, "trigger_trade_entry_pmkt"
        ] = True

        df["trigger_trade_entry"] = False
        df.loc[
            (df["trigger_trade_entry_pmkt"]) | (df["trigger_trade_entry_mkt"]),
            "trigger_trade_entry",
        ] = True

    def adjust_position(
        self, df: [pd.DataFrame, pl.DataFrame]
    ) -> (pd.DataFrame, pl.DataFrame):
        pass


class GapStrategyRiskManager(RiskManager):
    def quantify_risk(
        self, df: [pd.DataFrame, pl.DataFrame]
    ) -> (pd.DataFrame, pl.DataFrame):
        pass


class GapStrategyEvaluator(StrategyEvaluator):
    def evaluate(
        self, df: [pd.DataFrame, pl.DataFrame]
    ) -> (pd.DataFrame, pl.DataFrame):
        pass
