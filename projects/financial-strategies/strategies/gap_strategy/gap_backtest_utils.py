import pandas as pd
import polars as pl
from datetime import time as dtime

from bt_engine.engine import (
    RiskManager,
    PositionManager,
    StrategyEvaluator,
)


def first_true(series):
    idx = series.idxmax() if series.any() else None
    result = pd.Series(False, index=series.index)
    if idx is not None:
        result.loc[idx] = True
    return result


# data is loaded with DataLoader --> PolygonDataLoader from backtest-engine helpers.


class GapPositionManager(PositionManager):
    def __init__(
        self,
        overnight_gap: float,
        stop_loss_pct: float,
        take_profit_pct: float,
        entry_cutoff_time_cst: dtime = dtime(13, 45),
        flatten_trade_time_cst: dtime = dtime(14, 55),
    ):
        self.overnight_gap = overnight_gap
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.entry_cutoff_time_cst = entry_cutoff_time_cst
        self.flatten_trade_time_cst = flatten_trade_time_cst

    def detect_trade(
        self,
        df: [pd.DataFrame, pl.DataFrame],
    ) -> (pd.DataFrame, pl.DataFrame):
        df.loc[
            (df["adj_pmkt_pct_chg"] >= self.overnight_gap)
            & (df["timestamp_cst"].dt.time <= self.entry_cutoff_time_cst),
            "trigger_trade_entry_pmkt_raw",
        ] = True

        df.loc[
            (df["adj_mkt_pct_chg"] >= self.overnight_gap)
            & (df["timestamp_cst"].dt.time <= self.entry_cutoff_time_cst),
            "trigger_trade_entry_mkt_raw",
        ] = True

        df["trigger_trade_entry_pmkt"] = df.groupby(
            ["ticker", df["timestamp_cst"].dt.date]
        )["trigger_trade_entry_pmkt_raw"].transform(first_true)

        df["trigger_trade_entry_mkt"] = df.groupby(
            ["ticker", df["timestamp_cst"].dt.date]
        )["trigger_trade_entry_mkt_raw"].transform(first_true)

        df["trigger_trade_entry"] = (
            df["trigger_trade_entry_pmkt"] | df["trigger_trade_entry_mkt"]
        )
        df = df.drop(
            columns=["trigger_trade_entry_pmkt_raw", "trigger_trade_entry_mkt_raw"]
        )
        return df

    def _open_position(self, df: [pd.DataFrame, pl.DataFrame]):
        is_first_timestamp = df["timestamp_cst"] <= df.groupby(df["date"])[
            "timestamp_cst"
        ].transform("min")

        df.loc[df["trigger_trade_entry"], "theoretical_bto_price"] = df.loc[
            df["trigger_trade_entry"], "high"
        ].where(
            is_first_timestamp[
                df["trigger_trade_entry"]
            ],  # Use the pre-calculated condition
            df.loc[df["trigger_trade_entry"], "prev_close"] * (1 + self.overnight_gap),
        )

        df.loc[df["trigger_trade_entry"], "theoretical_sto_price"] = df.loc[
            df["trigger_trade_entry"], "low"
        ].where(
            is_first_timestamp[
                df["trigger_trade_entry"]
            ],  # Use the pre-calculated condition
            df.loc[df["trigger_trade_entry"], "prev_close"] * (1 + self.overnight_gap),
        )

    def _trigger_stop_loss(self, df: [pd.DataFrame, pl.DataFrame]):
        df.loc["stop_loss_triggered"] = False

    def adjust_position(
        self, df: [pd.DataFrame, pl.DataFrame]
    ) -> (pd.DataFrame, pl.DataFrame):
        pass


class GapStrategyRiskManager(RiskManager):
    def quantify_risk(
        self, df: [pd.DataFrame, pl.DataFrame]
    ) -> (pd.DataFrame, pl.DataFrame):
        return df


class GapStrategyEvaluator(StrategyEvaluator):
    def evaluate(
        self, df: [pd.DataFrame, pl.DataFrame]
    ) -> (pd.DataFrame, pl.DataFrame):
        return df
