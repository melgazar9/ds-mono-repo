import pandas as pd
import polars as pl
from datetime import time as dtime

from typing import Union
from bt_engine.execution import apply_slippage
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
        bet_amount: float,
        stop_loss_pct: float,
        take_profit_pct: float,
        entry_cutoff_time_cst: dtime = dtime(13, 45),
        flatten_trade_time_cst: dtime = dtime(14, 55),
        slippage_amount=0.38,
        slippage_mode="partway",
    ):
        self.overnight_gap = overnight_gap
        self.bet_amount = bet_amount
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.entry_cutoff_time_cst = entry_cutoff_time_cst
        self.flatten_trade_time_cst = flatten_trade_time_cst
        self.slippage_amount = slippage_amount
        self.slippage_mode = slippage_mode

    def detect_trade(
        self,
        df: Union[pd.DataFrame, pl.DataFrame],
    ) -> Union[pd.DataFrame, pl.DataFrame]:
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

        df["trigger_trade_entry_pmkt"] = df.groupby(["ticker", "date"])[
            "trigger_trade_entry_pmkt_raw"
        ].transform(first_true)
        df["trigger_trade_entry_mkt"] = df.groupby(["ticker", "date"])[
            "trigger_trade_entry_mkt_raw"
        ].transform(first_true)

        df["trigger_trade_entry"] = (
            df["trigger_trade_entry_pmkt"] | df["trigger_trade_entry_mkt"]
        )
        df["trigger_trade_entry"] = df.groupby(["ticker", "date"])[
            "trigger_trade_entry"
        ].transform(first_true)

        df = df.drop(
            columns=["trigger_trade_entry_pmkt_raw", "trigger_trade_entry_mkt_raw"]
        )
        return df

    def _open_position(self, df: Union[pd.DataFrame, pl.DataFrame]):
        first_timestamp = df.groupby(["ticker", "date"]).cumcount() == 0
        df.loc[df["trigger_trade_entry"], "theoretical_bto_price"] = (
            df["prev_market_close"] * self.overnight_gap
        )
        df["theoretical_sto_price"] = df["theoretical_bto_price"]

        # assume buy at high / sell at low if the gap happens at the open
        df.loc[first_timestamp, "theoretical_bto_price"] = df.loc[
            first_timestamp, "high"
        ]
        df.loc[first_timestamp, "theoretical_sto_price"] = df.loc[
            first_timestamp, "low"
        ]

        df["bto_price_with_slippage"] = apply_slippage(
            df,
            price_col="theoretical_bto_price",
            slippage_amount=self.slippage_amount,
            slippage_mode=self.slippage_mode,
            side="buy",
        )

        df["sto_price_with_slippage"] = apply_slippage(
            df,
            price_col="theoretical_sto_price",
            slippage_amount=self.slippage_amount,
            slippage_mode=self.slippage_mode,
            side="sell",
        )

        df.loc[df["trigger_trade_entry"], "shares_long"] = (
            self.bet_amount / df["bto_price_with_slippage"]
        )
        df.loc[df["trigger_trade_entry"], "shares_short"] = (
            self.bet_amount / df["sto_price_with_slippage"]
        )
        return df

    def _trigger_stop_loss(self, df: Union[pd.DataFrame, pl.DataFrame]):
        df.loc["stop_loss_triggered"] = False
        return df

    def _close_position(self, df: Union[pd.DataFrame, pl.DataFrame]):
        df = self._trigger_stop_loss(df)

    def adjust_position(
        self, df: Union[pd.DataFrame, pl.DataFrame]
    ) -> Union[pd.DataFrame, pl.DataFrame]:
        self._open_position(df)
        self._close_position(df)
        return df


class GapStrategyRiskManager(RiskManager):
    def quantify_risk(
        self, df: Union[pd.DataFrame, pl.DataFrame]
    ) -> Union[pd.DataFrame, pl.DataFrame]:
        return df


class GapStrategyEvaluator(StrategyEvaluator):
    def evaluate(
        self, df: Union[pd.DataFrame, pl.DataFrame]
    ) -> Union[pd.DataFrame, pl.DataFrame]:
        return df
