import pandas as pd
import polars as pl
import numpy as np
import warnings
from datetime import time as dtime
from typing import Union
import logging

from bt_engine.execution import apply_slippage
from bt_engine.evaluators import (
    calc_win_rate,
    calc_profit_factor,
    calc_intraday_sharpe_ratio,
    calc_max_drawdown,
    calc_sortino_ratio,
)

from bt_engine.engine import RiskManager, PositionManager, StrategyEvaluator
from ds_core.db_connectors import PostgresConnect

pd.set_option("future.no_silent_downcasting", True)


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
        self, df: Union[pd.DataFrame, pl.DataFrame]
    ) -> Union[pd.DataFrame, pl.DataFrame]:
        logging.info("🔍Detecting Trade Opportunities")
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

        df["trigger_trade_entry"] = df["trigger_trade_entry"].fillna(False)

        df = df.drop(
            columns=["trigger_trade_entry_pmkt_raw", "trigger_trade_entry_mkt_raw"]
        )
        return df

    def _open_position(self, df: Union[pd.DataFrame, pl.DataFrame]):
        df["first_trade_timestamp"] = df.groupby(["ticker", "date"]).cumcount() == 0
        df["first_pmkt_trade_timestamp"] = (
            df[df["timestamp_cst"].dt.time < dtime(8, 30)]
            .groupby(["ticker", "date"])
            .cumcount()
            == 0
        )
        df["first_mkt_trade_timestamp"] = (
            df[df["timestamp_cst"].dt.time >= dtime(8, 30)]
            .groupby(["ticker", "date"])
            .cumcount()
            == 0
        )
        df["first_pmkt_bar"] = (
            df[df["timestamp_cst"].dt.time == dtime(3, 0)]
            .groupby(["ticker", "date"])
            .cumcount()
            == 0
        )

        df.loc[df["trigger_trade_entry"], "theoretical_bto_price"] = df[
            "prev_market_close"
        ] * (1 + self.overnight_gap)
        df["theoretical_sto_price"] = df["theoretical_bto_price"]

        # assume buy at high / sell at low if the gap happens at the open
        mask = df["first_trade_timestamp"] & df["trigger_trade_entry"]
        df.loc[mask, "theoretical_bto_price"] = df.loc[mask, "high"]
        df.loc[mask, "theoretical_sto_price"] = df.loc[mask, "low"]

        entry_mask = df["trigger_trade_entry"]

        df.loc[entry_mask, "bto_price_with_slippage"] = apply_slippage(
            df.loc[entry_mask],
            price_col="theoretical_bto_price",
            slippage_amount=self.slippage_amount,
            slippage_mode=self.slippage_mode,
            side="buy",
        )

        df.loc[entry_mask, "sto_price_with_slippage"] = apply_slippage(
            df.loc[entry_mask],
            price_col="theoretical_sto_price",
            slippage_amount=self.slippage_amount,
            slippage_mode=self.slippage_mode,
            side="sell",
        )

        # Forward-fill within each trade group (ticker, date)
        df["bto_price_with_slippage"] = df.groupby(["ticker", "date"])[
            "bto_price_with_slippage"
        ].ffill()
        df["sto_price_with_slippage"] = df.groupby(["ticker", "date"])[
            "sto_price_with_slippage"
        ].ffill()

        df.loc[df["trigger_trade_entry"], "shares_long_first"] = (
            self.bet_amount / df["bto_price_with_slippage"]
        )
        df.loc[df["trigger_trade_entry"], "shares_short_first"] = (
            self.bet_amount / df["sto_price_with_slippage"]
        )

        return df

    def _trigger_stop_loss(self, df: Union[pd.DataFrame, pl.DataFrame]):
        # Long stop logic --> Creates columns:: long_stop_hit, long_stop_exit_price, short_stop_hit, short_stop_exit_price
        df["long_stop_limit_price_raw"] = df["bto_price_with_slippage"] * (
            1 - self.stop_loss_pct
        )
        df["long_stop_hit_raw"] = df["low"] <= df["long_stop_limit_price_raw"]
        df["long_stop_hit"] = df.groupby(["ticker", "date"])[
            "long_stop_hit_raw"
        ].transform(first_true)

        df["long_stop_limit_price"] = df["long_stop_limit_price_raw"].where(
            df["long_stop_hit"]
        )

        df["long_stop_hit"] = df["long_stop_hit"].fillna(False)

        # Short stop logic
        df["short_stop_limit_price_raw"] = df["sto_price_with_slippage"] * (
            1 + self.stop_loss_pct
        )
        df["short_stop_hit_raw"] = df["high"] >= df["short_stop_limit_price_raw"]
        df["short_stop_hit"] = df.groupby(["ticker", "date"])[
            "short_stop_hit_raw"
        ].transform(first_true)
        df["short_stop_limit_price"] = df["short_stop_limit_price_raw"].where(
            df["short_stop_hit"]
        )

        df["short_stop_hit"] = df["short_stop_hit"].fillna(False)

        df = df.drop(
            columns=[
                "long_stop_limit_price_raw",
                "long_stop_hit_raw",
                "short_stop_limit_price_raw",
                "short_stop_hit_raw",
            ]
        )

        # for stopouts assume fixed pct worse than stop price
        df.loc[df["long_stop_hit"], "long_stop_exit_price"] = apply_slippage(
            df.loc[df["long_stop_hit"]],
            price_col="long_stop_limit_price",
            slippage_amount=0.003,
            slippage_mode="fixed_pct",
            side="sell",
        )

        df.loc[df["short_stop_hit"], "short_stop_exit_price"] = apply_slippage(
            df.loc[df["short_stop_hit"]],
            price_col="short_stop_limit_price",
            slippage_amount=0.003,
            slippage_mode="fixed_pct",
            side="buy",
        )

        return df

    def _trigger_take_profit(self, df: Union[pd.DataFrame, pl.DataFrame]):
        # Long tp logic --> Creates columns:: long_tp_hit, long_tp_exit_price, short_tp_hit, short_tp_exit_price
        df["long_tp_limit_price_raw"] = df["bto_price_with_slippage"] * (
            1 + self.take_profit_pct
        )
        df["long_tp_hit_raw"] = df["high"] >= df["long_tp_limit_price_raw"]
        df["long_tp_hit"] = df.groupby(["ticker", "date"])["long_tp_hit_raw"].transform(
            first_true
        )
        df["long_tp_hit_ffilled"] = df.groupby(["ticker", "date"])[
            "long_tp_hit"
        ].ffill()

        df["long_tp_limit_price"] = df["long_tp_limit_price_raw"].where(
            df["long_tp_hit"]
        )

        df["long_tp_hit"] = df["long_tp_hit"].fillna(False)

        # Short tp logic
        df["short_tp_limit_price_raw"] = df["sto_price_with_slippage"] * (
            1 - self.take_profit_pct
        )
        df["short_tp_hit_raw"] = df["low"] <= df["short_tp_limit_price_raw"]
        df["short_tp_hit"] = df.groupby(["ticker", "date"])[
            "short_tp_hit_raw"
        ].transform(first_true)
        df["short_tp_hit_ffilled"] = df.groupby(["ticker", "date"])[
            "short_tp_hit"
        ].ffill()
        df["short_tp_limit_price"] = df["short_tp_limit_price_raw"].where(
            df["short_tp_hit"]
        )

        df["short_tp_hit"] = df["short_tp_hit"].fillna(False)

        df = df.drop(
            columns=[
                "long_tp_limit_price_raw",
                "long_tp_hit_raw",
                "short_tp_limit_price_raw",
                "short_tp_hit_raw",
            ]
        )

        # for tp assume fixed pct worse than tp price
        df.loc[df["long_tp_hit"], "long_tp_exit_price"] = apply_slippage(
            df.loc[df["long_tp_hit"]],
            price_col="long_tp_limit_price",
            slippage_amount=0.003,
            slippage_mode="fixed_pct",
            side="sell",
        )

        df.loc[df["short_tp_hit"], "short_tp_exit_price"] = apply_slippage(
            df.loc[df["short_tp_hit"]],
            price_col="short_tp_limit_price",
            slippage_amount=0.003,
            slippage_mode="fixed_pct",
            side="buy",
        )
        return df

    def _add_exit_prices(self, df):
        df["long_exit_price"] = np.nan
        df["short_exit_price"] = np.nan
        df["long_exit_timestamp"] = pd.Series(dtype=df["timestamp_cst"].dtype)
        df["short_exit_timestamp"] = pd.Series(dtype=df["timestamp_cst"].dtype)
        df["time_in_long_trade"] = pd.Series(dtype="timedelta64[ns]")
        df["time_in_short_trade"] = pd.Series(dtype="timedelta64[ns]")

        # Create helper columns
        df["has_long_entry"] = df["shares_long_first"].notna()
        df["has_short_entry"] = df["shares_short_first"].notna()

        # Forward fill entry flags within each group
        df["long_position_active"] = df.groupby(["ticker", "date"])[
            "has_long_entry"
        ].transform("cummax")
        df["short_position_active"] = df.groupby(["ticker", "date"])[
            "has_short_entry"
        ].transform("cummax")

        # Initialize exit priority (higher number = lower priority)
        df["long_exit_priority"] = np.inf
        df["short_exit_priority"] = np.inf

        # EOD flatten (lowest priority - set first)
        eod_mask = df["timestamp_cst"].dt.time >= self.flatten_trade_time_cst

        df["is_last_bar"] = (
            df.groupby(["ticker", "date"]).cumcount(ascending=False) == 0
        )
        last_bar_exit_mask = df["is_last_bar"]
        flatten_mask = eod_mask | last_bar_exit_mask

        df.loc[flatten_mask & df["long_position_active"], "long_exit_priority"] = 3
        df.loc[flatten_mask & df["short_position_active"], "short_exit_priority"] = 3

        # Take profits (2nd priority - can overwrite EOD priority)
        df.loc[df["long_tp_hit"], "long_exit_priority"] = np.minimum(
            df.loc[df["long_tp_hit"], "long_exit_priority"], 2
        )
        df.loc[df["short_tp_hit"], "short_exit_priority"] = np.minimum(
            df.loc[df["short_tp_hit"], "short_exit_priority"], 2
        )

        # Stop losses (highest priority - can overwrite everything)
        df.loc[df["long_stop_hit"], "long_exit_priority"] = np.minimum(
            df.loc[df["long_stop_hit"], "long_exit_priority"], 1
        )
        df.loc[df["short_stop_hit"], "short_exit_priority"] = np.minimum(
            df.loc[df["short_stop_hit"], "short_exit_priority"], 1
        )

        # Get first exit per group
        df["long_exit_rank"] = df.groupby(["ticker", "date"])[
            "long_exit_priority"
        ].rank(method="first")
        df["short_exit_rank"] = df.groupby(["ticker", "date"])[
            "short_exit_priority"
        ].rank(method="first")

        # Apply exit prices based on final priority
        first_long_exit = (df["long_exit_rank"] == 1) & (
            df["long_exit_priority"] < np.inf
        )
        first_short_exit = (df["short_exit_rank"] == 1) & (
            df["short_exit_priority"] < np.inf
        )

        # Stop exits (priority 1)
        df.loc[
            first_long_exit & (df["long_exit_priority"] == 1), "long_exit_price"
        ] = df.loc[
            first_long_exit & (df["long_exit_priority"] == 1), "long_stop_exit_price"
        ]
        df.loc[
            first_short_exit & (df["short_exit_priority"] == 1), "short_exit_price"
        ] = df.loc[
            first_short_exit & (df["short_exit_priority"] == 1), "short_stop_exit_price"
        ]

        # TP exits (priority 2)
        df.loc[
            first_long_exit & (df["long_exit_priority"] == 2), "long_exit_price"
        ] = df.loc[
            first_long_exit & (df["long_exit_priority"] == 2), "long_tp_exit_price"
        ]
        df.loc[
            first_short_exit & (df["short_exit_priority"] == 2), "short_exit_price"
        ] = df.loc[
            first_short_exit & (df["short_exit_priority"] == 2), "short_tp_exit_price"
        ]

        # EOD exits (priority 3)
        eod_long_exits = first_long_exit & (df["long_exit_priority"] == 3)
        eod_short_exits = first_short_exit & (df["short_exit_priority"] == 3)

        if eod_long_exits.any():
            df.loc[eod_long_exits, "long_exit_price"] = apply_slippage(
                df.loc[eod_long_exits],
                price_col="open",
                slippage_amount=0.005,
                slippage_mode="fixed_pct",
                side="sell",
            )

        if eod_short_exits.any():
            df.loc[eod_short_exits, "short_exit_price"] = apply_slippage(
                df.loc[eod_short_exits],
                price_col="open",
                slippage_amount=0.005,
                slippage_mode="fixed_pct",
                side="buy",
            )

        # === ADD EXIT TIMESTAMPS ===

        # Set exit timestamps for all exits
        df.loc[first_long_exit, "long_exit_timestamp"] = df.loc[
            first_long_exit, "timestamp_cst"
        ]
        df.loc[first_short_exit, "short_exit_timestamp"] = df.loc[
            first_short_exit, "timestamp_cst"
        ]

        # Get entry timestamps per group
        df["long_entry_timestamp"] = df.groupby(["ticker", "date"])[
            "timestamp_cst"
        ].transform(
            lambda x: (
                x[df.loc[x.index, "has_long_entry"]].iloc[0]
                if df.loc[x.index, "has_long_entry"].any()
                else pd.NaT
            )
        )
        df["short_entry_timestamp"] = df.groupby(["ticker", "date"])[
            "timestamp_cst"
        ].transform(
            lambda x: (
                x[df.loc[x.index, "has_short_entry"]].iloc[0]
                if df.loc[x.index, "has_short_entry"].any()
                else pd.NaT
            )
        )

        # Calculate time in trade for exit rows

        df["long_entry_timestamp"] = pd.to_datetime(df["long_entry_timestamp"])
        df["short_entry_timestamp"] = pd.to_datetime(df["short_entry_timestamp"])

        df.loc[first_long_exit, "time_in_long_trade"] = (
            df.loc[first_long_exit, "long_exit_timestamp"]
            - df.loc[first_long_exit, "long_entry_timestamp"]
        )
        df.loc[first_short_exit, "time_in_short_trade"] = (
            df.loc[first_short_exit, "short_exit_timestamp"]
            - df.loc[first_short_exit, "short_entry_timestamp"]
        )

        # Clean up helper columns
        df.drop(
            columns=[
                "has_long_entry",
                "has_short_entry",
                "long_position_active",
                "short_position_active",
                "long_exit_rank",
                "short_exit_rank",
                "is_last_bar",
            ],
            inplace=True,
        )

        return df

    @staticmethod
    def _ffill_shares(df):
        """Fully vectorized share forward-filling"""
        df["long_shares_ffilled"] = np.nan
        df["short_shares_ffilled"] = np.nan

        # === LONG POSITIONS ===
        # Forward fill shares from entry point within each group
        df["long_shares_temp"] = df.groupby(["ticker", "date"])[
            "shares_long_first"
        ].ffill()

        # Create position active flag (from entry to exit)
        df["long_entry_cumsum"] = df.groupby(["ticker", "date"])[
            "shares_long_first"
        ].transform(lambda x: x.notna().cumsum())
        df["long_exit_cumsum"] = df.groupby(["ticker", "date"])[
            "long_exit_price"
        ].transform(lambda x: x.notna().cumsum())

        # Position is active from first entry until first exit
        long_position_active = (df["long_entry_cumsum"] >= 1) & (
            df["long_exit_cumsum"] == 0
        )
        # Also include the exit bar itself
        long_exit_bar = df["long_exit_price"].notna()
        long_position_window = long_position_active | long_exit_bar

        # Apply shares only during active position window
        df.loc[long_position_window, "long_shares_ffilled"] = df.loc[
            long_position_window, "long_shares_temp"
        ]

        # === SHORT POSITIONS ===
        df["short_shares_temp"] = df.groupby(["ticker", "date"])[
            "shares_short_first"
        ].ffill()

        df["short_entry_cumsum"] = df.groupby(["ticker", "date"])[
            "shares_short_first"
        ].transform(lambda x: x.notna().cumsum())
        df["short_exit_cumsum"] = df.groupby(["ticker", "date"])[
            "short_exit_price"
        ].transform(lambda x: x.notna().cumsum())

        short_position_active = (df["short_entry_cumsum"] >= 1) & (
            df["short_exit_cumsum"] == 0
        )
        short_exit_bar = df["short_exit_price"].notna()
        short_position_window = short_position_active | short_exit_bar

        df.loc[short_position_window, "short_shares_ffilled"] = df.loc[
            short_position_window, "short_shares_temp"
        ]

        # Clean up temporary columns
        df.drop(
            columns=[
                "long_shares_temp",
                "short_shares_temp",
                "long_entry_cumsum",
                "long_exit_cumsum",
                "short_entry_cumsum",
                "short_exit_cumsum",
            ],
            inplace=True,
        )

        return df

    def _add_unrealized_pnl(self, df):
        """Fully vectorized unrealized PnL calculation"""
        df = self._ffill_shares(df)

        # === LONG PnL ===
        # Get entry price for each position (forward fill within group)
        df["long_entry_price_temp"] = df.groupby(["ticker", "date"])[
            "bto_price_with_slippage"
        ].ffill()

        # Calculate PnL only where shares exist
        long_pnl_mask = df["long_shares_ffilled"].notna()
        df["unrealized_long_pnl"] = np.where(
            long_pnl_mask,
            (df["open"] - df["long_entry_price_temp"]) * df["long_shares_ffilled"],
            np.nan,
        )

        # === SHORT PnL ===
        df["short_entry_price_temp"] = df.groupby(["ticker", "date"])[
            "sto_price_with_slippage"
        ].ffill()

        short_pnl_mask = df["short_shares_ffilled"].notna()
        df["unrealized_short_pnl"] = np.where(
            short_pnl_mask,
            (df["short_entry_price_temp"] - df["open"]) * df["short_shares_ffilled"],
            np.nan,
        )

        # Clean up
        df.drop(
            columns=["long_entry_price_temp", "short_entry_price_temp"], inplace=True
        )

        return df

    @staticmethod
    def _add_realized_pnl(df):
        df.loc[:, "long_realized_pnl"] = (
            df["long_exit_price"] - df["bto_price_with_slippage"]
        ) * df["long_shares_ffilled"]
        df.loc[:, "short_realized_pnl"] = (
            df["sto_price_with_slippage"] - df["short_exit_price"]
        ) * df["short_shares_ffilled"]
        return df

    def _calc_pnl(self, df: pd.DataFrame) -> Union[pd.DataFrame, pl.DataFrame]:
        df = self._add_exit_prices(df)
        df = self._add_unrealized_pnl(df)
        df = self._add_realized_pnl(df)
        return df

    def _close_position(self, df: Union[pd.DataFrame, pl.DataFrame]):
        df = self._trigger_stop_loss(df)
        df = self._trigger_take_profit(df)
        df = self._calc_pnl(df)
        return df

    def adjust_position(
        self, df: Union[pd.DataFrame, pl.DataFrame]
    ) -> Union[pd.DataFrame, pl.DataFrame]:
        logging.info("⚖️ Adjusting Positions")
        df = self._open_position(df)
        df = self._close_position(df)
        return df


class GapStrategyRiskManager(RiskManager):
    def quantify_risk(
        self, df: Union[pd.DataFrame, pl.DataFrame]
    ) -> Union[pd.DataFrame, pl.DataFrame]:
        """Should implement more realistic risk managing, like trailing stops and/or profits."""
        return df


class GapStrategyEvaluator(StrategyEvaluator):
    def __init__(self, segment_cols=None):
        self.simplified_daily_summary = None
        self.daily_summary_by_ticker = None
        self.daily_summary_with_segments = None
        self.daily_summary_by_ticker_and_segment = None
        self.segment_cols = segment_cols

    @staticmethod
    def _add_pct_change_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds percentage change columns to the DataFrame.

        Parameters:
        df (pd.DataFrame): Input DataFrame containing price and volume data.

        Returns:
        pd.DataFrame: DataFrame with the new percentage change columns added.
        """

        df["pct_high_from_prev_mkt_close"] = (
            df["high"] - df["prev_market_close"]
        ) / df["prev_market_close"]

        df["pct_low_from_prev_mkt_close"] = (df["low"] - df["prev_market_close"]) / df[
            "prev_market_close"
        ]

        df["pct_high_from_entry_price"] = (
            df["high"]
            - (df["bto_price_with_slippage"] + df["sto_price_with_slippage"]).mean()
        ) / (df["bto_price_with_slippage"] + df["sto_price_with_slippage"]).mean()
        df["pct_low_from_entry_price"] = (
            df["low"]
            - (df["bto_price_with_slippage"] + df["sto_price_with_slippage"]).mean()
        ) / (df["bto_price_with_slippage"] + df["sto_price_with_slippage"]).mean()

        df["open_pct_chg"] = (df["open"] - df["prev_market_close"]) / df[
            "prev_market_close"
        ]
        df["high_pct_chg"] = (df["high"] - df["prev_market_close"]) / df[
            "prev_market_close"
        ]
        df["low_pct_chg"] = (df["low"] - df["prev_market_close"]) / df[
            "prev_market_close"
        ]
        df["close_pct_chg"] = (df["close"] - df["prev_market_close"]) / df[
            "prev_market_close"
        ]

        df["volume_pct_chg"] = (df["volume"] - df["prev_market_volume"]) / df["prev_market_volume"]
        return df

    @staticmethod
    def _calc_daily_summary(df, include_pct_chg_metrics=False):
        def safe_mean(series):
            return series.mean() if len(series.dropna()) > 0 else np.nan

        def safe_median(series):
            return series.median() if len(series.dropna()) > 0 else np.nan

        def safe_std(series):
            return series.std() if len(series.dropna()) > 1 else np.nan

        def safe_min(series):
            return series.min() if len(series.dropna()) > 0 else np.nan

        def safe_max(series):
            return series.max() if len(series.dropna()) > 0 else np.nan

        def safe_sum(series):
            return series.sum() if len(series.dropna()) > 0 else 0

        has_trades = df["trigger_trade_entry"].sum() > 0
        has_long_pnl = df["long_realized_pnl"].notna().sum() > 0
        has_short_pnl = df["short_realized_pnl"].notna().sum() > 0
        has_long_exits = df["long_exit_price"].notna().sum() > 0
        has_short_exits = df["short_exit_price"].notna().sum() > 0

        # Base metrics that are always included
        summary_dict = {
            # === BASIC INFO ===
            "date": df["date"].iloc[0] if len(df) > 0 else None,
            "tickers": df["ticker"].nunique(),
            "n_trades": df["trigger_trade_entry"].sum(),
            "pct_entries_pmkt": (
                (
                    df["trigger_trade_entry_pmkt"].sum()
                    / (
                        df["trigger_trade_entry_pmkt"]
                        | df["trigger_trade_entry_mkt"]
                    ).sum()
                )
                if has_trades
                else 0
            ),
            "pct_entries_mkt": (
                (
                    df["trigger_trade_entry_mkt"].sum()
                    / (
                        df["trigger_trade_entry_pmkt"]
                        | df["trigger_trade_entry_mkt"]
                    ).sum()
                )
                if has_trades
                else 0
            ),
            # === GAP ANALYSIS ===
            "avg_gap_pct_chg_on_entry": (
                safe_mean(
                    df[df["trigger_trade_entry"]]["adj_pmkt_pct_chg"]
                    .abs()
                    .fillna(df[df["trigger_trade_entry"]]["adj_mkt_pct_chg"].abs())
                )
                if has_trades
                else np.nan
            ),
            "median_gap_pct_chg_on_entry": (
                safe_median(
                    df[df["trigger_trade_entry"]]["adj_pmkt_pct_chg"]
                    .abs()
                    .fillna(df[df["trigger_trade_entry"]]["adj_mkt_pct_chg"].abs())
                )
                if has_trades
                else np.nan
            ),
            "min_gap_size_on_entries": (
                safe_min(
                    df[df["trigger_trade_entry"]]["adj_pmkt_pct_chg"]
                    .abs()
                    .fillna(df[df["trigger_trade_entry"]]["adj_mkt_pct_chg"].abs())
                )
                if has_trades
                else np.nan
            ),
            "max_gap_size_on_entries": (
                safe_max(
                    df[df["trigger_trade_entry"]]["adj_pmkt_pct_chg"]
                    .abs()
                    .fillna(df[df["trigger_trade_entry"]]["adj_mkt_pct_chg"].abs())
                )
                if has_trades
                else np.nan
            ),
            # === LONG POSITION METRICS ===
            "long_realized_pnl": safe_sum(df["long_realized_pnl"]),
            "long_win_rate": (
                calc_win_rate(df["long_realized_pnl"]) if has_long_pnl else np.nan
            ),
            "long_profit_factor": (
                calc_profit_factor(df["long_realized_pnl"].dropna())
                if has_long_pnl
                else np.nan
            ),
            "long_avg_trade_pnl": safe_mean(df["long_realized_pnl"]),
            "long_median_trade_pnl": safe_median(df["long_realized_pnl"]),
            "long_max_winner": safe_max(df["long_realized_pnl"]),
            "long_max_loser": safe_min(df["long_realized_pnl"]),
            "long_pnl_std": safe_std(df["long_realized_pnl"]),
            "long_intraday_sharpe": (
                calc_intraday_sharpe_ratio(df["long_realized_pnl"])
                if has_long_pnl
                else np.nan
            ),
            "long_intraday_sortino": (
                calc_sortino_ratio(df["long_realized_pnl"])
                if has_long_pnl
                else np.nan
            ),
            "long_intraday_max_drawdown": (
                calc_max_drawdown(df["long_realized_pnl"])
                if has_long_pnl
                else np.nan
            ),
            # === SHORT POSITION METRICS ===
            "short_realized_pnl": safe_sum(df["short_realized_pnl"]),
            "short_win_rate": (
                calc_win_rate(df["short_realized_pnl"]) if has_short_pnl else np.nan
            ),
            "short_profit_factor": (
                calc_profit_factor(df["short_realized_pnl"].dropna())
                if has_short_pnl
                else np.nan
            ),
            "short_avg_trade_pnl": safe_mean(df["short_realized_pnl"]),
            "short_median_trade_pnl": safe_median(df["short_realized_pnl"]),
            "short_max_winner": safe_max(df["short_realized_pnl"]),
            "short_max_loser": safe_min(df["short_realized_pnl"]),
            "short_pnl_std": safe_std(df["short_realized_pnl"]),
            "short_intraday_sharpe": (
                calc_intraday_sharpe_ratio(df["short_realized_pnl"])
                if has_short_pnl
                else np.nan
            ),
            "short_intraday_sortino": (
                calc_sortino_ratio(df["short_realized_pnl"])
                if has_short_pnl
                else np.nan
            ),
            "short_intraday_max_drawdown": (
                calc_max_drawdown(df["short_realized_pnl"])
                if has_short_pnl
                else np.nan
            ),
            # === ENTRY/EXIT TIMESTAMPS ===
            "avg_trade_entry_timestamp": (
                safe_mean(df[df["trigger_trade_entry"]]["timestamp_cst"])
                if has_trades
                else pd.NaT
            ),
            "median_trade_entry_timestamp": (
                safe_median(df[df["trigger_trade_entry"]]["timestamp_cst"])
                if has_trades
                else pd.NaT
            ),
            "earliest_trade_entry": (
                safe_min(df[df["trigger_trade_entry"]]["timestamp_cst"])
                if has_trades
                else pd.NaT
            ),
            "latest_trade_entry": (
                safe_max(df[df["trigger_trade_entry"]]["timestamp_cst"])
                if has_trades
                else pd.NaT
            ),
            "avg_long_exit_timestamp": (
                safe_mean(df[df["long_exit_price"].notna()]["timestamp_cst"])
                if has_long_exits
                else pd.NaT
            ),
            "avg_short_exit_timestamp": (
                safe_mean(df[df["short_exit_price"].notna()]["timestamp_cst"])
                if has_short_exits
                else pd.NaT
            ),
            "median_long_exit_timestamp": (
                safe_median(df[df["long_exit_price"].notna()]["timestamp_cst"])
                if has_long_exits
                else pd.NaT
            ),
            "median_short_exit_timestamp": (
                safe_median(df[df["short_exit_price"].notna()]["timestamp_cst"])
                if has_short_exits
                else pd.NaT
            ),
            # === TIME IN TRADE ===
            "avg_time_in_long_trade": safe_mean(df["time_in_long_trade"]),
            "avg_time_in_short_trade": safe_mean(df["time_in_short_trade"]),
            "median_time_in_long_trade": safe_median(df["time_in_long_trade"]),
            "median_time_in_short_trade": safe_median(df["time_in_short_trade"]),
            "min_time_in_long_trade": safe_min(df["time_in_long_trade"]),
            "min_time_in_short_trade": safe_min(df["time_in_short_trade"]),
            "max_time_in_long_trade": safe_max(df["time_in_long_trade"]),
            "max_time_in_short_trade": safe_max(df["time_in_short_trade"]),
            # === EXIT TYPE ANALYSIS ===
            "n_long_stop_exits": df[
                (df["long_exit_price"].notna())
                & (df.get("long_exit_priority", pd.Series()) == 1)
            ].shape[0],
            "n_long_tp_exits": df[
                (df["long_exit_price"].notna())
                & (df.get("long_exit_priority", pd.Series()) == 2)
            ].shape[0],
            "n_long_eod_exits": df[
                (df["long_exit_price"].notna())
                & (df.get("long_exit_priority", pd.Series()) == 3)
            ].shape[0],
            "n_short_stop_exits": df[
                (df["short_exit_price"].notna())
                & (df.get("short_exit_priority", pd.Series()) == 1)
            ].shape[0],
            "n_short_tp_exits": df[
                (df["short_exit_price"].notna())
                & (df.get("short_exit_priority", pd.Series()) == 2)
            ].shape[0],
            "n_short_eod_exits": df[
                (df["short_exit_price"].notna())
                & (df.get("short_exit_priority", pd.Series()) == 3)
            ].shape[0],
            # === CORPORATE ACTIONS ===
            "n_had_split": df[df["split_ratio"] != 1]["ticker"].nunique(),
            "n_had_dividend": df[df["cash_amount"] != 0]["ticker"].nunique(),
            "n_had_split_or_dividend": df[
                (df["split_ratio"] != 1) | (df["cash_amount"] != 0)
            ]["ticker"].nunique(),
            # === INTRADAY PERFORMANCE DISTRIBUTION ===
            "long_trades_profitable": (df["long_realized_pnl"] > 0).sum(),
            "long_trades_losses": (df["long_realized_pnl"] < 0).sum(),
            "short_trades_profitable": (df["short_realized_pnl"] > 0).sum(),
            "short_trades_losses": (df["short_realized_pnl"] < 0).sum(),
        }

        # Only add detailed price metrics if requested
        if include_pct_chg_metrics:
            detailed_metrics = {
                # === PERCENT CHANGE METRICS ===
                "avg_pct_high_from_prev_mkt_close": safe_mean(
                    df["pct_high_from_prev_mkt_close"]
                ),
                "median_pct_high_from_prev_mkt_close": safe_median(
                    df["pct_high_from_prev_mkt_close"]
                ),
                "avg_pct_low_from_prev_mkt_close": safe_mean(
                    df["pct_low_from_prev_mkt_close"]
                ),
                "median_pct_low_from_prev_mkt_close": safe_median(
                    df["pct_low_from_prev_mkt_close"]
                ),
                "avg_open_pct_chg": safe_mean(df["open_pct_chg"]),
                "avg_high_pct_chg": safe_mean(df["high_pct_chg"]),
                "avg_low_pct_chg": safe_mean(df["low_pct_chg"]),
                "avg_close_pct_chg": safe_mean(df["close_pct_chg"]),
            }
            summary_dict.update(detailed_metrics)

        df_summary = pd.Series(summary_dict)
        return df_summary

    def _calc_daily_summary_by_ticker(self, df):
        traded_tickers = df[df["trigger_trade_entry"]]["ticker"].unique().tolist()
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            daily_summary_by_ticker = (
                df[df["ticker"].isin(traded_tickers)]
                .groupby(["ticker", "date"], observed=True)
                .apply(lambda x: self._calc_daily_summary(x, include_pct_chg_metrics=True))
            )
            daily_summary_by_ticker = daily_summary_by_ticker.drop(
                columns=["date"]
            ).reset_index()
        return daily_summary_by_ticker

    @staticmethod
    def _add_ticker_identifiers(df):
        with PostgresConnect(database="financial_elt") as db:
            df_ticker_map = db.run_sql(
                """
                select distinct
                    composite_figi,
                    share_class_figi,
                    cik,
                    ticker,
                    active,
                    delisted_utc,
                    locale,
                    market,
                    ticker_type,
                    primary_exchange
                from
                    polygon_analytics.polygon_ticker_map
            """
            )

        rows_before_merge = df.shape[0]
        df = df.merge(df_ticker_map, how="left")
        identifier_cols = [
            "composite_figi",
            "share_class_figi",
            "cik",
            "active",
            "delisted_utc",
            "locale",
            "market",
            "ticker_type",
            "primary_exchange",
        ]

        assert (
            rows_before_merge == df.shape[0]
        ), "additional rows joined after merging tickers!"

        df[identifier_cols] = df[identifier_cols].fillna("unknown")
        return df

    @staticmethod
    def _add_vix_buckets(df):
        with PostgresConnect(database="financial_elt") as db:
            df_vix_1d = db.run_sql(
                """
                select date(timestamp) as date, open as vix_open from tap_yfinance_production.prices_1d where ticker = '^VIX'
            """
            )

        min_vix, max_vix, multiplier, epsilon = (
            df_vix_1d["vix_open"].min(),
            df_vix_1d["vix_open"].max(),
            1.333,
            0.01,
        )

        bins = sorted(
            list(
                set(
                    [max(0, min_vix - epsilon)]
                    + [
                        min_vix * (multiplier ** i)
                        for i in range(25)
                        if min_vix * (multiplier ** i) > max(0, min_vix - epsilon)
                        and min_vix * (multiplier ** i) < max_vix + epsilon
                    ]
                    + [max_vix + epsilon]
                )
            )
        )
        bins = [bins[0]] + [
            bins[i] for i in range(1, len(bins)) if bins[i] - bins[i - 1] > 0.001
        ]

        df_vix_1d["vix_bucket"] = pd.cut(
            df_vix_1d["vix_open"],
            bins=bins,
            labels=[f"{b1:.2f}-{b2:.2f}" for b1, b2 in zip(bins[:-1], bins[1:])],
            right=False,
        )
        df_vix_1d["date"] = pd.to_datetime(df_vix_1d["date"]).dt.tz_localize(
            "America/Chicago"
        )
        df = df.merge(df_vix_1d, how="left", on="date")
        return df

    @staticmethod
    def _add_volume_bucket(df):
        volume_buckets = [0, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000, np.inf]
        bucket_labels = ["Very Low", "Low", "Medium", "High", "Very High"]

        df["dollar_volume"] = df["volume"].fillna(0) * df["close"].fillna(0)

        df["volume_bucket"] = pd.cut(
            df["dollar_volume"], bins=volume_buckets, labels=bucket_labels, right=True
        )
        return df

    def _add_segments(self, df):
        df = self._add_ticker_identifiers(df)
        df = self._add_vix_buckets(df)
        df = self._add_volume_bucket(df)
        return df

    def _calc_daily_summary_by_segment(self, df):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            df_summary = (
                df.groupby(self.segment_cols, observed=True)
                .apply(lambda x: self._calc_daily_summary(x, include_pct_chg_metrics=False))
                .reset_index()
            )
        return df_summary

    def _calc_daily_summary_by_ticker_and_segment(self, df):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            df_summary = (
                df.groupby(list(set(["ticker"] + self.segment_cols)), observed=True)
                .apply(lambda x: self._calc_daily_summary(x, include_pct_chg_metrics=True))
                .reset_index()
            )
        return df_summary

    def evaluate_strategy(
        self, df: Union[pd.DataFrame, pl.DataFrame]
    ) -> Union[pd.DataFrame, pl.DataFrame]:
        logging.info("📈 Evaluating strategy...")
        rows_before = df.shape[0]
        df = self._add_pct_change_columns(df)
        df = self._add_segments(df)
        assert (
            df.shape[0] == rows_before
        ), "Mismatch in rows after adding segments in evaluation."

        tickers_traded = df[df["trigger_trade_entry"]]["ticker"].unique()
        self.simplified_daily_summary = self._calc_daily_summary(df, include_pct_chg_metrics=False)
        self.daily_summary_by_ticker = self._calc_daily_summary_by_ticker(df)

        if self.segment_cols:
            self.daily_summary_with_segments = self._calc_daily_summary_by_segment(
                df[df["ticker"].isin(tickers_traded)]
            )
            self.daily_summary_by_ticker_and_segment = self._calc_daily_summary_by_ticker_and_segment(
                df[df["ticker"].isin(tickers_traded)]
            )
        return df