import pandas as pd
import polars as pl
import numpy as np

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

        entry_mask = df["trigger_trade_entry"].fillna(False)

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
        df.loc[eod_mask & df["long_position_active"], "long_exit_priority"] = 3
        df.loc[eod_mask & df["short_position_active"], "short_exit_priority"] = 3

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
        df.loc[first_long_exit & (df["long_exit_priority"] == 1), "long_exit_price"] = (
            df.loc[
                first_long_exit & (df["long_exit_priority"] == 1),
                "long_stop_exit_price",
            ]
        )
        df.loc[
            first_short_exit & (df["short_exit_priority"] == 1), "short_exit_price"
        ] = df.loc[
            first_short_exit & (df["short_exit_priority"] == 1), "short_stop_exit_price"
        ]

        # TP exits (priority 2)
        df.loc[first_long_exit & (df["long_exit_priority"] == 2), "long_exit_price"] = (
            df.loc[
                first_long_exit & (df["long_exit_priority"] == 2), "long_tp_exit_price"
            ]
        )
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
        df = self._open_position(df)
        df = self._close_position(df)
        return df


class GapStrategyRiskManager(RiskManager):
    def quantify_risk(
        self, df: Union[pd.DataFrame, pl.DataFrame]
    ) -> Union[pd.DataFrame, pl.DataFrame]:
        return df


class GapStrategyEvaluator(StrategyEvaluator):
    def evaluate(
        self, df: Union[pd.DataFrame, pl.DataFrame]
    ) -> Union[pd.Series, pl.Series]:
        def calc_win_rate(series):
            wins = (series > 0).sum()
            total = series.notna().sum()
            return wins / total if total > 0 else 0

        def calc_profit_factor(series):
            profits = series[series > 0].sum()
            losses = abs(series[series < 0].sum())
            return profits / losses if losses > 0 else (np.inf if profits > 0 else 0)

        def calc_intraday_sharpe_ratio(
            returns: pd.Series,
            risk_free_rate: float = 0.0525,
            min_observations: int = 10,
            remove_outliers: bool = True,
            outlier_threshold: float = 3.0,
        ) -> float:
            if returns is None or len(returns) == 0:
                return np.nan

            clean_returns = returns.dropna()
            clean_returns = clean_returns[np.isfinite(clean_returns)]

            n_trades = len(clean_returns)

            if n_trades < min_observations:
                return np.nan

            if clean_returns.std() == 0:
                return 0.0 if clean_returns.mean() >= 0 else -np.inf

            if (
                remove_outliers and n_trades > 20
            ):  # Only remove outliers if sufficient data
                z_scores = np.abs(
                    (clean_returns - clean_returns.mean()) / clean_returns.std()
                )
                clean_returns = clean_returns[z_scores <= outlier_threshold]

                # Recheck after outlier removal
                if len(clean_returns) < min_observations:
                    return np.nan
                if clean_returns.std() == 0:
                    return 0.0 if clean_returns.mean() >= 0 else -np.inf

            trading_days_per_year = 252
            daily_rf_rate = risk_free_rate / trading_days_per_year

            rf_rate_per_trade = daily_rf_rate / max(n_trades, 1)

            excess_returns = clean_returns - rf_rate_per_trade
            mean_excess_return = excess_returns.mean()
            volatility = excess_returns.std(ddof=1)  # Sample std dev (N-1 denominator)

            sharpe_ratio = mean_excess_return / volatility

            if abs(sharpe_ratio) > 50:  # Suspiciously high Sharpe suggests data issues
                return np.nan

            return sharpe_ratio

        def calc_max_drawdown(pnl_series):
            cumulative = pnl_series.cumsum()
            running_max = cumulative.expanding().max()
            drawdown = cumulative - running_max
            return drawdown.min()

        df_summary = pd.Series(
            dict(
                # === BASIC INFO ===
                date=df["date"].iloc[0] if len(df) > 0 else None,
                tickers=df["ticker"].nunique(),
                n_trades=df["trigger_trade_entry"].sum(),
                pct_entries_pmkt=(
                    (
                        df["trigger_trade_entry_pmkt"].sum()
                        / (
                            df["trigger_trade_entry_pmkt"]
                            | df["trigger_trade_entry_mkt"]
                        ).sum()
                    )
                    if df["trigger_trade_entry"].sum() > 0
                    else 0
                ),
                pct_entries_mkt=(
                    (
                        df["trigger_trade_entry_mkt"].sum()
                        / (
                            df["trigger_trade_entry_pmkt"]
                            | df["trigger_trade_entry_mkt"]
                        ).sum()
                    )
                    if df["trigger_trade_entry"].sum() > 0
                    else 0
                ),
                # === LONG POSITION METRICS ===
                long_realized_pnl=df["long_realized_pnl"].sum(),
                long_win_rate=calc_win_rate(df["long_realized_pnl"]),
                long_profit_factor=calc_profit_factor(df["long_realized_pnl"].dropna()),
                long_avg_trade_pnl=df["long_realized_pnl"].mean(),
                long_median_trade_pnl=df["long_realized_pnl"].median(),
                long_max_winner=df["long_realized_pnl"].max(),
                long_max_loser=df["long_realized_pnl"].min(),
                long_pnl_std=df["long_realized_pnl"].std(),
                long_intraday_sharpe=calc_intraday_sharpe_ratio(
                    df["long_realized_pnl"]
                ),
                long_intraday_max_drawdown=calc_max_drawdown(df["long_realized_pnl"]),
                # === SHORT POSITION METRICS ===
                short_realized_pnl=df["short_realized_pnl"].sum(),
                short_win_rate=calc_win_rate(df["short_realized_pnl"]),
                short_profit_factor=calc_profit_factor(
                    df["short_realized_pnl"].dropna()
                ),
                short_avg_trade_pnl=df["short_realized_pnl"].mean(),
                short_median_trade_pnl=df["short_realized_pnl"].median(),
                short_max_winner=df["short_realized_pnl"].max(),
                short_max_loser=df["short_realized_pnl"].min(),
                short_pnl_std=df["short_realized_pnl"].std(),
                short_intraday_sharpe=calc_intraday_sharpe_ratio(
                    df["short_realized_pnl"]
                ),
                short_intraday_max_drawdown=calc_max_drawdown(df["short_realized_pnl"]),
                # === TIMING METRICS ===
                avg_trade_entry_timestamp=df[df["trigger_trade_entry"]][
                    "timestamp_cst"
                ].mean(),
                median_trade_entry_timestamp=df[df["trigger_trade_entry"]][
                    "timestamp_cst"
                ].median(),
                earliest_trade_entry=df[df["trigger_trade_entry"]][
                    "timestamp_cst"
                ].min(),
                latest_trade_entry=df[df["trigger_trade_entry"]]["timestamp_cst"].max(),
                avg_long_exit_timestamp=df[df["long_exit_price"].notnull()][
                    "timestamp_cst"
                ].mean(),
                avg_short_exit_timestamp=df[df["short_exit_price"].notnull()][
                    "timestamp_cst"
                ].mean(),
                median_long_exit_timestamp=df[df["long_exit_price"].notnull()][
                    "timestamp_cst"
                ].median(),
                median_short_exit_timestamp=df[df["short_exit_price"].notnull()][
                    "timestamp_cst"
                ].median(),
                # === TIME IN TRADE ===
                avg_time_in_long_trade=df["time_in_long_trade"].mean(),
                avg_time_in_short_trade=df["time_in_short_trade"].mean(),
                median_time_in_long_trade=df["time_in_long_trade"].median(),
                median_time_in_short_trade=df["time_in_short_trade"].median(),
                min_time_in_long_trade=df["time_in_long_trade"].min(),
                min_time_in_short_trade=df["time_in_short_trade"].min(),
                max_time_in_long_trade=df["time_in_long_trade"].max(),
                max_time_in_short_trade=df["time_in_short_trade"].max(),
                # === GAP ANALYSIS ===
                avg_pmkt_pct_chg=df[df["trigger_trade_entry"]][
                    "adj_pmkt_pct_chg"
                ].mean(),
                avg_mkt_pct_chg=df[df["trigger_trade_entry"]]["adj_mkt_pct_chg"].mean(),
                median_pmkt_pct_chg=df[df["trigger_trade_entry"]][
                    "adj_pmkt_pct_chg"
                ].median(),
                median_mkt_pct_chg=df[df["trigger_trade_entry"]][
                    "adj_mkt_pct_chg"
                ].median(),
                pmkt_gap_std=df[df["trigger_trade_entry"]]["adj_pmkt_pct_chg"].std(),
                mkt_gap_std=df[df["trigger_trade_entry"]]["adj_mkt_pct_chg"].std(),
                # Gap size on actual trades
                avg_gap_size_on_entries=(
                    df[df["trigger_trade_entry"]]["adj_pmkt_pct_chg"].abs().mean()
                    if df["trigger_trade_entry"].sum() > 0
                    else np.nan
                ),
                median_gap_size_on_entries=(
                    df[df["trigger_trade_entry"]]["adj_pmkt_pct_chg"].abs().median()
                    if df["trigger_trade_entry"].sum() > 0
                    else np.nan
                ),
                min_gap_size_on_entries=(
                    df[df["trigger_trade_entry"]]["adj_pmkt_pct_chg"].abs().min()
                    if df["trigger_trade_entry"].sum() > 0
                    else np.nan
                ),
                max_gap_size_on_entries=(
                    df[df["trigger_trade_entry"]]["adj_pmkt_pct_chg"].abs().max()
                    if df["trigger_trade_entry"].sum() > 0
                    else np.nan
                ),
                # === EXIT TYPE ANALYSIS (if columns exist) ===
                n_long_stop_exits=df[
                    (df["long_exit_price"].notna())
                    & (df.get("long_exit_priority", pd.Series()) == 1)
                ].shape[0],
                n_long_tp_exits=df[
                    (df["long_exit_price"].notna())
                    & (df.get("long_exit_priority", pd.Series()) == 2)
                ].shape[0],
                n_long_eod_exits=df[
                    (df["long_exit_price"].notna())
                    & (df.get("long_exit_priority", pd.Series()) == 3)
                ].shape[0],
                n_short_stop_exits=df[
                    (df["short_exit_price"].notna())
                    & (df.get("short_exit_priority", pd.Series()) == 1)
                ].shape[0],
                n_short_tp_exits=df[
                    (df["short_exit_price"].notna())
                    & (df.get("short_exit_priority", pd.Series()) == 2)
                ].shape[0],
                n_short_eod_exits=df[
                    (df["short_exit_price"].notna())
                    & (df.get("short_exit_priority", pd.Series()) == 3)
                ].shape[0],
                # === CORPORATE ACTIONS ===
                n_had_split=df[df["split_ratio"] != 1]["ticker"].nunique(),
                n_had_dividend=df[df["cash_amount"] != 0]["ticker"].nunique(),
                n_had_split_or_dividend=df[
                    (df["split_ratio"] != 1) | (df["cash_amount"] != 0)
                ]["ticker"].nunique(),
                # === INTRADAY PERFORMANCE DISTRIBUTION ===
                long_trades_profitable=(df["long_realized_pnl"] > 0).sum(),
                long_trades_breakeven=(df["long_realized_pnl"] == 0).sum(),
                long_trades_loss=(df["long_realized_pnl"] < 0).sum(),
                short_trades_profitable=(df["short_realized_pnl"] > 0).sum(),
                short_trades_breakeven=(df["short_realized_pnl"] == 0).sum(),
                short_trades_loss=(df["short_realized_pnl"] < 0).sum(),
            )
        )

        return df_summary
