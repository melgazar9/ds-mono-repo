import os
import pandas as pd
import numpy as np

from datetime import time

from ds_core.db_connectors import PostgresConnect


class SingleDayBacktest:
    def __init__(self):
        self.prev_day_close_cache = {}
        self.df_prev = None
        self.df_cur = None

    def pull_splits_dividends(self):
        with PostgresConnect(database="financial_elt") as db:
            self.df_splits_dividends = db.run_sql(
                """
                WITH cte_dividends AS (
                    SELECT
                        ticker,
                        ex_dividend_date,
                        SUM(cash_amount) AS cash_amount
                    FROM
                        tap_polygon_production.dividends
                    WHERE
                        UPPER(currency) = 'USD'
                    GROUP BY 1, 2
                )
                
                SELECT
                    COALESCE(s.ticker, d.ticker) AS ticker,
                    COALESCE(s.execution_date, d.ex_dividend_date) AS event_date,
                    s.split_from,
                    s.split_to,
                    d.cash_amount,
                    CASE
                        WHEN s.ticker IS NOT NULL AND d.ticker IS NOT NULL THEN 'split_dividend'
                        WHEN s.ticker IS NOT NULL THEN 'split'
                        WHEN d.ticker IS NOT NULL THEN 'dividend'
                        ELSE NULL
                    END AS event_type
                FROM
                    tap_polygon_production.splits s
                FULL JOIN
                    cte_dividends d
                ON
                    s.ticker = d.ticker AND s.execution_date = d.ex_dividend_date
                ORDER BY ticker, event_date;
                """
            )
        self.df_splits_dividends["event_date"] = pd.to_datetime(self.df_splits_dividends["event_date"]).dt.tz_localize("America/New_York")

    @staticmethod
    def _load_intraday_dataset(cur_day_file: str) -> pd.DataFrame:
        df = pd.read_csv(cur_day_file, compression="gzip", keep_default_na=False)
        df = df.rename(columns={"window_start": "timestamp"})
        df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True)
        df["timestamp_cst"] = df["timestamp_utc"].dt.tz_convert("America/New_York")
        df["date"] = df["timestamp_cst"].dt.normalize()
        df = df.sort_values(by=["timestamp_cst", "ticker"])
        return df

    def pull_and_clean_data(self, cur_day_file: str, prev_day_file: str):
        pd.options.display.max_columns = 25
        prev_day_file = os.path.expanduser(prev_day_file)
        self.df_cur = self._load_intraday_dataset(cur_day_file)
        self.df_prev = self._load_intraday_dataset(prev_day_file)
        self.pull_splits_dividends()  # creates self.df_splits_dividends
        self.df_adj_chg = self._price_gap_with_split_or_dividend(self.df_prev, self.df_cur, self.df_splits_dividends)

        rows_before_join = self.df_cur.shape[0]
        self.df_cur = self.df_cur.merge(
            self.df_adj_chg[["ticker", "had_split_or_dividend", "split_from", "split_to", "split_ratio", "cash_amount",
                             "pre_market_open", "market_open", "prev_close", "adj_mkt_pct_chg", "adj_pre_mkt_pct_chg"]],
            on="ticker",
            how="left"
        )
        assert self.df_cur.shape[0] == rows_before_join, "Rows added from join --- bug in join logic!"
        return self

    @staticmethod
    def _likely_adjusted_state(open_, prev_close, adj, pct_tolerance) -> pd.DataFrame:
        gap_to_adj = np.abs(open_ - adj)
        gap_to_raw = np.abs(open_ - prev_close)
        state = np.full(open_.shape, "", dtype="object")
        valid = (~pd.isna(open_)) & (~pd.isna(prev_close)) & (~pd.isna(adj))
        mask_both_zero = valid & (gap_to_adj == 0) & (gap_to_raw == 0)
        state[mask_both_zero] = "unknown"
        mask_gap_to_adj_zero = valid & (gap_to_adj == 0) & (gap_to_raw != 0)
        mask_gap_to_raw_zero = valid & (gap_to_raw == 0) & (gap_to_adj != 0)
        state[mask_gap_to_adj_zero] = "adjusted"
        state[mask_gap_to_raw_zero] = "unadjusted"

        mask_standard = valid & ~(mask_both_zero | mask_gap_to_adj_zero | mask_gap_to_raw_zero)
        best = np.where(gap_to_adj < gap_to_raw, gap_to_adj, gap_to_raw)
        worst = np.where(gap_to_adj < gap_to_raw, gap_to_raw, gap_to_adj)
        label = np.where(gap_to_adj < gap_to_raw, "adjusted", "unadjusted")

        # Avoid division by zero
        mask_worst_zero = mask_standard & (worst == 0)
        state[mask_worst_zero] = np.where(best[mask_worst_zero] == 0, "unknown", label[mask_worst_zero])

        mask_normal = mask_standard & (worst != 0)
        ratio = np.full(open_.shape, np.nan)
        ratio[mask_normal] = best[mask_normal] / worst[mask_normal]
        mask_label = mask_normal & (ratio <= pct_tolerance)
        mask_unknown = mask_normal & (ratio > pct_tolerance)
        state[mask_label] = label[mask_label]
        state[mask_unknown] = "unknown"
        return state

    def _price_gap_with_split_or_dividend(self, df_prev, df_cur, df_splits_dividends, pct_tolerance=0.65) -> pd.DataFrame:
        # --- Prepare previous close ---
        df_prev = df_prev.copy()
        prev_close = (
            df_prev[df_prev["timestamp_cst"].dt.time <= time(15, 0)]
            .groupby(["ticker", "date"], sort=False)
            .agg(prev_close=("close", "last"))
            .reset_index()
            .rename(columns={"date": "prev_date"})
        )

        # --- Prepare current opens (market and pre-market) ---
        df_cur = df_cur.copy()
        mkt_open = (
            df_cur[df_cur["timestamp_cst"].dt.time >= time(8, 30)]
            .groupby(["ticker", "date"], sort=False)
            .agg(market_open=("open", "first"))
            .reset_index()
        )
        pmkt_open = (
            df_cur[df_cur["timestamp_cst"].dt.time >= time(3, 0)]
            .groupby(["ticker", "date"], sort=False)
            .agg(pre_market_open=("open", "first"))
            .reset_index()
        )
        df_grouped = pd.merge(mkt_open, pmkt_open, on=["ticker", "date"], how="outer")

        # --- Merge prev_close ---
        prev_dates = df_prev.groupby("ticker")["date"].max().rename("prev_date").reset_index()
        df_grouped = df_grouped.merge(prev_dates, on=["ticker"], how="left")

        assert df_grouped[df_grouped["prev_date"] > df_grouped["date"]].shape[
                   0] == 0, "Error joining prev df to cur df, prev dates must be less than cur date!"

        df_grouped = df_grouped.merge(
            prev_close,
            on=["ticker", "prev_date"],
            how="left",
            sort=False,
        )

        # --- Merge event info ---
        df_grouped = df_grouped.merge(
            df_splits_dividends,
            left_on=["ticker", "date"],
            right_on=["ticker", "event_date"],
            how="left",
        )
        df_grouped["had_split_or_dividend"] = df_grouped["event_type"].notnull()
        df_grouped["split_from"] = df_grouped["split_from"].fillna(1.0)
        df_grouped["split_to"] = df_grouped["split_to"].fillna(1.0)
        df_grouped["cash_amount"] = df_grouped["cash_amount"].fillna(0.0)
        df_grouped["split_ratio"] = df_grouped["split_from"] / df_grouped["split_to"]

        # --- Calculate expected df_grouped ---
        df_grouped["expected_unchanged_open_split"] = df_grouped["prev_close"] * df_grouped["split_ratio"]
        df_grouped["expected_unchanged_open_split_div"] = df_grouped["expected_unchanged_open_split"] - df_grouped[
            "cash_amount"]

        # --- Calculate percent changes ---
        df_grouped["raw_mkt_pct_chg"] = (df_grouped["market_open"] - df_grouped["prev_close"]) / df_grouped[
            "prev_close"]
        df_grouped["raw_pre_mkt_pct_chg"] = (df_grouped["pre_market_open"] - df_grouped["prev_close"]) / df_grouped[
            "prev_close"]

        # placeholder
        df_grouped["adj_mkt_pct_chg"] = df_grouped["raw_mkt_pct_chg"]
        df_grouped["adj_pre_mkt_pct_chg"] = df_grouped["raw_pre_mkt_pct_chg"]

        mask = df_grouped["had_split_or_dividend"]

        # For splits denom = expected_unchanged_open_split, for dividends denom = expected_unchanged_open_split_div
        adj_mkt_denom = np.where(
            df_grouped["event_type"] == "split",
            df_grouped["expected_unchanged_open_split"],
            df_grouped["expected_unchanged_open_split_div"],
        )
        adj_pre_mkt_denom = adj_mkt_denom.copy()

        df_grouped.loc[mask, "adj_mkt_pct_chg"] = (
                (df_grouped.loc[mask, "market_open"].values - adj_mkt_denom[mask]) / adj_mkt_denom[mask]
        )
        df_grouped.loc[mask, "adj_pre_mkt_pct_chg"] = (
                (df_grouped.loc[mask, "pre_market_open"].values - adj_pre_mkt_denom[mask]) / adj_pre_mkt_denom[mask]
        )

        # --- _likely_adjusted_state for both market and pre-market ---
        adj_for_mkt_state = np.where(
            df_grouped["event_type"] == "split",
            df_grouped["expected_unchanged_open_split"],
            df_grouped["expected_unchanged_open_split_div"],
        )
        adj_for_pre_mkt_state = adj_for_mkt_state.copy()

        df_grouped["mkt_likely_adjusted_state"] = self._likely_adjusted_state(
            df_grouped["market_open"].values,
            df_grouped["prev_close"].values,
            adj_for_mkt_state,
            pct_tolerance,
        )
        df_grouped["pre_mkt_likely_adjusted_state"] = self._likely_adjusted_state(
            df_grouped["pre_market_open"].values,
            df_grouped["prev_close"].values,
            adj_for_pre_mkt_state,
            pct_tolerance,
        )
        df_grouped.loc[~mask, "mkt_likely_adjusted_state"] = None
        df_grouped.loc[~mask, "pre_mkt_likely_adjusted_state"] = None

        # --- Organize columns for output ---
        column_order = [
            "ticker",
            "date",
            "event_date",
            "market_open",
            "pre_market_open",
            "prev_date",
            "prev_close",
            "raw_mkt_pct_chg",
            "raw_pre_mkt_pct_chg",
            "adj_mkt_pct_chg",
            "adj_pre_mkt_pct_chg",
            "had_split_or_dividend",
            "event_type",
            "split_from",
            "split_to",
            "cash_amount",
            "split_ratio",
            "expected_unchanged_open_split",
            "expected_unchanged_open_split_div",
            "mkt_likely_adjusted_state",
            "pre_mkt_likely_adjusted_state",
        ]
        return df_grouped[column_order]

    def _trigger_entry_booleans(self,
                                pre_market_pct_chg_range: tuple[float, float] = (0.01, 1),
                                market_pct_chg_range: tuple[float, float] = (0.01, 1),
                                trigger_condition="or",
                                direction="both") -> pd.DataFrame:
        """
        Adds entry criteria based on overnight and pre-market percentage changes.

        :param pre_market_pct_chg_range: Minimum range pre-market percentage change to trigger entry. The first value is the
        lower bound and the second value is the upper bound.

        :param market_pct_chg_range: Minimum range market percentage change to trigger entry. The first value is the
        lower bound and the second value is the upper bound.

        :param trigger_condition: Condition to apply for filtering pre-market and market percentage changes. If set to "or"
        then either condition can trigger the entry, if set to "and" then both conditions must be met. Additionally, you can
        set it to "pre_market_only" or "market_only" to only consider one of the conditions.

        :param direction: Direction of the trade, can be "up", "down", or "both". If set to "up", it will only consider
        to enter the position if the market moves up by the specified percentage change, if set to "down" it will
        only consider to enter the position if the market moves down by the specified percentage change, and if set to
        "both" it will consider both directions.
        """

        if isinstance(pre_market_pct_chg_range, tuple):
            assert len(pre_market_pct_chg_range) == 2, "Pre-market percentage change range must be a tuple of two floats."
            pmkt_pct_chg_lower, pmkt_pct_chg_upper = pre_market_pct_chg_range[0], pre_market_pct_chg_range[1]
            if direction == "up":
                self.df_cur["pmkt_trigger_entry"] = (self.df_cur["adj_pre_mkt_pct_chg"] >= pmkt_pct_chg_lower) & (self.df_cur["adj_pre_mkt_pct_chg"] <= pmkt_pct_chg_upper)
            elif direction == "down":
                self.df_cur["pmkt_trigger_entry"] = (self.df_cur["adj_pre_mkt_pct_chg"] <= -pmkt_pct_chg_lower) & (self.df_cur["adj_pre_mkt_pct_chg"] >= -pmkt_pct_chg_upper)
            else:  # direction == "both"
                self.df_cur["pmkt_trigger_entry"] = self.df_cur["adj_pre_mkt_pct_chg"].abs().between(pmkt_pct_chg_lower, pmkt_pct_chg_upper)
        else:
            self.df_cur["pmkt_trigger_entry"] = False

        if isinstance(market_pct_chg_range, tuple):
            assert len(market_pct_chg_range) == 2, "Market percentage change range must be a tuple of two floats."
            mkt_pct_chg_lower, mkt_pct_chg_upper = market_pct_chg_range[0], market_pct_chg_range[1]
            if direction == "up":
                self.df_cur["mkt_trigger_entry"] = (self.df_cur["adj_mkt_pct_chg"] >= mkt_pct_chg_lower) & (self.df_cur["adj_mkt_pct_chg"] <= mkt_pct_chg_upper)
            elif direction == "down":
                self.df_cur["mkt_trigger_entry"] = (self.df_cur["adj_mkt_pct_chg"] <= -mkt_pct_chg_lower) & (self.df_cur["adj_mkt_pct_chg"] >= -mkt_pct_chg_upper)
            else:  # direction == "both"
                self.df_cur["mkt_trigger_entry"] = self.df_cur["adj_mkt_pct_chg"].abs().between(mkt_pct_chg_lower, mkt_pct_chg_upper)
        else:
            self.df_cur["mkt_trigger_entry"] = False

        if trigger_condition == "and":
            assert isinstance(pre_market_pct_chg_range, tuple) and isinstance(market_pct_chg_range, tuple),\
                "Both market and pre-market percentage change must be provided when using 'and' condition."
            self.df_cur["trigger_trade_entry"] = (
                self.df_cur["pmkt_trigger_entry"] & self.df_cur["mkt_trigger_entry"]
            )
        elif trigger_condition == "or":
            assert isinstance(pre_market_pct_chg_range, tuple) or isinstance(market_pct_chg_range, tuple),\
                "At least one of market or pre-market percentage change must be provided when using 'or' condition."
            self.df_cur["trigger_trade_entry"] = (
                self.df_cur["pmkt_trigger_entry"] | self.df_cur["mkt_trigger_entry"]
            )
        elif trigger_condition == "pre_market_only":
            assert isinstance(pre_market_pct_chg_range, tuple),\
                "Pre-market percentage change must be provided for 'pre_market_only' condition."
            self.df_cur["trigger_trade_entry"] = self.df_cur["pmkt_trigger_entry"]
        elif trigger_condition == "market_only":
            assert isinstance(market_pct_chg_range, tuple),\
                "Market percentage change must be provided for 'market_only' condition."
            self.df_cur["trigger_trade_entry"] = self.df_cur["mkt_trigger_entry"]
        return self.df_cur

    def trigger_buy_sell(self,
                         pre_market_pct_chg_range: tuple[float, float] = (0.01, 1),
                         market_pct_chg_range: tuple[float, float] = (0.01, 1),
                         trigger_condition="or",
                         direction="both"):
        """
        Adds buy and sell signals based on the entry criteria.

        This method calls entry criteria have already been set using the `trigger_entry` method.
        It will create a 'buy' signal when the entry criteria are met and a 'sell' signal when the position is closed.
        For simplicity regarding slippage (can optimize later), assume that buy orders occur at the <1-minute> high
        and sells occur at the <1-minute> low.
        """
        # bto = buy-to-open, sto = sell-to-open ---> need to group by ticker and date and get the first occurrence
        # where `trigger_trade_entry` is True.
        self._trigger_entry_booleans(
            pre_market_pct_chg_range=pre_market_pct_chg_range,
            market_pct_chg_range=market_pct_chg_range,
            trigger_condition=trigger_condition,
            direction=direction
        )
        trigger_rows = self.df_cur[self.df_cur["trigger_trade_entry"]]
        first_high = (
            trigger_rows
            .groupby(["ticker", "date"])["high"]
            .first()
            .rename("bto_price")
        )
        self.df_cur = self.df_cur.merge(
            first_high, on=["ticker", "date"], how="left"
        )

        first_low = (
            trigger_rows
            .groupby(["ticker", "date"])["low"]
            .first()
            .rename("sto_price")
        )
        self.df_cur = self.df_cur.merge(
            first_low, on=["ticker", "date"], how="left"
        )
        return self

    def trigger_stop_loss(self, stop_loss_pct: float = .01):
        """
        Adds stop-loss criteria based on a percentage of the entry price,
        but only triggers at the first occurrence per (ticker, date) sorted by timestamp.
        """
        assert isinstance(stop_loss_pct, float), "Stop-loss percentage must be a percentage (float)."
        self.df_cur["btc_stop_price"] = self.df_cur["bto_price"] * (1 - stop_loss_pct)
        self.df_cur["stc_stop_price"] = self.df_cur["sto_price"] * (1 + stop_loss_pct)

        # Compute raw triggers
        self.df_cur["raw_long_stop"] = self.df_cur["low"] <= self.df_cur["btc_stop_price"]
        self.df_cur["raw_short_stop"] = self.df_cur["high"] >= self.df_cur["stc_stop_price"]

        # Only keep the first True per group, set the rest to False
        def first_true(series):
            idx = series.idxmax() if series.any() else None
            result = pd.Series(False, index=series.index)
            if idx is not None:
                result.loc[idx] = True
            return result

        self.df_cur["long_stop_triggered"] = (
            self.df_cur.groupby(["ticker", "date"])["raw_long_stop"].transform(first_true)
        )
        self.df_cur["short_stop_triggered"] = (
            self.df_cur.groupby(["ticker", "date"])["raw_short_stop"].transform(first_true)
        )

        self.df_cur = self.df_cur.drop(columns=["raw_long_stop", "raw_short_stop"])
        return self

    def trigger_take_profit(
            self, take_profit_pct=0.01, slippage_pct=0.01, slippage_method="partway"
    ):
        """
        For each (ticker, date), computes rolling PnL for long and short until their respective first take-profit trigger.
        After take-profit, PnL is fixed at take-profit value for the rest of the day for that direction only.

        For slippage, two supported methods:
            - "fixed_pct": Take-profit fill at tp price ± slippage_pct
            - "partway":   Take-profit fill at tp price ± (overrun * slippage_pct)

        Adds columns:
            - long_pnl_with_tp_only
            - short_pnl_with_tp_only
            - long_tp_price_with_slippage
            - short_tp_price_with_slippage

        Params
        ------
        :take_profit_pct: Percentage (float, e.g. 0.05 for 5%) for take-profit.
        :slippage_pct: Percentage (float, e.g. 0.01 for 1%) for slippage.
        :slippage_method: "partway" (default) or "fixed_pct"
        """

        assert isinstance(take_profit_pct, float) and 0 < take_profit_pct < 1, \
            "Take-profit percentage must be a float between 0 and 1."
        assert isinstance(slippage_pct, float) and 0 < slippage_pct < 1, \
            "Slippage percentage must be a float between 0 and 1."
        assert slippage_method in ("partway", "fixed_pct"), \
            "slippage_method must be 'partway' or 'fixed_pct'"

        self.df_cur["long_pnl_with_tp_only"] = np.nan
        self.df_cur["short_pnl_with_tp_only"] = np.nan
        self.df_cur["long_tp_price_with_slippage"] = np.nan
        self.df_cur["short_tp_price_with_slippage"] = np.nan

        # Calculate take profit prices and triggers
        self.df_cur["btc_tp_price"] = self.df_cur["bto_price"] * (1 + take_profit_pct)
        self.df_cur["stc_tp_price"] = self.df_cur["sto_price"] * (1 - take_profit_pct)
        self.df_cur["long_tp_triggered"] = self.df_cur["high"] >= self.df_cur["btc_tp_price"]
        self.df_cur["short_tp_triggered"] = self.df_cur["low"] <= self.df_cur["stc_tp_price"]

        def fill_rolling_tp(group):
            # Long TP logic
            entry_long = group["bto_price"].iloc[0]
            rolling_long = group["high"] - entry_long
            long_tp_slip = np.nan * np.ones(len(group))
            long_tp_mask = group["long_tp_triggered"].values
            if long_tp_mask.any():
                tp_idx = long_tp_mask.argmax()
                tp_price = group["btc_tp_price"].iloc[tp_idx]
                bar_high = group["high"].iloc[tp_idx]
                if slippage_method == "fixed_pct":
                    fill_price = tp_price * (1 + slippage_pct)
                elif slippage_method == "partway":
                    fill_price = tp_price + (bar_high - tp_price) * slippage_pct
                pnl_tp = fill_price - entry_long
                rolling_long.iloc[:tp_idx] = group["high"].iloc[:tp_idx] - entry_long
                rolling_long.iloc[tp_idx:] = pnl_tp
                long_tp_slip[tp_idx] = fill_price
            group["long_pnl_with_tp_only"] = rolling_long
            group["long_tp_price_with_slippage"] = long_tp_slip

            # Short TP logic
            entry_short = group["sto_price"].iloc[0]
            rolling_short = entry_short - group["low"]
            short_tp_slip = np.nan * np.ones(len(group))
            short_tp_mask = group["short_tp_triggered"].values
            if short_tp_mask.any():
                tp_idx = short_tp_mask.argmax()
                tp_price = group["stc_tp_price"].iloc[tp_idx]
                bar_low = group["low"].iloc[tp_idx]
                if slippage_method == "fixed_pct":
                    fill_price = tp_price * (1 - slippage_pct)
                elif slippage_method == "partway":
                    fill_price = tp_price + (bar_low - tp_price) * slippage_pct
                pnl_tp = entry_short - fill_price
                rolling_short.iloc[:tp_idx] = entry_short - group["low"].iloc[:tp_idx]
                rolling_short.iloc[tp_idx:] = pnl_tp
                short_tp_slip[tp_idx] = fill_price
            group["short_pnl_with_tp_only"] = rolling_short
            group["short_tp_price_with_slippage"] = short_tp_slip
            return group

        self.df_cur = (
            self.df_cur.groupby(["ticker", "date"], group_keys=False)
            .apply(fill_rolling_tp)
            .reset_index(drop=True)
        )

    def calc_rolling_pnl_no_stop_no_profit(self):
        self.df_cur["long_pnl_no_stop"] = self.df_cur["low"] - self.df_cur["bto_price"]
        self.df_cur["short_pnl_no_stop"] = self.df_cur["sto_price"] - self.df_cur["high"]

    def calc_rolling_pnl_with_stop(self, slippage_pct=0.05, slippage_method="partway"):
        """
        For each (ticker, date), computes rolling PnL for long and short until their respective first stop-out.
        After stop-out, PnL is fixed at stop value for the rest of the day for that direction only.

        For slippage, two supported methods:
            - "fixed_pct": Stop fill at stop price ± slippage_pct
            - "partway":   Stop fill at stop price ± (overrun * slippage_pct)

        Params
        ------
        :slippage_pct: Percentage (as a float, e.g. 0.01 for 1%) to account for slippage.
        :slippage_method: "partway" (default) or "fixed_pct"
        """

        self.df_cur["long_pnl_with_stop"] = np.nan
        self.df_cur["short_pnl_with_stop"] = np.nan

        self.df_cur["long_stop_price_with_slippage"] = np.nan
        self.df_cur["short_stop_price_with_slippage"] = np.nan

        assert isinstance(slippage_pct, float) and 0 < slippage_pct < 1, "Slippage percentage must be a float between 0 and 1."
        assert slippage_method in ("partway", "fixed_pct"), "slippage_method must be 'partway' or 'fixed_pct'"

        def fill_rolling_pnl(group):
            # Long logic
            entry_long = group["bto_price"].iloc[0]
            rolling_long = group["low"] - entry_long
            long_stop_slip = np.nan * np.ones(len(group))
            long_stop_mask = group["long_stop_triggered"].values
            if long_stop_mask.any():
                stop_idx = long_stop_mask.argmax()
                stop_price = group["btc_stop_price"].iloc[stop_idx]
                bar_low = group["low"].iloc[stop_idx]
                if slippage_method == "fixed_pct":
                    fill_price = stop_price * (1 - slippage_pct)
                elif slippage_method == "partway":
                    fill_price = stop_price + (bar_low - stop_price) * slippage_pct
                pnl_stop = fill_price - entry_long
                rolling_long.iloc[:stop_idx] = group["low"].iloc[:stop_idx] - entry_long
                rolling_long.iloc[stop_idx:] = pnl_stop
                long_stop_slip[stop_idx] = fill_price
            group["long_pnl_with_stop"] = rolling_long
            group["long_stop_price_with_slippage"] = long_stop_slip

            # Short logic
            entry_short = group["sto_price"].iloc[0]
            rolling_short = entry_short - group["high"]
            short_stop_slip = np.nan * np.ones(len(group))
            short_stop_mask = group["short_stop_triggered"].values
            if short_stop_mask.any():
                stop_idx = short_stop_mask.argmax()
                stop_price = group["stc_stop_price"].iloc[stop_idx]
                bar_high = group["high"].iloc[stop_idx]
                if slippage_method == "fixed_pct":
                    fill_price = stop_price * (1 + slippage_pct)
                elif slippage_method == "partway":
                    fill_price = stop_price + (bar_high - stop_price) * slippage_pct
                pnl_stop = entry_short - fill_price
                rolling_short.iloc[:stop_idx] = entry_short - group["high"].iloc[:stop_idx]
                rolling_short.iloc[stop_idx:] = pnl_stop
                short_stop_slip[stop_idx] = fill_price
            group["short_pnl_with_stop"] = rolling_short
            group["short_stop_price_with_slippage"] = short_stop_slip
            return group

        self.df_cur = (
            self.df_cur.groupby(["ticker", "date"], group_keys=False)
            .apply(fill_rolling_pnl)
            .reset_index(drop=True)
        )

    def calc_rolling_pnl_with_stop_or_tp(self):
        """
        For each (ticker, date), computes rolling PnL for long and short until the first stop, TP, or close.
        After exit, PnL is fixed at the slippage price for the rest of the day.
        """

        def flatten_pnl(group):
            entry_long = group["bto_price"].iloc[0]
            entry_short = group["sto_price"].iloc[0]

            # Find trigger indices
            n = len(group)
            stop_idx = group["long_stop_triggered"].values.argmax() if group["long_stop_triggered"].any() else n
            tp_idx = group["long_tp_triggered"].values.argmax() if group["long_tp_triggered"].any() else n
            close_idx = group["close_timestamp_triggered"].values.argmax() if group[
                "close_timestamp_triggered"].any() else n
            exit_idx = min(stop_idx, tp_idx, close_idx)

            # Pick correct slippage price for exit
            if exit_idx == stop_idx and stop_idx < n:
                fill_price_long = group["long_stop_price_with_slippage"].iloc[stop_idx]
            elif exit_idx == tp_idx and tp_idx < n:
                fill_price_long = group["long_tp_price_with_slippage"].iloc[tp_idx]
            elif exit_idx == close_idx and close_idx < n:
                fill_price_long = group["close"].iloc[close_idx]
            else:
                fill_price_long = np.nan

            rolling_long = np.full(n, np.nan)
            if exit_idx < n:
                # Mark-to-market until exit, then flatten at fill price
                rolling_long[:exit_idx] = group["low"].iloc[:exit_idx] - entry_long
                rolling_long[exit_idx:] = fill_price_long - entry_long
            else:
                rolling_long[:] = group["low"] - entry_long

            # Short direction
            stop_idx = group["short_stop_triggered"].values.argmax() if group["short_stop_triggered"].any() else n
            tp_idx = group["short_tp_triggered"].values.argmax() if group["short_tp_triggered"].any() else n
            close_idx = group["close_timestamp_triggered"].values.argmax() if group[
                "close_timestamp_triggered"].any() else n
            exit_idx = min(stop_idx, tp_idx, close_idx)

            if exit_idx == stop_idx and stop_idx < n:
                fill_price_short = group["short_stop_price_with_slippage"].iloc[stop_idx]
            elif exit_idx == tp_idx and tp_idx < n:
                fill_price_short = group["short_tp_price_with_slippage"].iloc[tp_idx]
            elif exit_idx == close_idx and close_idx < n:
                fill_price_short = group["close"].iloc[close_idx]
            else:
                fill_price_short = np.nan

            rolling_short = np.full(n, np.nan)
            if exit_idx < n:
                rolling_short[:exit_idx] = entry_short - group["high"].iloc[:exit_idx]
                rolling_short[exit_idx:] = entry_short - fill_price_short
            else:
                rolling_short[:] = entry_short - group["high"]

            group["long_pnl_with_stop_or_tp"] = rolling_long
            group["short_pnl_with_stop_or_tp"] = rolling_short
            return group

        self.df_cur = (
            self.df_cur.groupby(["ticker", "date"], group_keys=False)
            .apply(flatten_pnl)
            .reset_index(drop=True)
        )

    def trigger_exit(self, close_trade_timestamp_cst=time(14, 55)):
        """
        Adds exit criteria based on the stop-loss or take-profit triggers.
        It will create a 'sell' signal when the stop-loss or take-profit is triggered.
        """
        assert isinstance(close_trade_timestamp_cst, time), "close_trade_timestamp_cst must be a datetime.time object."
        self.df_cur["close_timestamp_triggered"] = self.df_cur["timestamp_cst"].dt.time >= close_trade_timestamp_cst
        self.df_cur["exit_long"] = self.df_cur["long_stop_triggered"] | self.df_cur["long_tp_triggered"] | self.df_cur["close_timestamp_triggered"]
        self.df_cur["exit_short"] = self.df_cur["short_stop_triggered"] | self.df_cur["short_tp_triggered"] | self.df_cur["close_timestamp_triggered"]

        self.df_cur["exit_long_timestamp"] = np.where(self.df_cur["exit_long"], self.df_cur["timestamp_cst"], pd.NaT)
        self.df_cur["exit_short_timestamp"] = np.where(self.df_cur["exit_short"], self.df_cur["timestamp_cst"], pd.NaT)
        return self

    def summarize_strategy_daily(self):
        """
        Summary DataFrame with one row per (ticker, date), with separate columns for long/short metrics.
        Only shows a direction if a real trade was entered for that direction (never fake zeros/timestamps).
        Silences ALL groupby-apply deprecation warnings (uses include_group=False).
        """
        df = self.df_cur.copy()
        self.summary = df.groupby(["ticker", "date"]).agg(
            total_volume=("volume", "sum"),
            had_split=("split_ratio", lambda x: (x != 1).any()),
            had_dividend=("cash_amount", lambda x: (x != 0).any()),
            adj_pre_mkt_pct_chg=("adj_pre_mkt_pct_chg", "first"),
            adj_mkt_pct_chg=("adj_mkt_pct_chg", "first"),
            entered_long=("bto_price", lambda x: max(x.notnull())),
            entered_short=("sto_price", lambda x: max(x.notnull())),
            long_stop_triggered=("long_stop_triggered", "max"),
            short_stop_triggered=("short_stop_triggered", "max"),
            long_pnl_no_stop=("long_pnl_no_stop", "last"),
            short_pnl_no_stop=("short_pnl_no_stop", "last"),
            # long_pnl_with_tp_only=("long_pnl_with_tp_only", "last"),
            # short_pnl_with_tp_only=("short_pnl_with_tp_only", "last"),
            long_pnl_with_stop=("long_pnl_with_stop", "last"),
            short_pnl_with_stop=("short_pnl_with_stop", "last"),
            exit_long_timestamp=("exit_long_timestamp", "last"),
            exit_short_timestamp=("exit_short_timestamp", "last"),
        )
        return self

    def integrate_segments(self):
        """
        Method to integrate segments to find optimal subgroups of the strategy (e.g. short_volume, short_interest, market_cap, etc.)
        """
        pass