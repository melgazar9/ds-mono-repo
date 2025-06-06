import os
import pandas as pd
import numpy as np

from datetime import time

from ds_core.db_connectors import PostgresConnect


def first_true(series):
    idx = series.idxmax() if series.any() else None
    result = pd.Series(False, index=series.index)
    if idx is not None:
        result.loc[idx] = True
    return result

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
                             "pre_market_open", "market_open", "prev_close", "adj_pmkt_pct_chg", "adj_mkt_pct_chg"]],
            on="ticker",
            how="left"
        )
        assert self.df_cur.shape[0] == rows_before_join, "Rows added from join --- bug in join logic!"
        return self

    def _price_gap_with_split_or_dividend(
            self, df_prev, df_cur, df_splits_dividends, abs_tol=0.02, rel_tol=0.002
    ):
        prev_close = (
            df_prev.groupby(["ticker", "date"], sort=False)
            .agg(prev_close=("close", "last"))
            .reset_index()
            .rename(columns={"date": "prev_date"})
        )
        pmkt_open = (
            df_cur[df_cur["timestamp_cst"].dt.time >= time(3, 0)]
            .groupby(["ticker", "date"], sort=False)
            .agg(pre_market_open=("open", "first"))
            .reset_index()
        )
        mkt_open = (
            df_cur[df_cur["timestamp_cst"].dt.time >= time(8, 30)]
            .groupby(["ticker", "date"], sort=False)
            .agg(market_open=("open", "first"))
            .reset_index()
        )
        df_open = pd.merge(pmkt_open, mkt_open, on=["ticker", "date"], how="left")
        prev_dates = df_prev.groupby("ticker")["date"].max().rename("prev_date").reset_index()
        df_open = df_open.merge(prev_dates, on=["ticker"], how="left").merge(prev_close, on=["ticker", "prev_date"],
                                                                             how="left")
        df_open = df_open.merge(df_splits_dividends, left_on=["ticker", "date"], right_on=["ticker", "event_date"],
                                how="left")

        df_open["split_from"] = df_open["split_from"].fillna(1.0)
        df_open["split_to"] = df_open["split_to"].fillna(1.0)
        df_open["cash_amount"] = df_open["cash_amount"].fillna(0.0)
        df_open["split_ratio"] = df_open["split_from"] / df_open["split_to"]
        df_open["expected_unchanged_adj_split_only"] = df_open["prev_close"] * df_open["split_ratio"]
        df_open["expected_unchanged_adj_split_dividend"] = df_open["prev_close"] * df_open["split_ratio"] - df_open["cash_amount"]
        df_open["had_split_or_dividend"] = df_open["event_type"].notna()

        is_split = df_open["event_type"] == "split"
        is_div = df_open["event_type"] == "dividend"

        def assign_adj_state(open_col):
            valid = ~df_open[open_col].isna() & ~df_open["prev_close"].isna() & ~df_open["expected_unchanged_adj_split_dividend"].isna()
            diff_adj = np.abs(df_open[open_col] - df_open["expected_unchanged_adj_split_dividend"])
            diff_prev = np.abs(df_open[open_col] - df_open["prev_close"])
            rel_adj = diff_adj / np.maximum(np.abs(df_open["expected_unchanged_adj_split_dividend"]), 1e-8)
            rel_prev = diff_prev / np.maximum(np.abs(df_open["prev_close"]), 1e-8)

            state = np.full(df_open.shape[0], "unknown", dtype=object)

            # SPLIT: usually adjusted, but check
            mask = valid & is_split & ((diff_adj <= abs_tol) | (rel_adj <= rel_tol))
            state[mask] = "adjusted"
            mask = valid & is_split & ((diff_prev <= abs_tol) | (rel_prev <= rel_tol)) & (state == "unknown")
            state[mask] = "unadjusted"
            # DIVIDEND: almost always unadjusted
            mask = valid & is_div & ((diff_prev <= abs_tol) | (rel_prev <= rel_tol))
            state[mask] = "unadjusted"
            # If you want to catch rare dividend adjustment, uncomment these lines:
            # mask = valid & is_div & ((diff_adj <= abs_tol) | (rel_adj <= rel_tol)) & (state == "unknown")
            # state[mask] = "adjusted"
            return state

        df_open["pre_market_open_adj_state"] = assign_adj_state("pre_market_open")
        df_open["market_open_adj_state"] = assign_adj_state("market_open")

        # Robust: prefer market open, else pre-market, else unknown
        robust = np.where(
            df_open["market_open_adj_state"] != "unknown",
            df_open["market_open_adj_state"],
            np.where(
                df_open["pre_market_open_adj_state"] != "unknown",
                df_open["pre_market_open_adj_state"],
                "unknown"
            )
        )
        df_open["robust_open_adj_state"] = robust

        # Set NaN for no-event rows in all *_adj_state columns
        mask_no_event = (df_open["split_ratio"] == 1.0) & (df_open["cash_amount"] == 0.0)
        for col in ["pre_market_open_adj_state", "market_open_adj_state", "robust_open_adj_state"]:
            df_open.loc[mask_no_event, col] = np.nan

        # Determine which baseline to use for each row
        is_div = df_open["event_type"] == "dividend"
        is_split = df_open["event_type"] == "split"
        is_div_unknown = is_div & (df_open["robust_open_adj_state"] == "unknown")
        baseline = np.where(is_div_unknown, df_open["prev_close"],
                            np.where(is_split, df_open["expected_unchanged_adj_split_only"],
                                     df_open["expected_unchanged_adj_split_dividend"]))

        df_open["adj_pmkt_pct_chg"] = (df_open["pre_market_open"] - baseline) / baseline
        df_open["adj_mkt_pct_chg"] = (df_open["market_open"] - baseline) / baseline

        column_order = [
            "ticker", "date", "pre_market_open", "market_open", "prev_close", "had_split_or_dividend",
            "event_type", "split_from", "split_to", "cash_amount", "split_ratio",
            "expected_unchanged_adj_split_dividend", "expected_unchanged_adj_split_only",
            "pre_market_open_adj_state", "market_open_adj_state", "robust_open_adj_state",
            "adj_pmkt_pct_chg", "adj_mkt_pct_chg"
        ]
        return df_open[column_order]

    def _trigger_entry_booleans(self,
                                pre_market_pct_chg_range: tuple[float, float] = (0.01, 1),
                                market_pct_chg_range: tuple[float, float] = (0.01, 1),
                                trigger_condition="or",
                                direction="both") -> pd.DataFrame:
        """
        Adds entry criteria based on overnight and pre-market percentage changes. For simulation purposes I'll test both
        long and short entries.

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
                self.df_cur["pmkt_trigger_entry"] = (self.df_cur["adj_pmkt_pct_chg"] >= pmkt_pct_chg_lower) & (self.df_cur["adj_pmkt_pct_chg"] <= pmkt_pct_chg_upper)
            elif direction == "down":
                self.df_cur["pmkt_trigger_entry"] = (self.df_cur["adj_pmkt_pct_chg"] <= -pmkt_pct_chg_lower) & (self.df_cur["adj_pmkt_pct_chg"] >= -pmkt_pct_chg_upper)
            else:  # direction == "both"
                self.df_cur["pmkt_trigger_entry"] = self.df_cur["adj_pmkt_pct_chg"].abs().between(pmkt_pct_chg_lower, pmkt_pct_chg_upper)
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

        # Compute raw triggers -- placeholder
        self.df_cur["raw_long_stop"] = self.df_cur["low"] <= self.df_cur["btc_stop_price"]
        self.df_cur["raw_short_stop"] = self.df_cur["high"] >= self.df_cur["stc_stop_price"]

        self.df_cur["long_stop_triggered"] = (
            self.df_cur.groupby(["ticker", "date"])["raw_long_stop"].transform(first_true)
        )
        self.df_cur["short_stop_triggered"] = (
            self.df_cur.groupby(["ticker", "date"])["raw_short_stop"].transform(first_true)
        )

        self.df_cur = self.df_cur.drop(columns=["raw_long_stop", "raw_short_stop"])
        return self

    def trigger_take_profit(
            self, take_profit_pct=0.01, slippage_pct=0.1, slippage_method="partway"
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
        self.df_cur["long_tp_triggered"] = (
            self.df_cur.groupby(["ticker", "date"])["high"].transform(
                lambda x: first_true(x >= self.df_cur.loc[x.index, "btc_tp_price"])
            )
        )
        self.df_cur["short_tp_triggered"] = (
            self.df_cur.groupby(["ticker", "date"])["low"].transform(
                lambda x: first_true(x <= self.df_cur.loc[x.index, "stc_tp_price"])
            )
        )

    def calc_rolling_pnl_no_stop_no_profit(self, slippage_pct=0.10, slippage_method="partway", min_slip=0.01):
        """
        Rolling PnL for long/short, with entry/exit slippage.
        """
        bto = self.df_cur["bto_price"]
        sto = self.df_cur["sto_price"]
        high = self.df_cur["high"]
        low = self.df_cur["low"]
        close = self.df_cur["close"]

        if slippage_method == "fixed_pct":
            long_entry_price_with_slippage = bto * (1 + slippage_pct)
            short_entry_price_with_slippage = sto * (1 - slippage_pct)
            long_exit_price_with_slippage = close * (1 - slippage_pct)
            short_exit_price_with_slippage = close * (1 + slippage_pct)
        elif slippage_method == "partway":
            long_entry_price_with_slippage = bto + np.maximum(high - bto, min_slip) * slippage_pct
            short_entry_price_with_slippage = sto + np.minimum(low - sto, -min_slip) * slippage_pct
            long_exit_price_with_slippage = close - np.maximum(close - low, min_slip) * slippage_pct
            short_exit_price_with_slippage = close + np.maximum(high - close, min_slip) * slippage_pct
        else:
            raise ValueError("slippage_method must be 'fixed_pct' or 'partway'")

        self.df_cur["long_pnl_no_stop"] = long_exit_price_with_slippage - long_entry_price_with_slippage
        self.df_cur["short_pnl_no_stop"] = short_entry_price_with_slippage - short_exit_price_with_slippage

    def calc_pnl_stop_only(self, slippage_pct=0.10, slippage_method="partway", min_slip=0.01):
        """
        Assigns PnL columns for stop-loss only (no take profit).
        Columns: long_pnl_stop_only, short_pnl_stop_only
        """
        bto = self.df_cur["bto_price"]
        sto = self.df_cur["sto_price"]
        high = self.df_cur["high"]
        low = self.df_cur["low"]
        btc_stop_price = self.df_cur["btc_stop_price"]
        stc_stop_price = self.df_cur["stc_stop_price"]

        # ENTRY
        if slippage_method == "fixed_pct":
            long_entry = bto * (1 + slippage_pct)
            short_entry = sto * (1 - slippage_pct)
        elif slippage_method == "partway":
            long_entry = bto + np.maximum(high - bto, min_slip) * slippage_pct
            short_entry = sto + np.minimum(low - sto, -min_slip) * slippage_pct
        else:
            raise ValueError("slippage_method must be 'fixed_pct' or 'partway'")

        # EXIT at stop
        long_exit = np.where(low <= btc_stop_price, np.minimum(btc_stop_price, low), np.nan)
        short_exit = np.where(high >= stc_stop_price, np.maximum(stc_stop_price, high), np.nan)

        self.df_cur["long_pnl_stop_only"] = long_exit - long_entry
        self.df_cur["short_pnl_stop_only"] = short_entry - short_exit

    def calc_pnl_tp_only(self, slippage_pct=0.10, slippage_method="partway", min_slip=0.01):
        """
        Assigns PnL columns for take-profit only (no stop).
        Columns: long_pnl_tp_only, short_pnl_tp_only
        """
        bto = self.df_cur["bto_price"]
        sto = self.df_cur["sto_price"]
        high = self.df_cur["high"]
        low = self.df_cur["low"]
        btc_tp_price = self.df_cur["btc_tp_price"]
        stc_tp_price = self.df_cur["stc_tp_price"]

        # ENTRY
        if slippage_method == "fixed_pct":
            long_entry = bto * (1 + slippage_pct)
            short_entry = sto * (1 - slippage_pct)
        elif slippage_method == "partway":
            long_entry = bto + np.maximum(high - bto, min_slip) * slippage_pct
            short_entry = sto + np.minimum(low - sto, -min_slip) * slippage_pct
        else:
            raise ValueError("slippage_method must be 'fixed_pct' or 'partway'")

        # EXIT at take profit
        long_exit = np.where(high >= btc_tp_price, np.maximum(btc_tp_price, high), np.nan)
        short_exit = np.where(low <= stc_tp_price, np.minimum(stc_tp_price, low), np.nan)

        self.df_cur["long_pnl_tp_only"] = long_exit - long_entry
        self.df_cur["short_pnl_tp_only"] = short_entry - short_exit

    # TODO: Fix
    def calc_pnl_with_stop_and_tp(self):
        """
        Assigns PnL for stop AND TP, whichever is hit first (stop wins if both in same bar).
        Uses the output columns of calc_pnl_stop_only and calc_pnl_tp_only.
        Columns: long_pnl_stop_or_tp, short_pnl_stop_or_tp
        """
        # Boolean columns for triggered exits
        long_stop_hit = self.df_cur["long_pnl_stop_only"].notnull()
        long_tp_hit = self.df_cur["long_pnl_tp_only"].notnull()
        short_stop_hit = self.df_cur["short_pnl_stop_only"].notnull()
        short_tp_hit = self.df_cur["short_pnl_tp_only"].notnull()

        # Conservative: stop wins if both triggered in same bar
        long_both = long_stop_hit & long_tp_hit
        short_both = short_stop_hit & short_tp_hit

        def first_trigger_idx(group, stop_hit, tp_hit, both_hit):
            # Returns index of the first relevant row in a group
            if both_hit.any():
                return group.index[both_hit.loc[group.index]].min()
            stop_idxs = group.index[stop_hit.loc[group.index]]
            tp_idxs = group.index[tp_hit.loc[group.index]]
            if len(stop_idxs) > 0 and (len(tp_idxs) == 0 or stop_idxs[0] < tp_idxs[0]):
                return stop_idxs[0]
            elif len(tp_idxs) > 0:
                return tp_idxs[0]
            else:
                return None

        # For each group, get the index of the first trigger
        long_first_trigger_idx = (
            self.df_cur.groupby(["ticker", "date"], group_keys=False)
            .apply(lambda g: first_trigger_idx(g, long_stop_hit, long_tp_hit, long_both))
        )
        short_first_trigger_idx = (
            self.df_cur.groupby(["ticker", "date"], group_keys=False)
            .apply(lambda g: first_trigger_idx(g, short_stop_hit, short_tp_hit, short_both))
        )

        # Build boolean masks aligned to self.df_cur
        long_first_exit_mask = pd.Series(False, index=self.df_cur.index)
        short_first_exit_mask = pd.Series(False, index=self.df_cur.index)
        for idx in long_first_trigger_idx.dropna().values:
            long_first_exit_mask.loc[idx] = True
        for idx in short_first_trigger_idx.dropna().values:
            short_first_exit_mask.loc[idx] = True

        self.df_cur["long_pnl_stop_or_tp"] = None
        self.df_cur["short_pnl_stop_or_tp"] = None

        self.df_cur.loc[long_first_exit_mask & long_stop_hit, "long_pnl_stop_or_tp"] = self.df_cur.loc[
            long_first_exit_mask & long_stop_hit, "long_pnl_stop_only"]
        self.df_cur.loc[long_first_exit_mask & ~long_stop_hit & long_tp_hit, "long_pnl_stop_or_tp"] = self.df_cur.loc[
            long_first_exit_mask & ~long_stop_hit & long_tp_hit, "long_pnl_tp_only"]

        self.df_cur.loc[short_first_exit_mask & short_stop_hit, "short_pnl_stop_or_tp"] = self.df_cur.loc[
            short_first_exit_mask & short_stop_hit, "short_pnl_stop_only"]
        self.df_cur.loc[short_first_exit_mask & ~short_stop_hit & short_tp_hit, "short_pnl_stop_or_tp"] = \
        self.df_cur.loc[
            short_first_exit_mask & ~short_stop_hit & short_tp_hit, "short_pnl_tp_only"]

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
            adj_pmkt_pct_chg=("adj_pmkt_pct_chg", "first"),
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

    def run_backtest(self, cur_day_file: str, prev_day_file: str):
        self.pull_and_clean_data(cur_day_file=cur_day_file, prev_day_file=prev_day_file)
        self.trigger_buy_sell(
            pre_market_pct_chg_range=(0.01, 1),
            market_pct_chg_range=(0.01, 1),
            trigger_condition="or",
            direction="both"
        )
        self.trigger_stop_loss(stop_loss_pct=.01)
        self.trigger_take_profit(take_profit_pct=0.01, slippage_pct=0.1, slippage_method="partway")
        self.calc_rolling_pnl_no_stop_no_profit()
        self.calc_rolling_pnl_with_stop(slippage_pct=0.1, slippage_method="partway")
        self.trigger_exit(close_trade_timestamp_cst=time(14, 55))
        self.summarize_strategy_daily()