import logging
from datetime import time as dtime
import numpy as np
import pandas as pd
import polars as pl
from ds_core.db_connectors import PostgresConnect
from bt_engine.engine import DataLoader


class PolygonBarLoader(DataLoader):
    def __init__(self, load_method="pandas"):
        self.load_method = load_method
        if self.load_method not in ["pandas", "polars"]:
            raise NotImplementedError(
                f"Unknown load method {self.load_method}. Must be 'pandas' or 'polars'."
            )

    def pull_splits_dividends(self):
        if hasattr(self, "df_splits_dividends"):
            logging.debug("df_splits_dividends is already loaded, no need to re-pull.")
        else:
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
                    """,
                    df_type=self.load_method,
                )

            if self.load_method == "pandas":
                self.df_splits_dividends["event_date"] = pd.to_datetime(
                    self.df_splits_dividends["event_date"]
                ).dt.tz_localize("America/New_York")
            elif self.load_method == "polars":
                self.df_splits_dividends = self.df_splits_dividends.with_columns(
                    [pl.col("event_date").dt.convert_time_zone("America/New_York")]
                )
        return self

    def load_raw_intraday_bars(self, cur_day_file: str) -> [pd.DataFrame, pl.DataFrame]:
        """Loads bars data (e.g. 1-minute) from polygon flat files."""
        if self.load_method == "pandas":
            df = pd.read_csv(cur_day_file, compression="gzip", keep_default_na=False)
            df = df.rename(columns={"window_start": "timestamp"})
            df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True)
            df["timestamp_cst"] = df["timestamp_utc"].dt.tz_convert("America/New_York")
            df["date"] = df["timestamp_cst"].dt.normalize()
            df = df.sort_values(by=["timestamp", "ticker"])
        elif self.load_method == "polars":
            df = pl.read_csv(cur_day_file)
            df = df.rename({"window_start": "timestamp"})
            df = df.with_columns(
                [
                    pl.from_epoch("timestamp", time_unit="ns")
                    .dt.replace_time_zone("UTC")
                    .alias("timestamp_utc")
                ]
            )
            df = df.with_columns(
                [
                    pl.col("timestamp_utc")
                    .dt.convert_time_zone("America/New_York")
                    .alias("timestamp_cst")
                ]
            )
            df = df.with_columns([pl.col("timestamp_cst").dt.date().alias("date")])
            df = df.sort(["timestamp", "ticker"])
        return df

    def load_and_clean_data(self, cur_day_file: str) -> [pd.DataFrame, pl.DataFrame]:
        """Assumes self.df_prev is attached to the class"""
        self.df = self.load_raw_intraday_bars(cur_day_file)
        self.pull_splits_dividends()  # does nothing if class already has attribute self.df_splits_dividends

        if (
            self.load_method == "polars"
        ):  # TODO: integrate polars functionality across methods
            logging.debug(
                "Temporarily converting self.df, self.df_prev, and self.df_splits_dividends to pandas."
            )
            self.df = self.df.to_pandas()
            self.df_prev = self.df_prev.to_pandas()
            self.df_splits_dividends = self.df_splits_dividends.to_pandas()
            logging.debug(
                "self.df, self.df_prev, and self.df_splits_dividends are now pandas dfs."
            )

        # pandas syntax
        self.df_adj_chg = self._price_gap_with_split_or_dividend(
            self.df_prev, self.df, self.df_splits_dividends
        )

        rows_before_join = self.df.shape[0]

        self.df = self.df.merge(
            self.df_adj_chg[
                [
                    "ticker",
                    "had_split_or_dividend",
                    "split_from",
                    "split_to",
                    "split_ratio",
                    "cash_amount",
                    "pre_market_open",
                    "market_open",
                    "prev_close",
                    "adj_pmkt_pct_chg",
                    "adj_mkt_pct_chg",
                ]
            ],
            on="ticker",
            how="left",
        )
        assert (
            self.df.shape[0] == rows_before_join
        ), "Rows added from join --- bug in join logic!"

        if self.load_method == "polars":
            self.df = pl.from_pandas(self.df)
            self.df_prev = pl.from_pandas(self.df_prev)
            logging.debug("Converted self.df and self.df_prev back to polars.")
        return self

    def _price_gap_with_split_or_dividend(
        self,
        df_prev: pd.DataFrame,
        df_cur: pd.DataFrame,
        df_splits_dividends: pd.DataFrame,
        abs_tol=0.02,
        rel_tol=0.002,
    ):
        if self.load_method == "polars":
            logging.debug(
                "load_method polars not implemented yet for detecting splits/dividends price gap. Data will be"
                "converted to a pandas df and converted back to polars for this step."
            )

        prev_close = (
            df_prev.groupby(["ticker", "date"], sort=False)
            .agg(prev_close=("close", "last"))
            .reset_index()
            .rename(columns={"date": "prev_date"})
        )
        pmkt_open = (
            df_cur[df_cur["timestamp_cst"].dt.time >= dtime(3, 0)]
            .groupby(["ticker", "date"], sort=False)
            .agg(pre_market_open=("open", "first"))
            .reset_index()
        )
        mkt_open = (
            df_cur[df_cur["timestamp_cst"].dt.time >= dtime(8, 30)]
            .groupby(["ticker", "date"], sort=False)
            .agg(market_open=("open", "first"))
            .reset_index()
        )
        df_open = pd.merge(pmkt_open, mkt_open, on=["ticker", "date"], how="left")
        prev_dates = (
            df_prev.groupby("ticker")["date"].max().rename("prev_date").reset_index()
        )
        df_open = df_open.merge(prev_dates, on=["ticker"], how="left").merge(
            prev_close, on=["ticker", "prev_date"], how="left"
        )
        df_open = df_open.merge(
            df_splits_dividends,
            left_on=["ticker", "date"],
            right_on=["ticker", "event_date"],
            how="left",
        )

        df_open["split_from"] = df_open["split_from"].fillna(1.0)
        df_open["split_to"] = df_open["split_to"].fillna(1.0)
        df_open["cash_amount"] = df_open["cash_amount"].fillna(0.0)
        df_open["split_ratio"] = df_open["split_from"] / df_open["split_to"]
        df_open["expected_unchanged_adj_split_only"] = (
            df_open["prev_close"] * df_open["split_ratio"]
        )
        df_open["expected_unchanged_adj_split_dividend"] = (
            df_open["prev_close"] * df_open["split_ratio"] - df_open["cash_amount"]
        )
        df_open["had_split_or_dividend"] = df_open["event_type"].notna()

        is_split = df_open["event_type"] == "split"
        is_div = df_open["event_type"] == "dividend"

        def assign_adj_state(open_col):
            valid = (
                ~df_open[open_col].isna()
                & ~df_open["prev_close"].isna()
                & ~df_open["expected_unchanged_adj_split_dividend"].isna()
            )
            diff_adj = np.abs(
                df_open[open_col] - df_open["expected_unchanged_adj_split_dividend"]
            )
            diff_prev = np.abs(df_open[open_col] - df_open["prev_close"])
            rel_adj = diff_adj / np.maximum(
                np.abs(df_open["expected_unchanged_adj_split_dividend"]), 1e-8
            )
            rel_prev = diff_prev / np.maximum(np.abs(df_open["prev_close"]), 1e-8)

            state = np.full(df_open.shape[0], "unknown", dtype=object)

            # SPLIT: usually adjusted, but check
            mask = valid & is_split & ((diff_adj <= abs_tol) | (rel_adj <= rel_tol))
            state[mask] = "adjusted"
            mask = (
                valid
                & is_split
                & ((diff_prev <= abs_tol) | (rel_prev <= rel_tol))
                & (state == "unknown")
            )
            state[mask] = "unadjusted"

            # DIVIDEND: almost always unadjusted
            mask = valid & is_div & ((diff_prev <= abs_tol) | (rel_prev <= rel_tol))
            state[mask] = "unadjusted"
            # To catch rare dividend adjustment, uncomment these lines:
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
                "unknown",
            ),
        )
        df_open["robust_open_adj_state"] = robust

        # Set NaN for no-event rows in all *_adj_state columns
        mask_no_event = (df_open["split_ratio"] == 1.0) & (
            df_open["cash_amount"] == 0.0
        )
        for col in [
            "pre_market_open_adj_state",
            "market_open_adj_state",
            "robust_open_adj_state",
        ]:
            df_open.loc[mask_no_event, col] = np.nan

        # Determine which baseline to use for each row
        is_div = df_open["event_type"] == "dividend"
        is_split = df_open["event_type"] == "split"
        is_div_unknown = is_div & (df_open["robust_open_adj_state"] == "unknown")
        baseline = np.where(
            is_div_unknown,
            df_open["prev_close"],
            np.where(
                is_split,
                df_open["expected_unchanged_adj_split_only"],
                df_open["expected_unchanged_adj_split_dividend"],
            ),
        )

        df_open["adj_pmkt_pct_chg"] = (df_open["pre_market_open"] - baseline) / baseline
        df_open["adj_mkt_pct_chg"] = (df_open["market_open"] - baseline) / baseline

        column_order = [
            "ticker",
            "date",
            "pre_market_open",
            "market_open",
            "prev_close",
            "had_split_or_dividend",
            "event_type",
            "split_from",
            "split_to",
            "cash_amount",
            "split_ratio",
            "expected_unchanged_adj_split_dividend",
            "expected_unchanged_adj_split_only",
            "pre_market_open_adj_state",
            "market_open_adj_state",
            "robust_open_adj_state",
            "adj_pmkt_pct_chg",
            "adj_mkt_pct_chg",
        ]
        return df_open[column_order]
