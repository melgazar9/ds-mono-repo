import logging
from datetime import time as dtime
import numpy as np
import pandas as pd
import polars as pl
from ds_core.db_connectors import PostgresConnect
from bt_engine.engine import DataLoader
import paramiko
import os
from glob import glob
import io
from typing import Union, Optional, List, Callable, Any


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class CSVReader:
    """Simple CSV reader for local and remote files with file listing"""

    def __init__(
        self,
        local_or_remote,
        host: Optional[str] = None,
        username: Optional[str] = None,
        ssh_rsa_loc: str = "~/.ssh/id_rsa",
    ):
        """
        Initialize CSV reader

        Args:
            local_or_remote: "local" or "remote"
            host: SSH host (defaults to NORDVPN_MESHNET_IP env var)
            username: SSH username (defaults to NORDVPN_USER env var)
            ssh_rsa_loc: Path to SSH private key
        """
        self.connection_type = local_or_remote
        self.host = host or os.getenv("NORDVPN_MESHNET_IP")
        self.username = username or os.getenv("NORDVPN_USER")
        self.ssh_rsa_loc = ssh_rsa_loc

    def _execute_ssh_operation(
        self, operation_func: Callable[[paramiko.SSHClient], Any], operation_name: str
    ) -> Any:
        """
        Execute an operation over SSH with connection management

        Args:
            operation_func: Function that takes an SSH client and returns a result
            operation_name: Description of the operation for logging

        Returns:
            Result from operation_func
        """
        ssh = None
        try:
            logging.info(f"Connecting to {self.host} for {operation_name}")
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                self.host,
                username=self.username,
                key_filename=os.path.expanduser(self.ssh_rsa_loc),
            )

            return operation_func(ssh)

        except Exception as e:
            logging.error(f"Failed {operation_name}: {str(e)}")
            raise

        finally:
            if ssh:
                ssh.close()
                logging.debug("SSH connection closed")

    def list_files(self, directory_path: str, pattern: str = "*") -> List[str]:
        """
        List files in directory (local or remote)

        Args:
            directory_path: Path to directory
            pattern: File pattern (e.g., "*.csv", "bars_1m_*.csv.gz")

        Returns:
            List of file paths
        """
        if self.connection_type == "local":
            return self._list_local_files(directory_path, pattern)
        else:
            return self._execute_ssh_operation(
                lambda ssh: self._list_remote_files(ssh, directory_path, pattern),
                f"listing files in {directory_path}",
            )

    @staticmethod
    def _list_local_files(directory_path: str, pattern: str) -> List[str]:
        """List local files"""
        full_pattern = os.path.join(os.path.expanduser(directory_path), pattern)
        files = sorted(glob(full_pattern))
        logging.info(f"Found {len(files)} local files matching {pattern}")
        return files

    @staticmethod
    def _list_remote_files(
        ssh: paramiko.SSHClient, directory_path: str, pattern: str
    ) -> List[str]:
        """List remote files operation (called within SSH connection)"""
        if pattern == "*":
            command = f"ls '{directory_path}' | sort"
        else:
            command = f"ls '{directory_path}'/{pattern} | sort"

        # Execute command
        stdin, stdout, stderr = ssh.exec_command(command)

        # Check for errors
        error_output = stderr.read().decode("utf-8")
        if error_output and "No such file" not in error_output:
            logging.warning(f"Remote ls warning: {error_output}")

        # Get file list
        files_output = stdout.read().decode("utf-8").strip()
        if not files_output:
            files = []
        else:
            files = [f.strip() for f in files_output.split("\n") if f.strip()]

        logging.info(f"Found {len(files)} remote files matching {pattern}")
        return files

    def read_csv(
        self, file_path: str, timeout: int = 300, **pd_read_csv_kwargs
    ) -> pd.DataFrame:
        """
        Read a single CSV file (local or remote)

        Args:
            file_path: Path to the CSV file
            timeout: Timeout for remote operations (seconds)
            **pd_read_csv_kwargs: Arguments passed to pd.read_csv

        Returns:
            DataFrame
        """
        if self.connection_type == "local":
            return pd.read_csv(file_path, **pd_read_csv_kwargs)
        else:
            return self._execute_ssh_operation(
                lambda ssh: self._read_remote_csv(
                    ssh, file_path, timeout, **pd_read_csv_kwargs
                ),
                f"reading {file_path}",
            )

    @staticmethod
    def _read_remote_csv(
        ssh: paramiko.SSHClient, file_path: str, timeout: int, **pd_read_csv_kwargs
    ) -> pd.DataFrame:
        """Read remote CSV file operation - handles gzip efficiently"""

        if file_path.endswith(".gz"):
            command = f"cat '{file_path}'"  # NOT zcat - get raw bytes
            stdin, stdout, stderr = ssh.exec_command(command, timeout=timeout)

            # Check for errors
            error_output = stderr.read().decode("utf-8")
            if error_output:
                raise Exception(f"Remote command error: {error_output}")

            # Get raw gzip bytes
            raw_bytes = stdout.read()
            if not raw_bytes:
                raise Exception(f"No data returned from {file_path}")
            df = pd.read_csv(io.BytesIO(raw_bytes), **pd_read_csv_kwargs)

        else:
            # Regular CSV files
            command = f"cat '{file_path}'"
            stdin, stdout, stderr = ssh.exec_command(command, timeout=timeout)

            error_output = stderr.read().decode("utf-8")
            if error_output:
                raise Exception(f"Remote command error: {error_output}")

            csv_data = stdout.read().decode("utf-8")
            if not csv_data.strip():
                raise Exception(f"No data returned from {file_path}")

            df = pd.read_csv(io.StringIO(csv_data), **pd_read_csv_kwargs)

        logging.info(f"Successfully loaded {len(df):,} rows from remote file")
        return df


class PolygonBarLoader(DataLoader):
    def __init__(
        self,
        load_method="pandas",
        cur_day_file=None,
        df_prev=None,
        local_or_remote=None,
    ):
        self.load_method = load_method
        self.cur_day_file = cur_day_file
        self.df_prev = df_prev
        self.local_or_remote = local_or_remote

        self.csv_reader = CSVReader(local_or_remote=self.local_or_remote)

        assert (
            local_or_remote is not None
        ), "local_or_remote cannot be None. Must be set to 'local' or 'remote'"

        if self.load_method not in ["pandas", "polars"]:
            raise NotImplementedError(
                f"Unknown load method {self.load_method}. Must be 'pandas' or 'polars'"
            )

    def pull_splits_dividends(self):
        if hasattr(self, "df_splits_dividends"):
            logging.info("df_splits_dividends is already loaded, no need to re-pull.")
        else:
            logging.warning("Pulling df_splits_dividends!")
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
                ).dt.tz_localize("America/Chicago")
            elif self.load_method == "polars":
                self.df_splits_dividends = self.df_splits_dividends.with_columns(
                    [pl.col("event_date").dt.convert_time_zone("America/Chicago")]
                )
        return self

    def load_raw_intraday_bars(self) -> Union[pd.DataFrame, pl.DataFrame]:
        """Loads bars data (e.g. 1-minute) from polygon flat files."""
        if self.load_method == "pandas":
            df = self.csv_reader.read_csv(self.cur_day_file, compression="gzip")
            df = df.rename(columns={"window_start": "timestamp"})
            df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True)
            df["timestamp_cst"] = df["timestamp_utc"].dt.tz_convert("America/Chicago")
            df["date"] = df["timestamp_cst"].dt.normalize()
            df = df.sort_values(by=["timestamp", "ticker"])
        elif self.load_method == "polars":
            logging.error(
                "polars is currently not supported."
            )  # TODO: Add Polars support
            df = pl.read_csv(self.cur_day_file)
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
                    .dt.convert_time_zone("America/Chicago")
                    .alias("timestamp_cst")
                ]
            )
            df = df.with_columns([pl.col("timestamp_cst").dt.date().alias("date")])
            df = df.sort(["timestamp", "ticker"])
        else:
            raise ValueError(f"Unsupported load_method: {self.load_method}")
        return df

    def load_and_clean_data(self) -> Union[pd.DataFrame, pl.DataFrame]:
        """Assumes self.df_prev is attached to the class"""
        self.df = self.load_raw_intraday_bars()
        self.pull_splits_dividends()  # does nothing if class already has attribute self.df_splits_dividends

        if (
            self.load_method == "polars"
        ):  # TODO: integrate polars functionality across methods
            logging.debug(
                "Temporarily converting self.df, self.df_prev, and self.df_splits_dividends to pandas dfs."
            )
            self.df = self.df.to_pandas()
            self.df_prev = self.df_prev.to_pandas()
            self.df_splits_dividends = self.df_splits_dividends.to_pandas()
            logging.debug(
                "self.df, self.df_prev, and self.df_splits_dividends are now pandas dfs."
            )

        rows_before = self.df.shape[0]
        # pandas syntax
        self.df = self._price_gap_with_split_or_dividend()
        assert (
            self.df.shape[0] == rows_before
        ), "Row mismatch after split and dividend validation."

        if self.load_method == "polars":
            self.df = pl.from_pandas(self.df)
            self.df_prev = pl.from_pandas(self.df_prev)
            logging.debug("Converted self.df and self.df_prev back to polars.")
        return self.df

    def _price_gap_with_split_or_dividend(
        self,
        abs_tol=0.02,
        rel_tol=0.002,
    ):
        if self.load_method == "polars":
            logging.debug(
                "load_method polars not implemented yet for detecting splits/dividends price gap. Data will be"
                "converted to a pandas df and converted back to polars for this step."
            )

        prev_after_hours_close = (
            self.df_prev.groupby(["ticker", "date"], sort=False)
            .agg(prev_close=("close", "last"))
            .reset_index()
            .rename(
                columns={"date": "prev_date", "prev_close": "prev_after_hours_close"}
            )
        )

        prev_market_close = (
            self.df_prev[self.df_prev["timestamp_cst"].dt.time <= dtime(15, 0)]
            .groupby(["ticker", "date"], sort=False)
            .agg(prev_close=("close", "last"))
            .reset_index()
            .rename(columns={"date": "prev_date", "prev_close": "prev_market_close"})
        )

        prev_dates = (
            self.df_prev.groupby("ticker")["date"]
            .max()
            .rename("prev_date")
            .reset_index()
        )

        self.df = (
            self.df.merge(prev_dates, on=["ticker"], how="left")
            .merge(prev_after_hours_close, on=["ticker", "prev_date"], how="left")
            .merge(prev_market_close, on=["ticker", "prev_date"], how="left")
        )

        self.df = self.df.merge(
            self.df_splits_dividends,
            left_on=["ticker", "date"],
            right_on=["ticker", "event_date"],
            how="left",
        )

        self.df["split_from"] = self.df["split_from"].fillna(1.0)
        self.df["split_to"] = self.df["split_to"].fillna(1.0)
        self.df["cash_amount"] = self.df["cash_amount"].fillna(0.0)
        self.df["split_ratio"] = self.df["split_from"] / self.df["split_to"]

        # Combinations
        # 1. market close -> pre-market open
        # 2. market close -> market open
        # 3. after hours close -> pre-market open
        # 4. after hours close -> market open
        # We will detect if ticker has splits/dividends applied based from official market close --> pre-market open.

        self.df["expected_unchanged_price_after_split"] = (
            self.df["prev_market_close"] * self.df["split_ratio"]
        )
        self.df["expected_unchanged_price_after_split_and_dividend"] = (
            self.df["prev_market_close"] * self.df["split_ratio"]
            - self.df["cash_amount"]
        )

        self.df["had_split_or_dividend"] = self.df["event_type"].notna()

        self.df["pre_market_open"] = (
            self.df[self.df["timestamp_cst"].dt.time < dtime(8, 30)]
            .groupby(["ticker", "date"])["open"]
            .transform("first")
        )
        self.df["pre_market_open"] = self.df.groupby(["ticker", "date"])[
            "pre_market_open"
        ].ffill()

        self.df["market_open"] = (
            self.df[self.df["timestamp_cst"].dt.time >= dtime(8, 30)]
            .groupby(["ticker", "date"])["open"]
            .transform("first")
        )

        expected_adj = self.df["prev_market_close"] * self.df[
            "split_ratio"
        ] - self.df.get("cash_amount", 0.0)

        self.df["adj_pmkt_pct_chg"] = np.nan
        self.df["adj_mkt_pct_chg"] = np.nan
        self.df.loc[
            self.df["timestamp_cst"].dt.time < dtime(8, 30), "adj_pmkt_pct_chg"
        ] = (self.df["open"] - expected_adj) / expected_adj
        self.df.loc[
            self.df["timestamp_cst"].dt.time >= dtime(8, 30), "adj_mkt_pct_chg"
        ] = (self.df["open"] - expected_adj) / expected_adj

        def assign_adj_state(
            df,
            open_col: str,
            prev_close_col: str = "prev_market_close",
            expected_col: str = "expected_unchanged_price_after_split",
            event_col: str = "event_type",
            abs_tol=1.0,
            rel_tol=0.05,
            proximity_margin=0.2,  # Require at least 20% closer to adjusted than prev_close
        ):
            is_split = df[event_col] == "split"
            is_div = df[event_col] == "dividend"
            valid = (
                ~df[open_col].isna()
                & ~df[prev_close_col].isna()
                & ~df[expected_col].isna()
            )
            diff_adj = np.abs(df[open_col] - df[expected_col])
            diff_prev = np.abs(df[open_col] - df[prev_close_col])
            rel_adj = diff_adj / np.maximum(np.abs(df[expected_col]), 1e-8)
            rel_prev = diff_prev / np.maximum(np.abs(df[prev_close_col]), 1e-8)
            state = np.full(df.shape[0], "unknown", dtype=object)

            # SPLIT: adjusted if much closer to adjusted than unadjusted
            closer_to_adj = (diff_prev - diff_adj) > (
                proximity_margin * np.abs(df[expected_col])
            )
            mask = valid & is_split & closer_to_adj
            state[mask] = "adjusted"

            # SPLIT: unadjusted if much closer to prev close
            closer_to_prev = (diff_adj - diff_prev) > (
                proximity_margin * np.abs(df[prev_close_col])
            )
            mask = valid & is_split & closer_to_prev & (state == "unknown")
            state[mask] = "unadjusted"

            # SPLIT: also allow for very close (tight tolerance) to adjusted
            mask = (
                valid
                & is_split
                & ((diff_adj <= abs_tol) | (rel_adj <= rel_tol))
                & (state == "unknown")
            )
            state[mask] = "adjusted"

            # DIVIDEND: almost always unadjusted
            mask = valid & is_div & ((diff_prev <= abs_tol) | (rel_prev <= rel_tol))
            state[mask] = "unadjusted"
            return state

        self.df["pre_market_open_adj_state"] = assign_adj_state(
            self.df, "pre_market_open"
        )
        self.df["market_open_adj_state"] = assign_adj_state(self.df, "market_open")

        # Robust: prefer market open, else pre-market, else unknown
        robust = np.where(
            self.df["market_open_adj_state"] != "unknown",
            self.df["market_open_adj_state"],
            np.where(
                self.df["pre_market_open_adj_state"] != "unknown",
                self.df["pre_market_open_adj_state"],
                "unknown",
            ),
        )
        self.df["robust_open_adj_state"] = robust

        # Set NaN for no-event rows in all *_adj_state columns
        mask_no_event = (self.df["split_ratio"] == 1.0) & (
            self.df["cash_amount"] == 0.0
        )
        for col in [
            "pre_market_open_adj_state",
            "market_open_adj_state",
            "robust_open_adj_state",
        ]:
            self.df.loc[mask_no_event, col] = np.nan

        self.df["adj_state"] = self.df["robust_open_adj_state"]

        keep_columns = [
            i
            for i in self.df.columns
            if i
            not in [
                "pre_market_open_adj_state",
                "market_open_adj_state",
                "robust_open_adj_state",
            ]
        ]
        return self.df[keep_columns]
