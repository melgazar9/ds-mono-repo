# flake8: noqa: W291, W293

import logging
import os
from datetime import time as dtime
import numpy as np
import pandas as pd
import polars as pl
from ds_core.db_connectors import PostgresConnect
from bt_engine.engine import DataLoader
from typing import Union, List
from pathlib import Path
import asyncio

import backoff


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)


class StreamingFileProcessor:
    def __init__(
        self,
        remote_host,
        remote_user,
        cache_dir=os.path.expanduser("~/.cache/file_cache/"),
        max_concurrent_downloads=3,
        max_cached_files=10,
        timeout=120,
    ):
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout

        # Control concurrent downloads and cache size
        self.download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.cache_semaphore = asyncio.Semaphore(max_cached_files)

        # Queue for ready files
        self.ready_files = asyncio.Queue()
        self.download_tasks = {}
        self.active_downloads = set()

    @backoff.on_exception(backoff.expo, (asyncio.TimeoutError, Exception), max_tries=3, jitter=backoff.full_jitter)
    async def download_file(self, remote_path, file_index):
        """Download a single file and put it in the ready queue when done"""
        async with self.download_semaphore:  # Limit concurrent downloads
            filename = Path(remote_path).name
            local_path = self.cache_dir / filename

            if local_path.exists():
                await self.ready_files.put((file_index, local_path))
                return

            logging.info(f"🔄 Starting download: {filename}")

            cmd = ["rsync", "-avz", f"{self.remote_user}@{self.remote_host}:{remote_path}", str(local_path)]
            try:
                proc = await asyncio.wait_for(
                    asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE),
                    timeout=self.timeout,
                )
                await proc.communicate()

                if proc.returncode == 0:
                    logging.info(f"✅ Downloaded: {filename}")
                    await self.ready_files.put((file_index, local_path))
                else:
                    logging.error(f"❌ Failed to download: {filename}")
                    await self.ready_files.put((file_index, None))
                    raise Exception(f"Failed to download {filename}")
            except asyncio.TimeoutError:
                logging.error(f"⏰ Timeout while downloading: {filename}")
                raise
            except Exception as e:
                logging.error(f"⚠️ Error downloading {filename}: {e}")
                raise

    async def start_downloads(self, remote_files):
        """Start downloading files in batches, respecting max_cached_files limit"""
        # Only start downloads up to max_cached_files
        max_to_start = min(len(remote_files), self.cache_semaphore._value)  # max_cached_files

        for i in range(max_to_start):
            if i < len(remote_files):
                task = asyncio.create_task(self.download_file(remote_files[i], i))
                self.download_tasks[i] = task
                self.active_downloads.add(i)

        logging.info(f"Started {max_to_start} initial downloads (max_cached_files={self.cache_semaphore._value})")

    async def start_next_download(self, remote_files, next_index):
        """Start the next download when a slot becomes available"""
        if next_index < len(remote_files) and next_index not in self.download_tasks:
            task = asyncio.create_task(self.download_file(remote_files[next_index], next_index))
            self.download_tasks[next_index] = task
            self.active_downloads.add(next_index)
            logging.info(f"🔄 Started download {next_index + 1}: {Path(remote_files[next_index]).name}")

    async def get_files(self, remote_files_or_path, pattern="*"):
        """Stream files as they become ready, in order"""
        # Handle both list of files and directory + pattern
        if isinstance(remote_files_or_path, list) and len(remote_files_or_path):
            files = sorted(remote_files_or_path)
            logging.info(f"📋 Processing {len(files)} provided files")
        else:
            files = await self.list_remote_files(remote_files_or_path, pattern)
        if not files:
            raise ValueError("No files found!")

        # Start downloading initial batch
        await self.start_downloads(files)

        # Keep track of what we've processed
        next_file_index = 0
        next_download_index = min(len(files), self.cache_semaphore._value)  # Start with batch size
        cached_files = {}  # {index: local_path}
        total_files = len(files)

        logging.info(f"Processing {total_files} files with max {self.cache_semaphore._value} cached...")

        while next_file_index < total_files:
            # Wait for the next file we need (in order)
            while next_file_index not in cached_files:
                try:
                    # Get the next ready file
                    file_index, local_path = await asyncio.wait_for(self.ready_files.get(), timeout=self.timeout)

                    if local_path:  # Successfully downloaded
                        cached_files[file_index] = local_path
                        logging.info(f"📦 Cached file {file_index + 1}/{total_files}: {local_path.name}")

                        # Start next download if available
                        if next_download_index < total_files:
                            await self.start_next_download(files, next_download_index)
                            next_download_index += 1

                    else:  # Download failed
                        logging.warning(f"⚠️ Skipping failed file {file_index + 1}")
                        cached_files[file_index] = None

                except asyncio.TimeoutError:
                    logging.error("⏰ Timeout waiting for file download")
                    break

            if next_file_index in cached_files:
                local_path = cached_files[next_file_index]

                if local_path and local_path.exists():
                    yield local_path

                    # Clean up
                    local_path.unlink()
                    logging.info(f"🗑️ Deleted: {local_path.name}")

                # Remove from cache and move to next
                del cached_files[next_file_index]
                next_file_index += 1

        # Clean up remaining tasks
        for task in self.download_tasks.values():
            if not task.done():
                task.cancel()

    async def list_remote_files(self, remote_path: str, pattern: str = "*") -> List[str]:
        """List files in remote directory with pattern"""
        cmd = ["ssh", f"{self.remote_user}@{self.remote_host}", f"ls {remote_path}/{pattern}"]

        try:
            proc = await asyncio.wait_for(
                asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE),
                timeout=self.timeout,
            )
            stdout, _ = await proc.communicate()

            if proc.returncode == 0:
                files = [f.strip() for f in stdout.decode().strip().split("\n") if f.strip()]
                logging.info(f"📋 Found {len(files)} files")
                return sorted(files)
            return []
        except asyncio.TimeoutError:
            logging.error(f"⏰ Timeout while listing files in remote path: {remote_path}")
            return []


class PolygonBarLoader(DataLoader):
    def __init__(
        self,
        load_method="pandas",
        cur_day_file=None,
        df_prev=None,
        remove_ma_tickers_from_intraday_bars=True,
        remove_earnings_calendar_dates_from_intraday_bars=False,
        remove_low_quality_tickers=False,
    ):
        self.load_method = load_method
        self.cur_day_file = cur_day_file
        self.df_prev = df_prev
        self.remove_low_quality_tickers = remove_low_quality_tickers

        self.df_corporate_actions = None
        self.remove_ma_tickers_from_intraday_bars = remove_ma_tickers_from_intraday_bars
        self.remove_earnings_calendar_dates_from_intraday_bars = remove_earnings_calendar_dates_from_intraday_bars

        if self.load_method not in ["pandas", "polars"]:
            raise NotImplementedError(f"Unknown load method {self.load_method}. Must be 'pandas' or 'polars'")

    def pull_cached_corporate_actions(self):
        if isinstance(self.df_corporate_actions, (pd.DataFrame, pl.DataFrame)):
            logging.info("df_corporate_actions is already loaded, no need to re-pull.")
        else:
            logging.warning("Pulling df_corporate_actions with historical ticker mapping.")
            with PostgresConnect(database="financial_elt") as db:
                self.df_corporate_actions = db.run_sql(
                    """
                    with cte_ranked as (
                        select
                            composite_figi,
                            cik,
                            ticker,
                            event_date,
                            event_type,
                            cash_amount,
                            currency,
                            split_from,
                            split_to,
                            split_ratio,
                            filing_type,
                            requires_price_adjustment,
                            potential_delisting,
                            data_quality_score,
                            row_number() over (partition by ticker, event_date order by data_quality_score desc nulls last) as rn
                        from
                            financial_analytics.gap_strategy_exclusions
                    ),
                    cte_agg as (
                        select
                            ticker,
                            event_date,
                            first_value(composite_figi) over (partition by ticker, event_date order by data_quality_score desc nulls last) as composite_figi,
                            first_value(cik) over (partition by ticker, event_date order by data_quality_score desc nulls last) as cik,
                            first_value(event_type) over (partition by ticker, event_date order by data_quality_score desc nulls last) as event_type,
                            sum(coalesce(cash_amount, 0)) over (partition by ticker, event_date) as cash_amount,
                            first_value(currency) over (partition by ticker, event_date order by data_quality_score desc nulls last) as currency,
                            exp(sum(ln(coalesce(nullif(split_from, 0), 1))) over (partition by ticker, event_date)) as split_from,
                            exp(sum(ln(coalesce(nullif(split_to, 0), 1))) over (partition by ticker, event_date)) as split_to,
                            exp(sum(ln(coalesce(nullif(split_ratio, 0), 1))) over (partition by ticker, event_date)) as split_ratio,
                            first_value(filing_type) over (partition by ticker, event_date order by data_quality_score desc nulls last) as filing_type,
                            bool_or(coalesce(requires_price_adjustment, false)) over (partition by ticker, event_date) as requires_price_adjustment,
                            bool_or(coalesce(potential_delisting, false)) over (partition by ticker, event_date) as potential_delisting,
                            max(coalesce(data_quality_score, 0)) over (partition by ticker, event_date) as data_quality_score,
                            row_number() over (partition by ticker, event_date order by data_quality_score desc nulls last) as rn
                        from
                            cte_ranked
                    )
                    select 
                        composite_figi,
                        cik,
                        ticker,
                        event_date,
                        event_type,
                        cash_amount,
                        currency,
                        split_from,
                        split_to,
                        split_ratio,
                        filing_type,
                        requires_price_adjustment,
                        potential_delisting,
                        data_quality_score
                    from
                        cte_agg
                    where
                        rn = 1
                """,
                    df_type=self.load_method,
                )
                logging.info("✅ Gap strategy exclusions data loaded - will be cached across tests.")

            if self.load_method == "pandas":
                self.df_corporate_actions["event_date_raw"] = self.df_corporate_actions["event_date"]
                self.df_corporate_actions["event_date"] = pd.to_datetime(
                    self.df_corporate_actions["event_date"]
                ).dt.tz_localize("America/Chicago")
            elif self.load_method == "polars":
                self.df_corporate_actions = self.df_corporate_actions.with_columns(
                    [
                        pl.col("event_date").alias("event_date_raw"),
                        pl.col("event_date")
                        .str.to_datetime(format="%Y-%m-%d %H:%M:%S")
                        .dt.replace_time_zone("America/Chicago")
                        .alias("event_date"),
                    ]
                )
        return self

    def _get_ma_ticker_dates(self):
        self.pull_cached_corporate_actions()
        if hasattr(self, "df_ma_events"):
            return

        self.df_ma_events = self.df_corporate_actions[
            (self.df_corporate_actions["requires_price_adjustment"])
            & (~self.df_corporate_actions["event_type"].isin(["dividend", "split", "reverse_split"]))
        ][["ticker", "event_date"]]

    def _get_earnings_calendar_dates(self):
        """Get earnings calendar dates for tickers.

        Fetches earnings dates from the unified earnings_calendar table (FMP + yfinance).
        The table includes pre/post-market timing and filter dates for backtesting.

        Filter logic:
        - Pre-market earnings: filter_date_primary = earnings_date (filter that day)
        - Post-market earnings: filter_date_primary = earnings_date + 1 (filter next day)
        - Unknown timing: both filter_date_primary and filter_date_secondary set (filter both days)
        """
        if hasattr(self, "df_earnings_events"):
            return

        logging.info("Fetching earnings calendar dates from database.")
        with PostgresConnect(database="financial_elt") as db:
            self.df_earnings_events = db.run_sql(
                """
                SELECT
                    ticker,
                    earnings_date,
                    source,
                    release_timing,
                    filter_date_primary,
                    filter_date_secondary,
                    data_quality
                FROM financial_analytics.earnings_calendar
                WHERE ticker IS NOT NULL
                  AND (filter_date_primary IS NOT NULL OR filter_date_secondary IS NOT NULL)
                ORDER BY ticker, earnings_date
                """,
                df_type=self.load_method,
            )

            total_events = len(self.df_earnings_events)
            if self.load_method == "pandas":
                n_with_timing = self.df_earnings_events["release_timing"].notna().sum()
                n_pre = (self.df_earnings_events["release_timing"] == "pre-market").sum()
                n_post = (self.df_earnings_events["release_timing"] == "post-market").sum()
            else:
                n_with_timing = self.df_earnings_events.filter(pl.col("release_timing").is_not_null()).height
                n_pre = self.df_earnings_events.filter(pl.col("release_timing") == "pre-market").height
                n_post = self.df_earnings_events.filter(pl.col("release_timing") == "post-market").height

            logging.info(
                f"✅ Fetched {total_events} earnings events. "
                f"Timing breakdown: {n_pre} pre-market, {n_post} post-market, "
                f"{total_events - n_with_timing} unknown (will filter conservatively)."
            )

        # Convert dates to timezone-aware datetime to match intraday bars
        if self.load_method == "pandas":
            self.df_earnings_events["filter_date_primary"] = pd.to_datetime(
                self.df_earnings_events["filter_date_primary"]
            ).dt.tz_localize("America/Chicago")

            # Handle secondary filter date (for unknown timing)
            if "filter_date_secondary" in self.df_earnings_events.columns:
                self.df_earnings_events["filter_date_secondary"] = pd.to_datetime(
                    self.df_earnings_events["filter_date_secondary"]
                ).dt.tz_localize("America/Chicago")

        elif self.load_method == "polars":
            self.df_earnings_events = self.df_earnings_events.with_columns(
                [
                    pl.col("filter_date_primary")
                    .str.to_datetime(format="%Y-%m-%d")
                    .dt.replace_time_zone("America/Chicago")
                    .alias("filter_date_primary"),
                    pl.col("filter_date_secondary")
                    .str.to_datetime(format="%Y-%m-%d")
                    .dt.replace_time_zone("America/Chicago")
                    .alias("filter_date_secondary"),
                ]
            )

    def _remove_ma_records(self):
        filter_keys = self.df_ma_events[["ticker", "event_date"]].rename(columns={"event_date": "date"})
        merged = self.df.merge(filter_keys, on=["ticker", "date"], how="left", indicator=True)
        removed_records = merged[merged["_merge"] == "both"].drop(columns=["_merge"])
        logging.info(f"Removed {len(removed_records)} records from df from M&A corporate actions.")
        self.df = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        self.df = self.df.sort_values(by=["timestamp", "ticker"])

    def _remove_earnings_calendar_records(self):
        """Remove records on earnings calendar dates.

        Removes intraday bars based on earnings timing:
        - Pre-market earnings: removes bars on earnings_date
        - Post-market earnings: removes bars on earnings_date + 1
        - Unknown timing: removes bars on BOTH dates (conservative approach)
        """
        if self.df_earnings_events.empty:
            logging.warning("df_earnings_events is empty, no earnings records to filter.")
            return

        # Create filter keys for primary filter dates
        filter_keys_primary = self.df_earnings_events[["ticker", "filter_date_primary"]].rename(
            columns={"filter_date_primary": "date"}
        )
        filter_keys_primary = filter_keys_primary[filter_keys_primary["date"].notna()]

        # Create filter keys for secondary filter dates (for unknown timing)
        filter_keys_secondary = self.df_earnings_events[["ticker", "filter_date_secondary"]].rename(
            columns={"filter_date_secondary": "date"}
        )
        filter_keys_secondary = filter_keys_secondary[filter_keys_secondary["date"].notna()]

        # Combine both filter sets
        filter_keys = pd.concat([filter_keys_primary, filter_keys_secondary], ignore_index=True).drop_duplicates()

        # Merge with intraday bars
        merged = self.df.merge(filter_keys, on=["ticker", "date"], how="left", indicator=True)
        removed_records = merged[merged["_merge"] == "both"]

        # Log removal stats
        n_removed = len(removed_records)
        n_tickers = removed_records["ticker"].nunique() if n_removed > 0 else 0
        n_dates = removed_records["date"].nunique() if n_removed > 0 else 0

        logging.info(
            f"Removed {n_removed:,} intraday bar records on earnings dates " f"({n_tickers} tickers, {n_dates} unique dates)."
        )

        # Keep only records that didn't match (left_only)
        self.df = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        self.df = self.df.sort_values(by=["timestamp", "ticker"])

    def _remove_low_quality_tickers(self, dollar_threshold=5000000, min_avg_close_price=5):
        volume_by_ticker = (
            self.df[(self.df["timestamp_cst"].dt.time >= dtime(8, 0)) & (self.df["timestamp_cst"].dt.time <= dtime(15, 0))]
            .groupby(["ticker", "date"])[["volume", "close"]]
            .agg({"volume": "sum", "close": "mean"})
        )
        volume_by_ticker["dollar_volume"] = volume_by_ticker["close"] * volume_by_ticker["volume"]

        good_volume_tickers = (
            volume_by_ticker[volume_by_ticker["dollar_volume"] >= dollar_threshold].index.get_level_values("ticker").tolist()
        )

        avg_close_by_ticker = (
            self.df[(self.df["timestamp_cst"].dt.time >= dtime(8, 0)) & (self.df["timestamp_cst"].dt.time <= dtime(15, 0))]
            .groupby(["ticker", "date"])["close"]
            .last()
        )

        good_price_tickers = (
            avg_close_by_ticker[avg_close_by_ticker >= min_avg_close_price].index.get_level_values("ticker").tolist()
        )

        good_tickers = list(set(good_volume_tickers) & set(good_price_tickers))
        tickers_before = self.df["ticker"].nunique()
        self.df = self.df[self.df["ticker"].isin(good_tickers)]
        tickers_after = self.df["ticker"].nunique()
        logging.info(
            f"Removed {tickers_before - tickers_after}/{tickers_before} ({((tickers_before - tickers_after) / tickers_before)*100:.2f}%) low quality tickers."
        )
        self.df = self.df.sort_values(by=["timestamp", "ticker"])

    def load_raw_intraday_bars(self) -> Union[pd.DataFrame, pl.DataFrame]:
        """Loads bars data (e.g. 1-minute) from polygon flat files."""
        if self.load_method == "pandas":
            df = pd.read_csv(self.cur_day_file, compression="gzip")
            df = df.rename(columns={"window_start": "timestamp"})
            df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True)
            df["timestamp_cst"] = df["timestamp_utc"].dt.tz_convert("America/Chicago")
            df["date"] = df["timestamp_cst"].dt.normalize()
            df = df.sort_values(by=["timestamp", "ticker"])

        elif self.load_method == "polars":
            logging.error("polars is currently not supported.")  # TODO: Add Polars support
            df = pl.read_csv(self.cur_day_file)
            df = df.rename({"window_start": "timestamp"})
            df = df.with_columns(
                [pl.from_epoch("timestamp", time_unit="ns").dt.replace_time_zone("UTC").alias("timestamp_utc")]
            )
            df = df.with_columns([pl.col("timestamp_utc").dt.convert_time_zone("America/Chicago").alias("timestamp_cst")])
            df = df.with_columns([pl.col("timestamp_cst").dt.date().alias("date")])
            df = df.sort(["timestamp", "ticker"])
        else:
            raise ValueError(f"Unsupported load_method: {self.load_method}")
        return df

    def load_and_clean_data(self) -> Union[pd.DataFrame, pl.DataFrame]:
        """Assumes self.df_prev is attached to the class"""
        self.pull_cached_corporate_actions()  # does nothing if class already has attribute self.df_corporate_actions
        self.df = self.load_raw_intraday_bars()

        if self.load_method == "polars":  # TODO: integrate polars functionality across methods
            logging.debug("Temporarily converting self.df, self.df_prev, and self.df_corporate_actions to pandas dfs.")
            self.df = self.df.to_pandas()
            self.df_prev = self.df_prev.to_pandas()
            logging.debug("self.df, self.df_prev, and self.df_corporate_actions are now pandas dfs.")

        if self.remove_ma_tickers_from_intraday_bars:
            self._get_ma_ticker_dates()
            self._remove_ma_records()

        if self.remove_earnings_calendar_dates_from_intraday_bars:
            self._get_earnings_calendar_dates()
            self._remove_earnings_calendar_records()

        rows_before = self.df.shape[0]
        self.df = self._add_prev_us_ohlcv()
        if self.remove_low_quality_tickers:
            self._remove_low_quality_tickers()
        self.df = self._price_gap_with_split_or_dividend()
        assert (
            self.df.shape[0] == rows_before
        ), f"Row mismatch after split and dividend validation: {rows_before} -> {self.df.shape[0]}"

        if self.load_method == "polars":
            self.df = pl.from_pandas(self.df)
            self.df_prev = pl.from_pandas(self.df_prev)
            logging.debug("Converted self.df and self.df_prev back to polars.")
        return self.df

    def _add_prev_us_ohlcv(self):
        """Adds prev ohlcv for pre-market, post-market, and regular market hours."""
        prev_pmkt_us_ohlcv = (
            self.df_prev[
                (self.df_prev["timestamp_cst"].dt.time >= dtime(3, 0)) & (self.df_prev["timestamp_cst"].dt.time < dtime(8, 30))
            ]
            .groupby(["ticker", "date"], sort=False)
            .agg(
                prev_pmkt_open=("open", "first"),
                prev_pmkt_high=("high", "max"),
                prev_pmkt_low=("low", "min"),
                prev_pmkt_close=("close", "last"),
                prev_pmkt_volume=("volume", "sum"),
            )
            .reset_index()
            .rename(columns={"date": "prev_date"})
        )

        prev_market_us_ohlcv = (
            self.df_prev[
                (self.df_prev["timestamp_cst"].dt.time >= dtime(8, 30))
                & (self.df_prev["timestamp_cst"].dt.time <= dtime(15, 0))
            ]
            .groupby(["ticker", "date"], sort=False)
            .agg(
                prev_market_open=("open", "first"),
                prev_market_high=("high", "max"),
                prev_market_low=("low", "min"),
                prev_market_close=("close", "last"),
                prev_market_volume=("volume", "sum"),
            )
            .reset_index()
            .rename(columns={"date": "prev_date"})
        )

        prev_after_hours_us_ohlcv = (
            self.df_prev[
                (self.df_prev["timestamp_cst"].dt.time > dtime(15, 0)) & (self.df_prev["timestamp_cst"].dt.time <= dtime(20, 0))
            ]
            .groupby(["ticker", "date"], sort=False)
            .agg(
                prev_after_hours_open=("open", "first"),
                prev_after_hours_high=("high", "max"),
                prev_after_hours_low=("low", "min"),
                prev_after_hours_close=("close", "last"),
                prev_after_hours_volume=("volume", "sum"),
            )
            .reset_index()
            .rename(columns={"date": "prev_date"})
        )

        prev_dates = self.df_prev.groupby("ticker")["date"].max().rename("prev_date").reset_index()

        self.df = (
            self.df.merge(prev_dates, on=["ticker"], how="left")
            .merge(prev_pmkt_us_ohlcv, on=["ticker", "prev_date"], how="left")
            .merge(prev_market_us_ohlcv, on=["ticker", "prev_date"], how="left")
            .merge(prev_after_hours_us_ohlcv, on=["ticker", "prev_date"], how="left")
        )

        self.df = self.df.sort_values(by=["timestamp", "ticker"])
        return self.df

    def _price_gap_with_split_or_dividend(self):
        # Combinations
        # 1. market close -> pre-market open
        # 2. market close -> market open
        # 3. after hours close -> pre-market open
        # 4. after hours close -> market open
        # For simplicity, only detect if ticker has splits/dividends applied from official market close --> pre-market open.

        if self.load_method == "polars":
            logging.debug(
                "load_method polars not implemented yet for detecting splits/dividends price gap. Data will be"
                "converted to a pandas df and converted back to polars for this step."
            )

        self.df = self.df.merge(
            self.df_corporate_actions, left_on=["ticker", "date"], right_on=["ticker", "event_date"], how="left"
        )

        self.df["split_from"] = self.df["split_from"].fillna(1.0)
        self.df["split_to"] = self.df["split_to"].fillna(1.0)
        self.df["cash_amount"] = self.df["cash_amount"].fillna(0.0)
        self.df["split_ratio"] = self.df["split_from"] / self.df["split_to"]

        self.df["expected_unchanged_price_after_split"] = self.df["prev_market_close"] * self.df["split_ratio"]
        self.df["expected_unchanged_price_after_split_and_dividend"] = (
            self.df["prev_market_close"] * self.df["split_ratio"] - self.df["cash_amount"]
        )

        self.df["had_split_or_dividend"] = self.df["event_type"].notna()

        self.df["pre_market_open"] = (
            self.df[self.df["timestamp_cst"].dt.time < dtime(8, 30)].groupby(["ticker", "date"])["open"].transform("first")
        )
        self.df["pre_market_open"] = self.df.groupby(["ticker", "date"])["pre_market_open"].ffill()

        self.df["market_open"] = (
            self.df[self.df["timestamp_cst"].dt.time >= dtime(8, 30)].groupby(["ticker", "date"])["open"].transform("first")
        )

        expected_adj = self.df["prev_market_close"] * self.df["split_ratio"] - self.df.get("cash_amount", 0.0)

        self.df["adj_pmkt_pct_chg"] = np.nan
        self.df["adj_mkt_pct_chg"] = np.nan
        self.df.loc[self.df["timestamp_cst"].dt.time < dtime(8, 30), "adj_pmkt_pct_chg"] = (
            self.df["open"] - expected_adj
        ) / expected_adj
        self.df.loc[self.df["timestamp_cst"].dt.time >= dtime(8, 30), "adj_mkt_pct_chg"] = (
            self.df["open"] - expected_adj
        ) / expected_adj

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
            valid = ~df[open_col].isna() & ~df[prev_close_col].isna() & ~df[expected_col].isna()
            diff_adj = np.abs(df[open_col] - df[expected_col])
            diff_prev = np.abs(df[open_col] - df[prev_close_col])
            rel_adj = diff_adj / np.maximum(np.abs(df[expected_col]), 1e-8)
            rel_prev = diff_prev / np.maximum(np.abs(df[prev_close_col]), 1e-8)
            state = np.full(df.shape[0], "unknown", dtype=object)

            # SPLIT: adjusted if much closer to adjusted than unadjusted
            closer_to_adj = (diff_prev - diff_adj) > (proximity_margin * np.abs(df[expected_col]))
            mask = valid & is_split & closer_to_adj
            state[mask] = "adjusted"

            # SPLIT: unadjusted if much closer to prev close
            closer_to_prev = (diff_adj - diff_prev) > (proximity_margin * np.abs(df[prev_close_col]))
            mask = valid & is_split & closer_to_prev & (state == "unknown")
            state[mask] = "unadjusted"

            # SPLIT: also allow for very close (tight tolerance) to adjusted
            mask = valid & is_split & ((diff_adj <= abs_tol) | (rel_adj <= rel_tol)) & (state == "unknown")
            state[mask] = "adjusted"

            # DIVIDEND: almost always unadjusted
            mask = valid & is_div & ((diff_prev <= abs_tol) | (rel_prev <= rel_tol))
            state[mask] = "unadjusted"
            return state

        self.df["pre_market_open_adj_state"] = assign_adj_state(self.df, "pre_market_open")
        self.df["market_open_adj_state"] = assign_adj_state(self.df, "market_open")

        # Robust: prefer market open, else pre-market, else unknown
        robust = np.where(
            self.df["market_open_adj_state"] != "unknown",
            self.df["market_open_adj_state"],
            np.where(self.df["pre_market_open_adj_state"] != "unknown", self.df["pre_market_open_adj_state"], "unknown"),
        )
        self.df["robust_open_adj_state"] = robust

        # Set NaN for no-event rows in all *_adj_state columns
        mask_no_event = (self.df["split_ratio"] == 1.0) & (self.df["cash_amount"] == 0.0)
        for col in ["pre_market_open_adj_state", "market_open_adj_state", "robust_open_adj_state"]:
            self.df.loc[mask_no_event, col] = np.nan

        self.df["adj_state"] = self.df["robust_open_adj_state"]

        keep_columns = [
            i
            for i in self.df.columns
            if i not in ["pre_market_open_adj_state", "market_open_adj_state", "robust_open_adj_state"]
        ]
        self.df = self.df[keep_columns]
        self.df = self.df.sort_values(by=["timestamp", "ticker"])
        return self.df
