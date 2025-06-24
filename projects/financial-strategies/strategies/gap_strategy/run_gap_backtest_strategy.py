import os
import asyncio
from pathlib import Path
from bt_engine.central_loaders import PolygonBarLoader, StreamingFileProcessor
from bt_engine.engine import BacktestEngine
from datetime import time as dtime, datetime
import logging
from typing import Optional, List, Dict
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, Future
import shutil
from itertools import product
from dataclasses import dataclass

from gap_backtest_utils import GapStrategyRiskManager, GapStrategyEvaluator, GapPositionManager

stop_vals = [0.5, 1.0]
tp_vals = [1.0, 0.5]
gap_vals = [0.2, 0.33]
PARAM_GRID = list(product(stop_vals, tp_vals, gap_vals))
BASE_RESULTS_DIR = Path.home() / "gap_backtrade_results"

DEBUG = True

REMOVE_LOW_QUALITY_TICKERS = False


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)


@dataclass
class BacktestParams:
    """Serializable container for backtest parameters"""

    overnight_gap: float = 0.33
    bet_amount: int = 2000
    stop_loss_pct: float = 1.0
    take_profit_pct: float = 1.0
    entry_cutoff_time_cst: tuple = (13, 45)  # (hour, minute) - serializable
    flatten_trade_time_cst: tuple = (14, 55)
    slippage_amount: float = 0.61
    slippage_mode: str = "partway"
    segment_cols: List[str] = None
    num_workers: int = 1

    def __post_init__(self):
        if self.segment_cols is None:
            self.segment_cols = ["market", "vix_bucket", "daily_volume_bucket"]


def run_single_backtest_worker(
    current_file_path: str, previous_file_path: Optional[str], params: BacktestParams, corporate_actions_data: pd.DataFrame
) -> dict:
    """
    Worker function that runs in a separate process.
    Takes only serializable inputs and returns serializable outputs.
    """
    try:
        logging.info(f"Worker PID {os.getpid()}: Processing {Path(current_file_path).name}")

        # Load previous data if available
        df_prev = None
        if previous_file_path and os.path.exists(previous_file_path):
            logging.info(f"Worker PID {os.getpid()}: Loading previous data from {Path(previous_file_path).name}")
            temp_loader = PolygonBarLoader(
                cur_day_file=previous_file_path,
                load_method="pandas",
                df_prev=None,
                remove_low_quality_tickers=REMOVE_LOW_QUALITY_TICKERS,
            )
            temp_loader.df_corporate_actions = corporate_actions_data
            df_prev = temp_loader.load_raw_intraday_bars()

        # Create runner with parameters
        runner = BacktestEngine(
            data_loader=None,  # set below
            risk_manager=GapStrategyRiskManager(),
            position_manager=GapPositionManager(
                overnight_gap=params.overnight_gap,
                bet_amount=params.bet_amount,
                stop_loss_pct=params.stop_loss_pct,
                take_profit_pct=params.take_profit_pct,
                entry_cutoff_time_cst=dtime(*params.entry_cutoff_time_cst),
                flatten_trade_time_cst=dtime(*params.flatten_trade_time_cst),
                slippage_amount=params.slippage_amount,
                slippage_mode=params.slippage_mode,
            ),
            strategy_evaluator=GapStrategyEvaluator(segment_cols=params.segment_cols),
        )

        # Setup data loader
        runner.data_loader = PolygonBarLoader(cur_day_file=current_file_path, load_method="pandas", df_prev=df_prev)

        # Use pre-loaded corporate actions
        runner.data_loader.df_corporate_actions = corporate_actions_data
        logging.info(f"Worker PID {os.getpid()}: Using pre-loaded corporate actions data")

        runner.run_backtest()

        results = {
            "simplified": runner.strategy_evaluator.simplified_daily_summary.to_frame().T,
            "by_ticker": runner.strategy_evaluator.daily_summary_by_ticker,
            "by_segment": runner.strategy_evaluator.daily_summary_with_segments,
            "by_ticker_and_segment": runner.strategy_evaluator.daily_summary_by_ticker_and_segment,
        }

        logging.info(f"Worker PID {os.getpid()}: ✅ Completed {Path(current_file_path).name}")
        return results

    except Exception as e:
        logging.error(f"Worker PID {os.getpid()}: Error processing {current_file_path}: {e}")
        raise


class GapBacktestRunner(BacktestEngine):
    # Class-level shared corporate actions data
    _df_corporate_actions = None

    def __init__(self, params: BacktestParams, results_dir: Path = None):
        self.params = params
        self.run_timestamp = datetime.now().strftime("%Y-%m-%d__%H-%M-%S")
        self.results_dir = results_dir or (Path.home() / "gap_backtrade_results")
        self.results_dir.mkdir(exist_ok=True)
        super().__init__(
            data_loader=None,
            risk_manager=GapStrategyRiskManager(),
            position_manager=GapPositionManager(
                overnight_gap=params.overnight_gap,
                bet_amount=params.bet_amount,
                stop_loss_pct=params.stop_loss_pct,
                take_profit_pct=params.take_profit_pct,
                entry_cutoff_time_cst=dtime(*params.entry_cutoff_time_cst),
                flatten_trade_time_cst=dtime(*params.flatten_trade_time_cst),
                slippage_amount=params.slippage_amount,
                slippage_mode=params.slippage_mode,
            ),
            strategy_evaluator=GapStrategyEvaluator(segment_cols=params.segment_cols),
        )

        num_workers = params.num_workers
        max_cached_files = 30
        if num_workers > max_cached_files:
            raise ValueError(
                f"num_workers ({num_workers}) cannot be greater than max_cached_files ({max_cached_files}). "
                f"You can't parallelize more jobs than files that are cached."
            )

        self.num_workers = num_workers
        self.max_cached_files = max_cached_files
        self.processor = StreamingFileProcessor(
            os.getenv("NORDVPN_MESHNET_IP"),
            os.getenv("NORDVPN_USER"),
            max_concurrent_downloads=6,
            max_cached_files=max_cached_files,
        )
        self.run_timestamp = datetime.now().strftime("%Y-%m-%d__%H-%M-%S")

        # Create a protected directory for files that are being processed
        self.processing_dir = self.results_dir / f"processing_{self.run_timestamp}"
        self.processing_dir.mkdir(exist_ok=True)

    def _load_corporate_actions(self) -> pd.DataFrame:
        """Load corporate actions data once and cache it"""
        if GapBacktestRunner._df_corporate_actions is None:
            logging.info("🔄 Loading corporate actions data once for all workers...")
            temp_loader = PolygonBarLoader(
                cur_day_file="dummy", load_method="pandas", remove_low_quality_tickers=REMOVE_LOW_QUALITY_TICKERS
            )
            temp_loader.pull_cached_corporate_actions()
            GapBacktestRunner._df_corporate_actions = temp_loader.df_corporate_actions
            logging.info("✅ Corporate actions data loaded and cached for all workers")
        else:
            logging.info("✅ Reusing cached corporate actions data")
        return GapBacktestRunner._df_corporate_actions

    def setup_data_loader(self, file_path: str, df_prev=None):
        """Set up the data loader with current file and previous data"""
        self.data_loader = PolygonBarLoader(
            cur_day_file=file_path, load_method="pandas", df_prev=df_prev, remove_low_quality_tickers=REMOVE_LOW_QUALITY_TICKERS
        )

        if GapBacktestRunner._df_corporate_actions is not None:
            self.data_loader.df_corporate_actions = GapBacktestRunner._df_corporate_actions
            logging.info("✅ Reusing shared corporate actions data")
        else:
            self.data_loader.pull_cached_corporate_actions()
            GapBacktestRunner._df_corporate_actions = self.data_loader.df_corporate_actions
            logging.info("✅ Corporate actions loaded and cached for reuse")

    def _get_result_file_paths(self) -> Dict[str, Path]:
        """Centralized result file path generation"""
        return {
            "simplified": self.results_dir / f"simplified_summary__{self.run_timestamp}.csv",
            "by_ticker": self.results_dir / f"summary_by_ticker__{self.run_timestamp}.csv",
            "by_segment": self.results_dir / f"summary_by_segment__{self.run_timestamp}.csv",
            "by_ticker_and_segment": self.results_dir / f"summary_by_ticker_and_segment__{self.run_timestamp}.csv",
        }

    def _save_results_to_files(self, result_files: Dict[str, Path], header: bool = False):
        """Centralized result saving logic"""
        self.strategy_evaluator.simplified_daily_summary.to_frame().T.to_csv(
            result_files["simplified"], index=False, header=header, mode="a"
        )

        self.strategy_evaluator.daily_summary_by_ticker.to_csv(result_files["by_ticker"], index=False, header=header, mode="a")

        self.strategy_evaluator.daily_summary_with_segments.to_csv(
            result_files["by_segment"], index=False, header=header, mode="a"
        )

        self.strategy_evaluator.daily_summary_by_ticker_and_segment.to_csv(
            result_files["by_ticker_and_segment"], index=False, header=header, mode="a"
        )

    def _save_results(self, all_results: List[dict], result_files: Dict[str, Path]):
        """Centralized result saving logic - combines all results and saves once"""
        combined_results = {"simplified": [], "by_ticker": [], "by_segment": [], "by_ticker_and_segment": []}

        # Combine all results
        for result in all_results:
            for key, df in result.items():
                if not df.empty:
                    combined_results[key].append(df)

        # Save combined results
        for key, df_list in combined_results.items():
            if df_list:
                combined_df = pd.concat(df_list, ignore_index=True)
                combined_df.to_csv(result_files[key], index=False)
                logging.info(f"✅ Saved {len(combined_df)} rows to {result_files[key]}")

    def _process_completed_futures(
        self,
        completed_futures: List[Future],
        active_futures: Dict[Future, int],
        all_results: List[dict],
        protected_files: Dict[int, Path],
    ) -> None:
        """
        DRY helper method to process completed futures and clean up files.
        This eliminates the duplicate code that PyCharm was complaining about.
        """
        for future in completed_futures:
            completed_file_index = active_futures.pop(future)
            try:
                result = future.result()
                all_results.append(result)
                logging.info(f"✅ Collected result for file {completed_file_index + 1}")

                # Clean up old protected files (keep only what we need)
                self._cleanup_old_protected_file(completed_file_index, protected_files, active_futures)

            except Exception as e:
                logging.error(f"❌ Error in file {completed_file_index + 1}: {e}")

    def _cleanup_old_protected_file(
        self, completed_file_index: int, protected_files: Dict[int, Path], active_futures: Dict[Future, int]
    ) -> None:
        """
        DRY helper method to clean up old protected files.
        Only deletes files that are no longer needed by any active workers.
        """
        cleanup_index = completed_file_index - 1
        if cleanup_index in protected_files and cleanup_index > 0:
            # Check if any active workers still need this file
            still_needed = any(other_index - 1 == cleanup_index for other_index in active_futures.values())

            if not still_needed:
                old_file = protected_files.pop(cleanup_index)
                if old_file.exists():
                    old_file.unlink()
                    logging.info(f"🗑️ Cleaned up protected file: {old_file.name}")

    async def run_backtest_sequence(self):
        """Run the complete backtest sequence using streaming files"""
        remote_files = await self.processor.list_remote_files("/home/melgazar9/polygon_data/us_stocks_sip/bars_1m", "*.csv.gz")

        logging.info(f"Found {len(remote_files)} files to process")

        if DEBUG:
            filtered_dates = [
                path
                for path in remote_files
                if datetime.strptime("2025-05-02", "%Y-%m-%d")
                <= datetime.strptime(path.split("_")[-1].split(".")[0], "%Y-%m-%d")
                <= datetime.strptime("2025-06-20", "%Y-%m-%d")
            ]
            remote_files = sorted(filtered_dates)

        result_files = self._get_result_file_paths()

        try:
            if self.num_workers == 1:
                await self._run_sequential(remote_files, result_files)
            else:
                await self._run_parallel_with_file_protection(remote_files, result_files)

            self._log_completion_message(result_files)
        finally:
            # Clean up processing directory
            if self.processing_dir.exists():
                shutil.rmtree(self.processing_dir)
                logging.info("🗑️ Cleaned up processing directory")

    async def _run_sequential(self, remote_files: List[str], result_files: Dict[str, Path]):
        """Run backtest sequentially (ORIGINAL BEHAVIOR - EXACT SAME LOGIC)"""
        header = True
        i = 0
        df_prev = None

        async for local_file_path in self.processor.get_files(remote_files):
            logging.info(f"Processing file {i + 1}/{len(remote_files)}: {local_file_path.name}")

            if i == 0:
                logging.info(f"Loading initial data from {local_file_path.name}")
                self.setup_data_loader(str(local_file_path))
                df_prev = self.data_loader.load_raw_intraday_bars()
                logging.info(f"Initial data loaded: {df_prev.shape[0]} rows")
                i += 1
                continue

            logging.info(f"Processing backtest for {local_file_path.name}")
            self.setup_data_loader(str(local_file_path), df_prev)

            # Run backtest using inherited methods
            self.run_backtest()

            # Save results using centralized method
            self._save_results_to_files(result_files, header)

            # Set df_prev for next iteration using inherited df attribute
            df_prev = self.df
            header = False
            logging.info(f"✅ Completed processing {local_file_path.name}")
            i += 1

    async def _run_parallel_with_file_protection(self, remote_files: List[str], result_files: Dict[str, Path]):
        """
        Run backtest in parallel by immediately copying files to protected directory
        before they can be deleted by the StreamingFileProcessor
        """
        corporate_actions_data = self._load_corporate_actions()
        logging.info(f"🚀 Starting parallel processing with {self.num_workers} workers")
        logging.info(f"📦 Respecting max_cached_files={self.max_cached_files} with file protection")

        all_results = []
        protected_files: Dict[int, Path] = {}

        loop = asyncio.get_event_loop()
        params = self.params

        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            file_index = 0
            active_futures: Dict[Future, int] = {}

            # Process files as they stream in with protection
            async for local_file_path in self.processor.get_files(remote_files):
                logging.info(f"📥 Received file {file_index + 1}/{len(remote_files)}: {local_file_path.name}")

                # Immediately protect file from deletion
                protected_path = self.processing_dir / local_file_path.name
                shutil.copy2(local_file_path, protected_path)
                protected_files[file_index] = protected_path
                logging.info(f"🛡️ Protected file: {protected_path.name}")

                # Skip first file (just for initial data loading context)
                if file_index == 0:
                    file_index += 1
                    continue

                # Submit file for processing if previous file is available
                if file_index - 1 in protected_files and file_index not in active_futures.values():
                    current_file = str(protected_files[file_index])
                    previous_file = str(protected_files[file_index - 1])

                    future = loop.run_in_executor(
                        executor, run_single_backtest_worker, current_file, previous_file, params, corporate_actions_data
                    )
                    active_futures[future] = file_index
                    logging.info(f"🔄 Submitted file {file_index + 1} for processing")

                # Process any immediately completed futures
                completed_futures = [f for f in active_futures.keys() if f.done()]
                self._process_completed_futures(completed_futures, active_futures, all_results, protected_files)

                # Wait if we have too many active workers
                while len(active_futures) >= self.num_workers:
                    done, pending = await asyncio.wait(active_futures.keys(), return_when=asyncio.FIRST_COMPLETED, timeout=0.1)

                    # Process completed futures using DRY method
                    self._process_completed_futures(list(done), active_futures, all_results, protected_files)

                file_index += 1

            # Wait for remaining futures to complete
            if active_futures:
                logging.info(f"⏳ Waiting for {len(active_futures)} remaining workers to complete...")
                remaining_results = await asyncio.gather(*active_futures.keys(), return_exceptions=True)

                for result in remaining_results:
                    if isinstance(result, dict):  # Valid result
                        all_results.append(result)
                    else:
                        logging.error(f"❌ Error in remaining worker: {result}")

        logging.info(f"💾 Saving combined results from {len(all_results)} processed files...")
        self._save_results(all_results, result_files)

    def _log_completion_message(self, result_files: Dict[str, Path]):
        """Centralized completion message logging"""
        logging.info(
            f"""
            Backtest complete! Results saved to {self.results_dir}.
            Files:
            - Simplified Results: {result_files['simplified']}
            - Results by Ticker: {result_files['by_ticker']}
            - Results by Segment: {result_files['by_segment']}
            - Results by Ticker and Segment: {result_files['by_ticker_and_segment']}
            """
        )


async def main(params: BacktestParams, results_dir: Path):
    """Main entry point"""
    runner = GapBacktestRunner(params=params, results_dir=results_dir)
    await runner.run_backtest_sequence()


if __name__ == "__main__":
    for stop, tp, gap in PARAM_GRID:
        ts = datetime.now().strftime("%Y-%m-%d__%H-%M-%S")
        result_dir = BASE_RESULTS_DIR / f"{ts}__stop_{stop}__tp_{tp}__gap_{gap}"
        result_dir.mkdir(parents=True, exist_ok=True)
        params = BacktestParams(
            stop_loss_pct=stop,
            take_profit_pct=tp,
            overnight_gap=gap,
            num_workers=20,
        )
        print(f"Running for stop={stop}, tp={tp}, gap={gap} in {result_dir}")
        asyncio.run(main(params, result_dir))
