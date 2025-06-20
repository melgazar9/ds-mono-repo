import os
import asyncio
from pathlib import Path
from bt_engine.central_loaders import PolygonBarLoader, StreamingFileProcessor
from bt_engine.engine import BacktestEngine
from datetime import time as dtime, datetime
import logging

from gap_backtest_utils import (
    GapStrategyRiskManager,
    GapPositionManager,
    GapStrategyEvaluator,
)

DEBUG = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class GapBacktestRunner(BacktestEngine):
    # Class-level shared corporate actions data
    _df_corporate_actions = None

    def __init__(self):
        super().__init__(
            data_loader=None,  # set dynamically
            risk_manager=GapStrategyRiskManager(),
            position_manager=GapPositionManager(
                overnight_gap=0.33,
                bet_amount=2000,
                stop_loss_pct=1.0,
                take_profit_pct=1.0,
                entry_cutoff_time_cst=dtime(13, 45),
                flatten_trade_time_cst=dtime(14, 55),
                slippage_amount=0.61,
                slippage_mode="partway",
            ),
            strategy_evaluator=GapStrategyEvaluator(
                segment_cols=["market", "vix_bucket"]
            ),
        )

        self.processor = StreamingFileProcessor(
            os.getenv("NORDVPN_MESHNET_IP"),
            os.getenv("NORDVPN_USER"),
            max_concurrent_downloads=3,
            max_cached_files=10,
        )
        self.run_timestamp = datetime.now().strftime("%Y-%m-%d__%H-%M-%S")
        self.results_dir = Path.home() / "gap_backtrade_results"
        self.results_dir.mkdir(exist_ok=True)

    def setup_data_loader(self, file_path: str, df_prev=None):
        """Set up the data loader with current file and previous data"""
        self.data_loader = PolygonBarLoader(
            cur_day_file=file_path, load_method="pandas", df_prev=df_prev
        )

        if GapBacktestRunner._df_corporate_actions is not None:
            self.data_loader.df_corporate_actions = (
                GapBacktestRunner._df_corporate_actions
            )
            logging.info("✅ Reusing shared corporate actions data")
        else:
            self.data_loader.pull_cached_corporate_actions()
            GapBacktestRunner._df_corporate_actions = (
                self.data_loader.df_corporate_actions
            )
            logging.info("✅ Corporate actions loaded and cached for reuse")

    async def run_backtest_sequence(self):
        """Run the complete backtest sequence using streaming files"""
        remote_files = await self.processor.list_remote_files(
            "/home/melgazar9/polygon_data/us_stocks_sip/bars_1m", "*.csv.gz"
        )

        logging.info(f"Found {len(remote_files)} files to process")

        if DEBUG:
            remote_files = remote_files[0:30]

        simplified_results_file = (
            self.results_dir / f"simplified_summary__{self.run_timestamp}.csv"
        )
        results_file_by_ticker = (
            self.results_dir / f"summary_by_ticker__{self.run_timestamp}.csv"
        )
        results_file_by_segment = (
            self.results_dir / f"summary_by_segment__{self.run_timestamp}.csv"
        )
        results_file_by_ticker_and_segment = (
            self.results_dir
            / f"summary_by_ticker_and_segment__{self.run_timestamp}.csv"
        )

        header = True
        i = 0
        df_prev = None

        async for local_file_path in self.processor.get_files(remote_files):
            logging.info(
                f"Processing file {i + 1}/{len(remote_files)}: {local_file_path.name}"
            )

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

            # Save results using inherited strategy_evaluator
            self.strategy_evaluator.simplified_daily_summary.to_frame().T.to_csv(
                simplified_results_file,
                index=False,
                header=header,
                mode="a",
            )

            self.strategy_evaluator.daily_summary_by_ticker.to_csv(
                results_file_by_ticker,
                index=False,
                header=header,
                mode="a",
            )

            self.strategy_evaluator.daily_summary_with_segments.to_csv(
                results_file_by_segment,
                index=False,
                header=header,
                mode="a",
            )

            self.strategy_evaluator.daily_summary_by_ticker_and_segment.to_csv(
                results_file_by_ticker_and_segment,
                index=False,
                header=header,
                mode="a",
            )

            # Set df_prev for next iteration using inherited df attribute
            df_prev = self.df
            header = False
            logging.info(f"✅ Completed processing {local_file_path.name}")
            i += 1

        logging.info(
            f"""
            Backtest complete! Results saved to {self.results_dir}.
            Files:
            - Simplified Results: {simplified_results_file}
            - Results by Ticker: {results_file_by_ticker}
            - Results by Segment: {results_file_by_segment}
            - Results by Ticker and Segment: {results_file_by_ticker_and_segment}
            """
        )


async def main():
    """Main entry point"""
    runner = GapBacktestRunner()
    await runner.run_backtest_sequence()


if __name__ == "__main__":
    # 1. If gap from previous day close to either pre-market open or market open is >= X% (adjusted) then trigger trade entry
    # 2. Iterate over different stop and take loss percentages
    # 3. Do not enter trade after 13:45 CST
    # 4. All positions must be flattened by 14:55 CST
    # 5. Segment by groups --> HTB / short interest/volume buckets, market cap bucket, industry, VIX range, etc.

    asyncio.run(main())
