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


class GapBacktestRunner:
    def __init__(self):
        self.processor = StreamingFileProcessor(
            os.getenv("NORDVPN_MESHNET_IP"),
            os.getenv("NORDVPN_USER"),
            max_concurrent_downloads=3,
            max_cached_files=10,
        )
        self.run_timestamp = datetime.now().strftime("%Y-%m-%d__%H-%M-%S")
        self.results_dir = Path.home() / "gap_backtrade_results"
        self.results_dir.mkdir(exist_ok=True)

        self.data_loader = None

    @staticmethod
    def create_backtest_engine(data_loader):
        """Create a backtest engine with standard parameters"""
        return BacktestEngine(
            data_loader=data_loader,
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
            strategy_evaluator=GapStrategyEvaluator(),
        )

    async def run_backtest_sequence(self):
        """Run the complete backtest sequence using streaming files"""
        remote_files = await self.processor.list_remote_files(
            "/home/melgazar9/polygon_data/us_stocks_sip/bars_1m", "*.csv.gz"
        )

        logging.info(f"Found {len(remote_files)} files to process")

        if DEBUG:
            remote_files = remote_files[0:30]

        # Setup results file
        results_file = self.results_dir / f"summary_{self.run_timestamp}.csv"
        header = True

        # Manual counter since we can't use enumerate() with async generators
        i = 0

        # Use the streaming file processor to get files as they're downloaded
        async for local_file_path in self.processor.get_files(remote_files):
            logging.info(
                f"Processing file {i + 1}/{len(remote_files)}: {local_file_path.name}"
            )

            try:
                if i == 0:
                    logging.info(f"Loading initial data from {local_file_path.name}")
                    self.data_loader = PolygonBarLoader(
                        cur_day_file=str(local_file_path), load_method="pandas"
                    )
                    df_prev = self.data_loader.load_raw_intraday_bars()

                    self.data_loader.pull_splits_dividends()
                    logging.info(
                        "✅ Splits/dividends data loaded once - will be reused!"
                    )

                    # Set df_prev for next iteration
                    self.data_loader.df_prev = df_prev

                    logging.info(f"Initial data loaded: {df_prev.shape[0]} rows")
                    i += 1
                    continue

                logging.info(f"Reusing data loader for {local_file_path.name}")
                self.data_loader.cur_day_file = str(local_file_path)

                backtest_engine = self.create_backtest_engine(self.data_loader)
                backtest_engine.run_backtest()

                # Save results
                backtest_engine.df_evaluation.to_frame().T.to_csv(
                    results_file,
                    index=False,
                    header=header,
                    mode="a",
                )

                self.data_loader.df_prev = backtest_engine.df
                header = False

                logging.info(f"✅ Completed processing {local_file_path.name}")

            except Exception as e:
                logging.error(f"❌ Error processing {local_file_path.name}: {e}")

            i += 1

        logging.info(f"Backtest complete! Results saved to {results_file}")


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
