import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("htb_backtest.log"), logging.StreamHandler()],
)


@dataclass
class Trade:
    """Individual trade record"""

    ticker: str
    trade_date: str
    entry_time: datetime
    entry_price: float
    exit_time: datetime
    exit_price: float
    position_size: int  # negative for short
    pnl: float
    pnl_pct: float
    exit_reason: str  # 'stop_loss', 'eod_close'
    overnight_move_pct: float
    prev_close: float


@dataclass
class BacktestConfig:
    """Backtest configuration"""

    data_directory: str = "~/polygon_data/bars_1min/"
    starting_balance: float = 100000.0
    bet_size: float = 2000.0
    max_loss: float = 4000.0
    overnight_threshold: float = 0.33  # 33%
    entry_cutoff_time: time = time(13, 45)  # 1:45 PM CST
    exit_time: time = time(14, 55)  # 2:55 PM CST
    timezone: str = "US/Central"


class HTBEquityBacktest:
    """Hard-to-Borrow Equity Shorting Backtest Engine"""

    def __init__(self, config: BacktestConfig):
        self.config = config
        self.tz = pytz.timezone(config.timezone)
        self.trades: List[Trade] = []
        self.daily_portfolio_values: List[Dict] = []
        self.current_balance = config.starting_balance
        self.data_dir = Path(os.path.expanduser(config.data_directory))

    def get_daily_files(self) -> List[Path]:
        """Get all daily data files sorted by date"""
        pattern = "*.csv.gz"
        files = list(self.data_dir.glob(pattern))
        files.sort()  # Assuming filename format allows chronological sorting
        logging.info(f"Found {len(files)} daily data files")
        return files

    def load_daily_data(self, file_path: Path) -> pd.DataFrame:
        """Load and process a single day's minute-level data"""
        try:
            # Load the compressed CSV
            df = pd.read_csv(file_path, compression="gzip")

            # Standardize column names (adjust based on your actual format)
            if "window_start" in df.columns:
                df = df.rename(columns={"window_start": "timestamp"})

            # Convert timestamp to datetime
            df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True)
            df["timestamp_cst"] = df["timestamp_utc"].dt.tz_convert(
                self.config.timezone
            )
            df["date"] = df["timestamp_cst"].dt.normalize()

            # Sort by time and ticker
            df = df.sort_values(["timestamp_cst", "ticker"])

            return df

        except Exception as e:
            logging.error(f"Error loading {file_path}: {e}")
            return pd.DataFrame()

    def get_previous_close_prices(self, prev_file: Path) -> Dict[str, float]:
        """Get closing prices from previous trading day"""
        if not prev_file.exists():
            return {}

        prev_data = self.load_daily_data(prev_file)
        if prev_data.empty:
            return {}

        # Get last price for each ticker (closing price)
        closing_prices = prev_data.groupby("ticker")["close"].last().to_dict()

        logging.info(
            f"Loaded {len(closing_prices)} closing prices from {prev_file.name}"
        )
        return closing_prices

    def calculate_overnight_moves(
        self, current_data: pd.DataFrame, prev_closes: Dict[str, float]
    ) -> Dict[str, Tuple[float, float]]:
        """Calculate overnight moves for all tickers"""
        overnight_moves = {}

        # Get first extended hours price for each ticker (after 3 AM CST)
        extended_hours_start = current_data["timestamp_cst"].iloc[
            0
        ].normalize() + pd.Timedelta(hours=3)
        extended_data = current_data[
            current_data["timestamp_cst"] >= extended_hours_start
        ]

        if extended_data.empty:
            return overnight_moves

        # Get first price after 3 AM for each ticker
        first_prices = extended_data.groupby("ticker").first()

        for ticker in first_prices.index:
            if ticker in prev_closes:
                prev_close = prev_closes[ticker]
                current_open = first_prices.loc[ticker, "open"]
                overnight_move = (current_open / prev_close) - 1

                overnight_moves[ticker] = (overnight_move, prev_close)

        return overnight_moves

    def find_entry_opportunities(
        self,
        current_data: pd.DataFrame,
        overnight_moves: Dict[str, Tuple[float, float]],
    ) -> List[Dict]:
        """Find tickers that meet entry criteria"""
        opportunities = []

        # Filter tickers with overnight move >= threshold
        qualifying_tickers = {
            ticker: (move, prev_close)
            for ticker, (move, prev_close) in overnight_moves.items()
            if move >= self.config.overnight_threshold
        }

        if not qualifying_tickers:
            return opportunities

        logging.info(
            f"Found {len(qualifying_tickers)} tickers with overnight move >= {self.config.overnight_threshold:.1%}"
        )

        # For each qualifying ticker, find entry point
        entry_cutoff = (
            current_data["timestamp_cst"]
            .iloc[0]
            .normalize()
            .replace(
                hour=self.config.entry_cutoff_time.hour,
                minute=self.config.entry_cutoff_time.minute,
            )
        )

        for ticker, (overnight_move, prev_close) in qualifying_tickers.items():
            ticker_data = current_data[
                (current_data["ticker"] == ticker)
                & (current_data["timestamp_cst"] <= entry_cutoff)
            ].copy()

            if ticker_data.empty:
                continue

            # Check if stock is still up >= 33% at any point before cutoff
            ticker_data["current_move"] = (ticker_data["high"] / prev_close) - 1
            qualifying_bars = ticker_data[
                ticker_data["current_move"] >= self.config.overnight_threshold
            ]

            if not qualifying_bars.empty:
                # Use the first qualifying bar's close as entry price
                entry_bar = qualifying_bars.iloc[0]

                opportunities.append(
                    {
                        "ticker": ticker,
                        "entry_time": entry_bar["timestamp_cst"],
                        "entry_price": entry_bar["close"],
                        "overnight_move": overnight_move,
                        "prev_close": prev_close,
                    }
                )

        return opportunities

    def execute_trade(
        self, opportunity: Dict, current_data: pd.DataFrame
    ) -> Optional[Trade]:
        """Execute a single trade from entry to exit"""
        ticker = opportunity["ticker"]
        entry_time = opportunity["entry_time"]
        entry_price = opportunity["entry_price"]
        overnight_move = opportunity["overnight_move"]
        prev_close = opportunity["prev_close"]

        # Calculate position size (negative for short)
        position_size = -int(self.config.bet_size / entry_price)

        if position_size == 0:
            return None

        # Get ticker data after entry time
        ticker_data = current_data[
            (current_data["ticker"] == ticker)
            & (current_data["timestamp_cst"] > entry_time)
        ].sort_values("timestamp_cst")

        if ticker_data.empty:
            return None

        # Define exit time for the day
        exit_time_target = entry_time.normalize().replace(
            hour=self.config.exit_time.hour, minute=self.config.exit_time.minute
        )

        # Monitor each minute for stop loss or exit time
        for idx, row in ticker_data.iterrows():
            current_time = row["timestamp_cst"]
            current_price = row["high"]  # Use high as worst case for short position

            # Calculate unrealized P&L
            unrealized_pnl = position_size * (current_price - entry_price)

            # Check stop loss (loss exceeds max_loss)
            if abs(unrealized_pnl) >= self.config.max_loss:
                final_pnl = position_size * (current_price - entry_price)

                trade = Trade(
                    ticker=ticker,
                    trade_date=entry_time.strftime("%Y-%m-%d"),
                    entry_time=entry_time,
                    entry_price=entry_price,
                    exit_time=current_time,
                    exit_price=current_price,
                    position_size=position_size,
                    pnl=final_pnl,
                    pnl_pct=final_pnl / self.config.bet_size,
                    exit_reason="stop_loss",
                    overnight_move_pct=overnight_move,
                    prev_close=prev_close,
                )

                self.current_balance += final_pnl
                return trade

            # Check if we've reached exit time
            if current_time >= exit_time_target:
                exit_price = row["close"]
                final_pnl = position_size * (exit_price - entry_price)

                trade = Trade(
                    ticker=ticker,
                    trade_date=entry_time.strftime("%Y-%m-%d"),
                    entry_time=entry_time,
                    entry_price=entry_price,
                    exit_time=current_time,
                    exit_price=exit_price,
                    position_size=position_size,
                    pnl=final_pnl,
                    pnl_pct=final_pnl / self.config.bet_size,
                    exit_reason="eod_close",
                    overnight_move_pct=overnight_move,
                    prev_close=prev_close,
                )

                self.current_balance += final_pnl
                return trade

        # If we reach here, exit at last available price
        if not ticker_data.empty:
            last_row = ticker_data.iloc[-1]
            exit_price = last_row["close"]
            final_pnl = position_size * (exit_price - entry_price)

            trade = Trade(
                ticker=ticker,
                trade_date=entry_time.strftime("%Y-%m-%d"),
                entry_time=entry_time,
                entry_price=entry_price,
                exit_time=last_row["timestamp_cst"],
                exit_price=exit_price,
                position_size=position_size,
                pnl=final_pnl,
                pnl_pct=final_pnl / self.config.bet_size,
                exit_reason="eod_close",
                overnight_move_pct=overnight_move,
                prev_close=prev_close,
            )

            self.current_balance += final_pnl
            return trade

        return None

    def run_backtest(self) -> Dict:
        """Run the complete backtest"""
        logging.info("Starting HTB Equity Backtest")

        daily_files = self.get_daily_files()

        if len(daily_files) < 2:
            logging.error("Need at least 2 daily files to run backtest")
            return {}

        # Process each day
        for i in range(1, len(daily_files)):
            current_file = daily_files[i]
            prev_file = daily_files[i - 1]

            logging.info(
                f"Processing day {i}/{len(daily_files) - 1}: {current_file.name}"
            )

            # Load data
            current_data = self.load_daily_data(current_file)
            if current_data.empty:
                continue

            prev_closes = self.get_previous_close_prices(prev_file)
            if not prev_closes:
                continue

            # Calculate overnight moves
            overnight_moves = self.calculate_overnight_moves(current_data, prev_closes)

            # Find entry opportunities
            opportunities = self.find_entry_opportunities(current_data, overnight_moves)

            daily_trades = []
            daily_pnl = 0

            # Execute trades
            for opportunity in opportunities:
                # Check if we have sufficient balance
                if self.current_balance < self.config.bet_size:
                    logging.warning("Insufficient balance for new trade")
                    continue

                trade = self.execute_trade(opportunity, current_data)

                if trade:
                    self.trades.append(trade)
                    daily_trades.append(trade)
                    daily_pnl += trade.pnl

                    logging.info(
                        f"Trade: {trade.ticker} | Entry: ${trade.entry_price:.2f} | "
                        f"Exit: ${trade.exit_price:.2f} | P&L: ${trade.pnl:.2f} | "
                        f"Reason: {trade.exit_reason}"
                    )

            # Record daily performance
            trade_date = current_data["date"].iloc[0].strftime("%Y-%m-%d")
            self.daily_portfolio_values.append(
                {
                    "date": trade_date,
                    "balance": self.current_balance,
                    "daily_pnl": daily_pnl,
                    "trades_count": len(daily_trades),
                    "opportunities": len(opportunities),
                }
            )

            if daily_trades:
                logging.info(
                    f"Day {trade_date}: {len(daily_trades)} trades, P&L: ${daily_pnl:.2f}"
                )

        logging.info(f"Backtest completed. Total trades: {len(self.trades)}")
        return self.generate_performance_report()

    def generate_performance_report(self) -> Dict:
        """Generate comprehensive performance report"""
        if not self.trades:
            return {"error": "No trades executed"}

        trades_df = pd.DataFrame(
            [
                {
                    "ticker": trade.ticker,
                    "trade_date": trade.trade_date,
                    "entry_time": trade.entry_time,
                    "entry_price": trade.entry_price,
                    "exit_time": trade.exit_time,
                    "exit_price": trade.exit_price,
                    "pnl": trade.pnl,
                    "pnl_pct": trade.pnl_pct,
                    "exit_reason": trade.exit_reason,
                    "overnight_move_pct": trade.overnight_move_pct,
                    "prev_close": trade.prev_close,
                }
                for trade in self.trades
            ]
        )

        portfolio_df = pd.DataFrame(self.daily_portfolio_values)

        # Calculate performance metrics
        total_pnl = trades_df["pnl"].sum()
        total_return = (self.current_balance / self.config.starting_balance) - 1

        winning_trades = trades_df[trades_df["pnl"] > 0]
        losing_trades = trades_df[trades_df["pnl"] < 0]

        win_rate = len(winning_trades) / len(trades_df) if len(trades_df) > 0 else 0
        avg_win = winning_trades["pnl"].mean() if len(winning_trades) > 0 else 0
        avg_loss = losing_trades["pnl"].mean() if len(losing_trades) > 0 else 0

        # Calculate drawdown
        portfolio_df["cumulative_pnl"] = portfolio_df["daily_pnl"].cumsum()
        portfolio_df["peak"] = portfolio_df["cumulative_pnl"].expanding().max()
        portfolio_df["drawdown"] = portfolio_df["cumulative_pnl"] - portfolio_df["peak"]
        max_drawdown = portfolio_df["drawdown"].min()

        # Calculate Sharpe ratio
        if len(portfolio_df) > 1:
            daily_returns = portfolio_df["daily_pnl"] / self.config.starting_balance
            sharpe_ratio = (
                np.sqrt(252) * daily_returns.mean() / daily_returns.std()
                if daily_returns.std() > 0
                else 0
            )
        else:
            sharpe_ratio = 0

        performance_report = {
            "summary": {
                "total_trades": len(self.trades),
                "total_pnl": total_pnl,
                "total_return_pct": total_return * 100,
                "starting_balance": self.config.starting_balance,
                "ending_balance": self.current_balance,
                "win_rate": win_rate * 100,
                "avg_win": avg_win,
                "avg_loss": avg_loss,
                "profit_factor": (
                    abs(avg_win * len(winning_trades) / (avg_loss * len(losing_trades)))
                    if avg_loss != 0 and len(losing_trades) > 0
                    else float("inf")
                ),
                "max_drawdown": max_drawdown,
                "sharpe_ratio": sharpe_ratio,
                "total_days_traded": len(portfolio_df),
                "avg_trades_per_day": (
                    len(self.trades) / len(portfolio_df) if len(portfolio_df) > 0 else 0
                ),
            },
            "exit_reasons": trades_df["exit_reason"].value_counts().to_dict(),
            "top_performers": trades_df.nlargest(10, "pnl")[
                ["ticker", "trade_date", "pnl", "overnight_move_pct"]
            ].to_dict("records"),
            "worst_performers": trades_df.nsmallest(10, "pnl")[
                ["ticker", "trade_date", "pnl", "overnight_move_pct"]
            ].to_dict("records"),
            "trades_data": trades_df.to_dict("records"),
            "daily_performance": portfolio_df.to_dict("records"),
        }

        return performance_report

    def save_results(self, results: Dict, filename: str = None):
        """Save backtest results to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"htb_backtest_results_{timestamp}.json"

        def serialize_datetime(obj):
            if isinstance(obj, (datetime, pd.Timestamp)):
                return obj.isoformat()
            elif hasattr(obj, "isoformat"):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        with open(filename, "w") as f:
            json.dump(results, f, indent=2, default=serialize_datetime)

        logging.info(f"Results saved to {filename}")


def main():
    """Main execution function"""

    # Configuration
    config = BacktestConfig(
        data_directory="~/polygon_data/bars_1min/",  # Your actual data directory
        starting_balance=100000.0,
        bet_size=2000.0,
        max_loss=4000.0,
        overnight_threshold=0.33,
    )

    # Initialize and run backtest
    backtest = HTBEquityBacktest(config)
    results = backtest.run_backtest()

    if results and "summary" in results:
        # Print summary
        summary = results["summary"]
        print("\n" + "=" * 60)
        print("HTB EQUITY SHORTING BACKTEST RESULTS")
        print("=" * 60)
        print(f"Total Days Traded: {summary['total_days_traded']}")
        print(f"Total Trades: {summary['total_trades']}")
        print(f"Avg Trades/Day: {summary['avg_trades_per_day']:.1f}")
        print(f"Total P&L: ${summary['total_pnl']:,.2f}")
        print(f"Total Return: {summary['total_return_pct']:.2f}%")
        print(f"Win Rate: {summary['win_rate']:.1f}%")
        print(f"Average Win: ${summary['avg_win']:,.2f}")
        print(f"Average Loss: ${summary['avg_loss']:,.2f}")
        print(f"Profit Factor: {summary['profit_factor']:.2f}")
        print(f"Max Drawdown: ${summary['max_drawdown']:,.2f}")
        print(f"Sharpe Ratio: {summary['sharpe_ratio']:.2f}")
        print("=" * 60)

        # Print exit reasons
        print("\nExit Reasons:")
        for reason, count in results["exit_reasons"].items():
            print(f"  {reason}: {count}")

        # Save results
        backtest.save_results(results)
    else:
        print("No results generated - check logs for errors")


if __name__ == "__main__":
    main()
