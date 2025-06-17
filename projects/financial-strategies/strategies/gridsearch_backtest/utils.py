import json
import logging
import os
import warnings
from dataclasses import dataclass
from datetime import datetime
from datetime import time
from datetime import time as dtime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pytz

# Import your database connector
from ds_core.db_connectors import PostgresConnect

warnings.filterwarnings("ignore")

# Setup logging
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
    adj_overnight_move_pct: float  # Split/dividend adjusted
    prev_close: float
    had_split_or_dividend: bool
    split_ratio: float
    cash_amount: float


@dataclass
class BacktestConfig:
    """Backtest configuration"""

    data_directory: str = "~/polygon_data/bars_1min/"
    starting_balance: float = 100000.0
    bet_size: float = 2000.0
    max_loss: float = 3000.0
    overnight_threshold: float = 0.38  # 33%
    entry_cutoff_time: time = time(13, 45)  # 1:45 PM CST
    exit_time: time = time(14, 55)  # 2:55 PM CST
    timezone: str = "America/New_York"  # Changed to match your original code
    abs_tol: float = 0.02  # For split/dividend detection
    rel_tol: float = 0.002  # For split/dividend detection


class HTBEquityBacktest:
    """Hard-to-Borrow Equity Shorting Backtest Engine with Split/Dividend Adjustment"""

    def __init__(self, config: BacktestConfig):
        self.config = config
        self.tz = pytz.timezone(config.timezone)
        self.trades: List[Trade] = []
        self.daily_portfolio_values: List[Dict] = []
        self.current_balance = config.starting_balance
        self.data_dir = Path(os.path.expanduser(config.data_directory))
        self.df_splits_dividends = None

    def pull_splits_dividends(self):
        """Pull splits and dividends data from database"""
        logging.info("Loading splits and dividends data from database")
        try:
            with PostgresConnect(database="financial_elt") as db:
                self.df_splits_dividends = db.run_sql(
                    """
                    WITH cte_dividends AS (
                        SELECT
                            ticker,
                            ex_dividend_date,
                            SUM(cash_amount) AS cash_amount
                       FROM tap_polygon_production.dividends
                       WHERE UPPER(currency) = 'USD'
                       GROUP BY 1, 2)

                    SELECT COALESCE(s.ticker, d.ticker)                   AS ticker,
                           COALESCE(s.execution_date, d.ex_dividend_date) AS event_date,
                           s.split_from,
                           s.split_to,
                           d.cash_amount,
                           CASE
                               WHEN s.ticker IS NOT NULL AND d.ticker IS NOT NULL THEN 'split_dividend'
                               WHEN s.ticker IS NOT NULL THEN 'split'
                               WHEN d.ticker IS NOT NULL THEN 'dividend'
                               ELSE NULL
                               END                                        AS event_type
                    FROM tap_polygon_production.splits s
                             FULL JOIN
                         cte_dividends d
                         ON
                             s.ticker = d.ticker AND s.execution_date = d.ex_dividend_date
                    ORDER BY ticker, event_date;
                    """
                )

            self.df_splits_dividends["event_date"] = pd.to_datetime(
                self.df_splits_dividends["event_date"]
            ).dt.tz_localize(self.config.timezone)

            logging.info(
                f"Loaded {len(self.df_splits_dividends)} split/dividend events"
            )

        except Exception as e:
            logging.error(f"Error loading splits/dividends data: {e}")
            # Create empty DataFrame if database fails
            self.df_splits_dividends = pd.DataFrame(
                columns=[
                    "ticker",
                    "event_date",
                    "split_from",
                    "split_to",
                    "cash_amount",
                    "event_type",
                ]
            )

    def get_daily_files(self) -> List[Path]:
        """Get all daily data files sorted by date"""
        pattern = "*.csv.gz"
        files = list(self.data_dir.glob(pattern))
        files.sort()  # Assuming filename format allows chronological sorting
        logging.info(f"Found {len(files)} daily data files")
        return files

    @staticmethod
    def _load_intraday_dataset(file_path: Path) -> pd.DataFrame:
        """Load and process a single day's minute-level data"""
        try:
            df = pd.read_csv(file_path, compression="gzip", keep_default_na=False)
            df = df.rename(columns={"window_start": "timestamp"})
            df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True)
            df["timestamp_cst"] = df["timestamp_utc"].dt.tz_convert("America/New_York")
            df["date"] = df["timestamp_cst"].dt.normalize()
            df = df.sort_values(by=["timestamp_cst", "ticker"])
            return df
        except Exception as e:
            logging.error(f"Error loading {file_path}: {e}")
            return pd.DataFrame()

    def _price_gap_with_split_or_dividend(
        self,
        df_prev: pd.DataFrame,
        df_cur: pd.DataFrame,
        df_splits_dividends: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calculate split/dividend adjusted price gaps - optimized version"""

        # Get previous close prices
        prev_close = (
            df_prev.groupby(["ticker", "date"], sort=False)
            .agg(prev_close=("close", "last"))
            .reset_index()
            .rename(columns={"date": "prev_date"})
        )

        # Get pre-market open (3 AM EST/EDT)
        pmkt_open = (
            df_cur[df_cur["timestamp_cst"].dt.time >= dtime(3, 0)]
            .groupby(["ticker", "date"], sort=False)
            .agg(pre_market_open=("open", "first"))
            .reset_index()
        )

        # Get market open (8:30 AM EST/EDT)
        mkt_open = (
            df_cur[df_cur["timestamp_cst"].dt.time >= dtime(8, 30)]
            .groupby(["ticker", "date"], sort=False)
            .agg(market_open=("open", "first"))
            .reset_index()
        )

        # Merge opening prices
        df_open = pd.merge(pmkt_open, mkt_open, on=["ticker", "date"], how="left")

        # Add previous dates and closes
        prev_dates = (
            df_prev.groupby("ticker")["date"].max().rename("prev_date").reset_index()
        )
        df_open = df_open.merge(prev_dates, on=["ticker"], how="left").merge(
            prev_close, on=["ticker", "prev_date"], how="left"
        )

        # Merge with splits/dividends data
        df_open = df_open.merge(
            df_splits_dividends,
            left_on=["ticker", "date"],
            right_on=["ticker", "event_date"],
            how="left",
        )

        # Fill missing values and calculate ratios
        df_open["split_from"] = df_open["split_from"].fillna(1.0)
        df_open["split_to"] = df_open["split_to"].fillna(1.0)
        df_open["cash_amount"] = df_open["cash_amount"].fillna(0.0)
        df_open["split_ratio"] = df_open["split_from"] / df_open["split_to"]

        # Calculate expected prices
        df_open["expected_unchanged_adj_split_only"] = (
            df_open["prev_close"] * df_open["split_ratio"]
        )
        df_open["expected_unchanged_adj_split_dividend"] = (
            df_open["prev_close"] * df_open["split_ratio"] - df_open["cash_amount"]
        )
        df_open["had_split_or_dividend"] = df_open["event_type"].notna()

        # Determine adjustment states
        is_split = df_open["event_type"] == "split"
        is_div = df_open["event_type"] == "dividend"

        def assign_adj_state(open_col):
            """Determine if opening prices are adjusted for splits/dividends"""
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
            mask = (
                valid
                & is_split
                & ((diff_adj <= self.config.abs_tol) | (rel_adj <= self.config.rel_tol))
            )
            state[mask] = "adjusted"

            mask = (
                valid
                & is_split
                & (
                    (diff_prev <= self.config.abs_tol)
                    | (rel_prev <= self.config.rel_tol)
                )
                & (state == "unknown")
            )
            state[mask] = "unadjusted"

            # DIVIDEND: almost always unadjusted
            mask = (
                valid
                & is_div
                & (
                    (diff_prev <= self.config.abs_tol)
                    | (rel_prev <= self.config.rel_tol)
                )
            )
            state[mask] = "unadjusted"

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

        # Set NaN for no-event rows
        mask_no_event = (df_open["split_ratio"] == 1.0) & (
            df_open["cash_amount"] == 0.0
        )
        for col in [
            "pre_market_open_adj_state",
            "market_open_adj_state",
            "robust_open_adj_state",
        ]:
            df_open.loc[mask_no_event, col] = np.nan

        # Determine baseline for percentage calculations
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

        # Calculate adjusted percentage changes
        df_open["adj_pmkt_pct_chg"] = (df_open["pre_market_open"] - baseline) / baseline
        df_open["adj_mkt_pct_chg"] = (df_open["market_open"] - baseline) / baseline

        # Select and order columns
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

    def calculate_adjusted_overnight_moves(
        self, df_cur: pd.DataFrame, df_prev: pd.DataFrame
    ) -> Dict[str, Dict]:
        """Calculate split/dividend adjusted overnight moves"""

        # Calculate price gaps with split/dividend adjustment
        df_adj_chg = self._price_gap_with_split_or_dividend(
            df_prev, df_cur, self.df_splits_dividends
        )

        overnight_moves = {}

        for _, row in df_adj_chg.iterrows():
            ticker = row["ticker"]

            # Use adjusted pre-market change, fallback to market open change
            adj_overnight_move = row["adj_pmkt_pct_chg"]
            if pd.isna(adj_overnight_move):
                adj_overnight_move = row["adj_mkt_pct_chg"]

            # Calculate raw overnight move for comparison
            if pd.notna(row["pre_market_open"]) and pd.notna(row["prev_close"]):
                raw_overnight_move = (row["pre_market_open"] / row["prev_close"]) - 1
            elif pd.notna(row["market_open"]) and pd.notna(row["prev_close"]):
                raw_overnight_move = (row["market_open"] / row["prev_close"]) - 1
            else:
                raw_overnight_move = 0.0

            overnight_moves[ticker] = {
                "adj_overnight_move": (
                    adj_overnight_move if pd.notna(adj_overnight_move) else 0.0
                ),
                "raw_overnight_move": raw_overnight_move,
                "prev_close": row["prev_close"],
                "had_split_or_dividend": row["had_split_or_dividend"],
                "split_ratio": row["split_ratio"],
                "cash_amount": row["cash_amount"],
                "pre_market_open": row["pre_market_open"],
                "market_open": row["market_open"],
            }

        return overnight_moves

    def find_entry_opportunities(
        self, current_data: pd.DataFrame, overnight_moves: Dict[str, Dict]
    ) -> List[Dict]:
        """Find tickers that meet entry criteria using adjusted overnight moves"""
        opportunities = []

        # Filter tickers with adjusted overnight move >= threshold
        qualifying_tickers = {
            ticker: data
            for ticker, data in overnight_moves.items()
            if data["adj_overnight_move"] >= self.config.overnight_threshold
        }

        if not qualifying_tickers:
            return opportunities

        logging.info(
            f"Found {len(qualifying_tickers)} tickers with adjusted overnight move >= {self.config.overnight_threshold:.1%}"
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

        for ticker, move_data in qualifying_tickers.items():
            ticker_data = current_data[
                (current_data["ticker"] == ticker)
                & (current_data["timestamp_cst"] <= entry_cutoff)
            ].copy()

            if ticker_data.empty:
                continue

            # Check if stock is still up >= 33% at any point before cutoff
            # Use adjusted baseline for split/dividend events
            prev_close = move_data["prev_close"]
            ticker_data["current_move"] = (ticker_data["high"] / prev_close) - 1

            # For split/dividend events, also check against adjusted baseline
            if move_data["had_split_or_dividend"]:
                split_ratio = move_data["split_ratio"]
                cash_amount = move_data["cash_amount"]
                adj_baseline = prev_close * split_ratio - cash_amount
                ticker_data["adj_current_move"] = (
                    ticker_data["high"] / adj_baseline
                ) - 1
                qualifying_condition = (
                    ticker_data["current_move"] >= self.config.overnight_threshold
                ) | (ticker_data["adj_current_move"] >= self.config.overnight_threshold)
            else:
                qualifying_condition = (
                    ticker_data["current_move"] >= self.config.overnight_threshold
                )

            qualifying_bars = ticker_data[qualifying_condition]

            if not qualifying_bars.empty:
                # Use the first qualifying bar's close as entry price
                entry_bar = qualifying_bars.iloc[0]

                opportunities.append(
                    {
                        "ticker": ticker,
                        "entry_time": entry_bar["timestamp_cst"],
                        "entry_price": entry_bar["close"],
                        "adj_overnight_move": move_data["adj_overnight_move"],
                        "raw_overnight_move": move_data["raw_overnight_move"],
                        "prev_close": prev_close,
                        "had_split_or_dividend": move_data["had_split_or_dividend"],
                        "split_ratio": move_data["split_ratio"],
                        "cash_amount": move_data["cash_amount"],
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
        adj_overnight_move = opportunity["adj_overnight_move"]
        raw_overnight_move = opportunity["raw_overnight_move"]
        prev_close = opportunity["prev_close"]
        had_split_or_dividend = opportunity["had_split_or_dividend"]
        split_ratio = opportunity["split_ratio"]
        cash_amount = opportunity["cash_amount"]

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
                    overnight_move_pct=raw_overnight_move,
                    adj_overnight_move_pct=adj_overnight_move,
                    prev_close=prev_close,
                    had_split_or_dividend=had_split_or_dividend,
                    split_ratio=split_ratio,
                    cash_amount=cash_amount,
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
                    overnight_move_pct=raw_overnight_move,
                    adj_overnight_move_pct=adj_overnight_move,
                    prev_close=prev_close,
                    had_split_or_dividend=had_split_or_dividend,
                    split_ratio=split_ratio,
                    cash_amount=cash_amount,
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
                overnight_move_pct=raw_overnight_move,
                adj_overnight_move_pct=adj_overnight_move,
                prev_close=prev_close,
                had_split_or_dividend=had_split_or_dividend,
                split_ratio=split_ratio,
                cash_amount=cash_amount,
            )

            self.current_balance += final_pnl
            return trade

        return None

    def run_backtest(self) -> Dict:
        """Run the complete backtest with optimized data loading"""
        logging.info("Starting HTB Equity Backtest with Split/Dividend Adjustment")

        # Load splits/dividends data once
        if self.df_splits_dividends is None:
            self.pull_splits_dividends()

        daily_files = self.get_daily_files()

        if len(daily_files) < 2:
            logging.error("Need at least 2 daily files to run backtest")
            return {}

        # Initialize with first file as previous day
        logging.info(f"Loading initial file: {daily_files[0].name}")
        df_prev = self._load_intraday_dataset(daily_files[0])

        if df_prev.empty:
            logging.error("Failed to load initial file")
            return {}

        # Process each subsequent day
        for i in range(1, len(daily_files)):
            current_file = daily_files[i]

            logging.info(
                f"Processing day {i}/{len(daily_files) - 1}: {current_file.name}"
            )

            # Load current day data
            df_cur = self._load_intraday_dataset(current_file)
            if df_cur.empty:
                logging.warning(f"Skipping empty file: {current_file.name}")
                continue

            # Calculate split/dividend adjusted overnight moves
            overnight_moves = self.calculate_adjusted_overnight_moves(df_cur, df_prev)

            # Find entry opportunities
            opportunities = self.find_entry_opportunities(df_cur, overnight_moves)

            daily_trades = []
            daily_pnl = 0
            split_dividend_filtered = 0

            # Execute trades
            for opportunity in opportunities:
                # Check if we have sufficient balance
                if self.current_balance < self.config.bet_size:
                    logging.warning("Insufficient balance for new trade")
                    continue

                # Log if we're filtering out split/dividend events
                if opportunity["had_split_or_dividend"]:
                    adj_move = opportunity["adj_overnight_move"]
                    raw_move = opportunity["raw_overnight_move"]
                    if (
                        raw_move >= self.config.overnight_threshold
                        and adj_move < self.config.overnight_threshold
                    ):
                        split_dividend_filtered += 1
                        logging.info(
                            f"Filtered {opportunity['ticker']}: Raw move {raw_move:.1%} vs Adj move {adj_move:.1%}"
                        )
                        continue

                trade = self.execute_trade(opportunity, df_cur)

                if trade:
                    self.trades.append(trade)
                    daily_trades.append(trade)
                    daily_pnl += trade.pnl

                    logging.info(
                        f"Trade: {trade.ticker} | Entry: ${trade.entry_price:.2f} | "
                        f"Exit: ${trade.exit_price:.2f} | P&L: ${trade.pnl:.2f} | "
                        f"Reason: {trade.exit_reason} | Split/Div: {trade.had_split_or_dividend}"
                    )

            # Record daily performance
            trade_date = df_cur["date"].iloc[0].strftime("%Y-%m-%d")
            self.daily_portfolio_values.append(
                {
                    "date": trade_date,
                    "balance": self.current_balance,
                    "daily_pnl": daily_pnl,
                    "trades_count": len(daily_trades),
                    "opportunities": len(opportunities),
                    "split_dividend_filtered": split_dividend_filtered,
                }
            )

            if daily_trades or split_dividend_filtered > 0:
                logging.info(
                    f"Day {trade_date}: {len(daily_trades)} trades, "
                    f"{split_dividend_filtered} filtered, P&L: ${daily_pnl:.2f}"
                )

            # Update previous day data for next iteration
            df_prev = df_cur

        logging.info(f"Backtest completed. Total trades: {len(self.trades)}")
        return self.generate_performance_report()

    def generate_performance_report(self) -> Dict:
        """Generate comprehensive performance report with split/dividend analysis"""
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
                    "adj_overnight_move_pct": trade.adj_overnight_move_pct,
                    "prev_close": trade.prev_close,
                    "had_split_or_dividend": trade.had_split_or_dividend,
                    "split_ratio": trade.split_ratio,
                    "cash_amount": trade.cash_amount,
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

        # Split/dividend analysis
        split_div_trades = trades_df[trades_df["had_split_or_dividend"]]
        normal_trades = trades_df[~trades_df["had_split_or_dividend"]]

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
                "total_split_dividend_filtered": portfolio_df[
                    "split_dividend_filtered"
                ].sum(),
            },
            "split_dividend_analysis": {
                "trades_with_split_div": len(split_div_trades),
                "trades_without_split_div": len(normal_trades),
                "split_div_win_rate": (
                    (
                        len(split_div_trades[split_div_trades["pnl"] > 0])
                        / len(split_div_trades)
                        * 100
                    )
                    if len(split_div_trades) > 0
                    else 0
                ),
                "normal_win_rate": (
                    (
                        len(normal_trades[normal_trades["pnl"] > 0])
                        / len(normal_trades)
                        * 100
                    )
                    if len(normal_trades) > 0
                    else 0
                ),
                "split_div_avg_pnl": (
                    split_div_trades["pnl"].mean() if len(split_div_trades) > 0 else 0
                ),
                "normal_avg_pnl": (
                    normal_trades["pnl"].mean() if len(normal_trades) > 0 else 0
                ),
            },
            "exit_reasons": trades_df["exit_reason"].value_counts().to_dict(),
            "top_performers": trades_df.nlargest(10, "pnl")[
                [
                    "ticker",
                    "trade_date",
                    "pnl",
                    "adj_overnight_move_pct",
                    "had_split_or_dividend",
                ]
            ].to_dict("records"),
            "worst_performers": trades_df.nsmallest(10, "pnl")[
                [
                    "ticker",
                    "trade_date",
                    "pnl",
                    "adj_overnight_move_pct",
                    "had_split_or_dividend",
                ]
            ].to_dict("records"),
            "trades_data": trades_df.to_dict("records"),
            "daily_performance": portfolio_df.to_dict("records"),
        }

        return performance_report

    def save_results(self, results: Dict, filename: str = None):
        """Save backtest results to JSON file with proper serialization"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"htb_backtest_results_{timestamp}.json"

        def serialize_objects(obj):
            """Handle all non-JSON serializable objects"""
            if isinstance(obj, (datetime, pd.Timestamp)):
                return obj.isoformat()
            elif isinstance(obj, pd.Period):
                return str(obj)
            elif isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif pd.isna(obj):
                return None
            elif hasattr(obj, "isoformat"):
                return obj.isoformat()
            else:
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        try:
            with open(filename, "w") as f:
                json.dump(results, f, indent=2, default=serialize_objects)

            logging.info(f"Results saved to {filename}")
        except Exception as e:
            logging.error(f"Error saving results: {e}")
            # Try to save a simplified version
            try:
                simplified_results = {
                    "summary": results.get("summary", {}),
                    "error": f"Full results could not be serialized: {str(e)}",
                }
                with open(f"simplified_{filename}", "w") as f:
                    json.dump(
                        simplified_results, f, indent=2, default=serialize_objects
                    )
                logging.info(f"Simplified results saved to simplified_{filename}")
            except Exception as e2:
                logging.error(f"Could not save even simplified results: {e2}")


def main():
    """Main execution function"""

    # Configuration
    config = BacktestConfig(
        data_directory="~/polygon_data/bars_1min/",  # Your actual data directory
        starting_balance=100000.0,
        bet_size=2000.0,
        max_loss=2000.0,
        overnight_threshold=0.33,
        abs_tol=0.02,  # Split/dividend detection tolerance
        rel_tol=0.002,  # Split/dividend detection tolerance
    )

    # Initialize and run backtest
    backtest = HTBEquityBacktest(config)
    results = backtest.run_backtest()

    if results and "summary" in results:
        # Print summary
        summary = results["summary"]
        split_div_analysis = results["split_dividend_analysis"]

        print("\n" + "=" * 70)
        print("HTB EQUITY SHORTING BACKTEST RESULTS (SPLIT/DIVIDEND ADJUSTED)")
        print("=" * 70)
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
        print(
            f"Split/Dividend Events Filtered: {summary['total_split_dividend_filtered']}"
        )
        print("-" * 70)
        print("SPLIT/DIVIDEND ANALYSIS:")
        print(
            f"Trades with Split/Div Events: {split_div_analysis['trades_with_split_div']}"
        )
        print(f"Normal Trades: {split_div_analysis['trades_without_split_div']}")
        print(f"Split/Div Win Rate: {split_div_analysis['split_div_win_rate']:.1f}%")
        print(f"Normal Win Rate: {split_div_analysis['normal_win_rate']:.1f}%")
        print(f"Split/Div Avg P&L: ${split_div_analysis['split_div_avg_pnl']:,.2f}")
        print(f"Normal Avg P&L: ${split_div_analysis['normal_avg_pnl']:,.2f}")
        print("=" * 70)

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
