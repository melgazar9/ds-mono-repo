### Imports ###

import os
from datetime import time
from pathlib import Path
import pandas as pd
# os.chdir('projects/financial-strategies/htb_equities')
from utils import SingleDayBacktest

# import polars as pl
# from polars import col, lit, when, plr

pd.set_option("future.no_silent_downcasting", True)


### --- Outline --- ###

# The goal is to essentially data mine specific types of <stocks> that optimizes entry and exit points for an intraday
# trading strategy. Aside from pure data mining, there should be fundamental reasons for sustainable profitability.

### Steps ###

# 1. Set a starting bankroll
# 2. Set a fixed bet amount per trade
# 3. Set a fixed dollar stop-loss amount
# 4. Set a time to flatten all positions at the end of the day
# 5. Set a percentage change trigger to enter trades
# 6. The above calculates strategy results for one specific combinations of parameters. We need to iterate over these
# parameters to find the optimal combination (e.g. best returns, lowest drawdown, lowest variance, highest Sharp, etc.).
# 7. Define a method that applies this strategy to a single trading day (requires intraday data).
# 8. Define methods that iterate over the necessary parameters and apply the strategy for that single trading day. The
# parameters we need to optimize are: stop_loss_amount and pct_change_trigger_to_enter.
# 9. Define a method that iterates over all trading days and applies the strategy, then concatenates the results.
# 10. Define a method that saves the results to a CSV file.
# 11. Optimize this strategy further by segmenting across data points from other sources such as:
#    - short_interest (buckets)
#    - short_volume (buckets)
#    - financial_data
#    - market_cap (nano (OTC), micro, small, mid, large, mega), etc.
#    - sector / industry
#    - VIX buckets (e.g. VIX between 0-5, 5-10, 10-15, etc.)
#    - Apply historical volatility (grouped by asset) to get a sense for expected volatility.

# Follow-up:
# 12. What this is essentially doing is building a short-depth decision tree - it's an interaction-feature analysis.
# A follow-up is to use existing features and build an ML model around this strategy with a well-defined target.
# We should also add features and compare the model with additional features to the simplified model.
# For testing, we can do it from scratch or use an open sourced tool like backtrader, zipline, or quantconnect.

### --- Global Configuration --- ###

START_DATE = "2018-01-01"
END_DATE = "2023-01-01"

STARTING_BANKROLL = 100000
BET_AMOUNT = 2000
STOP_LOSS_AMOUNT = 4000
TIME_TO_FLATTEN_POSITIONS = time(14, 55)
PCT_CHANGE_TRIGGER_TO_ENTER = 0.33

DATA_DIR = os.path.expanduser("~/polygon_data/bars_1min/")
PROCESSED_DIR = os.path.expanduser("~/processed_strategy_results/")
os.makedirs(PROCESSED_DIR, exist_ok=True)
all_csv_files = sorted(Path(DATA_DIR).glob("*.csv.gz"))

### --- Run Backtest --- ###

first_day_file = all_csv_files[0]

bt = SingleDayBacktest()
bt.run_backtest(cur_day_file=all_csv_files[1], prev_day_file=all_csv_files[0])
