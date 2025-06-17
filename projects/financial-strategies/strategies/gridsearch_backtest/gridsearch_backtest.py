### Imports ###

import os
import time
from datetime import datetime as dt
from datetime import time as dtime
from pathlib import Path

import pandas as pd
from utils import SingleDayBacktest, create_file_pairs

# os.chdir('projects/financial-strategies/htb_equities')
pd.set_option("future.no_silent_downcasting", True)


### --- Outline --- ###

# The goal is to essentially data mine specific types of <stocks> that optimizes entry and exit points for an intraday
# trading strategy. Aside from pure data mining, there should be fundamental reasons for sustainable profitability.

### Steps ###

# 1. Set a starting current_bankroll
# 2. Set a fixed bet amount per trade
# 3. Iterate over dollar stop-loss amounts
# 4. Set a time to flatten all positions (i.e. at the end of the day)
# 5. Iterate over percentage change triggers to enter trades
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
STOP_LOSS_PCT = 0.10
TIME_TO_FLATTEN_POSITIONS = dtime(14, 55)
PCT_CHANGE_TRIGGER_TO_ENTER = 0.33

DATA_DIR = os.path.expanduser("~/polygon_data/bars_1min/")
PROCESSED_DIR = os.path.expanduser("~/processed_strategy_results/")
os.makedirs(PROCESSED_DIR, exist_ok=True)

csv_files = create_file_pairs(Path(DATA_DIR))


### --- Run Backtest --- ###

summaries = pd.DataFrame()

start = time.monotonic()
bt = SingleDayBacktest()

first_day_backtest = True

for f in csv_files:
    start_i = time.monotonic()
    cur_day_file = f["cur_day_file"]
    prev_day_file = f["prev_day_file"]
    print(f"Processing {cur_day_file} (prev_day_file = {prev_day_file})")
    if first_day_backtest:
        long_strategy_starting_bankroll = STARTING_BANKROLL
        short_strategy_starting_bankroll = STARTING_BANKROLL

    bt.run_backtest(
        cur_day_file=cur_day_file,
        prev_day_file=prev_day_file,
        long_strategy_starting_bankroll=long_strategy_starting_bankroll,
        short_strategy_starting_bankroll=short_strategy_starting_bankroll,
        bet_amount=BET_AMOUNT,
        close_trade_timestamp_cst=TIME_TO_FLATTEN_POSITIONS,
        stop_loss_pct=STOP_LOSS_PCT,
        pre_market_pct_chg_range=(0.01, 1),
        market_pct_chg_range=(0.01, 1),
        trigger_condition="or",
        direction="both",
        slippage_pct=0.1,
        slippage_method="partway",
    )

    summaries = pd.concat([summaries, bt.summary])
    end_i = time.monotonic()
    first_day_backtest = False
    long_strategy_starting_bankroll = (
        bt.long_strategy_starting_bankroll
    )  # update for next iteration
    short_strategy_starting_bankroll = bt.short_strategy_starting_bankroll
    print(f"Processed {cur_day_file} in {end_i - start_i:.2f} seconds")


print(f"Processed {len(csv_files)} files in {time.monotonic() - start:.2f} seconds")


# Save the results to a CSV file
summaries.to_csv(
    PROCESSED_DIR
    + f"htb_equities_backtest_summary_{dt.now().strftime('%Y%m%d_%H%M%S')}.csv"
)
