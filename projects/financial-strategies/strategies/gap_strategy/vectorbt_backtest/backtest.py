import os

# import vectorbt as vbt
from bt_engine.helpers import *

# --- Config ---

STARTING_CASH = 100_000
TRADE_CASH = 2_000
TP_PCT = 0.02  # 2% take profit
SL_PCT = 0.05  # 5% stop loss
SLIPPAGE_PCT = 0.001  # 0.1%
CUTOFF_CST = pd.Timestamp("13:45:00").time()


### --- Functions ---


# --- Load Data ---

bar_loader = PolygonBarLoader()

bar_loader.df_prev = bar_loader.load_raw_intraday_bars(
    os.path.expanduser("~/polygon_data/bars_1min/bars_1m_2025-05-12.csv.gz")
)

pd.options.display.max_columns = 100

### Create entry signal for vbt ###

daily_metrics = []

files = sorted(os.listdir(os.path.expanduser("~/polygon_data/bars_1min/")))
for file in files[1:]:
    logging.info(f"Processing {file}")
    bar_loader.load_and_adjust_bars(
        cur_day_file=os.path.expanduser(f"~/polygon_data/bars_1min/{file}"),
    )

    df = bar_loader.df_cur.copy()

    # --- run backtest ---

    # I need to run backtest logic here

    # update prev day
    bar_loader.df_prev = bar_loader.df_cur.copy()
