import os
from glob import glob
from bt_engine.central_loaders import PolygonBarLoader
from bt_engine.engine import BacktestEngine
from datetime import time as dtime, datetime
import logging

# os.chdir('projects/financial-strategies/strategies/gap_strategy'); pd.options.display.max_columns=100
from gap_backtest_utils import (
    GapStrategyRiskManager,
    GapPositionManager,
    GapStrategyEvaluator,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Rules
# 1. If gap from previous day close to either pre-market open or market open is >= X% (adjusted) then trigger trade entry
# 2. Iterate over different stop and take loss percentages
# 3. Do not enter trade after 13:45 CST
# 4. All positions must be flattened by 14:55 CST if
# 5. Segment by groups --> HTB / short interest/volume buckets, market cap bucket, industry, VIX range, etc.

data_dir = "~/polygon_data/bars_1min/"
files = sorted(glob(os.path.expanduser(f"{data_dir}bars_1m_*.csv.gz")))
run_timestamp = datetime.now().strftime("%Y-%m-%d__%H:%M:%S")

df_prev = PolygonBarLoader(cur_day_file=files[0]).load_raw_intraday_bars()
cur_day_file = files[1]
header = True
data_loader = PolygonBarLoader(load_method="pandas", df_prev=df_prev)

for cur_day_file in files[1:]:
    logging.info(f"Processing {cur_day_file}")
    data_loader.cur_day_file = cur_day_file
    gbt = BacktestEngine(
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

    gbt.run_backtest()

    gbt.df_evaluation.to_frame().T.to_csv(
        f"~/gap_backtrade_results/summary_{run_timestamp}.csv",
        index=False,
        header=header,
        mode="a",
    )
    df_prev = gbt.df
    header = False
