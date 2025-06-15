import os
from bt_engine.central_loaders import PolygonBarLoader, CSVReader
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

csv_reader = CSVReader(
    local_or_remote="local",
    host=os.getenv("NORDVPN_MESHNET_IP"),
    username=os.getenv("NORDVPN_USERNAME"),
)
data_dir = "/home/melgazar9/polygon_data/us_stocks_sip/bars_1m"
files = sorted(csv_reader.list_files(data_dir))

run_timestamp = datetime.now().strftime("%Y-%m-%d__%H:%M:%S")

df_prev = PolygonBarLoader(
    cur_day_file=f"{data_dir}/{files[0]}", local_or_remote="remote"
).load_raw_intraday_bars()
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
