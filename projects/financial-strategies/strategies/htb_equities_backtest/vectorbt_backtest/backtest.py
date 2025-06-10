import os

import vectorbt as vbt
from bt_engine.helpers import *

# --- Config ---

STARTING_CASH = 100_000
TRADE_CASH = 2_000
TP_PCT = 0.02  # 2% take profit
SL_PCT = 0.05  # 5% stop loss
SLIPPAGE_PCT = 0.001  # 0.1%
CUTOFF_CST = pd.Timestamp("13:45:00").time()


### --- Functions ---


# TODO: Broken --> fix logic
def run_vbt(df, day):
    """
    Given a DataFrame for one day and the day string,
    prepare wide-form data, run vectorbt, and return per-ticker daily metrics DataFrame.
    """

    df = df.drop_duplicates(subset=["timestamp_utc", "ticker"]).sort_values(
        ["timestamp_utc", "ticker"]
    )
    df["dt"] = pd.to_datetime(df["timestamp_utc"])
    df = df.set_index(["dt", "ticker"]).sort_index()

    open_price = df["open"].unstack().astype(np.float32)
    high_price = df["high"].unstack().astype(np.float32)
    low_price = df["low"].unstack().astype(np.float32)
    close_price = df["close"].unstack().astype(np.float32)
    adj_mkt = df["adj_mkt_pct_chg"].unstack().astype(np.float32)
    adj_pmkt = df["adj_pmkt_pct_chg"].unstack().astype(np.float32)
    # cst_time in dt index, unstack to columns as times
    cst_time = pd.to_datetime(df["timestamp_cst"]).dt.time.unstack()

    entries = ((adj_mkt > 0.01) | (adj_pmkt > 0.01)) & (cst_time <= CUTOFF_CST)

    safe_open = open_price.replace(0, np.nan)
    size = np.where(entries, np.floor(TRADE_CASH / safe_open), 0).astype(np.int32)
    size = pd.DataFrame(size, index=open_price.index, columns=open_price.columns)

    pf_long = vbt.Portfolio.from_signals(
        close=close_price,
        entries=entries,
        exits=None,
        size=size,
        sl_stop=SL_PCT,
        tp_stop=TP_PCT,
        open=open_price,
        high=high_price,
        low=low_price,
        slippage=SLIPPAGE_PCT,
        freq="1min",
        init_cash=STARTING_CASH,
        fees=0.0,
        max_logs=0,
    )

    tickers = open_price.columns

    if pf_long.asset_value().shape[0] == 0:
        start_val = pd.Series([np.nan] * len(tickers), index=tickers)
        end_val = pd.Series([np.nan] * len(tickers), index=tickers)
    else:
        start_val = pf_long.asset_value().iloc[0]
        end_val = pf_long.asset_value().iloc[-1]

    daily_pnl = end_val - start_val
    daily_return = (end_val - start_val) / start_val.replace(0, np.nan)

    df_vbt = pd.DataFrame(
        {
            "date": day,
            "ticker": tickers,
            "start_val": start_val.values,
            "end_val": end_val.values,
            "daily_pnl": daily_pnl.values,
            "daily_return": daily_return.values,
            "trades_count": pf_long.trades.count().values,
            "win_rate": pf_long.trades.win_rate().values,
        }
    )
    return df_vbt


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

    day = file.split("bars_1m_")[-1].replace(".csv.gz", "")
    df_vbt = run_vbt(df, day)
    daily_metrics.append(df_vbt)

    # update prev day
    bar_loader.df_prev = bar_loader.df_cur.copy()

df_all = pd.concat(daily_metrics, ignore_index=True)
df_all["date"] = pd.to_datetime(df_all["date"])

# ---- Calculate long-term metrics: Sharpe ratio per ticker per year ----
df_all["year"] = df_all["date"].dt.year
df_all["month"] = df_all["date"].dt.month
df_all["weekday"] = df_all["date"].dt.weekday


def sharpe(x):
    if x["daily_return"].std() == 0 or x["daily_return"].isna().all():
        return np.nan
    return (x["daily_return"].mean() / x["daily_return"].std()) * np.sqrt(252)


sharpe_by_month = df_all.groupby(["ticker", "month"]).apply(sharpe).unstack()
total_return_by_year = (
    df_all.groupby(["ticker", "year"])["daily_return"].sum().unstack()
)

print("Sharpe by year (head):\n", sharpe_by_year.head())
print("Total return by year (head):\n", total_return_by_year.head())
