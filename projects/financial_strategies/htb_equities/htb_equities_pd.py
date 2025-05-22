from datetime import time as dtime

import numpy as np

from ds_core.db_connectors import *

################
### Strategy ###
################

# Ambiguous definition: Short HTB stocks if they move up significantly overnight
# Well-defined version 1: Shorrt <HTB> stocks (will filter for HTB later) if they move up more than 33% overnight
# additionally the opening trades must be between 3am and 13:45 (CST), closing all positions at the EOD (14:55 CST).
# Stop losses start at 2x the short value for a first round through, but we can iterate over different combinations
# of stops and profit taking to optimize the strategy per segment (e.g.
# per ticker, capitalization category, etc)


###############
### GLOBALS ###
###############

PCT_CHANGE_TRIGGER = 0.33
STARTING_BANKROLL = 100000
BET_AMOUNT = 2000
STOP_LOSS_AMOUNT = 4000
END_OF_DAY_TIME = dtime(15, 0)

# db = PostgresConnect(database="financial_elt")
#
#
# query = """
#     with cte as (
#         select
#           timestamp,
#           timestamp_tz_aware,
#           timezone,
#           ticker,
#           open,
#           high,
#           low,
#           close,
#           volume,
#           dividends,
#           stock_splits,
#           repaired,
#           row_number() over(partition by timestamp, ticker order by _sdc_batched_at) as rn
#         from
#           tap_yfinance_dev.stock_prices_5m
#         where
#           timezone = 'America/New_York'
#     )
#
#     select * from cte where rn = 1 limit 5000000
# """


# df = db.run_sql(query)

# df.to_feather('~/df_stock_prices_5m_5million.feather')
# df.to_parquet('~/df_stock_prices_5m_5million.parquet')

df = pd.read_feather("~/df_stock_prices_5m_5million.feather")


### LOGIC ###

df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
df["timestamp_cst"] = pd.to_datetime(df["timestamp_tz_aware"])
df["time_cst"] = df["timestamp_cst"].dt.time
df = df.sort_values(by=["timestamp_cst", "ticker"])
df["date"] = df["timestamp_cst"].dt.date
df.loc[:, "prev_close_5m"] = df.groupby("ticker")["close"].shift()


df_1d = df.copy()

df_1d = (
    df_1d.groupby(["date", "ticker"])
    .agg(
        {
            "timestamp_cst": "last",
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    .reset_index()
    .rename(
        columns={
            "open": "open_1d",
            "high": "high_1d",
            "low": "low_1d",
            "close": "close_1d",
            "volume": "volume_1d",
        }
    )
)

df_1d["prev_close_1d"] = df_1d.groupby("ticker")["close_1d"].shift(1)

df = df.merge(df_1d.drop("timestamp_cst", axis=1), on=["ticker", "date"], how="left")
del df_1d
gc.collect()

for col in ["open", "high", "low", "close"]:
    df[f"{col}_minus_prev_close"] = df[col] - df["prev_close_1d"]
    df[f"{col}_minus_prev_close_pct_chg"] = (
        df[f"{col}_minus_prev_close"] / df["prev_close_1d"]
    )

df.loc[
    df["high_minus_prev_close_pct_chg"] > PCT_CHANGE_TRIGGER, "trigger_condition_pct"
] = True

df["trigger_condition_pct"] = (
    df.groupby(["ticker", "date"])["trigger_condition_pct"]
    .apply(lambda x: x.ffill())
    .reset_index(level=[0, 1], drop=True)
)

df["trigger_condition_pct"] = df["trigger_condition_pct"].fillna(False)
df["trigger_within_time_range"] = df["time_cst"].between(
    dtime(3, 0), dtime(13, 45)
)  # time within 3am and 13:45 (1:45pm)


def track_trade(df):
    # 1. trigger_condition_pct
    # 2. trigger_within_time_range
    # 3. There will be a new column called no_stop_position_size where if both above are true then set the value to be
    #    equal to BET_AMOUNT / open (starting from the first 5-min occurrence --> so we open the trade). Additonally,
    #    this column should reset to 0 at the end of the trading day to show that we close all postitions at EOD.
    # 4. From here create a new column called no_stop_pnl where we track the pnl of the trade from the open price of the
    #    trade to the close price current 5-minute bar candle.
    # 5. Create another column called stop_triggered, where it is True if the pnl is down more than the
    # STOP_LOSS_AMOUNT (else False).
    # 6. Create a final column called pnl_with_stop --> the pnl of the trade. If the stop is not triggered we close the
    #    position at the end of the day (e.g. 5min open price when the timestamp is END_OF_DAY_TIME (14:55 CST)
    #    --> assume we close 5 minutes prior to close for liquidity and speed reasons

    ### init new columns ###

    df["no_stop_position_size"] = np.nan
    df["trade_open_price"] = np.nan
    df["no_stop_pnl"] = np.nan

    ### first trade entrance ###

    df["can_enter_trade"] = df["trigger_condition_pct"] & df["trigger_within_time_range"]

    first_entry_idx_per_day = df.groupby(["date"])["can_enter_trade"].apply(
        lambda s: s[s].index[0] if s.any() else np.nan
    )

    is_first_entry = pd.Series(False, index=df.index)
    is_first_entry.loc[first_entry_idx_per_day.dropna()] = True
    df.loc[is_first_entry, "trade_open_price"] = df.loc[is_first_entry, "open"]
    df["trade_open_price"] = df.groupby(["date"])["trade_open_price"].ffill().fillna(0)

    ### assign position size ###

    df.loc[is_first_entry, "no_stop_position_size"] = (
        BET_AMOUNT / df.loc[is_first_entry, "open"]
    )
    df["no_stop_position_size"] = (
        df.groupby(["date"])["no_stop_position_size"].ffill().fillna(0)
    )

    df.loc[:, "no_stop_pnl"] = (df["trade_open_price"] - df["close"]) * df[
        "no_stop_position_size"
    ]
    df["no_stop_pnl"] = df.groupby(["date"])["no_stop_pnl"].ffill().fillna(0)

    ### stop loss logic ###

    df["pnl_leq_stop"] = df["no_stop_pnl"] <= -abs(STOP_LOSS_AMOUNT)

    first_stop_idx_per_day = df.groupby(["date"])["pnl_leq_stop"].apply(
        lambda s: s[s].index[0] if s.any() else np.nan
    )
    first_stop_triggered = pd.Series(False, index=df.index)
    first_stop_triggered.loc[first_stop_idx_per_day.dropna()] = True

    df.loc[first_stop_triggered, "stop_triggered"] = df.loc[
        first_stop_triggered, "pnl_leq_stop"
    ]
    df["stop_triggered"] = df.groupby(["date"])["stop_triggered"].ffill()
    df.loc[df["no_stop_position_size"] > 0, "stop_triggered"] = df.loc[
        df["no_stop_position_size"] > 0, "stop_triggered"
    ].fillna(False)

    def apply_stop(group):
        if group["stop_triggered"].any():
            stop_idx = group["stop_triggered"].idxmax()
            stop_value = group.loc[stop_idx, "no_stop_pnl"]
            group.loc[group.index >= stop_idx, "pnl_with_stop"] = stop_value
        else:
            group["pnl_with_stop"] = group["no_stop_pnl"]

        group.loc[
            group["time_cst"] >= END_OF_DAY_TIME,
            [
                "no_stop_position_size",
                "trade_open_price",
                "no_stop_pnl",
                "pnl_with_stop",
            ],
        ] = 0
        group.loc[group["time_cst"] >= END_OF_DAY_TIME, "stop_triggered"] = False

        return group

    df["pnl_with_stop"] = df["no_stop_pnl"]
    df = df.groupby("date", group_keys=False).apply(apply_stop)
    df["flat_time"] = df["time_cst"] >= END_OF_DAY_TIME

    return df


df = df.groupby("ticker").apply(track_trade)

column_order = columns = [
    "timestamp",
    "timestamp_tz_aware",
    "timezone",
    "timestamp_cst",
    "time_cst",
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "dividends",
    "stock_splits",
    "repaired",
    "prev_close_5m",
    "open_1d",
    "high_1d",
    "low_1d",
    "close_1d",
    "volume_1d",
    "prev_close_1d",
    "open_minus_prev_close",
    "open_minus_prev_close_pct_chg",
    "high_minus_prev_close",
    "high_minus_prev_close_pct_chg",
    "low_minus_prev_close",
    "low_minus_prev_close_pct_chg",
    "close_minus_prev_close",
    "close_minus_prev_close_pct_chg",
    "trigger_condition_pct",
    "trigger_within_time_range",
    "no_stop_position_size",
    "trade_open_price",
    "no_stop_pnl",
    "can_enter_trade",
    "pnl_leq_stop",
    "stop_triggered",
    "pnl_with_stop",
]

df = df[column_order]

df.to_csv(f"~/harold_short_strategy_{str(datetime.today()).replace(' ', '__')}.csv")
