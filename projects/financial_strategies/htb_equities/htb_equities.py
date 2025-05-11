# import polars as pl
import pandas as pd

from ds_core.db_connectors import *

# db = PostgresConnect()
#
# db.connect()

###############
### GLOBALS ###
###############

PCT_CHANGE_TRIGGER = 0.33

STARTING_BANKROLL = 100000
BET_AMOUNT = 2000

### LOGIC ###

df = pd.read_csv(
    "/home/melgazar9/Downloads/bq-results-20250511-032700-1746934060995.csv"
)
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values(by=["timestamp", "ticker"])
df.loc[:, "prev_close_5m"] = df.groupby("ticker")["close"].shift()
df["date"] = df["timestamp"].dt.date

df_1d = df.copy()
df_1d = (
    df_1d.groupby(["date", "ticker"])
    .agg(
        {
            "timestamp": "last",
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
df = df.merge(df_1d.drop("timestamp", axis=1), on=["ticker", "date"], how="left")

for col in ["open", "high", "low", "close"]:
    df[f"{col}_from_prev_close"] = df[col] - df["prev_close_1d"]
    df[f"pct_{col}_from_prev_close"] = (
        df[f"{col}_from_prev_close"] / df["prev_close_1d"]
    )

df.loc[df["pct_high_from_prev_close"] > PCT_CHANGE_TRIGGER, "trigger_condition"] = True
df["trigger_condition"] = (
    df.groupby(["ticker", "date"])["trigger_condition"]
    .apply(lambda x: x.ffill())
    .reset_index(level=[0, 1], drop=True)
)
df["trigger_condition"] = df["trigger_condition"].fillna(False)

# RELEVANT_DEBUG_COLS = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'prev_close_1d'] + [i for i in df.columns if i.startswith('pct')] + ['trigger_condition']
# df[df['ticker'] == 'UBER'][RELEVANT_DEBUG_COLS]
