import os
from datetime import datetime
from datetime import time as dtime
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

# --- Global Configuration ---
PCT_CHANGE_TRIGGER = 0.33
STARTING_BANKROLL = (
    100000
)
BET_AMOUNT = 2000
STOP_LOSS_AMOUNT = 4000
END_OF_DAY_TIME = dtime(14, 55)
    100000
)
BET_AMOUNT = 2000
STOP_LOSS_AMOUNT = 4000
END_OF_DAY_TIME = dtime(14, 55)
>>>>>>> 4f752da (add meltano tap-polygon)

DATA_DIR = os.path.expanduser(
    "/home/melgazar9/scripts/dev/ds-mono-repo/projects/financial_strategies/htb_equities/data/"
)
PROCESSED_DIR = os.path.expanduser("~/processed_strategy_results/")
os.makedirs(PROCESSED_DIR, exist_ok=True)

all_csv_files = sorted(Path(DATA_DIR).glob("*.csv.gz"))
prev_day_close_cache = {}


def track_trades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies the trading strategy logic to a Pandas DataFrame, including
    position sizing, PNL calculation, stop-loss, and end-of-day flattening.
    This function operates on a single day's data, grouped by ticker.
    """
    df["no_stop_position_size"] = np.nan
    df["trade_open_price"] = np.nan
    df["no_stop_pnl"] = np.nan

    df["can_enter_trade"] = df["trigger_condition_pct"] & df["trigger_within_time_range"]

    # Find the very first entry point for each date
    first_entry_idx_per_day = df.groupby(["date"])["can_enter_trade"].apply(
        lambda s: s[s].index[0] if s.any() else np.nan
    )
    is_first_entry = pd.Series(False, index=df.index)
    is_first_entry.loc[first_entry_idx_per_day.dropna()] = True

    # Set the trade open price and position size at the first entry point, then forward fill
    df.loc[is_first_entry, "trade_open_price"] = df.loc[is_first_entry, "open"]
    df["trade_open_price"] = df.groupby(["date"])["trade_open_price"].ffill().fillna(0)

    df.loc[is_first_entry, "no_stop_position_size"] = (
        BET_AMOUNT / df.loc[is_first_entry, "open"]
    )
    df["no_stop_position_size"] = (
        df.groupby(["date"])["no_stop_position_size"].ffill().fillna(0)
    )

    # Calculate PNL assuming no stop-loss (PnL from open price to current close)
    df.loc[:, "no_stop_pnl"] = (df["trade_open_price"] - df["close"]) * df[
        "no_stop_position_size"
    ]
    df["no_stop_pnl"] = df.groupby(["date"])["no_stop_pnl"].ffill().fillna(0)

    # Determine if PNL is below the stop-loss threshold
    df["pnl_leq_stop"] = df["no_stop_pnl"] <= -abs(STOP_LOSS_AMOUNT)

    # Find the first time the stop-loss is triggered for each date
    first_stop_idx_per_day = df.groupby(["date"])["pnl_leq_stop"].apply(
        lambda s: s[s].index[0] if s.any() else np.nan
    )
    first_stop_triggered_series = pd.Series(False, index=df.index)
    first_stop_triggered_series.loc[first_stop_idx_per_day.dropna()] = True

    df.loc[first_stop_triggered_series, "stop_triggered_temp"] = df.loc[
        first_stop_triggered_series, "pnl_leq_stop"
    ]

    df["stop_triggered"] = df.groupby(["date"])["stop_triggered_temp"].ffill()

    df.loc[df["no_stop_position_size"] > 0, "stop_triggered"] = df.loc[
        df["no_stop_position_size"] > 0, "stop_triggered"
    ].fillna(False)

    df.drop(columns=["stop_triggered_temp"], errors="ignore", inplace=True)

    def close_trade(group):
        if group["stop_triggered"].any():
            stop_idx = group[
                "stop_triggered"
            ].idxmax()  # Get the index of the first stop trigger
            stop_value = group.loc[
                stop_idx, "no_stop_pnl"
            ]  # Get the PNL at the stop trigger
            group.loc[group.index >= stop_idx, "pnl_with_stop"] = stop_value
        else:
            group["pnl_with_stop"] = group[
                "no_stop_pnl"
            ]  # If no stop, PNL is just no_stop_pnl

        eod_mask = group["time_cst"] >= END_OF_DAY_TIME
        group.loc[
            eod_mask,
            [
                "no_stop_position_size",
                "trade_open_price",
                "no_stop_pnl",
                "pnl_with_stop",
            ],
        ] = 0
        group.loc[eod_mask, "stop_triggered"] = (
            False  # Stop is no longer active after EOD
        )
        return group

    df["pnl_with_stop"] = df["no_stop_pnl"]  # Initialize pnl_with_stop
    df = df.groupby("date", group_keys=False).apply(close_trade, include_groups=False)
    df["flat_time"] = df["time_cst"] >= END_OF_DAY_TIME
    return df


all_processed_dfs = []

for file_path in all_csv_files:
    print(f"Processing {file_path.name}")
    df_pl = pl.read_csv(
        file_path, try_parse_dates=False, schema_overrides={"window_start": pl.Int64}
    )

    df_pl = df_pl.with_columns(
        [
            pl.col("window_start")
            .cast(pl.Datetime(time_unit="ns"))
            .dt.replace_time_zone("UTC")
            .alias("window_start_utc"),
        ]
    )

    df_pl = df_pl.with_columns(
        [
            pl.col("window_start_utc")
            .dt.convert_time_zone("America/New_York")
            .alias("window_start_cst")
        ]
    ).with_columns(
        [
            pl.col("window_start_cst").dt.time().alias("time_cst"),
            pl.col("window_start_cst").dt.date().alias("date"),
        ]
    )

    df_pl = df_pl.sort(["window_start_cst", "ticker"])

    if prev_day_close_cache:
        prev_df_pl = pl.DataFrame(
            {
                "ticker": list(prev_day_close_cache.keys()),
                "prev_close_1d": list(prev_day_close_cache.values()),
            }
        ).with_columns(pl.col("prev_close_1d").cast(pl.Float64))
        df_pl = df_pl.join(prev_df_pl, on="ticker", how="left")
    else:
        df_pl = df_pl.with_columns(pl.lit(None, dtype=pl.Float64).alias("prev_close_1d"))

    df_pl = df_pl.with_columns(
        pl.col("close").shift(1).over("ticker").alias("prev_close_5m")
    )

    daily_agg_for_cache = df_pl.group_by(["date", "ticker"]).agg(
        pl.col("close").last().alias("close_1d")
    )

    prev_day_close_cache.update(
        dict(zip(daily_agg_for_cache["ticker"], daily_agg_for_cache["close_1d"]))
    )

    for col in ["open", "high", "low", "close"]:
        df_pl = df_pl.with_columns(
            [
                (pl.col(col) - pl.col("prev_close_1d")).alias(f"{col}_minus_prev_close"),
                ((pl.col(col) - pl.col("prev_close_1d")) / pl.col("prev_close_1d")).alias(
                    f"{col}_minus_prev_close_pct_chg"
                ),
            ]
        )

    df_pl = df_pl.with_columns(
        (pl.col("high_minus_prev_close_pct_chg") > PCT_CHANGE_TRIGGER).alias(
            "trigger_condition_pct"
        )
    )

    df_pl = df_pl.with_columns(
        pl.col("trigger_condition_pct").forward_fill().over(["ticker", "date"])
    ).with_columns(
        pl.col("trigger_condition_pct").fill_null(False)  # Fill initial nulls with False
    )

    df_pl = df_pl.with_columns(
        pl.col("time_cst")
        .is_between(dtime(3, 0), dtime(13, 45))
        .alias("trigger_within_time_range")
    )

    df_pd = df_pl.to_pandas()

    df_processed_pd = df_pd.groupby("ticker", group_keys=False).apply(track_trades)

    all_processed_dfs.append(df_processed_pd)


if all_processed_dfs:
    final_df = pd.concat(all_processed_dfs).reset_index(drop=True)

    column_order = [
        "window_start",
        "window_start_utc",
        "window_start_cst",
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
        "flat_time",
    ]

    existing_columns = [col for col in column_order if col in final_df.columns]
    final_df = final_df[existing_columns]

    output_filename = (
        f"harold_short_strategy_{datetime.now().strftime('%Y-%m-%d__%H-%M-%S')}.parquet"
    )
    final_df.to_parquet(os.path.join(PROCESSED_DIR, output_filename))
    print(
        f"Processing complete. Results saved to: {os.path.join(PROCESSED_DIR, output_filename)}"
    )
else:
    print("No data processed... Check paths...")
