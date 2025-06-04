import pandas as pd
import numpy as np
from datetime import time

from ds_core.db_connectors import PostgresConnect


def load_split_dividend_events():
    with PostgresConnect() as db:
        df = db.run_sql(
            """
            SELECT
                ticker, execution_date AS event_date, split_from, split_to, NULL::numeric AS cash_amount, 'split' AS event_type
            FROM
                tap_polygon_production.splits
            
            UNION ALL
            
            SELECT
                ticker, ex_dividend_date AS event_date, NULL AS split_from, NULL AS split_to, cash_amount, 'dividend' AS event_type
            FROM
                tap_polygon_production.dividends
            WHERE
                UPPER(currency) = 'USD'
            """
        )
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.tz_localize("America/New_York")
    return df

def likely_adjusted_state(open_, prev_close, adj, pct_tolerance):
    gap_to_adj = np.abs(open_ - adj)
    gap_to_raw = np.abs(open_ - prev_close)
    state = np.full(open_.shape, "", dtype="object")
    valid = (~pd.isna(open_)) & (~pd.isna(prev_close)) & (~pd.isna(adj))
    mask_both_zero = valid & (gap_to_adj == 0) & (gap_to_raw == 0)
    state[mask_both_zero] = "unknown"
    mask_gap_to_adj_zero = valid & (gap_to_adj == 0) & (gap_to_raw != 0)
    mask_gap_to_raw_zero = valid & (gap_to_raw == 0) & (gap_to_adj != 0)
    state[mask_gap_to_adj_zero] = "adjusted"
    state[mask_gap_to_raw_zero] = "unadjusted"

    mask_standard = valid & ~(mask_both_zero | mask_gap_to_adj_zero | mask_gap_to_raw_zero)
    best = np.where(gap_to_adj < gap_to_raw, gap_to_adj, gap_to_raw)
    worst = np.where(gap_to_adj < gap_to_raw, gap_to_raw, gap_to_adj)
    label = np.where(gap_to_adj < gap_to_raw, "adjusted", "unadjusted")

    # Avoid division by zero
    mask_worst_zero = mask_standard & (worst == 0)
    state[mask_worst_zero] = np.where(best[mask_worst_zero] == 0, "unknown", label[mask_worst_zero])

    mask_normal = mask_standard & (worst != 0)
    ratio = np.full(open_.shape, np.nan)
    ratio[mask_normal] = best[mask_normal] / worst[mask_normal]
    mask_label = mask_normal & (ratio <= pct_tolerance)
    mask_unknown = mask_normal & (ratio > pct_tolerance)
    state[mask_label] = label[mask_label]
    state[mask_unknown] = "unknown"
    return state

def price_gap_with_split_or_dividend(
    df_prev, df_cur, df_splits_dividends, pct_tolerance=0.65
):
    # --- Prepare previous close ---
    df_prev = df_prev.copy()
    prev_close = (
        df_prev[df_prev["timestamp_cst"].dt.time <= time(15, 0)]
        .groupby(["ticker", "date"], sort=False)
        .agg(prev_close=("close", "last"))
        .reset_index()
        .rename(columns={"date": "prev_date"})
    )

    # --- Prepare current opens (market and pre-market) ---
    df_cur = df_cur.copy()
    mkt_open = (
        df_cur[df_cur["timestamp_cst"].dt.time >= time(8, 30)]
        .groupby(["ticker", "date"], sort=False)
        .agg(market_open=("open", "first"))
        .reset_index()
    )
    pmkt_open = (
        df_cur[df_cur["timestamp_cst"].dt.time >= time(3, 0)]
        .groupby(["ticker", "date"], sort=False)
        .agg(pre_market_open=("open", "first"))
        .reset_index()
    )
    df_grouped = pd.merge(mkt_open, pmkt_open, on=["ticker", "date"], how="outer")

    # --- Merge prev_close ---
    prev_dates = df_prev.groupby("ticker")["date"].max().rename("prev_date").reset_index()
    df_grouped = df_grouped.merge(prev_dates, on=["ticker"], how="left")

    assert df_grouped[df_grouped["prev_date"] > df_grouped["date"]].shape[0] == 0, "Error joining prev df to cur df, prev dates must be less than cur date!"

    df_grouped = df_grouped.merge(
        prev_close,
        on=["ticker", "prev_date"],
        how="left",
        sort=False,
    )

    # --- Merge event info ---
    df_splits_dividends = df_splits_dividends.copy()
    df_grouped = df_grouped.merge(
        df_splits_dividends,
        left_on=["ticker", "date"],
        right_on=["ticker", "event_date"],
        how="left",
    )
    df_grouped["had_split_or_dividend"] = df_grouped["event_type"].notnull()
    df_grouped["split_from"] = df_grouped["split_from"].fillna(1.0)
    df_grouped["split_to"] = df_grouped["split_to"].fillna(1.0)
    df_grouped["cash_amount"] = df_grouped["cash_amount"].fillna(0.0)
    df_grouped["split_ratio"] = df_grouped["split_from"] / df_grouped["split_to"]

    # --- Calculate expected df_grouped ---
    df_grouped["expected_unchanged_open_split"] = df_grouped["prev_close"] * df_grouped["split_ratio"]
    df_grouped["expected_unchanged_open_split_div"] = df_grouped["expected_unchanged_open_split"] - df_grouped["cash_amount"]

    # --- Calculate percent changes ---
    df_grouped["raw_mkt_pct_chg"] = (df_grouped["market_open"] - df_grouped["prev_close"]) / df_grouped["prev_close"]
    df_grouped["raw_pre_mkt_pct_chg"] = (df_grouped["pre_market_open"] - df_grouped["prev_close"]) / df_grouped["prev_close"]

    # placeholder
    df_grouped["adj_mkt_pct_chg"] = df_grouped["raw_mkt_pct_chg"]
    df_grouped["adj_pre_mkt_pct_chg"] = df_grouped["raw_pre_mkt_pct_chg"]

    mask = df_grouped["had_split_or_dividend"]

    # For splits denom = expected_unchanged_open_split, for dividends denom = expected_unchanged_open_split_div
    adj_mkt_denom = np.where(
        df_grouped["event_type"] == "split",
        df_grouped["expected_unchanged_open_split"],
        df_grouped["expected_unchanged_open_split_div"],
    )
    adj_pre_mkt_denom = adj_mkt_denom.copy()

    df_grouped.loc[mask, "adj_mkt_pct_chg"] = (
        (df_grouped.loc[mask, "market_open"].values - adj_mkt_denom[mask]) / adj_mkt_denom[mask]
    )
    df_grouped.loc[mask, "adj_pre_mkt_pct_chg"] = (
        (df_grouped.loc[mask, "pre_market_open"].values - adj_pre_mkt_denom[mask]) / adj_pre_mkt_denom[mask]
    )

    # --- likely_adjusted_state for both market and pre-market ---
    adj_for_mkt_state = np.where(
        df_grouped["event_type"] == "split",
        df_grouped["expected_unchanged_open_split"],
        df_grouped["expected_unchanged_open_split_div"],
    )
    adj_for_pre_mkt_state = adj_for_mkt_state.copy()

    df_grouped["mkt_likely_adjusted_state"] = likely_adjusted_state(
        df_grouped["market_open"].values,
        df_grouped["prev_close"].values,
        adj_for_mkt_state,
        pct_tolerance,
    )
    df_grouped["pre_mkt_likely_adjusted_state"] = likely_adjusted_state(
        df_grouped["pre_market_open"].values,
        df_grouped["prev_close"].values,
        adj_for_pre_mkt_state,
        pct_tolerance,
    )
    df_grouped.loc[~mask, "mkt_likely_adjusted_state"] = None
    df_grouped.loc[~mask, "pre_mkt_likely_adjusted_state"] = None

    # --- Organize columns for output ---
    outcols = [
        "ticker",
        "date",
        "event_date",
        "market_open",
        "pre_market_open",
        "prev_date",
        "prev_close",
        "raw_mkt_pct_chg",
        "raw_pre_mkt_pct_chg",
        "adj_mkt_pct_chg",
        "adj_pre_mkt_pct_chg",
        "had_split_or_dividend",
        "event_type",
        "split_from",
        "split_to",
        "cash_amount",
        "split_ratio",
        "expected_unchanged_open_split",
        "expected_unchanged_open_split_div",
        "mkt_likely_adjusted_state",
        "pre_mkt_likely_adjusted_state",
    ]
    return df_grouped[outcols]

def pull_and_clean_data(cur_day_file: str) -> pd.DataFrame:
    df = pd.read_csv(cur_day_file, compression="gzip")
    df = df.rename(columns={"window_start": "timestamp"})
    df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True)
    df["timestamp_cst"] = df["timestamp_utc"].dt.tz_convert("America/New_York")
    df["date"] = df["timestamp_cst"].dt.normalize()
    df = df.sort_values(by=["timestamp_cst", "ticker"])
    return df


df_prev = pull_and_clean_data("/Users/melgazar9/polygon_data/bars_1min/bars_1m_2025-05-14.csv.gz")
df_cur = pull_and_clean_data("/Users/melgazar9/polygon_data/bars_1min/bars_1m_2025-05-15.csv.gz")
df_splits_dividends = load_split_dividend_events()

df_adj_chg = price_gap_with_split_or_dividend(df_prev, df_cur, df_splits_dividends)
