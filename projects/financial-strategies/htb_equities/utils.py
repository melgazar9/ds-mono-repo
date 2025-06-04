import pandas as pd

from ds_core.db_connectors import PostgresConnect


def load_split_dividend_events():
    with PostgresConnect() as db:
        df_events = db.run_sql("""
            SELECT ticker, execution_date AS event_date, split_from, split_to, NULL::numeric AS cash_amount, 'split' AS event_type
            FROM tap_polygon_production.splits
            UNION ALL
            SELECT ticker, ex_dividend_date AS event_date, NULL AS split_from, NULL AS split_to, cash_amount, 'dividend' AS event_type
            FROM tap_polygon_production.dividends
        """)
    df_events['event_date'] = pd.to_datetime(df_events['event_date'])
    return df_events



def price_gap_with_split_or_dividend(
    df_prev, df_cur, df_div_and_splits, pct_tolerance=0.65
):
    """
    Calculates percent change from prev close to current open for all tickers/dates,
    with flags for split/dividend event and for likely adjustment (adjusted/unadjusted/unknown).
    - likely_adjusted_state is "adjusted", "unadjusted", or "unknown" based on which scale the open is closer to.
    Args:
        df_prev: DataFrame of previous day's bars (must have 'timestamp_cst', 'ticker', 'close').
        df_cur: DataFrame of current day's bars (must have 'timestamp_cst', 'ticker', 'open').
        df_div_and_splits: DataFrame with split/dividend events (must have 'ticker', 'event_date',
                           'split_from', 'split_to', 'cash_amount', 'event_type').
        pct_tolerance: float, if closer of the two scales is not at least (1-pct_tolerance) closer, returns 'unknown'.
    Returns:
        DataFrame: One row per (ticker, date) with percent changes and event/adjustment flags.
    """
    def _likely_adjusted_state(open_, prev_close, adj, pct_tolerance):
        gap_to_adj = abs(open_ - adj)
        gap_to_raw = abs(open_ - prev_close)
        if pd.isna(open_) or pd.isna(prev_close) or pd.isna(adj):
            return "unknown"
        if gap_to_adj == 0 and gap_to_raw == 0:
            return "unknown"
        if gap_to_adj < gap_to_raw:
            best = gap_to_adj
            worst = gap_to_raw
            label = "adjusted"
        else:
            best = gap_to_raw
            worst = gap_to_adj
            label = "unadjusted"
        # If best is not sufficiently better than worst, declare unknown
        if worst == 0:  # avoid division by zero
            if best == 0:
                return "unknown"
            return label
        if best / worst > pct_tolerance:
            return "unknown"
        else:
            return label

    # --- Prepare previous close at session end ---
    df_prev = df_prev.copy()
    df_prev['timestamp_cst'] = pd.to_datetime(df_prev['timestamp_cst'])
    df_prev['date'] = df_prev['timestamp_cst'].dt.normalize()
    prev_close = (
        df_prev[df_prev['timestamp_cst'].dt.time <= time(15, 0)]
        .groupby(['ticker', 'date'], sort=False)
        .agg(prev_close=('close', 'last'))
        .reset_index()
        .rename(columns={'date': 'prev_date'})
    )

    # --- Prepare current open at session start ---
    df_cur = df_cur.copy()
    df_cur['timestamp_cst'] = pd.to_datetime(df_cur['timestamp_cst'])
    df_cur['date'] = df_cur['timestamp_cst'].dt.normalize()
    cur_open = (
        df_cur[df_cur['timestamp_cst'].dt.time >= time(8, 30)]
        .groupby(['ticker', 'date'], sort=False)
        .agg(open=('open', 'first'))
        .reset_index()
    )

    # --- Merge to get prev_close for each ticker/date ---
    cur_open['prev_date'] = cur_open['date'] - pd.Timedelta(days=1)
    prev_close['prev_date'] = pd.to_datetime(prev_close['prev_date'])
    cur_open['prev_date'] = pd.to_datetime(cur_open['prev_date'])
    prev_close['prev_date'] = prev_close['prev_date'].dt.tz_localize(None)
    cur_open['prev_date'] = cur_open['prev_date'].dt.tz_localize(None)
    merged = cur_open.merge(
        prev_close, on=['ticker', 'prev_date'], how='left', sort=False
    )

    # --- Add event info and flags ---
    df_div_and_splits = df_div_and_splits.copy()
    df_div_and_splits['event_date'] = pd.to_datetime(df_div_and_splits['event_date']).dt.normalize()
    merged['date'] = merged['date'].dt.tz_localize(None)
    df_div_and_splits['event_date'] = df_div_and_splits['event_date'].dt.tz_localize(None)
    merged = merged.merge(
        df_div_and_splits[['ticker', 'event_date', 'split_from', 'split_to', 'cash_amount', 'event_type']],
        left_on=['ticker', 'date'],
        right_on=['ticker', 'event_date'],
        how='left'
    )
    merged['had_split_or_dividend'] = merged['event_type'].notnull()

    # --- Fill missing values for split/dividend ---
    merged['split_from'] = merged['split_from'].fillna(1.0)
    merged['split_to'] = merged['split_to'].fillna(1.0)
    merged['cash_amount'] = merged['cash_amount'].fillna(0.0)

    # --- Calculate split ratio ---
    merged['split_ratio'] = merged['split_from'] / merged['split_to']

    # --- Calculate expected opens ---
    merged['expected_open_split'] = merged['prev_close'] * merged['split_ratio']
    merged['expected_open_split_div'] = merged['expected_open_split'] - merged['cash_amount']

    # --- Calculate percent changes ---
    merged['raw_pct_chg'] = (merged['open'] - merged['prev_close']) / merged['prev_close']
    merged['adj_pct_chg'] = merged['raw_pct_chg']  # default: no adjustment needed
    mask = merged['had_split_or_dividend']
    merged.loc[mask, 'adj_pct_chg'] = (
        (merged.loc[mask, 'open'] - merged.loc[mask, 'expected_open_split_div']) /
        merged.loc[mask, 'expected_open_split_div']
    )

    # --- Robust likely_adjusted_state for split/dividend days ---
    likely_adj_state = []
    for i, row in merged.iterrows():
        if not row['had_split_or_dividend']:
            likely_adj_state.append("")
        else:
            if row['event_type'] == 'split':
                adj = row['expected_open_split']
            elif row['event_type'] == 'dividend':
                adj = row['expected_open_split_div']
            else:
                adj = row['expected_open_split_div']
            likely_adj_state.append(
                _likely_adjusted_state(row['open'], row['prev_close'], adj, pct_tolerance)
            )
    merged['likely_adjusted_state'] = likely_adj_state

    # --- Organize columns for output ---
    out_cols = [
        'ticker', 'date', 'open', 'prev_date', 'prev_close', 'raw_pct_chg',
        'adj_pct_chg', 'had_split_or_dividend', 'event_type',
        'split_from', 'split_to', 'cash_amount', 'split_ratio',
        'expected_open_split', 'expected_open_split_div',
        'likely_adjusted_state'
    ]
    return merged[out_cols]




def pull_and_clean_data(file_loc):
    df = pd.read_csv(file_loc, compression='gzip')
    df = df.rename(columns={"window_start": "timestamp"})
    df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True)
    df["timestamp_cst"] = df["timestamp_utc"].dt.tz_convert("America/New_York")
    df["date"] = df["timestamp_cst"].dt.date
    df["time"] = df["timestamp_cst"].dt.time
    return df








#
#
#
#
# with PostgresConnect() as db:
#     df_div_and_splits = db.run_sql("""
#         SELECT
#             coalesce(s.ticker, d.ticker) as ticker,
#             s.execution_date as split_date,
#             s.split_from,
#             s.split_to,
#             d.ex_dividend_date,
#             d.pay_date as dividend_pay_date,
#             d.record_date as dividend_record_date,
#             d.cash_amount as dividend_cash_amount,
#             d.dividend_type,
#             d.frequency as dividend_frequency,
#             d.currency as dividend_currency
#         FROM
#             tap_polygon_production.splits s
#         full join
#             tap_polygon_production.dividends d
#         on
#             s.ticker = d.ticker
#             and s.execution_date = d.ex_dividend_date
#     """)
#
# def pull_and_clean_data(file_loc) -> pd.DataFrame:
#     """
#     Pulls data from polygon flat files 1-min bars --> returns a pandas df with adjusted OHLCV.
#     NOTE: Polygon raw data is completely unreliable in terms of splits and dividends, so for a first iteration
#     I'll just ignore all dates that have splits or dividends.
#     """
#     # Read 1-min bar data
#     df = pd.read_csv(file_loc, compression='gzip')
#     df = df.rename(columns={"window_start": "timestamp"})
#     df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True)
#     df["timestamp_cst"] = df["timestamp_utc"].dt.tz_convert("America/New_York")
#     df["date"] = df["timestamp_cst"].dt.date
#     df["time"] = df["timestamp_cst"].dt.time
#
#     # fix prices for splits and dividends
#     return df
