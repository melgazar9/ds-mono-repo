import pandas as pd

from ds_core.db_connectors import PostgresConnect

def pull_and_clean_data() -> pd.DataFrame:
    """
    Pulls data from polygon flat files 1-min bars --> returns a pandas df with adjusted OHLCV.
    """
    # Read 1-min bar data
    with PostgresConnect() as pg_conn:
        df = pg_conn.read_sql(
            "SELECT * FROM stock_bars_1_minute WHERE date(window_start) >= '2018-01-01' AND date(window_start) < '2023-01-01'"
        )

    df = df.rename(columns={"window_start": "timestamp"})
    df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True)
    df["timestamp_cst"] = df["timestamp_utc"].dt.tz_convert("America/New_York")
    df["date"] = df["timestamp_cst"].dt.date
    df["time"] = df["timestamp_cst"].dt.time
    return df