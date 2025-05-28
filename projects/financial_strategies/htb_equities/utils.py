import pandas as pd

from ds_core.db_connectors import PostgresConnect

### --- Helper Functions --- ###


def pull_and_clean_data(file_path: str) -> pd.DataFrame:
    """
    Pulls data from a CSV file and returns it as a Pandas DataFrame.
    """
    if not file_path.endswith(".csv.gz"):
        raise ValueError("File must be a .csv.gz file")

    df = pd.read_csv(file_path, parse_dates=True, compression="gzip")
    df = df.rename(columns={"window_start": "timestamp"})
    df["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True)
    df["timestamp_cst"] = df["timestamp_utc"].dt.tz_convert("America/New_York")
    df["date"] = df["timestamp_cst"].dt.date
    df["time"] = df["timestamp_cst"].dt.time

    # need to account for splits and dividends
    db = PostgresConnect()
    db.connect()
    # query = """
    #     SELECT
    # """

    return df
