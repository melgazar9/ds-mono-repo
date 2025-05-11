from ds_core.db_connectors import *
from tap_yfinance.price_utils import clean_strings
import sys
import pandas as pd
import yfinance as yf

home_path = os.path.expanduser("~")

sys.path.append(
    f"{home_path}/scripts/github/personal/ds-mono-repo/projects/financial_elt/tap-yfinance/.meltano/extractors/tap-yfinance/venv/lib/python3.9/site-packages"
)

db = PostgresConnect()
db.connect()

tables_query = """
    select distinct table_name
    from information_schema.tables
    where table_schema = 'tap_yfinance_dev'
    and table_type = 'BASE TABLE';
"""

tables = db.run_sql(tables_query)["table_name"].tolist()
tables = [i for i in tables if "_prices_" not in i and "_tickers" not in i]
ignore_cols = [
    "timestamp",
    "timestamp_tz_aware",
    "timezone",
    "ticker",
    "_sdc_extracted_at",
    "_sdc_received_at",
    "_sdc_batched_at",
    "_sdc_deleted_at",
    "_sdc_sequence",
    "_sdc_table_version",
    "_sdc_sync_started_at",
]

df_missing_yf = pd.DataFrame(columns=["table", "column"])
df_missing_db = df_missing_yf.copy()

ticker = "AAPL"
t = yf.Ticker(ticker)

for table in tables:
    try:
        if table in ["option_chain", "shares_full"]:
            # df_yfinance = getattr(t, f"get_{table}")()
            print(f"skipping table {table} --- check it manually")
            continue
        if table not in [
            "options",
            "quarterly_balance_sheet",
            "quarterly_cash_flow",
            "quarterly_financials",
            "quarterly_income_stmt",
        ]:
            df_yfinance = getattr(t, f"get_{table}")()
        else:
            if table == "options":
                print("skipping options -- should just return a tuple of dates")
                continue

            df_yfinance = getattr(t, f"{table}")

    except Exception:
        raise ValueError(
            f"Attribute {table} does not exist in the yfinance library anymore!"
        )

    if table in ["fast_info"]:
        cols = pd.DataFrame(df_yfinance).T.iloc[0].values
        df_yfinance = pd.DataFrame(pd.DataFrame(df_yfinance).T, columns=cols)

    if not isinstance(df_yfinance, pd.DataFrame):
        if table not in ["history_metadata"]:
            df_yfinance = pd.DataFrame(df_yfinance)

        if table in ["history_metadata"]:
            df_yfinance = pd.DataFrame.from_dict(df_yfinance, orient="index").T

    try:
        df_postgres = db.run_sql(f"select * from tap_yfinance_dev.{table} limit 1")
        df_postgres = df_postgres[
            [i for i in df_postgres.columns if i not in ignore_cols]
        ]
    except Exception:
        raise ValueError(
            f"Attribute tap_yfinance_dev.{table} does not exist in the postgres db!"
        )

    try:
        # if table is wide
        df_missing_yf_i = pd.DataFrame(
            {
                "column": [
                    i
                    for i in clean_strings(df_postgres.columns)
                    if i not in clean_strings(df_yfinance.columns)
                ]
            }
        )
        df_missing_yf_i["table"] = table
        df_missing_yf_i = df_missing_yf_i[["table", "column"]]

        df_missing_db_i = pd.DataFrame(
            {
                "column": [
                    i
                    for i in clean_strings(df_yfinance.columns)
                    if i not in clean_strings(df_postgres.columns)
                ]
            }
        )
        df_missing_db_i["table"] = table
        df_missing_db_i = df_missing_db_i[["table", "column"]]
    except Exception:
        try:
            # if table is long
            df_missing_yf_i = pd.DataFrame(
                {
                    "column": [
                        i
                        for i in clean_strings(df_postgres.columns)
                        if i not in clean_strings(df_yfinance.index)
                    ]
                }
            )
            df_missing_yf_i["table"] = table
            df_missing_yf_i = df_missing_yf_i[["table", "column"]]

            df_missing_db_i = pd.DataFrame(
                {
                    "column": [
                        i
                        for i in clean_strings(df_yfinance.index)
                        if i not in clean_strings(df_postgres.columns)
                    ]
                }
            )
            df_missing_db_i["table"] = table
            df_missing_db_i = df_missing_db_i[["table", "column"]]
        except Exception:
            raise ValueError(f"table {table} failed")

    df_missing_yf = pd.concat([df_missing_yf, df_missing_yf_i])
    df_missing_db = pd.concat([df_missing_db, df_missing_db_i])

    print(f"done running {table}")
