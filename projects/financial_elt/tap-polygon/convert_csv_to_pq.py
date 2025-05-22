from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import os

import pandas as pd

INPUT_DIRS = [
    Path("outputs/us_stocks_sip/bars_1m"),
    Path("outputs/us_stocks_sip/eod"),
    Path("outputs/us_stocks_sip/trades"),
]

MAX_WORKERS = 2


def convert_csv_gz_to_parquet(csv_gz_path_str):
    csv_gz_path = Path(csv_gz_path_str)

    try:
        df = pd.read_csv(csv_gz_path, compression="gzip", low_memory=False)

        output_dir = csv_gz_path.parent / "parquet_outputs"
        output_dir.mkdir(parents=True, exist_ok=True)

        if csv_gz_path.name.endswith(".csv.gz"):
            parquet_filename = csv_gz_path.name[:-7] + ".parquet"
        else:
            # fallback, just replace last suffix
            parquet_filename = csv_gz_path.with_suffix(".parquet").name

        parquet_path = output_dir / parquet_filename
        df.to_parquet(parquet_path, engine="pyarrow", index=False)
        return f"✔️ {csv_gz_path} → {parquet_path}"
    except Exception as e:
        return f"❌ Failed on {csv_gz_path}: {e}"


def batch_convert_parallel():
    all_files = []

    for dir_path in INPUT_DIRS:
        all_files.extend(dir_path.rglob("*.csv.gz"))

    print(f"Found {len(all_files)} files...")

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for result in executor.map(convert_csv_gz_to_parquet, all_files):
            print(result)


if __name__ == "__main__":
    batch_convert_parallel()

