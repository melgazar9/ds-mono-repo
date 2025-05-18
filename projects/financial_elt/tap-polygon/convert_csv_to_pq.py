from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pandas as pd

INPUT_DIRS = [
    Path("outputs/us_stocks_sip/bars_1m"),
    Path("outputs/us_stocks_sip/eod"),
    Path("outputs/us_stocks_sip/trades"),
]


def convert_csv_gz_to_parquet(csv_gz_path_str):
    csv_gz_path = Path(csv_gz_path_str)
    try:
        df = pd.read_csv(csv_gz_path, compression="gzip")

        # Create 'parquet_outputs' directory inside the current file's directory
        output_dir = csv_gz_path.parent / "parquet_outputs"
        output_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = output_dir / csv_gz_path.with_suffix(".parquet").name
        df.to_parquet(parquet_path, engine="pyarrow", index=False)
        return f"✔️ {csv_gz_path} → {parquet_path}"
    except Exception as e:
        return f"❌ Failed on {csv_gz_path}: {e}"


def batch_convert_parallel():
    all_files = []
    for dir_path in INPUT_DIRS:
        all_files.extend(dir_path.rglob("*.csv.gz"))

    print(f"Found {len(all_files)} files...")

    with ProcessPoolExecutor() as executor:
        for result in executor.map(convert_csv_gz_to_parquet, all_files):
            print(result)


if __name__ == "__main__":
    batch_convert_parallel()
