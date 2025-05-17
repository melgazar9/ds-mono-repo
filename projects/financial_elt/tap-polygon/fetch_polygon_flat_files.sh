#!/bin/bash

# Example run: ./fetch_polygon_flat_files.sh --days --start-date "2025-05-09" --end-date "2025-05-12"

if [[ -z "$POLYGON_FLAT_FILE_AWS_KEY" || -z "$POLYGON_FLAT_FILE_AWS_SECRET_KEY" ]]; then
  echo "Error: POLYGON_FLAT_FILE_AWS_KEY and POLYGON_FLAT_FILE_AWS_SECRET_KEY must be set"
  exit 1
fi

BASE_PATH="s3://flatfiles/us_stocks_sip"
ENDPOINT="--endpoint-url https://files.polygon.io"

usage() {
  echo "Usage: $0 --trades|--bars-1m|--days --start-date YYYY-MM-DD --end-date YYYY-MM-DD"
  exit 1
}

DATA_TYPE=""
START_DATE=""
END_DATE=""
PREFIX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --trades)
      DATA_TYPE="trades_v1"
      PREFIX="trades"
      shift
      ;;
    --bars-1m)
      DATA_TYPE="minute_aggs_v1"
      PREFIX="bars_1m"
      shift
      ;;
    --days)
      DATA_TYPE="day_aggs_v1"
      PREFIX="eod"
      shift
      ;;
    --start-date)
      START_DATE="$2"
      shift 2
      ;;
    --end-date)
      END_DATE="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

if [[ -z "$DATA_TYPE" || -z "$START_DATE" || -z "$END_DATE" ]]; then
  usage
fi

OUTPUT_DIR="./outputs/$PREFIX"
mkdir -p "$OUTPUT_DIR"

start_sec=$(date -d "$START_DATE" +%s)
end_sec=$(date -d "$END_DATE" +%s)

if (( end_sec < start_sec )); then
  echo "Error: end-date must be after or equal to start-date"
  exit 1
fi

current_sec=$start_sec

while (( current_sec <= end_sec )); do
  current_date=$(date -d "@$current_sec" +%Y-%m-%d)
  year=$(date -d "@$current_sec" +%Y)
  month=$(date -d "@$current_sec" +%m)

  s3_path="$BASE_PATH/$DATA_TYPE/$year/$month/$current_date.csv.gz"
  local_file="$OUTPUT_DIR/${PREFIX}_${current_date}.csv.gz"

  echo "Downloading $s3_path as $local_file"
  aws s3 cp "$s3_path" "$local_file" $ENDPOINT

  current_sec=$((current_sec + 86400))
done

