#!/bin/bash

TAPS="tap-polygon|tap-yfinance|tap-yahooquery|tap-fmp|tap-fred"

for arg in "$@"; do
  if [[ "$arg" =~ ^($TAPS)$ ]]; then
    find state/*/state.json -exec ln -sf {} "/app/$arg/state/state.json" \;
    break
  fi
done

exec "$@"