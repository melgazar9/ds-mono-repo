#!/bin/bash

# Execute the original command
exec "$@"

# Determine the tap and create the symlink
for arg in "$@"; do
  if [[ "$arg" == "tap-polygon" ]]; then
    find state/*/state.json -exec ln -sf {} /app/tap-polygon/state/state.json \;
    break
  elif [[ "$arg" == "tap-yfinance" ]]; then
    find state/*/state.json -exec ln -sf {} /app/tap-yfinance/state/state.json \;
    break
  fi
done