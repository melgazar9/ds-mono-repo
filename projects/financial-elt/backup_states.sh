#!/bin/bash

# Simple state backup script
# Usage: ./simple_backup_states.sh [environment]

ENVIRONMENT=${1:-${ENVIRONMENT:-production}}
TAPS=("tap-yfinance" "tap-yahooquery" "tap-fmp" "tap-polygon" "tap-fred")

echo "Backing up states for environment: $ENVIRONMENT"
mkdir -p saved_states

for tap in "${TAPS[@]}"; do
    echo "Processing $tap..."

    if [ -d "$tap" ]; then
        cd "$tap"

        # Get state list
        states=$(meltano --environment "$ENVIRONMENT" state list 2>/dev/null | grep -v "^{" | grep -v "level" | grep -v "timestamp" | grep -v "event")

        if [ -n "$states" ]; then
            mkdir -p "../saved_states/$tap"

            # Save each state
            while IFS= read -r state_id; do
                if [ -n "$state_id" ]; then
                    echo "  Saving state: $state_id"
                    meltano --environment "$ENVIRONMENT" state get "$state_id" > "../saved_states/$tap/${state_id}_${ENVIRONMENT}.json"
                fi
            done <<< "$states"
        else
            echo "  No states found for $tap"
        fi

        cd ..
    else
        echo "  Directory $tap not found, skipping..."
    fi
done

echo "Done! States saved to saved_states/"
