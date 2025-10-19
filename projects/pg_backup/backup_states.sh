#!/bin/bash

# State backup script - backs up from local state/ directories
# Usage: ./backup_states.sh [environment]

ENVIRONMENT=${1:-${ENVIRONMENT:-production}}
TAPS=("tap-yfinance" "tap-yahooquery" "tap-fmp" "tap-polygon" "tap-fred")

echo "Backing up states for environment: $ENVIRONMENT"
mkdir -p saved_states

for tap in "${TAPS[@]}"; do
    echo "Processing $tap..."

    if [ -d "$tap/state" ]; then
        state_count=0
        mkdir -p "saved_states/$tap"

        # Copy all state directories
        for state_dir in "$tap/state"/*; do
            if [ -d "$state_dir" ]; then
                state_name=$(basename "$state_dir")
                echo "  Backing up state: $state_name"

                # Copy the entire state directory
                cp -r "$state_dir" "saved_states/$tap/${state_name}_${ENVIRONMENT}"
                ((state_count++))
            fi
        done

        if [ $state_count -eq 0 ]; then
            echo "  No states found for $tap"
        else
            echo "  Backed up $state_count states"
        fi
    else
        echo "  No state directory found for $tap"
    fi
done

echo "Done! States saved to saved_states/"
