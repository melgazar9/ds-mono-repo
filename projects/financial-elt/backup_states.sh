#!/bin/bash

# Meltano State Backup Script
# Usage: ./backup_states.sh [environment]

# Check if we should log to file and redirect all output
if [[ -z "$BACKUP_LOGGING_ACTIVE" ]]; then
    export BACKUP_LOGGING_ACTIVE=1
    SCRIPT_DIR="$(dirname "$0")"
    mkdir -p "$SCRIPT_DIR/saved_states/backup_logs"
    LOGFILE="$SCRIPT_DIR/saved_states/backup_logs/meltano_state_backup_$(date +%Y%m%d_%H%M%S).log"

    # Track start time
    START_TIME=$(date +%s)

    # Re-run this script with output redirected to log file
    "$0" "$@" > "$LOGFILE" 2>&1
    EXIT_CODE=$?

    # Calculate execution time
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    HOURS=$((DURATION / 3600))
    MINUTES=$(((DURATION % 3600) / 60))
    SECONDS=$((DURATION % 60))

    # Append execution time to log
    echo "" >> "$LOGFILE"
    echo "========================================" >> "$LOGFILE"
    echo "State backup completed at: $(date)" >> "$LOGFILE"
    printf "Total execution time: %02d:%02d:%02d (%d seconds)\n" $HOURS $MINUTES $SECONDS $DURATION >> "$LOGFILE"
    echo "========================================" >> "$LOGFILE"

    # Copy log to /mnt/backup if directory exists
    if [[ -d "/mnt/backup/saved_states" ]]; then
        cp "$LOGFILE" /mnt/backup/saved_states/ 2>/dev/null || true
    fi

    exit $EXIT_CODE
fi

ENVIRONMENT=${1:-${ENVIRONMENT:-production}}
TAPS=("tap-yfinance" "tap-yahooquery" "tap-fmp" "tap-polygon" "tap-fred")

echo "=========================================="
echo "Meltano State Backup Script"
echo "=========================================="
echo "Environment: $ENVIRONMENT"
echo "Timestamp: $(date)"
echo "=========================================="
echo ""

mkdir -p saved_states

# Track backup statistics
TOTAL_STATES=0
TOTAL_TAPS=0
FAILED_TAPS=0

for tap in "${TAPS[@]}"; do
    echo "Processing $tap..."

    if [ -d "$tap" ]; then
        cd "$tap"

        # Get state list
        states=$(meltano --environment "$ENVIRONMENT" state list 2>/dev/null | grep -v "^{" | grep -v "level" | grep -v "timestamp" | grep -v "event")

        if [ -n "$states" ]; then
            mkdir -p "../saved_states/$tap"
            TAP_STATE_COUNT=0

            # Save each state
            while IFS= read -r state_id; do
                if [ -n "$state_id" ]; then
                    echo "  ✓ Saving state: $state_id"
                    meltano --environment "$ENVIRONMENT" state get "$state_id" > "../saved_states/$tap/${state_id}_${ENVIRONMENT}.json"
                    ((TAP_STATE_COUNT++))
                    ((TOTAL_STATES++))
                fi
            done <<< "$states"

            echo "  → Saved $TAP_STATE_COUNT state(s) for $tap"
            ((TOTAL_TAPS++))
        else
            echo "  ⚠ No states found for $tap"
        fi

        cd ..
    else
        echo "  ✗ Directory $tap not found, skipping..."
        ((FAILED_TAPS++))
    fi
    echo ""
done

echo "=========================================="
echo "Backup Summary"
echo "=========================================="
echo "Total taps processed: $TOTAL_TAPS"
echo "Total states backed up: $TOTAL_STATES"
echo "Failed/skipped taps: $FAILED_TAPS"
echo "States saved to: $(pwd)/saved_states/"
echo ""

# Copy saved_states to /mnt/backup if it exists
if [[ -d "/mnt/backup" ]]; then
    echo "Copying states to /mnt/backup/saved_states..."
    mkdir -p /mnt/backup/saved_states
    rsync -av saved_states/ /mnt/backup/saved_states/ 2>&1 | grep -v "sending incremental file list" || true
    echo "✓ States copied to /mnt/backup/saved_states/"
fi

echo "Done!"
