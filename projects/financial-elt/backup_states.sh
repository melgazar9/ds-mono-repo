#!/bin/bash

# Enhanced Meltano states backup script with argument parsing
# Usage:
#   ./backup_states.sh --all
#   ./backup_states.sh --extractors tap-fmp tap-polygon
#   ./backup_states.sh --extractors tap-yfinance

set -e

# Available taps and environments
AVAILABLE_TAPS=("tap-yfinance" "tap-yahooquery" "tap-fmp" "tap-polygon" "tap-fred")
AVAILABLE_ENVIRONMENTS=("dev" "production")

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --all                      Backup states for all available taps"
    echo "  --extractors TAP1 TAP2     Backup states for specified taps"
    echo "  --environment ENV          Meltano environment (dev or production)"
    echo "  -h, --help                Show this help message"
    echo ""
    echo "Available taps: ${AVAILABLE_TAPS[*]}"
    echo "Available environments: dev, production"
    echo ""
    echo "Examples:"
    echo "  $0 --all --environment dev"
    echo "  $0 --extractors tap-fmp tap-polygon --environment production"
    exit 1
}

# Function to validate tap name
validate_tap() {
    local tap=$1
    for available_tap in "${AVAILABLE_TAPS[@]}"; do
        if [[ "$tap" == "$available_tap" ]]; then
            return 0
        fi
    done
    echo "Error: Invalid tap '$tap'. Available taps: ${AVAILABLE_TAPS[*]}"
    exit 1
}

# Function to validate environment name
validate_environment() {
    local env=$1
    for available_env in "${AVAILABLE_ENVIRONMENTS[@]}"; do
        if [[ "$env" == "$available_env" ]]; then
            return 0
        fi
    done
    echo "Error: Invalid environment '$env'. Available environments: ${AVAILABLE_ENVIRONMENTS[*]}"
    exit 1
}

# Function to backup states for a specific tap
backup_tap_states() {
    local tap=$1
    local environment=$2
    echo "Processing tap: $tap (environment: $environment)"

    # Check if tap directory exists
    if [[ ! -d "$tap" ]]; then
        echo "Warning: Directory '$tap' not found, skipping..."
        return 1
    fi

    # Create tap-specific backup directory
    local tap_backup_dir="saved_states/$tap"
    mkdir -p "$tap_backup_dir"

    # Change to tap directory
    cd "$tap"

    # Get state list for this tap
    echo "  Getting state list for $tap..."
    local state_list_file="../${tap_backup_dir}/state_list_${environment}.json"

    if ! meltano --environment "$environment" state list > "$state_list_file" 2>/dev/null; then
        echo "  Warning: Failed to get state list for $tap, skipping..."
        cd ..
        return 1
    fi

    # Check if state list is empty
    if [[ ! -s "$state_list_file" ]]; then
        echo "  No states found for $tap"
        cd ..
        return 0
    fi

    # Backup each state
    local states_backed_up=0
    while read -r state_id; do
        if [[ ! -z "$state_id" && "$state_id" != "null" ]]; then
            echo "    Backing up state: $state_id"
            local state_file="../${tap_backup_dir}/${state_id}_${environment}.json"

            if meltano --environment "$environment" state get "$state_id" > "$state_file" 2>/dev/null; then
                ((states_backed_up++))
            else
                echo "    Warning: Failed to backup state '$state_id' for $tap"
                rm -f "$state_file" 2>/dev/null || true
            fi
        fi
    done < "$state_list_file"

    echo "  Backed up $states_backed_up states for $tap"
    cd ..
    return 0
}

# Parse command line arguments
SELECTED_TAPS=()
ENVIRONMENT=""

if [[ $# -eq 0 ]]; then
    usage
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --all)
            SELECTED_TAPS=("${AVAILABLE_TAPS[@]}")
            shift
            ;;
        --extractors)
            shift
            # Collect all tap names after --extractors
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                validate_tap "$1"
                SELECTED_TAPS+=("$1")
                shift
            done
            if [[ ${#SELECTED_TAPS[@]} -eq 0 ]]; then
                echo "Error: --extractors requires at least one tap name"
                usage
            fi
            ;;
        --environment)
            shift
            if [[ $# -eq 0 ]]; then
                echo "Error: --environment requires an environment name"
                usage
            fi
            validate_environment "$1"
            ENVIRONMENT="$1"
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Error: Unknown option '$1'"
            usage
            ;;
    esac
done

# Validate that we have taps to process
if [[ ${#SELECTED_TAPS[@]} -eq 0 ]]; then
    echo "Error: No taps selected for backup"
    usage
fi

# Validate that environment is specified
if [[ -z "$ENVIRONMENT" ]]; then
    echo "Error: Environment must be specified with --environment"
    usage
fi

# Create backup directory
mkdir -p saved_states

# Store current directory
ORIGINAL_DIR=$(pwd)

# Initialize counters
TOTAL_TAPS_PROCESSED=0
TOTAL_STATES_BACKED_UP=0

echo "Starting Meltano state backup..."
echo "Selected taps: ${SELECTED_TAPS[*]}"
echo "Environment: $ENVIRONMENT"
echo ""

# Process each selected tap
for tap in "${SELECTED_TAPS[@]}"; do
    echo "=========================================="
    # Temporarily disable set -e for this function call to prevent script exit
    set +e
    backup_tap_states "$tap" "$ENVIRONMENT"
    local backup_result=$?
    set -e

    if [[ $backup_result -eq 0 ]]; then
        ((TOTAL_TAPS_PROCESSED++))
        # Count states for this tap
        TAP_STATES=$(ls saved_states/${tap}/*_${ENVIRONMENT}.json 2>/dev/null | wc -l || echo 0)
        TOTAL_STATES_BACKED_UP=$((TOTAL_STATES_BACKED_UP + TAP_STATES))
    fi

    # Ensure we're back in the original directory
    cd "$ORIGINAL_DIR"
done

echo "=========================================="
echo "Backup completed!"
echo "Taps processed: $TOTAL_TAPS_PROCESSED/${#SELECTED_TAPS[@]}"
echo "Total states backed up: $TOTAL_STATES_BACKED_UP"
echo "Backup files saved to: saved_states/"
echo ""
echo "Directory structure:"
for tap in "${SELECTED_TAPS[@]}"; do
    tap_dir="saved_states/$tap"
    if [[ -d "$tap_dir" ]]; then
        echo "  $tap_dir/"
        echo "    ├── state_list_${ENVIRONMENT}.json"
        state_files=(${tap_dir}/*_${ENVIRONMENT}.json)
        for file in "${state_files[@]}"; do
            if [[ -f "$file" ]]; then
                echo "    └── $(basename "$file")"
            fi
        done
    fi
done