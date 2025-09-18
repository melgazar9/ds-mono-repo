#!/bin/bash

# Enhanced Meltano states restore script with argument parsing
# Usage:
#   ./restore_states.sh --all
#   ./restore_states.sh --extractors tap-fmp tap-polygon
#   ./restore_states.sh --extractors tap-yfinance

set -e

# Available taps and environments
AVAILABLE_TAPS=("tap-yfinance" "tap-yahooquery" "tap-fmp" "tap-polygon" "tap-fred")
AVAILABLE_ENVIRONMENTS=("dev" "production")

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --all                      Restore states for all available taps"
    echo "  --extractors TAP1 TAP2     Restore states for specified taps"
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

# Function to restore states for a specific tap
restore_tap_states() {
    local tap=$1
    local environment=$2
    echo "Processing tap: $tap (environment: $environment)"

    # Check if tap directory exists
    if [[ ! -d "$tap" ]]; then
        echo "Warning: Directory '$tap' not found, skipping..."
        return 1
    fi

    # Check if tap backup directory exists
    local tap_backup_dir="saved_states/$tap"
    if [[ ! -d "$tap_backup_dir" ]]; then
        echo "  No backup directory found for $tap (${tap_backup_dir})"
        return 0
    fi

    # Check for backup files for this tap and environment
    local state_files=(${tap_backup_dir}/*_${environment}.json)
    if [[ ! -f "${state_files[0]}" ]]; then
        echo "  No backup files found for $tap with environment $environment"
        return 0
    fi

    # All files matching the pattern are actual state files
    local actual_state_files=()
    for file in "${state_files[@]}"; do
        if [[ -f "$file" ]]; then
            actual_state_files+=("$file")
        fi
    done

    if [[ ${#actual_state_files[@]} -eq 0 ]]; then
        echo "  No state backup files found for $tap with environment $environment"
        return 0
    fi

    # Change to tap directory
    cd "$tap"

    # Restore each state
    local states_restored=0
    for state_file in "${actual_state_files[@]}"; do
        if [[ -f "../$state_file" ]]; then
            # Extract state ID from filename by removing the environment suffix
            # Format: state_id_environment.json -> state_id
            local filename=$(basename "$state_file")
            local state_id="${filename%_${environment}.json}"

            echo "    Restoring state: $state_id"

            if meltano --environment "$environment" state set "$state_id" --input-file "../$state_file" --force 2>/dev/null; then
                ((states_restored++))
            else
                echo "    Warning: Failed to restore state '$state_id' for $tap"
            fi
        fi
    done

    echo "  Restored $states_restored states for $tap"
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
    echo "Error: No taps selected for restore"
    usage
fi

# Validate that environment is specified
if [[ -z "$ENVIRONMENT" ]]; then
    echo "Error: Environment must be specified with --environment"
    usage
fi

# Check if saved_states directory exists
if [[ ! -d "saved_states" ]]; then
    echo "Error: saved_states directory not found!"
    echo "Please run backup_states.sh first to create state backups."
    exit 1
fi

# Store current directory
ORIGINAL_DIR=$(pwd)

# Initialize counters
TOTAL_TAPS_PROCESSED=0
TOTAL_STATES_RESTORED=0

echo "Starting Meltano state restoration..."
echo "Selected taps: ${SELECTED_TAPS[*]}"
echo "Environment: $ENVIRONMENT"
echo ""

# Process each selected tap
for tap in "${SELECTED_TAPS[@]}"; do
    echo "=========================================="
    if restore_tap_states "$tap" "$ENVIRONMENT"; then
        ((TOTAL_TAPS_PROCESSED++))
        # Count restored states for this tap
        local tap_backup_dir="saved_states/$tap"
        if [[ -d "$tap_backup_dir" ]]; then
            TAP_RESTORED_FILES=(${tap_backup_dir}/*_${ENVIRONMENT}.json)
            local tap_states=0
            for file in "${TAP_RESTORED_FILES[@]}"; do
                if [[ -f "$file" ]]; then
                    ((tap_states++))
                fi
            done
            TOTAL_STATES_RESTORED=$((TOTAL_STATES_RESTORED + tap_states))
        fi
    fi

    # Ensure we're back in the original directory
    cd "$ORIGINAL_DIR"
done

echo "=========================================="
echo "Restoration completed!"
echo "Taps processed: $TOTAL_TAPS_PROCESSED/${#SELECTED_TAPS[@]}"
echo "Total states restored: $TOTAL_STATES_RESTORED"
echo ""
echo "Note: States were restored using the --force flag to overwrite existing states."