#!/bin/bash

# PostgreSQL Backup Script
# Usage: ./backup_postgres.sh [--type full|incremental]

# Load common functions
source "$(dirname "$0")/pgbackrest_common.sh"

BACKUP_TYPE="incremental"

show_help() {
    cat << EOF
PostgreSQL Backup Script

USAGE:
    ./backup_postgres.sh [options]

OPTIONS:
    --type <full|incremental>   Backup type (default: incremental, first backup is always full)
    --help                      Show this help message

EXAMPLES:
    ./backup_postgres.sh                    # Regular incremental backup (auto-detects OS)
    ./backup_postgres.sh --type full        # Force full backup

The script automatically:
- Detects your operating system (macOS or Ubuntu)
- Takes a full backup on first run
- Takes incremental backups on subsequent runs

EOF
}

perform_backup() {
    print_info "Starting $BACKUP_TYPE backup..."

    # Check if this is the first backup (stanza exists)
    if ! sudo -u postgres $PGBACKREST_BIN --stanza="$STANZA" info 2>/dev/null | grep -q "status: ok"; then
        print_warning "Stanza not found. This appears to be the first backup."
        print_info "Creating stanza..."

        sudo -u postgres $PGBACKREST_BIN --stanza="$STANZA" --log-level-console=info stanza-create || {
            print_error "Failed to create stanza"
            exit 1
        }
        print_status "Stanza created successfully"

        print_info "Taking full backup (required for first backup)..."
        BACKUP_TYPE="full"
    fi

    local backup_cmd="sudo -u postgres $PGBACKREST_BIN --stanza=\"$STANZA\" --log-level-console=info backup"
    [[ "$BACKUP_TYPE" == "full" ]] && backup_cmd="$backup_cmd --type=full"

    print_info "Executing: $backup_cmd"

    if eval "$backup_cmd"; then
        print_status "$BACKUP_TYPE backup completed successfully"

        # Verify backup integrity
        print_info "Verifying backup integrity..."

        if ! sudo -u postgres $PGBACKREST_BIN --stanza="$STANZA" check > /dev/null 2>&1; then
            print_error "CRITICAL: Backup integrity check FAILED"
            print_error "The backup may be corrupted or incomplete"
            exit 1
        fi

        # Verify latest backup is accessible
        local latest_backup=$((sudo -u postgres $PGBACKREST_BIN --stanza="$STANZA" info --output=json 2>/dev/null || echo "{}") | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    if data and 'backup' in data[0]:
        print(data[0]['backup'][-1]['label'])
    else:
        print('ERROR')
except:
    print('ERROR')
" 2>/dev/null)

        if [[ "$latest_backup" == "ERROR" || -z "$latest_backup" ]]; then
            print_error "CRITICAL: Cannot verify latest backup was created"
            exit 1
        fi

        print_status "✅ Backup integrity verification passed"
        print_status "✅ Latest backup: $latest_backup"

        print_info "Backup information:"
        sudo -u postgres $PGBACKREST_BIN --stanza="$STANZA" --log-level-console=info info || {
            print_error "Failed to display backup information"
            exit 1
        }
    else
        print_error "$BACKUP_TYPE backup failed"
        exit 1
    fi
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --type) BACKUP_TYPE="$2"; shift 2 ;;
        -h|--help) show_help; exit 0 ;;
        *) print_error "Unknown option: $1"; show_help; exit 1 ;;
    esac
done

# Validate backup type
[[ "$BACKUP_TYPE" != "full" && "$BACKUP_TYPE" != "incremental" ]] && {
    print_error "Invalid backup type: $BACKUP_TYPE. Must be 'full' or 'incremental'"
    exit 1
}

print_info "=== PostgreSQL Backup Script ==="
print_info "OS: $OS (auto-detected)"
print_info "Backup type: $BACKUP_TYPE"
print_info "pgBackRest binary: $PGBACKREST_BIN"
print_info "Config path: $CONFIG_PATH"
print_info "Backup directory: $BACKUP_DIR"
print_info "PostgreSQL data: $PGDATA"
echo

# Check prerequisites
validate_pgbackrest
validate_pgdata
check_postgres_connection

# Perform backup
perform_backup

print_status "Backup completed successfully!"