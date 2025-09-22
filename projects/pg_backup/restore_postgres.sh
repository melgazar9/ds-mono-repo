#!/bin/bash

# PostgreSQL Restore Script
# Usage: ./restore_postgres.sh [--set BACKUP_SET]

# Load common functions
source "$(dirname "$0")/pgbackrest_common.sh"

BACKUP_SET=""

show_help() {
    cat << EOF
PostgreSQL Restore Script

USAGE:
    ./restore_postgres.sh [options]

OPTIONS:
    --set <backup_set>          Specific backup set to restore from (optional)
    --help                      Show this help message

EXAMPLES:
    ./restore_postgres.sh                           # Restore from latest backup
    ./restore_postgres.sh --set 20250920-162442F    # Restore from specific backup

The script automatically:
- Detects your operating system (macOS or Ubuntu)
- Stops PostgreSQL before restore
- Restores from latest backup (or specified backup)
- Starts PostgreSQL after restore

EOF
}

choose_backup_location() {
    [[ "$OS" != "ubuntu" ]] || [[ -z "$BACKUP_DIR_REMOTE" ]] && return

    print_info "Ubuntu system detected with dual backup locations:"
    echo "1. Local backup: $BACKUP_DIR"
    echo "2. Remote backup: $BACKUP_DIR_REMOTE"
    echo

    local local_has_backup=false
    local remote_has_backup=false

    [[ -d "$BACKUP_DIR" ]] && $PGBACKREST_BIN --config-path="$CONFIG_PATH" --stanza="$STANZA" info 2>/dev/null | grep -q "status: ok" && local_has_backup=true

    if [[ -d "$BACKUP_DIR_REMOTE" ]]; then
        local temp_config="/tmp/pgbackrest_remote.conf"
        sed "s|$BACKUP_DIR|$BACKUP_DIR_REMOTE|g" "$CONFIG_PATH/pgbackrest.conf" > "$temp_config" 2>/dev/null && \
        $PGBACKREST_BIN --config-path="$(dirname $temp_config)" --stanza="$STANZA" info 2>/dev/null | grep -q "status: ok" && remote_has_backup=true
        rm -f "$temp_config" 2>/dev/null
    fi

    echo "$([[ "$local_has_backup" == true ]] && echo "✅" || echo "❌") Local backup available"
    echo "$([[ "$remote_has_backup" == true ]] && echo "✅" || echo "❌") Remote backup available"
    echo

    if [[ "$local_has_backup" == true && "$remote_has_backup" == true ]]; then
        read -p "Choose backup location (1=local, 2=remote): " choice
        case $choice in
            2) print_info "Using remote backup: $BACKUP_DIR_REMOTE"; BACKUP_DIR="$BACKUP_DIR_REMOTE" ;;
            *) print_info "Using local backup: $BACKUP_DIR" ;;
        esac
    elif [[ "$remote_has_backup" == true ]]; then
        print_warning "Only remote backup available, using: $BACKUP_DIR_REMOTE"
        BACKUP_DIR="$BACKUP_DIR_REMOTE"
    elif [[ "$local_has_backup" == true ]]; then
        print_info "Using local backup: $BACKUP_DIR"
    else
        print_error "No backups found in either location"
        exit 1
    fi
}

perform_restore() {
    print_warning "DANGER: This will completely replace your PostgreSQL data!"
    print_warning "All current database content will be lost!"
    echo

    show_available_backups

    print_warning "You are about to restore from $([[ -n "$BACKUP_SET" ]] && echo "backup set: $BACKUP_SET" || echo "the LATEST backup")"
    echo
    read -p "Are you sure you want to continue? Type 'yes' to proceed: " confirmation

    [[ "$confirmation" != "yes" ]] && { print_info "Restore cancelled by user"; exit 0; }

    stop_postgres

    # Construct restore command
    local restore_cmd="$PGBACKREST_BIN --config-path=\"$CONFIG_PATH\" --stanza=\"$STANZA\" --log-level-console=info --delta restore"
    [[ -n "$BACKUP_SET" ]] && restore_cmd="$restore_cmd --set=\"$BACKUP_SET\""

    print_info "Restoring from $([[ -n "$BACKUP_SET" ]] && echo "specific backup set: $BACKUP_SET" || echo "latest backup")"

    # CRITICAL SAFETY CHECK before deletion
    [[ -z "$PGDATA" ]] || [[ "$PGDATA" == "/" ]] || [[ "$PGDATA" == "/*" ]] && {
        print_error "CRITICAL: Refusing to delete dangerous path: '$PGDATA'"
        exit 1
    }

    [[ ! -f "$PGDATA/PG_VERSION" ]] && {
        print_error "CRITICAL: Directory '$PGDATA' doesn't contain PG_VERSION - refusing to delete"
        exit 1
    }

    print_info "Removing existing PostgreSQL data directory contents..."
    if [[ "$OS" == "mac" ]]; then
        find "$PGDATA" -mindepth 1 -delete 2>/dev/null || {
            print_error "Failed to clean PostgreSQL data directory"
            exit 1
        }
    else
        sudo find "$PGDATA" -mindepth 1 -delete 2>/dev/null || {
            print_error "Failed to clean PostgreSQL data directory"
            exit 1
        }
    fi

    print_status "PostgreSQL data directory cleaned safely"

    print_info "Executing restore command..."

    if [[ "$OS" == "mac" ]]; then
        eval "$restore_cmd" || { print_error "Restore failed"; exit 1; }
    else
        sudo -u postgres eval "$restore_cmd" || { print_error "Restore failed"; exit 1; }
    fi

    print_status "Restore completed successfully"

    # Fix the restore command path in postgresql.auto.conf
    local auto_conf="$PGDATA/postgresql.auto.conf"
    if [[ -f "$auto_conf" ]]; then
        if [[ "$OS" == "mac" ]]; then
            sed -i '' "s|restore_command = 'pgbackrest|restore_command = '$PGBACKREST_BIN|g" "$auto_conf"
        else
            sudo sed -i "s|restore_command = 'pgbackrest|restore_command = '$PGBACKREST_BIN|g" "$auto_conf"
        fi
        print_status "Fixed restore command path in postgresql.auto.conf"
    fi

    start_postgres

    # Verify restore completed successfully
    print_info "Verifying restore integrity..."

    # Check if PostgreSQL is responding
    if ! psql -d postgres -c "SELECT version();" > /dev/null 2>&1; then
        print_error "CRITICAL: PostgreSQL is not responding after restore"
        exit 1
    fi

    # Verify database connectivity
    if ! psql -d postgres -c "\\l" > /dev/null 2>&1; then
        print_error "CRITICAL: Cannot list databases after restore"
        exit 1
    fi

    # Check for any corrupt databases
    local databases=$(psql -d postgres -t -c "SELECT datname FROM pg_database WHERE datname NOT IN ('template0', 'template1');")
    local corrupt_found=false

    while IFS= read -r dbname; do
        dbname=$(echo "$dbname" | xargs)  # trim whitespace
        [[ -z "$dbname" ]] && continue

        if ! psql -d "$dbname" -c "SELECT 1;" > /dev/null 2>&1; then
            print_error "CRITICAL: Database '$dbname' is corrupted or inaccessible"
            corrupt_found=true
        fi
    done <<< "$databases"

    if [[ "$corrupt_found" == true ]]; then
        print_error "CRITICAL: One or more databases are corrupted after restore"
        exit 1
    fi

    # Verify WAL replay is working
    if ! $PGBACKREST_BIN --config-path="$CONFIG_PATH" --stanza="$STANZA" check > /dev/null 2>&1; then
        print_error "CRITICAL: pgBackRest archive check failed after restore"
        exit 1
    fi

    print_status "✅ Restore integrity verification passed"
    print_status "Database restored successfully!"

    print_info "Restored database information:"
    psql -d postgres -c "\\l" || {
        print_error "Failed to display database list"
        exit 1
    }
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --set) BACKUP_SET="$2"; shift 2 ;;
        -h|--help) show_help; exit 0 ;;
        *) print_error "Unknown option: $1"; show_help; exit 1 ;;
    esac
done

print_info "=== PostgreSQL Restore Script ==="
print_info "OS: $OS (auto-detected)"
print_info "Target backup: $([[ -n "$BACKUP_SET" ]] && echo "$BACKUP_SET" || echo "Latest")"
print_info "pgBackRest binary: $PGBACKREST_BIN"
print_info "Config path: $CONFIG_PATH"
print_info "Backup directory: $BACKUP_DIR"
print_info "PostgreSQL data: $PGDATA"
echo

# Check prerequisites
validate_pgbackrest
validate_pgdata
validate_backup_set
check_disk_space

# Choose backup location for Ubuntu
choose_backup_location

# Perform restore
perform_restore

print_status "Restore completed successfully!"