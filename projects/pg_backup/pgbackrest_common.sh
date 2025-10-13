#!/bin/bash

# PostgreSQL pgBackRest Common Library --> Shared functions for backup and restore scripts

set -e

# Constants
STANZA="main"
OS=$([[ "$OSTYPE" == "darwin"* ]] && echo "mac" || echo "ubuntu")

# Colors and print functions
print_status() { echo -e "\033[0;32m✅ $1\033[0m"; }
print_warning() { echo -e "\033[1;33m⚠️  $1\033[0m"; }

print_error() { echo -e "\033[0;31m❌ $1\033[0m"; }
print_info() { echo -e "\033[0;34mℹ️  $1\033[0m"; }

# Set OS-specific paths
if [[ "$OS" == "mac" ]]; then
    PGBACKREST_BIN="/opt/homebrew/bin/pgbackrest"
    CONFIG_PATH="$HOME/.pgbackrest"
    BACKUP_DIR="$HOME/pgbackrest_backups"
    PGDATA="/opt/homebrew/var/postgresql@15"
    PG_SERVICE_CMD="brew services"
    PG_SERVICE_NAME="postgresql@15"
else
    PGBACKREST_BIN="/usr/bin/pgbackrest"
    CONFIG_PATH="/etc/pgbackrest"
    BACKUP_DIR="/var/lib/pgbackrest"
    BACKUP_DIR_REMOTE="/mnt/backup/pg_backups/pg_backrest_backups"
    PGDATA="/var/lib/postgresql/16/main"
    PG_SERVICE_CMD="sudo systemctl"
    PG_SERVICE_NAME="postgresql"
fi

# Validation functions
validate_pgbackrest() {
    [[ ! -x "$PGBACKREST_BIN" ]] && {
        print_error "pgBackRest not found at $PGBACKREST_BIN"
        print_info "Install: $([[ "$OS" == "mac" ]] && echo "brew install pgbackrest" || echo "sudo apt update && sudo apt install pgbackrest")"
        exit 1
    }
    print_status "pgBackRest found at $PGBACKREST_BIN"
}

validate_pgdata() {
    [[ -z "$PGDATA" ]] && { print_error "PGDATA variable is empty"; exit 1; }

    # Check if directory exists (using sudo for permission)
    if ! sudo test -d "$PGDATA"; then
        print_error "PostgreSQL data directory does not exist: $PGDATA"
        exit 1
    fi

    # Check if PG_VERSION exists (using sudo for permission)
    if ! sudo test -f "$PGDATA/PG_VERSION"; then
        print_error "Not a valid PostgreSQL data directory (missing PG_VERSION): $PGDATA"
        exit 1
    fi

    print_status "PostgreSQL data directory validated: $PGDATA"
}

check_postgres_connection() {
    print_info "Checking PostgreSQL connection..."
    if ! sudo -u $USER psql -d postgres -c "SELECT version();" > /dev/null 2>&1; then
        print_error "Cannot connect to PostgreSQL"
        print_info "Start PostgreSQL: $([[ "$OS" == "mac" ]] && echo "$PG_SERVICE_CMD start $PG_SERVICE_NAME" || echo "$PG_SERVICE_CMD start $PG_SERVICE_NAME")"
        exit 1
    fi
    print_status "PostgreSQL connection successful"
}

stop_postgres() {
    print_info "Stopping PostgreSQL..."
    if [[ "$OS" == "mac" ]]; then
        if $PG_SERVICE_CMD list | grep -q "$PG_SERVICE_NAME.*started"; then
            $PG_SERVICE_CMD stop $PG_SERVICE_NAME
            print_status "PostgreSQL stopped"
        else
            print_info "PostgreSQL was already stopped"
        fi
    else
        if systemctl is-active --quiet $PG_SERVICE_NAME; then
            $PG_SERVICE_CMD stop $PG_SERVICE_NAME
            print_status "PostgreSQL stopped"
        else
            print_info "PostgreSQL was already stopped"
        fi
    fi
    sleep 2
}

start_postgres() {
    print_info "Starting PostgreSQL..."
    $PG_SERVICE_CMD start $PG_SERVICE_NAME

    print_info "Waiting for PostgreSQL to start..."
    sleep 5

    local max_attempts=6
    local attempt=1

    while [[ $attempt -le $max_attempts ]]; do
        if sudo -u $USER psql -d postgres -c "SELECT version();" > /dev/null 2>&1; then
            print_status "PostgreSQL started successfully"
            return 0
        else
            print_info "Attempt $attempt/$max_attempts: Waiting for PostgreSQL to start..."
            sleep 5
            ((attempt++))
        fi
    done

    print_error "PostgreSQL failed to start"
    print_info "Check logs: $([[ "$OS" == "mac" ]] && echo "tail /opt/homebrew/var/log/postgresql@15.log" || echo "sudo journalctl -u postgresql")"
    exit 1
}

check_disk_space() {
    local required_space_mb=100
    local data_dir_parent=$(dirname "$PGDATA")

    if command -v df >/dev/null 2>&1; then
        local available_space_kb=$(df -k "$data_dir_parent" | tail -1 | awk '{print $4}')
        local available_space_mb=$((available_space_kb / 1024))

        if [[ $available_space_mb -lt $required_space_mb ]]; then
            print_error "Insufficient disk space. Available: ${available_space_mb}MB, Required: ${required_space_mb}MB"
            exit 1
        fi

        print_status "Disk space check passed: ${available_space_mb}MB available"
    else
        print_warning "Cannot check disk space (df command not available)"
    fi
}

show_available_backups() {
    print_info "Available backups:"
    if $PGBACKREST_BIN --config-path="$CONFIG_PATH" --stanza="$STANZA" --log-level-console=info info; then
        echo
    else
        print_error "Failed to retrieve backup information"
        print_info "Make sure pgBackRest is configured and at least one backup exists"
        exit 1
    fi
}

validate_backup_set() {
    if [[ -n "$BACKUP_SET" ]]; then
        print_info "Validating backup set: $BACKUP_SET"
        if ! $PGBACKREST_BIN --config-path="$CONFIG_PATH" --stanza="$STANZA" info 2>/dev/null | grep -q "$BACKUP_SET"; then
            print_error "Backup set '$BACKUP_SET' not found"
            show_available_backups
            exit 1
        fi
        print_status "Backup set '$BACKUP_SET' found"
    fi
}

# Ubuntu dual backup functions
copy_to_remote() {
    [[ "$OS" != "ubuntu" ]] || [[ -z "$BACKUP_DIR_REMOTE" ]] && return 0

    print_info "Copying backup repository to remote location: $BACKUP_DIR_REMOTE"

    [[ ! -d "$BACKUP_DIR" ]] || [[ ! -f "$BACKUP_DIR/backup/main/backup.info" ]] && {
        print_error "CRITICAL: Source backup directory invalid or incomplete: $BACKUP_DIR"
        return 1
    }

    sudo mkdir -p "$BACKUP_DIR_REMOTE" || {
        print_error "CRITICAL: Failed to create remote backup directory: $BACKUP_DIR_REMOTE"
        return 1
    }

    print_info "Copying backup repository (this may take several minutes)..."

    if sudo rsync -av --checksum "$BACKUP_DIR/" "$BACKUP_DIR_REMOTE/"; then
        print_status "Backup repository successfully copied to remote location"
    else
        print_error "CRITICAL: Failed to copy backup repository to remote location"
        return 1
    fi

    local required_files=("backup/main/backup.info" "archive/main/archive.info")
    for file in "${required_files[@]}"; do
        [[ ! -f "$BACKUP_DIR_REMOTE/$file" ]] && {
            print_error "CRITICAL: Required file missing in remote backup: $file"
            return 1
        }
    done

    local local_count=$(find "$BACKUP_DIR" -type f | wc -l)
    local remote_count=$(find "$BACKUP_DIR_REMOTE" -type f | wc -l)

    [[ "$local_count" -ne "$remote_count" ]] && {
        print_error "CRITICAL: File count mismatch - Local: $local_count, Remote: $remote_count"
        return 1
    }

    print_status "✅ Remote backup copy completed and verified"
    return 0
}

verify_backup_integrity() {
    [[ "$OS" != "ubuntu" ]] && return 0

    print_info "Verifying backup integrity in both locations..."

    print_info "Verifying local backup: $BACKUP_DIR"
    $PGBACKREST_BIN --config-path="$CONFIG_PATH" --stanza="$STANZA" check 2>/dev/null || {
        print_error "CRITICAL: Local backup integrity check FAILED"
        return 1
    }
    print_status "Local backup integrity verified"

    print_info "Verifying remote backup: $BACKUP_DIR_REMOTE"

    local temp_config_dir="/tmp/pgbackrest_verify_$$"
    local temp_config="$temp_config_dir/pgbackrest.conf"

    mkdir -p "$temp_config_dir"

    if [[ -f "$CONFIG_PATH/pgbackrest.conf" ]]; then
        sed "s|repo1-path=$BACKUP_DIR|repo1-path=$BACKUP_DIR_REMOTE|g" "$CONFIG_PATH/pgbackrest.conf" > "$temp_config"
    else
        print_error "CRITICAL: Cannot find pgbackrest.conf at $CONFIG_PATH"
        rm -rf "$temp_config_dir"
        return 1
    fi

    if ! $PGBACKREST_BIN --config-path="$temp_config_dir" --stanza="$STANZA" check 2>/dev/null; then
        print_error "CRITICAL: Remote backup integrity check FAILED"
        rm -rf "$temp_config_dir"
        return 1
    fi

    rm -rf "$temp_config_dir"

    print_status "Remote backup integrity verified"
    print_status "✅ BOTH backup locations verified successfully"

    return 0
}
