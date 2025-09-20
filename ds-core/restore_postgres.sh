#!/bin/bash

# PostgreSQL Restore Script
# Usage: ./restore_postgres.sh [--set BACKUP_SET]

set -e

# Function to detect OS
detect_os() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "mac"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "ubuntu"
    else
        echo "ubuntu"  # Default fallback
    fi
}

# Default values
OS=$(detect_os)  # Auto-detect OS
BACKUP_SET=""
PGBACKREST_BIN=""
CONFIG_PATH=""
BACKUP_DIR=""
PGDATA=""
STANZA="main"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Function to display help
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

# Function to set OS-specific paths
set_os_paths() {
    case "$OS" in
        "mac")
            PGBACKREST_BIN="/opt/homebrew/bin/pgbackrest"
            CONFIG_PATH="$HOME/.pgbackrest"
            BACKUP_DIR="$HOME/pgbackrest_backups"
            PGDATA="/opt/homebrew/var/postgresql@15"
            ;;
        "ubuntu")
            PGBACKREST_BIN="/usr/bin/pgbackrest"
            CONFIG_PATH="/etc/pgbackrest"
            BACKUP_DIR="/var/lib/pgbackrest"
            PGDATA="/var/lib/postgresql/15/main"
            ;;
        *)
            print_error "Unsupported OS: $OS"
            exit 1
            ;;
    esac

    # Validate critical paths
    if [[ -z "$PGBACKREST_BIN" ]] || [[ -z "$CONFIG_PATH" ]] || [[ -z "$BACKUP_DIR" ]] || [[ -z "$PGDATA" ]]; then
        print_error "Critical path variables are empty after OS detection"
        exit 1
    fi
}

# Function to check if pgBackRest is installed
check_pgbackrest() {
    if [[ ! -x "$PGBACKREST_BIN" ]]; then
        print_error "pgBackRest not found at $PGBACKREST_BIN"
        print_info "Install pgBackRest first:"
        case "$OS" in
            "mac")
                echo "  brew install pgbackrest"
                ;;
            "ubuntu")
                echo "  sudo apt update && sudo apt install pgbackrest"
                ;;
        esac
        exit 1
    fi
    print_status "pgBackRest found at $PGBACKREST_BIN"
}

# Function to validate backup set exists
validate_backup_set() {
    if [[ -n "$BACKUP_SET" ]]; then
        print_info "Validating backup set: $BACKUP_SET"
        if ! $PGBACKREST_BIN --config-path="$CONFIG_PATH" --stanza="$STANZA" info 2>/dev/null | grep -q "$BACKUP_SET"; then
            print_error "Backup set '$BACKUP_SET' not found"
            print_info "Available backups:"
            $PGBACKREST_BIN --config-path="$CONFIG_PATH" --stanza="$STANZA" info 2>/dev/null || true
            exit 1
        fi
        print_status "Backup set '$BACKUP_SET' found"
    fi
}

# Function to validate PostgreSQL data directory
validate_pgdata() {
    if [[ -z "$PGDATA" ]]; then
        print_error "PGDATA variable is empty"
        exit 1
    fi

    if [[ ! -d "$PGDATA" ]]; then
        print_error "PostgreSQL data directory does not exist: $PGDATA"
        exit 1
    fi

    if [[ ! -f "$PGDATA/PG_VERSION" ]]; then
        print_error "Not a valid PostgreSQL data directory (missing PG_VERSION): $PGDATA"
        exit 1
    fi

    print_status "PostgreSQL data directory validated: $PGDATA"
}

# Function to check disk space
check_disk_space() {
    local required_space_mb=100  # Minimum 100MB free space
    local data_dir_parent=$(dirname "$PGDATA")

    if command -v df >/dev/null 2>&1; then
        local available_space_kb
        if [[ "$OS" == "mac" ]]; then
            available_space_kb=$(df -k "$data_dir_parent" | tail -1 | awk '{print $4}')
        else
            available_space_kb=$(df -k "$data_dir_parent" | tail -1 | awk '{print $4}')
        fi

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

# Function to show available backups
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

# Function to perform restore
perform_restore() {
    print_warning "DANGER: This will completely replace your PostgreSQL data!"
    print_warning "All current database content will be lost!"
    echo

    # Show available backups
    show_available_backups

    # Confirm restore
    if [[ -n "$BACKUP_SET" ]]; then
        print_warning "You are about to restore from backup set: $BACKUP_SET"
    else
        print_warning "You are about to restore from the LATEST backup"
    fi

    echo
    read -p "Are you sure you want to continue? Type 'yes' to proceed: " confirmation

    if [[ "$confirmation" != "yes" ]]; then
        print_info "Restore cancelled by user"
        exit 0
    fi

    # Check if PostgreSQL is running and stop it
    print_info "Stopping PostgreSQL..."
    case "$OS" in
        "mac")
            if brew services list | grep -q "postgresql@15.*started"; then
                brew services stop postgresql@15
                print_status "PostgreSQL stopped"
            else
                print_info "PostgreSQL was already stopped"
            fi
            ;;
        "ubuntu")
            if systemctl is-active --quiet postgresql; then
                sudo systemctl stop postgresql
                print_status "PostgreSQL stopped"
            else
                print_info "PostgreSQL was already stopped"
            fi
            ;;
    esac

    # Wait a moment for PostgreSQL to fully stop
    sleep 2

    # Construct restore command
    local restore_cmd="$PGBACKREST_BIN --config-path=\"$CONFIG_PATH\" --stanza=\"$STANZA\" --log-level-console=info restore"

    if [[ -n "$BACKUP_SET" ]]; then
        restore_cmd="$restore_cmd --set=\"$BACKUP_SET\""
        print_info "Restoring from specific backup set: $BACKUP_SET"
    else
        print_info "Restoring from latest backup"
    fi

    # CRITICAL SAFETY CHECK before deletion
    if [[ -z "$PGDATA" ]] || [[ "$PGDATA" == "/" ]] || [[ "$PGDATA" == "/*" ]]; then
        print_error "CRITICAL: Refusing to delete dangerous path: '$PGDATA'"
        exit 1
    fi

    # Double-check it's a PostgreSQL directory before deletion
    if [[ ! -f "$PGDATA/PG_VERSION" ]]; then
        print_error "CRITICAL: Directory '$PGDATA' doesn't contain PG_VERSION - refusing to delete"
        exit 1
    fi

    # Remove existing data with additional safety
    print_info "Removing existing PostgreSQL data directory contents..."
    if [[ "$OS" == "mac" ]]; then
        # Use find for safer deletion
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
        if eval "$restore_cmd"; then
            print_status "Restore completed successfully"
        else
            print_error "Restore failed"
            exit 1
        fi
    else
        if sudo -u postgres eval "$restore_cmd"; then
            print_status "Restore completed successfully"
        else
            print_error "Restore failed"
            exit 1
        fi
    fi

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

    print_info "Starting PostgreSQL..."
    case "$OS" in
        "mac")
            brew services start postgresql@15
            ;;
        "ubuntu")
            sudo systemctl start postgresql
            ;;
    esac

    # Wait for PostgreSQL to start
    print_info "Waiting for PostgreSQL to start..."
    sleep 5

    # Test connection with retries
    local max_attempts=6
    local attempt=1

    while [[ $attempt -le $max_attempts ]]; do
        if psql -d postgres -c "SELECT version();" > /dev/null 2>&1; then
            print_status "PostgreSQL started successfully after restore"
            print_status "Database restored successfully!"

            # Show basic info about restored database
            print_info "Restored database information:"
            psql -d postgres -c "\l" 2>/dev/null || true

            return 0
        else
            print_info "Attempt $attempt/$max_attempts: Waiting for PostgreSQL to start..."
            sleep 5
            ((attempt++))
        fi
    done

    print_error "PostgreSQL failed to start after restore"
    print_info "Check the PostgreSQL logs for details:"
    case "$OS" in
        "mac")
            echo "  tail /opt/homebrew/var/log/postgresql@15.log"
            ;;
        "ubuntu")
            echo "  sudo journalctl -u postgresql"
            ;;
    esac
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --set)
            BACKUP_SET="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Set OS-specific paths
set_os_paths

print_info "=== PostgreSQL Restore Script ==="
print_info "OS: $OS (auto-detected)"
if [[ -n "$BACKUP_SET" ]]; then
    print_info "Target backup: $BACKUP_SET"
else
    print_info "Target backup: Latest"
fi
print_info "pgBackRest binary: $PGBACKREST_BIN"
print_info "Config path: $CONFIG_PATH"
print_info "Backup directory: $BACKUP_DIR"
print_info "PostgreSQL data: $PGDATA"
echo

# Check prerequisites
check_pgbackrest
validate_pgdata
validate_backup_set
check_disk_space

# Perform restore
perform_restore

print_status "Restore completed successfully!"