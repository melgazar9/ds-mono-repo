#!/bin/bash

# PostgreSQL Backup Script
# Usage: ./backup_postgres.sh [--type full|incremental]

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
BACKUP_TYPE="incremental"
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

# Function to check PostgreSQL connection
check_postgres() {
    print_info "Checking PostgreSQL connection..."
    if ! psql -d postgres -c "SELECT version();" > /dev/null 2>&1; then
        print_error "Cannot connect to PostgreSQL"
        print_info "Make sure PostgreSQL is running:"
        case "$OS" in
            "mac")
                echo "  brew services start postgresql@15"
                ;;
            "ubuntu")
                echo "  sudo systemctl start postgresql"
                ;;
        esac
        exit 1
    fi
    print_status "PostgreSQL connection successful"
}

# Function to perform backup
perform_backup() {
    print_info "Starting $BACKUP_TYPE backup..."

    # Check if this is the first backup (stanza exists)
    if ! $PGBACKREST_BIN --config-path="$CONFIG_PATH" --stanza="$STANZA" info 2>/dev/null | grep -q "status: ok"; then
        print_warning "Stanza not found. This appears to be the first backup."
        print_info "Creating stanza..."

        if $PGBACKREST_BIN --config-path="$CONFIG_PATH" --stanza="$STANZA" --log-level-console=info stanza-create; then
            print_status "Stanza created successfully"
        else
            print_error "Failed to create stanza"
            exit 1
        fi

        print_info "Taking full backup (required for first backup)..."
        BACKUP_TYPE="full"
    fi

    # Construct backup command
    local backup_cmd="$PGBACKREST_BIN --config-path=\"$CONFIG_PATH\" --stanza=\"$STANZA\" --log-level-console=info backup"

    if [[ "$BACKUP_TYPE" == "full" ]]; then
        backup_cmd="$backup_cmd --type=full"
    fi

    print_info "Executing: $backup_cmd"

    if eval "$backup_cmd"; then
        print_status "$BACKUP_TYPE backup completed successfully"

        # Show backup info
        print_info "Backup information:"
        $PGBACKREST_BIN --config-path="$CONFIG_PATH" --stanza="$STANZA" --log-level-console=info info
    else
        print_error "$BACKUP_TYPE backup failed"
        exit 1
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --type)
            BACKUP_TYPE="$2"
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

# Validate backup type
if [[ "$BACKUP_TYPE" != "full" && "$BACKUP_TYPE" != "incremental" ]]; then
    print_error "Invalid backup type: $BACKUP_TYPE. Must be 'full' or 'incremental'"
    exit 1
fi

# Set OS-specific paths
set_os_paths

print_info "=== PostgreSQL Backup Script ==="
print_info "OS: $OS (auto-detected)"
print_info "Backup type: $BACKUP_TYPE"
print_info "pgBackRest binary: $PGBACKREST_BIN"
print_info "Config path: $CONFIG_PATH"
print_info "Backup directory: $BACKUP_DIR"
print_info "PostgreSQL data: $PGDATA"
echo

# Check prerequisites
check_pgbackrest
validate_pgdata
check_postgres

# Perform backup
perform_backup

print_status "Backup completed successfully!"