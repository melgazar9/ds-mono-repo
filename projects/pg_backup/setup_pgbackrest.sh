#!/bin/bash

# pgBackRest Setup Script for PostgreSQL Backup to /mnt/backup/pg_backups

set -e

# Colors
print_status() { echo -e "\033[0;32m✅ $1\033[0m"; }
print_error() { echo -e "\033[0;31m❌ $1\033[0m"; }
print_info() { echo -e "\033[0;34mℹ️  $1\033[0m"; }
print_warning() { echo -e "\033[1;33m⚠️  $1\033[0m"; }

print_info "=== pgBackRest Setup for PostgreSQL Backup ==="

# Configuration
STANZA="main"
BACKUP_PATH="/mnt/backup/pg_backups"
CONFIG_DIR="/etc/pgbackrest"
CONFIG_FILE="$CONFIG_DIR/pgbackrest.conf"
PGDATA="/var/lib/postgresql/16/main"
PG_CONF="/etc/postgresql/16/main/postgresql.conf"

# Check if running as root or with sudo
if [[ $EUID -ne 0 ]]; then
   print_error "This script must be run with sudo"
   echo "Usage: sudo ./setup_pgbackrest.sh"
   exit 1
fi

print_info "Backing up LOCAL PostgreSQL at $PGDATA to $BACKUP_PATH"

# Create backup directory
print_info "Creating backup directory: $BACKUP_PATH"
mkdir -p "$BACKUP_PATH"
chown postgres:postgres "$BACKUP_PATH"
chmod 750 "$BACKUP_PATH"
print_status "Backup directory created"

# Create config directory
print_info "Creating pgBackRest config directory: $CONFIG_DIR"
mkdir -p "$CONFIG_DIR"
chmod 755 "$CONFIG_DIR"
print_status "Config directory created"

# Create lock directory
print_info "Creating pgBackRest lock directory: /tmp/pgbackrest"
mkdir -p /tmp/pgbackrest
chown postgres:postgres /tmp/pgbackrest
chmod 770 /tmp/pgbackrest
print_status "Lock directory created"

# Create log directory
print_info "Creating pgBackRest log directory: /var/log/pgbackrest"
mkdir -p /var/log/pgbackrest
chown postgres:postgres /var/log/pgbackrest
chmod 770 /var/log/pgbackrest
print_status "Log directory created"

# Create spool directory
print_info "Creating pgBackRest spool directory: /var/spool/pgbackrest"
mkdir -p /var/spool/pgbackrest
chown postgres:postgres /var/spool/pgbackrest
chmod 770 /var/spool/pgbackrest
print_status "Spool directory created"

# Create pgBackRest configuration
print_info "Creating pgBackRest configuration at $CONFIG_FILE"
cat > "$CONFIG_FILE" <<EOF
[global]
repo1-path=/mnt/backup/pg_backups
repo1-retention-full=2
log-level-console=info
log-level-file=debug
start-fast=y

[main]
pg1-path=/var/lib/postgresql/16/main
pg1-socket-path=/var/run/postgresql
EOF

chmod 644 "$CONFIG_FILE"
print_status "Configuration file created"

# Display configuration
print_info "pgBackRest configuration:"
cat "$CONFIG_FILE"
echo

# Configure PostgreSQL for archiving
print_info "Configuring PostgreSQL for continuous archiving..."

# Backup postgresql.conf
cp "$PG_CONF" "$PG_CONF.backup.$(date +%Y%m%d_%H%M%S)"
print_status "Backed up postgresql.conf"

# Check and update archive_mode
if ! grep -q "^archive_mode = on" "$PG_CONF"; then
    print_info "Enabling archive_mode..."
    if grep -q "^archive_mode" "$PG_CONF"; then
        sed -i "s/^archive_mode.*/archive_mode = on/" "$PG_CONF"
    else
        echo "archive_mode = on" >> "$PG_CONF"
    fi
    print_status "archive_mode enabled"
fi

# Check and update archive_command
if ! grep -q "^archive_command.*pgbackrest" "$PG_CONF"; then
    print_info "Setting archive_command..."
    if grep -q "^archive_command" "$PG_CONF"; then
        sed -i "s|^archive_command.*|archive_command = 'pgbackrest --stanza=$STANZA archive-push %p'|" "$PG_CONF"
    else
        echo "archive_command = 'pgbackrest --stanza=$STANZA archive-push %p'" >> "$PG_CONF"
    fi
    print_status "archive_command configured"
fi

# Check and update wal_level
if ! grep -q "^wal_level = replica" "$PG_CONF"; then
    print_info "Setting wal_level to replica..."
    if grep -q "^wal_level" "$PG_CONF"; then
        sed -i "s/^wal_level.*/wal_level = replica/" "$PG_CONF"
    else
        echo "wal_level = replica" >> "$PG_CONF"
    fi
    print_status "wal_level set to replica"
fi

# Check and update max_wal_senders
if ! grep -q "^max_wal_senders" "$PG_CONF"; then
    print_info "Setting max_wal_senders..."
    echo "max_wal_senders = 3" >> "$PG_CONF"
    print_status "max_wal_senders configured"
fi

print_status "PostgreSQL configuration updated"

# Restart PostgreSQL to apply changes
print_warning "PostgreSQL needs to restart to apply archiving settings"
print_info "Restarting PostgreSQL..."
systemctl restart postgresql

# Wait for PostgreSQL to start
sleep 5
if systemctl is-active --quiet postgresql; then
    print_status "PostgreSQL restarted successfully"
else
    print_error "PostgreSQL failed to restart"
    print_info "Check logs: sudo journalctl -u postgresql -n 50"
    exit 1
fi

# Clean up any existing locks
print_info "Cleaning up any existing pgBackRest locks..."
rm -f /tmp/pgbackrest/*.lock 2>/dev/null || true
pkill -9 pgbackrest 2>/dev/null || true
sleep 2
print_status "Cleanup complete"

# Create stanza
print_info "Creating pgBackRest stanza..."
if sudo -u postgres pgbackrest --stanza="$STANZA" stanza-create; then
    print_status "Stanza created successfully"
else
    print_error "Failed to create stanza"
    exit 1
fi

# Verify configuration
print_info "Verifying pgBackRest configuration..."
if sudo -u postgres pgbackrest --stanza="$STANZA" check; then
    print_status "Configuration verified successfully"
else
    print_error "Configuration verification failed"
    exit 1
fi

print_status "✅ pgBackRest setup complete!"
echo
print_info "Backup location: $BACKUP_PATH"
print_info "PostgreSQL archiving: ENABLED"
echo
print_info "To create your first backup, run:"
print_info "  sudo -u postgres pgbackrest --stanza=main --type=full backup"
echo
print_info "Or use the backup script:"
print_info "  ./backup_postgres.sh"