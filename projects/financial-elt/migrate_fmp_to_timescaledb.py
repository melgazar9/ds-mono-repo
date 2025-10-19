"""
Migration script to convert existing tap_fmp_production tables to TimescaleDB hypertables.

This script handles tables that already have data and may have different primary key structures
than what's ideal for TimescaleDB. It uses a phased approach:
1. Validate table structure
2. Add required indexes if missing
3. Convert to hypertable with migrate_data=true
4. Add space partitioning
5. Configure compression
6. Add compression policies

Usage:
    python migrate_fmp_to_timescaledb.py --dry-run  # Preview changes
    python migrate_fmp_to_timescaledb.py            # Execute migration
    python migrate_fmp_to_timescaledb.py --table eod_bulk  # Migrate single table
"""

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from ds_core.db_connectors import PostgresConnect  # noqa: E402

ENV = os.getenv("ENVIRONMENT", "production")
SCHEMA = f"tap_fmp_{ENV}"

# ==============================
# Top 20 Priority Tables to Migrate
# ==============================
# Table: (time_column, segment_column, chunk_interval, compress_after_days, hash_partitions, requires_index)
MIGRATION_TABLES = {
    # Tier 1: Largest tables (78GB - 23GB)
    "eod_bulk": ("date", "symbol", "7 days", "30 days", 16, True),
    "exponential_moving_average": ("date", "symbol", "30 days", "180 days", 4, False),
    "simple_moving_average": ("date", "symbol", "30 days", "180 days", 4, False),
    "standard_deviation": ("date", "symbol", "30 days", "180 days", 4, False),
    "double_exponential_moving_average": ("date", "symbol", "30 days", "180 days", 4, False),
    "weighted_moving_average": ("date", "symbol", "30 days", "180 days", 4, False),
    "relative_strength_index": ("date", "symbol", "30 days", "180 days", 4, False),
    "triple_exponential_moving_average": ("date", "symbol", "30 days", "180 days", 4, False),
    "average_directional_index": ("date", "symbol", "30 days", "180 days", 4, False),
    "williams": ("date", "symbol", "30 days", "180 days", 2, False),
    # Tier 2: High-frequency data (16-22 GB)
    "commodities_full_chart": ("date", "symbol", "30 days", "180 days", 4, True),
    "forex_1min": ("date", "symbol", "1 day", "7 days", 8, False),
    "forex_full_chart": ("date", "symbol", "30 days", "180 days", 4, True),
    "commodities_1min": ("date", "symbol", "1 day", "7 days", 4, False),
    "index_1min": ("date", "symbol", "1 day", "7 days", 4, False),
    # Tier 3: Medium priority (11-13 GB)
    "historical_market_cap": ("date", "symbol", "30 days", "180 days", 4, False),
    "crypto_1h": ("date", "symbol", "7 days", "30 days", 4, False),
    "index_1h": ("date", "symbol", "7 days", "30 days", 4, False),
    "historical_index_full_chart": ("date", "symbol", "30 days", "180 days", 4, False),
    "historical_crypto_full_chart": ("date", "symbol", "30 days", "180 days", 4, False),
}

# SQL Templates
CHECK_TABLE_EXISTS = """
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = '{schema}'
      AND table_name = '{table}'
);
"""

CHECK_IS_HYPERTABLE = """
SELECT EXISTS (
    SELECT 1
    FROM timescaledb_information.hypertables
    WHERE hypertable_schema = '{schema}'
      AND hypertable_name = '{table}'
);
"""

CHECK_INDEX_EXISTS = """
SELECT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = '{schema}'
      AND tablename = '{table}'
      AND indexdef LIKE '%{time_col}%{segment_col}%'
);
"""

CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS {index_name}
ON {full_table} ({time_col} DESC, {segment_col});
"""

ANALYZE_TABLE = """
ANALYZE {full_table};
"""

CREATE_HYPERTABLE = """
SELECT create_hypertable(
    '{full_table}',
    by_range('{time_col}'),
    chunk_time_interval => INTERVAL '{chunk_interval}',
    migrate_data => true
);
"""

ADD_DIMENSION = """
SELECT add_dimension(
    '{full_table}',
    by_hash('{segment_col}', {hash_partitions})
);
"""

SET_COMPRESSION = """
ALTER TABLE {full_table} SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = '{segment_col}',
    timescaledb.compress_orderby = '{time_col} DESC'
);
"""

ADD_COMPRESSION_POLICY = """
SELECT add_compression_policy(
    '{full_table}',
    compress_after => INTERVAL '{compress_interval}'
);
"""

GET_TABLE_INFO = """
SELECT
    pg_size_pretty(pg_total_relation_size('{full_table}')) as size,
    (SELECT count(*) FROM {full_table}) as row_count,
    (SELECT min({time_col}) FROM {full_table}) as min_date,
    (SELECT max({time_col}) FROM {full_table}) as max_date;
"""


class TimescaleDBMigrator:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.db = None
        self.migration_log = []

    def log(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        self.migration_log.append(log_entry)
        print(log_entry)

    def execute_sql(self, sql, description, df_type=None, ignore_errors=False):
        """Execute SQL with proper error handling and dry-run support."""
        if self.dry_run:
            self.log(f"[DRY-RUN] Would execute: {description}", "INFO")
            self.log(f"[DRY-RUN] SQL: {sql[:200]}...", "DEBUG")
            return None

        try:
            result = self.db.run_sql(sql, df_type=df_type)
            self.log(f"✓ {description}", "SUCCESS")
            return result
        except Exception as e:
            error_msg = str(e)
            if ignore_errors:
                self.log(f"⚠ {description} - {error_msg} (ignored)", "WARNING")
                return None
            else:
                self.log(f"✗ {description} - {error_msg}", "ERROR")
                raise

    def check_table_exists(self, table):
        """Check if table exists in schema."""
        sql = CHECK_TABLE_EXISTS.format(schema=SCHEMA, table=table)
        result = self.db.run_sql(sql, df_type="pandas")
        return result.iloc[0, 0] if not result.empty else False

    def check_is_hypertable(self, table):
        """Check if table is already a hypertable."""
        sql = CHECK_IS_HYPERTABLE.format(schema=SCHEMA, table=table)
        result = self.db.run_sql(sql, df_type="pandas")
        return result.iloc[0, 0] if not result.empty else False

    def get_table_info(self, table, time_col):
        """Get basic table statistics."""
        full_table = f"{SCHEMA}.{table}"
        sql = GET_TABLE_INFO.format(full_table=full_table, time_col=time_col)
        try:
            result = self.db.run_sql(sql, df_type="pandas")
            return result.iloc[0].to_dict() if not result.empty else None
        except Exception as e:
            self.log(f"Could not get table info for {table}: {e}", "WARNING")
            return None

    def migrate_table(self, table, config):
        """Migrate a single table to TimescaleDB hypertable."""
        time_col, segment_col, chunk_interval, compress_interval, hash_partitions, requires_index = config
        full_table = f"{SCHEMA}.{table}"

        self.log(f"\n{'='*80}", "INFO")
        self.log(f"Migrating table: {full_table}", "INFO")
        self.log(f"{'='*80}", "INFO")

        # Step 1: Validate table exists
        if not self.dry_run and not self.check_table_exists(table):
            self.log(f"Table {table} does not exist. Skipping.", "WARNING")
            return False

        # Step 2: Check if already a hypertable
        if not self.dry_run and self.check_is_hypertable(table):
            self.log(f"Table {table} is already a hypertable. Skipping.", "WARNING")
            return False

        # Step 3: Get table info
        if not self.dry_run:
            info = self.get_table_info(table, time_col)
            if info:
                self.log(f"Table size: {info['size']}", "INFO")
                self.log(f"Row count: {info['row_count']:,}", "INFO")
                self.log(f"Date range: {info['min_date']} to {info['max_date']}", "INFO")

        # Step 4: Create index if required (for tables with composite PKs)
        if requires_index:
            index_name = f"idx_{table}_{time_col}_{segment_col}"
            sql = CREATE_INDEX.format(
                index_name=index_name,
                full_table=full_table,
                time_col=time_col,
                segment_col=segment_col,
            )
            self.execute_sql(
                sql,
                f"Creating index on ({time_col}, {segment_col})",
                ignore_errors=True
            )

        # Step 5: Run ANALYZE before migration (helps TimescaleDB optimize chunks)
        sql = ANALYZE_TABLE.format(full_table=full_table)
        self.execute_sql(sql, "Analyzing table statistics")

        # Step 6: Convert to hypertable
        sql = CREATE_HYPERTABLE.format(
            full_table=full_table, time_col=time_col, chunk_interval=chunk_interval
        )
        self.execute_sql(sql, f"Converting to hypertable with {chunk_interval} chunks")

        # Step 7: Add space partitioning (hash dimension)
        if hash_partitions > 1:
            sql = ADD_DIMENSION.format(
                full_table=full_table, segment_col=segment_col, hash_partitions=hash_partitions
            )
            self.execute_sql(
                sql,
                f"Adding space partitioning on {segment_col} ({hash_partitions} partitions)",
                ignore_errors=True
            )

        # Step 8: Enable compression
        sql = SET_COMPRESSION.format(
            full_table=full_table, segment_col=segment_col, time_col=time_col
        )
        self.execute_sql(
            sql,
            f"Enabling compression (segmentby={segment_col}, orderby={time_col})",
            ignore_errors=True
        )

        # Step 9: Add compression policy
        sql = ADD_COMPRESSION_POLICY.format(
            full_table=full_table, compress_interval=compress_interval
        )
        self.execute_sql(
            sql,
            f"Adding compression policy (compress after {compress_interval})",
            ignore_errors=True
        )

        self.log(f"✓ Successfully migrated {table}", "SUCCESS")
        return True

    def run_migration(self, target_tables=None):
        """Run migration for specified tables or all configured tables."""
        tables_to_migrate = target_tables if target_tables else list(MIGRATION_TABLES.keys())

        self.log(f"\n{'#'*80}", "INFO")
        self.log(f"TimescaleDB Migration - tap_fmp_{ENV}", "INFO")
        self.log(f"Mode: {'DRY-RUN' if self.dry_run else 'LIVE MIGRATION'}", "INFO")
        self.log(f"Tables to migrate: {len(tables_to_migrate)}", "INFO")
        self.log(f"{'#'*80}\n", "INFO")

        if self.dry_run:
            self.log("DRY-RUN MODE: No changes will be made to the database", "WARNING")

        success_count = 0
        failed_count = 0

        with PostgresConnect(database="financial_elt") as db:
            self.db = db
            self.db.con = self.db.con.execution_options(isolation_level="AUTOCOMMIT")

            for table in tables_to_migrate:
                if table not in MIGRATION_TABLES:
                    self.log(f"Table {table} not in migration config. Skipping.", "WARNING")
                    continue

                try:
                    config = MIGRATION_TABLES[table]
                    if self.migrate_table(table, config):
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    self.log(f"Failed to migrate {table}: {e}", "ERROR")
                    failed_count += 1
                    if not self.dry_run:
                        # Ask whether to continue
                        response = input(f"\nContinue with remaining tables? (y/n): ")
                        if response.lower() != 'y':
                            break

        # Summary
        self.log(f"\n{'#'*80}", "INFO")
        self.log(f"Migration Summary", "INFO")
        self.log(f"{'#'*80}", "INFO")
        self.log(f"Successfully migrated: {success_count}", "SUCCESS")
        self.log(f"Failed: {failed_count}", "ERROR" if failed_count > 0 else "INFO")

        # Show final hypertable status
        if not self.dry_run:
            self.log("\nQuerying final hypertable status...", "INFO")
            with PostgresConnect(database="financial_elt") as db:
                sql = f"""
                SELECT
                    hypertable_name,
                    num_dimensions,
                    num_chunks,
                    compression_enabled
                FROM timescaledb_information.hypertables
                WHERE hypertable_schema = '{SCHEMA}'
                ORDER BY hypertable_name;
                """
                result = db.run_sql(sql, df_type="pandas")
                if not result.empty:
                    print("\nMigrated Hypertables:")
                    print(result.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description="Migrate tap_fmp_production tables to TimescaleDB hypertables"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview migration without making changes"
    )
    parser.add_argument(
        "--table",
        type=str,
        help="Migrate a specific table (default: migrate all configured tables)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List tables configured for migration"
    )

    args = parser.parse_args()

    if args.list:
        print("Tables configured for migration:")
        for i, (table, config) in enumerate(MIGRATION_TABLES.items(), 1):
            time_col, segment_col, chunk_interval, compress_interval, hash_partitions, _ = config
            print(f"{i:2d}. {table}")
            print(f"    Time column: {time_col}, Segment: {segment_col}")
            print(f"    Chunks: {chunk_interval}, Compress: {compress_interval}, Partitions: {hash_partitions}")
        return

    target_tables = [args.table] if args.table else None

    migrator = TimescaleDBMigrator(dry_run=args.dry_run)
    migrator.run_migration(target_tables=target_tables)


if __name__ == "__main__":
    main()
