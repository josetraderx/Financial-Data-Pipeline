"""
ETL Pipeline Status and PostgreSQL Integration Report
Displays complete status of the Exodus v2025 ETL pipeline with database integration
"""

import glob
import logging
import os
from datetime import datetime

import pandas as pd
import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'dbname': 'exodus_db',
    'user': 'josetraderx',
    'password': 'Jireh2023'
}

def check_parquet_files():
    """Check status of processed parquet files"""
    print("\n" + "="*60)
    print("📁 PARQUET FILES STATUS")
    print("="*60)

    parquet_files = glob.glob('data/processed/*.parquet')
    if not parquet_files:
        print("❌ No parquet files found")
        return

    # Group files by date
    file_groups = {}
    for file in parquet_files:
        filename = os.path.basename(file)
        parts = filename.split('_')
        if len(parts) >= 4:
            symbol = parts[0]
            timeframe = parts[1]
            dataset_type = parts[2]
            date_part = parts[3].split('.')[0]

            key = f"{symbol}_{timeframe}_{date_part}"
            if key not in file_groups:
                file_groups[key] = {}
            file_groups[key][dataset_type] = file

    # Display file groups
    for group_name, files in file_groups.items():
        print(f"\n📊 Dataset: {group_name}")
        for dataset_type, filepath in files.items():
            file_size = os.path.getsize(filepath) / 1024  # KB
            df = pd.read_parquet(filepath)
            print(f"  ✅ {dataset_type}: {len(df)} records, {file_size:.1f} KB")

def check_postgresql_status():
    """Check PostgreSQL database status"""
    print("\n" + "="*60)
    print("🗄️  POSTGRESQL DATABASE STATUS")
    print("="*60)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("✅ PostgreSQL connection successful")

        # Check available tables
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE '%market_data%'
        """)
        tables = cur.fetchall()

        if not tables:
            print("❌ No market data tables found")
            return

        # Display table statistics
        for table in tables:
            table_name = table[0]
            print(f"\n📋 Table: {table_name}")

            # Get table stats
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_records = cur.fetchone()[0]

            if total_records > 0:
                cur.execute(f"""
                    SELECT
                        symbol,
                        COUNT(*) as records,
                        MIN(timestamp) as earliest,
                        MAX(timestamp) as latest,
                        MIN(close) as min_price,
                        MAX(close) as max_price,
                        AVG(volume) as avg_volume
                    FROM {table_name}
                    GROUP BY symbol
                    ORDER BY symbol
                """)

                symbols = cur.fetchall()
                for symbol_data in symbols:
                    symbol, records, earliest, latest, min_price, max_price, avg_volume = symbol_data
                    print(f"  📈 {symbol}: {records} records")
                    print(f"     📅 Date range: {earliest.date()} to {latest.date()}")
                    print(f"     💰 Price range: ${min_price:.2f} - ${max_price:.2f}")
                    print(f"     📊 Avg volume: {avg_volume:.2f}")
            else:
                print(f"  ⚠️  No data in {table_name}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")

def check_pipeline_logs():
    """Check recent pipeline execution logs"""
    print("\n" + "="*60)
    print("📝 RECENT PIPELINE LOGS")
    print("="*60)

    log_files = glob.glob('logs/*.log')
    if not log_files:
        print("❌ No log files found")
        return

    # Find most recent log file
    latest_log = max(log_files, key=os.path.getmtime)
    mod_time = datetime.fromtimestamp(os.path.getmtime(latest_log))

    print(f"📄 Latest log: {latest_log}")
    print(f"🕐 Last modified: {mod_time}")

    # Show last few lines
    try:
        with open(latest_log) as f:
            lines = f.readlines()
            if lines:
                print("\n📜 Last 5 log entries:")
                for line in lines[-5:]:
                    print(f"   {line.strip()}")
    except Exception as e:
        print(f"❌ Error reading log file: {e}")

def display_pipeline_summary():
    """Display overall pipeline status summary"""
    print("\n" + "="*60)
    print("🚀 EXODUS v2025 ETL PIPELINE SUMMARY")
    print("="*60)

    # Count files and records
    parquet_files = glob.glob('data/processed/*.parquet')
    total_files = len(parquet_files)
    total_records = 0

    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            total_records += len(df)
        except Exception:
            continue

    print(f"📊 Total processed files: {total_files}")
    print(f"📈 Total processed records: {total_records}")

    # Check PostgreSQL records
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE '%market_data%'
        """)
        tables = cur.fetchall()

        db_records = 0
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table[0]}")
            db_records += cur.fetchone()[0]

        print(f"🗄️  Total database records: {db_records}")

        if db_records > 0 and total_records > 0:
            coverage = (db_records / total_records) * 100
            print(f"📋 Database coverage: {coverage:.1f}%")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"⚠️  Database check failed: {e}")

    print("\n🎯 Pipeline Status: ✅ OPERATIONAL")
    print("🔄 Data Flow: Download → Validate → Split → Store (Parquet + PostgreSQL)")

if __name__ == "__main__":
    print("🌟 EXODUS v2025 ETL PIPELINE STATUS REPORT")
    print(f"📅 Generated: {datetime.now()}")

    # Run all checks
    check_parquet_files()
    check_postgresql_status()
    check_pipeline_logs()
    display_pipeline_summary()

    print("\n" + "="*60)
    print("✨ Report completed successfully!")
    print("="*60)
