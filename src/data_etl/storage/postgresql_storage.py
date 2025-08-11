"""
PostgreSQL Data Storage Module for ETL Pipeline
Handles insertion of processed market data into PostgreSQL database
"""
import logging
import os
from typing import Any

import pandas as pd
import psycopg2

logger = logging.getLogger(__name__)

class PostgreSQLStorage:
    """Handles PostgreSQL storage operations for market data"""

    def __init__(self, connection_params: dict[str, Any]):
        """
        Initialize PostgreSQL storage handler

        Args:
            connection_params: Dictionary with connection parameters
                              (host, port, dbname, user, password)
        """
        self.connection_params = connection_params
        self.conn = None
        self.cur = None

    def connect(self) -> bool:
        """
        Establish connection to PostgreSQL database

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.cur = self.conn.cursor()
            logger.info("Successfully connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False

    def disconnect(self):
        """Close database connections"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connections closed")

    def create_market_data_table(self, table_name: str = "market_data_clean") -> bool:
        """
        Create market data table if it doesn't exist

        Args:
            table_name: Name of the table to create

        Returns:
            bool: True if table created/exists, False otherwise
        """
        try:
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    open DECIMAL(20, 8) NOT NULL,
                    high DECIMAL(20, 8) NOT NULL,
                    low DECIMAL(20, 8) NOT NULL,
                    close DECIMAL(20, 8) NOT NULL,
                    volume DECIMAL(20, 8) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp, symbol)
                )
            """
            self.cur.execute(create_table_sql)
            self.conn.commit()
            logger.info(f"Table '{table_name}' ensured to exist")
            return True
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return False

    def insert_market_data(self, df: pd.DataFrame, symbol: str,
                          table_name: str = "market_data_clean") -> dict[str, int]:
        """
        Insert market data from DataFrame into PostgreSQL table

        Args:
            df: DataFrame with market data (timestamp, open, high, low, close, volume)
            symbol: Trading symbol (e.g., 'BTCUSDT')
            table_name: Name of the target table

        Returns:
            Dict with insertion statistics (inserted, skipped, errors)
        """
        stats = {"inserted": 0, "skipped": 0, "errors": 0}

        try:
            # Add symbol column if not present
            if 'symbol' not in df.columns:
                df = df.copy()
                df['symbol'] = symbol

            # Prepare data for insertion
            data_to_insert = []
            for _, row in df.iterrows():
                try:
                    data_to_insert.append((
                        row['timestamp'],
                        row['symbol'],
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        float(row['volume'])
                    ))
                except Exception as e:
                    logger.warning(f"Skipping row due to data conversion error: {e}")
                    stats["errors"] += 1

            # Insert data using executemany for better performance
            if data_to_insert:
                self.cur.executemany(f"""
                    INSERT INTO {table_name} (timestamp, symbol, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp, symbol) DO NOTHING
                """, data_to_insert)

                # Count actually inserted records
                stats["inserted"] = self.cur.rowcount
                stats["skipped"] = len(data_to_insert) - stats["inserted"]

                self.conn.commit()
                logger.info(f"Inserted {stats['inserted']} records, skipped {stats['skipped']} duplicates")

        except Exception as e:
            logger.error(f"Error during data insertion: {e}")
            self.conn.rollback()
            stats["errors"] = len(data_to_insert)

        return stats

    def get_table_stats(self, symbol: str, table_name: str = "market_data_clean") -> dict | None:
        """
        Get statistics for inserted data

        Args:
            symbol: Trading symbol to get stats for
            table_name: Name of the table

        Returns:
            Dictionary with statistics or None if error
        """
        try:
            self.cur.execute(f"""
                SELECT
                    COUNT(*) as total_records,
                    MIN(timestamp) as earliest_date,
                    MAX(timestamp) as latest_date,
                    MIN(close) as min_price,
                    MAX(close) as max_price,
                    AVG(volume) as avg_volume
                FROM {table_name}
                WHERE symbol = %s
            """, (symbol,))

            result = self.cur.fetchone()
            if result:
                return {
                    "total_records": result[0],
                    "earliest_date": result[1],
                    "latest_date": result[2],
                    "min_price": float(result[3]) if result[3] else None,
                    "max_price": float(result[4]) if result[4] else None,
                    "avg_volume": float(result[5]) if result[5] else None
                }
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
            return None

def store_processed_data_to_postgresql(parquet_file_path: str,
                                     connection_params: dict[str, Any],
                                     table_name: str = "market_data_clean") -> bool:
    """
    Main function to store processed parquet data to PostgreSQL

    Args:
        parquet_file_path: Path to the processed parquet file
        connection_params: PostgreSQL connection parameters
        table_name: Target table name

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Load parquet data
        df = pd.read_parquet(parquet_file_path)

        # Extract symbol from filename
        filename = os.path.basename(parquet_file_path)
        symbol = filename.split('_')[0]

        logger.info(f"Loading {len(df)} records for {symbol} from {parquet_file_path}")

        # Initialize storage handler
        storage = PostgreSQLStorage(connection_params)

        if not storage.connect():
            return False

        # Create table
        if not storage.create_market_data_table(table_name):
            storage.disconnect()
            return False

        # Insert data
        storage.insert_market_data(df, symbol, table_name)

        # Get and log statistics
        table_stats = storage.get_table_stats(symbol, table_name)
        if table_stats:
            logger.info(f"Storage completed for {symbol}:")
            logger.info(f"  Total records in DB: {table_stats['total_records']}")
            logger.info(f"  Date range: {table_stats['earliest_date']} to {table_stats['latest_date']}")
            logger.info(f"  Price range: ${table_stats['min_price']:.2f} to ${table_stats['max_price']:.2f}")
            logger.info(f"  Average volume: {table_stats['avg_volume']:.2f}")

        storage.disconnect()
        return True

    except Exception as e:
        logger.error(f"Failed to store data to PostgreSQL: {e}")
        return False

# Default connection parameters (can be overridden)
DEFAULT_CONNECTION_PARAMS = {
    'host': 'localhost',
    'port': 5433,
    'dbname': 'exodus_db',
    'user': 'josetraderx',
    'password': 'Jireh2023'
}

if __name__ == "__main__":
    # Example usage as standalone script
    import glob

    # Find processed parquet files
    parquet_files = glob.glob('data/processed/BTCUSDT_D_full*.parquet')

    if parquet_files:
        success = store_processed_data_to_postgresql(
            parquet_files[0],
            DEFAULT_CONNECTION_PARAMS
        )
        if success:
            print("✅ Data successfully stored to PostgreSQL")
        else:
            print("❌ Failed to store data to PostgreSQL")
    else:
        print("❌ No processed parquet files found")
