"""
TimescaleDB connector for efficient time series data storage in Exodus v2025.
Handles OHLCV data as hypertables for fast queries and automatic partitioning.
"""

import logging
from datetime import datetime
from typing import Literal

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

REQUIRED_OHLCV_COLUMNS = [
    'timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume',
    'hour', 'day_of_week', 'month', 'year'
]

DEFAULT_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'exodus_trading',
    'user': 'postgres',
    'password': 'Jireh2023.'
}

class TimeSeriesDB:
    """
    PostgreSQL/TimescaleDB connector for time series data storage.
    Provides efficient storage and retrieval of OHLCV trading data.
    """

    def __init__(
        self,
        config: dict | None = None,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize TimescaleDB connection.

        Args:
            config: Database configuration dictionary with host, port, database, user, password
            logger: Optional logger instance
        """
        self.config = config if config is not None else DEFAULT_CONFIG
        self.logger = logger or logging.getLogger(__name__)
        self.conn = None
        self.engine = None
        self._verify_config()

    def _verify_config(self):
        """Verify that all required config keys are present."""
        required_keys = ['host', 'port', 'database', 'user', 'password']
        for key in required_keys:
            if key not in self.config:
                raise KeyError(f"Missing required configuration key: {key}")

        # Ensure port is an integer
        try:
            self.config['port'] = int(self.config['port'])
        except (ValueError, TypeError):
            raise ValueError("Port must be a valid integer")

    def is_connected(self) -> bool:
        """Check if database is connected."""
        return self.conn is not None and not self.conn.closed

    def connect(self) -> bool:
        """
        Connect to the TimescaleDB database.

        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            if self.is_connected():
                return True

            # Create connection
            self.conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )

            # Create SQLAlchemy engine for DataFrame operations
            db_url = f"postgresql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            self.engine = create_engine(db_url)

            # Verify TimescaleDB extension
            with self.conn.cursor() as cur:
                cur.execute("SELECT extname FROM pg_extension WHERE extname = 'timescaledb';")
                if not cur.fetchone():
                    self.logger.warning("TimescaleDB extension not found in database")
                    return False

            self.logger.info(f"Successfully connected to {self.config['database']} at {self.config['host']}:{self.config['port']}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            return False

    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
        if self.engine:
            self.engine.dispose()
        self.conn = None
        self.engine = None

    def create_ohlcv_hypertable(self, table_name: str = "ohlcv_data") -> bool:
        """
        Create hypertable for OHLCV data if it doesn't exist.
        Uses FLOAT for OHLCV columns for performance.

        Args:
            table_name: Name of the hypertable.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            with self.engine.begin() as conn:
                # Create table
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    timestamp TIMESTAMPTZ NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    open FLOAT NOT NULL,
                    high FLOAT NOT NULL,
                    low FLOAT NOT NULL,
                    close FLOAT NOT NULL,
                    volume FLOAT NOT NULL,
                    hour INT,
                    day_of_week INT,
                    month INT,
                    year INT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
                conn.execute(text(create_table_sql))

                # Create hypertable (TimescaleDB specific)
                hypertable_sql = """
                SELECT create_hypertable(:table, 'timestamp', if_not_exists => TRUE);
                """
                conn.execute(text(hypertable_sql), {"table": table_name})

                # Create index for symbol lookups
                index_sql = f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_timestamp
                ON {table_name} (symbol, timestamp DESC);
                """
                conn.execute(text(index_sql))

                # Compresión nativa TimescaleDB (opcional)
                try:
                    conn.execute(text(f"ALTER TABLE {table_name} SET (timescaledb.compress, timescaledb.compress_segmentby = 'symbol');"))
                    conn.execute(text(f"SELECT add_compression_policy('{table_name}', INTERVAL '90 days');"))
                except Exception:
                    pass

                self.logger.info(f"Hypertable {table_name} created successfully")
                return True

        except Exception as e:
            self.logger.error(f"Failed to create hypertable {table_name}: {e}")
            return False

    def insert_ohlcv_data(
        self,
        df: pd.DataFrame,
        table_name: str = "ohlcv_data",
        if_exists: Literal['append', 'replace', 'fail'] = "append"
    ) -> bool:
        """
        Insert OHLCV DataFrame into TimescaleDB hypertable.
        Validates DataFrame columns before insert.

        Args:
            df: DataFrame with OHLCV data.
            table_name: Target table name.
            if_exists: How to behave if table exists ('append', 'replace', 'fail').

        Returns:
            bool: True if successful, False otherwise.
        """
        # Validación de columnas
        missing = [col for col in REQUIRED_OHLCV_COLUMNS if col not in df.columns]
        if missing:
            self.logger.error(f"Missing columns for insert: {missing}")
            return False
        try:
            # Ensure timestamp column is timezone-aware
            if 'timestamp' in df.columns:
                if df['timestamp'].dt.tz is None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
                else:
                    df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')

            # Insert data
            df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=False,
                method='multi'
            )

            self.logger.info(f"Inserted {len(df)} rows into {table_name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to insert data into {table_name}: {e}")
            return False

    def query_ohlcv_data(
        self,
        symbol: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        table_name: str = "ohlcv_data",
        limit: int | None = None
    ) -> pd.DataFrame:
        """
        Query OHLCV data from TimescaleDB. Uses parameters to prevent SQL injection.

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT').
            start_time: Start timestamp (optional).
            end_time: End timestamp (optional).
            table_name: Source table name.
            limit: Maximum number of rows to return.

        Returns:
            pd.DataFrame: Query results.
        """
        try:
            # Build WHERE clause
            where_conditions = ["symbol = :symbol"]
            params = {"symbol": symbol}

            if start_time:
                where_conditions.append("timestamp >= :start_time")
                params["start_time"] = start_time
            if end_time:
                where_conditions.append("timestamp <= :end_time")
                params["end_time"] = end_time

            where_clause = " AND ".join(where_conditions)

            # Build query
            query = f"""
            SELECT timestamp, symbol, open, high, low, close, volume,
                   hour, day_of_week, month, year
            FROM {table_name}
            WHERE {where_clause}
            ORDER BY timestamp DESC
            """

            if limit:
                query += f" LIMIT {limit}"

            # Execute query
            df = pd.read_sql(text(query), self.engine, params=params)

            # Convert timestamp to datetime with timezone
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

            self.logger.info(f"Retrieved {len(df)} rows for {symbol}")
            return df

        except Exception as e:
            self.logger.error(f"Failed to query data for {symbol}: {e}")
            return pd.DataFrame()

    def get_latest_timestamp(self, symbol: str, table_name: str = "ohlcv_data") -> datetime | None:
        """
        Get the latest timestamp for a specific symbol.
        Uses parameters for safety.

        Args:
            symbol: Trading symbol.
            table_name: Source table name.

        Returns:
            datetime: Latest timestamp or None if no data.
        """
        try:
            query = f"""
            SELECT MAX(timestamp) as latest_timestamp
            FROM {table_name}
            WHERE symbol = :symbol
            """

            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"symbol": symbol})
                latest = result.fetchone()[0]

            if latest:
                self.logger.info(f"Latest timestamp for {symbol}: {latest}")
                return latest
            else:
                self.logger.info(f"No data found for {symbol}")
                return None

        except Exception as e:
            self.logger.error(f"Failed to get latest timestamp for {symbol}: {e}")
            return None

    def get_symbol_stats(self, symbol: str, table_name: str = "ohlcv_data") -> dict:
        """
        Get basic statistics for a symbol.
        Uses parameters for safety.

        Args:
            symbol: Trading symbol.
            table_name: Source table name.

        Returns:
            dict: Statistics including count, date range, etc.
        """
        try:
            query = f"""
            SELECT
                COUNT(*) as record_count,
                MIN(timestamp) as first_timestamp,
                MAX(timestamp) as last_timestamp,
                MIN(close) as min_price,
                MAX(close) as max_price,
                AVG(close) as avg_price,
                SUM(volume) as total_volume
            FROM {table_name}
            WHERE symbol = :symbol
            """

            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"symbol": symbol})
                row = result.fetchone()

            stats = {
                'symbol': symbol,
                'record_count': row[0],
                'first_timestamp': row[1],
                'last_timestamp': row[2],
                'min_price': float(row[3]) if row[3] else None,
                'max_price': float(row[4]) if row[4] else None,
                'avg_price': float(row[5]) if row[5] else None,
                'total_volume': float(row[6]) if row[6] else None
            }

            self.logger.info(f"Retrieved stats for {symbol}: {stats['record_count']} records")
            return stats

        except Exception as e:
            self.logger.error(f"Failed to get stats for {symbol}: {e}")
            return {}

    def upsert_ohlcv_data(
        self,
        df: pd.DataFrame,
        table_name: str = "ohlcv_data"
    ) -> bool:
        """
        Upsert (insert or update) OHLCV data to avoid duplicates.
        """
        # Validación de columnas
        missing = [col for col in REQUIRED_OHLCV_COLUMNS if col not in df.columns]
        if missing:
            self.logger.error(f"Missing columns for upsert: {missing}")
            return False
        try:
            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    upsert_sql = text(f"""
                        INSERT INTO {table_name} (timestamp, symbol, open, high, low, close, volume, hour, day_of_week, month, year)
                        VALUES (:timestamp, :symbol, :open, :high, :low, :close, :volume, :hour, :day_of_week, :month, :year)
                        ON CONFLICT (timestamp, symbol) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            hour = EXCLUDED.hour,
                            day_of_week = EXCLUDED.day_of_week,
                            month = EXCLUDED.month,
                            year = EXCLUDED.year;
                    """)
                    conn.execute(upsert_sql, row.to_dict())
            self.logger.info(f"Upserted {len(df)} rows into {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to upsert data into {table_name}: {e}")
            return False

    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            self.logger.info("TimescaleDB connection closed")

if __name__ == "__main__":
    import os
    from datetime import datetime

    import pandas as pd
    from dotenv import load_dotenv

    # Cargar variables de entorno desde el archivo de configuración
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
                              'config', 'database.env')
    load_dotenv(config_path)

    # Configuración desde variables de entorno
    db = TimeSeriesDB(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME', 'exodus_db'),
        username=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'password')
    )

    # Test de conexión y operaciones
    if db.connect():
        print("✅ Conexión exitosa a PostgreSQL")

        if db.create_ohlcv_hypertable():
            print("✅ Tabla OHLCV creada exitosamente")

            # Crear datos de ejemplo
            df = pd.DataFrame({
                'timestamp': [pd.Timestamp(datetime.now(), tz='UTC')],
                'symbol': ['BTCUSDT'],
                'open': [30000.0],
                'high': [30500.0],
                'low': [29900.0],
                'close': [30400.0],
                'volume': [100.0],
                'hour': [datetime.now().hour],
                'day_of_week': [datetime.now().weekday()],
                'month': [datetime.now().month],
                'year': [datetime.now().year]
            })

            if db.insert_ohlcv_data(df):
                print("✅ Datos de ejemplo insertados correctamente")

                # Consultar datos
                result_df = db.query_ohlcv_data('BTCUSDT', limit=5)
                if not result_df.empty:
                    print("\n📊 Últimos datos:")
                    print(result_df)

                stats = db.get_symbol_stats('BTCUSDT')
                if stats:
                    print("\n📈 Estadísticas:")
                    for key, value in stats.items():
                        print(f"{key}: {value}")

        db.close()
        print("\n✅ Conexión cerrada correctamente")
