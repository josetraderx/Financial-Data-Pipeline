"""
PostgreSQL metadata management for Exodus v2025 data module.
Handles storage and retrieval of dataset metadata, validation reports, and data lineage.
"""

import logging
from typing import Any

import psycopg2
from psycopg2.extras import Json, RealDictCursor

logger = logging.getLogger(__name__)


class MetadataDB:
    """
    PostgreSQL database manager for metadata storage and retrieval.
    """

    def __init__(self, connection_params: dict[str, Any]):
        """
        Initialize the metadata database connection.

        Args:
            connection_params: Database connection parameters
                (host, port, database, user, password)
        """
        self.connection_params = connection_params
        self.connection = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            logger.info("Connected to PostgreSQL metadata database")
        except Exception as e:
            logger.error(f"Failed to connect to metadata database: {e}")
            raise

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from metadata database")

    def create_tables(self) -> None:
        """Create necessary tables for metadata storage."""
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()

        # Dataset metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dataset_metadata (
                id SERIAL PRIMARY KEY,
                dataset_name VARCHAR(255) NOT NULL,
                provider VARCHAR(100) NOT NULL,
                symbol VARCHAR(50),
                timeframe VARCHAR(20),
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                total_records INTEGER,
                file_path TEXT,
                file_size_bytes BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata_json JSONB,
                UNIQUE(dataset_name, provider, symbol, timeframe)
            )
        """)

        # Validation reports table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS validation_reports (
                id SERIAL PRIMARY KEY,
                dataset_id INTEGER REFERENCES dataset_metadata(id),
                validation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_valid BOOLEAN NOT NULL,
                total_records INTEGER,
                valid_records INTEGER,
                invalid_records INTEGER,
                missing_values INTEGER,
                outliers_detected INTEGER,
                duplicates_found INTEGER,
                validation_details JSONB,
                errors JSONB,
                warnings JSONB
            )
        """)

        # Data lineage table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_lineage (
                id SERIAL PRIMARY KEY,
                source_dataset_id INTEGER REFERENCES dataset_metadata(id),
                target_dataset_id INTEGER REFERENCES dataset_metadata(id),
                transformation_type VARCHAR(100),
                transformation_details JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Data quality metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_metrics (
                id SERIAL PRIMARY KEY,
                dataset_id INTEGER REFERENCES dataset_metadata(id),
                metric_name VARCHAR(100) NOT NULL,
                metric_value FLOAT,
                metric_details JSONB,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Indexes for better performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_dataset_metadata_provider
            ON dataset_metadata(provider)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_dataset_metadata_symbol
            ON dataset_metadata(symbol)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_validation_reports_dataset_id
            ON validation_reports(dataset_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_data_lineage_source
            ON data_lineage(source_dataset_id)
        """)

        self.connection.commit()
        logger.info("Metadata tables created successfully")

    def insert_dataset_metadata(self, metadata: dict[str, Any]) -> int:
        """
        Insert or update dataset metadata.

        Args:
            metadata: Dictionary containing dataset metadata

        Returns:
            Dataset ID
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()

        # Check if dataset already exists
        cursor.execute("""
            SELECT id FROM dataset_metadata
            WHERE dataset_name = %s AND provider = %s
            AND COALESCE(symbol, '') = COALESCE(%s, '')
            AND COALESCE(timeframe, '') = COALESCE(%s, '')
        """, (
            metadata.get('dataset_name'),
            metadata.get('provider'),
            metadata.get('symbol'),
            metadata.get('timeframe')
        ))

        existing = cursor.fetchone()

        if existing:
            # Update existing record
            cursor.execute("""
                UPDATE dataset_metadata SET
                    start_date = %s,
                    end_date = %s,
                    total_records = %s,
                    file_path = %s,
                    file_size_bytes = %s,
                    updated_at = CURRENT_TIMESTAMP,
                    metadata_json = %s
                WHERE id = %s
                RETURNING id
            """, (
                metadata.get('start_date'),
                metadata.get('end_date'),
                metadata.get('total_records'),
                metadata.get('file_path'),
                metadata.get('file_size_bytes'),
                Json(metadata.get('additional_metadata', {})),
                existing[0]
            ))
            dataset_id = cursor.fetchone()[0]
        else:
            # Insert new record
            cursor.execute("""
                INSERT INTO dataset_metadata (
                    dataset_name, provider, symbol, timeframe,
                    start_date, end_date, total_records, file_path,
                    file_size_bytes, metadata_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                metadata.get('dataset_name'),
                metadata.get('provider'),
                metadata.get('symbol'),
                metadata.get('timeframe'),
                metadata.get('start_date'),
                metadata.get('end_date'),
                metadata.get('total_records'),
                metadata.get('file_path'),
                metadata.get('file_size_bytes'),
                Json(metadata.get('additional_metadata', {}))
            ))
            dataset_id = cursor.fetchone()[0]

        self.connection.commit()
        logger.info(f"Dataset metadata stored with ID: {dataset_id}")
        return dataset_id

    def insert_validation_report(self, dataset_id: int, report: dict[str, Any]) -> int:
        """
        Insert validation report for a dataset.

        Args:
            dataset_id: ID of the dataset
            report: Validation report dictionary

        Returns:
            Validation report ID
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()

        cursor.execute("""
            INSERT INTO validation_reports (
                dataset_id, is_valid, total_records, valid_records,
                invalid_records, missing_values, outliers_detected,
                duplicates_found, validation_details, errors, warnings
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            dataset_id,
            report.get('is_valid', False),
            report.get('total_records', 0),
            report.get('valid_records', 0),
            report.get('invalid_records', 0),
            report.get('missing_values', 0),
            report.get('outliers_detected', 0),
            report.get('duplicates_found', 0),
            Json(report.get('validation_details', {})),
            Json(report.get('errors', [])),
            Json(report.get('warnings', []))
        ))

        report_id = cursor.fetchone()[0]
        self.connection.commit()
        logger.info(f"Validation report stored with ID: {report_id}")
        return report_id

    def insert_data_lineage(self, source_id: int, target_id: int,
                          transformation_type: str, details: dict[str, Any]) -> int:
        """
        Insert data lineage record.

        Args:
            source_id: Source dataset ID
            target_id: Target dataset ID
            transformation_type: Type of transformation
            details: Transformation details

        Returns:
            Lineage record ID
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()

        cursor.execute("""
            INSERT INTO data_lineage (
                source_dataset_id, target_dataset_id,
                transformation_type, transformation_details
            ) VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (source_id, target_id, transformation_type, Json(details)))

        lineage_id = cursor.fetchone()[0]
        self.connection.commit()
        logger.info(f"Data lineage record stored with ID: {lineage_id}")
        return lineage_id

    def insert_quality_metrics(self, dataset_id: int, metrics: dict[str, float]) -> None:
        """
        Insert data quality metrics for a dataset.

        Args:
            dataset_id: ID of the dataset
            metrics: Dictionary of metric names and values
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor()

        for metric_name, metric_value in metrics.items():
            cursor.execute("""
                INSERT INTO data_quality_metrics (
                    dataset_id, metric_name, metric_value
                ) VALUES (%s, %s, %s)
            """, (dataset_id, metric_name, metric_value))

        self.connection.commit()
        logger.info(f"Quality metrics stored for dataset {dataset_id}")

    def get_dataset_metadata(self, dataset_id: int | None = None,
                           provider: str | None = None,
                           symbol: str | None = None) -> list[dict[str, Any]]:
        """
        Retrieve dataset metadata.

        Args:
            dataset_id: Specific dataset ID (optional)
            provider: Filter by provider (optional)
            symbol: Filter by symbol (optional)

        Returns:
            List of dataset metadata records
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor(cursor_factory=RealDictCursor)

        query = "SELECT * FROM dataset_metadata WHERE 1=1"
        params = []

        if dataset_id:
            query += " AND id = %s"
            params.append(dataset_id)
        if provider:
            query += " AND provider = %s"
            params.append(provider)
        if symbol:
            query += " AND symbol = %s"
            params.append(symbol)

        query += " ORDER BY created_at DESC"

        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def get_validation_reports(self, dataset_id: int) -> list[dict[str, Any]]:
        """
        Retrieve validation reports for a dataset.

        Args:
            dataset_id: ID of the dataset

        Returns:
            List of validation reports
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT * FROM validation_reports
            WHERE dataset_id = %s
            ORDER BY validation_timestamp DESC
        """, (dataset_id,))

        return [dict(row) for row in cursor.fetchall()]

    def get_data_lineage(self, dataset_id: int) -> dict[str, list[dict[str, Any]]]:
        """
        Retrieve data lineage for a dataset.

        Args:
            dataset_id: ID of the dataset

        Returns:
            Dictionary with upstream and downstream lineage
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor(cursor_factory=RealDictCursor)

        # Get upstream lineage (sources)
        cursor.execute("""
            SELECT l.*, dm.dataset_name as source_name
            FROM data_lineage l
            JOIN dataset_metadata dm ON l.source_dataset_id = dm.id
            WHERE l.target_dataset_id = %s
            ORDER BY l.created_at DESC
        """, (dataset_id,))
        upstream = [dict(row) for row in cursor.fetchall()]

        # Get downstream lineage (targets)
        cursor.execute("""
            SELECT l.*, dm.dataset_name as target_name
            FROM data_lineage l
            JOIN dataset_metadata dm ON l.target_dataset_id = dm.id
            WHERE l.source_dataset_id = %s
            ORDER BY l.created_at DESC
        """, (dataset_id,))
        downstream = [dict(row) for row in cursor.fetchall()]

        return {
            'upstream': upstream,
            'downstream': downstream
        }

    def get_quality_metrics(self, dataset_id: int) -> list[dict[str, Any]]:
        """
        Retrieve quality metrics for a dataset.

        Args:
            dataset_id: ID of the dataset

        Returns:
            List of quality metrics
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT * FROM data_quality_metrics
            WHERE dataset_id = %s
            ORDER BY calculated_at DESC
        """, (dataset_id,))

        return [dict(row) for row in cursor.fetchall()]

    def get_dataset_summary(self) -> dict[str, Any]:
        """
        Get a summary of all datasets in the database.

        Returns:
            Summary statistics
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")

        cursor = self.connection.cursor(cursor_factory=RealDictCursor)

        # Total datasets
        cursor.execute("SELECT COUNT(*) as total_datasets FROM dataset_metadata")
        total_datasets = cursor.fetchone()['total_datasets']

        # Datasets by provider
        cursor.execute("""
            SELECT provider, COUNT(*) as count
            FROM dataset_metadata
            GROUP BY provider
            ORDER BY count DESC
        """)
        by_provider = [dict(row) for row in cursor.fetchall()]

        # Recent validation status
        cursor.execute("""
            SELECT
                SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid_datasets,
                SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) as invalid_datasets
            FROM validation_reports vr
            WHERE vr.id IN (
                SELECT MAX(id) FROM validation_reports
                GROUP BY dataset_id
            )
        """)
        validation_status = dict(cursor.fetchone())

        return {
            'total_datasets': total_datasets,
            'by_provider': by_provider,
            'validation_status': validation_status
        }

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
