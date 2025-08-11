"""
Enhanced metadata manager for Exodus v2025.
Provides comprehensive dataset tracking, validation, and integrity checking.
"""

import hashlib
import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

# Exodus v2025 imports with fallback
try:
    from data_etl.utils.config import get_config
    from data_etl.utils.logging import get_logger
except ImportError:
    # Fallback para cuando se ejecuta standalone
    def get_logger(name):
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / "metadata_manager.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )
        return logging.getLogger(name)

    def get_config():
        return {}


logger = get_logger(__name__)


@dataclass
class DatasetMetadata:
    """Structured metadata for datasets."""

    symbol: str
    interval: str
    csv_file: str
    num_records: int
    hash_sha256: str
    created_at: str
    exodus_version: str = "v2025"
    script_version: str | None = None
    data_source: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    file_size_bytes: int | None = None
    columns: list[str] | None = None
    data_quality_score: float | None = None
    missing_data_percentage: float | None = None
    extra_fields: dict | None = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        # Remove None values
        return {k: v for k, v in result.items() if v is not None}


class EnhancedMetadataManager:
    """
    Enhanced metadata manager for Exodus v2025.
    Provides comprehensive dataset tracking and validation.
    """

    def __init__(self, base_data_dir: Path | None = None):
        """Initialize metadata manager."""
        self.logger = get_logger(__name__)
        self.config = get_config()

        # Set base directory
        self.base_data_dir = base_data_dir or Path("data")
        self.metadata_dir = self.base_data_dir / "raw" / "metadata"
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

        # Metadata index for quick searches
        self.metadata_index_file = self.metadata_dir / "index.json"
        self._load_metadata_index()

    def _load_metadata_index(self):
        """Load metadata index for fast searches."""
        try:
            if self.metadata_index_file.exists():
                with open(self.metadata_index_file, encoding="utf-8") as f:
                    self.metadata_index = json.load(f)
            else:
                self.metadata_index = {}
                self._rebuild_metadata_index()
        except Exception as e:
            self.logger.warning(f"Could not load metadata index: {e}")
            self.metadata_index = {}

    def _save_metadata_index(self):
        """Save metadata index to disk."""
        try:
            with open(self.metadata_index_file, "w", encoding="utf-8") as f:
                json.dump(self.metadata_index, f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Could not save metadata index: {e}")

    def _rebuild_metadata_index(self):
        """Rebuild metadata index from existing files."""
        self.logger.info("Rebuilding metadata index...")
        self.metadata_index = {}

        for metadata_file in self.metadata_dir.glob("*.json"):
            if metadata_file.name == "index.json":
                continue

            try:
                with open(metadata_file, encoding="utf-8") as f:
                    metadata = json.load(f)

                key = f"{metadata.get('symbol', 'unknown')}_{metadata.get('interval', 'unknown')}"
                self.metadata_index[key] = {
                    "file": metadata_file.name,
                    "symbol": metadata.get("symbol"),
                    "interval": metadata.get("interval"),
                    "created_at": metadata.get("created_at"),
                    "num_records": metadata.get("num_records"),
                }
            except Exception as e:
                self.logger.warning(f"Could not read metadata file {metadata_file}: {e}")

        self._save_metadata_index()
        self.logger.info(f"Rebuilt metadata index with {len(self.metadata_index)} entries")

    def calculate_sha256(self, file_path: str | Path) -> str:
        """Calculate SHA-256 hash of a file."""
        sha256_hash = hashlib.sha256()
        file_path = Path(file_path)

        try:
            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()
        except Exception as e:
            self.logger.error(f"Error calculating hash for {file_path}: {e}")
            return "error_calculating_hash"

    def analyze_data_quality(self, df: pd.DataFrame) -> dict[str, float]:
        """Analyze data quality metrics."""
        try:
            total_cells = df.size
            missing_cells = df.isnull().sum().sum()
            missing_percentage = (missing_cells / total_cells) * 100 if total_cells > 0 else 0

            # Calculate a basic quality score
            quality_score = max(0, 100 - missing_percentage)

            return {
                "data_quality_score": round(quality_score, 2),
                "missing_data_percentage": round(missing_percentage, 2),
            }
        except Exception as e:
            self.logger.warning(f"Could not analyze data quality: {e}")
            return {"data_quality_score": None, "missing_data_percentage": None}

    def get_data_date_range(self, df: pd.DataFrame) -> dict[str, str | None]:
        """Extract date range from dataframe."""
        try:
            if "timestamp" in df.columns:
                timestamps = pd.to_datetime(df["timestamp"], errors="coerce")
                valid_timestamps = timestamps.dropna()

                if len(valid_timestamps) > 0:
                    return {
                        "start_date": valid_timestamps.min().isoformat(),
                        "end_date": valid_timestamps.max().isoformat(),
                    }
        except Exception as e:
            self.logger.warning(f"Could not extract date range: {e}")

        return {"start_date": None, "end_date": None}

    def save_metadata(
        self,
        csv_path: str | Path,
        symbol: str,
        interval: str,
        num_records: int,
        script_version: str | None = None,
        data_source: str | None = None,
        extra_fields: dict | None = None,
        df: pd.DataFrame | None = None,
    ) -> Path:
        """
        Save comprehensive metadata for a dataset.

        Args:
            csv_path: Path to the CSV file
            symbol: Trading symbol (e.g., 'BTCUSDT')
            interval: Time interval (e.g., '1m', '1h')
            num_records: Number of records in the dataset
            script_version: Version of the script that generated the data
            data_source: Source of the data (e.g., 'binance', 'bybit')
            extra_fields: Additional metadata fields
            df: DataFrame for quality analysis (optional)

        Returns:
            Path to the metadata file
        """
        csv_path = Path(csv_path)

        # Calculate file hash and size
        file_hash = self.calculate_sha256(csv_path)
        file_size = csv_path.stat().st_size if csv_path.exists() else None

        # Analyze data quality if DataFrame provided
        quality_metrics = {}
        date_range = {}
        columns = None

        if df is not None:
            quality_metrics = self.analyze_data_quality(df)
            date_range = self.get_data_date_range(df)
            columns = df.columns.tolist()

        # Create metadata object
        metadata = DatasetMetadata(
            symbol=symbol,
            interval=interval,
            csv_file=csv_path.name,
            num_records=num_records,
            hash_sha256=file_hash,
            created_at=datetime.now(UTC).isoformat(),
            script_version=script_version,
            data_source=data_source,
            file_size_bytes=file_size,
            columns=columns,
            **quality_metrics,
            **date_range,
            extra_fields=extra_fields,
        )

        # Determine metadata file path
        base_name = csv_path.stem
        metadata_path = self.metadata_dir / f"{base_name}.json"

        # Save metadata
        try:
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata.to_dict(), f, indent=4, default=str)

            # Update index
            index_key = f"{symbol}_{interval}"
            self.metadata_index[index_key] = {
                "file": metadata_path.name,
                "symbol": symbol,
                "interval": interval,
                "created_at": metadata.created_at,
                "num_records": num_records,
            }
            self._save_metadata_index()

            self.logger.info(f"Metadata saved: {metadata_path}")
            return metadata_path

        except Exception as e:
            self.logger.error(f"Error saving metadata: {e}")
            raise

    def load_metadata(self, metadata_path: str | Path) -> dict | None:
        """Load metadata from file."""
        metadata_path = Path(metadata_path)

        try:
            with open(metadata_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading metadata from {metadata_path}: {e}")
            return None

    def find_metadata_by_symbol_interval(self, symbol: str, interval: str) -> dict | None:
        """Find metadata by symbol and interval."""
        index_key = f"{symbol}_{interval}"

        if index_key in self.metadata_index:
            metadata_file = self.metadata_dir / self.metadata_index[index_key]["file"]
            return self.load_metadata(metadata_file)

        return None

    def list_all_datasets(self) -> list[dict]:
        """List all tracked datasets."""
        datasets = []

        for _key, index_entry in self.metadata_index.items():
            metadata_file = self.metadata_dir / index_entry["file"]
            metadata = self.load_metadata(metadata_file)

            if metadata:
                datasets.append(metadata)

        return datasets

    def verify_file_integrity(self, csv_path: str | Path) -> bool:
        """Verify file integrity using stored hash."""
        csv_path = Path(csv_path)
        base_name = csv_path.stem
        metadata_path = self.metadata_dir / f"{base_name}.json"

        if not metadata_path.exists():
            self.logger.warning(f"No metadata found for {csv_path}")
            return False

        metadata = self.load_metadata(metadata_path)
        if not metadata:
            return False

        stored_hash = metadata.get("hash_sha256")
        current_hash = self.calculate_sha256(csv_path)

        if stored_hash == current_hash:
            self.logger.info(f"File integrity verified for {csv_path}")
            return True
        else:
            self.logger.error(f"File integrity check failed for {csv_path}")
            return False

    def get_metadata_summary(self) -> dict:
        """Get summary statistics of all metadata."""
        datasets = self.list_all_datasets()

        if not datasets:
            return {"total_datasets": 0}

        symbols = set()
        intervals = set()
        total_records = 0
        data_sources = set()

        for dataset in datasets:
            symbols.add(dataset.get("symbol"))
            intervals.add(dataset.get("interval"))
            total_records += dataset.get("num_records", 0)
            if dataset.get("data_source"):
                data_sources.add(dataset.get("data_source"))

        return {
            "total_datasets": len(datasets),
            "unique_symbols": len(symbols),
            "unique_intervals": len(intervals),
            "total_records": total_records,
            "symbols": sorted(symbols),
            "intervals": sorted(intervals),
            "data_sources": sorted(data_sources),
        }


# Legacy functions for backward compatibility
def calculate_sha256(file_path: str) -> str:
    """Legacy function for backward compatibility."""
    manager = EnhancedMetadataManager()
    return manager.calculate_sha256(file_path)


def save_metadata(
    csv_path: str,
    symbol: str,
    interval: str,
    num_records: int,
    script_version: str = None,
    extra_fields: dict = None,
):
    """Legacy function for backward compatibility."""
    manager = EnhancedMetadataManager()
    return manager.save_metadata(
        csv_path=csv_path,
        symbol=symbol,
        interval=interval,
        num_records=num_records,
        script_version=script_version,
        extra_fields=extra_fields,
    )


# Testing function
def test_metadata_manager():
    """Test the metadata manager functionality."""
    print("🧪 Testing Enhanced Metadata Manager...")

    # Create test data directory
    test_dir = Path("data/raw/test")
    test_dir.mkdir(parents=True, exist_ok=True)

    # Create test CSV file
    test_file = test_dir / "BTCUSDT_1m_test.csv"
    test_data = "timestamp,open,high,low,close,volume\n"
    test_data += "2024-01-01T00:00:00Z,50000,50100,49900,50050,1000\n"
    test_data += "2024-01-01T00:01:00Z,50050,50150,49950,50100,1100\n"
    test_file.write_text(test_data)

    # Test the enhanced manager
    manager = EnhancedMetadataManager()

    # Create test DataFrame for quality analysis
    df = pd.read_csv(test_file)

    # Save metadata with DataFrame analysis
    metadata_path = manager.save_metadata(
        csv_path=str(test_file),
        symbol="BTCUSDT",
        interval="1m",
        num_records=len(df),
        script_version="2.0.0",
        data_source="test",
        df=df,
    )

    print(f"✅ Metadata saved to: {metadata_path}")

    # Test metadata retrieval
    metadata = manager.find_metadata_by_symbol_interval("BTCUSDT", "1m")
    if metadata:
        print(f"✅ Metadata retrieved: {metadata['symbol']} {metadata['interval']}")

    # Test file integrity
    if manager.verify_file_integrity(test_file):
        print("✅ File integrity verified")

    # Test summary
    summary = manager.get_metadata_summary()
    print(f"✅ Summary: {summary}")

    # Test legacy functions
    legacy_path = save_metadata(
        csv_path=str(test_file),
        symbol="BTCUSDT",
        interval="1m",
        num_records=2,
        script_version="1.0.0",
    )
    print(f"✅ Legacy function works: {legacy_path}")

    print("🎉 All tests passed!")


if __name__ == "__main__":
    test_metadata_manager()
