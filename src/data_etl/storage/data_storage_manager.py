"""
Data storage management module for Exodus v2025.
Handles data archiving, backup, and compression.
"""

import bz2
import hashlib
import json
import lzma
import shutil
import zlib
from datetime import datetime
from pathlib import Path

import pandas as pd


class DataStorageManager:
    """
    Unified manager for data storage, archiving, and backup operations.
    Combines functionality of previous DataArchiver, DataBackup, and DataCompressor.
    """

    COMPRESSION_METHODS = {
        "zlib": {"compress": zlib.compress, "decompress": zlib.decompress, "extension": ".zlib"},
        "lzma": {"compress": lzma.compress, "decompress": lzma.decompress, "extension": ".xz"},
        "bz2": {"compress": bz2.compress, "decompress": bz2.decompress, "extension": ".bz2"},
    }

    def __init__(
        self, base_dir: str | Path, compression_method: str = "zlib", compression_level: int = 6
    ):
        """
        Initialize storage manager.

        Args:
            base_dir: Base directory for all storage operations
            compression_method: Compression method to use
            compression_level: Compression level (0-9)
        """
        self.base_dir = Path(base_dir)
        self.archive_dir = self.base_dir / "archives"
        self.backup_dir = self.base_dir / "backups"

        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        if compression_method not in self.COMPRESSION_METHODS:
            raise ValueError(f"Unsupported compression method: {compression_method}")

        self.compression_method = compression_method
        self.compression_level = compression_level
        self.metadata_file = self.backup_dir / "storage_metadata.json"
        self.load_metadata()

    def load_metadata(self):
        """Load storage metadata from JSON file."""
        if self.metadata_file.exists():
            with open(self.metadata_file) as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {}

    def save_metadata(self):
        """Save storage metadata to JSON file."""
        with open(self.metadata_file, "w") as f:
            json.dump(self.metadata, f, indent=4)

    def calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA-256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def archive_data(
        self, df: pd.DataFrame, symbol: str, timeframe: str, compress: bool = True
    ) -> Path:
        """
        Archive market data with optional compression.

        Args:
            df: OHLCV data to archive
            symbol: Trading pair symbol
            timeframe: Data timeframe
            compress: Whether to compress the data

        Returns:
            Path to archived file
        """
        if df.empty:
            raise ValueError("Cannot archive empty DataFrame")

        # Extract date components from the first timestamp
        first_date = pd.to_datetime(df.index[0])
        year, month = first_date.year, first_date.month

        # Create archive directory structure
        archive_path = self.archive_dir / str(year) / f"{month:02d}" / symbol
        archive_path.mkdir(parents=True, exist_ok=True)

        # Create filename with metadata
        base_filename = f"{symbol}_{timeframe}_{year}{month:02d}"
        file_path = archive_path / f"{base_filename}.parquet"

        # Save to parquet
        df.to_parquet(file_path)

        if compress:
            compressed_path = file_path.with_suffix(
                self.COMPRESSION_METHODS[self.compression_method]["extension"]
            )
            self._compress_file(file_path, compressed_path)
            file_path.unlink()  # Remove uncompressed file
            file_path = compressed_path

        # Update metadata
        self.metadata[str(file_path)] = {
            "symbol": symbol,
            "timeframe": timeframe,
            "date": f"{year}-{month:02d}",
            "compressed": compress,
            "checksum": self.calculate_checksum(file_path),
            "rows": len(df),
            "size": file_path.stat().st_size,
        }
        self.save_metadata()

        return file_path

    def retrieve_data(
        self, symbol: str, start_date: str | datetime, end_date: str | datetime, timeframe: str
    ) -> pd.DataFrame:
        """
        Retrieve archived data.

        Args:
            symbol: Trading pair symbol
            start_date: Start date
            end_date: End date
            timeframe: Data timeframe

        Returns:
            Retrieved OHLCV data
        """
        import logging

        logger = logging.getLogger(__name__)

        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        logger.debug(f"Retrieving data for {symbol} from {start_date} to {end_date}")

        # Generate list of year/month combinations - ensure we cover partial months
        dates = pd.date_range(
            start_date.replace(day=1),
            end_date + pd.Timedelta(days=32),  # Add buffer to ensure we get the end month
            freq="MS",  # Month Start frequency
        )
        dfs = []

        for date in dates:
            year, month = date.year, date.month
            archive_path = self.archive_dir / str(year) / f"{month:02d}" / symbol

            if not archive_path.exists():
                logger.debug(f"No archive found for {year}-{month:02d}")
                continue

            # Check for both compressed and uncompressed files
            base_filename = f"{symbol}_{timeframe}_{year}{month:02d}"
            potential_files = list(archive_path.glob(f"{base_filename}.*"))

            if not potential_files:
                logger.debug(f"No matching files for {base_filename}")
                continue

            file_path = potential_files[0]
            logger.debug(f"Found file: {file_path}")

            try:
                # Try to determine if file is compressed by extension
                is_compressed = False
                compression_type = None
                for method, info in self.COMPRESSION_METHODS.items():
                    if file_path.suffix == info["extension"]:
                        is_compressed = True
                        compression_type = method
                        break

                if is_compressed:
                    # Use correct decompression method based on file extension
                    logger.debug(f"Decompressing file using {compression_type}")
                    temp_path = file_path.with_suffix(".parquet")
                    with open(file_path, "rb") as f_in:
                        compressed_data = f_in.read()
                    decompressed = self.COMPRESSION_METHODS[compression_type]["decompress"](
                        compressed_data
                    )
                    with open(temp_path, "wb") as f_out:
                        f_out.write(decompressed)
                    df = pd.read_parquet(temp_path)
                    temp_path.unlink()
                else:
                    logger.debug("Reading uncompressed parquet file")
                    df = pd.read_parquet(file_path)

                if not df.empty:
                    logger.debug(f"Loaded data shape: {df.shape}")
                    dfs.append(df)
                else:
                    logger.debug("Empty DataFrame loaded")

            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                continue

        if not dfs:
            logger.warning("No data found for the specified parameters")
            return pd.DataFrame()

        # Concatenate and filter by date range
        logger.debug("Concatenating DataFrames")
        result = pd.concat(dfs).sort_index()
        logger.debug(f"Combined data shape: {result.shape}")

        # Filter by date range
        mask = (result.index >= start_date) & (result.index <= end_date)
        result = result[mask]
        logger.debug(f"Final data shape after date filtering: {result.shape}")

        return result

    def create_backup(
        self, source_path: str | Path, description: str | None = None, compress: bool = True
    ) -> dict:
        """
        Create a backup with optional compression.

        Args:
            source_path: Path to file/directory to backup
            description: Backup description
            compress: Whether to compress the backup

        Returns:
            Backup metadata
        """
        source_path = Path(source_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Source path does not exist: {source_path}")

        # Create timestamp-based backup directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = self.backup_dir / timestamp

        # Create backup
        if source_path.is_file():
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, backup_path)

            if compress:
                compressed_path = backup_path.with_suffix(
                    self.COMPRESSION_METHODS[self.compression_method]["extension"]
                )
                self._compress_file(backup_path, compressed_path)
                backup_path.unlink()
                backup_path = compressed_path

            checksum = self.calculate_checksum(backup_path)
            is_directory = False
        else:
            shutil.copytree(source_path, backup_path)
            if compress:
                self._compress_directory(backup_path)
            checksum = None
            is_directory = True

        # Create metadata
        metadata = {
            "timestamp": timestamp,
            "source_path": str(source_path),
            "backup_path": str(backup_path),
            "description": description,
            "is_directory": is_directory,
            "compressed": compress,
            "checksum": checksum,
            "size": backup_path.stat().st_size,
        }

        # Update metadata storage
        self.metadata[timestamp] = metadata
        self.save_metadata()

        return metadata

    def restore_backup(self, timestamp: str, restore_path: str | Path | None = None) -> bool:
        """
        Restore from backup.

        Args:
            timestamp: Backup timestamp to restore
            restore_path: Custom restore location

        Returns:
            True if restore successful
        """
        if timestamp not in self.metadata:
            raise ValueError(f"No backup found for timestamp: {timestamp}")

        backup_info = self.metadata[timestamp]
        source = Path(backup_info["backup_path"])

        if not source.exists():
            raise FileNotFoundError(f"Backup files not found: {source}")

        # Determine restore location
        if restore_path:
            target = Path(restore_path)
        else:
            target = Path(backup_info["source_path"])

        # Handle compressed backups
        is_compressed = backup_info.get("compressed", False)
        if is_compressed and source.suffix in [
            ext["extension"] for ext in self.COMPRESSION_METHODS.values()
        ]:
            temp_path = source.with_suffix("")
            self._decompress_file(source, temp_path)
            source = temp_path

        # Perform restore
        if backup_info["is_directory"]:
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(source, target)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)

            # Verify checksum if available
            if backup_info["checksum"]:
                restored_checksum = self.calculate_checksum(target)
                if restored_checksum != backup_info["checksum"]:
                    raise ValueError("Checksum verification failed after restore")

        # Clean up temporary files
        if is_compressed and "temp_path" in locals():
            temp_path.unlink()

        return True

    def _compress_file(self, source: Path, target: Path) -> None:
        """Internal method to compress a file."""
        with open(source, "rb") as f_in:
            data = f_in.read()

        if self.compression_method == "zlib":
            compressed = self.COMPRESSION_METHODS[self.compression_method]["compress"](
                data, level=self.compression_level
            )
        else:
            compressed = self.COMPRESSION_METHODS[self.compression_method]["compress"](data)

        with open(target, "wb") as f_out:
            f_out.write(compressed)

    def _decompress_file(self, source: Path, target: Path) -> None:
        """Internal method to decompress a file."""
        with open(source, "rb") as f_in:
            compressed_data = f_in.read()

        decompressed = self.COMPRESSION_METHODS[self.compression_method]["decompress"](
            compressed_data
        )

        with open(target, "wb") as f_out:
            f_out.write(decompressed)

    def _compress_directory(self, dir_path: Path) -> None:
        """Internal method to compress all files in a directory."""
        for file_path in dir_path.rglob("*"):
            if file_path.is_file():
                compressed_path = file_path.with_suffix(
                    self.COMPRESSION_METHODS[self.compression_method]["extension"]
                )
                self._compress_file(file_path, compressed_path)
                file_path.unlink()

    def cleanup_old_backups(self, keep_days: int = 30, minimum_keep: int = 5) -> list[str]:
        """
        Remove old backups while keeping a minimum number.

        Args:
            keep_days: Number of days to keep backups
            minimum_keep: Minimum number of backups to keep

        Returns:
            List of removed backup timestamps
        """
        import time

        current_timestamp = time.time()
        removed = []

        # Sort backups by date
        sorted_backups = sorted(self.metadata.items(), key=lambda x: x[0], reverse=True)

        # Always keep the minimum number of backups
        backups_to_check = sorted_backups[minimum_keep:]

        for timestamp, metadata in backups_to_check:
            backup_time = time.mktime(datetime.strptime(timestamp, "%Y%m%d_%H%M%S").timetuple())
            days_old = (current_timestamp - backup_time) / (24 * 3600)

            if days_old > keep_days:
                backup_path = Path(metadata["backup_path"])

                # Remove backup files
                if backup_path.exists():
                    if metadata["is_directory"]:
                        shutil.rmtree(backup_path)
                    else:
                        backup_path.unlink()

                # Remove from metadata
                del self.metadata[timestamp]
                removed.append(timestamp)

        if removed:
            self.save_metadata()

        return removed
