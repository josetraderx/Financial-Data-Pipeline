"""
Tests for data storage management module.
"""

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from data_etl.storage.data_storage_manager import DataStorageManager


@pytest.fixture
def test_dir(tmp_path):
    """Create temporary directory for testing."""
    return tmp_path


@pytest.fixture
def storage_manager(test_dir):
    """Create DataStorageManager instance for testing."""
    return DataStorageManager(test_dir)


@pytest.fixture
def sample_data():
    """Create sample OHLCV data for testing."""
    dates = pd.date_range(start="2025-01-01", end="2025-01-10", freq="1min")
    data = {
        "open": np.random.uniform(30000, 31000, len(dates)),
        "high": np.random.uniform(31000, 32000, len(dates)),
        "low": np.random.uniform(29000, 30000, len(dates)),
        "close": np.random.uniform(30000, 31000, len(dates)),
        "volume": np.random.uniform(1, 100, len(dates)),
    }
    return pd.DataFrame(data, index=dates)


@pytest.fixture
def sample_file(test_dir):
    """Create a sample file for testing backups."""
    file_path = test_dir / "test_file.txt"
    with open(file_path, "w") as f:
        f.write("Test content")
    return file_path


class TestDataStorageManager:
    """Test suite for DataStorageManager."""

    def test_archive_data(self, storage_manager, sample_data):
        """Test archiving OHLCV data."""
        archived_path = storage_manager.archive_data(
            sample_data, symbol="BTCUSDT", timeframe="1m", compress=True
        )

        assert archived_path.exists()
        assert archived_path.suffix in [".zlib", ".xz", ".bz2"]

        # Verify metadata was created
        assert str(archived_path) in storage_manager.metadata
        assert storage_manager.metadata[str(archived_path)]["symbol"] == "BTCUSDT"

    def test_retrieve_data(self, storage_manager, sample_data):
        """Test retrieving archived data."""
        import logging

        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)

        # Ensure sample data has the right index
        if not isinstance(sample_data.index, pd.DatetimeIndex):
            sample_data.index = pd.to_datetime(sample_data.index)

        logger.debug("Sample data info:")
        logger.debug(f"Shape: {sample_data.shape}")
        logger.debug(f"Index: {sample_data.index.dtype}")
        logger.debug(
            f"Date range: {sample_data.index.min()} to {sample_data.index.max()}"
        )
        logger.debug(f"Columns: {sample_data.columns.tolist()}")

        # Archive data first with compression
        archived_path = storage_manager.archive_data(
            sample_data.copy(),  # Use a copy to prevent modifications
            symbol="BTCUSDT",
            timeframe="1m",
            compress=True,
        )

        logger.debug(f"Archived data at: {archived_path}")
        assert archived_path.exists(), f"Archive file does not exist at {archived_path}"

        # Verify archive structure
        archive_dir = archived_path.parent
        logger.debug(f"Archive directory contents: {list(archive_dir.glob('*'))}")

        # Verify archive file size is reasonable
        file_size = archived_path.stat().st_size
        logger.debug(f"Archive file size: {file_size} bytes")
        assert file_size > 0, "Archive file is empty"

        # Retrieve data using the actual date range from sample_data
        start_date = sample_data.index.min()
        end_date = sample_data.index.max()

        logger.debug(f"Retrieving data for date range: {start_date} to {end_date}")

        # Retrieve the data
        retrieved = storage_manager.retrieve_data(
            symbol="BTCUSDT", start_date=start_date, end_date=end_date, timeframe="1m"
        )

        # Verify the data was retrieved
        assert not retrieved.empty, "Retrieved data is empty"

        logger.debug("Retrieved data info:")
        logger.debug(f"Shape: {retrieved.shape}")
        logger.debug(f"Index: {retrieved.index.dtype}")
        logger.debug(f"Date range: {retrieved.index.min()} to {retrieved.index.max()}")
        logger.debug(f"Columns: {retrieved.columns.tolist()}")

        assert (
            retrieved.shape == sample_data.shape
        ), f"Shape mismatch: expected {sample_data.shape}, got {retrieved.shape}"

        # Compare data
        try:
            # Reset index to make comparison easier
            sample_reset = sample_data.reset_index(drop=True)
            retrieved_reset = retrieved.reset_index(drop=True)

            # Log some sample values
            logger.debug("\nFirst few rows comparison:")
            logger.debug(f"Sample data:\n{sample_reset.head()}")
            logger.debug(f"Retrieved data:\n{retrieved_reset.head()}")

            pd.testing.assert_frame_equal(
                sample_reset,
                retrieved_reset,
                check_exact=False,
                check_dtype=False,
                check_index_type=False,
                atol=1e-5,  # Allow small numerical differences
            )
            logger.debug("Data comparison successful")

        except AssertionError as e:
            logger.error(f"Data comparison failed: {str(e)}")
            # Log detailed differences
            for col in sample_reset.columns:
                if not (sample_reset[col] == retrieved_reset[col]).all():
                    logger.error(f"\nDifferences in column {col}:")
                    logger.error(f"Sample values: {sample_reset[col].head()}")
                    logger.error(f"Retrieved values: {retrieved_reset[col].head()}")
            raise

    def test_create_backup(self, storage_manager, sample_file):
        """Test creating a backup."""
        backup_info = storage_manager.create_backup(
            sample_file, description="Test backup", compress=True
        )

        assert Path(backup_info["backup_path"]).exists()
        assert backup_info["compressed"]
        assert "checksum" in backup_info

    def test_restore_backup(self, storage_manager, sample_file, test_dir):
        """Test restoring from backup."""
        # Get original content before backup
        original_content = sample_file.read_text()

        # Create backup with compression disabled for this test
        backup_info = storage_manager.create_backup(sample_file, compress=False)

        # Delete original file
        sample_file.unlink()

        # Restore backup
        restore_path = test_dir / "restored_file.txt"
        success = storage_manager.restore_backup(backup_info["timestamp"], restore_path)

        assert success
        assert restore_path.exists()
        restored_content = restore_path.read_text()
        assert (
            restored_content == original_content
        ), f"Expected '{original_content}', got '{restored_content}'"

    def test_cleanup_old_backups(self, storage_manager, sample_file, monkeypatch):
        """Test cleaning up old backups."""
        import time

        # Mock time.time() to return a fixed timestamp
        FIXED_TIME = time.mktime(datetime(2025, 7, 7).timetuple())
        monkeypatch.setattr(time, "time", lambda: FIXED_TIME)

        # Create multiple backups with old timestamps
        timestamps = []
        base_date = datetime(2025, 6, 1)  # Create backups from June 1st

        for i in range(10):
            backup_date = base_date + timedelta(days=i)
            backup_timestamp = backup_date.strftime("%Y%m%d_%H%M%S")

            backup_info = storage_manager.create_backup(
                sample_file, description=f"Backup {i}"
            )

            # Manually update the timestamp in metadata to simulate old backups
            old_path = Path(
                storage_manager.metadata[backup_info["timestamp"]]["backup_path"]
            )
            new_path = old_path.parent / f"{backup_timestamp}{old_path.suffix}"
            old_path.rename(new_path)

            storage_manager.metadata[backup_timestamp] = storage_manager.metadata.pop(
                backup_info["timestamp"]
            )
            storage_manager.metadata[backup_timestamp]["timestamp"] = backup_timestamp
            storage_manager.metadata[backup_timestamp]["backup_path"] = str(new_path)
            storage_manager.save_metadata()

            timestamps.append(backup_timestamp)

        # Cleanup backups older than 15 days, keeping at least 5
        removed = storage_manager.cleanup_old_backups(
            keep_days=15,  # Remove backups older than 15 days
            minimum_keep=5,
        )

        # Should have removed some old backups
        assert len(removed) > 0, "No backups were removed"
        assert (
            len(storage_manager.metadata) >= 5
        ), f"Too few backups kept: {len(storage_manager.metadata)}"

    def test_compression_methods(self, storage_manager, sample_data):
        """Test different compression methods."""
        for method in ["zlib", "lzma", "bz2"]:
            storage_manager.compression_method = method
