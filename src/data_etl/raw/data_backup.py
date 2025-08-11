"""
Data backup module for market data.
"""

import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path


class DataBackup:
    """Handles backup of market data and metadata."""

    def __init__(self, backup_dir: str | Path):
        """
        Initialize the DataBackup.

        Args:
            backup_dir (Union[str, Path]): Base directory for backups
        """
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_file = self.backup_dir / "backup_metadata.json"
        self.load_metadata()

    def load_metadata(self):
        """Load backup metadata from JSON file."""
        if self.metadata_file.exists():
            with open(self.metadata_file) as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {}

    def save_metadata(self):
        """Save backup metadata to JSON file."""
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=4)

    def calculate_checksum(self, file_path: Path) -> str:
        """
        Calculate SHA-256 checksum of a file.

        Args:
            file_path (Path): Path to the file

        Returns:
            str: Hexadecimal checksum
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def create_backup(
        self,
        source_path: str | Path,
        description: str | None = None
    ) -> dict:
        """
        Create a backup of a data file or directory.

        Args:
            source_path (Union[str, Path]): Path to file/directory to backup
            description (str, optional): Description of the backup

        Returns:
            Dict: Backup metadata
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
            checksum = self.calculate_checksum(backup_path)
            is_directory = False
        else:
            shutil.copytree(source_path, backup_path)
            checksum = None
            is_directory = True

        # Create metadata
        metadata = {
            'timestamp': timestamp,
            'source_path': str(source_path),
            'backup_path': str(backup_path),
            'description': description,
            'is_directory': is_directory,
            'checksum': checksum
        }

        # Update metadata storage
        self.metadata[timestamp] = metadata
        self.save_metadata()

        return metadata

    def restore_backup(
        self,
        timestamp: str,
        restore_path: str | Path | None = None
    ) -> bool:
        """
        Restore data from a backup.

        Args:
            timestamp (str): Backup timestamp to restore
            restore_path (Union[str, Path], optional): Custom restore location

        Returns:
            bool: True if restore successful
        """
        if timestamp not in self.metadata:
            raise ValueError(f"No backup found for timestamp: {timestamp}")

        backup_info = self.metadata[timestamp]
        source = Path(backup_info['backup_path'])

        if not source.exists():
            raise FileNotFoundError(f"Backup files not found: {source}")

        # Determine restore location
        if restore_path:
            target = Path(restore_path)
        else:
            target = Path(backup_info['source_path'])

        # Perform restore
        if backup_info['is_directory']:
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(source, target)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)

            # Verify checksum if available
            if backup_info['checksum']:
                restored_checksum = self.calculate_checksum(target)
                if restored_checksum != backup_info['checksum']:
                    raise ValueError("Checksum verification failed after restore")

        return True

    def list_backups(
        self,
        start_date: str | None = None,
        end_date: str | None = None
    ) -> list[dict]:
        """
        List available backups with optional date filtering.

        Args:
            start_date (str, optional): Start date for filtering (YYYY-MM-DD)
            end_date (str, optional): End date for filtering (YYYY-MM-DD)

        Returns:
            List[Dict]: List of backup metadata
        """
        backups = []

        for timestamp, metadata in self.metadata.items():
            if start_date or end_date:
                backup_date = datetime.strptime(timestamp, "%Y%m%d_%H%M%S")

                if start_date and backup_date < datetime.strptime(start_date, "%Y-%m-%d"):
                    continue
                if end_date and backup_date > datetime.strptime(end_date, "%Y-%m-%d"):
                    continue

            backups.append(metadata)

        return sorted(backups, key=lambda x: x['timestamp'], reverse=True)

    def verify_backup(self, timestamp: str) -> bool:
        """
        Verify the integrity of a backup.

        Args:
            timestamp (str): Backup timestamp to verify

        Returns:
            bool: True if backup is valid
        """
        if timestamp not in self.metadata:
            raise ValueError(f"No backup found for timestamp: {timestamp}")

        backup_info = self.metadata[timestamp]
        backup_path = Path(backup_info['backup_path'])

        if not backup_path.exists():
            return False

        if not backup_info['is_directory'] and backup_info['checksum']:
            current_checksum = self.calculate_checksum(backup_path)
            return current_checksum == backup_info['checksum']

        return True

    def cleanup_old_backups(
        self,
        keep_days: int = 30,
        minimum_keep: int = 5
    ) -> list[str]:
        """
        Remove old backups while keeping a minimum number.

        Args:
            keep_days (int): Number of days to keep backups
            minimum_keep (int): Minimum number of backups to keep

        Returns:
            List[str]: List of removed backup timestamps
        """
        current_date = datetime.now()
        removed = []

        # Sort backups by date
        sorted_backups = sorted(
            self.metadata.items(),
            key=lambda x: x[0],
            reverse=True
        )

        # Always keep the minimum number of backups
        backups_to_check = sorted_backups[minimum_keep:]

        for timestamp, metadata in backups_to_check:
            backup_date = datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
            days_old = (current_date - backup_date).days

            if days_old > keep_days:
                backup_path = Path(metadata['backup_path'])

                # Remove backup files
                if backup_path.exists():
                    if metadata['is_directory']:
                        shutil.rmtree(backup_path)
                    else:
                        backup_path.unlink()

                # Remove from metadata
                del self.metadata[timestamp]
                removed.append(timestamp)

        if removed:
            self.save_metadata()

        return removed
