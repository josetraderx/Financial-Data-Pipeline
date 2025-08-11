"""
Data archiving module for managing historical market data.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd


class DataArchiver:
    """Handles archiving of historical market data."""

    def __init__(self, archive_dir: str | Path):
        """
        Initialize the DataArchiver.

        Args:
            archive_dir (Union[str, Path]): Base directory for archives
        """
        self.archive_dir = Path(archive_dir)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def create_archive_structure(self, symbol: str, year: int, month: int) -> Path:
        """
        Create directory structure for archived data.

        Args:
            symbol (str): Trading pair symbol
            year (int): Year of the data
            month (int): Month of the data

        Returns:
            Path: Path to the archive directory
        """
        archive_path = self.archive_dir / str(year) / f"{month:02d}" / symbol
        archive_path.mkdir(parents=True, exist_ok=True)
        return archive_path

    def archive_data(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        compression: str | None = "gzip",
    ) -> Path:
        """
        Archive market data to compressed parquet files.

        Args:
            df (pd.DataFrame): OHLCV data to archive
            symbol (str): Trading pair symbol
            timeframe (str): Data timeframe
            compression (str, optional): Compression method for parquet

        Returns:
            Path: Path to the archived file
        """
        if df.empty:
            raise ValueError("Cannot archive empty DataFrame")

        # Extract date components from the first timestamp
        first_date = pd.to_datetime(df.index[0])
        year, month = first_date.year, first_date.month

        # Create archive directory structure
        archive_path = self.create_archive_structure(symbol, year, month)

        # Create filename with metadata
        filename = f"{symbol}_{timeframe}_{year}{month:02d}.parquet"
        file_path = archive_path / filename

        # Save to parquet with compression
        df.to_parquet(file_path, compression=compression)

        return file_path

    def retrieve_archived_data(
        self,
        symbol: str,
        start_date: str | datetime,
        end_date: str | datetime,
        timeframe: str,
    ) -> pd.DataFrame:
        """
        Retrieve data from archives.

        Args:
            symbol (str): Trading pair symbol
            start_date (Union[str, datetime]): Start date
            end_date (Union[str, datetime]): End date
            timeframe (str): Data timeframe

        Returns:
            pd.DataFrame: Retrieved OHLCV data
        """
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        # Generate list of year/month combinations to check
        dates = pd.date_range(start_date, end_date, freq="M")
        dfs = []

        for date in dates:
            year, month = date.year, date.month
            archive_path = self.archive_dir / str(year) / f"{month:02d}" / symbol

            if not archive_path.exists():
                continue

            filename = f"{symbol}_{timeframe}_{year}{month:02d}.parquet"
            file_path = archive_path / filename

            if file_path.exists():
                df = pd.read_parquet(file_path)
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        # Concatenate all dataframes and filter by date range
        result = pd.concat(dfs).sort_index()
        mask = (result.index >= start_date) & (result.index <= end_date)
        return result[mask]

    def list_available_archives(
        self, symbol: str | None = None, year: int | None = None
    ) -> list[Path]:
        """
        List available archived files.

        Args:
            symbol (str, optional): Filter by symbol
            year (int, optional): Filter by year

        Returns:
            List[Path]: List of archive file paths
        """
        if year:
            base_path = self.archive_dir / str(year)
        else:
            base_path = self.archive_dir

        if not base_path.exists():
            return []

        files = []
        for path in base_path.rglob("*.parquet"):
            if symbol and symbol not in path.name:
                continue
            files.append(path)

        return sorted(files)
