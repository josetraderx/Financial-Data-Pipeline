"""
Timeframe aggregation module for resampling market data.
"""

import pandas as pd


class TimeframeAggregator:
    """Handles timeframe aggregation and resampling of OHLCV data."""

    VALID_TIMEFRAMES = {
        "1m": "1T",
        "3m": "3T",
        "5m": "5T",
        "15m": "15T",
        "30m": "30T",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "6h": "6H",
        "8h": "8H",
        "12h": "12H",
        "1d": "1D",
        "3d": "3D",
        "1w": "1W",
        "1M": "1M",
    }

    @classmethod
    def validate_timeframe(cls, timeframe: str) -> str:
        """
        Validate and convert timeframe to pandas frequency string.

        Args:
            timeframe (str): Timeframe string (e.g., '1m', '1h', '1d')

        Returns:
            str: Pandas frequency string
        """
        if timeframe not in cls.VALID_TIMEFRAMES:
            raise ValueError(
                f"Invalid timeframe. Valid options are: {list(cls.VALID_TIMEFRAMES.keys())}"
            )
        return cls.VALID_TIMEFRAMES[timeframe]

    @staticmethod
    def aggregate_ohlcv(df: pd.DataFrame, freq: str) -> pd.DataFrame:
        """
        Aggregate OHLCV data to a higher timeframe.

        Args:
            df (pd.DataFrame): OHLCV data with columns ['open', 'high', 'low', 'close', 'volume']
            freq (str): Target frequency in pandas format

        Returns:
            pd.DataFrame: Aggregated OHLCV data
        """
        resampled = pd.DataFrame()

        # Ensure the index is datetime
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be DatetimeIndex")

        # Define aggregation functions for OHLCV
        agg_dict = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }

        # Perform resampling
        resampled = df.resample(freq).agg(agg_dict)

        # Forward fill missing values (if any)
        resampled = resampled.fillna(method="ffill")

        return resampled

    def resample_timeframe(
        self, df: pd.DataFrame, target_timeframe: str
    ) -> pd.DataFrame:
        """
        Resample OHLCV data to target timeframe.

        Args:
            df (pd.DataFrame): OHLCV data
            target_timeframe (str): Target timeframe (e.g., '1h', '4h', '1d')

        Returns:
            pd.DataFrame: Resampled OHLCV data
        """
        freq = self.validate_timeframe(target_timeframe)
        return self.aggregate_ohlcv(df, freq)

    def generate_multiple_timeframes(
        self, df: pd.DataFrame, timeframes: list
    ) -> dict[str, pd.DataFrame]:
        """
        Generate multiple timeframe versions of the input data.

        Args:
            df (pd.DataFrame): Base OHLCV data
            timeframes (list): List of target timeframes

        Returns:
            Dict[str, pd.DataFrame]: Dictionary of resampled dataframes
        """
        results = {}

        for tf in timeframes:
            results[tf] = self.resample_timeframe(df, tf)

        return results
