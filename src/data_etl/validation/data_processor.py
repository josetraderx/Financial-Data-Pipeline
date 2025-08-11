"""
Enhanced data validation and cleaning module for Exodus v2025.
Combines validation and cleaning functionalities.
"""

import logging

import numpy as np
import pandas as pd

from data_etl.utils.logging import get_logger


class DataProcessor:
    """
    Handles data validation, cleaning and preprocessing for market data.
    Combines functionality from previous DataValidator and DataCleaner classes.
    """

    def __init__(self, logger: logging.Logger | None = None):
        """
        Initialize data processor.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger(__name__)

    def validate_ohlcv_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and clean OHLCV data.

        Args:
            df (pd.DataFrame): DataFrame with OHLCV data

        Returns:
            pd.DataFrame: Validated and cleaned data

        Raises:
            ValueError: If data fails validation
        """
        if df.empty:
            raise ValueError("Empty DataFrame provided")

        required_columns = ['open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"Missing required columns. Expected: {required_columns}")

        # Ensure numeric types
        for col in required_columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                try:
                    df[col] = pd.to_numeric(df[col])
                except ValueError as e:
                    self.logger.error(f"Non-numeric values in {col}: {e}")
                    raise ValueError(f"Non-numeric values in {col}")

        # Validate price relationships
        invalid_rows = (
            (df['high'] < df['low']) |
            (df['high'] < df['open']) |
            (df['high'] < df['close']) |
            (df['low'] > df['open']) |
            (df['low'] > df['close']) |
            (df['volume'] < 0)
        )

        if invalid_rows.any():
            self.logger.warning(f"Found {invalid_rows.sum()} rows with invalid price relationships")
            df = df[~invalid_rows].copy()

        # Handle missing values
        if df.isnull().any().any():
            self.logger.warning("Found missing values, forward filling...")
            df = df.ffill()

        # Ensure index is sorted
        if not df.index.is_monotonic_increasing:
            self.logger.warning("Index not sorted, sorting...")
            df = df.sort_index()

        return df

    def clean_outliers(
        self,
        df: pd.DataFrame,
        threshold: float = 3.0,
        method: str = 'zscore'
    ) -> pd.DataFrame:
        """
        Clean outliers from OHLCV data.

        Args:
            df (pd.DataFrame): OHLCV data
            threshold (float): Threshold for outlier detection
            method (str): Method to use ('zscore' or 'iqr')

        Returns:
            pd.DataFrame: Data with outliers removed or replaced
        """
        df = df.copy()
        price_columns = ['open', 'high', 'low', 'close']

        for col in price_columns + ['volume']:
            if method == 'zscore':
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                outliers = z_scores > threshold
            else:  # IQR method
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                outliers = (df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))

            if outliers.any():
                self.logger.warning(f"Found {outliers.sum()} outliers in {col}")
                if col in price_columns:
                    # For prices, use forward fill
                    df.loc[outliers, col] = np.nan
                    df[col] = df[col].ffill()
                else:
                    # For volume, use median
                    df.loc[outliers, col] = df[col].median()

        return df

    def validate_and_clean(
        self,
        df: pd.DataFrame,
        clean_outliers: bool = True,
        outlier_threshold: float = 3.0
    ) -> pd.DataFrame:
        """
        Complete validation and cleaning pipelines.

        Args:
            df (pd.DataFrame): Raw OHLCV data
            clean_outliers (bool): Whether to clean outliers
            outlier_threshold (float): Threshold for outlier detection

        Returns:
            pd.DataFrame: Validated and cleaned data
        """
        # First validate basic structure and relationships
        df = self.validate_ohlcv_data(df)

        # Then clean outliers if requested
        if clean_outliers:
            df = self.clean_outliers(df, threshold=outlier_threshold)

        return df
