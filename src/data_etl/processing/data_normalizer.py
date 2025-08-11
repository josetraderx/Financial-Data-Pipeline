"""
Data normalization module for preprocessing market data.
"""


import numpy as np
import pandas as pd


class DataNormalizer:
    """
    Handles data normalization and standardization for market data.

    Methods
    -------
    normalize_ohlcv(df, method):
        Normalize OHLCV data using the specified method ('zscore', 'minmax', 'log').
    """
    def __init__(self):
        """
        Initialize the DataNormalizer.
        """
        self.scalers: dict[str, dict[str, float | np.ndarray]] = {}

    def normalize_ohlcv(self, df: pd.DataFrame, method: str = 'zscore') -> pd.DataFrame:
        """
        Normalize OHLCV data using the specified method.

        Args:
            df (pd.DataFrame): OHLCV data with columns ['open', 'high', 'low', 'close', 'volume']
            method (str): Normalization method ('zscore', 'minmax', 'log')

        Returns:
            pd.DataFrame: Normalized OHLCV data

        Raises:
            ValueError: If the normalization method is not supported.
        """
        if method not in ['zscore', 'minmax', 'log']:
            raise ValueError(f"Unsupported normalization method: {method}")
        normalized_df = df.copy()
        price_columns = ['open', 'high', 'low', 'close']
        if method == 'zscore':
            for col in price_columns:
                mean = df[col].mean()
                std = df[col].std()
                normalized_df[col] = (df[col] - mean) / std
                self.scalers[col] = {'mean': mean, 'std': std}
            # Volume normalization
            vol_mean = df['volume'].mean()
            vol_std = df['volume'].std()
            normalized_df['volume'] = (df['volume'] - vol_mean) / vol_std
            self.scalers['volume'] = {'mean': vol_mean, 'std': vol_std}
        elif method == 'minmax':
            for col in price_columns + ['volume']:
                min_val = df[col].min()
                max_val = df[col].max()
                normalized_df[col] = (df[col] - min_val) / (max_val - min_val)
                self.scalers[col] = {'min': min_val, 'max': max_val}
        else:  # log normalization
            for col in price_columns + ['volume']:
                normalized_df[col] = np.log1p(df[col])
        return normalized_df

    def denormalize_ohlcv(self, df: pd.DataFrame, method: str = 'zscore') -> pd.DataFrame:
        """
        Denormalize data back to original scale.

        Args:
            df (pd.DataFrame): Normalized OHLCV data
            method (str): Normalization method used

        Returns:
            pd.DataFrame: Denormalized OHLCV data
        """
        if not self.scalers and method != 'log':
            raise ValueError("No scaling parameters found. Did you normalize the data first?")

        denormalized_df = df.copy()

        if method == 'zscore':
            for col in df.columns:
                if col in self.scalers:
                    mean = self.scalers[col]['mean']
                    std = self.scalers[col]['std']
                    denormalized_df[col] = (df[col] * std) + mean

        elif method == 'minmax':
            for col in df.columns:
                if col in self.scalers:
                    min_val = self.scalers[col]['min']
                    max_val = self.scalers[col]['max']
                    denormalized_df[col] = df[col] * (max_val - min_val) + min_val

        else:  # log denormalization
            for col in df.columns:
                denormalized_df[col] = np.expm1(df[col])

        return denormalized_df
