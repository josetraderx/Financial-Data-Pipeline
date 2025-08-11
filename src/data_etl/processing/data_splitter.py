"""
Utility for splitting data into training, validation, and test sets for ML pipelines.
"""

import logging

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)


class DataSplitter:
    """
    Utility class for splitting data for training, validation, and testing.

    Methods
    -------
    train_test_split(df, test_size, method, ...):
        Split data into train and test sets with different methods.
    split_random(df, test_size, val_size, ...):
        Randomly split data into train, validation, and test sets.
    split_time_series(df, train_ratio, val_ratio, ...):
        Chronologically split time series data into train, validation, and test sets.
    split_by_time(df, train_end, val_end, ...):
        Split data using specific date cutoffs.
    create_sliding_windows(data, window_size, ...):
        Create sliding windows for time series data.
    """

    def train_test_split(
        self,
        df: pd.DataFrame,
        test_size: float = 0.2,
        method: str = "chronological",
        timestamp_col: str = "timestamp",
        random_state: int | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split data into train and test sets using specified method.

        Args:
            df (pd.DataFrame): DataFrame to split
            test_size (float): Proportion for test set
            method (str): Split method ('chronological' or 'random')
            timestamp_col (str): Name of timestamp column for chronological split
            random_state (Optional[int]): Random seed for reproducibility

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: (train, test)
        """
        try:
            if method == "chronological":
                # Sort by timestamp and split chronologically
                df_sorted = df.sort_values(timestamp_col).reset_index(drop=True)
                n = len(df_sorted)
                train_size = int(n * (1 - test_size))
                train = df_sorted.iloc[:train_size]
                test = df_sorted.iloc[train_size:]
                logger.info(f"Chronological split: {len(train)} train, {len(test)} test")
                return train, test

            elif method == "random":
                # Random split
                train, test = train_test_split(
                    df, test_size=test_size, random_state=random_state, shuffle=True
                )
                logger.info(f"Random split: {len(train)} train, {len(test)} test")
                return train, test

            else:
                logger.error(f"Unknown split method: {method}")
                return df, pd.DataFrame()

        except Exception as e:
            logger.error(f"train_test_split failed: {e}")
            return df, pd.DataFrame()

    def split_by_date(self, df: pd.DataFrame, split_date, timestamp_col: str = "timestamp") -> dict:
        """
        Split data by a specific date.

        Args:
            df (pd.DataFrame): DataFrame to split
            split_date: Date to split on
            timestamp_col (str): Name of timestamp column

        Returns:
            dict: Dictionary with 'before' and 'after' keys
        """
        try:
            if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
                df[timestamp_col] = pd.to_datetime(df[timestamp_col])

            before = df[df[timestamp_col] <= pd.to_datetime(split_date)]
            after = df[df[timestamp_col] > pd.to_datetime(split_date)]

            logger.info(f"Date split: {len(before)} before, {len(after)} after {split_date}")
            return {"before": before, "after": after}

        except Exception as e:
            logger.error(f"split_by_date failed: {e}")
            return {"before": df, "after": pd.DataFrame()}

    @staticmethod
    def split_random(
        df: pd.DataFrame,
        test_size: float = 0.2,
        val_size: float = 0.2,
        shuffle: bool = True,
        random_state: int | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Randomly split data into train, validation, and test sets.

        Args:
            df (pd.DataFrame): DataFrame to split.
            test_size (float): Proportion for test set.
            val_size (float): Proportion for validation set.
            shuffle (bool): Whether to shuffle before splitting.
            random_state (Optional[int]): Seed for reproducibility.
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: (train, val, test)
        """
        try:
            train_val, test = train_test_split(
                df, test_size=test_size, shuffle=shuffle, random_state=random_state
            )
            if val_size > 0:
                val_ratio = val_size / (1 - test_size)
                train, val = train_test_split(
                    train_val, test_size=val_ratio, shuffle=shuffle, random_state=random_state
                )
                if train.empty or val.empty or test.empty:
                    logger.warning("One of the splits (train/val/test) is empty in split_random.")
                return train, val, test
            if train_val.empty or test.empty:
                logger.warning("One of the splits (train/test) is empty in split_random.")
            return train_val, pd.DataFrame(), test
        except Exception as e:
            logger.error(f"split_random failed: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    @staticmethod
    def split_time_series(
        df: pd.DataFrame,
        train_ratio: float = 0.6,
        val_ratio: float = 0.2,
        timestamp_col: str = "timestamp",
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Chronologically split time series data into train, validation, and test sets.

        Args:
            df (pd.DataFrame): DataFrame with time series data.
            train_ratio (float): Proportion for training set.
            val_ratio (float): Proportion for validation set.
            timestamp_col (str): Name of the timestamp column.
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: (train, val, test)
        """
        try:
            df = df.sort_values(timestamp_col).reset_index(drop=True)
            n = len(df)
            train_idx = int(n * train_ratio)
            val_idx = int(n * (train_ratio + val_ratio))
            train = df.iloc[:train_idx]
            val = df.iloc[train_idx:val_idx]
            test = df.iloc[val_idx:]
            if train.empty or test.empty:
                logger.warning("Train or test split is empty in split_time_series.")
            return train, val, test
        except Exception as e:
            logger.error(f"split_time_series failed: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    @staticmethod
    def split_by_time(
        df: pd.DataFrame,
        train_end: str,
        val_end: str | None = None,
        timestamp_col: str = "timestamp",
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Split data using specific date cutoffs.

        Args:
            df (pd.DataFrame): DataFrame with time series data.
            train_end (str): End date for training set (inclusive).
            val_end (Optional[str]): End date for validation set (inclusive).
            timestamp_col (str): Name of the timestamp column.
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: (train, val, test)
        """
        try:
            if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
                df[timestamp_col] = pd.to_datetime(df[timestamp_col])
            train = df[df[timestamp_col] <= train_end]
            if val_end:
                val = df[(df[timestamp_col] > train_end) & (df[timestamp_col] <= val_end)]
                test = df[df[timestamp_col] > val_end]
            else:
                val = pd.DataFrame()
                test = df[df[timestamp_col] > train_end]
            if train.empty or (val_end and val.empty) or test.empty:
                logger.warning("One of the splits (train/val/test) is empty in split_by_time.")
            return train, val, test
        except Exception as e:
            logger.error(f"split_by_time failed: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    @staticmethod
    def create_sliding_windows(
        data: np.ndarray, window_size: int, target_size: int = 1, stride: int = 1
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Create sliding windows for time series data.

        Args:
            data (np.ndarray): Input array.
            window_size (int): Size of the input window.
            target_size (int): Size of the target window.
            stride (int): Step between windows.
        Returns:
            Tuple[np.ndarray, np.ndarray]: (X, y) of input and target arrays.
        """
        try:
            length = len(data)
            n_samples = ((length - window_size - target_size) // stride) + 1
            if n_samples <= 0:
                logger.warning("Not enough data to create sliding windows.")
                return np.array([]), np.array([])
            X = np.zeros((n_samples, window_size) + data.shape[1:])
            y = np.zeros((n_samples, target_size) + data.shape[1:])
            for i in range(n_samples):
                start_x = i * stride
                end_x = start_x + window_size
                start_y = end_x
                end_y = start_y + target_size
                X[i] = data[start_x:end_x]
                y[i] = data[start_y:end_y]
            return X, y
        except Exception as e:
            logger.error(f"create_sliding_windows failed: {e}")
            return np.array([]), np.array([])
