"""
Resilient data sources with enhanced capabilities for Exodus v2025.
Integrates with the existing architecture for crypto trading data.
"""

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import sleep

import numpy as np
import pandas as pd
import requests

from data_etl.processing.enhanced_metadata_manager import EnhancedMetadataManager
from data_etl.utils.config import get_config

# Exodus v2025 imports
from data_etl.utils.logging import get_logger


@dataclass
class DataSourceError:
    """Track data source errors for monitoring."""
    error_type: str
    timestamp: datetime
    url: str
    message: str
    retry_count: int


class EnhancedDataValidator:
    """
    Advanced trading data validator and preprocessor for Exodus v2025.
    Includes robust cleaning, validation, and feature engineering for OHLCV data.
    """
    def __init__(self, logger_instance: logging.Logger | None = None):
        self.logger = logger_instance or get_logger(__name__)
        self.error_stats = {
            "validation_errors": 0,
            "processing_errors": 0,
            "recovery_attempts": 0,
            "uncorrectable_issues": 0,
            "invalid_rows_removed": 0,
            "total_rows": 0,
            "clean_rows": 0,
        }
        self.validation_results = {}
        self.numeric_columns = ["open", "high", "low", "close", "volume"]
        self.required_columns = ["timestamp"] + self.numeric_columns

    def validate_and_clean(
        self,
        df: pd.DataFrame,
        timestamp_format: str = "s",
        impute_missing: bool = True,
        outlier_factor: float = 1.5,
        price_tolerance: float = 0.05,
        add_time_features: bool = True,
        extreme_change_threshold: float = 0.2,
    ) -> tuple[pd.DataFrame, dict]:
        """
        Enhanced data validation and cleaning for OHLCV DataFrames.
        Args:
            df: Raw DataFrame.
            timestamp_format: Format of timestamp column (default: seconds).
            impute_missing: Whether to impute missing values.
            outlier_factor: IQR factor for outlier detection.
            price_tolerance: Tolerance for OHLC price validation (default 5%).
            add_time_features: Whether to add time-based features.
            extreme_change_threshold: Threshold for extreme price changes (default: 20%).
        Returns:
            (cleaned DataFrame, validation report dict)
        """
        import traceback
        try:
            report = {}
            if df is None or df.empty:
                self.logger.warning(f"Input DataFrame is empty or None. df: {df}")
                return df, report
            self.logger.info(f"Initial DataFrame shape: {df.shape}, columns: {list(df.columns)}")
            self.error_stats["total_rows"] = len(df)
            self.error_stats["clean_rows"] = len(df)
            # Lowercase columns
            df.columns = df.columns.str.lower()
            self.logger.info(f"Columns after lowercasing: {list(df.columns)}")
            # Check required columns
            missing_cols = [col for col in self.required_columns if col not in df.columns]
            if missing_cols:
                self.logger.error(f"Missing required columns: {missing_cols}")
                report["missing_cols"] = missing_cols
                self.logger.error(f"DataFrame head: {df.head()}")
                raise ValueError(f"Missing required columns: {missing_cols}")
            report["missing_cols"] = missing_cols
            # Remove duplicates
            initial_len = len(df)
            df = df.drop_duplicates(subset=["timestamp"])
            self.logger.info(f"Duplicates removed: {initial_len - len(df)}. Shape now: {df.shape}")
            report["duplicates_removed"] = initial_len - len(df)
            self.error_stats["invalid_rows_removed"] += report["duplicates_removed"]
            self.error_stats["clean_rows"] = len(df)
            # Convert timestamps
            df = self._convert_timestamps(df, timestamp_format)
            self.logger.info(f"After timestamp conversion: {df.shape}")
            # Convert numeric columns
            for col in self.numeric_columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            self.logger.info(f"After numeric conversion: {df.shape}")
            report["initial_nans"] = int(df.isnull().sum().sum())
            # Handle missing values
            df = self._handle_missing_values(df, impute_missing)
            self.logger.info(f"After handling missing values: {df.shape}")
            # Remove negative values
            df = self._check_negative_values(df)
            self.logger.info(f"After removing negatives: {df.shape}")
            # Handle outliers
            df, outlier_report = self._handle_outliers(df, outlier_factor)
            self.logger.info(f"After outlier handling: {df.shape}")
            report.update(outlier_report)
            # Validate OHLC price consistency
            df, price_report = self._validate_price_consistency(df, price_tolerance, impute_missing)
            self.logger.info(f"After price consistency: {df.shape}")
            report.update(price_report)
            # Check time intervals/gaps
            gap_report = self._check_time_intervals(df)
            report.update(gap_report)
            # Add time features
            if add_time_features:
                df = self._generate_time_features(df)
                report["time_features_added"] = True
                self.logger.info(f"After adding time features: {df.shape}")
            else:
                report["time_features_added"] = False
            # Timestamp validation (private)
            ts_report = self._validate_timestamps(df, timestamp_format)
            report.update(ts_report)
            # Zero price check
            zero_price_report = self._check_zero_prices(df)
            report.update(zero_price_report)
            # Extreme price change check
            extreme_change_report = self._check_extreme_price_changes(df, threshold=extreme_change_threshold)
            report.update(extreme_change_report)
            # Zero volume check
            zero_vol_report = self._check_zero_volume(df)
            report.update(zero_vol_report)
            report["final_rows"] = len(df)
            report["nans_after"] = int(df.isnull().sum().sum())
            # Agregar campos requeridos por el pipeline
            report["valid_records"] = len(df)
            report["total_records"] = self.error_stats["total_rows"]
            report["is_valid"] = len(df) > 0
            self.logger.info(f"Final DataFrame shape: {df.shape}")
            return df.reset_index(drop=True), report
        except Exception as e:
            self.logger.error(f"Exception in validate_and_clean: {e}")
            self.logger.error(f"Type of df: {type(df)}")
            self.logger.error(traceback.format_exc())
            raise

    def _convert_timestamps(self, df: pd.DataFrame, timestamp_format: str) -> pd.DataFrame:
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            if df["timestamp"].dtype.name in ["int64", "float64"]:
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit=timestamp_format)
            else:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            self.logger.info("Timestamp column converted to datetime")
        # Ensure UTC
        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")
        return df

    def _handle_missing_values(self, df: pd.DataFrame, impute: bool) -> pd.DataFrame:
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            self.logger.warning(f"Null values found: {null_counts[null_counts > 0].to_dict()}")
            if impute:
                for col in self.numeric_columns:
                    if col in df.columns:
                        df[col] = df[col].fillna(method="ffill").fillna(method="bfill")
                self.logger.info("Null values imputed")
            else:
                df = df.dropna()
                self.logger.info("Rows with null values dropped")
        return df

    def _check_negative_values(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.numeric_columns:
            if col in df.columns:
                neg_count = (df[col] < 0).sum()
                if neg_count > 0:
                    self.logger.warning(f"{neg_count} negative values found in {col}")
                    df = df[df[col] >= 0]
                    self.error_stats["invalid_rows_removed"] += neg_count
                    self.error_stats["clean_rows"] = len(df)
        return df

    def _handle_outliers(self, df: pd.DataFrame, factor: float) -> tuple[pd.DataFrame, dict]:
        outlier_report = {"outliers_detected": {}, "outliers_fixed": 0}
        for col in self.numeric_columns:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower = Q1 - factor * IQR
            upper = Q3 + factor * IQR
            outliers = (df[col] < lower) | (df[col] > upper)
            count = int(outliers.sum())
            if count > 0:
                outlier_report["outliers_detected"][col] = count
                outlier_report["outliers_fixed"] += count
                df.loc[outliers, col] = df.loc[outliers, col].clip(lower=lower, upper=upper)
        return df, outlier_report

    def _validate_price_consistency(self, df: pd.DataFrame, tolerance: float, impute_missing: bool) -> tuple[pd.DataFrame, dict]:
        price_report = {"invalid_ohlc": 0}
        inconsistent = (
            (df["high"] < df["low"] * (1 - tolerance)) |
            (df["high"] < df["open"] * (1 - tolerance)) |
            (df["high"] < df["close"] * (1 - tolerance)) |
            (df["low"] > df["open"] * (1 + tolerance)) |
            (df["low"] > df["close"] * (1 + tolerance))
        )
        count = int(inconsistent.sum())
        price_report["invalid_ohlc"] = count
        if count > 0:
            self.logger.warning(f"{count} rows with inconsistent OHLC prices (tolerance: {tolerance*100}%)")
            if impute_missing:
                affected = df.loc[inconsistent].copy()
                df.loc[inconsistent, "high"] = affected[["open", "close", "high"]].max(axis=1)
                df.loc[inconsistent, "low"] = affected[["open", "close", "low"]].min(axis=1)
                self.logger.info(f"Fixed price inconsistencies in {count} rows")
            else:
                df = df[~inconsistent]
                self.error_stats["invalid_rows_removed"] += count
                self.error_stats["clean_rows"] = len(df)
                self.logger.info(f"Removed {count} rows with inconsistent prices")
        return df, price_report

    def _check_time_intervals(self, df: pd.DataFrame) -> dict:
        report = {"time_gaps": 0}
        if len(df) > 1:
            df = df.sort_values("timestamp").reset_index(drop=True)
            df["interval"] = df["timestamp"].diff()
            median_interval = df["interval"].median()
            large_gaps = df["interval"] > (6 * median_interval)
            gap_count = int(large_gaps.sum())
            if gap_count > 0:
                self.logger.warning(f"{gap_count} large time gaps detected in timestamps")
            report["time_gaps"] = gap_count
            df.drop("interval", axis=1, inplace=True)
        return report

    def _generate_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df["hour"] = df["timestamp"].dt.hour
        df["day_of_week"] = df["timestamp"].dt.dayofweek
        df["month"] = df["timestamp"].dt.month
        df["year"] = df["timestamp"].dt.year
        return df

    def _validate_timestamps(self, df: pd.DataFrame, timestamp_format: str = "s") -> dict:
        """
        Private method to validate timestamps: checks for duplicates, sorts, detects gaps, and reports interval regularity.
        Args:
            df: DataFrame with a 'timestamp' column.
            timestamp_format: Format of the timestamp ('s' for seconds, 'ms' for milliseconds).
        Returns:
            dict: Report with duplicate count, most common interval, and irregular intervals count.
        """
        report = {}
        if "timestamp" not in df.columns:
            self.logger.error("No 'timestamp' column found in data.")
            report["missing_timestamp_col"] = True
            return report
        # Convert timestamp to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit=timestamp_format)
        # Sort by timestamp
        df = df.sort_values("timestamp").reset_index(drop=True)
        # Check for duplicates
        duplicates = df.duplicated(subset=["timestamp"], keep=False)
        dup_count = int(duplicates.sum())
        if dup_count > 0:
            self.logger.warning(f"Found {dup_count} duplicated timestamps.")
            df = df.drop_duplicates(subset=["timestamp"], keep="first")
            self.logger.info("Duplicated timestamps removed.")
        report["duplicated_timestamps"] = dup_count
        # Check intervals
        df["interval"] = df["timestamp"].diff().dt.total_seconds()
        intervals = df["interval"].dropna().values
        if len(intervals) > 0:
            unique_intervals, counts = np.unique(intervals, return_counts=True)
            most_common_interval = unique_intervals[np.argmax(counts)]
            report["most_common_interval_seconds"] = most_common_interval
            # Count irregular intervals
            missing_intervals = intervals != most_common_interval
            missing_count = int(missing_intervals.sum())
            if missing_count > 0:
                self.logger.warning(f"Found {missing_count} irregular timestamp intervals.")
            report["irregular_intervals"] = missing_count
        # Drop interval column
        if "interval" in df.columns:
            df.drop("interval", axis=1, inplace=True)
        return report

    def _check_zero_prices(self, df: pd.DataFrame) -> dict:
        """
        Check for rows where open, high, low, or close are exactly zero.
        Returns a report with the count and indices.
        """
        zero_mask = (df[["open", "high", "low", "close"]] == 0).any(axis=1)
        zero_count = int(zero_mask.sum())
        if zero_count > 0:
            self.logger.warning(f"Found {zero_count} rows with zero prices.")
        return {"zero_price_rows": zero_count, "zero_price_indices": df.index[zero_mask].tolist() if zero_count > 0 else []}

    def _check_extreme_price_changes(self, df: pd.DataFrame, threshold: float = 0.2) -> dict:
        """
        Check for extreme price changes between consecutive closes (default threshold: 20%).
        Returns a report with the count and indices.
        """
        if "close" not in df.columns:
            return {"extreme_price_changes": 0, "extreme_change_indices": []}
        price_changes = df["close"].pct_change().abs()
        extreme_mask = price_changes > threshold
        extreme_count = int(extreme_mask.sum())
        if extreme_count > 0:
            self.logger.warning(f"Found {extreme_count} rows with extreme price changes (>{threshold*100:.0f}%).")
        return {"extreme_price_changes": extreme_count, "extreme_change_indices": df.index[extreme_mask].tolist() if extreme_count > 0 else []}

    def _check_zero_volume(self, df: pd.DataFrame) -> dict:
        """
        Check for rows where volume is exactly zero.
        Returns a report with the count and indices.
        """
        if "volume" not in df.columns:
            return {"zero_volume_rows": 0, "zero_volume_indices": []}
        zero_vol_mask = df["volume"] == 0
        zero_vol_count = int(zero_vol_mask.sum())
        if zero_vol_count > 0:
            self.logger.warning(f"Found {zero_vol_count} rows with zero volume.")
        return {"zero_volume_rows": zero_vol_count, "zero_volume_indices": df.index[zero_vol_mask].tolist() if zero_vol_count > 0 else []}

class ResilientDataSource(EnhancedDataValidator):
    """
    Resilient data source implementation for Exodus v2025.
    Provides robust data retrieval with automatic retries, rate limiting,
    and error recovery capabilities.
    """

    def __init__(self, cache_dir: Path | None = None):
        """Initialize resilient data source."""
        super().__init__()
        self.logger = get_logger(__name__)
        self.config = get_config()

        # Error tracking
        self.error_stats = {
            "timeout_errors": 0,
            "network_errors": 0,
            "validation_errors": 0,
            "rate_limit_hits": 0,
        }
        self.error_history: list[DataSourceError] = []

        # Cache configuration
        self.cache_dir = cache_dir or Path("data/interim/cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Rate limiting
        self.last_request_time = {}
        self.min_request_interval = 1.0  # seconds

        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Exodus-v2025/1.0',
            'Accept': 'application/json'
        })

    def __del__(self):
        """Clean up session on deletion."""
        if hasattr(self, 'session'):
            self.session.close()

    def _track_error(self, error_type: str, url: str, message: str, retry_count: int):
        """Track errors for monitoring and analysis."""
        error = DataSourceError(
            error_type=error_type,
            timestamp=datetime.now(UTC),
            url=url,
            message=message,
            retry_count=retry_count
        )
        self.error_history.append(error)
        self.error_stats[error_type] += 1

        # Log critical errors
        if retry_count >= 3:
            self.logger.error(f"Critical error after {retry_count} retries: {message}")

    def _respect_rate_limit(self, endpoint: str):
        """Ensure rate limits are respected."""
        now = datetime.now()
        last_request = self.last_request_time.get(endpoint)

        if last_request:
            time_since_last = (now - last_request).total_seconds()
            if time_since_last < self.min_request_interval:
                sleep_time = self.min_request_interval - time_since_last
                self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                sleep(sleep_time)

        self.last_request_time[endpoint] = now

    def get_data_with_retry(
        self,
        url: str,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 30.0,
        headers: dict | None = None
    ) -> dict | None:
        """Get data with automatic retries and exponential backoff."""
        self._respect_rate_limit(url)

        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)

        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Requesting data from {url} (attempt {attempt + 1})")

                response = self.session.get(
                    url,
                    timeout=timeout,
                    headers=request_headers
                )

                if response.status_code == 200:
                    data = response.json()
                    self.logger.debug(f"Successfully retrieved data from {url}")
                    return data
                elif response.status_code == 429:
                    # Rate limit hit
                    self._track_error("rate_limit_hits", url, "Rate limit exceeded", attempt)
                    retry_after = int(response.headers.get('Retry-After', retry_delay))
                    sleep(retry_after)
                else:
                    response.raise_for_status()

            except (TimeoutError, requests.ConnectionError, requests.Timeout) as e:
                self._track_error("network_errors", url, str(e), attempt)
                if attempt == max_retries - 1:
                    self.logger.error(f"Failed to get data from {url} after {max_retries} attempts")
                    raise

                # Exponential backoff
                backoff_delay = retry_delay * (2 ** attempt)
                self.logger.warning(f"Network error on attempt {attempt + 1}, retrying in {backoff_delay}s")
                sleep(backoff_delay)

            except requests.RequestException as e:
                self._track_error("network_errors", url, str(e), attempt)
                self.logger.error(f"Request error: {e}")
                if attempt == max_retries - 1:
                    raise
                sleep(retry_delay)

        return None

    def clean_and_validate_partial_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate partially corrupted data."""
        try:
            # Use parent class validation
            df = self.validate_ohlcv_data(df)

            # Handle missing values
            df = self._handle_missing_values(df, impute=True)

            # Sort by timestamp
            if 'timestamp' in df.columns:
                df = df.sort_values('timestamp').reset_index(drop=True)

            self.logger.info(f"Cleaned data: {len(df)} records, {df.isnull().sum().sum()} missing values")
            return df

        except Exception as e:
            self._track_error("validation_errors", "local_data", str(e), 0)
            raise

    def get_data_with_rate_limit(
        self,
        url: str,
        rate_limit_delay: float = 1.0,
        max_rate_limit_retries: int = 5
    ) -> dict | None:
        """Get data respecting rate limits with intelligent backoff."""
        self._respect_rate_limit(url)

        for attempt in range(max_rate_limit_retries):
            try:
                response = self.session.get(url)

                if response.status_code == 429:  # Rate limit
                    self._track_error("rate_limit_hits", url, "Rate limit exceeded", attempt)

                    # Try to get retry-after header
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        delay = int(retry_after)
                    else:
                        # Exponential backoff if no retry-after header
                        delay = rate_limit_delay * (2 ** attempt)

                    self.logger.warning(f"Rate limited, waiting {delay}s (attempt {attempt + 1})")
                    sleep(delay)
                    continue

                elif response.status_code == 200:
                    return response.json()
                else:
                    response.raise_for_status()

            except requests.RequestException as e:
                self.logger.error(f"Error in rate limited request: {e}")
                if attempt == max_rate_limit_retries - 1:
                    raise
                sleep(rate_limit_delay)

        return None

    def get_latest_data(self, symbol: str = "BTCUSDT") -> pd.DataFrame:
        """Get latest data in a thread-safe manner."""
        # This is a placeholder implementation
        # In production, this would connect to your chosen exchange API
        timestamp = datetime.now(UTC)

        data = {
            'timestamp': [timestamp],
            'symbol': [symbol],
            'price': [50000.0],  # Mock price
            'volume': [1000.0]
        }

        return pd.DataFrame(data)

    def get_cached_data_with_recovery(
        self,
        cache_key: str,
        fallback_url: str,
        max_cache_age_hours: int = 24
    ) -> pd.DataFrame | dict:
        """Get cached data with automatic recovery and freshness validation."""
        cache_path = self.cache_dir / f"{cache_key}.json"

        try:
            if cache_path.exists():
                # Check cache age
                cache_age = datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)

                if cache_age.total_seconds() < max_cache_age_hours * 3600:
                    # Cache is fresh
                    with open(cache_path, encoding='utf-8') as f:
                        data = json.load(f)

                    self.logger.debug(f"Using cached data for {cache_key}")
                    return pd.DataFrame(data) if isinstance(data, list) else data
                else:
                    self.logger.info(f"Cache expired for {cache_key}, fetching fresh data")

        except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
            self.logger.warning(f"Cache corruption for {cache_key}: {e}")

        # Fetch fresh data and cache it
        try:
            fresh_data = self.get_data_with_retry(fallback_url)
            if fresh_data:
                # Save to cache
                with open(cache_path, 'w', encoding='utf-8') as f:
                    json.dump(fresh_data, f, indent=2, default=str)

                self.logger.info(f"Cached fresh data for {cache_key}")
                return fresh_data

        except Exception as e:
            self.logger.error(f"Failed to fetch fresh data for {cache_key}: {e}")

            # Try to use stale cache as last resort
            if cache_path.exists():
                with open(cache_path, encoding='utf-8') as f:
                    stale_data = json.load(f)
                self.logger.warning(f"Using stale cache for {cache_key}")
                return stale_data

        return None

    def validate_and_fix_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and fix timestamps with timezone awareness."""
        df = df.copy()

        # Convert to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

        # Remove rows with invalid timestamps
        initial_count = len(df)
        df = df.dropna(subset=['timestamp'])

        if len(df) < initial_count:
            self.logger.warning(f"Removed {initial_count - len(df)} rows with invalid timestamps")

        # Ensure timezone awareness
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        else:
            df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')

        # Sort by timestamp and remove duplicates
        df = df.sort_values('timestamp').drop_duplicates(subset=['timestamp'])

        return df.reset_index(drop=True)

    def get_data_with_timeout(self, url: str, timeout: float = 30.0) -> dict:
        """Get data with configurable timeout."""
        try:
            self._respect_rate_limit(url)
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()

        except requests.Timeout:
            self._track_error("timeout_errors", url, f"Request timed out after {timeout}s", 0)
            raise TimeoutError(f"Request to {url} timed out after {timeout}s")

    def get_error_summary(self) -> dict:
        """Get summary of all errors encountered."""
        return {
            'error_stats': self.error_stats.copy(),
            'recent_errors': [
                {
                    'type': error.error_type,
                    'timestamp': error.timestamp.isoformat(),
                    'url': error.url,
                    'message': error.message
                }
                for error in self.error_history[-10:]  # Last 10 errors
            ],
            'total_errors': len(self.error_history)
        }

    def reset_error_stats(self):
        """Reset error statistics (useful for monitoring intervals)."""
        self.error_stats = dict.fromkeys(self.error_stats, 0)
        self.error_history.clear()
        self.logger.info("Error statistics reset")

    def clean_and_export_to_parquet(
        self,
        csv_path: str,
        output_dir: str = "data/processed",
        impute: bool = True,
        script_version: str = "1.0.0",
        data_source: str = None
    ) -> str:
        """
        Limpia un archivo CSV crudo, exporta la versión limpia a Parquet y genera metadatos avanzados.
        """
        logger = get_logger("data_cleaner")
        cleaner = EnhancedDataValidator()
        metadata_manager = EnhancedMetadataManager()

        # Leer CSV crudo
        df = pd.read_csv(csv_path)
        logger.info(f"Leído archivo crudo: {csv_path} ({len(df)} registros)")

        # Validar y limpiar (un solo método)
        df, validation_report = cleaner.validate_and_clean(df, impute=impute)
        logger.info(f"Reporte de validación: {validation_report}")

        # Guardar Parquet limpio
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = output_dir / (Path(csv_path).stem + ".parquet")
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Datos limpios guardados en Parquet: {parquet_path}")

        # Generar metadatos avanzados
        metadata_manager.save_metadata(
            csv_path=parquet_path,
            symbol=df["symbol"].iloc[0] if "symbol" in df.columns else "unknown",
            interval="unknown",  # Ajustar si se puede inferir
            num_records=len(df),
            script_version=script_version,
            data_source=data_source,
            df=df
        )
        return str(parquet_path)
