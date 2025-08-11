"""
Data validation module for Exodus v2025.
Handles OHLCV data validation and cleaning.
"""

import logging

import numpy as np
import pandas as pd

from data_etl.utils.logging import get_logger


class DataValidator:
    """
    Validates and cleans OHLCV data according to trading requirements.
    """

    def __init__(self, logger: logging.Logger | None = None):
        """
        Initialize data validator.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger(__name__)

    def validate_ohlcv_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and clean OHLCV data.

        Args:
            df: DataFrame with OHLCV data

        Returns:
            pd.DataFrame: Cleaned and validated data
        """
        if df.empty:
            self.logger.warning("Empty DataFrame provided")
            return df

        try:
            # 1. Validar columnas requeridas
            required_columns = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"Missing required columns: {missing_columns}")
                return pd.DataFrame()

            # 2. Eliminar duplicados
            original_len = len(df)
            df = df.drop_duplicates(subset=["timestamp", "symbol"])
            if len(df) < original_len:
                self.logger.warning(f"Removed {original_len - len(df)} duplicate rows")

            # 3. Validar tipos de datos
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            numeric_columns = ["open", "high", "low", "close", "volume"]
            df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

            # 4. Validar valores
            # 4.1 No negativos
            for col in numeric_columns:
                invalid_rows = df[df[col] < 0].index
                if not invalid_rows.empty:
                    self.logger.warning(f"Found {len(invalid_rows)} negative values in {col}")
                    df.loc[invalid_rows, col] = np.nan

            # 4.2 Validar OHLC (high >= low, etc.)
            invalid_rows = df[
                (df["high"] < df["low"])
                | (df["open"] < df["low"])
                | (df["open"] > df["high"])
                | (df["close"] < df["low"])
                | (df["close"] > df["high"])
            ].index

            if not invalid_rows.empty:
                self.logger.warning(
                    f"Found {len(invalid_rows)} rows with invalid OHLC relationships"
                )
                df = df.drop(invalid_rows)

            # 5. Detectar y manejar outliers
            for col in ["high", "low", "close"]:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 3 * IQR
                upper_bound = Q3 + 3 * IQR

                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)].index
                if not outliers.empty:
                    self.logger.warning(f"Found {len(outliers)} outliers in {col}")
                    # Opcionalmente, podrías eliminarlos o marcarlos

            # 6. Validar gaps temporales
            df = df.sort_values("timestamp")
            time_diff = df["timestamp"].diff()
            expected_diff = pd.Timedelta(minutes=1)  # Ajusta según el intervalo esperado

            gaps = time_diff[time_diff > expected_diff]
            if not gaps.empty:
                self.logger.warning(f"Found {len(gaps)} time gaps in data")

            # 7. Eliminar filas con NaN
            original_len = len(df)
            df = df.dropna()
            if len(df) < original_len:
                self.logger.warning(f"Removed {original_len - len(df)} rows with NaN values")

            self.logger.info(f"Validation complete. {len(df)} valid rows remaining")
            return df

        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            return pd.DataFrame()

    def calculate_quality_score(self, df: pd.DataFrame) -> float:
        """
        Calculate a quality score for the dataset.

        Args:
            df: DataFrame with OHLCV data

        Returns:
            float: Quality score between 0 and 1
        """
        if df.empty:
            return 0.0

        try:
            scores = []

            # 1. Completitud (no NaN)
            completeness = 1 - (df.isna().sum().sum() / (len(df) * len(df.columns)))
            scores.append(completeness)

            # 2. Consistencia OHLC
            ohlc_valid = (
                (df["high"] >= df["low"])
                & (df["open"] >= df["low"])
                & (df["open"] <= df["high"])
                & (df["close"] >= df["low"])
                & (df["close"] <= df["high"])
            ).mean()
            scores.append(ohlc_valid)

            # 3. Continuidad temporal
            time_diff = df["timestamp"].diff()
            expected_diff = pd.Timedelta(minutes=1)  # Ajusta según el intervalo
            temporal_continuity = (time_diff <= expected_diff).mean()
            scores.append(temporal_continuity)

            # 4. Validez de volumen
            volume_valid = (df["volume"] > 0).mean()
            scores.append(volume_valid)

            # Promedio ponderado
            weights = [0.3, 0.3, 0.2, 0.2]  # Ajusta según importancia
            quality_score = sum(
                score * weight for score, weight in zip(scores, weights, strict=False)
            )

            self.logger.info(f"Data quality score: {quality_score:.2f}")
            return quality_score

        except Exception as e:
            self.logger.error(f"Failed to calculate quality score: {str(e)}")
            return 0.0
