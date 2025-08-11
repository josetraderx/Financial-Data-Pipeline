"""
Tests for data validation module.
"""

import pandas as pd
import pytest
import pytz

from data_etl.validation.data_validator import DataValidator


@pytest.fixture
def validator():
    """Instancia de DataValidator para pruebas."""
    return DataValidator()

@pytest.fixture
def valid_data():
    """Datos OHLCV válidos para pruebas."""
    return pd.DataFrame({
        'timestamp': [pd.Timestamp.now(tz=pytz.UTC)],
        'symbol': ['BTCUSDT'],
        'open': [30000.0],
        'high': [30500.0],
        'low': [29900.0],
        'close': [30400.0],
        'volume': [100.0]
    })
