"""
Tests for data validation and processing module.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from data_etl.validation.data_processor import DataProcessor

@pytest.fixture
def processor():
    """DataProcessor instance for testing."""
    return DataProcessor()

@pytest.fixture
def valid_data():
    """Valid OHLCV data for testing."""
    return pd.DataFrame({
        'timestamp': [pd.Timestamp.now(tz=pytz.UTC)],
        'symbol': ['BTCUSDT'],
        'open': [30000.0],
        'high': [30500.0],
        'low': [29900.0],
        'close': [30400.0],
        'volume': [100.0]
    })

@pytest.fixture
def invalid_data():
    """Invalid OHLCV data for testing."""
    return pd.DataFrame({
        'timestamp': [pd.Timestamp.now(tz=pytz.UTC)],
        'symbol': ['BTCUSDT'],
        'open': [30000.0],
        'high': [29000.0],  # High less than low
        'low': [29900.0],
        'close': [30400.0],
        'volume': [-100.0]  # Negative volume
    })

def test_validate_ohlcv_valid_data(processor, valid_data):
    """Test validation with valid data."""
    result = processor.validate_ohlcv_data(valid_data)
    assert not result.empty
    assert all(col in result.columns for col in ['open', 'high', 'low', 'close', 'volume'])

def test_validate_ohlcv_invalid_data(processor, invalid_data):
    """Test validation with invalid data."""
    result = processor.validate_ohlcv_data(invalid_data)
    assert result.empty  # Invalid rows should be removed

def test_clean_outliers_zscore(processor, valid_data):
    """Test outlier cleaning with z-score method."""
    # Add an outlier
    outlier_data = valid_data.copy()
    outlier_data.loc[len(outlier_data)] = outlier_data.iloc[0]
    outlier_data.iloc[-1]['close'] = 50000.0  # Significant outlier
    
    result = processor.clean_outliers(outlier_data, threshold=2.0, method='zscore')
    assert not pd.isna(result['close']).any()  # Should be filled
    assert result['close'].iloc[-1] != 50000.0  # Outlier should be replaced

def test_clean_outliers_iqr(processor, valid_data):
    """Test outlier cleaning with IQR method."""
    # Add an outlier
    outlier_data = valid_data.copy()
    outlier_data.loc[len(outlier_data)] = outlier_data.iloc[0]
    outlier_data.iloc[-1]['volume'] = 1000000.0  # Significant outlier
    
    result = processor.clean_outliers(outlier_data, method='iqr')
    assert not pd.isna(result['volume']).any()  # Should be filled
    assert result['volume'].iloc[-1] != 1000000.0  # Outlier should be replaced

def test_validate_and_clean_pipelines(processor, valid_data):
    """Test complete validation and cleaning pipelines."""
    # Add some issues to the data
    test_data = valid_data.copy()
    test_data.loc[len(test_data)] = test_data.iloc[0]
    test_data.iloc[-1]['close'] = 50000.0  # Outlier
    test_data.iloc[-1]['volume'] = -100.0  # Invalid volume
    
    result = processor.validate_and_clean(
        test_data,
        clean_outliers=True,
        outlier_threshold=2.0
    )
    
    assert not result.empty
    assert result.shape[0] <= test_data.shape[0]  # Invalid rows might be removed
    assert all(result['volume'] >= 0)  # No negative volumes
    assert result['close'].iloc[-1] != 50000.0  # Outlier should be replaced
