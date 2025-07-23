"""
Shared test configuration and fixtures.
"""

import pytest
import pandas as pd
from datetime import datetime
import pytz
from data_etl.storage.timeseries_db import TimeSeriesDB
from data_etl.pipelines.crypto_pipeline import CryptoPipeline

@pytest.fixture
def sample_ohlcv_data():
    """Sample OHLCV data for testing."""
    return pd.DataFrame({
        'timestamp': [pd.Timestamp.now(tz=pytz.UTC)],
        'symbol': ['BTCUSDT'],
        'open': [30000.0],
        'high': [30500.0],
        'low': [29900.0],
        'close': [30400.0],
        'volume': [100.0],
        'hour': [datetime.now().hour],
        'day_of_week': [datetime.now().weekday()],
        'month': [datetime.now().month],
        'year': [datetime.now().year]
    })

@pytest.fixture
def db_test_config():
    """Database configuration for testing."""
    return {
        'host': 'localhost',
        'port': 5433,
        'database': 'exodus_test_db',
        'user': 'postgres',
        'password': 'Jireh2023.'
    }

@pytest.fixture
def pipeline(db_test_config):
    """Create a test pipeline instance."""
    pipeline = CryptoPipeline(
        symbols=['BTCUSDT'],
        interval='1m',
        db_config=db_test_config,
        test_mode=True
    )
    yield pipeline
    # Clean up after tests if needed
    if hasattr(pipeline, 'db') and pipeline.db and pipeline.db.is_connected():
        pipeline.db.disconnect()
