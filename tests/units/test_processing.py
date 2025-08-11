"""
Tests for the data processing modules.
"""

import numpy as np
import pandas as pd
import pytest

from data_etl.processing.data_normalizer import DataNormalizer
from data_etl.processing.technical_indicators import TechnicalIndicators
from data_etl.processing.timeframe_aggregator import TimeframeAggregator


@pytest.fixture
def sample_ohlcv_data():
    """Create sample OHLCV data for testing."""
    dates = pd.date_range(start='2025-01-01', end='2025-01-10', freq='1min')
    data = {
        'open': np.random.uniform(30000, 31000, len(dates)),
        'high': np.random.uniform(31000, 32000, len(dates)),
        'low': np.random.uniform(29000, 30000, len(dates)),
        'close': np.random.uniform(30000, 31000, len(dates)),
        'volume': np.random.uniform(1, 100, len(dates))
    }
    return pd.DataFrame(data, index=dates)

class TestDataNormalizer:
    """Test suite for DataNormalizer class."""

    def test_zscore_normalization(self, sample_ohlcv_data):
        """Test z-score normalization of OHLCV data."""
        normalizer = DataNormalizer()
        normalized = normalizer.normalize_ohlcv(sample_ohlcv_data, method='zscore')

        # Check if normalized data has zero mean and unit variance
        for col in normalized.columns:
            assert abs(normalized[col].mean()) < 0.01
            assert abs(normalized[col].std() - 1.0) < 0.01

    def test_minmax_normalization(self, sample_ohlcv_data):
        """Test min-max normalization of OHLCV data."""
        normalizer = DataNormalizer()
        normalized = normalizer.normalize_ohlcv(sample_ohlcv_data, method='minmax')

        # Check if values are in [0,1] range
        for col in normalized.columns:
            assert normalized[col].min() >= 0
            assert normalized[col].max() <= 1

    def test_log_normalization(self, sample_ohlcv_data):
        """Test logarithmic normalization of OHLCV data."""
        normalizer = DataNormalizer()
        normalized = normalizer.normalize_ohlcv(sample_ohlcv_data, method='log')

        # Check if values are transformed
        assert not (normalized == sample_ohlcv_data).all().all()

    def test_denormalization(self, sample_ohlcv_data):
        """Test denormalization back to original scale."""
        normalizer = DataNormalizer()

        for method in ['zscore', 'minmax', 'log']:
            normalized = normalizer.normalize_ohlcv(sample_ohlcv_data, method=method)
            denormalized = normalizer.denormalize_ohlcv(normalized, method=method)

            # Check if denormalized data matches original
            pd.testing.assert_frame_equal(
                sample_ohlcv_data,
                denormalized,
                check_exact=False,
                rtol=1e-10
            )

class TestTechnicalIndicators:
    """Test suite for TechnicalIndicators class."""

    def test_sma_calculation(self, sample_ohlcv_data):
        """Test Simple Moving Average calculation."""
        ti = TechnicalIndicators()
        sma = ti.sma(sample_ohlcv_data['close'], period=20)

        assert isinstance(sma, pd.Series)
        assert len(sma) == len(sample_ohlcv_data)
        assert pd.isna(sma[:19]).all()  # First 19 values should be NaN
        assert not pd.isna(sma[19:]).any()  # Rest should be calculated

    def test_ema_calculation(self, sample_ohlcv_data):
        """Test Exponential Moving Average calculation."""
        ti = TechnicalIndicators()
        ema = ti.ema(sample_ohlcv_data['close'], period=20)

        assert isinstance(ema, pd.Series)
        assert len(ema) == len(sample_ohlcv_data)
        assert not pd.isna(ema).any()  # EMA should not have NaN values

    def test_rsi_calculation(self, sample_ohlcv_data):
        """Test Relative Strength Index calculation."""
        ti = TechnicalIndicators()
        rsi = ti.rsi(sample_ohlcv_data['close'])

        assert isinstance(rsi, pd.Series)
        assert len(rsi) == len(sample_ohlcv_data)
        assert pd.isna(rsi[:13]).all()  # First 13 values should be NaN
        assert not pd.isna(rsi[14:]).any()  # Rest should be calculated
        assert (rsi[14:] >= 0).all() and (rsi[14:] <= 100).all()  # RSI range

    def test_macd_calculation(self, sample_ohlcv_data):
        """Test MACD calculation."""
        ti = TechnicalIndicators()
        macd = ti.macd(sample_ohlcv_data['close'])

        assert isinstance(macd, pd.DataFrame)
        assert all(col in macd.columns for col in ['macd', 'signal', 'histogram'])
        assert len(macd) == len(sample_ohlcv_data)

    def test_bollinger_bands(self, sample_ohlcv_data):
        """Test Bollinger Bands calculation."""
        ti = TechnicalIndicators()
        bb = ti.bollinger_bands(sample_ohlcv_data['close'])

        assert isinstance(bb, pd.DataFrame)
        assert all(col in bb.columns for col in ['middle', 'upper', 'lower'])
        assert len(bb) == len(sample_ohlcv_data)
        # Skip first n-1 rows where n is the period (default 20) due to rolling window
        assert (bb['upper'][20:] >= bb['middle'][20:]).all()
        assert (bb['lower'][20:] <= bb['middle'][20:]).all()

class TestTimeframeAggregator:
    """Test suite for TimeframeAggregator class."""

    def test_timeframe_validation(self):
        """Test timeframe validation."""
        ta = TimeframeAggregator()

        # Test valid timeframes
        assert ta.validate_timeframe('1m') == '1T'
        assert ta.validate_timeframe('1h') == '1H'
        assert ta.validate_timeframe('1d') == '1D'

        # Test invalid timeframe
        with pytest.raises(ValueError):
            ta.validate_timeframe('invalid')

    def test_ohlcv_aggregation(self, sample_ohlcv_data):
        """Test OHLCV data aggregation."""
        ta = TimeframeAggregator()

        # Test 1-hour aggregation
        hourly = ta.aggregate_ohlcv(sample_ohlcv_data, '1H')

        assert isinstance(hourly, pd.DataFrame)
        assert all(col in hourly.columns for col in ['open', 'high', 'low', 'close', 'volume'])
        assert len(hourly) < len(sample_ohlcv_data)  # Should be fewer rows

        # Verify aggregation logic
        assert (hourly['high'] >= hourly['low']).all()  # High should be >= Low
        assert (hourly['high'] >= hourly['open']).all()  # High should be >= Open
        assert (hourly['high'] >= hourly['close']).all()  # High should be >= Close

    def test_multiple_timeframes(self, sample_ohlcv_data):
        """Test generation of multiple timeframes."""
        ta = TimeframeAggregator()
        timeframes = ['5m', '15m', '1h']

        results = ta.generate_multiple_timeframes(sample_ohlcv_data, timeframes)

        assert isinstance(results, dict)
        assert all(tf in results for tf in timeframes)
        assert all(isinstance(df, pd.DataFrame) for df in results.values())

        # Verify decreasing number of rows with increasing timeframe
        assert len(results['5m']) >= len(results['15m']) >= len(results['1h'])
