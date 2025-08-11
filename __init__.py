"""
Data module for Exodus v2025
Handles data acquisition, processing, and storage.
"""

# Storage components
# Pipeline
from data_etl.pipelines.crypto_pipeline import CryptoPipeline

# Processing components
from data_etl.processing.data_normalizer import DataNormalizer
from data_etl.processing.technical_indicators import TechnicalIndicators
from data_etl.processing.timeframe_aggregator import TimeframeAggregator

# Provider components
from data_etl.providers.crypto.bybit_downloader import BybitDownloader
from data_etl.storage.data_storage_manager import DataStorageManager
from data_etl.storage.timeseries_db import TimeSeriesDB

# Validation components
from data_etl.validation.data_processor import DataProcessor

__all__ = [
    "TimeSeriesDB",
    "DataStorageManager",
    "BybitDownloader",
    "DataNormalizer",
    "TechnicalIndicators",
    "TimeframeAggregator",
    "DataProcessor",
    "CryptoPipeline",
]
