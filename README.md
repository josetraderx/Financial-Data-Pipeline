

# Financial Data Pipeline �
![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/josetraderx/financial-data-pipeline/main)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/josetraderx/financial-data-pipeline/blob/main/notebooks/exodus_demo.ipynb)

A modular, enterprise-grade ETL pipeline for financial market data. Supports extraction, transformation, validation, and storage from multiple sources (crypto, equities, derivatives) with advanced analytics and database integration.

## Table of Contents
1. Overview
2. Requirements
3. Architecture
4. Key Features
5. Quick Start
6. Configuration
7. Data Validation
8. Data Storage
# Financial Data Pipeline
10. Monitoring and Quality Assurance
11. Error Handling
12. Performance Optimization
13. Testing
14. Examples
15. Extension Points
16. Troubleshooting
17. Contributing
18. License

## Overview

Exodus v2025 is a professional and modular ETL pipeline for financial data (crypto, stocks, derivatives), with advanced validation, splits, storage in TimescaleDB/PostgreSQL, and export to Parquet/CSV/JSON files.

## Requirements

- **Python**: >=3.8
- **Poetry** (recommended) or pip for dependency management
- **PostgreSQL** and **TimescaleDB** for database storage (if using DB features)
- **pytest** for running tests

### Installation

1. Clone the repository:
   ```bash
   git clone <repo_url>
   cd Data_ETL
   ```
2. Install dependencies (using Poetry):
   ```bash
   poetry install
   ```
   Or with pip:

## Architecture

```
Data_ETL/
├── providers/          # Data provider implementations
│   ├── crypto/         # Cryptocurrency data providers
│   ├── equities/       # Stock market data providers
│   ├── options/        # Options data providers
│   └── other_derivatives/  # Other derivative instruments
├── processing/         # Data processing and validation
│   ├── enhanced_metadata_manager.py  # Centralized metadata management
│   ├── data_cleaner.py              # Data validation and cleaning
│   └── data_splitter.py             # Data splitting utilities
├── storage/            # Database storage solutions
│   ├── timeseries_db.py             # TimescaleDB for time series data
│   └── metadata_db.py               # PostgreSQL for metadata
├── pipelines/          # Complete data pipelines
│   ├── crypto_pipeline.py           # Crypto data pipeline
│   ├── config_manager.py            # Configuration management
│   └── example_usage.py             # Usage examples
├── feeds/              # Data feeds
├── raw/                # Raw data and archiving
├── validation/         # Data validation modules
├── tests/              # Test scripts and setup
├── notebooks/          # Jupyter notebooks and demos
└── README.md           # Project documentation
```

## Key Features

### 1. Data Providers
- **Bybit**: Cryptocurrency data with real-time and historical OHLCV data
- **Extensible**: Easy to add new providers (equities, options, etc.)
- **Rate Limiting**: Built-in rate limiting and error handling

### 2. Data Processing
- **Enhanced Validation**: Comprehensive OHLCV data validation
- **Data Cleaning**: Automated missing value imputation and outlier detection
- **Quality Scoring**: Automated data quality assessment
- **Metadata Management**: Centralized metadata storage and retrieval

### 3. Data Splitting
- **Chronological Splits**: Time-based train/test splits
- **Random Splits**: Randomized data splitting
- **Date-based Splits**: Split by specific dates
- **Sliding Windows**: Generate sliding window datasets for time series

### 4. Storage Solutions
- **TimescaleDB**: High-performance time series storage
- **PostgreSQL**: Metadata and data lineage storage
- **File Storage**: Parquet, CSV, and JSON export options
- **Data Lineage**: Track data transformations and relationships

### 5. Pipeline Orchestration
- **End-to-End Pipelines**: Complete data processing workflows
- **Configuration Management**: Flexible configuration system
- **Error Handling**: Comprehensive error handling and logging
- **Monitoring**: Built-in data quality monitoring

## Quick Start

### 1. Basic Setup

```python
from Data_ETL.pipelines.crypto_pipeline import CryptoPipeline
from Data_ETL.pipelines.config_manager import PipelineConfig

# Load configuration
config = PipelineConfig('config/pipeline_config.json')

# Create pipeline
pipeline = CryptoPipeline(config.get())
```

### 2. Simple Data Download and Processing

```python
# Configure pipeline run
pipeline_config = config.create_pipeline_config(
    provider='bybit',
    symbol='BTCUSDT',
    timeframe='1h',
    days_back=30,
    save_files=True,
    store_db=False
)

# Run pipeline
results = pipeline.run_pipeline(pipeline_config)
```

### 3. Advanced Pipeline with Database Storage

```python
# Configure advanced pipeline
pipeline_config = config.create_pipeline_config(
    provider='bybit',
    symbol='ETHUSDT',
    timeframe='4h',
    days_back=30,
    splits={
        'train_test_split': {
            'test_size': 0.2,
            'method': 'chronological'
        }
    },
    store_db=True,
    save_files=True
)

# Run pipeline
results = pipeline.run_pipeline(pipeline_config)
```

## Configuration

### Environment Variables
```bash
# Database Configuration
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=exodus_data
export DB_USER=postgres
export DB_PASSWORD=your_password

# Bybit API Configuration
export BYBIT_API_KEY=your_api_key
export BYBIT_API_SECRET=your_api_secret
export BYBIT_TESTNET=true
```

### Configuration File Example
```json
{
  "data_dir": "data/processed",
  "db_config": {
    "host": "localhost",
    "port": 5432,
    "database": "exodus_data",
    "user": "postgres",
    "password": "your_password"
  },
  "providers": {
    "bybit": {
      "api_key": "your_api_key",
      "api_secret": "your_api_secret",
      "testnet": true
    }
  },
  "validation_config": {
    "handle_missing": "interpolate",
    "outlier_method": "iqr",
    "outlier_threshold": 1.5
  }
}
```

## Data Validation

The `EnhancedDataValidator` performs comprehensive data validation:

### Validation Checks
- **Required Columns**: Ensures OHLCV columns are present
- **Data Types**: Validates numeric types for price and volume
- **OHLCV Relationships**: Validates price relationships (High >= Low, etc.)
- **Missing Values**: Detects and handles missing data
- **Outliers**: Identifies and handles outliers using IQR method
- **Duplicates**: Detects and removes duplicate records
- **Timestamps**: Validates and regularizes timestamp format
- **Zero Values**: Detects zero prices and volumes
- **Extreme Changes**: Identifies unrealistic price movements

### Validation Report
```python
{
    "is_valid": true,
    "total_records": 720,
    "valid_records": 718,
    "invalid_records": 2,
    "missing_values": 0,
    "outliers_detected": 5,
    "duplicates_found": 0,
    "quality_score": 0.97,
    "errors": [],
    "warnings": ["Some outliers detected"],
    "validation_details": {
        "column_stats": {...},
        "missing_analysis": {...},
        "outlier_analysis": {...}
    }
}
```

## Data Storage

### TimescaleDB Integration
```python
from Data_ETL.storage.timeseries_db import TimeSeriesDB

# Connect to TimescaleDB
db = TimeSeriesDB(db_config)
db.connect()
db.create_hypertable()

# Insert data
db.insert_data(dataframe)

# Query data
data = db.query_data(
    symbol='BTCUSDT',
    start_time=start_date,
    end_time=end_date
)
```

### Metadata Management
```python
from Data_ETL.storage.metadata_db import MetadataDB

# Connect to metadata database
metadata_db = MetadataDB(db_config)
metadata_db.connect()
metadata_db.create_tables()

# Store metadata
dataset_id = metadata_db.insert_dataset_metadata(metadata)

# Store validation report
metadata_db.insert_validation_report(dataset_id, report)
```

## Data Splitting

### Chronological Split
```python
from Data_ETL.processing.data_splitter import DataSplitter

splitter = DataSplitter()
train_data, test_data = splitter.train_test_split(
    data, 
    test_size=0.2, 
    method='chronological'
)
```

### Date-based Split
```python
splits = splitter.split_by_date(
    data, 
    split_date=datetime(2024, 1, 1)
)
```

### Sliding Windows
```python
windows = splitter.create_sliding_windows(
    data, 
    window_size=100, 
    step_size=10
)
```

## Monitoring and Quality Assurance

### Data Quality Metrics
- **Completeness**: Percentage of non-null values
- **Accuracy**: Validation rule compliance
- **Consistency**: OHLCV relationship consistency
- **Timeliness**: Data freshness and update frequency
- **Validity**: Data type and range validation

### Monitoring Dashboard
The pipeline provides data quality metrics that can be integrated with monitoring systems:

```python
# Get quality metrics
metrics = validator.get_quality_metrics(data)

# Store metrics in database
metadata_db.insert_quality_metrics(dataset_id, metrics)
```

## Error Handling

The pipeline includes comprehensive error handling:

- **Connection Errors**: Database and API connection failures
- **Data Validation Errors**: Invalid data detection and reporting
- **Processing Errors**: Transformation and computation errors
- **Storage Errors**: File and database storage failures

## Performance Optimization

### Database Optimization
- **Hypertables**: TimescaleDB hypertables for time series data
- **Indexes**: Optimized indexes for common queries
- **Partitioning**: Automatic time-based partitioning
- **Compression**: Built-in data compression

### Memory Management
- **Chunked Processing**: Process large datasets in chunks
- **Lazy Loading**: Load data only when needed
- **Memory Monitoring**: Track memory usage during processing

## Testing

### Unit Tests
```bash
# Run unit tests
pytest Data_ETL/processing/tests/

# Run specific test module
pytest Data_ETL/processing/tests/test_data_cleaner.py
```

### Integration Tests
```bash
# Run integration tests
pytest tests/setup_test_db.py
```

## Examples

### Complete Examples
See `Data_ETL/pipelines/example_usage.py` for complete working examples including:

1. **Basic Pipeline**: Simple data download and processing
2. **Advanced Pipeline**: Full pipeline with database storage
3. **Metadata Management**: Metadata operations and queries
4. **Data Quality Analysis**: Quality assessment and reporting

### Running Examples
```bash
# Run all examples
python Data_ETL/pipelines/example_usage.py

# Set up configuration
python Data_ETL/pipelines/config_manager.py
```

## Extension Points

### Adding New Providers
1. Create provider class in appropriate subdirectory
2. Implement required methods (download, authenticate, etc.)
3. Add provider configuration to config system
4. Update pipeline to support new provider

### Custom Validation Rules
1. Extend `EnhancedDataValidator` class
2. Add custom validation methods
3. Update validation configuration
4. Register custom validators in pipeline

### Storage Backends
1. Create new storage class implementing storage interface
2. Add configuration options
3. Integrate with pipeline storage system

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check database credentials and connectivity
   - Ensure PostgreSQL/TimescaleDB is running
   - Verify database permissions

2. **API Rate Limiting**
   - Check API key permissions
   - Implement proper rate limiting
   - Use testnet for development

3. **Data Validation Failures**
   - Review validation configuration
   - Check data format and structure
   - Examine validation report for details

4. **Memory Issues**
   - Reduce batch sizes
   - Use chunked processing
   - Monitor memory usage

### Debug Mode
```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Run pipeline with debug info
results = pipeline.run_pipeline(config, debug=True)
```

## Contributing

1. Follow the existing code structure and patterns
2. Add comprehensive tests for new features
3. Update documentation for any changes
4. Ensure backward compatibility
5. Follow Python best practices and type hints

## License

This module is part of the Exodus v2025 project and follows the project's licensing terms.
