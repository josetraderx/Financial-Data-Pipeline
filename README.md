
# Financial Data Pipeline
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
9. Data Splitting
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

- Python >=3.8
- Poetry or pip for dependencies
- PostgreSQL and TimescaleDB (for DB features)
- pytest for testing

### Installation

```bash
git clone <repo_url>
cd Data_ETL
poetry install # or pip install -r requirements.txt
docker-compose up # Upload TimescaleDB and environment
python run_pipeline.py # Run the pipeline
python pipeline_status_report.py # Audit and report status
```

## Architecture

```
Data_ETL/
├── providers/
├── processing/
├── storage/
├── pipelines/
├── feeds/
├── raw/
├── validation/
├── tests/
├── notebooks/
└── README.md
```

## Key Features

- OHLCV data download from exchanges (Bybit, extensible)
- Advanced validation and cleansing (outliers, gaps, duplicates, types, OHLCV relationships)
- Optional normalization and temporal aggregation
- Splits: train/test, by date, sliding windows
- Storage in TimescaleDB/PostgreSQL and export to Parquet/CSV/JSON
- Centralized metadata management and validation reports
- Professional logging and robust error handling (rate limiting, retries)
- Docker Compose for reproducible environments
- Unit and integration testing (pytest)
- Audit scripts and reporting of pipeline and database status

## Quick Start

```python
from src.data_etl.pipelines.crypto_pipeline import CryptoPipeline
from config.pipeline_config import EXAMPLE_PIPELINE_CONFIG

pipeline = CryptoPipeline(EXAMPLE_PIPELINE_CONFIG)
results = pipeline.run_pipeline(EXAMPLE_PIPELINE_CONFIG)
print(results)
```

## Roadmap: What's missing for 100%?

- Advanced TimescaleDB optimization (composite indexes, compression, timeframe retention)
- Performance benchmarks with large data volumes
- Concrete examples of rollback and data corruption handling
- Real-time ingestion with guaranteed integrity
- Advanced nomenclature engine (relationship and lineage tracking)
- Evidence of successful handoff and automated onboarding
- Extended documentation for onboarding and advanced troubleshooting

## License

MIT. Project part of Exodus v2025.