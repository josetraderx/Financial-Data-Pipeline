"""
Example usage of the Exodus v2025 crypto data pipeline.
Demonstrates complete end-to-end data processing workflow.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Add src to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from src.data.pipelines.crypto_pipeline import CryptoPipeline
from src.data.pipelines.config_manager import PipelineConfig, create_example_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_example_environment():
    """Set up example environment and configuration."""
    print("Setting up example environment...")
    
    # Create config directory
    config_dir = Path('config')
    config_dir.mkdir(exist_ok=True)
    
    # Create example configuration
    config_file = config_dir / 'pipeline_config.json'
    if not config_file.exists():
        create_example_config(str(config_file))
    
    # Create data directories
    data_dir = Path('data')
    (data_dir / 'processed').mkdir(parents=True, exist_ok=True)
    (data_dir / 'raw').mkdir(parents=True, exist_ok=True)
    
    print("Example environment set up successfully!")
    print(f"Configuration file: {config_file}")
    print(f"Data directory: {data_dir}")


def run_basic_pipeline_example():
    """Run a basic pipeline example without database storage."""
    print("\n" + "="*50)
    print("BASIC PIPELINE EXAMPLE (File-based)")
    print("="*50)
    
    # Load configuration
    config = PipelineConfig('config/pipeline_config.json')
    
    # Override database storage for this example
    config.set('storage_config.store_db', False)
    config.set('storage_config.save_files', True)
    
    # Create pipeline
    pipeline = CryptoPipeline(config.get())
    
    # Configure pipeline run
    pipeline_config = config.create_pipeline_config(
        provider='bybit',
        symbol='BTCUSDT',
        timeframe='1h',
        days_back=7,  # Small dataset for example
        store_db=False,
        save_files=True
    )
    
    print(f"Running pipeline for {pipeline_config['symbol']} {pipeline_config['timeframe']}")
    print(f"Date range: {pipeline_config['start_date']} to {pipeline_config['end_date']}")
    
    # Run pipeline
    results = pipeline.run_pipeline(pipeline_config)
    
    # Display results
    print("\nPipeline Results:")
    print(f"Success: {results['success']}")
    
    if results['success']:
        print(f"Datasets created: {list(results['datasets'].keys())}")
        print(f"Dataset sizes: {results['datasets']}")
        
        if results.get('file_paths'):
            print(f"Files saved: {list(results['file_paths'].values())}")
        
        # Print validation summary
        validation_report = results.get('validation_report', {})
        print(f"\nValidation Report:")
        print(f"  Total records: {validation_report.get('total_records', 0)}")
        print(f"  Valid records: {validation_report.get('valid_records', 0)}")
        print(f"  Missing values: {validation_report.get('missing_values', 0)}")
        print(f"  Outliers detected: {validation_report.get('outliers_detected', 0)}")
    
    if results['errors']:
        print(f"Errors: {results['errors']}")
    
    return results


def run_advanced_pipeline_example():
    """Run an advanced pipeline example with database storage."""
    print("\n" + "="*50)
    print("ADVANCED PIPELINE EXAMPLE (Database + Files)")
    print("="*50)
    
    # Load configuration
    config = PipelineConfig('config/pipeline_config.json')
    
    # Validate configuration
    validation_result = config.validate_config()
    
    if not validation_result['is_valid']:
        print("Configuration validation failed!")
        print(f"Errors: {validation_result['errors']}")
        print("Please check your configuration file and database settings.")
        return None
    
    if validation_result['warnings']:
        print(f"Configuration warnings: {validation_result['warnings']}")
    
    # Create pipeline
    pipeline = CryptoPipeline(config.get())
    
    # Configure pipeline run with advanced features
    pipeline_config = config.create_pipeline_config(
        provider='bybit',
        symbol='ETHUSDT',
        timeframe='4h',
        days_back=30,
        splits={
            'train_test_split': {
                'test_size': 0.2,
                'method': 'chronological'
            },
            'date_split': {
                'split_date': datetime.now() - timedelta(days=15)
            }
        },
        store_db=True,
        save_files=True
    )
    
    print(f"Running advanced pipeline for {pipeline_config['symbol']} {pipeline_config['timeframe']}")
    print(f"Date range: {pipeline_config['start_date']} to {pipeline_config['end_date']}")
    print(f"Splits configured: {list(pipeline_config['splits'].keys())}")
    
    # Run pipeline
    results = pipeline.run_pipeline(pipeline_config)
    
    # Display results
    print("\nAdvanced Pipeline Results:")
    print(f"Success: {results['success']}")
    
    if results['success']:
        print(f"Datasets created: {list(results['datasets'].keys())}")
        print(f"Dataset sizes: {results['datasets']}")
        
        if results.get('dataset_ids'):
            print(f"Database IDs: {results['dataset_ids']}")
        
        if results.get('file_paths'):
            print(f"Files saved: {len(results['file_paths'])}")
        
        # Print validation summary
        validation_report = results.get('validation_report', {})
        print(f"\nValidation Report:")
        print(f"  Total records: {validation_report.get('total_records', 0)}")
        print(f"  Valid records: {validation_report.get('valid_records', 0)}")
        print(f"  Data quality score: {validation_report.get('quality_score', 0):.2f}")
        
        if validation_report.get('errors'):
            print(f"  Validation errors: {len(validation_report['errors'])}")
        
        if validation_report.get('warnings'):
            print(f"  Validation warnings: {len(validation_report['warnings'])}")
    
    if results['errors']:
        print(f"Errors: {results['errors']}")
    
    return results


def demonstrate_metadata_management():
    """Demonstrate metadata management capabilities."""
    print("\n" + "="*50)
    print("METADATA MANAGEMENT EXAMPLE")
    print("="*50)
    
    from src.data.processing.enhanced_metadata_manager import EnhancedMetadataManager
    
    # Initialize metadata manager
    metadata_manager = EnhancedMetadataManager('data/processed')
    
    # List all metadata
    all_metadata = metadata_manager.list_datasets()
    print(f"Total datasets in metadata: {len(all_metadata)}")
    
    for dataset in all_metadata:
        print(f"  - {dataset['dataset_name']} ({dataset['provider']})")
        print(f"    Records: {dataset.get('total_records', 0)}")
        print(f"    Date range: {dataset.get('start_date', 'N/A')} to {dataset.get('end_date', 'N/A')}")
    
    # Search for specific datasets
    btc_datasets = metadata_manager.search_datasets(symbol='BTC')
    print(f"\nBTC datasets found: {len(btc_datasets)}")
    
    # Show metadata statistics
    stats = metadata_manager.get_statistics()
    print(f"\nMetadata Statistics:")
    print(f"  Total datasets: {stats['total_datasets']}")
    print(f"  Providers: {stats['providers']}")
    print(f"  Symbols: {stats['symbols']}")
    print(f"  Timeframes: {stats['timeframes']}")


def run_data_quality_analysis():
    """Run data quality analysis on processed datasets."""
    print("\n" + "="*50)
    print("DATA QUALITY ANALYSIS")
    print("="*50)
    
    from src.data.processing.data_cleaner import EnhancedDataValidator
    import pandas as pd
    
    # Initialize validator
    validator = EnhancedDataValidator()
    
    # Find processed data files
    data_dir = Path('data/processed')
    parquet_files = list(data_dir.glob('*.parquet'))
    
    print(f"Found {len(parquet_files)} data files for analysis")
    
    for file_path in parquet_files[:3]:  # Analyze first 3 files
        print(f"\nAnalyzing: {file_path.name}")
        
        try:
            # Load data
            data = pd.read_parquet(file_path)
            
            # Run validation
            cleaned_data, report = validator.validate_and_clean(data)
            
            print(f"  Total records: {report['total_records']}")
            print(f"  Valid records: {report['valid_records']}")
            print(f"  Quality score: {report['quality_score']:.2f}")
            print(f"  Missing values: {report['missing_values']}")
            print(f"  Outliers: {report['outliers_detected']}")
            
        except Exception as e:
            print(f"  Error analyzing {file_path.name}: {e}")


def main():
    """Main example execution."""
    print("Exodus v2025 Data Pipeline Examples")
    print("="*50)
    
    # Setup environment
    setup_example_environment()
    
    # Run basic example
    try:
        results1 = run_basic_pipeline_example()
    except Exception as e:
        logger.error(f"Basic pipeline example failed: {e}")
        results1 = None
    
    # Run advanced example (only if basic worked)
    if results1 and results1['success']:
        try:
            results2 = run_advanced_pipeline_example()
        except Exception as e:
            logger.error(f"Advanced pipeline example failed: {e}")
            print("Note: Advanced example requires database configuration")
    
    # Demonstrate metadata management
    try:
        demonstrate_metadata_management()
    except Exception as e:
        logger.error(f"Metadata management example failed: {e}")
    
    # Run data quality analysis
    try:
        run_data_quality_analysis()
    except Exception as e:
        logger.error(f"Data quality analysis failed: {e}")
    
    print("\n" + "="*50)
    print("EXAMPLES COMPLETED")
    print("="*50)
    print("\nNext steps:")
    print("1. Configure your API keys in config/pipeline_config.json")
    print("2. Set up PostgreSQL/TimescaleDB for database storage")
    print("3. Run the pipeline with your own symbols and timeframes")
    print("4. Explore the generated data files and metadata")


if __name__ == "__main__":
    main()
