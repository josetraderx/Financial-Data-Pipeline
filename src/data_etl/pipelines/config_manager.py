"""
Configuration management for Exodus v2025 data pipelines.
"""

import os
import json
from typing import Dict, Any
from datetime import datetime, timedelta
from pathlib import Path


class PipelineConfig:
    """
    Configuration manager for data pipelines.
    """
    
    def __init__(self, config_file: str = None):
        """
        Initialize configuration manager.
        
        Args:
            config_file: Path to configuration file (optional)
        """
        self.config_file = config_file
        self.config = self._load_default_config()
        
        if config_file and os.path.exists(config_file):
            self._load_from_file(config_file)
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default configuration."""
        return {
            'data_dir': 'data/processed',
            'db_config': {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', 5432)),
                'database': os.getenv('DB_NAME', 'exodus_data'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'your_password')
            },
            'providers': {
                'bybit': {
                    'api_key': os.getenv('BYBIT_API_KEY', 'your_api_key'),
                    'api_secret': os.getenv('BYBIT_API_SECRET', 'your_api_secret'),
                    'testnet': os.getenv('BYBIT_TESTNET', 'true').lower() == 'true'
                }
            },
            'validation_config': {
                'handle_missing': 'interpolate',
                'outlier_method': 'iqr',
                'outlier_threshold': 1.5,
                'min_records': 100,
                'max_gap_hours': 24
            },
            'split_config': {
                'train_test_split': {
                    'test_size': 0.2,
                    'method': 'chronological'
                }
            },
            'storage_config': {
                'save_files': True,
                'store_db': True,
                'file_format': 'parquet',
                'compression': 'snappy'
            }
        }
    
    def _load_from_file(self, config_file: str) -> None:
        """Load configuration from file."""
        try:
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                self._merge_config(file_config)
        except Exception as e:
            print(f"Warning: Failed to load config file {config_file}: {e}")
    
    def _merge_config(self, new_config: Dict[str, Any]) -> None:
        """Merge new configuration with existing one."""
        def merge_dict(base: Dict[str, Any], update: Dict[str, Any]) -> None:
            for key, value in update.items():
                if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                    merge_dict(base[key], value)
                else:
                    base[key] = value
        
        merge_dict(self.config, new_config)
    
    def get(self, key: str = None) -> Any:
        """
        Get configuration value.
        
        Args:
            key: Configuration key (dot notation supported)
            
        Returns:
            Configuration value
        """
        if key is None:
            return self.config
        
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return None
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value.
        
        Args:
            key: Configuration key (dot notation supported)
            value: Value to set
        """
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def save(self, config_file: str = None) -> None:
        """
        Save configuration to file.
        
        Args:
            config_file: Path to save configuration (optional)
        """
        file_path = config_file or self.config_file
        
        if file_path is None:
            raise ValueError("No configuration file specified")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w') as f:
            json.dump(self.config, f, indent=2, default=str)
    
    def create_pipeline_config(self, provider: str, symbol: str, timeframe: str,
                              days_back: int = 30, **kwargs) -> Dict[str, Any]:
        """
        Create a pipeline configuration for a specific symbol.
        
        Args:
            provider: Data provider name
            symbol: Trading symbol
            timeframe: Timeframe
            days_back: Number of days to look back
            **kwargs: Additional configuration options
            
        Returns:
            Pipeline configuration dictionary
        """
        config = {
            'provider': provider,
            'symbol': symbol,
            'timeframe': timeframe,
            'start_date': datetime.now() - timedelta(days=days_back),
            'end_date': datetime.now(),
            'splits': self.get('split_config') or {},
            'save_files': self.get('storage_config.save_files'),
            'store_db': self.get('storage_config.store_db')
        }
        
        # Merge any additional kwargs
        config.update(kwargs)
        
        return config
    
    def validate_config(self) -> Dict[str, Any]:
        """
        Validate configuration and return issues.
        
        Returns:
            Dictionary with validation results
        """
        issues = {
            'errors': [],
            'warnings': [],
            'is_valid': True
        }
        
        # Check database configuration
        db_config = self.get('db_config')
        if not db_config:
            issues['errors'].append("Database configuration missing")
            issues['is_valid'] = False
        else:
            required_db_fields = ['host', 'port', 'database', 'user', 'password']
            for field in required_db_fields:
                if not db_config.get(field):
                    issues['errors'].append(f"Database field '{field}' missing")
                    issues['is_valid'] = False
        
        # Check provider configurations
        providers = self.get('providers')
        if not providers:
            issues['warnings'].append("No data providers configured")
        else:
            for provider_name, provider_config in providers.items():
                if provider_name == 'bybit':
                    if not provider_config.get('api_key') or provider_config.get('api_key') == 'your_api_key':
                        issues['warnings'].append(f"Bybit API key not configured")
                    if not provider_config.get('api_secret') or provider_config.get('api_secret') == 'your_api_secret':
                        issues['warnings'].append(f"Bybit API secret not configured")
        
        # Check data directory
        data_dir = self.get('data_dir')
        if data_dir:
            data_path = Path(data_dir)
            if not data_path.exists():
                try:
                    data_path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    issues['errors'].append(f"Cannot create data directory: {e}")
                    issues['is_valid'] = False
        
        return issues


# Default configuration instance
default_config = PipelineConfig()


def create_example_config(config_file: str = 'config/pipeline_config.json') -> None:
    """
    Create an example configuration file.
    
    Args:
        config_file: Path to create configuration file
    """
    config = PipelineConfig()
    
    # Set some example values
    config.set('providers.bybit.api_key', 'your_bybit_api_key_here')
    config.set('providers.bybit.api_secret', 'your_bybit_api_secret_here')
    config.set('providers.bybit.testnet', True)
    
    config.set('db_config.host', 'localhost')
    config.set('db_config.database', 'exodus_data')
    config.set('db_config.user', 'postgres')
    config.set('db_config.password', 'your_password_here')
    
    config.set('data_dir', 'data/processed')
    
    # Add some comments as additional metadata
    config.set('_comments', {
        'providers': 'Configure your data provider API credentials here',
        'db_config': 'PostgreSQL/TimescaleDB connection parameters',
        'data_dir': 'Directory for storing processed data files',
        'validation_config': 'Data validation and cleaning parameters',
        'split_config': 'Data splitting configurations for train/test sets'
    })
    
    config.save(config_file)
    print(f"Example configuration saved to {config_file}")


if __name__ == "__main__":
    # Create example configuration
    create_example_config()
    
    # Validate configuration
    config = PipelineConfig()
    validation_result = config.validate_config()
    
    print("Configuration validation:")
    print(f"Valid: {validation_result['is_valid']}")
    if validation_result['errors']:
        print(f"Errors: {validation_result['errors']}")
    if validation_result['warnings']:
        print(f"Warnings: {validation_result['warnings']}")
