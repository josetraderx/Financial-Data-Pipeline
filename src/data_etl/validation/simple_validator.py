"""
Validador de datos simplificado para arreglar problemas del pipeline.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)

class SimpleDataValidator:
    """
    Validador simplificado para OHLCV data.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.logger = logger
        
    def validate_and_clean(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Validación y limpieza simplificada de datos OHLCV.
        
        Args:
            df: DataFrame con datos OHLCV
            
        Returns:
            Tuple[pd.DataFrame, dict]: (datos limpios, reporte)
        """
        try:
            if df is None or df.empty:
                return pd.DataFrame(), {
                    'is_valid': False,
                    'total_records': 0,
                    'valid_records': 0,
                    'errors': ['DataFrame is empty or None']
                }
            
            initial_count = len(df)
            self.logger.info(f"Validating {initial_count} records")
            
            # Asegurar que las columnas están en minúsculas
            df.columns = df.columns.str.lower()
            
            # Verificar columnas requeridas
            required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                self.logger.error(f"Missing required columns: {missing_cols}")
                return pd.DataFrame(), {
                    'is_valid': False,
                    'total_records': initial_count,
                    'valid_records': 0,
                    'errors': [f'Missing columns: {missing_cols}']
                }
            
            # Convertir tipos de datos
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convertir timestamp si es necesario
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Eliminar filas con valores nulos
            df_clean = df.dropna()
            
            # Eliminar valores negativos en precios y volumen
            df_clean = df_clean[
                (df_clean['open'] > 0) & 
                (df_clean['high'] > 0) & 
                (df_clean['low'] > 0) & 
                (df_clean['close'] > 0) & 
                (df_clean['volume'] >= 0)
            ]
            
            # Verificar consistencia OHLC (High >= Low, etc.)
            valid_ohlc = (
                (df_clean['high'] >= df_clean['low']) &
                (df_clean['high'] >= df_clean['open']) &
                (df_clean['high'] >= df_clean['close']) &
                (df_clean['low'] <= df_clean['open']) &
                (df_clean['low'] <= df_clean['close'])
            )
            df_clean = df_clean[valid_ohlc]
            
            final_count = len(df_clean)
            
            report = {
                'is_valid': final_count > 0,
                'total_records': initial_count,
                'valid_records': final_count,
                'missing_values': initial_count - len(df.dropna()),
                'outliers_detected': 0,  # Placeholder
                'quality_score': final_count / initial_count if initial_count > 0 else 0,
                'errors': [],
                'warnings': []
            }
            
            if final_count < initial_count:
                report['warnings'].append(f'Removed {initial_count - final_count} invalid records')
            
            self.logger.info(f"Validation complete: {final_count}/{initial_count} valid records")
            
            return df_clean.reset_index(drop=True), report
            
        except Exception as e:
            self.logger.error(f"Validation failed: {e}")
            return pd.DataFrame(), {
                'is_valid': False,
                'total_records': len(df) if df is not None else 0,
                'valid_records': 0,
                'errors': [str(e)]
            }
