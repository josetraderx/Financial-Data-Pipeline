"""
Technical indicators calculation module.
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Union

class TechnicalIndicators:
    """Calculates various technical indicators for market data."""
    
    @staticmethod
    def sma(data: pd.Series, period: int) -> pd.Series:
        """Simple Moving Average"""
        return data.rolling(window=period).mean()
    
    @staticmethod
    def ema(data: pd.Series, period: int) -> pd.Series:
        """Exponential Moving Average"""
        return data.ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def rsi(data: pd.Series, period: int = 14) -> pd.Series:
        """Relative Strength Index"""
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    @staticmethod
    def macd(data: pd.Series, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> pd.DataFrame:
        """Moving Average Convergence Divergence"""
        exp1 = data.ewm(span=fast_period, adjust=False).mean()
        exp2 = data.ewm(span=slow_period, adjust=False).mean()
        macd_line = exp1 - exp2
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
        histogram = macd_line - signal_line
        
        return pd.DataFrame({
            'macd': macd_line,
            'signal': signal_line,
            'histogram': histogram
        })
    
    @staticmethod
    def bollinger_bands(data: pd.Series, period: int = 20, num_std: float = 2.0) -> pd.DataFrame:
        """Bollinger Bands"""
        sma = data.rolling(window=period).mean()
        std = data.rolling(window=period).std()
        upper_band = sma + (std * num_std)
        lower_band = sma - (std * num_std)
        
        return pd.DataFrame({
            'middle': sma,
            'upper': upper_band,
            'lower': lower_band
        })
    
    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Average True Range"""
        high_low = high - low
        high_close = (high - close.shift()).abs()
        low_close = (low - close.shift()).abs()
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        return true_range.rolling(window=period).mean()
    
    @staticmethod
    def volume_profile(price: pd.Series, volume: pd.Series, bins: int = 100) -> pd.DataFrame:
        """Volume Profile"""
        hist, bins = np.histogram(price, bins=bins, weights=volume)
        bin_centers = (bins[:-1] + bins[1:]) / 2
        
        return pd.DataFrame({
            'price_level': bin_centers,
            'volume': hist
        })
    
    def calculate_all(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate all basic technical indicators for OHLCV data.
        
        Args:
            df (pd.DataFrame): OHLCV data with columns ['open', 'high', 'low', 'close', 'volume']
            
        Returns:
            pd.DataFrame: Original data with additional technical indicator columns
        """
        result = df.copy()
        
        # Add SMAs
        result['sma_20'] = self.sma(df['close'], 20)
        result['sma_50'] = self.sma(df['close'], 50)
        result['sma_200'] = self.sma(df['close'], 200)
        
        # Add EMAs
        result['ema_12'] = self.ema(df['close'], 12)
        result['ema_26'] = self.ema(df['close'], 26)
        
        # Add RSI
        result['rsi'] = self.rsi(df['close'])
        
        # Add MACD
        macd_data = self.macd(df['close'])
        result['macd'] = macd_data['macd']
        result['macd_signal'] = macd_data['signal']
        result['macd_hist'] = macd_data['histogram']
        
        # Add Bollinger Bands
        bb = self.bollinger_bands(df['close'])
        result['bb_middle'] = bb['middle']
        result['bb_upper'] = bb['upper']
        result['bb_lower'] = bb['lower']
        
        # Add ATR
        result['atr'] = self.atr(df['high'], df['low'], df['close'])
        
        return result
