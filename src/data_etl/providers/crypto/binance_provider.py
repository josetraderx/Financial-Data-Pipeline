"""
Binance data provider for Exodus v2025.
Handles data download from Binance API.
"""

import logging
import time
from datetime import datetime

import numpy as np
import pandas as pd
import pytz
import requests

from data_etl.utils.logging import get_logger


class BinanceDataProvider:
    """
    Downloads historical and real-time data from Binance.
    """

    def __init__(self, logger: logging.Logger | None = None, test_mode: bool = False):
        """
        Initialize Binance data provider.

        Args:
            logger: Optional logger instance
            test_mode: Whether to use test data instead of real API calls
        """
        self.logger = logger or get_logger(__name__)
        self.base_url = "https://api.binance.com/api/v3"
        self.rate_limit_wait = 1.1  # segundos entre llamadas
        self.test_mode = test_mode

    def download_historical_data(
        self, symbol: str, start_date: datetime, end_date: datetime, interval: str = "1h"
    ) -> pd.DataFrame:
        """
        Download historical OHLCV data from Binance.

        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            start_date: Start date
            end_date: End date
            interval: Kline interval

        Returns:
            pd.DataFrame: OHLCV data
        """
        if self.test_mode:
            return self._get_test_data(symbol, start_date, end_date, interval)

        try:
            # 1. Preparar parámetros
            start_ts = int(start_date.timestamp() * 1000)
            end_ts = int(end_date.timestamp() * 1000)

            # 2. Construir URL
            endpoint = f"{self.base_url}/klines"
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": start_ts,
                "endTime": end_ts,
                "limit": 1000,  # máximo por request
            }

            all_data = []

            while start_ts < end_ts:
                # 3. Hacer request
                response = requests.get(endpoint, params=params)

                if response.status_code != 200:
                    self.logger.error(f"API request failed: {response.text}")
                    return pd.DataFrame()

                # 4. Procesar datos
                data = response.json()
                if not data:
                    break

                all_data.extend(data)

                # 5. Actualizar para siguiente request
                start_ts = data[-1][0] + 1
                params["startTime"] = start_ts

                # 6. Respetar rate limits
                time.sleep(self.rate_limit_wait)

            if not all_data:
                self.logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()

            # 7. Convertir a DataFrame
            df = pd.DataFrame(
                all_data,
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_volume",
                    "trades",
                    "taker_buy_volume",
                    "taker_buy_quote_volume",
                    "ignore",
                ],
            )

            # 8. Limpiar y formatear datos
            df = df[["timestamp", "open", "high", "low", "close", "volume"]]
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df[["open", "high", "low", "close", "volume"]] = df[
                ["open", "high", "low", "close", "volume"]
            ].astype(float)
            df["symbol"] = symbol

            self.logger.info(f"Downloaded {len(df)} rows for {symbol}")
            return df

        except Exception as e:
            self.logger.error(f"Failed to download data: {str(e)}")
            return pd.DataFrame()

    def _get_test_data(
        self, symbol: str, start_date: datetime, end_date: datetime, interval: str = "1h"
    ) -> pd.DataFrame:
        """
        Generate test data for testing purposes.

        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            start_date: Start date
            end_date: End date
            interval: Kline interval

        Returns:
            pd.DataFrame: Test OHLCV data
        """
        # Generate dates
        if interval == "1h":
            dates = pd.date_range(start=start_date, end=end_date, freq="H", tz=pytz.UTC)
        else:  # Default to 1m
            dates = pd.date_range(start=start_date, end=end_date, freq="min", tz=pytz.UTC)

        # Generate mock price data with some randomness but realistic movement
        base_price = 30000.0  # Starting price for BTC
        if symbol == "ETHUSDT":
            base_price = 2000.0

        n = len(dates)
        # Generate random walk prices
        changes = np.random.normal(0, 0.001, n).cumsum()
        closes = base_price * (1 + changes)

        # Generate OHLCV data
        data = pd.DataFrame(
            {
                "timestamp": dates,
                "symbol": symbol,
                "open": closes * (1 + np.random.normal(0, 0.0002, n)),
                "high": closes * (1 + abs(np.random.normal(0, 0.0005, n))),
                "low": closes * (1 - abs(np.random.normal(0, 0.0005, n))),
                "close": closes,
                "volume": abs(np.random.normal(100, 30, n)),
            }
        )

        # Add time components
        data["hour"] = data["timestamp"].dt.hour
        data["day_of_week"] = data["timestamp"].dt.dayofweek
        data["month"] = data["timestamp"].dt.month
        data["year"] = data["timestamp"].dt.year

        return data

    def check_symbol_status(self, symbol: str) -> bool:
        """
        Check if a symbol is currently trading on Binance.

        Args:
            symbol: Trading pair to check

        Returns:
            bool: True if symbol is valid and trading
        """
        try:
            endpoint = f"{self.base_url}/exchangeInfo"
            response = requests.get(endpoint)

            if response.status_code != 200:
                self.logger.error(f"Failed to get exchange info: {response.text}")
                return False

            data = response.json()
            symbols = {s["symbol"]: s["status"] for s in data["symbols"]}

            if symbol not in symbols:
                self.logger.warning(f"Symbol {symbol} not found on Binance")
                return False

            is_trading = symbols[symbol] == "TRADING"
            if not is_trading:
                self.logger.warning(f"Symbol {symbol} is not currently trading")

            return is_trading

        except Exception as e:
            self.logger.error(f"Failed to check symbol status: {str(e)}")
            return False
