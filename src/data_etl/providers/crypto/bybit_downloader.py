import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from data_etl.processing.enhanced_metadata_manager import EnhancedMetadataManager

# Logging setup adaptado para Exodus v2025
try:
    from data_etl.utils.logging import get_logger

    logger = get_logger(__name__)
except ImportError:
    # Fallback si no encuentra el módulo
    LOG_DIR = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "logs"
    )
    os.makedirs(LOG_DIR, exist_ok=True)
    LOG_FILE = os.path.join(LOG_DIR, "bybit_data.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
    )
    logger = logging.getLogger(__name__)


class BybitDownloader:
    def __init__(self, api_key=None, api_secret=None, testnet=False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.base_url = "https://api.bybit.com"
        self.timeframes = ["1", "5", "15", "60", "240"]  # 1min, 5min, 15min, 1h, 4h
        self.symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]  # Default symbols
        self.metadata_manager = EnhancedMetadataManager()

    def get_kline_data(
        self, symbol: str, interval: str, start_time: int, limit: int = 1000
    ) -> list[dict]:
        """
        Get kline data from Bybit

        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            interval: Time interval in minutes ('1', '5', '15', '60', '240')
            start_time: Initial timestamp in milliseconds
            limit: Maximum number of candles to get (max 1000)

        Returns:
            Lista de velas OHLCV
        """
        endpoint = f"{self.base_url}/v5/market/kline"
        params = {
            "category": "spot",
            "symbol": symbol,
            "interval": interval,
            "start": start_time,
            "limit": limit,
        }

        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            if data["retCode"] == 0 and "result" in data:
                return data["result"]["list"]
            else:
                logger.error(f"Error en la respuesta de Bybit: {data}")
                return []

        except Exception as e:
            logger.error(f"Error al obtener datos de Bybit: {str(e)}")
            return []

    def download_complete_history(
        self, symbol: str, interval: str, start_date=None, end_date=None
    ) -> pd.DataFrame:
        """
        Descarga el historial completo para un símbolo y timeframe entre start_date y end_date.
        Args:
            symbol: Par de trading
            interval: Intervalo de tiempo
            start_date: Fecha de inicio (str 'YYYY-MM-DD' o datetime)
            end_date: Fecha de fin (str 'YYYY-MM-DD' o datetime)
        Returns:
            DataFrame con datos históricos
        """
        # Convertir fechas a datetime
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        if start_date is None:
            start_date = datetime.now() - timedelta(days=30)
        if end_date is None:
            end_date = datetime.now()
        start_time = int(start_date.timestamp() * 1000)
        end_time = int(end_date.timestamp() * 1000)

        all_candles = []
        current_start = start_time

        while current_start < end_time:
            candles = self.get_kline_data(symbol, interval, current_start)
            if not candles:
                break
            all_candles.extend(candles)
            last_timestamp = int(candles[-1][0])
            if last_timestamp <= current_start:
                break
            current_start = last_timestamp
            # Rate limiting
            time.sleep(0.1)
            # Si la última vela supera end_time, cortamos
            if last_timestamp >= end_time:
                break

        # Convertir a DataFrame
        if all_candles:
            df = pd.DataFrame(
                all_candles,
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "turnover",
                ],
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.set_index("timestamp", inplace=True)
            for col in ["open", "high", "low", "close", "volume", "turnover"]:
                df[col] = df[col].astype(float)
            # Filtrar por rango exacto
            df = df[(df.index >= start_date) & (df.index <= end_date)]
            df.reset_index(inplace=True)  # Asegura que 'timestamp' sea columna
            return df
        return pd.DataFrame()

    def save_data(self, df: pd.DataFrame, symbol: str, interval: str):
        """Save data to CSV and generate metadata"""
        if df.empty:
            logger.warning(f"No data to save - {symbol} {interval}")
            return

        filename = f"{symbol.lower()}_{interval}m.csv"
        # Save CSV in the raw folder (adaptado para Exodus v2025)
        raw_dir = os.path.join("data", "raw")
        os.makedirs(raw_dir, exist_ok=True)
        csv_path = os.path.join(raw_dir, filename)
        df.to_csv(csv_path)
        logger.info(f"Data saved to {csv_path} - Records: {len(df)}")

        # Save metadata usando el gestor avanzado
        self.metadata_manager.save_metadata(
            csv_path=csv_path,
            symbol=symbol,
            interval=interval,
            num_records=len(df),
            script_version="1.0.0",  # Versión de tu script
            data_source="bybit",
            df=df,
        )

        # TODO: Store data in TimescaleDB (comentado hasta que tengamos tsdb_manager)
        # write_ohlcv_dataframe(df, symbol, interval)

    def download_all_data(self, symbols: list[str] | None = None, days_back: int = 30):
        """Download data for all symbols and timeframes"""
        if symbols:
            self.symbols = symbols

        for symbol in self.symbols:
            logger.info(f"Descargando datos para {symbol}")

            for interval in self.timeframes:
                logger.info(f"Timeframe: {interval}min")

                # Convertir intervalo a minutos para el nombre del archivo
                interval_map = {
                    "1": "1",
                    "5": "5",
                    "15": "15",
                    "60": "60",
                    "240": "240",
                }

                df = self.download_complete_history(symbol, interval, days_back)
                self.save_data(df, symbol, interval_map[interval])


def main():
    downloader = BybitDownloader()

    # List of symbols to download
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "AVAXUSDT"]

    # Download 60 days of data
    downloader.download_all_data(symbols=symbols, days_back=60)


if __name__ == "__main__":
    main()
