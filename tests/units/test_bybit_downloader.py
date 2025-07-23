import pytest
import pandas as pd 
from data_etl.providers.crypto.bybit_downloader import BybitDownloader

def test_bybit_downloader():
    downloader = BybitDownloader()
    df = downloader.download_complete_history(
        symbol="BTCUSDT",   
        interval="60",
        start_date="2024-01-01",
        end_date="2025-01-02",
    )
    assert isinstance (df, pd.DataFrame), 'The results are not a empty DataFrame'
    assert not df.empty, 'The results are an empty DataFrame'

    expected_columns = {"open", "high", "low", "close", "volume", "turnover"}
    assert expected_columns.issubset(df.columns), f"Faltan columnas esperadas: {expected_columns - set(df.columns)}"
    






    
