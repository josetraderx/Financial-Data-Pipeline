from unittest.mock import patch

import pandas as pd

from data_etl.providers.crypto.bybit_downloader import BybitDownloader


@patch("data_etl.providers.crypto.bybit_downloader.requests.get")
def test_bybit_downloader(mock_get):
    # Simula respuesta exitosa de la API de Bybit incluyendo retCode
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "retCode": 0,  # <-- Este campo es necesario
        "result": {
            "list": [["1704067200000", "50000", "50100", "49950", "50050", "100", "10"]]
        },
    }

    downloader = BybitDownloader()
    df = downloader.download_complete_history(
        symbol="BTCUSDT",
        interval="60",
        start_date="2024-01-01",
        end_date="2025-01-02",
    )
    assert isinstance(df, pd.DataFrame), "El resultado no es un DataFrame"
    assert not df.empty, "El DataFrame está vacío"

    expected_columns = {"open", "high", "low", "close", "volume", "turnover"}
    assert expected_columns.issubset(
        df.columns
    ), f"Faltan columnas esperadas: {expected_columns - set(df.columns)}"
