import numpy as np
import pandas as pd

from data_etl.processing.data_splitter import DataSplitter


def make_sample_df(n=100):
    dates = pd.date_range(start='2023-01-01', periods=n, freq='D')
    data = {
        'timestamp': dates,
        'value': np.arange(n)
    }
    return pd.DataFrame(data)

def test_split_random():
    df = make_sample_df(50)
    train, val, test = DataSplitter.split_random(df, test_size=0.2, val_size=0.2, random_state=42)
    assert len(train) + len(val) + len(test) == len(df)
    assert not train.empty and not test.empty

def test_split_time_series():
    df = make_sample_df(60)
    train, val, test = DataSplitter.split_time_series(df, train_ratio=0.5, val_ratio=0.25)
    assert len(train) + len(val) + len(test) == len(df)
    assert train['timestamp'].max() < val['timestamp'].min() or val.empty
    assert val['timestamp'].max() < test['timestamp'].min() or test.empty

def test_split_by_time():
    df = make_sample_df(30)
    train, val, test = DataSplitter.split_by_time(df, train_end='2023-01-10', val_end='2023-01-20')
    assert len(train) + len(val) + len(test) == len(df)
    assert train['timestamp'].max() <= pd.Timestamp('2023-01-10')
    assert val['timestamp'].max() <= pd.Timestamp('2023-01-20') or val.empty
    assert test['timestamp'].min() > pd.Timestamp('2023-01-20') or test.empty

def test_create_sliding_windows():
    arr = np.arange(20).reshape(-1, 1)
    X, y = DataSplitter.create_sliding_windows(arr, window_size=5, target_size=2, stride=1)
    assert X.shape[0] == y.shape[0]
    assert X.shape[1] == 5
    assert y.shape[1] == 2
