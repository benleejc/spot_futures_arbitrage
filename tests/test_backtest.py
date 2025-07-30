import pytest
import pandas as pd
from spot_futures_arbitrage.backtest import simple_backtest


def test_simple_backtest_basic():

    df = pd.DataFrame({
        'datetime': pd.date_range('2024-01-01', periods=4, freq='D'),
        'close': [100, 102, 101, 103],
        'signal': [0, 1, 1, -1]
    })
    result = simple_backtest(df)
    assert 'price_change' in result.columns
    assert 'position' in result.columns
    assert 'pnl' in result.columns
    assert 'cumulative_pnl' in result.columns
    assert len(result) == 4
    # Check that position changes according to signal
    assert list(result['position']) == [0, 1, 1, 0]

def test_simple_backtest_all_flat():

    df = pd.DataFrame({
        'datetime': pd.date_range('2024-01-01', periods=3, freq='D'),
        'close': [100, 101, 102],
        'signal': [0, 0, 0]
    })
    result = simple_backtest(df)
    assert all(result['position'] == 0)
    assert all(result['pnl'] == 0)
    assert all(result['cumulative_pnl'] == 0)

def test_simple_backtest_long_short():

    df = pd.DataFrame({
        'datetime': pd.date_range('2024-01-01', periods=5, freq='D'),
        'close': [100, 105, 103, 108, 107],
        'signal': [0, 1, -1, 1, 0]
    })
    result = simple_backtest(df)
    assert isinstance(result['cumulative_pnl'].iloc[-1], float)
    # Check that position changes as expected
    assert list(result['position']) == [0, 1, 0, 1, 1]

def test_simple_backtest_missing_columns():
    df = pd.DataFrame({'datetime': [1,2,3], 'close': [1,2,3]})
    with pytest.raises(AssertionError):
        simple_backtest(df)

