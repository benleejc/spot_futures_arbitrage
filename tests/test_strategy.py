import pytest
import pandas as pd
from datetime import datetime, timedelta
from spot_futures_arbitrage import strategy

def test_carry_futures():
    result = strategy.carry(fut_price=110, spot_price=100, days_to_expiry=10)
    expected = ((110-100)/100) * (365/10)
    assert pytest.approx(result, 0.0001) == expected

def test_carry_perpetual():
    result = strategy.carry(fut_price=110, spot_price=100, funding_rate=0.01, perpetual=True)
    expected = ((110-100)/100) + (0.01 * strategy.FUNDING_INTERVALS_PER_YEAR)
    assert pytest.approx(result, 0.0001) == expected

def test_carry_zero():
    assert strategy.carry(fut_price=100, spot_price=100) == 0

def test_evaluate_trade_perpetual():
    symbol = 'BTC/USDT:USDT'
    spot_price = 100
    fut_price = 110
    result = strategy.evaluate_trade(symbol, spot_price, fut_price)
    expected = strategy.carry(fut_price, spot_price, funding_rate=strategy.FUNDING_RATES[symbol], perpetual=True)
    assert pytest.approx(result, 0.0001) == expected

def test_evaluate_trade_futures():
    symbol = 'FOO/BAR:BAR'
    spot_price = 100
    fut_price = 110
    days_to_expiry = 10
    result = strategy.evaluate_trade(symbol, spot_price, fut_price, days_to_expiry)
    expected = strategy.carry(fut_price, spot_price, days_to_expiry=days_to_expiry, perpetual=False)
    assert pytest.approx(result, 0.0001) == expected

def test_resample_prices():
    df = pd.DataFrame({
        'datetime': [datetime(2024,1,1,0,0), datetime(2024,1,1,0,2), datetime(2024,1,1,0,5)],
        'symbol': ['BTC/USDT:USDT']*3,
        'close': [100, 101, 102]
    })
    out = strategy.resample_prices(df, freq='5min')
    assert 'datetime' in out.columns
    assert 'symbol' in out.columns
    assert len(out) > 0

def test_symbol_filter():
    df = pd.DataFrame({
        'symbol': ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'FOO/BAR:BAR'],
        'close': [1,2,3]
    })
    filtered = strategy.symbol_filter(df, 'BTC', 'USDT')
    assert all(filtered['symbol'].str.startswith('BTC/USDT'))

def test_generate_signals():
    def dummy_strategy(base, quote, timeframe, **kwargs):
        return pd.DataFrame({'signal': [0.1, -0.1, 0.0]})
    df = strategy.generate_signals(dummy_strategy, 'BTC', 'USDT', signal_col='signal', threshold=0.05)
    assert list(df['signal']) == [1, -1, 0]

def test_calculate_carry(monkeypatch):
    # Patch get_historical_prices to return a DataFrame
    data = {
        'datetime': [datetime(2024,1,1,0,0), datetime(2024,1,1,0,0)],
        'symbol': ['BTC/USDT:USDT', 'BTC/USDT:USDT'],
        'futures': [0, 1],
        'close': [100, 110],
        'expiration_date': [datetime(2024,1,11), datetime(2024,1,11)]
    }
    monkeypatch.setattr(strategy, "get_historical_prices", lambda: pd.DataFrame(data))
    df = strategy.calculate_carry('BTC', 'USDT')
    assert 'carry' in df.columns
    assert len(df) == 1

def test_carry_strategy(monkeypatch):
    # Patch calculate_carry to return a DataFrame
    data = {
        'datetime': [datetime(2024,1,1,0,0)],
        'symbol': ['BTC/USDT:USDT'],
        'close': [110],
        'carry': [0.1],
        'signal': [1],
        'days_to_expiry': [10],
        'expiration_date': [datetime(2024,1,11)],
        'close_spot': [100]
    }
    monkeypatch.setattr(strategy, "calculate_carry", lambda *a, **kw: pd.DataFrame(data))
    df = strategy.carry_strategy('BTC', 'USDT')
    assert 'signal' in df.columns
    assert 'symbol' in df.columns