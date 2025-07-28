import sqlite3
import time
import pytest
import ccxt
from datetime import datetime
import logging

import spot_futures_arbitrage
from spot_futures_arbitrage.db.data_scraper import fetch_and_store, find_symbol, find_future_symbols

@pytest.fixture
def in_memory_db(monkeypatch, tmp_path):
    db_path = tmp_path / "test_prices.db"
    monkeypatch.setattr(spot_futures_arbitrage.db.data_scraper, "DB_PATH", str(db_path))

    # Create DB schema
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE prices (
            timestamp INTEGER,
            datetime TEXT,
            symbol TEXT,
            last REAL,
            bid REAL,
            ask REAL,
            high REAL,
            low REAL,
            futures INTEGER,
            expiry TEXT
        )
    """)
    conn.commit()
    conn.close()
    return db_path

class DummyExchange:
    def __init__(self, markets, tickers=None):
        self.markets = markets
        self.tickers = tickers
    def fetch_ticker(self, symbol):
        return self.tickers


def test_find_symbol_spot_match():
    markets = {
        'BTC/USDT': {'contract': False},
        'ETH/USDT': {'contract': False},
    }
    exchange = DummyExchange(markets)
    result = find_symbol(exchange, 'BTC', 'USDT')
    assert result == ['BTC/USDT']

def test_find_symbol_no_match():
    markets = {
        'BTC/USD': {'contract': False},
        'ETH/USDT': {'contract': False},
    }
    exchange = DummyExchange(markets)
    result = find_symbol(exchange, 'XRP', 'USDT')
    assert result == []

def test_find_symbol_ignores_contract_markets():
    markets = {
        'BTC/USDT:USDT': {'contract': True},  # futures
        'BTC/USDT': {'contract': False},      # spot
    }
    exchange = DummyExchange(markets)
    result = find_symbol(exchange, 'BTC', 'USDT')
    assert result == ['BTC/USDT']

def test_find_symbol_prefers_first_match():
    markets = {
        'ETH/USDT': {'contract': False},
        'ETH/USDT2': {'contract': False},
    }
    exchange = DummyExchange(markets)
    result = find_symbol(exchange, 'ETH', 'USDT')
    assert result[0] == 'ETH/USDT'

def test_finds_single_future_symbol():
    markets = {
        'BTC/USDT:USDT': {'contract': True},
        'BTC/USDT': {'contract': False},
    }
    exchange = DummyExchange(markets)
    result = find_future_symbols(exchange, 'BTC', 'USDT')
    assert result == ['BTC/USDT:USDT']

def test_finds_multiple_futures():
    markets = {
        'BTC/USDT:USDT': {'contract': True},
        'BTC/USDT-240927': {'contract': True},
        'ETH/USDT:USDT': {'contract': True},
    }
    exchange = DummyExchange(markets)
    result = find_future_symbols(exchange, 'BTC', 'USDT')
    assert sorted(result) == sorted(['BTC/USDT:USDT', 'BTC/USDT-240927'])

def test_filters_out_spot_markets():
    markets = {
        'BTC/USDT': {'contract': False},
        'ETH/USDT': {'contract': False},
    }
    exchange = DummyExchange(markets)
    result = find_future_symbols(exchange, 'BTC', 'USDT')
    assert result == []

def test_no_match_returns_empty():
    markets = {
        'BTC/USD:USD': {'contract': True},
        'ETH/USDC:USDC': {'contract': True},
    }
    exchange = DummyExchange(markets)
    result = find_future_symbols(exchange, 'XRP', 'USDT')
    assert result == []

def test_fetch_and_store_spot(in_memory_db, caplog):
    ts = 1600000000000
    iso = datetime.fromtimestamp(ts / 1000).isoformat()
    ticker = {
        "timestamp": ts,
        "datetime": iso,
        "last": 100,
        "bid": 99,
        "ask": 101,
        "high": 105,
        "low": 90,
    }

    exchange = DummyExchange({"BTC/USDT": {"contract": False}}, ticker)
    caplog.set_level(logging.INFO)

    fetch_and_store(exchange, ["BTC/USDT"], futures=0)

    conn = sqlite3.connect(in_memory_db)
    rows = conn.execute("SELECT * FROM prices").fetchall()
    print(rows)
    assert len(rows) == 1
    row = rows[0]
    assert row[2] == "BTC/USDT"
    assert row[8] == 0
    assert row[9] is None  # no expiry
    assert "BTC/USDT" in caplog.text
    conn.close()


def test_fetch_and_store_future_with_expiry(in_memory_db):
    ts = 1600000000000
    expiry_ts = 1600010000000
    iso = datetime.fromtimestamp(ts / 1000).isoformat()
    expiry_iso = datetime.fromtimestamp(expiry_ts / 1000).isoformat()
    ticker = {
        "timestamp": ts,
        "datetime": iso,
        "last": 200,
        "bid": 199,
        "ask": 201,
        "high": 210,
        "low": 190,
    }

    markets = {"BTC/USDT-FUT": {"contract": True, "expiry": expiry_ts}}
    exchange = DummyExchange(markets, ticker)

    fetch_and_store(exchange, ["BTC/USDT-FUT"], futures=1)

    conn = sqlite3.connect(in_memory_db)
    row = conn.execute("SELECT * FROM prices").fetchone()
    assert row[2] == "BTC/USDT-FUT"
    assert row[8] == 1
    assert row[9] == expiry_iso
    conn.close()