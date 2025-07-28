import ccxt
import sqlite3
import time
from datetime import datetime
import logging
from spot_futures_arbitrage.db.db_config import DB_PATH

logger = logging.getLogger('okx_prices')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

spot = ccxt.okx({
    'options': {'defaultType': 'spot'},
    'enableRateLimit': True,
})
fut = ccxt.okx({
    'options': {'defaultType': 'swap'},  # or 'futures' depending your target
    'enableRateLimit': True,
})
spot.load_markets()
fut.load_markets()

def find_symbol(exchange: ccxt.Exchange, base: str, quote: str) -> list[str]:
    """Find a spot symbol for the given base and quote."""
    for sym, m in exchange.markets.items():
        if base in sym and quote in sym and (not m.get('contract', False)):
            return [sym]
    return []

def find_future_symbols(exchange: ccxt.Exchange, base: str, quote: str) -> list[str]:
    """Find future symbols for the given base and quote."""
    lst = []
    for sym, m in exchange.markets.items():
        if base in sym and quote in sym and m.get('contract', False):
            lst.append(sym)
    return lst

SYM_SPOT_BTC = find_symbol(spot, 'BTC', 'USDT')
SYM_FUT_BTC  = find_future_symbols(fut, 'BTC', 'USDT')
SYM_SPOT_ETH = find_symbol(spot, 'ETH', 'USDT')
SYM_FUT_ETH  = find_future_symbols(fut, 'ETH', 'USDT')


def fetch_and_store(exchange: ccxt.Exchange, symbols: list[str], futures: int = 0):
    """Fetch ticker data for the given symbols and store in the database."""
    print(DB_PATH)
    for symbol in symbols:
        if futures:
            expiration_date = exchange.markets[symbol].get('expiry', None)

        try:
            ticker = exchange.fetch_ticker(symbol)
            ts = ticker['timestamp'] or int(time.time() * 1000)
            dt = ticker['datetime'] or datetime.fromtimestamp(ts/1000).isoformat()
            if futures:
                expiration_date = datetime.fromtimestamp(expiration_date / 1000).isoformat() if expiration_date else None
            else:
                expiration_date = None
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (ts,
                           dt,
                           symbol,
                           ticker.get('last'),
                           ticker.get('bid'),
                           ticker.get('ask'),
                           ticker.get('high'),
                           ticker.get('low'),
                           futures,
                           expiration_date
                           ))
                conn.commit()
            logger.info(f"[{dt}] {symbol}: last={ticker.get('last')}, bid={ticker.get('bid')}, ask={ticker.get('ask')}")
        except Exception as e:
            logger.error(f"Error fetching  {symbol}: {e}")

def main():
    while True:
        now = time.time()
        fetch_and_store(spot,  SYM_SPOT_BTC)
        fetch_and_store(spot,  SYM_SPOT_ETH)
        fetch_and_store(fut,  SYM_FUT_BTC, 1)
        fetch_and_store(fut,  SYM_FUT_ETH, 1)
        sleep_secs = 60 - (time.time() - now)
        if sleep_secs > 0:
            time.sleep(sleep_secs)

if __name__ == '__main__':
    main()
