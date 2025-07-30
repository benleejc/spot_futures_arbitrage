from spot_futures_arbitrage.db.db_config import DB_PATH
import sqlite3
from datetime import timedelta
import pandas as pd


# hardcoded or pulled from API
FUNDING_RATES = {
    'BTC/USDT:USDT': 0.0005,   # 0.05% per funding interval
    'ETH/USDT:USDT': -0.0003,  # -0.03%
}

FUNDING_INTERVALS_PER_YEAR = 3 * 365  # 3 funding periods per day



def carry(
        fut_price: float, 
        spot_price: float, 
        funding_rate: float = None, 
        days_to_expiry: int = None, 
        perpetual: bool = False
    ) -> float:
    """Calculate the carry of a futures contract over spot price.
    
    Note that this is a simplified calculation and does not take into account lending out the shorted asset.
    
    :param fut_price: Futures price.
    :param spot_price: Spot price.
    :param funding_rate: Funding rate for perpetual contracts.
    :param days_to_expiry: Days to expiry for futures contracts.
    :param perpetual: Whether the contract is a perpetual contract.
    :return: Annualized carry rate.
    """
    if days_to_expiry is not None and days_to_expiry > 0:
        raw_carry = (fut_price - spot_price) / spot_price
        annualized_carry = raw_carry * (365 / days_to_expiry) 
        return annualized_carry
    elif perpetual:
        funding_annualized = funding_rate * FUNDING_INTERVALS_PER_YEAR 
        raw_carry = (fut_price - spot_price) / spot_price 
        return raw_carry + funding_annualized
    else:
        return 0
    
def evaluate_trade(symbol, spot_price, fut_price, days_to_expiry=None) -> float:
    """Evaluate the carry of a trade based on futures and spot prices.

    :param symbol: Trading pair symbol.
    :param spot_price: Current spot price.
    :param fut_price: Current futures price.
    :param days_to_expiry: Days to expiry for futures contracts.
    :return: Carry value for the trade.
    """
    if symbol in FUNDING_RATES:
        rate = FUNDING_RATES[symbol]
        result = carry(
            fut_price=fut_price,
            spot_price=spot_price,
            funding_rate=rate,
            perpetual=True
        )
    else:
        result = carry(
            fut_price=fut_price,
            spot_price=spot_price,
            days_to_expiry=days_to_expiry,
            perpetual=False
        )
    return result

def get_prices():
    """Fetch the latest prices from the database."""
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query("SELECT * FROM prices", conn)
        return df
    
def get_historical_prices():
    """Fetch historical prices from the database."""
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query("SELECT * FROM historical_prices", conn)
        return df
    
def resample_prices(df: pd.DataFrame, freq: str = '5min') -> pd.DataFrame:
    """Resample prices to a specified frequency.
    
    :param df: DataFrame with 'datetime' and 'symbol' columns.
    :param freq: Resampling frequency, e.g., '5min', '1H'
    :return: Resampled DataFrame.
    """

    assert 'datetime' in df.columns, "DataFrame must contain 'datetime' column"
    assert 'symbol' in df.columns, "DataFrame must contain 'symbol' column"
    if df.empty:
        return df
    df = df.set_index(pd.to_datetime(df['datetime'])).drop(columns='datetime')
    df = (
        df.groupby('symbol')
          .resample(freq)
          .last()  # or use .ohlc(), .mean(), etc.
          .drop(columns='symbol')  # optional cleanup
          .reset_index()
    )
    return df

def symbol_filter(df: pd.DataFrame, base: str, quote: str) -> pd.DataFrame:
    """Filter DataFrame for specific base and quote symbols.

    Helper function to filter dataframe based on base and quote currencies.
    
    :param df: DataFrame with 'symbol' column.
    :param base: Base currency.
    :param quote: Quote currency.
    :return: Filtered DataFrame.
    """
    assert 'symbol' in df.columns, "DataFrame must contain 'symbol' column"
    if df.empty:
        return df
    return df[df['symbol'].str.startswith(f"{base}/{quote}")]


def calculate_carry(base: str, quote: str, timeframe: str='5min', signal_col='signal') -> pd.DataFrame:
    """Calculate carry signal using the latest data from the DB.

    The carry is defined as the difference between futures and spot prices, adjusted for funding rates (perpetuals) and time to expiry.
    we pull data from the database, resample it and calculate the carry for each futures contract.
    
    :param base: Base currency.
    :param quote: Quote currency.
    :param timeframe: Timeframe for resampling.
    :param signal_col: Column name for the carry signal.
    :return: DataFrame with carry signals.
    """
    records = get_historical_prices()
    df = resample_prices(records, freq=timeframe)

    # Filter for symbol 
    filtered_df = symbol_filter(df , base, quote)

    spot = filtered_df[filtered_df['futures'] == 0]
    futs = filtered_df[filtered_df['futures'] == 1]

    futs = futs.merge(spot[['datetime', 'close']], on='datetime', suffixes=('', '_spot'), how='left')
    futs['expiration_date'] = pd.to_datetime(futs['expiration_date'])
    futs['days_to_expiry'] = (futs['expiration_date'].dt.date - futs['datetime'].dt.date).apply(lambda x: x.days if isinstance(x, timedelta) else None)

    futs['carry'] = futs.apply(lambda row: evaluate_trade(row['symbol'], row['close_spot'], row['close'], row['days_to_expiry']), axis=1)
    futs[signal_col] = futs['carry']
    return futs


def generate_signals(f, base: str, quote: str, signal_col: str='signal', timeframe: str='5min', threshold: float=0.05, **kwargs) -> pd.DataFrame:
    """Generate trading signals based on strategy function.

    The function f should return a DataFrame with a signal column. 
    generate_signals will apply a threshold to the signal column to determine long, short, or flat positions.
    if the signal is above the threshold, it will be a long position (1), if below -threshold, a short position (-1), 
    and in between, it will be flat (0).

    
    :param f: Strategy function that takes base, quote, timeframe, and other kwargs.
    :param base: Base currency.
    :param quote: Quote currency.
    :param signal_col: Column name for the signal.
    :param timeframe: Timeframe for the strategy.
    :param threshold: Threshold for generating signals.
    :param kwargs: Additional arguments for the strategy function.
    :return: DataFrame with signals.
    """
    df = f(base=base, quote=quote, timeframe=timeframe, **kwargs)
    df['signal'] = df[signal_col].apply(lambda x: 1 if x > threshold else (-1  if x < -threshold else 0)) 
    return df

def carry_strategy(base: str, quote: str, timeframe: str='5min', signal_col:str='signal', threshold: float=0.05, **kwargs) -> pd.DataFrame:
    """Carry strategy that generates signals based on carry calculations.

    Carry is defined as the difference between futures and spot prices, adjusted for funding rates (perpetuals) and time to expiry.

    .. math::
        carry = \frac{FuturesPrice - SpotPrice}{SpotPrice} + FundingRate
    
    :param base: Base currency.
    :param quote: Quote currency.
    :param timeframe: Timeframe for the strategy.
    :param signal_col: Column name for the carry signal.
    :return: DataFrame with carry signals.
    """
    df = generate_signals(calculate_carry, base, quote, signal_col=signal_col, timeframe=timeframe, threshold=threshold, **kwargs)
    df['signal_spot'] = df[signal_col]
    df[signal_col] = df[signal_col] * -1
    df['symbol_spot'] = df['symbol'].str[:7]
    df['fut_pair'] = df['symbol']
    spot_cols = ['datetime', 'symbol_spot', 'close_spot', 'signal_spot', 'fut_pair']
    fut_cols = ['datetime', 'symbol', 'close', 'carry', signal_col, 'days_to_expiry', 'expiration_date', 'fut_pair']
    futs = df[fut_cols]
    spot = df[spot_cols]
    spot = spot.rename(columns={k: v for k,v in zip(spot.columns, spot.columns.str.replace('_spot', ''))})
    df = pd.concat([spot, futs], ignore_index=True)
    df.dropna(subset=[], inplace=True)
    return df 

