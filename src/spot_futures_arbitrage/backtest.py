import pandas as pd
import numpy as np
import logging
logger = logging.getLogger('okx_prices')
logger.setLevel(logging.INFO)


def simple_backtest(df, price_col='close', signal_col='signal') -> pd.DataFrame:
    """A simple backtest function that calculates PnL based on price changes and trade signals.
    Assumes the DataFrame has 'datetime', 'price_col', and 'signal_col' columns.
    Signal values: 1=long, -1=short, 0=flat
    This function assumes positions only change when the signal changes from 1 to -1 or vice versa.
    Computes daily PnL assuming position held for next period.

    :param df: DataFrame with 'datetime', 'price_col', and 'signal_col' columns.
    :param price_col: Column name for the price to use in backtesting.
    :param signal_col: Column name for the trade signals.
    :return: DataFrame with additional columns for price change, position, PnL,
    """

    assert 'datetime' in df.columns, "DataFrame must contain 'datetime' column"
    assert price_col in df.columns, f"DataFrame must contain '{price_col}' column"
    assert signal_col in df.columns, f"DataFrame must contain '{signal_col}' column"

    df = df.sort_values('datetime').copy()
    df['price_change'] = df[price_col].pct_change().fillna(0)

    position = []
    current_pos = 0
    for signal in df[signal_col]:
        if current_pos != signal:
            current_pos += signal
        position.append(current_pos)
    df['position'] = position

    df['pnl'] = df['position'].shift().fillna(0) * df['price_change']
    df['cumulative_pnl'] = (1 + df['pnl']).cumprod() - 1
    return df

def run_backtest(df: pd.DataFrame, backtest_func, groupby: list[str]=[], price_col='close', signal_col='signal', pair_price_col='') -> pd.DataFrame:
    """
    A simple backtester that takes in a DataFrame with datetime, price, and trade signals.
    Signal values: 1=long, -1=short, 0=flat
    Computes daily PnL assuming position held for next period.

    :param df: DataFrame with 'datetime', 'price_col', and 'signal_col' columns.
    :param backtest_func: Function to apply for backtesting, e.g., simple_backtest.
    :param groupby: List of columns to group by before applying the backtest function.
    :param price_col: Column name for the price to use in backtesting.
    :param signal_col: Column name for the trade signals.
    :param pair_price_col: Optional column name for pair price if needed.
    :return: DataFrame with backtest results.
    """
    df = df.dropna(subset=[price_col])
    if df.empty:
        logger.warning("DataFrame is empty, returning empty DataFrame.")
        return df
    df = df.sort_values('datetime').copy()
    if groupby:
        return df.groupby(groupby).apply(backtest_func).reset_index(drop=True)
    else:
        return df.apply(backtest_func).reset_index(drop=True)

def summarize_portfolio(df, groupby: list[str], pnl_col='pnl', cumulative_pnl_col='cumulative_pnl', date_col='datetime') -> pd.DataFrame:
    """ Summarize portfolio performance by calculating annualized returns, volatility, and drawdowns.
    
    :param df: DataFrame with 'datetime', 'symbol', and 'pnl' columns.
    :param groupby: List of columns to group by for summarization.
    :param pnl_col: Column name for the PnL values.
    :param cumulative_pnl_col: Column name for cumulative PnL values.
    :return: DataFrame with summarized portfolio metrics.
    """
    grouped_df = df.groupby(groupby).agg(
        start=(date_col, 'min'),
        end=(date_col, 'max'),
        total_return=(cumulative_pnl_col, 'last'),
        volatility=(pnl_col, 'std'),
    ).reset_index()
    end = grouped_df['end']
    start = grouped_df['start']
    num_seconds = (end - start).dt.seconds
    num_years = num_seconds / 365 * 24 * 3600

    grouped_df['annualized_return'] = (1 + grouped_df['total_return']) ** (1 / num_years) - 1
    grouped_df['annualized_volatility'] = grouped_df['volatility'] * np.sqrt(365)
    grouped_df['max_single_period_drawdown'] = df[pnl_col].max()
    return grouped_df

