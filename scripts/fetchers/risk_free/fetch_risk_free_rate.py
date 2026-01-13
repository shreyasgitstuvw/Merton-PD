"""
scripts/fetchers/risk_free/fetch_risk_free_rate.py

Fetch risk-free rate (1-Year Treasury) from FRED or Yahoo Finance.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime


def fetch_treasury_1y(start_date: str = "2020-01-01") -> pd.DataFrame:
    """
    Fetch 1-Year Treasury rate from Yahoo Finance (^IRX is 13-week, ^FVX is 5-year).

    For 1-year, we'll use the 1-year Treasury ETF or approximate from available data.
    Alternative: Use FRED API for DGS1 (1-Year Treasury Constant Maturity Rate).

    Args:
        start_date: Start date for historical data

    Returns:
        DataFrame with columns: date, rate
    """

    # Use 13-week Treasury (^IRX) as proxy for short-term rate
    # Note: For production, use FRED API for DGS1 (1-Year Treasury)
    ticker = "^IRX"  # 13-week Treasury Bill

    treasury = yf.Ticker(ticker)
    df = treasury.history(start=start_date)

    if df.empty:
        raise ValueError(f"No data retrieved for {ticker}")

    # Extract close price (which represents the yield)
    df = df.reset_index()
    df = df.rename(columns={'Date': 'date', 'Close': 'rate'})

    # Convert rate from percentage to decimal (e.g., 4.5% -> 0.045)
    df['rate'] = df['rate'] / 100

    # Keep only date and rate
    df = df[['date', 'rate']]
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['source'] = 'yahoo_irx'
    df['ingested_at'] = datetime.utcnow()

    return df


def fetch_risk_free_rate_fred(start_date: str = "2020-01-01") -> pd.DataFrame:
    """
    Fetch 1-Year Treasury rate from FRED (Federal Reserve Economic Data).

    Requires: pip install fredapi

    Args:
        start_date: Start date for historical data

    Returns:
        DataFrame with columns: date, rate
    """
    try:
        from fredapi import Fred
    except ImportError:
        raise ImportError("fredapi not installed. Run: pip install fredapi")

    # You need a FRED API key (free from https://fred.stlouisfed.org/docs/api/api_key.html)
    # For now, this is a placeholder
    fred = Fred(api_key='YOUR_FRED_API_KEY')

    # DGS1 = 1-Year Treasury Constant Maturity Rate
    data = fred.get_series('DGS1', observation_start=start_date)

    df = pd.DataFrame({'date': data.index, 'rate': data.values})
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Convert from percentage to decimal
    df['rate'] = df['rate'] / 100

    df['source'] = 'fred_dgs1'
    df['ingested_at'] = datetime.utcnow()

    return df[['date', 'rate', 'source', 'ingested_at']]