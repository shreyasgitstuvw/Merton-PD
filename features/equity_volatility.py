"""
features/equity_volatility.py

Compute equity volatility for Merton model.

Methods:
- Log returns calculation
- Rolling window volatility (252-day default)
- Optional EWMA (Exponentially Weighted Moving Average)
"""

import pandas as pd
import numpy as np


def calculate_log_returns(prices: pd.Series) -> pd.Series:
    """
    Calculate log returns from price series.

    Returns = log(P_t / P_{t-1})

    Args:
        prices: Series of prices

    Returns:
        Series of log returns (first value is NaN)
    """
    return np.log(prices / prices.shift(1))


def calculate_rolling_volatility(
        returns: pd.Series,
        window: int = 252,
        annualization_factor: float = 252.0,
        min_periods: int = 30
) -> pd.Series:
    """
    Calculate rolling volatility (annualized).

    Args:
        returns: Series of log returns
        window: Rolling window size (default 252 trading days = 1 year)
        annualization_factor: Factor to annualize volatility (default 252)
        min_periods: Minimum observations required (default 30)

    Returns:
        Series of annualized volatility
    """
    # Calculate rolling standard deviation
    rolling_std = returns.rolling(
        window=window,
        min_periods=min_periods
    ).std()

    # Annualize: σ_annual = σ_daily × sqrt(252)
    annualized_vol = rolling_std * np.sqrt(annualization_factor)

    return annualized_vol


def calculate_ewma_volatility(
        returns: pd.Series,
        span: int = 60,
        annualization_factor: float = 252.0,
        min_periods: int = 30
) -> pd.Series:
    """
    Calculate EWMA (Exponentially Weighted Moving Average) volatility.

    Gives more weight to recent observations.

    Args:
        returns: Series of log returns
        span: Span for EWMA (default 60 days ≈ 3 months)
        annualization_factor: Factor to annualize volatility
        min_periods: Minimum observations required

    Returns:
        Series of annualized EWMA volatility
    """
    # Calculate EWMA of squared returns (variance)
    ewma_var = returns.ewm(
        span=span,
        min_periods=min_periods
    ).var()

    # Convert variance to volatility and annualize
    ewma_vol = np.sqrt(ewma_var) * np.sqrt(annualization_factor)

    return ewma_vol


def add_volatility_to_panel(
        panel: pd.DataFrame,
        price_col: str = 'close',
        method: str = 'rolling',
        window: int = 252,
        ewma_span: int = 60
) -> pd.DataFrame:
    """
    Add equity volatility to daily panel.

    Args:
        panel: Daily feature panel with prices
        price_col: Column name for prices (default 'close')
        method: 'rolling' or 'ewma'
        window: Window for rolling volatility
        ewma_span: Span for EWMA volatility

    Returns:
        Panel with added volatility columns
    """
    panel = panel.copy()

    # Calculate log returns
    panel['returns'] = calculate_log_returns(panel[price_col])

    # Calculate volatility based on method
    if method == 'rolling':
        panel['equity_volatility'] = calculate_rolling_volatility(
            panel['returns'],
            window=window
        )
    elif method == 'ewma':
        panel['equity_volatility'] = calculate_ewma_volatility(
            panel['returns'],
            span=ewma_span
        )
    elif method == 'both':
        panel['equity_vol_rolling'] = calculate_rolling_volatility(
            panel['returns'],
            window=window
        )
        panel['equity_vol_ewma'] = calculate_ewma_volatility(
            panel['returns'],
            span=ewma_span
        )
        # Use rolling as primary
        panel['equity_volatility'] = panel['equity_vol_rolling']
    else:
        raise ValueError(f"Unknown method: {method}. Use 'rolling', 'ewma', or 'both'")

    return panel


def validate_volatility(panel: pd.DataFrame, ticker: str) -> dict:
    """
    Validate volatility estimates for reasonableness.

    Args:
        panel: Panel with equity_volatility column
        ticker: Ticker symbol

    Returns:
        Validation results dict
    """
    if 'equity_volatility' not in panel.columns:
        return {
            'ticker': ticker,
            'passed': False,
            'issues': ['equity_volatility column not found']
        }

    vol = panel['equity_volatility'].dropna()

    issues = []
    warnings = []

    if len(vol) == 0:
        issues.append("No valid volatility estimates")
        return {
            'ticker': ticker,
            'passed': False,
            'issues': issues,
            'warnings': warnings
        }

    # Check for reasonable ranges
    min_vol = vol.min()
    max_vol = vol.max()
    mean_vol = vol.mean()

    # Volatility should typically be between 5% and 150% annualized
    if min_vol < 0:
        issues.append(f"Negative volatility detected: {min_vol:.2%}")

    if min_vol < 0.05:
        warnings.append(f"Very low volatility: {min_vol:.2%} (< 5%)")

    if max_vol > 1.5:
        warnings.append(f"Very high volatility: {max_vol:.2%} (> 150%)")

    if mean_vol > 1.0:
        warnings.append(f"High average volatility: {mean_vol:.2%}")

    # Check for NaN percentage
    nan_pct = panel['equity_volatility'].isna().sum() / len(panel) * 100
    if nan_pct > 80:
        warnings.append(f"High NaN percentage: {nan_pct:.1f}%")

    return {
        'ticker': ticker,
        'passed': len(issues) == 0,
        'issues': issues,
        'warnings': warnings,
        'stats': {
            'min': float(min_vol),
            'max': float(max_vol),
            'mean': float(mean_vol),
            'median': float(vol.median()),
            'valid_pct': float((1 - nan_pct / 100) * 100)
        }
    }