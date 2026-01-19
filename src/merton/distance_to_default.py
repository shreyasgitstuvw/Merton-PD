"""
src/merton/distance_to_default.py

Calculate Distance to Default (DD) and Probability of Default (PD)
using both risk-neutral and real-world approaches.
"""

import numpy as np
import pandas as pd
from scipy.stats import norm
from typing import Optional


def calculate_dd_risk_neutral(
        V: float,
        D: float,
        sigma_V: float,
        r: float,
        T: float
) -> float:
    """
    Calculate risk-neutral Distance to Default.

    This uses the risk-free rate as the asset drift (risk-neutral measure).

    Formula:
        DD = [ln(V/D) + (r - 0.5·σ_V²)·T] / (σ_V·√T)

    Args:
        V: Asset value
        D: Debt / default point
        sigma_V: Asset volatility
        r: Risk-free rate
        T: Time to maturity

    Returns:
        Distance to default (in standard deviations)
    """
    if V <= 0 or D <= 0 or sigma_V <= 0 or T <= 0:
        return np.nan

    try:
        numerator = np.log(V / D) + (r - 0.5 * sigma_V ** 2) * T
        denominator = sigma_V * np.sqrt(T)
        DD = numerator / denominator
        return DD
    except (ValueError, ZeroDivisionError):
        return np.nan


def calculate_dd_real_world(
        V: float,
        D: float,
        sigma_V: float,
        mu: float,
        T: float
) -> float:
    """
    Calculate real-world Distance to Default.

    This uses estimated asset drift μ (real-world measure).

    Formula:
        DD = [ln(V/D) + (μ - 0.5·σ_V²)·T] / (σ_V·√T)

    Args:
        V: Asset value
        D: Debt / default point
        sigma_V: Asset volatility
        mu: Asset drift (real-world expected return)
        T: Time to maturity

    Returns:
        Distance to default (in standard deviations)
    """
    if V <= 0 or D <= 0 or sigma_V <= 0 or T <= 0:
        return np.nan

    try:
        numerator = np.log(V / D) + (mu - 0.5 * sigma_V ** 2) * T
        denominator = sigma_V * np.sqrt(T)
        DD = numerator / denominator
        return DD
    except (ValueError, ZeroDivisionError):
        return np.nan


def calculate_pd_from_dd(DD: float) -> float:
    """
    Calculate Probability of Default from Distance to Default.

    Uses standard normal CDF: PD = Φ(-DD)

    Args:
        DD: Distance to default

    Returns:
        Probability of default (0 to 1)
    """
    if np.isnan(DD):
        return np.nan

    try:
        # Explicit float conversion to satisfy type checkers
        PD = float(norm.cdf(-float(DD)))
        return PD
    except (ValueError, TypeError):
        return np.nan


def calculate_pd_risk_neutral(
        V: float,
        D: float,
        sigma_V: float,
        r: float,
        T: float
) -> tuple:
    """
    Calculate risk-neutral PD.

    Returns:
        (DD, PD) tuple
    """
    DD = calculate_dd_risk_neutral(V, D, sigma_V, r, T)
    PD = calculate_pd_from_dd(DD)
    return DD, PD


def calculate_pd_real_world(
        V: float,
        D: float,
        sigma_V: float,
        mu: float,
        T: float
) -> tuple:
    """
    Calculate real-world PD.

    Returns:
        (DD, PD) tuple
    """
    DD = calculate_dd_real_world(V, D, sigma_V, mu, T)
    PD = calculate_pd_from_dd(DD)
    return DD, PD


def estimate_mu_from_asset_series(
        asset_values: np.ndarray,
        frequency: str = 'annual'
) -> float:
    """
    Estimate asset drift (μ) from historical asset values.

    Args:
        asset_values: Array of asset values in chronological order
        frequency: 'daily', 'monthly', 'annual'

    Returns:
        Annualized mean log-return (μ)
    """
    arr = np.array(asset_values, dtype=float)

    if arr.size < 2:
        raise ValueError("Need at least 2 observations to estimate returns")

    # Calculate log returns
    log_returns = np.diff(np.log(arr))
    mean_lr = np.mean(log_returns)

    # Annualize based on frequency
    if frequency == 'daily':
        return mean_lr * 252.0
    elif frequency == 'monthly':
        return mean_lr * 12.0
    elif frequency == 'annual':
        return mean_lr
    else:
        raise ValueError("frequency must be 'daily', 'monthly', or 'annual'")


def shrink_mu(
        mu_company: float,
        mu_sector: float,
        n_years: float,
        tau: float = 2.0
) -> float:
    """
    Shrinkage estimator for asset drift.

    Blends company-specific μ with sector μ based on data history.

    Args:
        mu_company: Company-specific drift estimate
        mu_sector: Sector average drift
        n_years: Years of data used for company estimate
        tau: Shrinkage parameter (default 2.0)

    Returns:
        Shrunk μ estimate
    """
    weight = n_years / (n_years + tau)
    return weight * mu_company + (1 - weight) * mu_sector


def add_dd_pd_to_dataframe(
        df: pd.DataFrame,
        method: str = 'risk_neutral',
        mu_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Add DD and PD columns to DataFrame with Merton outputs.

    Args:
        df: DataFrame with columns: V, D, sigma_V, r, T
        method: 'risk_neutral' or 'real_world'
        mu_col: Column name for μ (required if method='real_world')

    Returns:
        DataFrame with added DD and PD columns
    """
    df = df.copy()

    if method == 'risk_neutral':
        df['DD'] = df.apply(
            lambda row: calculate_dd_risk_neutral(
                row['V'], row['D'], row['sigma_V'], row['r'], row['T']
            ),
            axis=1
        )
    elif method == 'real_world':
        if mu_col is None:
            raise ValueError("mu_col required for real_world method")
        df['DD'] = df.apply(
            lambda row: calculate_dd_real_world(
                row['V'], row['D'], row['sigma_V'], row[mu_col], row['T']
            ),
            axis=1
        )
    else:
        raise ValueError("method must be 'risk_neutral' or 'real_world'")

    # Calculate PD from DD
    df['PD'] = df['DD'].apply(calculate_pd_from_dd)

    return df


# Example usage
if __name__ == "__main__":
    # Test with example values
    V = 232_500_000_000
    D = 3_000_000_000
    sigma_V = 0.2
    r = 0.04
    mu = 0.02
    T = 1.0

    # Risk-neutral
    DD_rn, PD_rn = calculate_pd_risk_neutral(V, D, sigma_V, r, T)
    print(f"Risk-Neutral:")
    print(f"  DD: {DD_rn:.4f}")
    print(f"  PD: {PD_rn:.4%}")

    # Real-world
    DD_rw, PD_rw = calculate_pd_real_world(V, D, sigma_V, mu, T)
    print(f"\nReal-World:")
    print(f"  DD: {DD_rw:.4f}")
    print(f"  PD: {PD_rw:.4%}")