"""
features/merton_inputs.py

Construct complete Merton model inputs.

Combines:
- E: Equity value (market cap)
- D: Debt level
- σ_E: Equity volatility
- r: Risk-free rate
- T: Time to maturity (default 1 year)
"""

import pandas as pd
import numpy as np
from sqlalchemy import text


def build_merton_inputs(
        ticker: str,
        engine,
        time_to_maturity: float = 1.0,
        volatility_method: str = 'rolling',
        volatility_window: int = 252
) -> pd.DataFrame:
    """
    Build complete Merton model inputs for a ticker.

    Args:
        ticker: Stock ticker
        engine: Database engine
        time_to_maturity: T parameter (years, default 1.0)
        volatility_method: 'rolling' or 'ewma'
        volatility_window: Window for volatility calculation

    Returns:
        DataFrame with Merton inputs: E, D, σ_E, r, T
    """
    from features.daily_panel import build_daily_panel
    from features.equity_volatility import add_volatility_to_panel
    from scripts.fetchers.risk_free.store_risk_free_rate import build_daily_risk_free_rate

    print(f"\n[BUILD MERTON INPUTS] {ticker}")

    # Step 1: Get daily panel (E, D)
    panel = build_daily_panel(ticker, engine, include_staleness_cols=False)
    print(f"  ✅ Panel: {len(panel)} days")

    # Step 2: Add equity volatility (σ_E)
    panel = add_volatility_to_panel(
        panel,
        method=volatility_method,
        window=volatility_window
    )
    print(f"  ✅ Volatility calculated")

    # Step 3: Add risk-free rate (r)
    rf_rates = build_daily_risk_free_rate(engine)
    panel = panel.merge(rf_rates, on='date', how='left')

    # Forward-fill any missing rates
    panel['rate'] = panel['rate'].ffill().bfill()
    print(f"  ✅ Risk-free rate merged")

    # Step 4: Add time to maturity (T)
    panel['time_to_maturity'] = time_to_maturity

    # Step 5: Rename to Merton convention
    merton_inputs = panel.rename(columns={
        'equity_value': 'E',
        'total_debt': 'D',
        'equity_volatility': 'sigma_E',
        'rate': 'r',
        'time_to_maturity': 'T'
    })

    # Step 6: Select final columns
    final_cols = [
        'ticker', 'date',
        'E', 'D', 'sigma_E', 'r', 'T',
        'close', 'shares_outstanding', 'market_cap'
    ]

    # Only keep rows with complete Merton inputs
    required = ['E', 'D', 'sigma_E', 'r']
    complete = merton_inputs.dropna(subset=required)

    dropped = len(merton_inputs) - len(complete)
    if dropped > 0:
        print(f"  ⚠️  Dropped {dropped} rows with incomplete inputs")

    print(f"  ✅ Final: {len(complete)} complete Merton input rows")

    return complete[final_cols]


def validate_merton_inputs(merton_inputs: pd.DataFrame, ticker: str) -> dict:
    """
    Validate Merton inputs for reasonableness.

    Args:
        merton_inputs: DataFrame with E, D, sigma_E, r, T
        ticker: Ticker symbol

    Returns:
        Validation results dict
    """
    issues = []
    warnings = []

    # Check required columns
    required_cols = ['E', 'D', 'sigma_E', 'r', 'T']
    missing_cols = [col for col in required_cols if col not in merton_inputs.columns]

    if missing_cols:
        issues.append(f"Missing columns: {missing_cols}")
        return {
            'ticker': ticker,
            'passed': False,
            'issues': issues,
            'warnings': warnings
        }

    # Check for negative values
    if (merton_inputs['E'] <= 0).any():
        issues.append("Equity value (E) has non-positive values")

    if (merton_inputs['D'] < 0).any():
        issues.append("Debt (D) has negative values")

    if (merton_inputs['sigma_E'] <= 0).any():
        issues.append("Equity volatility (σ_E) has non-positive values")

    if (merton_inputs['r'] < 0).any():
        warnings.append("Risk-free rate (r) has negative values")

    # Check leverage ratio (D/E)
    leverage = merton_inputs['D'] / merton_inputs['E']

    if (leverage > 5).any():
        high_lev_count = (leverage > 5).sum()
        warnings.append(f"High leverage (D/E > 5): {high_lev_count} rows")

    # Check volatility range
    vol_stats = merton_inputs['sigma_E'].describe()

    if vol_stats['min'] < 0.05:
        warnings.append(f"Low volatility detected: {vol_stats['min']:.2%}")

    if vol_stats['max'] > 1.5:
        warnings.append(f"High volatility detected: {vol_stats['max']:.2%}")

    return {
        'ticker': ticker,
        'passed': len(issues) == 0,
        'issues': issues,
        'warnings': warnings,
        'stats': {
            'E_mean': float(merton_inputs['E'].mean()),
            'D_mean': float(merton_inputs['D'].mean()),
            'sigma_E_mean': float(merton_inputs['sigma_E'].mean()),
            'r_mean': float(merton_inputs['r'].mean()),
            'leverage_mean': float(leverage.mean()),
            'row_count': len(merton_inputs)
        }
    }


def build_merton_inputs_all_tickers(
        engine,
        tickers: list,
        time_to_maturity: float = 1.0,
        validate: bool = True
) -> dict:
    """
    Build Merton inputs for multiple tickers.

    Args:
        engine: Database engine
        tickers: List of ticker symbols
        time_to_maturity: T parameter (years)
        validate: Whether to validate inputs

    Returns:
        Dict mapping ticker -> DataFrame of Merton inputs
    """
    results = {}

    for ticker in tickers:
        try:
            inputs = build_merton_inputs(ticker, engine, time_to_maturity)

            if validate:
                validation = validate_merton_inputs(inputs, ticker)

                print(f"\n[VALIDATION] {ticker}")
                if validation['passed']:
                    print(f"  ✅ PASS")
                else:
                    print(f"  ❌ FAIL")
                    for issue in validation['issues']:
                        print(f"    ❌ {issue}")

                if validation['warnings']:
                    for warning in validation['warnings']:
                        print(f"    ⚠️  {warning}")

            results[ticker] = inputs

        except Exception as e:
            print(f"\n❌ {ticker} FAILED: {e}")
            import traceback
            traceback.print_exc()

    return results