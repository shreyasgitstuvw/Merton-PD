"""
features/data_validation.py

Data quality checks for daily feature panels.

Validates:
- No missing critical fields
- No negative debt
- No zero/negative market cap
- No negative prices
- Reasonable value ranges
"""

import pandas as pd


class PanelValidator:
    """Validates daily feature panels for data quality issues."""

    def __init__(self, panel: pd.DataFrame, ticker: str):
        self.panel = panel
        self.ticker = ticker
        self.issues = []
        self.warnings = []

    def validate_all(self) -> dict:
        """
        Run all validation checks.

        Returns:
            dict with 'passed', 'issues', 'warnings'
        """
        self.check_missing_values()
        self.check_negative_values()
        self.check_zero_values()
        self.check_value_ranges()
        self.check_temporal_consistency()

        return {
            'ticker': self.ticker,
            'passed': len(self.issues) == 0,
            'issues': self.issues,
            'warnings': self.warnings,
            'row_count': len(self.panel)
        }

    def check_missing_values(self):
        """Check for unexpected missing values in critical fields."""
        critical_fields = ['close', 'total_debt', 'shares_outstanding', 'market_cap']

        for field in critical_fields:
            if field not in self.panel.columns:
                self.issues.append(f"Missing column: {field}")
                continue

            missing = self.panel[field].isna().sum()
            if missing > 0:
                pct = missing / len(self.panel) * 100
                self.issues.append(f"{field}: {missing} missing values ({pct:.1f}%)")

    def check_negative_values(self):
        """Check for negative values where they shouldn't exist."""
        non_negative_fields = {
            'close': 'Stock price',
            'total_debt': 'Total debt',
            'short_term_debt': 'Short-term debt',
            'long_term_debt': 'Long-term debt',
            'shares_outstanding': 'Shares outstanding',
            'market_cap': 'Market cap'
        }

        for field, name in non_negative_fields.items():
            if field not in self.panel.columns:
                continue

            negative = (self.panel[field] < 0).sum()
            if negative > 0:
                self.issues.append(f"{name}: {negative} negative values detected")

    def check_zero_values(self):
        """Check for zero values in fields that should never be zero."""
        never_zero_fields = {
            'close': 'Stock price',
            'shares_outstanding': 'Shares outstanding',
            'market_cap': 'Market cap'
        }

        for field, name in never_zero_fields.items():
            if field not in self.panel.columns:
                continue

            zeros = (self.panel[field] == 0).sum()
            if zeros > 0:
                self.issues.append(f"{name}: {zeros} zero values detected")

    def check_value_ranges(self):
        """Check for unreasonable value ranges."""

        # Market cap should be reasonable (> $1B for large caps)
        if 'market_cap' in self.panel.columns:
            min_mcap = self.panel['market_cap'].min()
            max_mcap = self.panel['market_cap'].max()

            if min_mcap < 1e9:  # Less than $1B
                self.warnings.append(f"Market cap below $1B: ${min_mcap:,.0f}")

            if max_mcap > 10e12:  # More than $10T
                self.warnings.append(f"Market cap above $10T: ${max_mcap:,.0f}")

        # Price volatility check (day-over-day change > 50% is suspicious)
        if 'close' in self.panel.columns and len(self.panel) > 1:
            returns = self.panel['close'].pct_change()
            extreme_moves = (returns.abs() > 0.5).sum()

            if extreme_moves > 0:
                self.warnings.append(f"Extreme price moves (>50%): {extreme_moves} days")

        # Debt-to-equity ratio (very high values are suspicious)
        if 'total_debt' in self.panel.columns and 'market_cap' in self.panel.columns:
            de_ratio = self.panel['total_debt'] / self.panel['market_cap']
            high_leverage = (de_ratio > 5).sum()

            if high_leverage > 0:
                self.warnings.append(f"Very high leverage (D/E > 5): {high_leverage} days")

    def check_temporal_consistency(self):
        """Check for temporal inconsistencies."""

        # Check date ordering
        if 'date' in self.panel.columns:
            # ✅ FIX: is_monotonic_increasing returns bool, not Series
            if not self.panel['date'].is_monotonic_increasing:
                self.issues.append("Dates are not in chronological order")

            # Check for duplicate dates
            duplicates = self.panel['date'].duplicated().sum()
            if duplicates > 0:
                self.issues.append(f"Duplicate dates: {duplicates}")

        # Check staleness (if column exists)
        if 'shares_outstanding_staleness_days' in self.panel.columns:
            max_staleness = self.panel['shares_outstanding_staleness_days'].max()

            if max_staleness > 365:
                self.warnings.append(f"Shares data stale (max {max_staleness:.0f} days)")


def validate_panel(panel: pd.DataFrame, ticker: str, verbose: bool = True) -> dict:
    """
    Validate a daily feature panel.

    Args:
        panel: Daily feature panel DataFrame
        ticker: Ticker symbol
        verbose: Print validation results

    Returns:
        Validation results dict
    """
    validator = PanelValidator(panel, ticker)
    results = validator.validate_all()

    if verbose:
        print(f"\n{'=' * 60}")
        print(f"DATA QUALITY REPORT: {ticker}")
        print(f"{'=' * 60}")
        print(f"Rows: {results['row_count']}")
        print(f"Status: {'✅ PASS' if results['passed'] else '❌ FAIL'}")

        if results['issues']:
            print(f"\n🚨 ISSUES ({len(results['issues'])}):")
            for issue in results['issues']:
                print(f"  ❌ {issue}")

        if results['warnings']:
            print(f"\n⚠️  WARNINGS ({len(results['warnings'])}):")
            for warning in results['warnings']:
                print(f"  ⚠️  {warning}")

        if not results['issues'] and not results['warnings']:
            print("\n✅ No issues or warnings detected")

    return results


def validate_all_tickers(engine, tickers: list, verbose: bool = True) -> pd.DataFrame:
    """
    Validate daily panels for multiple tickers.

    Args:
        engine: Database engine
        tickers: List of ticker symbols
        verbose: Print results

    Returns:
        DataFrame with validation summary
    """
    from features.daily_panel import build_daily_panel

    results = []

    for ticker in tickers:
        try:
            panel = build_daily_panel(ticker, engine, include_staleness_cols=True)
            result = validate_panel(panel, ticker, verbose=verbose)
            results.append(result)
        except Exception as e:
            results.append({
                'ticker': ticker,
                'passed': False,
                'issues': [f"Failed to build panel: {str(e)}"],
                'warnings': [],
                'row_count': 0
            })
            if verbose:
                print(f"\n❌ {ticker}: Failed to build panel - {e}")

    # Summary
    if verbose:
        print(f"\n{'=' * 60}")
        print("VALIDATION SUMMARY")
        print(f"{'=' * 60}")

        passed = sum(1 for r in results if r['passed'])
        total = len(results)

        print(f"Passed: {passed}/{total}")

        for result in results:
            status = "✅" if result['passed'] else "❌"
            print(f"  {status} {result['ticker']}: {result['row_count']} rows")

    return pd.DataFrame(results)