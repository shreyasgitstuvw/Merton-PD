"""
Day 5 Testing Script - Daily Feature Panel Validation

Purpose:
- Build daily panels for test tickers
- Validate temporal constraints
- Display diagnostics
- Optionally store to database (future)
"""

from src.db.engine import ENGINE
from features.daily_panel import build_daily_panel
import traceback

TICKERS = ["AAPL", "MSFT", "TSLA", "JPM", "XOM"]


def test_single_ticker(ticker, engine, include_staleness=True):
    """
    Test panel construction for a single ticker with full diagnostics.
    """
    print("\n" + "=" * 60)
    print(f"TESTING: {ticker}")
    print("=" * 60)

    try:
        # Build panel with staleness tracking visible
        panel = build_daily_panel(
            ticker,
            engine,
            max_staleness_days=365,
            include_staleness_cols=include_staleness
        )

        # Success metrics
        print(f"\n✅ SUCCESS: {ticker}")
        print(f"   Rows: {len(panel)}")
        print(f"   Date range: {panel['date'].min().date()} to {panel['date'].max().date()}")
        print(f"   Columns: {len(panel.columns)}")

        # Sample data
        print(f"\n[FIRST 3 ROWS]")
        print(panel.head(3))

        print(f"\n[LAST 3 ROWS]")
        print(panel.tail(3))

        # Staleness stats (if included)
        if include_staleness:
            staleness_cols = [col for col in panel.columns if 'staleness_days' in col]
            if staleness_cols:
                print(f"\n[STALENESS STATS]")
                for col in staleness_cols:
                    print(f"  {col}:")
                    print(f"    Mean: {panel[col].mean():.0f} days")
                    print(f"    Max: {panel[col].max():.0f} days")

        return panel

    except ValueError as e:
        # Validation caught an issue (this is GOOD - it means safeguards work)
        print(f"\n🚨 VALIDATION ERROR: {ticker}")
        print(f"   {str(e)}")
        print(f"\n   This is expected if data has issues - validation is working!")
        return None

    except Exception as e:
        # Unexpected error (this is BAD)
        print(f"\n❌ UNEXPECTED ERROR: {ticker}")
        print(traceback.format_exc())
        return None


def run_validation_tests():
    """
    Run validation tests on all tickers.
    """
    print("\n" + "#" * 60)
    print("# DAY 5 VALIDATION TEST SUITE")
    print("#" * 60)

    results = {}

    for ticker in TICKERS:
        panel = test_single_ticker(ticker, ENGINE, include_staleness=True)
        results[ticker] = panel is not None

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    success_count = sum(results.values())
    total_count = len(results)

    for ticker, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {ticker}: {status}")

    print(f"\nTotal: {success_count}/{total_count} passed")

    if success_count == total_count:
        print("\n🎉 ALL TESTS PASSED - Day 5 validation is working!")
    else:
        print(f"\n⚠️  {total_count - success_count} ticker(s) failed - review errors above")


if __name__ == "__main__":
    run_validation_tests()