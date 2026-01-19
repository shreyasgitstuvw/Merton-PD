"""
scripts/run_advanced_analysis.py

Run advanced Merton model analysis:
- Bootstrap uncertainty quantification
- Sensitivity analysis
- Stress testing
- PD calibration

Usage:
    python scripts/run_advanced_analysis.py AAPL --all
    python scripts/run_advanced_analysis.py AAPL --bootstrap
    python scripts/run_advanced_analysis.py AAPL --sensitivity
    python scripts/run_advanced_analysis.py AAPL --stress
"""

import sys
from pathlib import Path
import argparse

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np

try:
    from src.db.engine import ENGINE
    from src.merton.bootstrap import run_bootstrap_analysis
    from src.merton.sensitivity import run_sensitivity_analysis
    from src.merton.stress_testing import run_stress_test
    from src.utils.logger import setup_logger
except ImportError:
    from db.engine import ENGINE
    from merton.bootstrap import run_bootstrap_analysis
    from merton.sensitivity import run_sensitivity_analysis
    from merton.stress_testing import run_stress_test
    from utils.logger import setup_logger

logger = setup_logger('advanced_analysis', log_file='logs/advanced_analysis.log')

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.precision', 4)


def run_bootstrap(ticker: str, n_iterations: int = 1000):
    """Run bootstrap uncertainty analysis."""
    logger.info(f"\n{'=' * 60}")
    logger.info(f"BOOTSTRAP UNCERTAINTY ANALYSIS: {ticker}")
    logger.info(f"{'=' * 60}")
    logger.info(f"Running {n_iterations} bootstrap iterations...")

    try:
        results = run_bootstrap_analysis(ticker, ENGINE, n_iterations=n_iterations)

        # Show summary for most recent date
        recent = results.iloc[-1]

        logger.info(f"\nMost Recent Date: {recent['date']}")
        logger.info(f"Convergence Rate: {recent['convergence_rate']:.1%}")
        logger.info(f"\nProbability of Default:")
        logger.info(f"  Median:  {recent['PD_median']:.4%}")
        logger.info(f"  95% CI:  [{recent['PD_lower']:.4%}, {recent['PD_upper']:.4%}]")
        logger.info(f"\nDistance to Default:")
        logger.info(f"  Median:  {recent['DD_median']:.2f}")
        logger.info(f"  95% CI:  [{recent['DD_lower']:.2f}, {recent['DD_upper']:.2f}]")

        # Save results
        output_file = f"results/bootstrap_{ticker}.csv"
        Path("results").mkdir(exist_ok=True)
        results.to_csv(output_file, index=False)
        logger.info(f"\n[OK] Results saved to {output_file}")

        return results

    except Exception as e:
        logger.error(f"❌ Bootstrap failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def run_sensitivity(ticker: str):
    """Run sensitivity analysis."""
    logger.info(f"\n{'=' * 60}")
    logger.info(f"SENSITIVITY ANALYSIS: {ticker}")
    logger.info(f"{'=' * 60}")

    try:
        results = run_sensitivity_analysis(ticker, ENGINE)

        # Display volatility sensitivity
        logger.info(f"\nVolatility Sensitivity:")
        vol_df = results['volatility']
        logger.info(f"\n{vol_df[['sigma_E', 'DD', 'PD']].head(10).to_string()}")

        # Display debt sensitivity
        logger.info(f"\nDebt Sensitivity (+/- 50%):")
        debt_df = results['debt']
        logger.info(f"\n{debt_df[['debt_change_pct', 'D', 'DD', 'PD']].to_string()}")

        # Save results
        Path("results").mkdir(exist_ok=True)
        for name, df in results.items():
            output_file = f"results/sensitivity_{ticker}_{name}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"[OK] {name.title()} sensitivity saved to {output_file}")

        return results

    except Exception as e:
        logger.error(f"❌ Sensitivity analysis failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def run_stress_tests(ticker: str):
    """Run stress testing."""
    logger.info(f"\n{'=' * 60}")
    logger.info(f"STRESS TESTING: {ticker}")
    logger.info(f"{'=' * 60}")

    try:
        results = run_stress_test(ticker, ENGINE)

        # Display results
        logger.info(f"\nStress Test Results:")
        display_cols = ['scenario_name', 'base_PD', 'stressed_PD', 'PD_change_pct']
        logger.info(f"\n{results[display_cols].to_string()}")

        # Detailed breakdown
        logger.info(f"\nDetailed Results:")
        for _, row in results.iterrows():
            logger.info(f"\n{row['scenario_name']}:")
            logger.info(f"  {row['scenario_description']}")
            logger.info(f"  Base DD:      {row['base_DD']:.2f}")
            logger.info(f"  Stressed DD:  {row['stressed_DD']:.2f}")
            logger.info(f"  Change:       {row['DD_change']:.2f} ({row['PD_change_pct']:.1f}%)")

        # Save results
        output_file = f"results/stress_test_{ticker}.csv"
        Path("results").mkdir(exist_ok=True)
        results.to_csv(output_file, index=False)
        logger.info(f"\n[OK] Results saved to {output_file}")

        return results

    except Exception as e:
        logger.error(f"❌ Stress testing failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(description='Run advanced Merton analysis')
    parser.add_argument('ticker', type=str, help='Stock ticker symbol')
    parser.add_argument('--all', action='store_true', help='Run all analyses')
    parser.add_argument('--bootstrap', action='store_true', help='Run bootstrap analysis')
    parser.add_argument('--sensitivity', action='store_true', help='Run sensitivity analysis')
    parser.add_argument('--stress', action='store_true', help='Run stress tests')
    parser.add_argument('--iterations', type=int, default=1000, help='Bootstrap iterations (default: 1000)')

    args = parser.parse_args()

    ticker = args.ticker.upper()

    logger.info(f"\n{'#' * 60}")
    logger.info(f"# ADVANCED MERTON ANALYSIS: {ticker}")
    logger.info(f"{'#' * 60}")

    # Run selected analyses
    if args.all or args.bootstrap:
        run_bootstrap(ticker, n_iterations=args.iterations)

    if args.all or args.sensitivity:
        run_sensitivity(ticker)

    if args.all or args.stress:
        run_stress_tests(ticker)

    logger.info(f"\n{'#' * 60}")
    logger.info(f"# ANALYSIS COMPLETE")
    logger.info(f"{'#' * 60}")
    logger.info(f"\nResults saved to results/ directory")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # No arguments - run demo
        print("Running demo for AAPL...")
        sys.argv = ['run_advanced_analysis.py', 'AAPL', '--all']

    main()