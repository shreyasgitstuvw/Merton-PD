"""
scripts/run_merton_model.py

Run Merton model for credit risk calculation.

Usage:
    python scripts/run_merton_model.py              # Run for default tickers
    python scripts/run_merton_model.py AAPL MSFT    # Run for specific tickers
"""

import sys
from src.db.engine import ENGINE
from src.merton.pipeline import MertonPipeline
from src.utils.logger import setup_logger

# Setup logging
logger = setup_logger('merton', log_file='logs/merton_model.log')

# Default tickers
DEFAULT_TICKERS = ["AAPL", "MSFT", "TSLA", "JPM", "XOM"]


def run_merton_model(tickers: list, store_results: bool = True):
    """
    Run Merton model for list of tickers.

    Args:
        tickers: List of ticker symbols
        store_results: Whether to store results to database
    """
    logger.info("=" * 60)
    logger.info("MERTON MODEL CREDIT RISK CALCULATION")
    logger.info("=" * 60)
    logger.info(f"Tickers: {', '.join(tickers)}")
    logger.info(f"Store results: {store_results}")

    # Initialize pipeline
    pipeline = MertonPipeline(
        engine=ENGINE,
        use_real_world=False,  # Use risk-neutral by default
        mu_default=0.02  # 2% default asset drift
    )

    # Run for all tickers
    results = pipeline.run_for_tickers(
        tickers,
        store_results=store_results,
        validate=True
    )

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)

    for ticker, df in results.items():
        if df.empty:
            logger.info(f"❌ {ticker}: No results")
        else:
            converged = df['converged'].sum()
            total = len(df)
            mean_pd = df['PD'].mean()

            logger.info(f"✅ {ticker}:")
            logger.info(f"   Rows: {total}")
            logger.info(f"   Converged: {converged}/{total}")
            logger.info(f"   Mean PD: {mean_pd:.2%}")

            # Show recent PDs
            recent = df.nlargest(3, 'date')[['date', 'DD', 'PD']]
            logger.info(f"   Recent PDs:")
            for _, row in recent.iterrows():
                logger.info(f"     {row['date']}: DD={row['DD']:.2f}, PD={row['PD']:.2%}")

    logger.info("\n" + "=" * 60)
    logger.info("MERTON MODEL COMPLETE")
    logger.info("=" * 60)

    return results


def display_sample_results(results: dict):
    """Display sample results for visual inspection."""
    import pandas as pd

    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.precision', 4)

    for ticker, df in results.items():
        if not df.empty:
            print(f"\n{'=' * 80}")
            print(f"SAMPLE RESULTS: {ticker}")
            print(f"{'=' * 80}")

            # Show key columns
            display_cols = [
                'date', 'E', 'D', 'sigma_E', 'V', 'sigma_V',
                'DD', 'PD', 'converged'
            ]
            display_cols = [col for col in display_cols if col in df.columns]

            # Show last 5 rows
            print("\nMost Recent 5 Days:")
            print(df[display_cols].tail(5).to_string())

            # Show statistics
            print(f"\nStatistics:")
            print(f"  Mean PD: {df['PD'].mean():.2%}")
            print(f"  Median PD: {df['PD'].median():.2%}")
            print(f"  Min PD: {df['PD'].min():.2%}")
            print(f"  Max PD: {df['PD'].max():.2%}")
            print(f"  Std PD: {df['PD'].std():.2%}")


if __name__ == "__main__":
    # Get tickers from command line or use defaults
    if len(sys.argv) > 1:
        tickers = [t.upper() for t in sys.argv[1:]]
    else:
        tickers = DEFAULT_TICKERS

    # Run Merton model
    results = run_merton_model(tickers, store_results=True)

    # Display sample results
    display_sample_results(results)