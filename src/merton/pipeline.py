"""
src/merton/pipeline.py

End-to-end pipeline for Merton model credit risk calculation.

Orchestrates:
1. Load Merton inputs (E, D, σ_E, r, T)
2. Solve for V and σ_V
3. Calculate DD and PD
4. Validate outputs
5. Store to database
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, List
from sqlalchemy import text

try:
    from src.merton.solver import MertonSolver
    from src.merton.distance_to_default import add_dd_pd_to_dataframe
    from src.utils.logger import get_logger
except ImportError:
    # Fallback for different import styles
    from merton.solver import MertonSolver
    from merton.distance_to_default import add_dd_pd_to_dataframe
    from utils.logger import get_logger

logger = get_logger('merton.pipeline')


class MertonPipeline:
    """
    End-to-end Merton model pipeline.
    """

    def __init__(
            self,
            engine,
            config: Optional[Dict] = None,
            use_real_world: bool = False,
            mu_default: float = 0.02
    ):
        """
        Initialize Merton pipeline.

        Args:
            engine: Database engine
            config: Configuration dict (if None, loads from config/merton.yaml)
            use_real_world: Use real-world vs risk-neutral DD/PD
            mu_default: Default asset drift if not estimated
        """
        self.engine = engine
        self.use_real_world = use_real_world
        self.mu_default = mu_default

        # Load configuration
        if config is None:
            config = self._load_config()
        self.config = config

        # Initialize solver with config parameters
        solver_config = config.get('solver', {})
        self.solver = MertonSolver(
            max_iter=solver_config.get('max_iterations', 2000),
            tolerance=solver_config.get('tolerance', 1e-12),
            min_sigma=solver_config.get('min_sigma', 1e-4),
            max_sigma=solver_config.get('max_sigma', 3.0),
            initial_guess_method=solver_config.get('initial_guess_method', 'scaled')
        )

    def _load_config(self) -> Dict:
        """Load configuration from file with fallback to defaults."""
        try:
            from src.utils.config_loader import get_config
            return get_config('merton')
        except (ImportError, FileNotFoundError):
            # Fallback to defaults if config file not found
            logger.warning("Config file not found, using defaults")
            return {
                'solver': {
                    'max_iterations': 2000,
                    'tolerance': 1e-12,
                    'min_sigma': 1e-4,
                    'max_sigma': 3.0,
                    'initial_guess_method': 'scaled'
                },
                'model': {
                    'time_to_maturity': 1.0
                }
            }

    def run_for_ticker(
            self,
            ticker: str,
            store_results: bool = True,
            validate: bool = True
    ) -> pd.DataFrame:
        """
        Run complete Merton pipeline for a single ticker.

        Args:
            ticker: Stock ticker
            store_results: Whether to store to database
            validate: Whether to validate outputs

        Returns:
            DataFrame with Merton outputs
        """
        logger.info(f"Running Merton pipeline for {ticker}")

        # Step 1: Load Merton inputs
        logger.info(f"  Loading Merton inputs...")
        inputs = self._load_merton_inputs(ticker)

        if inputs.empty:
            logger.warning(f"  No Merton inputs found for {ticker}")
            return pd.DataFrame()

        logger.info(f"  Loaded {len(inputs)} rows")

        # Step 2: Solve Merton model
        logger.info(f"  Solving Merton model...")
        results = self.solver.solve_dataframe(inputs)

        # Check convergence
        converged_count = int(results['converged'].sum())
        total_count = len(results)
        logger.info(f"  Converged: {converged_count}/{total_count}")

        # Step 3: Calculate DD and PD
        logger.info(f"  Calculating DD and PD...")

        if self.use_real_world:
            # Add default mu if not present
            if 'mu' not in results.columns:
                results['mu'] = self.mu_default
            results = add_dd_pd_to_dataframe(results, method='real_world', mu_col='mu')
        else:
            results = add_dd_pd_to_dataframe(results, method='risk_neutral')

        # Step 4: Add metadata
        results['ticker'] = ticker
        results['solver_method'] = 'kmv_fsolve'

        # Step 5: Validate
        if validate:
            validation_results = self._validate_outputs(results, ticker)
            if not validation_results['passed']:
                logger.warning(f"  Validation warnings for {ticker}")
                for warning in validation_results.get('warnings', []):
                    logger.warning(f"    {warning}")

        # Step 6: Store to database
        if store_results:
            self._store_results(results)
            logger.info(f"  Stored {len(results)} rows to database")

        logger.info(f"✅ Completed {ticker}")

        return results

    def run_for_tickers(
            self,
            tickers: List[str],
            store_results: bool = True,
            validate: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Run Merton pipeline for multiple tickers.

        Args:
            tickers: List of ticker symbols
            store_results: Whether to store to database
            validate: Whether to validate outputs

        Returns:
            Dictionary mapping ticker -> results DataFrame
        """
        results = {}

        for ticker in tickers:
            try:
                ticker_results = self.run_for_ticker(
                    ticker,
                    store_results=store_results,
                    validate=validate
                )
                results[ticker] = ticker_results
            except Exception as e:
                logger.error(f"❌ Failed for {ticker}: {e}")
                import traceback
                logger.error(traceback.format_exc())

        return results

    def _load_merton_inputs(self, ticker: str) -> pd.DataFrame:
        """
        Load Merton inputs from database.

        Uses the build_merton_inputs function from features.
        """
        from features.merton_inputs import build_merton_inputs

        return build_merton_inputs(
            ticker,
            self.engine,
            time_to_maturity=1.0,
            volatility_method='rolling',
            volatility_window=252
        )

    @staticmethod
    def _validate_outputs(df: pd.DataFrame, ticker: str) -> Dict:
        """
        Validate Merton model outputs.

        Args:
            df: DataFrame with Merton outputs
            ticker: Ticker symbol

        Returns:
            Validation results dictionary
        """
        warnings = []
        issues = []

        # Check for non-converged rows
        if 'converged' in df.columns:
            non_converged = (~df['converged']).sum()
            if non_converged > 0:
                pct = non_converged / len(df) * 100
                warnings.append(f"Non-converged: {non_converged} rows ({pct:.1f}%)")

        # Check V > E (asset value should exceed equity)
        if 'V' in df.columns and 'E' in df.columns:
            invalid_V = (df['V'] <= df['E']).sum()
            if invalid_V > 0:
                warnings.append(f"V <= E: {invalid_V} rows (unexpected)")

        # Check sigma_V < sigma_E (asset vol should be less than equity vol)
        if 'sigma_V' in df.columns and 'sigma_E' in df.columns:
            invalid_vol = (df['sigma_V'] >= df['sigma_E']).sum()
            if invalid_vol > 0:
                warnings.append(f"sigma_V >= sigma_E: {invalid_vol} rows (unexpected)")

        # Check PD range
        if 'PD' in df.columns:
            pd_stats = df['PD'].describe()
            if pd_stats['max'] > 0.5:
                warnings.append(f"High PD detected: max={pd_stats['max']:.2%}")
            if pd_stats['min'] < 0 or pd_stats['max'] > 1:
                issues.append(f"PD out of range [0,1]: [{pd_stats['min']:.4f}, {pd_stats['max']:.4f}]")

        # Check DD range
        if 'DD' in df.columns:
            dd_stats = df['DD'].describe()
            if dd_stats['min'] < -5:
                warnings.append(f"Very low DD: min={dd_stats['min']:.2f}")
            if dd_stats['max'] > 10:
                warnings.append(f"Very high DD: max={dd_stats['max']:.2f}")

        return {
            'ticker': ticker,
            'passed': len(issues) == 0,
            'issues': issues,
            'warnings': warnings
        }

    def _store_results(self, df: pd.DataFrame):
        """
        Store Merton outputs to database.

        Args:
            df: DataFrame with Merton outputs
        """
        if df.empty:
            return

        # Select columns for storage
        store_cols = [
            'ticker', 'date', 'V', 'sigma_V', 'DD', 'PD',
            'd1', 'd2', 'converged', 'iterations', 'solver_method'
        ]

        # Calculate additional metrics
        df['leverage_ratio'] = df['D'] / df['V']
        df['equity_to_asset_ratio'] = df['E'] / df['V']

        store_cols.extend(['leverage_ratio', 'equity_to_asset_ratio'])

        # Filter to columns that exist
        store_cols = [col for col in store_cols if col in df.columns]

        df_store = df[store_cols].copy()

        # Convert date to date object
        df_store['date'] = pd.to_datetime(df_store['date']).dt.date

        # Rename columns to match database schema
        df_store = df_store.rename(columns={
            'V': 'asset_value',
            'sigma_V': 'asset_volatility',
            'DD': 'distance_to_default',
            'PD': 'probability_default'
        })

        # Delete existing records for this ticker and date range
        ticker = df_store['ticker'].iloc[0]
        min_date = df_store['date'].min()
        max_date = df_store['date'].max()

        with self.engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM merton_outputs
                WHERE ticker = :ticker
                AND date BETWEEN :min_date AND :max_date
            """), {
                'ticker': ticker,
                'min_date': min_date,
                'max_date': max_date
            })

        # Insert new records
        df_store.to_sql(
            'merton_outputs',
            self.engine,
            if_exists='append',
            index=False,
            method='multi'
        )


# Convenience function
def run_merton_pipeline(
        ticker: str,
        engine,
        store_results: bool = True
) -> pd.DataFrame:
    """
    Run Merton pipeline for a single ticker.

    Args:
        ticker: Stock ticker
        engine: Database engine
        store_results: Whether to store results

    Returns:
        DataFrame with Merton outputs
    """
    pipeline = MertonPipeline(engine)
    return pipeline.run_for_ticker(ticker, store_results=store_results)