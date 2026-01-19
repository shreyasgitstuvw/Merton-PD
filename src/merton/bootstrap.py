"""
src/merton/bootstrap.py

Bootstrap uncertainty quantification for Merton model outputs.

Estimates confidence intervals for V, σ_V, DD, and PD by:
1. Adding noise to input parameters (E, σ_E, D, μ)
2. Re-solving Merton model for each bootstrap sample
3. Computing percentiles to get confidence intervals
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple, Optional
from scipy.stats import norm

try:
    from src.merton.solver import MertonSolver
    from src.merton.distance_to_default import (
        calculate_dd_real_world,
        calculate_pd_from_dd
    )
except ImportError:
    from merton.solver import MertonSolver
    from merton.distance_to_default import (
        calculate_dd_real_world,
        calculate_pd_from_dd
    )


class BootstrapUncertainty:
    """
    Bootstrap uncertainty quantification for Merton model.
    """

    def __init__(
            self,
            n_iterations: int = 2000,
            confidence_level: float = 0.95,
            random_seed: Optional[int] = None
    ):
        """
        Initialize bootstrap analyzer.

        Args:
            n_iterations: Number of bootstrap iterations
            confidence_level: Confidence level (e.g., 0.95 for 95% CI)
            random_seed: Random seed for reproducibility
        """
        self.n_iterations = n_iterations
        self.confidence_level = confidence_level

        if random_seed is not None:
            np.random.seed(random_seed)

        # Calculate percentiles for confidence intervals
        alpha = 1 - confidence_level
        self.lower_percentile = (alpha / 2) * 100
        self.upper_percentile = (1 - alpha / 2) * 100

    def run_bootstrap(
            self,
            E: float,
            sigma_E: float,
            D: float,
            r: float,
            T: float,
            mu: float,
            se_sigma_E: Optional[float] = None,
            se_mu: Optional[float] = None,
            pct_noise_D: float = 0.03
    ) -> Dict:
        """
        Run bootstrap simulation for single observation.

        Args:
            E: Equity value (typically held fixed)
            sigma_E: Equity volatility
            D: Debt
            r: Risk-free rate
            T: Time to maturity
            mu: Asset drift (real-world)
            se_sigma_E: Standard error of equity volatility (default: 10% of σ_E)
            se_mu: Standard error of mu (default: 50% of μ)
            pct_noise_D: Percentage noise for debt (default: 3%)

        Returns:
            Dictionary with bootstrap results and confidence intervals
        """
        # Set default standard errors if not provided
        if se_sigma_E is None:
            se_sigma_E = sigma_E * 0.1  # 10% of equity volatility

        if se_mu is None:
            se_mu = abs(mu) * 0.5 if mu != 0 else 0.01  # 50% of mu

        # Storage for bootstrap samples
        V_samples = []
        sigma_V_samples = []
        DD_samples = []
        PD_samples = []

        solver = MertonSolver()

        for _ in range(self.n_iterations):
            # Draw noisy inputs
            E_b = E  # Typically hold equity fixed
            sigma_E_b = self._draw_positive_normal(sigma_E, se_sigma_E)
            D_b = D * np.exp(np.random.normal(0, pct_noise_D))  # Lognormal noise
            mu_b = np.random.normal(mu, se_mu)

            # Solve Merton model
            result = solver.solve(E_b, sigma_E_b, D_b, r, T)

            if not result['converged']:
                continue  # Skip non-converged samples

            V_b = result['V']
            sigma_V_b = result['sigma_V']

            # Calculate DD and PD (real-world)
            DD_b = calculate_dd_real_world(V_b, D_b, sigma_V_b, mu_b, T)
            PD_b = calculate_pd_from_dd(DD_b)

            # Store samples
            V_samples.append(V_b)
            sigma_V_samples.append(sigma_V_b)
            DD_samples.append(DD_b)
            PD_samples.append(PD_b)

        # Calculate statistics
        results = {
            'n_samples': len(V_samples),
            'convergence_rate': len(V_samples) / self.n_iterations,
            'V': self._compute_stats(V_samples),
            'sigma_V': self._compute_stats(sigma_V_samples),
            'DD': self._compute_stats(DD_samples),
            'PD': self._compute_stats(PD_samples)
        }

        return results

    def run_bootstrap_dataframe(
            self,
            df: pd.DataFrame,
            se_sigma_E: Optional[float] = None,
            se_mu: Optional[float] = None,
            show_progress: bool = True
    ) -> pd.DataFrame:
        """
        Run bootstrap for entire DataFrame.

        Args:
            df: DataFrame with columns: E, sigma_E, D, r, T, mu
            se_sigma_E: Standard error of equity volatility
            se_mu: Standard error of mu
            show_progress: Show progress bar

        Returns:
            DataFrame with bootstrap confidence intervals
        """
        results = []

        # Limit to recent data for speed (bootstrap is slow for 291 rows)
        print(f"Original data: {len(df)} rows")
        if len(df) > 10:
            print(f"Using last 10 rows for bootstrap (full dataset too slow)")
            df = df.tail(10)

        total_rows = len(df)

        for idx, row in df.iterrows():
            if show_progress:
                print(f"Bootstrap progress: {idx + 1}/{total_rows}", end='\r')

            bootstrap_result = self.run_bootstrap(
                E=row['E'],
                sigma_E=row['sigma_E'],
                D=row['D'],
                r=row['r'],
                T=row['T'],
                mu=row.get('mu', 0.02),  # Default mu if not present
                se_sigma_E=se_sigma_E,
                se_mu=se_mu
            )

            # Flatten results for DataFrame
            flat_result = {
                'date': row.get('date'),
                'ticker': row.get('ticker'),
                'convergence_rate': bootstrap_result['convergence_rate'],
                'V_median': bootstrap_result['V']['median'],
                'V_lower': bootstrap_result['V']['ci_lower'],
                'V_upper': bootstrap_result['V']['ci_upper'],
                'sigma_V_median': bootstrap_result['sigma_V']['median'],
                'sigma_V_lower': bootstrap_result['sigma_V']['ci_lower'],
                'sigma_V_upper': bootstrap_result['sigma_V']['ci_upper'],
                'DD_median': bootstrap_result['DD']['median'],
                'DD_lower': bootstrap_result['DD']['ci_lower'],
                'DD_upper': bootstrap_result['DD']['ci_upper'],
                'PD_median': bootstrap_result['PD']['median'],
                'PD_lower': bootstrap_result['PD']['ci_lower'],
                'PD_upper': bootstrap_result['PD']['ci_upper']
            }

            results.append(flat_result)

        if show_progress:
            print(f"\nBootstrap complete: {total_rows} rows processed")

        results_df = pd.DataFrame(results)

        return results_df

    def _draw_positive_normal(self, mean: float, std: float) -> float:
        """Draw from normal distribution, ensuring positive value."""
        value = np.random.normal(mean, std)

        # Keep redrawing until positive (simple rejection sampling)
        while value <= 0:
            value = np.random.normal(mean, std)

        return value

    def _compute_stats(self, samples: list) -> Dict:
        """Compute statistics from bootstrap samples."""
        samples_array = np.array(samples)

        return {
            'median': float(np.median(samples_array)),
            'mean': float(np.mean(samples_array)),
            'std': float(np.std(samples_array)),
            'ci_lower': float(np.percentile(samples_array, self.lower_percentile)),
            'ci_upper': float(np.percentile(samples_array, self.upper_percentile)),
            'samples': samples_array  # Keep samples for plotting
        }


def run_bootstrap_analysis(
        ticker: str,
        engine,
        n_iterations: int = 2000,
        confidence_level: float = 0.95
) -> pd.DataFrame:
    """
    Run bootstrap analysis for a ticker's Merton outputs.

    Args:
        ticker: Stock ticker
        engine: Database engine
        n_iterations: Number of bootstrap iterations
        confidence_level: Confidence level for intervals

    Returns:
        DataFrame with bootstrap confidence intervals
    """
    from features.merton_inputs import build_merton_inputs

    # Load Merton inputs
    inputs = build_merton_inputs(ticker, engine)

    if inputs.empty:
        raise ValueError(f"No Merton inputs found for {ticker}")

    # Add default mu if not present
    if 'mu' not in inputs.columns:
        inputs['mu'] = 0.02

    # Run bootstrap
    bootstrap = BootstrapUncertainty(
        n_iterations=n_iterations,
        confidence_level=confidence_level
    )

    results = bootstrap.run_bootstrap_dataframe(inputs)

    return results


# Example usage
if __name__ == "__main__":
    # Test with single observation
    E = 230_000_000_000
    sigma_E = 0.23  # 23%
    D = 3_000_000_000
    r = 0.04
    T = 1.0
    mu = 0.02

    bootstrap = BootstrapUncertainty(n_iterations=1000)
    results = bootstrap.run_bootstrap(E, sigma_E, D, r, T, mu)

    print("Bootstrap Results:")
    print(f"Convergence rate: {results['convergence_rate']:.1%}")
    print(f"\nPD:")
    print(f"  Median: {results['PD']['median']:.4%}")
    print(f"  95% CI: [{results['PD']['ci_lower']:.4%}, {results['PD']['ci_upper']:.4%}]")
    print(f"\nDD:")
    print(f"  Median: {results['DD']['median']:.2f}")
    print(f"  95% CI: [{results['DD']['ci_lower']:.2f}, {results['DD']['ci_upper']:.2f}]")