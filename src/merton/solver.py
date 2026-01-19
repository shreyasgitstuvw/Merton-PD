"""
src/merton/solver.py

Merton model solver for asset value and volatility estimation.

Based on KMV approach using scipy.optimize.fsolve to solve the system:
    E = V·N(d1) - D·exp(-rT)·N(d2)
    σ_E = (V/E)·N(d1)·σ_V

where:
    E = Equity value (market cap)
    V = Asset value (to solve)
    σ_E = Equity volatility (observed)
    σ_V = Asset volatility (to solve)
    D = Debt (default point)
    r = Risk-free rate
    T = Time to maturity
"""

import numpy as np
import pandas as pd
import warnings
from typing import Tuple, Optional, Dict

# Scipy imports with error handling
try:
    from scipy.stats import norm
    from scipy.optimize import fsolve
except ImportError as e:
    raise ImportError(
        "scipy is required for Merton solver. Install with: pip install scipy"
    ) from e


class MertonSolver:
    """
    Solve Merton structural credit model for asset value and volatility.

    Uses simultaneous equation solver (fsolve) to find V and σ_V that satisfy
    both the equity value equation and the volatility relationship.
    """

    def __init__(
            self,
            max_iter: int = 2000,
            tolerance: float = 1e-12,
            min_sigma: float = 1e-4,
            max_sigma: float = 3.0,
            initial_guess_method: str = 'scaled'
    ):
        """
        Initialize Merton solver.

        Args:
            max_iter: Maximum iterations for fsolve
            tolerance: Convergence tolerance
            min_sigma: Minimum allowed asset volatility
            max_sigma: Maximum allowed asset volatility (sanity check)
            initial_guess_method: Method for initial guess
                - 'scaled': σ_V = σ_E × (E/V) [most robust]
                - 'half': σ_V = σ_E × 0.5 [conservative]
                - 'fixed': σ_V = 0.2 [simple default]
                - 'custom': Provide custom guess in solve() call
        """
        self.max_iter = max_iter
        self.tolerance = tolerance
        self.min_sigma = min_sigma
        self.max_sigma = max_sigma
        self.initial_guess_method = initial_guess_method

    def solve(
            self,
            E: float,
            sigma_E: float,
            D: float,
            r: float,
            T: float,
            initial_guess: Optional[Tuple[float, float]] = None
    ) -> Dict[str, float]:
        """
        Solve for asset value (V) and asset volatility (σ_V).

        Args:
            E: Equity value (market cap)
            sigma_E: Equity volatility (annualized)
            D: Debt / default point
            r: Risk-free rate (annual)
            T: Time to maturity (years)
            initial_guess: Optional (V_0, σ_V_0) starting point

        Returns:
            Dictionary with:
                - V: Asset value
                - sigma_V: Asset volatility
                - d1: Black-Scholes d1
                - d2: Black-Scholes d2
                - converged: Whether solver converged
                - iterations: Number of iterations (approximate)
        """

        # Input validation
        if E <= 0:
            return self._failed_result("Equity (E) must be positive")
        if sigma_E <= 0:
            return self._failed_result("Equity volatility (sigma_E) must be positive")
        if D < 0:
            return self._failed_result("Debt (D) cannot be negative")
        if T <= 0:
            return self._failed_result("Time to maturity (T) must be positive")

        # Define the system of equations
        def equations(x):
            V, sigma_V = x

            # Guard against invalid values
            if V <= 0 or sigma_V <= 0:
                return [1e9, 1e9]

            # Guard against log(0) or log(negative)
            if V <= D:
                return [1e9, 1e9]

            # Calculate d1 and d2
            try:
                sqrt_T = np.sqrt(T)
                d1 = (np.log(V / D) + (r + 0.5 * sigma_V ** 2) * T) / (sigma_V * sqrt_T)
                d2 = d1 - sigma_V * sqrt_T
            except (ValueError, ZeroDivisionError):
                return [1e9, 1e9]

            # Equation 1: E = V·N(d1) - D·exp(-rT)·N(d2)
            eq1 = V * norm.cdf(d1) - np.exp(-r * T) * D * norm.cdf(d2) - E

            # Equation 2: σ_E = (V/E)·N(d1)·σ_V
            eq2 = (V / E) * norm.cdf(d1) * sigma_V - sigma_E

            return [eq1, eq2]

        # Set initial guess using specified method
        if initial_guess is None:
            V_0 = E + D
            sigma_V_0 = self._compute_initial_sigma_guess(E, D, sigma_E, V_0)
            initial_guess = [V_0, sigma_V_0]

        # Solve the system
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                solution = fsolve(
                    equations,
                    initial_guess,
                    xtol=self.tolerance,
                    maxfev=self.max_iter,
                    full_output=True
                )

            sol, info, ier, msg = solution
            V, sigma_V = float(sol[0]), float(sol[1])

            # Check convergence
            converged = (ier == 1)

            # Validate solution
            if not converged:
                return self._failed_result(f"Solver did not converge: {msg}")

            if V <= 0 or V <= D:
                return self._failed_result(f"Invalid V={V:.2f} (must be > D={D:.2f})")

            if sigma_V < self.min_sigma or sigma_V > self.max_sigma:
                return self._failed_result(
                    f"Invalid sigma_V={sigma_V:.4f} (range: {self.min_sigma}-{self.max_sigma})"
                )

            # Calculate d1, d2
            sqrt_T = np.sqrt(T)
            d1 = (np.log(V / D) + (r + 0.5 * sigma_V ** 2) * T) / (sigma_V * sqrt_T)
            d2 = d1 - sigma_V * sqrt_T

            return {
                'V': V,
                'sigma_V': sigma_V,
                'd1': d1,
                'd2': d2,
                'converged': True,
                'iterations': info['nfev'],
                'error': None
            }

        except Exception as e:
            return self._failed_result(f"Solver exception: {str(e)}")

    def _compute_initial_sigma_guess(self, E: float, D: float, sigma_E: float, V_0: float) -> float:
        """
        Compute initial guess for asset volatility based on configured method.

        Args:
            E: Equity value
            D: Debt value
            sigma_E: Equity volatility
            V_0: Initial guess for asset value (typically E + D)

        Returns:
            Initial guess for asset volatility
        """
        if self.initial_guess_method == 'scaled':
            # Scale by equity proportion: σ_V ≈ σ_E × (E/V)
            # Most robust for wide range of volatilities
            sigma_V_0 = sigma_E * (E / V_0)

        elif self.initial_guess_method == 'half':
            # Conservative: σ_V ≈ 0.5 × σ_E
            # Works well for typical volatilities
            sigma_V_0 = sigma_E * 0.5

        elif self.initial_guess_method == 'fixed':
            # Simple default: σ_V = 20%
            # Fast but may fail for extreme cases
            sigma_V_0 = 0.2

        else:
            # Default to scaled method
            sigma_V_0 = sigma_E * (E / V_0)

        # Clamp to reasonable range
        sigma_V_0 = max(self.min_sigma, min(sigma_V_0, self.max_sigma))

        return sigma_V_0

    def _failed_result(self, error_msg: str) -> Dict[str, float]:
        """Return failed result dictionary."""
        return {
            'V': np.nan,
            'sigma_V': np.nan,
            'd1': np.nan,
            'd2': np.nan,
            'converged': False,
            'iterations': 0,
            'error': error_msg
        }

    def solve_dataframe(
            self,
            df: pd.DataFrame,
            E_col: str = 'E',
            sigma_E_col: str = 'sigma_E',
            D_col: str = 'D',
            r_col: str = 'r',
            T_col: str = 'T'
    ) -> pd.DataFrame:
        """
        Solve Merton model for entire DataFrame.

        Args:
            df: DataFrame with input columns
            E_col: Column name for equity value
            sigma_E_col: Column name for equity volatility
            D_col: Column name for debt
            r_col: Column name for risk-free rate
            T_col: Column name for time to maturity

        Returns:
            DataFrame with added columns: V, sigma_V, d1, d2, converged
        """
        results = []

        for idx, row in df.iterrows():
            result = self.solve(
                E=row[E_col],
                sigma_E=row[sigma_E_col],
                D=row[D_col],
                r=row[r_col],
                T=row[T_col]
            )
            results.append(result)

        # Convert results to DataFrame
        results_df = pd.DataFrame(results)

        # Combine with original DataFrame
        output_df = pd.concat([df.reset_index(drop=True), results_df], axis=1)

        return output_df


def solve_merton_single(
        E: float,
        sigma_E: float,
        D: float,
        r: float,
        T: float
) -> Dict[str, float]:
    """
    Convenience function to solve Merton model for single observation.

    Args:
        E: Equity value
        sigma_E: Equity volatility
        D: Debt
        r: Risk-free rate
        T: Time to maturity

    Returns:
        Solution dictionary
    """
    solver = MertonSolver()
    return solver.solve(E, sigma_E, D, r, T)


# Example usage for testing
if __name__ == "__main__":
    # Test with example from Colab
    SharePriceCurrent = 230
    NetOutstandingShares = 1_000_000_000
    E = SharePriceCurrent * NetOutstandingShares
    E_sigma = 2.3
    STL = 500_000_000
    LTL = 5_000_000_000
    DP = STL + (0.5 * LTL)
    r = 0.04  # 4%
    T = 1.0

    solver = MertonSolver()
    result = solver.solve(E, E_sigma, DP, r, T)

    print("Merton Solver Test:")
    print(f"  V: {result['V']:,.0f}")
    print(f"  sigma_V: {result['sigma_V']:.4f}")
    print(f"  d1: {result['d1']:.4f}")
    print(f"  d2: {result['d2']:.4f}")
    print(f"  Converged: {result['converged']}")
    print(f"  Iterations: {result['iterations']}")
    if not result['converged']:
        print(f"  Error: {result['error']}")