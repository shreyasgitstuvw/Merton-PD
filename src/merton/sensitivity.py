"""
src/merton/sensitivity.py

Sensitivity analysis for Merton model.

Analyzes how PD changes with variations in:
- Equity volatility (σ_E)
- Debt level (D)
- Risk-free rate (r)
- Time to maturity (T)
- Asset drift (μ)
"""

import numpy as np
import pandas as pd
from typing import List, Dict, Optional

try:
    from src.merton.solver import MertonSolver
    from src.merton.distance_to_default import (
        calculate_dd_risk_neutral,
        calculate_dd_real_world,
        calculate_pd_from_dd
    )
except ImportError:
    from merton.solver import MertonSolver
    from merton.distance_to_default import (
        calculate_dd_risk_neutral,
        calculate_dd_real_world,
        calculate_pd_from_dd
    )


class SensitivityAnalyzer:
    """
    Sensitivity analysis for Merton model parameters.
    """

    def __init__(self, solver: Optional[MertonSolver] = None):
        """
        Initialize sensitivity analyzer.

        Args:
            solver: MertonSolver instance (creates default if None)
        """
        self.solver = solver if solver is not None else MertonSolver()

    def analyze_parameter(
            self,
            param_name: str,
            param_values: np.ndarray,
            base_params: Dict,
            use_real_world: bool = False
    ) -> pd.DataFrame:
        """
        Analyze sensitivity to a single parameter.

        Args:
            param_name: Name of parameter to vary ('sigma_E', 'D', 'r', 'T', 'mu')
            param_values: Array of values to test
            base_params: Dict with base values for all parameters
            use_real_world: Use real-world DD/PD (requires 'mu' in base_params)

        Returns:
            DataFrame with parameter values and resulting metrics
        """
        results = []

        for value in param_values:
            # Set current parameter value
            current_params = base_params.copy()
            current_params[param_name] = value

            # Solve Merton model
            result = self.solver.solve(
                E=current_params['E'],
                sigma_E=current_params['sigma_E'],
                D=current_params['D'],
                r=current_params['r'],
                T=current_params['T']
            )

            if not result['converged']:
                results.append({
                    param_name: value,
                    'V': np.nan,
                    'sigma_V': np.nan,
                    'DD': np.nan,
                    'PD': np.nan,
                    'converged': False
                })
                continue

            # Calculate DD and PD
            if use_real_world and 'mu' in current_params:
                DD = calculate_dd_real_world(
                    result['V'],
                    current_params['D'],
                    result['sigma_V'],
                    current_params['mu'],
                    current_params['T']
                )
            else:
                DD = calculate_dd_risk_neutral(
                    result['V'],
                    current_params['D'],
                    result['sigma_V'],
                    current_params['r'],
                    current_params['T']
                )

            PD = calculate_pd_from_dd(DD)

            results.append({
                param_name: value,
                'V': result['V'],
                'sigma_V': result['sigma_V'],
                'DD': DD,
                'PD': PD,
                'leverage': current_params['D'] / result['V'],
                'converged': True
            })

        return pd.DataFrame(results)

    def analyze_volatility_sensitivity(
            self,
            base_params: Dict,
            vol_range: tuple[float, float] = (0.1, 1.0),
            n_points: int = 20
    ) -> pd.DataFrame:
        """
        Analyze sensitivity to equity volatility.

        Args:
            base_params: Base parameter values
            vol_range: (min, max) volatility range to test
            n_points: Number of points to test

        Returns:
            DataFrame with volatility vs PD
        """
        vol_values = np.linspace(vol_range[0], vol_range[1], n_points)

        return self.analyze_parameter('sigma_E', vol_values, base_params)

    def analyze_debt_sensitivity(
            self,
            base_params: Dict,
            debt_changes: np.ndarray = None
    ) -> pd.DataFrame:
        """
        Analyze sensitivity to debt level.

        Args:
            base_params: Base parameter values
            debt_changes: Array of percentage changes (e.g., [-0.2, -0.1, 0, 0.1, 0.2])
                         Default: -50% to +50% in 10% increments

        Returns:
            DataFrame with debt level vs PD
        """
        if debt_changes is None:
            debt_changes = np.arange(-0.5, 0.6, 0.1)  # -50% to +50%

        base_debt = base_params['D']
        debt_values = base_debt * (1 + debt_changes)

        results = self.analyze_parameter('D', debt_values, base_params)
        results['debt_change_pct'] = debt_changes * 100

        return results

    def analyze_rate_sensitivity(
            self,
            base_params: Dict,
            rate_range: tuple[float, float] = (0.01, 0.1),
            n_points: int = 20
    ) -> pd.DataFrame:
        """
        Analyze sensitivity to risk-free rate.

        Args:
            base_params: Base parameter values
            rate_range: (min, max) rate range to test
            n_points: Number of points to test

        Returns:
            DataFrame with rate vs PD
        """
        rate_values = np.linspace(rate_range[0], rate_range[1], n_points)

        return self.analyze_parameter('r', rate_values, base_params)

    def run_comprehensive_analysis(
            self,
            base_params: Dict,
            use_real_world: bool = False
    ) -> Dict[str, pd.DataFrame]:
        """
        Run comprehensive sensitivity analysis on all major parameters.

        Args:
            base_params: Base parameter values
            use_real_world: Use real-world DD/PD

        Returns:
            Dictionary mapping parameter name to sensitivity DataFrame
        """
        results = {}

        # Volatility sensitivity
        print("Analyzing volatility sensitivity...")
        results['volatility'] = self.analyze_volatility_sensitivity(base_params)

        # Debt sensitivity
        print("Analyzing debt sensitivity...")
        results['debt'] = self.analyze_debt_sensitivity(base_params)

        # Rate sensitivity
        print("Analyzing rate sensitivity...")
        results['rate'] = self.analyze_rate_sensitivity(base_params)

        return results


def run_sensitivity_analysis(
        ticker: str,
        engine,
        date: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    Run sensitivity analysis for a specific ticker and date.

    Args:
        ticker: Stock ticker
        engine: Database engine
        date: Specific date (default: most recent)

    Returns:
        Dictionary with sensitivity results
    """
    from features.merton_inputs import build_merton_inputs

    # Load Merton inputs
    inputs = build_merton_inputs(ticker, engine)

    if inputs.empty:
        raise ValueError(f"No Merton inputs found for {ticker}")

    # Select date
    if date is not None:
        inputs = inputs[inputs['date'] == pd.to_datetime(date)]
    else:
        inputs = inputs.iloc[[-1]]  # Most recent

    if inputs.empty:
        raise ValueError(f"No data for {ticker} on {date}")

    # Get base parameters
    row = inputs.iloc[0]
    base_params = {
        'E': row['E'],
        'sigma_E': row['sigma_E'],
        'D': row['D'],
        'r': row['r'],
        'T': row['T'],
        'mu': row.get('mu', 0.02)
    }

    # Run analysis
    analyzer = SensitivityAnalyzer()
    results = analyzer.run_comprehensive_analysis(base_params)

    return results


# Example usage
if __name__ == "__main__":
    # Test sensitivity analysis
    base_params = {
        'E': 230_000_000_000,
        'sigma_E': 0.23,
        'D': 3_000_000_000,
        'r': 0.04,
        'T': 1.0,
        'mu': 0.02
    }

    analyzer = SensitivityAnalyzer()

    # Volatility sensitivity
    vol_results = analyzer.analyze_volatility_sensitivity(base_params)
    print("\nVolatility Sensitivity:")
    print(vol_results[['sigma_E', 'DD', 'PD']].head())

    # Debt sensitivity
    debt_results = analyzer.analyze_debt_sensitivity(base_params)
    print("\nDebt Sensitivity (+/- 50%):")
    print(debt_results[['debt_change_pct', 'D', 'DD', 'PD']])