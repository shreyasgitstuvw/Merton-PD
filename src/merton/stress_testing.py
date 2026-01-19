"""
src/merton/stress_testing.py

Stress testing framework for Merton model.

Applies historical stress scenarios to current portfolio:
- GFC 2008 (Global Financial Crisis)
- COVID 2020
- Rate Hikes 2022
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime

try:
    from src.merton.solver import MertonSolver
    from src.merton.distance_to_default import (
        calculate_dd_risk_neutral,
        calculate_pd_from_dd
    )
    from src.utils.config_loader import get_config
except ImportError:
    from merton.solver import MertonSolver
    from merton.distance_to_default import (
        calculate_dd_risk_neutral,
        calculate_pd_from_dd
    )


class StressScenario:
    """Represents a stress testing scenario."""

    def __init__(
            self,
            name: str,
            volatility_shock: float,
            debt_shock: float,
            rate_shock: float,
            equity_shock: float = 0.0,
            description: str = ""
    ):
        """
        Initialize stress scenario.

        Args:
            name: Scenario name
            volatility_shock: Multiplicative shock to volatility (e.g., 2.0 = double)
            debt_shock: Additive shock to debt (e.g., 0.1 = +10%)
            rate_shock: Additive shock to rate (e.g., 0.02 = +2%)
            equity_shock: Multiplicative shock to equity (e.g., -0.3 = -30%)
            description: Scenario description
        """
        self.name = name
        self.volatility_shock = volatility_shock
        self.debt_shock = debt_shock
        self.rate_shock = rate_shock
        self.equity_shock = equity_shock
        self.description = description

    def apply(self, base_params: Dict) -> Dict:
        """
        Apply scenario shocks to base parameters.

        Args:
            base_params: Dictionary with E, sigma_E, D, r, T

        Returns:
            Stressed parameters
        """
        stressed = base_params.copy()

        # Apply shocks
        stressed['E'] = base_params['E'] * (1 + self.equity_shock)
        stressed['sigma_E'] = base_params['sigma_E'] * self.volatility_shock
        stressed['D'] = base_params['D'] * (1 + self.debt_shock)
        stressed['r'] = base_params['r'] + self.rate_shock

        return stressed


class StressTester:
    """
    Stress testing framework for Merton model.
    """

    # Pre-defined historical scenarios
    HISTORICAL_SCENARIOS = {
        'GFC_2008': StressScenario(
            name='Global Financial Crisis (2008)',
            volatility_shock=2.5,  # Volatility increased 2.5x
            debt_shock=0.0,  # Debt relatively stable
            rate_shock=-0.04,  # Rates dropped 4%
            equity_shock=-0.45,  # Markets fell 45%
            description='2008 financial crisis - extreme volatility and equity decline'
        ),
        'COVID_2020': StressScenario(
            name='COVID-19 Pandemic (2020)',
            volatility_shock=2.0,  # Volatility doubled
            debt_shock=0.15,  # Debt increased 15%
            rate_shock=-0.015,  # Rates cut 1.5%
            equity_shock=-0.30,  # Markets fell 30% initially
            description='COVID-19 pandemic - sharp volatility spike and market decline'
        ),
        'RATES_2022': StressScenario(
            name='Rate Hikes (2022-2023)',
            volatility_shock=1.3,  # Modest volatility increase
            debt_shock=0.05,  # Debt up 5%
            rate_shock=0.04,  # Rates up 4%
            equity_shock=-0.15,  # Markets down 15%
            description='2022-2023 rate hiking cycle - rising rates and inflation'
        ),
        'MILD_RECESSION': StressScenario(
            name='Mild Recession',
            volatility_shock=1.5,
            debt_shock=0.10,
            rate_shock=0.0,
            equity_shock=-0.20,
            description='Generic mild recession scenario'
        ),
        'SEVERE_RECESSION': StressScenario(
            name='Severe Recession',
            volatility_shock=2.0,
            debt_shock=0.15,
            rate_shock=-0.02,
            equity_shock=-0.40,
            description='Severe recession with deep market decline'
        )
    }

    def __init__(self, solver: Optional[MertonSolver] = None):
        """
        Initialize stress tester.

        Args:
            solver: MertonSolver instance
        """
        self.solver = solver if solver is not None else MertonSolver()

    def test_scenario(
            self,
            scenario: StressScenario,
            base_params: Dict
    ) -> Dict:
        """
        Test a single stress scenario.

        Args:
            scenario: StressScenario to apply
            base_params: Base parameter values

        Returns:
            Dictionary with base and stressed results
        """
        # Solve for base case
        base_result = self.solver.solve(
            E=base_params['E'],
            sigma_E=base_params['sigma_E'],
            D=base_params['D'],
            r=base_params['r'],
            T=base_params['T']
        )

        if base_result['converged']:
            base_DD = calculate_dd_risk_neutral(
                base_result['V'],
                base_params['D'],
                base_result['sigma_V'],
                base_params['r'],
                base_params['T']
            )
            base_PD = calculate_pd_from_dd(base_DD)
        else:
            base_DD = np.nan
            base_PD = np.nan

        # Apply stress scenario
        stressed_params = scenario.apply(base_params)

        # Solve for stressed case
        stressed_result = self.solver.solve(
            E=stressed_params['E'],
            sigma_E=stressed_params['sigma_E'],
            D=stressed_params['D'],
            r=stressed_params['r'],
            T=stressed_params['T']
        )

        if stressed_result['converged']:
            stressed_DD = calculate_dd_risk_neutral(
                stressed_result['V'],
                stressed_params['D'],
                stressed_result['sigma_V'],
                stressed_params['r'],
                stressed_params['T']
            )
            stressed_PD = calculate_pd_from_dd(stressed_DD)
        else:
            stressed_DD = np.nan
            stressed_PD = np.nan

        # Calculate changes
        DD_change = stressed_DD - base_DD if not np.isnan(stressed_DD) else np.nan
        PD_change = stressed_PD - base_PD if not np.isnan(stressed_PD) else np.nan

        return {
            'scenario_name': scenario.name,
            'scenario_description': scenario.description,
            # Base case
            'base_V': base_result['V'],
            'base_sigma_V': base_result['sigma_V'],
            'base_DD': base_DD,
            'base_PD': base_PD,
            # Stressed case
            'stressed_V': stressed_result['V'],
            'stressed_sigma_V': stressed_result['sigma_V'],
            'stressed_DD': stressed_DD,
            'stressed_PD': stressed_PD,
            # Changes
            'DD_change': DD_change,
            'PD_change': PD_change,
            'PD_change_pct': (PD_change / base_PD * 100) if base_PD > 0 else np.nan,
            # Convergence
            'base_converged': base_result['converged'],
            'stressed_converged': stressed_result['converged']
        }

    def test_all_scenarios(
            self,
            base_params: Dict,
            scenarios: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Test multiple stress scenarios.

        Args:
            base_params: Base parameter values
            scenarios: List of scenario names (default: all historical)

        Returns:
            DataFrame with results for all scenarios
        """
        if scenarios is None:
            scenarios = list(self.HISTORICAL_SCENARIOS.keys())

        results = []

        for scenario_name in scenarios:
            if scenario_name not in self.HISTORICAL_SCENARIOS:
                print(f"Warning: Unknown scenario '{scenario_name}', skipping")
                continue

            scenario = self.HISTORICAL_SCENARIOS[scenario_name]
            result = self.test_scenario(scenario, base_params)
            results.append(result)

        return pd.DataFrame(results)


def run_stress_test(
        ticker: str,
        engine,
        scenarios: Optional[List[str]] = None,
        date: Optional[str] = None
) -> pd.DataFrame:
    """
    Run stress tests for a ticker.

    Args:
        ticker: Stock ticker
        engine: Database engine
        scenarios: List of scenario names (default: all)
        date: Specific date (default: most recent)

    Returns:
        DataFrame with stress test results
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
        'T': row['T']
    }

    # Run stress tests
    tester = StressTester()
    results = tester.test_all_scenarios(base_params, scenarios)

    # Add ticker and date
    results.insert(0, 'ticker', ticker)
    results.insert(1, 'date', row['date'])

    return results


# Example usage
if __name__ == "__main__":
    # Test stress scenarios
    base_params = {
        'E': 3_900_000_000_000,  # $3.9T (AAPL current)
        'sigma_E': 0.32,
        'D': 100_000_000_000,  # $100B debt
        'r': 0.04,
        'T': 1.0
    }

    tester = StressTester()
    results = tester.test_all_scenarios(base_params)

    print("\nStress Test Results:")
    print(results[['scenario_name', 'base_PD', 'stressed_PD', 'PD_change_pct']])