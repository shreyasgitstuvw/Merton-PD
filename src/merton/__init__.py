# src/merton/__init__.py
"""
Merton structural credit model implementation.

This module provides:
- Asset value and volatility estimation
- Distance to Default (DD) calculation
- Probability of Default (PD) estimation
- End-to-end pipeline for credit risk analysis
- Bootstrap uncertainty quantification
- Sensitivity analysis
- Stress testing
- PD calibration
"""

from .solver import MertonSolver, solve_merton_single
from .distance_to_default import (
    calculate_dd_risk_neutral,
    calculate_dd_real_world,
    calculate_pd_from_dd,
    calculate_pd_risk_neutral,
    calculate_pd_real_world,
    estimate_mu_from_asset_series,
    shrink_mu,
    add_dd_pd_to_dataframe
)
from .pipeline import MertonPipeline, run_merton_pipeline
from .bootstrap import BootstrapUncertainty, run_bootstrap_analysis
from .sensitivity import SensitivityAnalyzer, run_sensitivity_analysis
from .stress_testing import StressTester, StressScenario, run_stress_test
from .calibration import PDCalibrator, train_calibration_model

__version__ = '1.0.0'

__all__ = [
    # Solver
    'MertonSolver',
    'solve_merton_single',

    # Distance to Default / Probability of Default
    'calculate_dd_risk_neutral',
    'calculate_dd_real_world',
    'calculate_pd_from_dd',
    'calculate_pd_risk_neutral',
    'calculate_pd_real_world',
    'estimate_mu_from_asset_series',
    'shrink_mu',
    'add_dd_pd_to_dataframe',

    # Pipeline
    'MertonPipeline',
    'run_merton_pipeline',

    # Bootstrap
    'BootstrapUncertainty',
    'run_bootstrap_analysis',

    # Sensitivity
    'SensitivityAnalyzer',
    'run_sensitivity_analysis',

    # Stress Testing
    'StressTester',
    'StressScenario',
    'run_stress_test',

    # Calibration
    'PDCalibrator',
    'train_calibration_model'
]