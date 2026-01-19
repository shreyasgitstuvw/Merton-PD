"""
scripts/test_advanced_features.py

Quick test of all advanced features with fast execution.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.db.engine import ENGINE
from src.merton.sensitivity import run_sensitivity_analysis
from src.merton.stress_testing import run_stress_test
from src.merton.bootstrap import run_bootstrap_analysis

print("="*60)
print("TESTING ADVANCED FEATURES")
print("="*60)

ticker = 'AAPL'

# Test 1: Sensitivity (Fast)
print("\n1. Testing Sensitivity Analysis...")
try:
    sens_results = run_sensitivity_analysis(ticker, ENGINE)
    print(f"   [OK] Sensitivity: {len(sens_results)} result sets")
    print(f"   - Volatility: {len(sens_results['volatility'])} points")
    print(f"   - Debt: {len(sens_results['debt'])} points")
    print(f"   - Rate: {len(sens_results['rate'])} points")
except Exception as e:
    print(f"   [FAIL] {e}")

# Test 2: Stress Testing (Fast)
print("\n2. Testing Stress Testing...")
try:
    stress_results = run_stress_test(ticker, ENGINE)
    print(f"   [OK] Stress tests: {len(stress_results)} scenarios")
    for _, row in stress_results.iterrows():
        print(f"   - {row['scenario_name']}: PD {row['base_PD']:.2e} -> {row['stressed_PD']:.2e}")
except Exception as e:
    print(f"   [FAIL] {e}")

# Test 3: Bootstrap (Slow - only 100 iterations)
print("\n3. Testing Bootstrap (100 iterations, last 5 rows only)...")
try:
    bootstrap_results = run_bootstrap_analysis(ticker, ENGINE, n_iterations=100)
    print(f"   [OK] Bootstrap: {len(bootstrap_results)} rows")
    recent = bootstrap_results.iloc[-1]
    print(f"   - PD Median: {recent['PD_median']:.4%}")
    print(f"   - PD 95% CI: [{recent['PD_lower']:.4%}, {recent['PD_upper']:.4%}]")
except Exception as e:
    print(f"   [FAIL] {e}")

print("\n" + "="*60)
print("TESTING COMPLETE")
print("="*60)
print("\nIf all tests passed, advanced features are working!")
print("Run full analysis with: python scripts/run_advanced_analysis.py AAPL --all")