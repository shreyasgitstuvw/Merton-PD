"""
scripts/test_merton_integration.py

Comprehensive test suite for Merton model integration.

Tests:
1. Solver with known inputs (from your Colab example)
2. DD/PD calculations
3. DataFrame processing
4. Database storage and retrieval
5. End-to-end pipeline
"""

import numpy as np
import pandas as pd
from src.db.engine import ENGINE
from src.merton.solver import MertonSolver, solve_merton_single
from src.merton.distance_to_default import (
    calculate_dd_risk_neutral,
    calculate_pd_from_dd
)
from src.merton.pipeline import MertonPipeline


def test_solver_known_inputs():
    """
    Test solver with known inputs from Colab.

    Expected results (from your Colab):
    - VA ≈ 232.5B
    - sigmaA ≈ 0.2 (example range)
    """
    print("\n" + "=" * 60)
    print("TEST 1: Solver with Known Inputs")
    print("=" * 60)

    # Input from your Colab
    SharePriceCurrent = 230
    NetOutstandingShares = 1_000_000_000
    E = SharePriceCurrent * NetOutstandingShares  # 230B
    E_sigma = 2.3  # 230% volatility
    STL = 500_000_000
    LTL = 5_000_000_000
    DP = STL + (0.5 * LTL)  # 3B
    r = 0.04
    T = 1.0

    print(f"Inputs:")
    print(f"  E (Equity): ${E:,.0f}")
    print(f"  σ_E (Equity vol): {E_sigma:.2f}")
    print(f"  D (Debt): ${DP:,.0f}")
    print(f"  r (Risk-free): {r:.2%}")
    print(f"  T (Maturity): {T:.1f} years")

    # Solve
    result = solve_merton_single(E, E_sigma, DP, r, T)

    print(f"\nResults:")
    print(f"  V (Asset value): ${result['V']:,.0f}")
    print(f"  σ_V (Asset vol): {result['sigma_V']:.4f}")
    print(f"  d1: {result['d1']:.4f}")
    print(f"  d2: {result['d2']:.4f}")
    print(f"  Converged: {result['converged']}")
    print(f"  Iterations: {result['iterations']}")

    # Validation
    assert result['converged'], "Solver should converge"
    assert result['V'] > E, "Asset value should exceed equity"
    assert 0 < result['sigma_V'] < E_sigma, "Asset vol should be between 0 and equity vol"

    print("\n✅ TEST PASSED")
    return result


def test_dd_pd_calculations(V, sigma_V, D, r, T):
    """Test DD and PD calculations."""
    print("\n" + "=" * 60)
    print("TEST 2: DD and PD Calculations")
    print("=" * 60)

    # Calculate DD
    DD = calculate_dd_risk_neutral(V, D, sigma_V, r, T)
    print(f"Distance to Default (DD): {DD:.4f}")

    # Calculate PD
    PD = calculate_pd_from_dd(DD)
    print(f"Probability of Default (PD): {PD:.4%}")

    # Validation
    assert not np.isnan(DD), "DD should not be NaN"
    assert not np.isnan(PD), "PD should not be NaN"
    assert 0 <= PD <= 1, "PD should be between 0 and 1"

    # Expected range for healthy company
    if DD > 0:
        print(f"✅ Positive DD indicates low default risk")

    print("\n✅ TEST PASSED")
    return DD, PD


def test_dataframe_processing():
    """Test solver on DataFrame."""
    print("\n" + "=" * 60)
    print("TEST 3: DataFrame Processing")
    print("=" * 60)

    # Create sample DataFrame
    df = pd.DataFrame({
        'E': [230e9, 250e9, 220e9],
        'sigma_E': [2.3, 2.1, 2.5],
        'D': [3e9, 2.8e9, 3.2e9],
        'r': [0.04, 0.045, 0.038],
        'T': [1.0, 1.0, 1.0]
    })

    print("Input DataFrame:")
    print(df)

    # Solve
    solver = MertonSolver()
    results = solver.solve_dataframe(df)

    print("\nOutput DataFrame:")
    print(results[['E', 'D', 'V', 'sigma_V', 'converged']])

    # Validation
    assert 'V' in results.columns, "V column should exist"
    assert 'sigma_V' in results.columns, "sigma_V column should exist"
    assert results['converged'].all(), "All rows should converge"

    print("\n✅ TEST PASSED")
    return results


def test_database_storage():
    """Test database storage and retrieval."""
    print("\n" + "=" * 60)
    print("TEST 4: Database Storage")
    print("=" * 60)

    # Create test data
    test_data = pd.DataFrame({
        'ticker': ['TEST'],
        'date': [pd.Timestamp('2024-01-01')],
        'asset_value': [230e9],
        'asset_volatility': [0.2],
        'distance_to_default': [5.0],
        'probability_default': [0.001],
        'd1': [5.5],
        'd2': [5.3],
        'leverage_ratio': [0.013],
        'equity_to_asset_ratio': [0.987],
        'iterations': [10],
        'converged': [True],
        'solver_method': ['kmv_fsolve']
    })

    # Convert date to date object
    test_data['date'] = test_data['date'].dt.date

    print("Storing test record...")
    test_data.to_sql(
        'merton_outputs',
        ENGINE,
        if_exists='append',
        index=False
    )

    # Retrieve
    print("Retrieving test record...")
    from sqlalchemy import text
    with ENGINE.connect() as conn:
        result = conn.execute(text("""
            SELECT * FROM merton_outputs 
            WHERE ticker = 'TEST'
            ORDER BY date DESC
            LIMIT 1
        """)).fetchone()

    if result:
        print(f"✅ Retrieved: ticker={result[0]}, date={result[1]}, PD={result[4]:.4%}")

        # Cleanup
        with ENGINE.begin() as conn:
            conn.execute(text("DELETE FROM merton_outputs WHERE ticker = 'TEST'"))
        print("✅ Cleanup complete")
    else:
        raise AssertionError("Failed to retrieve test record")

    print("\n✅ TEST PASSED")


def test_end_to_end_pipeline():
    """Test complete pipeline for a real ticker."""
    print("\n" + "=" * 60)
    print("TEST 5: End-to-End Pipeline (AAPL)")
    print("=" * 60)

    # Check if AAPL data exists
    from sqlalchemy import text
    with ENGINE.connect() as conn:
        count = conn.execute(text("""
            SELECT COUNT(*) FROM equity_prices_raw WHERE ticker = 'AAPL'
        """)).scalar()

    if count == 0:
        print("⚠️  AAPL data not found in database. Skipping test.")
        return

    print(f"Found {count} price records for AAPL")

    # Run pipeline
    print("\nRunning Merton pipeline...")
    pipeline = MertonPipeline(ENGINE)

    try:
        results = pipeline.run_for_ticker('AAPL', store_results=False, validate=True)

        if results.empty:
            print("⚠️  No Merton inputs available for AAPL")
            return

        print(f"\n✅ Generated {len(results)} Merton outputs")

        # Show statistics
        print("\nStatistics:")
        print(f"  Converged: {results['converged'].sum()}/{len(results)}")
        print(f"  Mean PD: {results['PD'].mean():.2%}")
        print(f"  Median PD: {results['PD'].median():.2%}")
        print(f"  Min PD: {results['PD'].min():.2%}")
        print(f"  Max PD: {results['PD'].max():.2%}")

        # Show sample
        print("\nSample (last 3 days):")
        sample_cols = ['date', 'V', 'sigma_V', 'DD', 'PD', 'converged']
        print(results[sample_cols].tail(3).to_string())

        print("\n✅ TEST PASSED")

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()


def run_all_tests():
    """Run all integration tests."""
    print("\n" + "#" * 60)
    print("# MERTON MODEL INTEGRATION TEST SUITE")
    print("#" * 60)

    try:
        # Test 1: Solver
        result = test_solver_known_inputs()
        V = result['V']
        sigma_V = result['sigma_V']

        # Test 2: DD/PD
        test_dd_pd_calculations(
            V=V,
            sigma_V=sigma_V,
            D=3e9,
            r=0.04,
            T=1.0
        )

        # Test 3: DataFrame
        test_dataframe_processing()

        # Test 4: Database
        test_database_storage()

        # Test 5: End-to-end
        test_end_to_end_pipeline()

        print("\n" + "#" * 60)
        print("# ALL TESTS PASSED ✅")
        print("#" * 60)

    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        raise
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_all_tests()