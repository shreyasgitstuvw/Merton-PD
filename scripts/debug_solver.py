"""
scripts/debug_solver.py

Debug Merton solver convergence issues.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import numpy as np
from scipy.stats import norm
from scipy.optimize import fsolve

print("=" * 60)
print("MERTON SOLVER DEBUG")
print("=" * 60)

# Test inputs (from Colab)
SharePriceCurrent = 230
NetOutstandingShares = 1_000_000_000
E = SharePriceCurrent * NetOutstandingShares
E_sigma = 2.3  # 230% - VERY HIGH!
STL = 500_000_000
LTL = 5_000_000_000
DP = STL + (0.5 * LTL)
r = 0.04
T = 1.0

print(f"\nInputs:")
print(f"  E (Equity): ${E:,.0f}")
print(f"  σ_E (Equity vol): {E_sigma:.2f} ({E_sigma * 100:.0f}%)")
print(f"  D (Debt): ${DP:,.0f}")
print(f"  r (Risk-free): {r:.2%}")
print(f"  T (Maturity): {T:.1f} years")
print(f"  Leverage (D/E): {DP / E:.4f}")


# Define equations
def equations(x):
    V, sigma_V = x

    if V <= 0 or sigma_V <= 0:
        return [1e9, 1e9]

    if V <= DP:
        return [1e9, 1e9]

    try:
        sqrt_T = np.sqrt(T)
        d1 = (np.log(V / DP) + (r + 0.5 * sigma_V ** 2) * T) / (sigma_V * sqrt_T)
        d2 = d1 - sigma_V * sqrt_T
    except (ValueError, ZeroDivisionError):
        return [1e9, 1e9]

    # Equation 1: E = V·N(d1) - D·exp(-rT)·N(d2)
    eq1 = V * norm.cdf(d1) - np.exp(-r * T) * DP * norm.cdf(d2) - E

    # Equation 2: σ_E = (V/E)·N(d1)·σ_V
    eq2 = (V / E) * norm.cdf(d1) * sigma_V - E_sigma

    return [eq1, eq2]


# Test different initial guesses
print("\n" + "=" * 60)
print("TESTING INITIAL GUESSES")
print("=" * 60)

initial_guesses = [
    ("Standard", [E + DP, 0.5]),
    ("Conservative", [E + DP, 0.1]),
    ("High Vol", [E + DP, 1.0]),
    ("Scaled by E/V", [E + DP, E_sigma * E / (E + DP)]),
]

for name, guess in initial_guesses:
    print(f"\n{name}: V={guess[0]:,.0f}, σ_V={guess[1]:.4f}")

    # Evaluate equations at initial guess
    residuals = equations(guess)
    print(f"  Residuals: eq1={residuals[0]:.2e}, eq2={residuals[1]:.2e}")

    # Try to solve
    try:
        sol = fsolve(equations, guess, xtol=1e-12, maxfev=2000, full_output=True)
        V, sigma_V = sol[0][0], sol[0][1]
        info = sol[1]
        ier = sol[2]
        msg = sol[3]

        if ier == 1:
            print(f"  ✅ CONVERGED: V=${V:,.0f}, σ_V={sigma_V:.4f}")
            print(f"     Iterations: {info['nfev']}")

            # Verify solution
            residuals_final = equations([V, sigma_V])
            print(f"     Final residuals: eq1={residuals_final[0]:.2e}, eq2={residuals_final[1]:.2e}")

            # Calculate d1, d2, PD
            d1 = (np.log(V / DP) + (r + 0.5 * sigma_V ** 2) * T) / (sigma_V * np.sqrt(T))
            d2 = d1 - sigma_V * np.sqrt(T)
            DD = (np.log(V / DP) + (r - 0.5 * sigma_V ** 2) * T) / (sigma_V * np.sqrt(T))
            PD = norm.cdf(-DD)

            print(f"     d1={d1:.4f}, d2={d2:.4f}")
            print(f"     DD={DD:.4f}, PD={PD:.4%}")
        else:
            print(f"  ❌ FAILED: {msg}")
            print(f"     Final values: V=${V:,.0f}, σ_V={sigma_V:.4f}")
    except Exception as e:
        print(f"  ❌ EXCEPTION: {e}")

# Check if the problem is the extremely high volatility
print("\n" + "=" * 60)
print("TESTING WITH NORMAL VOLATILITY (for comparison)")
print("=" * 60)

E_sigma_normal = 0.3  # 30% - more typical
print(f"\nUsing σ_E = {E_sigma_normal:.2f} ({E_sigma_normal * 100:.0f}%)")


def equations_normal(x):
    V, sigma_V = x

    if V <= 0 or sigma_V <= 0:
        return [1e9, 1e9]

    if V <= DP:
        return [1e9, 1e9]

    try:
        sqrt_T = np.sqrt(T)
        d1 = (np.log(V / DP) + (r + 0.5 * sigma_V ** 2) * T) / (sigma_V * sqrt_T)
        d2 = d1 - sigma_V * sqrt_T
    except (ValueError, ZeroDivisionError):
        return [1e9, 1e9]

    eq1 = V * norm.cdf(d1) - np.exp(-r * T) * DP * norm.cdf(d2) - E
    eq2 = (V / E) * norm.cdf(d1) * sigma_V - E_sigma_normal

    return [eq1, eq2]


guess = [E + DP, 0.15]
sol = fsolve(equations_normal, guess, xtol=1e-12, maxfev=2000, full_output=True)

if sol[2] == 1:
    V, sigma_V = sol[0][0], sol[0][1]
    print(f"✅ CONVERGED with normal volatility")
    print(f"   V=${V:,.0f}, σ_V={sigma_V:.4f}")

    d1 = (np.log(V / DP) + (r + 0.5 * sigma_V ** 2) * T) / (sigma_V * np.sqrt(T))
    d2 = d1 - sigma_V * np.sqrt(T)
    DD = (np.log(V / DP) + (r - 0.5 * sigma_V ** 2) * T) / (sigma_V * np.sqrt(T))
    PD = norm.cdf(-DD)

    print(f"   DD={DD:.4f}, PD={PD:.4%}")
else:
    print(f"❌ FAILED even with normal volatility")

print("\n" + "=" * 60)
print("DIAGNOSIS")
print("=" * 60)
print(f"""
The issue is likely:
1. Extremely high equity volatility (σ_E = {E_sigma:.2f} = {E_sigma * 100:.0f}%)
   - This is unrealistic for real companies (typically 15-50%)
   - Makes numerical optimization very difficult

2. Your test data may have σ_E in decimal form, not percentage
   - Try: E_sigma = 0.23 instead of 2.3

3. Check your actual data - equity volatility calculation
   - Should be annualized standard deviation of returns
   - Typical range: 0.15 to 0.50 (15% to 50%)

Recommendation:
- Use realistic test values: σ_E ≈ 0.20 to 0.40
- Check your volatility calculation in features/equity_volatility.py
""")