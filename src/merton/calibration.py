"""
src/merton/calibration.py

Logistic regression calibration for DD → PD mapping.

Maps theoretical Distance-to-Default to empirical default probabilities
using historical default data.
"""

import numpy as np
import pandas as pd
from typing import Optional, Dict, Tuple
from scipy.stats import norm

try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score, classification_report

    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("Warning: scikit-learn not installed. Calibration features limited.")


class PDCalibrator:
    """
    Calibrate Merton PD using logistic regression.
    """

    def __init__(self):
        """Initialize calibrator."""
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn required for calibration. Install with: pip install scikit-learn")

        self.model = None
        self.is_fitted = False
        self.coefficients = None

    def fit(
            self,
            DD_values: np.ndarray,
            default_flags: np.ndarray
    ) -> 'PDCalibrator':
        """
        Fit logistic regression model.

        Args:
            DD_values: Array of distance-to-default values
            default_flags: Array of default indicators (0/1)

        Returns:
            Self (fitted model)
        """
        # Reshape for sklearn
        X = DD_values.reshape(-1, 1)
        y = default_flags

        # Fit logistic regression
        self.model = LogisticRegression(random_state=42)
        self.model.fit(X, y)

        # Extract coefficients
        self.coefficients = {
            'intercept': float(self.model.intercept_[0]),
            'slope': float(self.model.coef_[0][0])
        }

        self.is_fitted = True

        return self

    def predict_pd(self, DD: float) -> float:
        """
        Predict calibrated PD for given DD.

        Args:
            DD: Distance to default

        Returns:
            Calibrated probability of default
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")

        DD_array = np.array([[DD]])
        pd_calibrated = self.model.predict_proba(DD_array)[0, 1]

        return float(pd_calibrated)

    def predict_pd_batch(self, DD_values: np.ndarray) -> np.ndarray:
        """
        Predict calibrated PD for array of DD values.

        Args:
            DD_values: Array of distance-to-default values

        Returns:
            Array of calibrated probabilities
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")

        X = DD_values.reshape(-1, 1)
        pd_calibrated = self.model.predict_proba(X)[:, 1]

        return pd_calibrated

    def compare_methods(
            self,
            DD_values: np.ndarray,
            default_flags: Optional[np.ndarray] = None
    ) -> pd.DataFrame:
        """
        Compare raw (Φ(-DD)) vs calibrated PD.

        Args:
            DD_values: Array of DD values
            default_flags: Optional actual default flags for comparison

        Returns:
            DataFrame with DD, raw PD, calibrated PD
        """
        # Raw PD (standard normal)
        pd_raw = norm.cdf(-DD_values)

        # Calibrated PD
        pd_calibrated = self.predict_pd_batch(DD_values)

        results = pd.DataFrame({
            'DD': DD_values,
            'PD_raw': pd_raw,
            'PD_calibrated': pd_calibrated
        })

        if default_flags is not None:
            results['default_flag'] = default_flags

        return results

    def get_coefficients(self) -> Dict:
        """Get fitted coefficients."""
        if not self.is_fitted:
            raise ValueError("Model must be fitted first")

        return self.coefficients.copy()


def create_synthetic_training_data(
        n_samples: int = 1000,
        default_rate: float = 0.02
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Create synthetic training data for demonstration.

    In production, replace with actual historical default data.

    Args:
        n_samples: Number of samples
        default_rate: Overall default rate

    Returns:
        (DD_values, default_flags) arrays
    """
    # Generate DD values (higher DD = safer)
    # Defaulted companies have lower DD
    n_defaults = int(n_samples * default_rate)
    n_non_defaults = n_samples - n_defaults

    # Defaults: DD typically 0-3
    DD_defaults = np.random.exponential(scale=1.0, size=n_defaults)
    DD_defaults = np.clip(DD_defaults, 0, 3)

    # Non-defaults: DD typically 3-10
    DD_non_defaults = np.random.normal(loc=6.0, scale=2.0, size=n_non_defaults)
    DD_non_defaults = np.clip(DD_non_defaults, 3, 12)

    # Combine
    DD_values = np.concatenate([DD_defaults, DD_non_defaults])
    default_flags = np.concatenate([
        np.ones(n_defaults),
        np.zeros(n_non_defaults)
    ])

    # Shuffle
    indices = np.random.permutation(n_samples)
    DD_values = DD_values[indices]
    default_flags = default_flags[indices]

    return DD_values, default_flags


def train_calibration_model(
        historical_data: Optional[pd.DataFrame] = None,
        use_synthetic: bool = False
) -> PDCalibrator:
    """
    Train PD calibration model.

    Args:
        historical_data: DataFrame with 'DD' and 'default_flag' columns
        use_synthetic: Use synthetic data if no historical data provided

    Returns:
        Fitted PDCalibrator
    """
    if historical_data is not None:
        DD_values = historical_data['DD'].values
        default_flags = historical_data['default_flag'].values
    elif use_synthetic:
        print("Using synthetic training data (replace with actual defaults in production)")
        DD_values, default_flags = create_synthetic_training_data()
    else:
        raise ValueError("Must provide historical_data or set use_synthetic=True")

    # Train model
    calibrator = PDCalibrator()
    calibrator.fit(DD_values, default_flags)

    # Print coefficients
    coefs = calibrator.get_coefficients()
    print(f"\nCalibration coefficients:")
    print(f"  Intercept (a): {coefs['intercept']:.3f}")
    print(f"  Slope (b): {coefs['slope']:.3f}")
    print(f"  Formula: PD = 1 / (1 + exp(-(a + b*DD)))")

    return calibrator


# Example usage
if __name__ == "__main__":
    # Train with synthetic data
    calibrator = train_calibration_model(use_synthetic=True)

    # Test predictions
    test_DD_values = np.array([2.0, 4.0, 6.0, 8.0, 10.0])

    print("\nComparison:")
    results = calibrator.compare_methods(test_DD_values)
    print(results)

    print("\nInterpretation:")
    print("- PD_raw: Theoretical PD from Φ(-DD)")
    print("- PD_calibrated: Empirically calibrated PD from historical defaults")