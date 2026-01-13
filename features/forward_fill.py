# features/forward_fill.py

"""
Explicit forward-fill utilities with temporal safeguards.

Design principles:
- Forward-fill only AFTER first valid observation
- Explicit ordering and bounds
- No silent leakage
"""

import pandas as pd


def forward_fill_bounded(df, value_cols, date_col='date'):
    """
    Forward-fill with explicit bounds: only fill AFTER first valid observation.

    This prevents:
    - Filling backward in time (leakage)
    - Unbounded forward propagation before data exists

    Args:
        df: DataFrame to forward-fill
        value_cols: List of columns to forward-fill
        date_col: Date column name (must be sorted ascending)

    Returns:
        DataFrame with bounded forward-fill applied

    Example:
        >>> df = pd.DataFrame({
        ...     'date': ['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04'],
        ...     'debt': [None, None, 100, None]
        ... })
        >>> forward_fill_bounded(df, ['debt'])
        # debt will be: [None, None, 100, 100]  (not [100, 100, 100, 100])
    """
    df = df.sort_values(date_col).reset_index(drop=True)

    for col in value_cols:
        # Find first valid index
        first_valid_idx = df[col].first_valid_index()

        if first_valid_idx is None:
            # No valid data at all - leave as NaN
            continue

        # Only forward-fill AFTER first valid observation
        df.loc[first_valid_idx:, col] = df.loc[first_valid_idx:, col].ffill()

    return df


def forward_fill_with_staleness_limit(df, value_cols, date_col='date', max_days=365):
    """
    Forward-fill with staleness limit.

    Sets values to NaN if they haven't been updated in max_days.

    Args:
        df: DataFrame (must be sorted by date_col)
        value_cols: Columns to forward-fill
        date_col: Date column
        max_days: Maximum staleness in days

    Returns:
        DataFrame with staleness-limited forward-fill
    """
    df = df.sort_values(date_col).reset_index(drop=True)
    df[date_col] = pd.to_datetime(df[date_col])

    for col in value_cols:
        # Track when each value was last updated
        as_of_col = f"__{col}_as_of"
        df[as_of_col] = df[date_col].where(df[col].notna())

        # Forward-fill the as_of dates
        df[as_of_col] = df[as_of_col].ffill()

        # Calculate staleness
        staleness = (df[date_col] - df[as_of_col]).dt.days

        # Forward-fill the values
        df[col] = df[col].ffill()

        # Set to NaN if too stale
        df.loc[staleness > max_days, col] = None

        # Clean up temporary column
        df = df.drop(columns=[as_of_col])

    return df


def validate_no_future_data(df, value_col, as_of_col, date_col='date'):
    """
    Validate that no data point has as_of_date > date (future leakage).

    Raises ValueError if future data detected.
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df[as_of_col] = pd.to_datetime(df[as_of_col])

    future_mask = df[as_of_col] > df[date_col]

    if future_mask.any():
        problematic_rows = df[future_mask]
        raise ValueError(
            f"Future data detected in {value_col}:\n"
            f"{problematic_rows[[date_col, as_of_col, value_col]]}"
        )

    return True