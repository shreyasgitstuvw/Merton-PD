# scripts/fetchers/balance_sheet/normalize_balance_sheet.py

import pandas as pd
from datetime import datetime, timezone

NORMALIZATION_METHOD = "raw_passthrough_v1"


def normalize_balance_sheet_raw(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize raw balance sheet data into a canonical debt snapshot format.

    Output contract (guaranteed):
        ticker              : str (upper)
        as_of_date           : date
        short_term_debt      : float | NaN
        long_term_debt       : float | NaN
        total_debt           : float | NaN
        normalization_method : str
        source               : str
        created_at           : tz-aware datetime (UTC)
    """

    if df_raw is None or df_raw.empty:
        raise ValueError("Raw balance sheet DataFrame is empty or None")

    df = df_raw.copy()

    # ------------------------------------------------------------------
    # Required identity columns (HARD CONTRACT)
    # ------------------------------------------------------------------
    required_cols = {"ticker", "report_date", "source"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required raw columns: {missing}")

    # ------------------------------------------------------------------
    # Canonical reporting date
    # ------------------------------------------------------------------
    df["as_of_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date

    if df["as_of_date"].isna().any():
        raise ValueError("Invalid report_date values detected")

    # ------------------------------------------------------------------
    # Short-term debt (ALWAYS a Series)
    # Try new field names first (no spaces), then legacy (with spaces)
    # ------------------------------------------------------------------
    if "CurrentDebt" in df.columns:
        short_term = pd.to_numeric(df["CurrentDebt"], errors="coerce")
    elif "Short Long Term Debt" in df.columns:
        short_term = pd.to_numeric(df["Short Long Term Debt"], errors="coerce")
    else:
        short_term = pd.Series(float("nan"), index=df.index, dtype="float64")

    # ------------------------------------------------------------------
    # Long-term debt (robust fallback chain)
    # ------------------------------------------------------------------
    if "LongTermDebt" in df.columns:
        long_term = pd.to_numeric(df["LongTermDebt"], errors="coerce")
    elif "Long Term Debt" in df.columns:
        long_term = pd.to_numeric(df["Long Term Debt"], errors="coerce")
    elif "Long Term Debt Noncurrent" in df.columns:
        long_term = pd.to_numeric(df["Long Term Debt Noncurrent"], errors="coerce")
    else:
        long_term = pd.Series(float("nan"), index=df.index, dtype="float64")

    # ------------------------------------------------------------------
    # Total debt (reported preferred, else constructed)
    # ------------------------------------------------------------------
    if "TotalDebt" in df.columns:
        total_reported = pd.to_numeric(df["TotalDebt"], errors="coerce")
    elif "Total Debt" in df.columns:
        total_reported = pd.to_numeric(df["Total Debt"], errors="coerce")
    else:
        total_reported = pd.Series(float("nan"), index=df.index, dtype="float64")

    components_present = short_term.notna() | long_term.notna()

    total_constructed = short_term.fillna(0) + long_term.fillna(0)
    total_constructed = total_constructed.where(components_present)

    total_debt = total_reported.combine_first(total_constructed)

    # ------------------------------------------------------------------
    # Assign normalized fields
    # ------------------------------------------------------------------
    df["short_term_debt"] = short_term
    df["long_term_debt"] = long_term
    df["total_debt"] = total_debt

    df["debt_coverage"] = (
            df["total_debt"].notna() |
            df["short_term_debt"].notna() |
            df["long_term_debt"].notna()
    )

    # ------------------------------------------------------------------
    # Metadata & identity propagation
    # ------------------------------------------------------------------
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["normalization_method"] = NORMALIZATION_METHOD
    df["created_at"] = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # Final schema guardrail (NEVER REMOVE)
    # ------------------------------------------------------------------
    final_cols = [
        "ticker",
        "as_of_date",
        "short_term_debt",
        "long_term_debt",
        "total_debt",
        "debt_coverage",
        "normalization_method",
        "source",
        "created_at",
    ]

    missing_final = set(final_cols) - set(df.columns)
    if missing_final:
        raise RuntimeError(f"Normalization bug — missing columns: {missing_final}")

    return df[final_cols].reset_index(drop=True)

