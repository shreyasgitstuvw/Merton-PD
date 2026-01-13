"""
features/daily_panel.py

Combine equity prices, debt, and shares outstanding into
aligned daily panels with explicit temporal constraints and staleness tracking.

Design principles:
- Use debt_daily (already forward-filled) instead of re-expanding quarterly
- Shares remain quarterly with merge_asof (correct)
- Staleness tracking only where needed
"""

import pandas as pd
from sqlalchemy import text
from features.forward_fill import validate_no_future_data


def get_equity_prices(ticker, engine):
    """Get daily equity prices"""
    query = text("""
        SELECT trade_date as date, close, adj_close, volume
        FROM equity_prices_raw
        WHERE ticker = :ticker
        ORDER BY trade_date
    """)
    df = pd.read_sql_query(query, engine, params={"ticker": ticker})
    df['date'] = pd.to_datetime(df['date'])
    return df


def get_debt_daily(ticker, engine):
    """
    Get DAILY debt (already forward-filled in debt_daily table).

    CRITICAL: This is the core fix. We use debt_daily, not balance_sheet_normalized.
    """
    query = text("""
        SELECT date, short_term_debt, long_term_debt, total_debt
        FROM debt_daily
        WHERE ticker = :ticker
        ORDER BY date
    """)
    df = pd.read_sql_query(query, engine, params={"ticker": ticker})
    df['date'] = pd.to_datetime(df['date'])
    return df


def get_shares_snapshots(ticker, engine):
    """Get shares outstanding snapshots (quarterly ONLY)"""
    query = text("""
        SELECT as_of_date, shares_outstanding
        FROM shares_outstanding
        WHERE ticker = :ticker
        ORDER BY as_of_date
    """)
    df = pd.read_sql_query(query, engine, params={"ticker": ticker})
    df['as_of_date'] = pd.to_datetime(df['as_of_date'])
    return df


def merge_quarterly_asof(daily_df, quarterly_df, value_cols, as_of_col='as_of_date'):
    """
    Merge quarterly data onto daily calendar with temporal constraints.
    """
    result = pd.merge_asof(
        daily_df.sort_values('date'),
        quarterly_df[[as_of_col] + value_cols].sort_values(as_of_col),
        left_on='date',
        right_on=as_of_col,
        direction='backward',
    )
    return result


def add_staleness_tracking(df, value_col, as_of_col):
    """Track staleness for a single column"""
    df = df.copy()
    staleness_col = f"{value_col}_staleness_days"
    df[staleness_col] = (df['date'] - df[as_of_col]).dt.days
    return df


def filter_panel_with_diagnostics(panel, required_cols, ticker):
    """Filter to complete rows with explicit diagnostic logging"""
    initial_count = len(panel)

    print(f"\n[DATA QUALITY] {ticker}")
    print(f"  Initial rows: {initial_count}")

    # Check each required column
    for col in required_cols:
        missing = panel[col].isna().sum()
        if missing > 0:
            pct = missing / initial_count * 100
            print(f"  {col}: {missing} missing ({pct:.1f}%)")

    # Filter
    complete_panel = panel.dropna(subset=required_cols)
    dropped = initial_count - len(complete_panel)

    if dropped > 0:
        pct = dropped / initial_count * 100
        print(f"  Dropped: {dropped} rows ({pct:.1f}%)")

    print(f"  Final rows: {len(complete_panel)}")

    if len(complete_panel) > 0:
        print(f"  Date range: {complete_panel['date'].min().date()} to {complete_panel['date'].max().date()}")

    return complete_panel


def build_daily_panel(ticker, engine, max_staleness_days=365, include_staleness_cols=False):
    """
    Build aligned daily feature panel.

    Key change: Uses debt_daily (already forward-filled) instead of re-expanding quarterly.
    """

    print(f"\n[BUILD PANEL] {ticker}")

    # Get data sources
    prices = get_equity_prices(ticker, engine)
    debt_daily = get_debt_daily(ticker, engine)  # ✅ USE DEBT_DAILY
    shares = get_shares_snapshots(ticker, engine)

    print(f"  Prices: {len(prices)} days")
    print(f"  Debt daily: {len(debt_daily)} days")  # ✅ CHANGED
    print(f"  Shares snapshots: {len(shares)} reports")

    # Start with equity prices
    panel = prices.copy()

    # ✅ MERGE DEBT DAILY (simple 1:1 join, no merge_asof needed)
    panel = panel.merge(debt_daily, on='date', how='left')

    # ✅ MERGE SHARES QUARTERLY (use merge_asof with temporal constraints)
    shares = shares.sort_values("as_of_date")
    shares = shares.drop_duplicates(subset="as_of_date", keep="last")

    shares_cols = ['shares_outstanding']
    panel = merge_quarterly_asof(panel, shares, shares_cols, as_of_col='as_of_date')
    panel = panel.rename(columns={'as_of_date': 'shares_as_of_date'})

    # ✅ VALIDATE NO FUTURE LEAKAGE (only for shares, debt already validated)
    validate_no_future_data(
        panel,
        value_col='shares_outstanding',
        as_of_col='shares_as_of_date',
        date_col='date'
    )

    # ✅ ADD STALENESS TRACKING (only for shares)
    panel = add_staleness_tracking(panel, 'shares_outstanding', 'shares_as_of_date')

    # Calculate derived features
    panel['ticker'] = ticker
    panel['market_cap'] = panel['close'] * panel['shares_outstanding']
    panel['equity_value'] = panel['market_cap']

    # Define output schema
    core_cols = [
        'ticker', 'date', 'close', 'adj_close',
        'shares_outstanding', 'total_debt',
        'short_term_debt', 'long_term_debt',
        'market_cap', 'equity_value'
    ]

    staleness_cols = [col for col in panel.columns if 'staleness' in col or 'as_of_date' in col]

    if include_staleness_cols:
        final_cols = core_cols + staleness_cols
    else:
        final_cols = core_cols

    # ✅ RELAXED: Only require close and total_debt (not shares)
    required_cols = ['close', 'total_debt']
    panel = filter_panel_with_diagnostics(panel, required_cols, ticker)

    return panel[final_cols]