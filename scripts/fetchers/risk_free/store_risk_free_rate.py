"""
scripts/fetchers/risk_free/store_risk_free_rate.py

Store risk-free rate data to database.
"""

from sqlalchemy import text
import pandas as pd


def store_risk_free_rate(df: pd.DataFrame, engine):
    """
    Store risk-free rate data with forward-fill for missing days.

    Args:
        df: DataFrame with columns: date, rate, source, ingested_at
        engine: Database engine
    """

    if df.empty:
        print("[WARN] No risk-free rate data to store")
        return

    # Ensure proper types
    df = df.copy()
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['rate'] = pd.to_numeric(df['rate'], errors='coerce')

    # Drop rows with missing rates
    df = df.dropna(subset=['rate'])

    insert_sql = text("""
        INSERT INTO risk_free_rate (
            date, rate, source, ingested_at
        )
        VALUES (
            :date, :rate, :source, :ingested_at
        )
        ON CONFLICT (date) 
        DO UPDATE SET
            rate = EXCLUDED.rate,
            source = EXCLUDED.source,
            ingested_at = EXCLUDED.ingested_at
    """)

    records = df.to_dict(orient='records')

    with engine.begin() as conn:
        conn.execute(insert_sql, records)

    print(f"[OK] Stored {len(records)} risk-free rate records")


def build_daily_risk_free_rate(engine) -> pd.DataFrame:
    """
    Build daily risk-free rate with forward-fill for missing days.

    Args:
        engine: Database engine

    Returns:
        DataFrame with daily risk-free rates
    """
    # Get all trading days from equity prices
    calendar_query = """
        SELECT DISTINCT trade_date as date
        FROM equity_prices_raw
        ORDER BY date
    """
    calendar = pd.read_sql_query(calendar_query, engine)
    calendar['date'] = pd.to_datetime(calendar['date'])

    # Get risk-free rate data
    rf_query = """
        SELECT date, rate
        FROM risk_free_rate
        ORDER BY date
    """
    rf_data = pd.read_sql_query(rf_query, engine, parse_dates=['date'])

    # Merge and forward-fill
    daily_rf = calendar.merge(rf_data, on='date', how='left')
    daily_rf['rate'] = daily_rf['rate'].ffill()

    # Backfill any leading NaNs with first available rate
    daily_rf['rate'] = daily_rf['rate'].bfill()

    return daily_rf