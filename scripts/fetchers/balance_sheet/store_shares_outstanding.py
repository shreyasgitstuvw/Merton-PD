# scripts/fetchers/shares/store_shares_outstanding.py

from sqlalchemy import text
import pandas as pd


def store_shares_outstanding(df: pd.DataFrame, engine):
    """Idempotent storage of shares outstanding snapshots"""

    if df.empty:
        return

    insert_sql = text("""
        INSERT INTO shares_outstanding (
            ticker, as_of_date, shares_outstanding, source, ingested_at
        )
        VALUES (
            :ticker, :as_of_date, :shares_outstanding, :source, :ingested_at
        )
        ON CONFLICT (ticker, as_of_date) 
        DO UPDATE SET
            shares_outstanding = EXCLUDED.shares_outstanding,
            source = EXCLUDED.source,
            ingested_at = EXCLUDED.ingested_at
    """)

    records = df.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(insert_sql, records)

    print(f"[OK] Stored {len(records)} shares outstanding records")