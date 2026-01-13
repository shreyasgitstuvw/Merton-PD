# scripts/fetchers/balance_sheet/store_balance_sheet_normalized.py

import pandas as pd
from sqlalchemy import text
from src.db.engine import ENGINE


def store_balance_sheet_normalized(df: pd.DataFrame, engine) -> None:
    """
    Persist normalized balance sheet rows into Postgres.
    Idempotent by construction.
    """

    if df.empty:
        print("[WARN] No rows to insert into balance_sheet_normalized")
        return

    insert_sql = text("""
        INSERT INTO balance_sheet_normalized (
            ticker,
            as_of_date,
            short_term_debt,
            long_term_debt,
            total_debt,
            normalization_method,
            source,
            created_at
        )
        VALUES (
            :ticker,
            :as_of_date,
            :short_term_debt,
            :long_term_debt,
            :total_debt,
            :normalization_method,
            :source,
            :created_at
        )
        ON CONFLICT (ticker, as_of_date, source, normalization_method)
        DO NOTHING
    """)

    records = df.to_dict(orient="records")

    with ENGINE.begin() as conn:
        conn.execute(insert_sql, records)

    print(f"[OK] Insert attempted for {len(records)} normalized balance sheet rows")
