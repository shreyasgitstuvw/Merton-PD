import json
import pandas as pd
from sqlalchemy import text


def store_balance_sheet_raw(engine, df: pd.DataFrame) -> None:
    """
    RAW balance sheet ingestion.
    Stores Yahoo output as JSONB with minimal coercion.
    """

    if df.empty:
        return

    records = []

    for _, row in df.iterrows():
        payload = (
            row.drop(["ticker", "report_date", "source", "fetched_at"])
               .where(pd.notnull(row), None)
               .to_dict()
        )

        records.append({
            "ticker": str(row["ticker"]),
            "report_date": row["report_date"].date(),        # MUST be datetime.date
            "source": str(row["source"]),
            "payload": json.dumps(payload),                  # MUST be string
            "fetched_at": row["fetched_at"].to_pydatetime()  # MUST be datetime
        })

    sql = text("""
        INSERT INTO balance_sheet_raw
        (ticker, report_date, source, payload, fetched_at)
        VALUES
        (:ticker, :report_date, :source, :payload, :fetched_at)
        ON CONFLICT (ticker, report_date, source) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(sql, records)


