# scripts/check_raw_balance_sheet.py

from src.db.engine import ENGINE
from sqlalchemy import text

with ENGINE.connect() as conn:
    result = conn.execute(text("""
        SELECT ticker, report_date, payload
        FROM balance_sheet_raw
        WHERE ticker = 'AAPL'
        ORDER BY report_date DESC
        LIMIT 1
    """)).fetchone()

    if result:
        print(f"Ticker: {result[0]}")
        print(f"Report date: {result[1]}")
        print(f"\nAll payload keys:")

        payload = result[2]  # Already a dict, no json.loads needed

        for key in sorted(payload.keys()):
            value = payload[key]
            if 'debt' in key.lower() or 'debt' in key.lower():
                print(f"  {key}: {value}")

        print(f"\nAll keys (first 50):")
        for i, key in enumerate(sorted(payload.keys())[:50]):
            print(f"  {key}")
    else:
        print("No raw balance sheet data found")