# scripts/check_date_ranges.py

from src.db.engine import ENGINE
from sqlalchemy import text

def check_date_ranges():
    with ENGINE.connect() as conn:
        # Check equity prices
        prices = conn.execute(text("""
            SELECT ticker, MIN(trade_date) as min_date, MAX(trade_date) as max_date, COUNT(*) as count
            FROM equity_prices_raw
            GROUP BY ticker
        """)).fetchall()
        
        print("\n" + "="*60)
        print("EQUITY PRICES DATE RANGES")
        print("="*60)
        for row in prices:
            print(f"{row[0]}: {row[1]} to {row[2]} ({row[3]} days)")
        
        # Check balance sheets
        bs = conn.execute(text("""
            SELECT ticker, MIN(as_of_date) as min_date, MAX(as_of_date) as max_date, COUNT(*) as count
            FROM balance_sheet_normalized
            GROUP BY ticker
        """)).fetchall()
        
        print("\n" + "="*60)
        print("BALANCE SHEET DATE RANGES")
        print("="*60)
        for row in bs:
            print(f"{row[0]}: {row[1]} to {row[2]} ({row[3]} reports)")
        
        # Check shares outstanding
        shares = conn.execute(text("""
            SELECT ticker, MIN(as_of_date) as min_date, MAX(as_of_date) as max_date, COUNT(*) as count
            FROM shares_outstanding
            GROUP BY ticker
        """)).fetchall()
        
        print("\n" + "="*60)
        print("SHARES OUTSTANDING DATE RANGES")
        print("="*60)
        for row in shares:
            print(f"{row[0]}: {row[1]} to {row[2]} ({row[3]} reports)")

if __name__ == "__main__":
    check_date_ranges()