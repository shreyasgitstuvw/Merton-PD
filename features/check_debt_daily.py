# scripts/check_debt_daily.py
from src.db.engine import ENGINE
from sqlalchemy import text

with ENGINE.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM debt_daily")).scalar()
    print(f"debt_daily row count: {result}")
    
    if result > 0:
        sample = conn.execute(text("SELECT * FROM debt_daily LIMIT 5")).fetchall()
        print("Sample rows:")
        for row in sample:
            print(row)
    else:
        print("❌ debt_daily is EMPTY!")