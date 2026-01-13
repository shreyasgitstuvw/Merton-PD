# scripts/test_risk_free_rate.py

from src.db.engine import ENGINE
from scripts.fetchers.risk_free.fetch_risk_free_rate import fetch_treasury_1y
from scripts.fetchers.risk_free.store_risk_free_rate import store_risk_free_rate, build_daily_risk_free_rate

if __name__ == "__main__":
    print("Fetching 1-Year Treasury rates...")
    df = fetch_treasury_1y(start_date="2020-01-01")
    print(f"✅ Fetched {len(df)} records")
    print(df.head())

    print("\nStoring to database...")
    store_risk_free_rate(df, ENGINE)

    print("\nBuilding daily risk-free rate...")
    daily_rf = build_daily_risk_free_rate(ENGINE)
    print(f"✅ Built {len(daily_rf)} daily records")
    print(daily_rf.head())
    print(f"\nRate range: {daily_rf['rate'].min():.4f} to {daily_rf['rate'].max():.4f}")