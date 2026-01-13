# features/debug_debt_daily.py

import pandas as pd
from src.db.engine import ENGINE
from features.debt_daily import get_trading_calendar, get_balance_sheet_normalized

ticker = "AAPL"

print("="*60)
print("DEBUG: build_daily_debt for AAPL")
print("="*60)

# Step 1: Check trading calendar
calendar = get_trading_calendar(ENGINE)
print(f"\n1. Trading calendar: {len(calendar)} days")
print(f"   Date range: {calendar['date'].min()} to {calendar['date'].max()}")
print(f"   First 3 dates:\n{calendar.head(3)}")

# Step 2: Check balance sheet
balance_df = get_balance_sheet_normalized(ticker, ENGINE)
print(f"\n2. Balance sheet normalized: {len(balance_df)} reports")
print(f"   Columns: {balance_df.columns.tolist()}")
print(f"   Date range: {balance_df['as_of_date'].min()} to {balance_df['as_of_date'].max()}")
print(f"\n   Data:\n{balance_df[['ticker', 'as_of_date', 'total_debt']]}")

# Step 3: Check first_balance_date filter
balance_df["as_of_date"] = pd.to_datetime(balance_df["as_of_date"])
first_balance_date = balance_df["as_of_date"].min()
print(f"\n3. First balance date: {first_balance_date}")

calendar["date"] = pd.to_datetime(calendar["date"])
filtered_calendar = calendar[calendar["date"] >= first_balance_date]
print(f"   Calendar after filter: {len(filtered_calendar)} days")

# Step 4: Check merge
merged = filtered_calendar.merge(
    balance_df,
    left_on="date",
    right_on="as_of_date",
    how="left"
)
print(f"\n4. After merge: {len(merged)} rows")
print(f"   Total debt non-null: {merged['total_debt'].notna().sum()}")

# Step 5: Check forward-fill
debt_cols = ["short_term_debt", "long_term_debt", "total_debt"]
merged[debt_cols] = merged[debt_cols].ffill()
print(f"\n5. After forward-fill:")
print(f"   Total debt non-null: {merged['total_debt'].notna().sum()}")

# Step 6: Check dropna
final = merged.dropna(subset=debt_cols, how='all')
print(f"\n6. After dropna(how='all'): {len(final)} rows")

print("\n" + "="*60)