# scripts/test_data_quality.py

from src.db.engine import ENGINE
from features.data_validation import validate_all_tickers

TICKERS = ["AAPL", "MSFT", "TSLA", "JPM", "XOM"]

if __name__ == "__main__":
    results = validate_all_tickers(ENGINE, TICKERS, verbose=True)

    # Save summary
    results.to_csv("data_quality_report.csv", index=False)
    print("\n✅ Report saved to data_quality_report.csv")