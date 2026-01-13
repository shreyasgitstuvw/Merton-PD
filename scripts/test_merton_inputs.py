# scripts/test_merton_inputs.py

from src.db.engine import ENGINE
from features.merton_inputs import build_merton_inputs_all_tickers

TICKERS = ["AAPL", "MSFT", "TSLA", "JPM", "XOM"]

if __name__ == "__main__":
    print("=" * 60)
    print("BUILDING MERTON INPUTS FOR ALL TICKERS")
    print("=" * 60)

    results = build_merton_inputs_all_tickers(
        ENGINE,
        TICKERS,
        time_to_maturity=1.0,
        validate=True
    )

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for ticker, inputs in results.items():
        print(f"\n{ticker}:")
        print(f"  Rows: {len(inputs)}")
        print(f"  Date range: {inputs['date'].min().date()} to {inputs['date'].max().date()}")
        print(f"\n  Sample Merton inputs:")
        print(inputs[['date', 'E', 'D', 'sigma_E', 'r', 'T']].tail(3))