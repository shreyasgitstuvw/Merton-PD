# scripts/test_equity_volatility.py

from src.db.engine import ENGINE
from features.daily_panel import build_daily_panel
from features.equity_volatility import add_volatility_to_panel, validate_volatility

TICKER = "AAPL"

if __name__ == "__main__":
    print(f"Building panel for {TICKER}...")
    panel = build_daily_panel(TICKER, ENGINE)

    print(f"\nAdding volatility...")
    panel_with_vol = add_volatility_to_panel(panel, method='rolling', window=252)

    print(f"\nVolatility stats:")
    vol_stats = panel_with_vol['equity_volatility'].describe()
    print(vol_stats)

    print(f"\nSample data:")
    print(panel_with_vol[['date', 'close', 'returns', 'equity_volatility']].tail(10))

    print(f"\nValidating...")
    validation = validate_volatility(panel_with_vol, TICKER)

    print(f"\nValidation result: {'✅ PASS' if validation['passed'] else '❌ FAIL'}")
    if validation.get('stats'):
        stats = validation['stats']
        print(f"  Min vol: {stats['min']:.2%}")
        print(f"  Max vol: {stats['max']:.2%}")
        print(f"  Mean vol: {stats['mean']:.2%}")
        print(f"  Valid: {stats['valid_pct']:.1f}%")