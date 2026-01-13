"""
Complete ingestion runner for Merton Model Day 5

Populates:
1. Equity prices (equity_prices_raw)
2. Balance sheets (balance_sheet_raw, balance_sheet_normalized, debt_daily)
3. Shares outstanding (shares_outstanding)
"""

from src.db.engine import ENGINE

# Balance sheet imports (Day 4)
from scripts.fetchers.balance_sheet.fetch_balance_sheet_raw import fetch_balance_sheet_raw
from scripts.fetchers.balance_sheet.store_balance_sheet import store_balance_sheet_raw
from scripts.fetchers.balance_sheet.normalize_balance_sheet import normalize_balance_sheet_raw
from scripts.fetchers.balance_sheet.store_balance_sheet_normalized import store_balance_sheet_normalized

# Shares outstanding imports (Day 5)
from scripts.fetchers.balance_sheet.fetch_shares_outstanding import fetch_shares_outstanding_history
from scripts.fetchers.balance_sheet.store_shares_outstanding import store_shares_outstanding

# Debt daily imports
from features.debt_daily import get_balance_sheet_normalized, build_daily_debt, store_debt_daily

# Equity price imports (you need to add these based on your Day 3 work)
import yfinance as yf
import pandas as pd
from datetime import datetime
from sqlalchemy import text


def fetch_and_store_equity_prices(ticker: str) -> None:
    """
    Fetch equity prices from Yahoo Finance and store to equity_prices_raw.
    """
    print(f"  [1/3] Fetching equity prices...")

    stock = yf.Ticker(ticker)
    df = stock.history(period="5y")  # Last 5 years

    if df.empty:
        raise ValueError(f"No price data for {ticker}")

    # Prepare for database
    df = df.reset_index()
    df['ticker'] = ticker
    df['source'] = 'yahoo'
    df['ingested_at'] = datetime.utcnow()

    # ✅ Handle both 'Date' and datetime index
    if 'Date' in df.columns:
        df = df.rename(columns={'Date': 'trade_date'})
    else:
        # Index is already a datetime
        df['trade_date'] = df.index

    # ✅ Rename columns with error handling
    column_mapping = {
        'Close': 'close',
        'Volume': 'volume'
    }
    df = df.rename(columns=column_mapping)

    # ✅ Add adj_close (with fallback)
    if 'Adj Close' in df.columns:
        df['adj_close'] = df['Adj Close']
    else:
        df['adj_close'] = df['close']

    # ✅ Ensure required columns exist
    required_cols = ['ticker', 'trade_date', 'close', 'volume']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns after processing: {missing_cols}")

    # Select final columns
    df = df[['ticker', 'trade_date', 'close', 'adj_close', 'volume', 'source', 'ingested_at']]
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

    # Store to database
    insert_sql = text("""
        INSERT INTO equity_prices_raw (ticker, trade_date, close, adj_close, volume, source, ingested_at)
        VALUES (:ticker, :trade_date, :close, :adj_close, :volume, :source, :ingested_at)
        ON CONFLICT (ticker, trade_date) DO UPDATE SET
            close = EXCLUDED.close,
            adj_close = EXCLUDED.adj_close,
            volume = EXCLUDED.volume,
            ingested_at = EXCLUDED.ingested_at
    """)

    records = df.to_dict(orient='records')

    with ENGINE.begin() as conn:
        conn.execute(insert_sql, records)

    print(f"    ✅ Stored {len(records)} price records")


def fetch_and_store_balance_sheet(ticker: str) -> None:
    """
    Fetch balance sheet → normalize → build daily debt
    """
    print(f"  [2/3] Fetching balance sheet...")

    # Fetch raw
    df_raw = fetch_balance_sheet_raw(ticker)
    print(f"    → Fetched {len(df_raw)} raw balance sheet rows")

    # Store raw
    store_balance_sheet_raw(ENGINE, df_raw)
    print(f"    → Stored raw balance sheet")

    # Normalize
    df_normalized = normalize_balance_sheet_raw(df_raw)
    print(f"    → Normalized {len(df_normalized)} rows")
    store_balance_sheet_normalized(df_normalized, ENGINE)
    print(f"    → Stored normalized balance sheet")

    # Build daily debt
    print(f"    → Building daily debt...")
    balance_df = get_balance_sheet_normalized(ticker, ENGINE)
    print(f"    → Retrieved {len(balance_df)} normalized rows from DB")

    df_daily = build_daily_debt(ENGINE, balance_df)
    print(f"    → Built {len(df_daily)} daily debt rows")

    store_debt_daily(df_daily, ENGINE)
    print(f"    → Stored daily debt")

    print(f"    ✅ Balance sheet complete")


def fetch_and_store_shares_outstanding(ticker: str) -> None:
    """
    Fetch shares outstanding snapshots
    """
    print(f"  [3/3] Fetching shares outstanding...")

    df = fetch_shares_outstanding_history(ticker)
    store_shares_outstanding(df, ENGINE)

    print(f"    ✅ Stored {len(df)} shares snapshots")


def run_complete_ingestion(ticker: str) -> None:
    """
    Run complete ingestion: prices + balance sheet + shares
    """
    print(f"\n{'=' * 60}")
    print(f"INGESTING: {ticker}")
    print(f"{'=' * 60}")

    try:
        # Step 1: Equity prices
        fetch_and_store_equity_prices(ticker)

        # Step 2: Balance sheet
        fetch_and_store_balance_sheet(ticker)

        # Step 3: Shares outstanding
        fetch_and_store_shares_outstanding(ticker)

        print(f"\n✅ {ticker} COMPLETE\n")

    except Exception as e:
        print(f"\n❌ {ticker} FAILED: {e}\n")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    tickers = ["AAPL", "MSFT", "TSLA", "JPM", "XOM"]

    print("\n" + "#" * 60)
    print("# COMPLETE DATA INGESTION")
    print("#" * 60)

    for ticker in tickers:
        run_complete_ingestion(ticker)

    print("#" * 60)
    print("# INGESTION COMPLETE")
    print("#" * 60)







