import pandas as pd
import yfinance as yf
from datetime import datetime


def fetch_balance_sheet_raw(ticker: str) -> pd.DataFrame:
    ticker = ticker.upper()
    stock = yf.Ticker(ticker)

    # ✅ TRY QUARTERLY FIRST (16-20 reports instead of 4-5)
    bs = stock.get_balance_sheet(freq="quarterly")

    # Fallback to yearly if quarterly fails
    if bs is None or bs.empty:
        bs = stock.get_balance_sheet(freq="yearly")

    if bs is None or bs.empty:
        raise ValueError(f"No balance sheet data for {ticker}")

    df = bs.T.reset_index().rename(columns={"index": "report_date"})
    df["ticker"] = ticker
    df["source"] = "yahoo"
    df["fetched_at"] = datetime.utcnow()

    return df


