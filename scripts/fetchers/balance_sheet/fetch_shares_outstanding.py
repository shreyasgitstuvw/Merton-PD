import yfinance as yf
import pandas as pd
from datetime import datetime


def fetch_shares_outstanding_history(ticker: str) -> pd.DataFrame:
    """
    Fetch historical shares outstanding from Yahoo Finance quarterly balance sheets.
    """
    ticker = ticker.upper()
    stock = yf.Ticker(ticker)

    # Get quarterly balance sheets (same as debt fetch)
    bs_quarterly = stock.get_balance_sheet(freq="quarterly")

    if bs_quarterly is None or bs_quarterly.empty:
        raise ValueError(f"No balance sheet data for {ticker}")

    df = bs_quarterly.T.reset_index().rename(columns={"index": "as_of_date"})

    # Try multiple field names (Yahoo inconsistent naming)
    shares_col = None
    for field in [
        "OrdinarySharesNumber",
        "ShareIssued",
        "Ordinary Shares Number",
        "Share Issued",
        "Common Stock Shares Outstanding"
    ]:
        if field in df.columns:
            shares_col = field
            break

    if shares_col is None:
        raise ValueError(f"Cannot find shares outstanding for {ticker}")

    result = df[["as_of_date", shares_col]].copy()
    result.columns = ["as_of_date", "shares_outstanding"]
    result["ticker"] = ticker
    result["source"] = "yahoo"
    result["ingested_at"] = datetime.utcnow()

    # Data type enforcement
    result["as_of_date"] = pd.to_datetime(result["as_of_date"]).dt.date
    result["shares_outstanding"] = pd.to_numeric(result["shares_outstanding"], errors="coerce")
    result = result.dropna(subset=["shares_outstanding"])

    return result[["ticker", "as_of_date", "shares_outstanding", "source", "ingested_at"]]