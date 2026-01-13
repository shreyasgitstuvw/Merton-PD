import yfinance as yf
import pandas as pd
from datetime import date
from core.tickers import normalize_ticker
from db.connection import get_connection




def fetch_daily_prices(ticker: str, start="2000-01-01") -> pd.DataFrame:
    t = yf.Ticker(ticker)
    df = t.history(start=start, auto_adjust=False)

    if df.empty:
        raise ValueError(f"No price data for {ticker}")

    df = df.reset_index()
    df["ticker"] = normalize_ticker(ticker)

    return df[[
        "ticker",
        "Date",
        "Close",
        "Adj Close",
        "Volume"
    ]]


def fetch_shares_outstanding(ticker: str) -> dict:
    t = yf.Ticker(ticker)
    shares = t.info.get("sharesOutstanding")

    if shares is None:
        raise ValueError(f"No shares outstanding for {ticker}")

    return {
        "ticker": normalize_ticker(ticker),
        "shares_outstanding": shares,
        "source": "yahoo",
        "as_of": date.today()
    }


def save_prices(df: pd.DataFrame):
    conn = get_connection()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO equity_prices_raw
            (ticker, trade_date, close, adj_close, volume, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, trade_date) DO NOTHING
        """, (
            row["ticker"],
            row["Date"],
            row["Close"],
            row["Adj Close"],
            row["Volume"],
            "yahoo"
        ))

    conn.commit()


def save_shares(data: dict):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO shares_outstanding
        (ticker, shares_outstanding, source, as_of)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (ticker) DO UPDATE
        SET shares_outstanding = EXCLUDED.shares_outstanding,
            as_of = EXCLUDED.as_of,
            ingested_at = NOW()
    """, (
        data["ticker"],
        data["shares_outstanding"],
        data["source"],
        data["as_of"]
    ))

    conn.commit()


def run(tickers: list[str]):
    for t in tickers:
        print(f"Fetching {t}")

        prices = fetch_daily_prices(t)
        save_prices(prices)

        shares = fetch_shares_outstanding(t)
        save_shares(shares)

if __name__ == "__main__":
    tickers = ["AAPL", "MSFT", "JPM"]
    run(tickers)
