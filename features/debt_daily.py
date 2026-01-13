import pandas as pd
from sqlalchemy import text


def get_trading_calendar(engine):
    """
    Trading calendar derived from actual equity price dates.
    No separate trading_calendar table needed.
    """
    query = """
        SELECT DISTINCT trade_date as date
        FROM equity_prices_raw
        ORDER BY date
    """
    return pd.read_sql_query(query, engine)


def get_balance_sheet_normalized(ticker, engine):
    query = text("""
        SELECT
            ticker,
            as_of_date,
            short_term_debt,
            long_term_debt,
            total_debt,
            source
        FROM balance_sheet_normalized
        WHERE ticker = :ticker
        ORDER BY as_of_date
    """)

    return pd.read_sql_query(
        query,
        engine,
        params={"ticker": ticker},
        parse_dates=["as_of_date"]
    )


def build_daily_debt(engine, balance_df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand balance sheet debt to daily frequency.
    """

    if balance_df.empty:
        raise ValueError("Normalized balance sheet is empty")

    balance_df = balance_df.copy()
    balance_df["as_of_date"] = pd.to_datetime(balance_df["as_of_date"])

    ticker = balance_df["ticker"].iloc[0]
    first_balance_date = balance_df["as_of_date"].min()

    # ✅ GET CALENDAR FROM EQUITY PRICES
    calendar_df = get_trading_calendar(engine)
    calendar_df = calendar_df.copy()
    calendar_df["date"] = pd.to_datetime(calendar_df["date"])

    # Only use calendar dates >= first balance sheet date
    calendar_df = calendar_df[calendar_df["date"] >= first_balance_date]

    # Merge (calendar → balance sheet)
    df = calendar_df.merge(
        balance_df,
        left_on="date",
        right_on="as_of_date",
        how="left"
    )

    # Forward fill debt values
    debt_cols = ["short_term_debt", "long_term_debt", "total_debt"]
    df[debt_cols] = df[debt_cols].ffill()

    # Drop rows where all debt values are still NULL
    df = df.dropna(subset=debt_cols, how='all')

    # Final schema
    df["ticker"] = ticker
    df["source"] = balance_df["source"].iloc[0]

    return df[
        [
            "ticker",
            "date",
            "short_term_debt",
            "long_term_debt",
            "total_debt",
            "source",
        ]
    ]


def store_debt_daily(df, engine):
    if df.empty:
        return

    if "as_of_date" in df.columns:
        df = df.drop(columns=["as_of_date"])

    # Remove duplicates within DataFrame
    df = df.drop_duplicates(subset=['ticker', 'date'], keep='last')

    with engine.connect() as conn:
        conn.execute(text("""
               DELETE FROM debt_daily
               WHERE ticker = :ticker
               AND date BETWEEN :start_date AND :end_date
           """), {
            "ticker": df["ticker"].iloc[0],
            "start_date": df["date"].min(),
            "end_date": df["date"].max()
        })
        conn.commit()

    # Optionally drop rows with NULLs
    df = df.dropna(subset=["ticker", "date"])

    # Ensure proper data types
    df["date"] = pd.to_datetime(df["date"]).dt.date

    df.to_sql(
        "debt_daily",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )




