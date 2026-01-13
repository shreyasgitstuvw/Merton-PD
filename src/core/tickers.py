def normalize_ticker(ticker: str) -> str:
    return ticker.upper().split(".")[0]
