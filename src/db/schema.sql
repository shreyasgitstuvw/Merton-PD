-- ============================================================
-- MERTON MODEL SCHEMA v1.0
-- Principles: Immutability, Explicit Contracts, Layer Separation
-- ============================================================

-- ----------------------------
-- LAYER 1: RAW INGESTION (Immutable Yahoo Finance Data)
-- ----------------------------

-- Raw balance sheet (JSONB payload, zero assumptions)
CREATE TABLE IF NOT EXISTS balance_sheet_raw (
    ticker TEXT NOT NULL,
    report_date DATE NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,              -- Entire Yahoo response
    fetched_at TIMESTAMP NOT NULL,
    PRIMARY KEY (ticker, report_date, source)
);

CREATE INDEX IF NOT EXISTS idx_balance_sheet_raw_ticker
ON balance_sheet_raw(ticker);

COMMENT ON TABLE balance_sheet_raw IS
'Immutable storage of raw balance sheet API responses. No semantic interpretation.';

-- ----------------------------
-- LAYER 2: NORMALIZED DATA (Semantic Contract Layer)
-- ----------------------------

-- Normalized debt snapshot (explicit field extraction)
CREATE TABLE IF NOT EXISTS balance_sheet_normalized (
    ticker TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    source TEXT NOT NULL,
    normalization_method TEXT NOT NULL,  -- Tracks schema version

    -- Debt fields (NULLable = explicit "not available")
    short_term_debt NUMERIC,
    long_term_debt NUMERIC,
    total_debt NUMERIC,

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (ticker, as_of_date, source, normalization_method)
);

CREATE INDEX IF NOT EXISTS idx_balance_norm_ticker_date
ON balance_sheet_normalized(ticker, as_of_date DESC);

COMMENT ON TABLE balance_sheet_normalized IS
'Normalized debt extracted from raw balance sheets. Explicit field semantics.';

COMMENT ON COLUMN balance_sheet_normalized.normalization_method IS
'Tracks extraction logic version (e.g., raw_passthrough_v1). Allows schema evolution.';

-- ----------------------------
-- LAYER 3: DAILY FEATURES (Time-Aligned for Models)
-- ----------------------------

-- Daily debt (forward-filled from quarterly reports)
CREATE TABLE IF NOT EXISTS debt_daily (
    ticker TEXT NOT NULL,
    date DATE NOT NULL,

    -- Debt values (forward-filled from balance_sheet_normalized)
    short_term_debt NUMERIC,
    long_term_debt NUMERIC,
    total_debt NUMERIC,

    source TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_debt_daily_ticker_date
ON debt_daily(ticker, date DESC);

COMMENT ON TABLE debt_daily IS
'Daily debt levels via forward-fill from quarterly balance sheets. Direct input to Merton model.';

-- ----------------------------
-- SUPPORTING TABLES
-- ----------------------------

-- Trading calendar (market days only)
CREATE TABLE IF NOT EXISTS trading_calendar (
    date DATE PRIMARY KEY,
    is_trading_day BOOLEAN DEFAULT TRUE,
    exchange TEXT DEFAULT 'NYSE',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trading_calendar_date
ON trading_calendar(date DESC);

COMMENT ON TABLE trading_calendar IS
'Valid trading days. Used for daily debt alignment.';

-- ----------------------------
-- EQUITY DATA (Already exists, kept for completeness)
-- ----------------------------

CREATE TABLE IF NOT EXISTS equity_prices_raw (
    ticker TEXT NOT NULL,
    trade_date DATE NOT NULL,
    close NUMERIC,
    adj_close NUMERIC,
    volume BIGINT,
    source TEXT,
    ingested_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_equity_prices_date
ON equity_prices_raw(trade_date);


CREATE TABLE IF NOT EXISTS shares_outstanding (
    ticker TEXT NOT NULL,
    as_of_date DATE NOT NULL,  -- ✅ Time-aware
    shares_outstanding BIGINT NOT NULL,
    source TEXT,
    ingested_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ticker, as_of_date)  -- ✅ Composite key
);

-- ----------------------------
-- Risk-Free Rate (Daily)
-- ----------------------------
CREATE TABLE IF NOT EXISTS risk_free_rate (
    date DATE PRIMARY KEY,
    rate NUMERIC(10, 6) NOT NULL,  -- Stored as decimal (e.g., 0.045 for 4.5%)
    source TEXT,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_risk_free_rate_date
ON risk_free_rate(date DESC);

COMMENT ON TABLE risk_free_rate IS
'Daily risk-free rate (1-Year Treasury). Forward-filled for missing days.';
