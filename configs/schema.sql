-- Schema for ELT pipeline: raw, staging, marts
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- ============================================================================
-- RAW LAYER: Generic storage with hash-based change detection
-- ============================================================================

-- Main raw data table (legacy name: raw.data)
CREATE TABLE IF NOT EXISTS raw.data (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    payload JSONB
);

-- New raw table for source events (recommended structure)
CREATE TABLE IF NOT EXISTS raw.source_events (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    sheet_row_number INTEGER,
    received_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB,
    payload_hash TEXT,
    last_seen TIMESTAMPTZ DEFAULT now()
);

-- Add hash tracking to existing raw.data table
ALTER TABLE raw.data
    ADD COLUMN IF NOT EXISTS payload_hash TEXT,
    ADD COLUMN IF NOT EXISTS last_seen TIMESTAMPTZ DEFAULT now();

-- Index for incremental processing
CREATE INDEX IF NOT EXISTS idx_raw_data_hash ON raw.data(payload_hash);
CREATE INDEX IF NOT EXISTS idx_raw_events_hash ON raw.source_events(payload_hash);
CREATE INDEX IF NOT EXISTS idx_raw_events_row_number ON raw.source_events(sheet_row_number);

-- ============================================================================
-- STAGING LAYER: Normalized data with business fields
-- ============================================================================

-- Legacy staging table (keep for compatibility) CREATE TABLE IF NOT EXISTS staging.data (
    id TEXT PRIMARY KEY,
    source TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    payload JSONB,
    payment_ts TIMESTAMPTZ,
    type_text TEXT,
    total_rub_num NUMERIC
);

-- New staging.records table with full field normalization
CREATE TABLE IF NOT EXISTS staging.records (
    raw_id BIGINT PRIMARY KEY,
    sheet_row_number INTEGER,
    received_at TIMESTAMPTZ,
    date TIMESTAMPTZ,
    payment_date TIMESTAMPTZ,
    task TEXT,
    type TEXT,
    year INTEGER,
    hours NUMERIC,
    month INTEGER,
    client TEXT,
    fx_rub NUMERIC,
    fx_usd NUMERIC,
    vendor TEXT,
    cashier TEXT,
    cat_new TEXT,
    quarter INTEGER,
    service TEXT,
    approver TEXT,
    category TEXT,
    currency TEXT,
    cat_final TEXT,
    total_rub NUMERIC,
    total_usd NUMERIC,
    subcat_new TEXT,
    paket TEXT,
    description TEXT,
    subcategory TEXT,
    payment_date_orig TIMESTAMPTZ,
    subcat_final TEXT,
    count_vendor INTEGER,
    statya TEXT,
    sum_total_rub NUMERIC,
    usd_summa NUMERIC,
    direct_indirect TEXT,
    package_secondary TEXT,
    total_in_currency NUMERIC,
    rub_summa NUMERIC,
    kategoriya TEXT,
    podstatya TEXT,
    vidy_raskhodov TEXT,
    payload_hash TEXT NOT NULL,
    raw_payload JSONB
);

-- Indexes for staging.data
CREATE INDEX IF NOT EXISTS idx_staging_payment_ts ON staging.data (payment_ts);
CREATE INDEX IF NOT EXISTS idx_staging_type_text ON staging.data (type_text);

-- Indexes for staging.records (for efficient queries and joins)
CREATE INDEX IF NOT EXISTS idx_staging_records_hash ON staging.records(payload_hash);
CREATE INDEX IF NOT EXISTS idx_staging_records_date ON staging.records(payment_date);
CREATE INDEX IF NOT EXISTS idx_staging_records_type ON staging.records(type);
CREATE INDEX IF NOT EXISTS idx_staging_records_category ON staging.records(category);
CREATE INDEX IF NOT EXISTS idx_staging_records_client ON staging.records(client);

-- ============================================================================
-- MARTS LAYER: Aggregated business metrics
-- ============================================================================

CREATE TABLE IF NOT EXISTS marts.campaigns_summary (
    campaign_id TEXT PRIMARY KEY,
    name TEXT,
    impressions BIGINT DEFAULT 0,
    clicks BIGINT DEFAULT 0,
    cost NUMERIC DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS marts.expenses_by_category (
    category TEXT PRIMARY KEY,
    total_rub NUMERIC DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT now()
);
