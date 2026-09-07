-- ==============================================================================
-- STAR ASIA MARITIME INTELLIGENCE CORPUS EXTRACTION SCHEMA
-- Production DDL: SQLite & PostgreSQL Compatible
-- ==============================================================================

-- 1. Pipeline Execution Runs
CREATE TABLE IF NOT EXISTS extraction_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    run_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    parser_version VARCHAR(32) NOT NULL,
    total_issues INT NOT NULL,
    tables_expected INT NOT NULL,
    tables_parsed INT NOT NULL,
    tables_failed INT NOT NULL,
    cells_expected INT NOT NULL,
    cells_parsed INT NOT NULL,
    cells_failed INT NOT NULL,
    status VARCHAR(32) NOT NULL
);

-- 2. Document Inventory & Metadata
CREATE TABLE IF NOT EXISTS market_reports (
    issue_id VARCHAR(128) PRIMARY KEY,
    broker VARCHAR(64) NOT NULL DEFAULT 'STAR_ASIA',
    report_date DATE,
    year INT NOT NULL,
    week INT NOT NULL,
    doc_type VARCHAR(32) NOT NULL,
    num_pages INT NOT NULL,
    file_path TEXT NOT NULL,
    hash_sha256 VARCHAR(64),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3. Canonical Entities & Alias Mapping (Child Table)
CREATE TABLE IF NOT EXISTS canonical_entities (
    canonical_id VARCHAR(64) PRIMARY KEY,
    entity_name VARCHAR(128) NOT NULL,
    entity_type VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS entity_aliases (
    alias_id VARCHAR(64) PRIMARY KEY,
    canonical_id VARCHAR(64) NOT NULL REFERENCES canonical_entities(canonical_id) ON DELETE CASCADE,
    alias_name VARCHAR(128) NOT NULL UNIQUE,
    entity_type VARCHAR(32) NOT NULL
);

-- 4. Baltic Indices (Dry & Tanker)
CREATE TABLE IF NOT EXISTS baltic_indices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    index_name VARCHAR(64) NOT NULL,
    current_value NUMERIC(10, 2),
    change_val NUMERIC(10, 2),
    change_pct NUMERIC(6, 2),
    yoy_pct NUMERIC(6, 2),
    raw_text TEXT,
    CONSTRAINT uq_baltic_index UNIQUE (issue_id, index_name)
);

-- 5. Vessel Valuations (Dry, Tanker, Container)
CREATE TABLE IF NOT EXISTS vessel_valuations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    sector VARCHAR(32) NOT NULL,
    vessel_class VARCHAR(64) NOT NULL,
    age_category VARCHAR(32) NOT NULL,
    value_usd_m NUMERIC(10, 2),
    CONSTRAINT uq_vessel_valuation UNIQUE (issue_id, sector, vessel_class, age_category)
);

-- 6. Time Charter Averages
CREATE TABLE IF NOT EXISTS time_charter_averages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    sector VARCHAR(32) NOT NULL,
    vessel_class VARCHAR(64) NOT NULL,
    duration VARCHAR(32) NOT NULL,
    rate_usd_day NUMERIC(12, 2),
    CONSTRAINT uq_tc_averages UNIQUE (issue_id, sector, vessel_class, duration)
);

-- 7. Sale & Purchase Reported Fixtures (Split built_year and builder_country)
CREATE TABLE IF NOT EXISTS sale_purchase_fixtures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    sector VARCHAR(32) NOT NULL,
    vessel_name VARCHAR(128) NOT NULL,
    vessel_type VARCHAR(64),
    dwt NUMERIC(12, 2),
    built_year INT,
    builder_country VARCHAR(64),
    price_usd_m NUMERIC(10, 2),
    buyers VARCHAR(128),
    comments TEXT,
    CONSTRAINT uq_sp_fixture UNIQUE (issue_id, vessel_name, dwt)
);

-- 8. Demolition Current Market Snapshot (Prices only, sentiment moved to yard table)
CREATE TABLE IF NOT EXISTS demolition_current_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    destination VARCHAR(64) NOT NULL,
    vessel_type VARCHAR(64) NOT NULL,
    price_low NUMERIC(10, 2),
    price_high NUMERIC(10, 2),
    CONSTRAINT uq_demo_snapshot UNIQUE (issue_id, destination, vessel_type)
);

-- 9. Demolition Yard Sentiment & Status (Yard-Level Decoupled)
CREATE TABLE IF NOT EXISTS demolition_yard_sentiment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    destination VARCHAR(64) NOT NULL,
    sentiment VARCHAR(32),
    yard_commentary TEXT,
    CONSTRAINT uq_demo_yard_sentiment UNIQUE (issue_id, destination)
);

-- 10. Demolition 5-Year Historical Average Prices
CREATE TABLE IF NOT EXISTS demolition_historical_averages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    destination VARCHAR(64) NOT NULL,
    year_label VARCHAR(16) NOT NULL,
    price_usd_ldt NUMERIC(10, 2),
    CONSTRAINT uq_demo_hist_avg UNIQUE (issue_id, destination, year_label)
);

-- 11. Demolition Reported Sales
CREATE TABLE IF NOT EXISTS demolition_reported_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    vessel_name VARCHAR(128) NOT NULL,
    vessel_type VARCHAR(64),
    ldt NUMERIC(12, 2),
    price_usd_ldt NUMERIC(10, 2),
    destination VARCHAR(64),
    comments TEXT,
    CONSTRAINT uq_demo_sales UNIQUE (issue_id, vessel_name, ldt)
);

-- 12. Anchorage & Beaching Records (Alang, Chattogram, Gaddani)
CREATE TABLE IF NOT EXISTS anchorage_beaching_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    location VARCHAR(64) NOT NULL,
    vessel_name VARCHAR(128) NOT NULL,
    vessel_type VARCHAR(64),
    ldt NUMERIC(12, 2),
    arrival_date DATE,
    beaching_date DATE,
    status VARCHAR(64),
    CONSTRAINT uq_anchorage_beaching UNIQUE (issue_id, location, vessel_name, ldt)
);

-- 13. Commodity Rates (Iron Ore, Industrial Metals, Crude & Gas)
CREATE TABLE IF NOT EXISTS commodity_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    commodity_category VARCHAR(64) NOT NULL,
    item_name VARCHAR(64) NOT NULL,
    price_usd NUMERIC(12, 4),
    unit VARCHAR(32),
    raw_price NUMERIC(12, 2),
    raw_unit VARCHAR(32),
    contract VARCHAR(32),
    change_val NUMERIC(10, 2),
    change_pct NUMERIC(6, 2),
    CONSTRAINT uq_commodity_rates UNIQUE (issue_id, item_name)
);

-- 14. Foreign Exchange Rates
CREATE TABLE IF NOT EXISTS foreign_exchange_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    currency_pair VARCHAR(32) NOT NULL,
    rate NUMERIC(12, 4),
    change_val NUMERIC(10, 4),
    change_pct NUMERIC(6, 2),
    CONSTRAINT uq_fx_rates UNIQUE (issue_id, currency_pair)
);

-- 15. Bunker Fuel Prices
CREATE TABLE IF NOT EXISTS bunker_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    port VARCHAR(64) NOT NULL,
    fuel_grade VARCHAR(32) NOT NULL,
    price_usd_mt NUMERIC(10, 2),
    change_val NUMERIC(10, 2),
    CONSTRAINT uq_bunker_prices UNIQUE (issue_id, port, fuel_grade)
);

-- 16. Audit Log: Completeness Invariant Tracking
CREATE TABLE IF NOT EXISTS extraction_audit_log (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    table_name VARCHAR(64) NOT NULL,
    page_num INT,
    cells_expected INT NOT NULL DEFAULT 0,
    cells_parsed INT NOT NULL DEFAULT 0,
    cells_failed INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    error_message TEXT
);

-- 17. Validation Violations Log
CREATE TABLE IF NOT EXISTS validation_violations (
    violation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
    rule_id VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    page_num INT,
    field_name VARCHAR(64),
    source_value TEXT,
    expected_behavior TEXT,
    remediation_applied TEXT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_sp_vessel ON sale_purchase_fixtures(vessel_name);
CREATE INDEX IF NOT EXISTS idx_demo_sales_vessel ON demolition_reported_sales(vessel_name);
CREATE INDEX IF NOT EXISTS idx_anchorage_vessel ON anchorage_beaching_records(vessel_name);
CREATE INDEX IF NOT EXISTS idx_audit_issue ON extraction_audit_log(issue_id);
CREATE INDEX IF NOT EXISTS idx_violations_issue ON validation_violations(issue_id);