-- Automotive Sales AU - Initial Schema
-- =====================================

-- Scrape run tracking
CREATE TABLE IF NOT EXISTS scrape_runs (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    records_count INTEGER DEFAULT 0,
    error_message TEXT
);

-- Marklines: monthly sales by make
CREATE TABLE IF NOT EXISTS marklines_sales (
    id SERIAL PRIMARY KEY,
    scrape_run_id INTEGER REFERENCES scrape_runs(id),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    make TEXT NOT NULL,
    units_sold INTEGER,
    source_url TEXT,
    UNIQUE(year, month, make)
);

CREATE INDEX IF NOT EXISTS idx_ml_sales_year_month ON marklines_sales(year, month);
CREATE INDEX IF NOT EXISTS idx_ml_sales_make ON marklines_sales(make);

-- Marklines: monthly totals from chart data
CREATE TABLE IF NOT EXISTS marklines_totals (
    id SERIAL PRIMARY KEY,
    scrape_run_id INTEGER REFERENCES scrape_runs(id),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    total_units INTEGER,
    source_url TEXT,
    UNIQUE(year, month)
);

-- FCAI: downloaded publications
CREATE TABLE IF NOT EXISTS fcai_publications (
    id SERIAL PRIMARY KEY,
    scrape_run_id INTEGER REFERENCES scrape_runs(id),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    filename TEXT NOT NULL,
    url TEXT NOT NULL,
    file_hash TEXT,
    file_size_bytes INTEGER,
    downloaded_at TIMESTAMPTZ DEFAULT NOW(),
    parsed BOOLEAN DEFAULT FALSE,
    UNIQUE(year, month, filename)
);

-- FCAI: extracted sales data
CREATE TABLE IF NOT EXISTS fcai_sales_data (
    id SERIAL PRIMARY KEY,
    publication_id INTEGER REFERENCES fcai_publications(id),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    make TEXT,
    model TEXT,
    segment TEXT,
    fuel_type TEXT,
    units_sold INTEGER,
    market_share NUMERIC(5,2),
    UNIQUE(year, month, make, model, segment)
);

CREATE INDEX IF NOT EXISTS idx_fcai_sales_year_month ON fcai_sales_data(year, month);
CREATE INDEX IF NOT EXISTS idx_fcai_sales_make ON fcai_sales_data(make);
CREATE INDEX IF NOT EXISTS idx_fcai_sales_segment ON fcai_sales_data(segment);
