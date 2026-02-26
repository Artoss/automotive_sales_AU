-- Monthly new vehicle sales by State/Territory, extracted from FCAI articles.

CREATE TABLE IF NOT EXISTS fcai_state_sales (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    state TEXT NOT NULL,
    state_abbrev TEXT NOT NULL,
    units_sold INTEGER,
    units_sold_prev_year INTEGER,
    yoy_pct NUMERIC(6,2),
    source_table_id INTEGER REFERENCES fcai_article_extracted_tables(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (year, month, state)
);

CREATE INDEX IF NOT EXISTS idx_fcai_state_sales_period
    ON fcai_state_sales (year, month);
