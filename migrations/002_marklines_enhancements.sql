-- Marklines Enhancements
-- ======================
-- Add share/YoY columns to marklines_sales,
-- new tables for vehicle type sales and commentary,
-- drop unused marklines_totals table.

-- Add columns to marklines_sales for share/YoY data
ALTER TABLE marklines_sales ADD COLUMN IF NOT EXISTS market_share NUMERIC(5,1);
ALTER TABLE marklines_sales ADD COLUMN IF NOT EXISTS units_sold_prev_year INTEGER;
ALTER TABLE marklines_sales ADD COLUMN IF NOT EXISTS yoy_pct NUMERIC(6,1);

-- Vehicle type breakdown per month
CREATE TABLE IF NOT EXISTS marklines_vehicle_type_sales (
    id SERIAL PRIMARY KEY,
    scrape_run_id INTEGER REFERENCES scrape_runs(id),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    vehicle_type TEXT NOT NULL,
    units_sold INTEGER,
    units_sold_prev_year INTEGER,
    yoy_pct NUMERIC(6,1),
    source_url TEXT,
    UNIQUE(year, month, vehicle_type)
);

CREATE INDEX IF NOT EXISTS idx_ml_vtype_year_month
    ON marklines_vehicle_type_sales(year, month);

-- Monthly text commentary from flash reports
CREATE TABLE IF NOT EXISTS marklines_commentary (
    id SERIAL PRIMARY KEY,
    scrape_run_id INTEGER REFERENCES scrape_runs(id),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    report_date TEXT,
    commentary TEXT NOT NULL,
    source_url TEXT,
    UNIQUE(year, month)
);

-- Drop unused marklines_totals table (no chart data exists in HTML)
DROP TABLE IF EXISTS marklines_totals;
