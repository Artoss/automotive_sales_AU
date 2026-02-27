-- Add content_hash column to scrape_runs for incremental mode
-- Stores a hash of the fetched content to detect changes between runs.

ALTER TABLE scrape_runs ADD COLUMN IF NOT EXISTS content_hash TEXT;
