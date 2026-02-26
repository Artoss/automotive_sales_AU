-- FCAI article-based scraping tables
-- Stores articles, embedded images, and Vision LLM-extracted table data

CREATE TABLE IF NOT EXISTS fcai_articles (
    id SERIAL PRIMARY KEY,
    scrape_run_id INTEGER REFERENCES scrape_runs(id),
    url TEXT NOT NULL UNIQUE,
    slug TEXT NOT NULL,
    title TEXT NOT NULL,
    published_date DATE,
    year INTEGER,
    month INTEGER,
    article_text TEXT,
    is_sales_article BOOLEAN DEFAULT FALSE,
    scraped_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fcai_article_images (
    id SERIAL PRIMARY KEY,
    article_id INTEGER NOT NULL REFERENCES fcai_articles(id),
    image_url TEXT NOT NULL,
    image_filename TEXT NOT NULL,
    local_path TEXT,
    image_order INTEGER NOT NULL DEFAULT 0,
    image_label TEXT DEFAULT '',
    width INTEGER,
    height INTEGER,
    downloaded_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(article_id, image_url)
);

CREATE TABLE IF NOT EXISTS fcai_article_extracted_tables (
    id SERIAL PRIMARY KEY,
    image_id INTEGER NOT NULL REFERENCES fcai_article_images(id),
    table_index INTEGER DEFAULT 0,
    headers JSONB,
    row_data JSONB,
    dataframe_csv TEXT,
    extraction_method TEXT DEFAULT 'vision_llm',
    confidence NUMERIC(3,2) DEFAULT 0.85,
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);
