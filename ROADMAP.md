# Roadmap - Scraper_0061_MotorVehicles

## Completed

- [x] Marklines HTML scraper (fetch, parse, load by make + vehicle type + commentary)
- [x] Marklines browser fallback via Playwright (optional, config toggle)
- [x] FCAI PDF pipeline (download, pdfplumber extraction, load by make/model/segment)
- [x] FCAI article scraper (listing pagination, sales article classification)
- [x] Vision LLM table extraction from article images (OpenRouter + Anthropic providers)
- [x] State/territory time-series derived from extracted article tables
- [x] Monthly `update` command with dynamic year scope and skip-processed logic
- [x] Structured Pydantic report models from update (JSON export, `summary_text()`)
- [x] CSV export for marklines_sales, fcai_sales_data, fcai_state_sales
- [x] Database migrations (5 numbered SQL files)
- [x] Core test suite (parser, catalog, PDF extraction, live DB integration)

## Short-term

### Fix Marklines current-year URL routing
The `update` command fetches `[current_year, current_year - 1]` but routes the current year through the historical URL template when it's not in `config.marklines.recent_years`, causing a 404. The current page already covers the latest data so no data is lost, but the error is noisy. Options:
- Have `compute_marklines_years()` only return previous year (current page handles current year)
- Or have the update step use the recent URL format (`base_url-{year}`) for current year

### Incremental mode
Config infrastructure exists (`run_mode: "full" | "incremental"`, `--mode` CLI flag, `get_publication_hash()` in DB) but no pipeline actually branches on it. Implement:
- Marklines: skip pages where data hasn't changed (compare page hash)
- FCAI PDFs: skip downloads where file hash matches DB record
- The `update` command already has skip-processed logic for articles; extend pattern to other pipelines

### Export: JSON and Excel formats
`ExportConfig.format` accepts `"csv"`, `"json"`, and `"excel"` but only CSV is implemented in the `export` command. Implement:
- JSON export (array of objects per table)
- Excel export (one sheet per source) -- requires adding `openpyxl` to dependencies

### Expand test coverage
Current tests cover parsing and catalog logic. Add:
- Unit tests for `update.py` report models and `compute_marklines_years()`
- Unit tests for `state_sales.py` extraction with fixture data
- Mock-based tests for update steps (mock DB + HTTP, verify report counts)
- Tests for `classify_sales_article()` edge cases

## Medium-term

### Prefect flow integration
`run_monthly_update()` returns a Pydantic `UpdateReport` by design. Wrap in a Prefect flow:
- Schedule monthly run (e.g. 5th of each month)
- Each step as a Prefect task for observability
- Retry/alert on step failures
- Store run artifacts in Prefect

### Slack notifications
Post `report.summary_text()` to a Slack channel after each update:
- Success: summary with record counts and coverage
- Failure: error details with step that failed
- Coverage gap alerts when new gaps appear

### Data quality checks
Add validation between extraction and loading:
- Flag months with unusually low/high record counts vs historical average
- Cross-check Marklines totals against sum of make-level records
- Detect duplicate or near-duplicate articles
- Validate state sales sum against national total from same article

### FCAI article image-free extraction
Some newer FCAI articles have no embedded images (text-only releases). Investigate:
- Whether table data is available in article HTML/text
- Fallback extraction from article body when no images present

## Long-term

### Historical coverage expansion
- Extend Marklines scraping to pre-2018 if data is available
- Backfill FCAI articles beyond current listing pagination depth
- Consolidate PDF and article pipelines into a unified FCAI time-series

### Dashboard / visualization layer
- Expose data via a lightweight API (FastAPI) or static site
- Time-series charts for national + state/territory trends
- Make/model market share analysis
- YoY comparison views

### Multi-country expansion
Marklines has similar pages for other countries. The scraper architecture (config-driven URL patterns, generic table parsing) could be extended to other markets with per-country config sections.
