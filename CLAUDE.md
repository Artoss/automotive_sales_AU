# Scraper_0061_MotorVehicles

Australian automotive sales data scraper from two sources.

## Sources

### Marklines (HTML + JS)
- **Base URL**: `https://www.marklines.com/en/statistics/flash_sales/automotive-sales-in-australia-by-month`
- **Recent years (2021+)**: append `-{year}` to base URL
- **Historical (2020-)**: `...salesfig_australia_{year}` (different URL pattern)
- Monthly sales by make (manufacturer) in HTML tables
- Vehicle type breakdown (passenger, SUV, light/heavy commercial)
- Monthly commentary text from flash reports
- Tables parsed with BeautifulSoup; odd-indexed tables contain data rows

### FCAI Articles (HTML + Vision LLM)
- **Listing URL**: `https://www.fcai.com.au/news-and-media/?_sft_category=media-release`
- Monthly media release articles with embedded table images
- Images extracted via Vision LLM (OpenRouter/Anthropic) to structured table data
- State/territory sales breakdown derived from extracted tables
- Article classification via keyword matching (`classify_sales_article()`)

### FCAI PDFs (historical backfill)
- **URL pattern**: `https://www.fcai.com.au/library/publication/{month}_{year}_vfacts_media_release_and_industry_summary.pdf`
- Monthly PDFs with sales by make/model/segment
- Segmentation: passenger, SUV, light commercial, heavy commercial
- Used for historical backfill only; articles pipeline is preferred for ongoing updates

## Database

- **Name**: `automotive_sales_au`
- **Tables**:
  - `scrape_runs` - run history tracking (source, status, timestamps, record counts)
  - `marklines_sales` - monthly sales by make (unique on year/month/make)
  - `marklines_vehicle_type_sales` - vehicle type breakdown (unique on year/month/vehicle_type)
  - `marklines_commentary` - monthly report commentary (unique on year/month)
  - `fcai_publications` - PDF publication metadata (unique on year/month/filename)
  - `fcai_sales_data` - sales from PDFs (unique on year/month/make/model/segment)
  - `fcai_articles` - article metadata (unique on url)
  - `fcai_article_images` - images from articles (unique on article_id/image_url)
  - `fcai_article_extracted_tables` - Vision LLM extracted tables (unique on image_id/table_index)
  - `fcai_state_sales` - state/territory time-series (unique on year/month/state)

## CLI Commands

```
motor-vehicles update              # Monthly update (recommended for routine use)
motor-vehicles marklines run       # Full Marklines pipeline (config-driven years)
motor-vehicles marklines download  # Fetch and save Marklines HTML locally
motor-vehicles marklines parse     # Parse saved HTML files (no DB)
motor-vehicles fcai run            # Full FCAI PDF pipeline
motor-vehicles fcai download       # Download FCAI PDFs
motor-vehicles fcai parse          # Extract from downloaded PDFs (no DB)
motor-vehicles fcai articles       # Article pipeline (--url, --list-only, --process-all)
motor-vehicles fcai build-state-sales  # Extract state/territory data from article tables
motor-vehicles run                 # Both Marklines + FCAI PDF pipelines
motor-vehicles migrate             # Run SQL migrations
motor-vehicles status              # Show scrape history and DB stats
motor-vehicles export              # Export to CSV/JSON/Excel (--source, --format)
```

## Monthly Update Process

Run `uv run motor-vehicles update` for routine monthly updates.

**What it does (4 steps):**
1. **Marklines** - Fetches current + previous year pages (dynamically scoped, no config changes needed)
2. **FCAI Articles** - Fetches article listings, skips already-processed URLs, processes new articles via Vision LLM or HTML table fallback
3. **State Sales** - Re-extracts state/territory time-series from all article tables (idempotent upserts)
4. **Quality Checks** - Validates totals vs sums, flags anomalous record counts, checks for duplicate articles

**No config changes needed** - the update command dynamically determines which years to fetch based on the current date. The FCAI PDF pipeline is excluded (articles cover the same data plus state breakdowns). PDFs remain available via `fcai run` for historical backfill.

**Coverage gaps** in the report indicate months where no state/territory data was extracted between the first and last available months. Check FCAI media releases manually: `https://www.fcai.com.au/news-and-media/`

**JSON report** is saved to `exports/update_report_{timestamp}.json` for programmatic consumption. The `run_monthly_update()` function in `update.py` returns a Pydantic model directly, suitable for Prefect flow integration.

**Slack notifications** are sent automatically if `SLACK_WEBHOOK_URL` is set in `.env`. Success posts the summary; failure posts the error and step name. No-op when not configured.

**Prefect scheduling** (optional): `uv run python -m motor_vehicles.prefect_flow --serve` starts a cron-scheduled flow (5th of each month). Requires `pip install motor-vehicles[prefect]`.

## Key Modules

- `update.py` - Monthly update orchestrator (pure Python, no Click dependency, returns Pydantic report models)
- `quality.py` - Data quality checks (totals cross-check, anomaly detection, duplicate detection)
- `notify.py` - Slack webhook notifications (graceful no-op if not configured)
- `prefect_flow.py` - Prefect flow wrapping update steps as tasks (optional dependency)
- `main.py` - Click CLI entry point and command implementations
- `config.py` - AppConfig (Pydantic v2) + YAML/.env loader
- `scraping/marklines_client.py` - httpx fetcher with tenacity retry
- `scraping/marklines_parser.py` - BeautifulSoup HTML parser for sales tables
- `scraping/fcai_articles.py` - Article listing scraper + classification
- `extraction/image_tables.py` - Vision LLM table extraction from images
- `extraction/state_sales.py` - State/territory parsing from extracted tables
- `storage/database.py` - PostgreSQL interface (psycopg 3, dict_row)
- `storage/loader.py` - Orchestrate parse + load to DB

## Package

- **Name**: `motor-vehicles` (CLI) / `motor_vehicles` (Python)
- **Entry point**: `motor_vehicles.main:cli`
- **Roadmap**: See `ROADMAP.md` for planned work and future direction
