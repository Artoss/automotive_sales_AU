# Scraper_0061_MotorVehicles

Australian automotive sales data scraper from two sources.

## Sources

### Marklines (HTML + JS)
- **Base URL**: `https://www.marklines.com/en/statistics/flash_sales/automotive-sales-in-australia-by-month`
- **Historical**: `...salesfig_australia_{year}` (append year for older data)
- Monthly sales by make (manufacturer) in HTML tables
- Total monthly figures embedded in JavaScript (`Data{year} = [...]`)
- Tables at odd indices (1, 3, 5...) contain data rows

### FCAI (PDF publications)
- **URL pattern**: `https://www.fcai.com.au/library/publication/{month}_{year}_vfacts_media_release_and_industry_summary.pdf`
- Monthly PDFs with sales by make/model/segment
- Segmentation: passenger, SUV, light commercial, heavy commercial
- Metadata: `https://www.fcai.com.au/sales/segmentation-criteria`

## Database

- **Name**: `automotive_sales_au`
- **Tables**: `scrape_runs`, `marklines_sales`, `marklines_totals`, `fcai_publications`, `fcai_sales_data`

## CLI Commands

```
motor-vehicles marklines run       # Full Marklines pipeline
motor-vehicles fcai run            # Full FCAI pipeline
motor-vehicles run                 # Both sources
motor-vehicles migrate             # Run SQL migrations
motor-vehicles status              # Show scrape history
motor-vehicles export              # Export to CSV
```

## Package

- **Name**: `motor-vehicles` (CLI) / `motor_vehicles` (Python)
- **Entry point**: `motor_vehicles.main:cli`
