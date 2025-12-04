# ChileKids ETL Pipeline — Google Sheets to Supabase

Production-ready async ELT pipeline for extracting data from Google Sheets, loading to Supabase PostgreSQL, and creating normalized staging tables and data marts.

## Architecture

- **Extract**: Async extractors for Google Sheets, Bitrix24, Google Ads, Yandex Direct, Meta Ads, and YouTube
- **Load**: Raw data archival to local disk and Supabase Storage, then bulk load to `raw.data` schema
- **Transform**: Python-based normalization to `staging.data` with robust type inference
- **Marts**: Aggregated data marts in `marts` schema (e.g., expenses by category)

## Quick Start

### Prerequisites

1. Create `.env` file with required credentials:
   ```bash
   POSTGRES_URI=postgresql://user:pass@host:5432/db
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_SERVICE_KEY=your_service_key
   SHEETS_SA_JSON=./secrets/your-service-account.json
   SHEETS_SPREADSHEET_ID=your_spreadsheet_id
   SHEETS_RANGE=A4:AF
   ```

2. Install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

### Run Pipeline

```bash
# Full ELT pipeline with environment configuration
PYTHONPATH=. python scripts/run_elt.py

# Or with explicit arguments
PYTHONPATH=. python scripts/run_elt.py <spreadsheet_id> <range>
```

### Available Scripts

- `scripts/run_elt.py` - Main orchestration script (Extract → Load → Transform)
- `scripts/load_sheet_to_raw.py` - Extract and load to raw layer
- `scripts/transform_raw_to_staging.py` - Transform raw to staging
- `scripts/verify_data.py` - Verify data in database
- `scripts/check_env.py` - Check environment configuration
- `scripts/export_staging_to_csv.py` - Export staging data to CSV

### Testing

```bash
# Run all tests
PYTHONPATH=. pytest tests/

# Run specific test suite
PYTHONPATH=. pytest tests/test_elt_flow.py
```

## CI/CD

For GitHub Actions or other CI systems, configure these secrets:
- `POSTGRES_URI`
- `SUPABASE_SERVICE_KEY`
- `SUPABASE_URL`
- `SHEETS_SA_JSON` (service account JSON contents)
- API tokens: `BITRIX_WEBHOOK`, `GOOGLE_ADS_TOKEN`, `YANDEX_DIRECT_TOKEN`, `META_TOKEN`, `YOUTUBE_KEY`
