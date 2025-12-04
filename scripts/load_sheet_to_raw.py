#!/usr/bin/env python3
"""Extracts data from Google Sheets and loads to raw.data in Supabase/Postgres."""
import sys
import asyncio
import os
import hashlib
from src.extract.google_sheets import fetch_google_sheets
from src.load.postgres_loader import load_raw

def make_id(row: dict, rownum: int) -> str:
    # Use a hash of the row content and rownum for uniqueness
    h = hashlib.sha256()
    h.update(str(row).encode('utf-8'))
    h.update(str(rownum).encode('utf-8'))
    return f"gsheet_{rownum}_" + h.hexdigest()[:12]

async def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/load_sheet_to_raw.py <spreadsheet_id> [range]")
        sys.exit(2)
    spreadsheet_id = sys.argv[1]
    range_name = sys.argv[2] if len(sys.argv) > 2 else "Sheet1!A:AF"
    # ensure small DB pool for one-shot script to avoid exhausting hosted DB connections
    os.environ.setdefault('DB_POOL_MAX', '1')
    print(f"Extracting {spreadsheet_id} {range_name} ...")
    records = await fetch_google_sheets(spreadsheet_id, range_name)
    print(f"Fetched {len(records)} rows. Loading to raw.data ...")
    rows = []
    for i, r in enumerate(records):
        rows.append({
            'id': make_id(r, i),
            'payload': r
        })
    await load_raw('google_sheets', rows)
    print(f"Loaded {len(rows)} rows to raw.data.")

if __name__ == '__main__':
    asyncio.run(main())
