#!/usr/bin/env python3
"""Small runner to test Google Sheets extractor locally.

Usage: python scripts/test_fetch_sheet.py <spreadsheet_id> [range]
"""
import sys
import asyncio
import json
import os
from src.sheets import fetch_google_sheets


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_fetch_sheet.py <spreadsheet_id> [range]")
        sys.exit(2)
    spreadsheet_id = sys.argv[1]
    range_name = sys.argv[2] if len(sys.argv) > 2 else "Sheet1!A1:1000"
    # adopt small DB pool defaults in case any downstream code touches the DB
    os.environ.setdefault('DB_POOL_MAX', '1')
    print("Running fetch_google_sheets for", spreadsheet_id, range_name)
    try:
        records = asyncio.run(fetch_google_sheets(spreadsheet_id, range_name))
        print("Records fetched:", len(records))
        if records:
            print("First record:", json.dumps(records[0], ensure_ascii=False))
    except Exception as exc:
        print("Extractor error:", type(exc).__name__, exc)


if __name__ == '__main__':
    main()
