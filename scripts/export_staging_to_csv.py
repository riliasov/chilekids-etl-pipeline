#!/usr/bin/env python3
"""Export normalized staging.payload rows to CSV.

Usage: python scripts/export_staging_to_csv.py <source> [outpath] [limit]

Writes a CSV with one column per key found in the JSON payloads. Keys from the
first row are kept in order, new keys appended alphabetically.
"""
import sys
import csv
import json
from pathlib import Path
import asyncio
from src.utils.db import fetch_one_off


def normalize_value(v):
    # Convert values to CSV-friendly strings
    if v is None:
        return ''
    # datetimes are stored as iso strings in payload; leave as-is
    if isinstance(v, (int, float, str)):
        return str(v)
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


async def export(source: str, outpath: str | None = None, limit: int | None = None):
    outpath = outpath or f'archive/export_staging_{source}.csv'
    sql = "SELECT id, payload FROM staging.data WHERE source = $1 ORDER BY payment_ts DESC NULLS LAST"
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = await fetch_one_off(sql, source)
    if not rows:
        print('No rows found for', source)
        return

    # collect headers in order: start with first row keys, then append any others
    headers = []
    seen = set()
    payloads = []
    for r in rows:
        p = r['payload']
        if isinstance(p, str):
            try:
                p = json.loads(p)
            except Exception:
                p = {'_raw': p}
        payloads.append(p if isinstance(p, dict) else {})
        for k in (list(p.keys()) if isinstance(p, dict) else []):
            if k not in seen:
                headers.append(k)
                seen.add(k)

    # ensure stable ordering for any remaining keys (shouldn't be any)
    # but keep headers unique
    out_file = Path(outpath)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    with out_file.open('w', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        for p in payloads:
            row = [normalize_value(p.get(h)) for h in headers]
            writer.writerow(row)

    print('Wrote', out_file)


def main():
    if len(sys.argv) < 2:
        print('Usage: python scripts/export_staging_to_csv.py <source> [outpath] [limit]')
        sys.exit(2)
    source = sys.argv[1]
    outpath = sys.argv[2] if len(sys.argv) > 2 else None
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
    asyncio.run(export(source, outpath, limit))


if __name__ == '__main__':
    main()
