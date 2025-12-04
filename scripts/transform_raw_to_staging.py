#!/usr/bin/env python3
"""Runner: transform raw.data -> staging.data for a given source."""
import sys
import asyncio
import os
from src.transform.transformer import transform_from_raw

async def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/transform_raw_to_staging.py <source> [limit]")
        sys.exit(2)
    source = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    # ensure small DB pool for one-shot script
    os.environ.setdefault('DB_POOL_MAX', '1')
    df = await transform_from_raw(source, limit)
    print(f"Transformed {len(df)} rows from raw.{source} -> staging.data")

if __name__ == '__main__':
    asyncio.run(main())
