import asyncio
from src.utils.db import init_db_pool, close_db_pool, fetch_one_off

async def check_hashes():
    await init_db_pool()
    try:
        # Check total count and count of non-null hashes
        sql = """
            SELECT 
                COUNT(*) as total,
                COUNT(payload_hash) as filled_hashes,
                COUNT(*) FILTER (WHERE payload_hash IS NULL) as null_hashes
            FROM raw.data
            WHERE source = 'google_sheets'
        """
        rows = await fetch_one_off(sql)
        if rows:
            print(f"Total records: {rows[0]['total']}")
            print(f"Filled hashes: {rows[0]['filled_hashes']}")
            print(f"Null hashes: {rows[0]['null_hashes']}")
    finally:
        await close_db_pool()

if __name__ == "__main__":
    asyncio.run(check_hashes())
