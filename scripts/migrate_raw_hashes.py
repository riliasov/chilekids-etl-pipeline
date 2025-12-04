import asyncio
import json
import logging
from src.utils.db import init_db_pool, close_db_pool, get_db_pool
from src.utils.hash import payload_hash

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def migrate_hashes():
    await init_db_pool()
    pool = get_db_pool()
    
    try:
        async with pool.acquire() as conn:
            # 1. Add columns if not exist
            logger.info("Adding columns to raw.data...")
            await conn.execute("""
                ALTER TABLE raw.data
                ADD COLUMN IF NOT EXISTS payload_hash TEXT,
                ADD COLUMN IF NOT EXISTS last_seen TIMESTAMPTZ DEFAULT now();
            """)
            
            # 2. Create index
            logger.info("Creating index on payload_hash...")
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_data_hash ON raw.data(payload_hash);
            """)
            
            # 3. Fetch records needing hash
            logger.info("Fetching records needing hash...")
            rows = await conn.fetch("""
                SELECT id, payload 
                FROM raw.data 
                WHERE payload_hash IS NULL AND source = 'google_sheets'
            """)
            
            if not rows:
                logger.info("No records to update.")
                return

            logger.info(f"Found {len(rows)} records to update. Processing...")
            
            # 4. Update in batches
            updates = []
            for row in rows:
                try:
                    payload = json.loads(row['payload'])
                    phash = payload_hash(payload)
                    updates.append((phash, row['id']))
                except Exception as e:
                    logger.warning(f"Failed to parse payload for id {row['id']}: {e}")
            
            if updates:
                logger.info(f"Updating {len(updates)} records...")
                await conn.executemany("""
                    UPDATE raw.data 
                    SET payload_hash = $1 
                    WHERE id = $2
                """, updates)
                logger.info("Update complete.")
                
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        await close_db_pool()

if __name__ == "__main__":
    asyncio.run(migrate_hashes())
