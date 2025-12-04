"""Loader for upserting normalized records to staging.records."""
import logging
import json
from typing import List, Dict, Any
import asyncpg

from ..utils.db import get_db_pool

logger = logging.getLogger(__name__)


async def upsert_staging_records(records: List[Dict[str, Any]]) -> int:
    """
    Upsert records to staging.records table.
    
    Uses ON CONFLICT (raw_id) DO UPDATE to handle incremental updates.
    All fields are updated on conflict to ensure staging reflects latest raw data.
    
    Args:
        records: List of normalized record dictionaries
        
    Returns:
        Number of records successfully upserted
        
    Raises:
        Exception if database operation fails
    """
    if not records:
        logger.info("No records to upsert")
        return 0
    
    # Field names matching staging.records schema
    fields = [
        'raw_id', 'sheet_row_number', 'received_at',
        'date', 'payment_date', 'task', 'type', 'year', 'hours', 'month',
        'client', 'fx_rub', 'fx_usd', 'vendor', 'cashier', 'cat_new',
        'quarter', 'service', 'approver', 'category', 'currency', 'cat_final',
        'total_rub', 'total_usd', 'subcat_new', 'paket', 'description',
        'subcategory', 'payment_date_orig', 'subcat_final', 'count_vendor',
        'statya', 'sum_total_rub', 'usd_summa', 'direct_indirect',
        'package_secondary', 'total_in_currency', 'rub_summa', 'kategoriya',
        'podstatya', 'vidy_raskhodov', 'payload_hash', 'raw_payload'
    ]
    
    # Build INSERT ... ON CONFLICT query
    placeholders = ', '.join(f'${i+1}' for i in range(len(fields)))
    field_list = ', '.join(fields)
    
    # Update all fields except raw_id (PK)
    update_fields = [f for f in fields if f != 'raw_id']
    update_clause = ', '.join(f'{f} = EXCLUDED.{f}' for f in update_fields)
    
    sql = f"""
        INSERT INTO staging.records ({field_list})
        VALUES ({placeholders})
        ON CONFLICT (raw_id) DO UPDATE SET
            {update_clause}
    """
    
    pool = get_db_pool()
    if pool is None:
        raise RuntimeError("Database pool not initialized")
    
    successful = 0
    failed = 0
    
    async with pool.acquire() as conn:
        # Prepare all records for batch insertion
        prepared_records = []
        for record in records:
            try:
                record_copy = record.copy()
                # Serialize raw_payload dict to JSON string for JSONB field
                if 'raw_payload' in record_copy and isinstance(record_copy['raw_payload'], dict):
                    record_copy['raw_payload'] = json.dumps(record_copy['raw_payload'])
                
                # Extract values in field order
                values = tuple(record_copy.get(f) for f in fields)
                prepared_records.append(values)
            except Exception as e:
                logger.error(f"Failed to prepare record for upsert: {e}")
                failed += 1

        if not prepared_records:
            return 0

        # Try fast batch upsert first
        try:
            async with conn.transaction():
                await conn.executemany(sql, prepared_records)
                successful = len(prepared_records)
        except Exception as e:
            logger.warning(f"Batch upsert failed, falling back to individual upserts: {e}")
            # Fallback to slow individual upserts to isolate errors
            async with conn.transaction():
                for i, values in enumerate(prepared_records):
                    try:
                        await conn.execute(sql, *values)
                        successful += 1
                    except Exception as inner_e:
                        failed += 1
                        # Try to identify which record failed
                        raw_id = records[i].get('raw_id', 'unknown')
                        logger.error(
                            f"Failed to upsert record raw_id={raw_id}: {inner_e}"
                        )
    
    logger.info(
        f"Upserted {successful} records to staging.records "
        f"({failed} failed)"
    )
    
    return successful


async def upsert_staging_records_batch(
    records: List[Dict[str, Any]],
    batch_size: int = 100
) -> int:
    """
    Upsert records in batches for better performance.
    
    Args:
        records: List of normalized record dictionaries
        batch_size: Number of records to process per batch
        
    Returns:
        Total number of records successfully upserted
    """
    if not records:
        return 0
    
    total_upserted = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        logger.debug(f"Processing batch {i//batch_size + 1} ({len(batch)} records)")
        
        try:
            count = await upsert_staging_records (batch)
            total_upserted += count
        except Exception as e:
            logger.error(f"Batch upsert failed: {e}", exc_info=True)
            # Continue with next batch
            continue
    
    logger.info(f"Total upserted: {total_upserted} out of {len(records)} records")
    return total_upserted
