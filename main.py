#!/usr/bin/env python3
"""
Инкрементальный ELT-процесс с обнаружением изменений на основе хеша:
1. Запрашивает новые/измененные записи из raw.source_events
2. Нормализует в staging.records
3. Поддерживает режим --test для ограниченной обработки

Использование:
    python -m scripts.run_elt          # Полный инкрементальный запуск
    python -m scripts.run_elt --test   # Тестовый режим (первые 100 записей, показать примеры)
"""
import sys
import asyncio
import argparse
import logging
import json
import time
from typing import List, Dict, Any

from src.transform.normalizer import get_changed_raw_records, normalize_record
from src.transform.loader import upsert_staging_records_batch
from src.utils.db import init_db_pool, close_db_pool
from src.utils.config import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_incremental_elt(test_mode: bool = False):
    """
    Запустить инкрементальный ELT: трансформация измененных raw-записей в staging.
    
    Args:
        test_mode: Если True, обрабатывать только первые 100 записей и показать примеры
    """
    await init_db_pool()
    
    try:
        # Determine processing limits
        limit = settings.TEST_LIMIT if test_mode else None
        batch_size = settings.BATCH_SIZE
        
        mode_str = "TEST" if test_mode else "FULL"
        logger.info(f"=" * 60)
        logger.info(f"Starting {mode_str} incremental ELT process")
        logger.info(f"Batch size: {batch_size}, Limit: {limit or 'None'}")
        logger.info(f"=" * 60)
        
        start_time = time.time()
        
        # Step 1: Query changed/new records from raw
        logger.info("Step 1: Querying changed/new records from raw.source_events...")
        query_start = time.time()
        raw_records = await get_changed_raw_records(limit=limit)
        query_duration = time.time() - query_start
        
        if not raw_records:
            logger.info("No changed records found. Nothing to process.")
            return
        
        logger.info(f"Found {len(raw_records)} records to process (took {query_duration:.2f}s)")
        
        # Step 2: Normalize records
        logger.info("Step 2: Normalizing records...")
        norm_start = time.time()
        normalized_records: List[Dict[str, Any]] = []
        errors = 0
        
        for idx, raw_rec in enumerate(raw_records):
            try:
                normalized = normalize_record(
                    raw_id=raw_rec['raw_id'],
                    sheet_row_number=raw_rec.get('sheet_row_number'),
                    received_at=raw_rec['received_at'],
                    payload=raw_rec['raw_payload']
                )
                normalized_records.append(normalized)
                
            except Exception as e:
                errors += 1
                raw_id = raw_rec.get('raw_id', 'unknown')
                logger.error(
                    f"Failed to normalize record {idx+1}/{len(raw_records)} "
                    f"(raw_id={raw_id}): {e}",
                    exc_info=True
                )
                # Continue processing other records
                continue
        
        norm_duration = time.time() - norm_start
        logger.info(
            f"Normalized {len(normalized_records)} records "
            f"({errors} errors) in {norm_duration:.2f}s"
        )

        # Monitoring: Check error rate
        total_processed = len(raw_records)
        if total_processed > 0:
            error_rate = errors / total_processed
            if error_rate > 0.1:  # 10% threshold
                logger.error(
                    f"ALERT: High normalization error rate detected! "
                    f"{error_rate:.1%} ({errors}/{total_processed} records failed). "
                    f"Check logs for details."
                )
            elif errors > 0:
                logger.warning(
                    f"Normalization completed with {errors} errors "
                    f"({error_rate:.1%} failure rate)."
                )
        
        # Step 3: Show examples in test mode
        if test_mode and normalized_records:
            logger.info("\n" + "=" * 60)
            logger.info("SAMPLE NORMALIZED RECORDS (first 3):")
            logger.info("=" * 60)
            
            for i, rec in enumerate(normalized_records[:3], 1):
                logger.info(f"\nExample {i}:")
                logger.info(f"  raw_id: {rec.get('raw_id')}")
                logger.info(f"  client: {rec.get('client')}")
                logger.info(f"  type: {rec.get('type')}")
                logger.info(f"  category: {rec.get('category')}")
                logger.info(f"  total_rub: {rec.get('total_rub')}")
                logger.info(f"  payment_date: {rec.get('payment_date')}")
                logger.info(f"  vendor: {rec.get('vendor')}")
                logger.info(f"  payload_hash: {rec.get('payload_hash')[:16]}...")
            
            logger.info("\n" + "=" * 60)
        
        # Step 4: Upsert to staging
        upsert_start = time.time()
        upserted_count = 0
        if normalized_records:
            logger.info(f"Step 3: Upserting {len(normalized_records)} records to staging.records...")
            upserted_count = await upsert_staging_records_batch(
                normalized_records,
                batch_size=batch_size
            )
            logger.info(f"Successfully upserted {upserted_count} records")
        else:
            logger.warning("No records to upsert (all failed normalization)")
        upsert_duration = time.time() - upsert_start
        
        total_duration = time.time() - start_time
        
        # Calculate data volume (approximate)
        total_bytes = sum(len(json.dumps(r.get('raw_payload', {}))) for r in raw_records)
        total_mb = total_bytes / (1024 * 1024)

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("ELT SUMMARY:")
        logger.info(f"  Total Duration: {total_duration:.2f}s")
        logger.info(f"  Data Processed: {total_mb:.2f} MB ({len(raw_records)} records)")
        logger.info("-" * 60)
        logger.info("  Stage Durations:")
        logger.info(f"    1. Querying:    {query_duration:.2f}s")
        logger.info(f"    2. Normalizing: {norm_duration:.2f}s")
        logger.info(f"    3. Upserting:   {upsert_duration:.2f}s")
        logger.info("-" * 60)
        logger.info(f"  Records queried:    {len(raw_records)}")
        logger.info(f"  Records normalized: {len(normalized_records)}")
        logger.info(f"  Normalization errors: {errors}")
        logger.info(f"  Records upserted:   {upserted_count}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"ELT process failed: {e}", exc_info=True)
        raise
    
    finally:
        await close_db_pool()


def main():
    """Точка входа CLI."""
    parser = argparse.ArgumentParser(
        description="Incremental ELT: raw.source_events → staging.records"
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode: process only first 100 records and show examples'
    )
    
    args = parser.parse_args()
    
    try:
        asyncio.run(run_incremental_elt(test_mode=args.test))
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

