#!/usr/bin/env python3
"""
Инкрементальный ELT-процесс с обнаружением изменений на основе хеша:
1. Запрашивает новые/измененные записи из raw.source_events
2. Нормализует в staging.records
3. Поддерживает режим --test для ограниченной обработки

Использование:
    python main.py run          # Полный инкрементальный запуск
    python main.py run --test   # Тестовый режим (первые 100 записей, показать примеры)
    python main.py load <SPREADSHEET_ID> [RANGE]  # Загрузить из Google Sheets
    python main.py check        # Проверить окружение
"""
import sys
import asyncio
import argparse
import logging
import json
import time
from typing import List, Dict, Any

from src.transform import get_changed_raw_records, normalize_record, upsert_staging_records_batch
from src.db import init_db_pool, close_db_pool, fetch
from src.config import settings
from src.sheets import fetch_google_sheets
from src.logger import setup_logging


logger = logging.getLogger(__name__)

# --- Command: RUN ---

async def run_incremental_elt(test_mode: bool = False, source: str = 'google_sheets', source_type: str = 'live'):
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
        
        mode_str = "ТЕСТОВЫЙ" if test_mode else "ПОЛНЫЙ"
        logger.info(f"🚀 === {mode_str} ELT ПРОЦЕСС ===")
        logger.info(f"Пакет: {batch_size}, Лимит: {limit or 'Нет'}")
        
        start_time = time.time()
        
        # Step 1: Query changed/new records from raw
        logger.info(f"🔍 1. Поиск новых записей в raw.source_events (source={source})...")
        query_start = time.time()
        raw_records = await get_changed_raw_records(source=source, limit=limit)
        query_duration = time.time() - query_start
        
        if not raw_records:
            logger.info("💤 Новых записей не найдено. Работа завершена.")
            return
        
        logger.info(f"✅ Найдено записей: {len(raw_records)} (поиск занял {query_duration:.1f}с)")
        
        # Step 2: Normalize records
        logger.info("🛠️ 2. Нормализация данных...")
        norm_start = time.time()
        normalized_records: List[Dict[str, Any]] = []
        errors = 0
        
        for idx, raw_rec in enumerate(raw_records):
            try:
                normalized = normalize_record(
                    raw_id=raw_rec['raw_id'],
                    sheet_row_number=raw_rec.get('sheet_row_number'),
                    received_at=raw_rec['received_at'],
                    payload=raw_rec['raw_payload'],
                    source_type=source_type
                )
                normalized_records.append(normalized)
                
            except Exception as e:
                errors += 1
                # Log errors only if critical or in debug
                if errors <= 5: # Show first 5 errors only to keep log compact
                    logger.error(f"❌ Ошибка нормализации (ID={raw_rec.get('raw_id')}): {e}")
                continue
        
        norm_duration = time.time() - norm_start
        logger.info(
            f"✨ Нормализовано: {len(normalized_records)} "
            f"(ошибок: {errors}) за {norm_duration:.1f}с"
        )

        # Monitoring: Check error rate
        total_processed = len(raw_records)
        if total_processed > 0:
            error_rate = errors / total_processed
            if error_rate > 0.1:  # 10% threshold
                logger.warning(
                    f"⚠️ ВНИМАНИЕ: Высокий процент ошибок! "
                    f"{error_rate:.1%} ({errors}/{total_processed})."
                )
        
        # Step 3: Show examples in test mode
        if test_mode and normalized_records:
            logger.info("--- ПРИМЕРЫ ЗАПИСЕЙ (первые 3) ---")
            
            for i, rec in enumerate(normalized_records[:3], 1):
                logger.info(f"Запись {i}: {rec.get('client')} | {rec.get('total_rub')} руб. | {rec.get('category')}")
                
        # Step 4: Upsert to staging
        upsert_start = time.time()
        upserted_count = 0
        if normalized_records:
            logger.info(f"💾 3. Сохранение {len(normalized_records)} записей в БД...")
            upserted_count = await upsert_staging_records_batch(
                normalized_records,
                batch_size=batch_size
            )
            logger.info(f"✅ Успешно сохранено: {upserted_count}")
        else:
            logger.warning("⚠️ Нет записей для сохранения.")
        upsert_duration = time.time() - upsert_start
        
        total_duration = time.time() - start_time
        
        # Summary
        logger.info("📊 === ИТОГИ ===")
        logger.info(f"Время: {total_duration:.1f}с | Обработано: {len(raw_records)} | Сохранено: {upserted_count}")
        logger.info(f"Этапы (сек): Поиск={query_duration:.1f}, Норм={norm_duration:.1f}, Сохр={upsert_duration:.1f}")
        logger.info("=========================")
        
    except Exception as e:
        logger.error(f"ELT process failed: {e}", exc_info=True)
        raise
    
    finally:
        await close_db_pool()


async def load_raw(source: str, records: List[Dict[str, Any]]):
    """Bulk insert raw records into raw.data table."""
    if not records:
        return
    
    pool = await init_db_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                "INSERT INTO raw.data (id, source, payload, payload_hash) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
                [
                    (
                        r['id'],
                        source,
                        json.dumps(r['payload'], ensure_ascii=False),
                        __import__('hashlib').md5(
                            json.dumps(r['payload'], sort_keys=True).encode()
                        ).hexdigest()
                    )
                    for r in records
                ]
            )


async def run_load_sheets(spreadsheet_id: str, range_name: str, source: str = 'google_sheets'):
    """Load data from Google Sheets into raw.data."""
    await init_db_pool()
    try:
        logger.info(f"📥 Извлечение из Google Sheets: {spreadsheet_id} {range_name} (source={source}) ...")
        records = await fetch_google_sheets(spreadsheet_id, range_name)
        logger.info(f"✅ Получено {len(records)} строк. Загрузка в raw.data ...")
        
        # Prepare rows with duplicate detection
        rows = []
        seen_hashes = {}
        duplicates_count = 0
        
        for i, r in enumerate(records):
            # 1. Try to get explicit ID
            # Normalize keys to find 'id' case-insensitively
            keys_norm = {k.lower().strip(): k for k in r.keys()}
            id_key = keys_norm.get('pk') or keys_norm.get('id') or keys_norm.get('row_id') or keys_norm.get('uuid')
            
            raw_id = None
            if id_key and r[id_key]:
                raw_id = str(r[id_key]).strip()
            
            # 2. Fallback to Content Hash
            import hashlib
            payload_str = json.dumps(r, sort_keys=True)
            h = hashlib.sha256(payload_str.encode('utf-8')).hexdigest()
            
            if not raw_id:
                # User warning logic: Check for full duplicates in source
                if h in seen_hashes:
                    duplicates_count += 1
                    if duplicates_count <= 5:
                        logger.warning(f"⚠️ Найдена строка-дубликат (строка {i+2}). Рекомендуется добавить уникальный ID. Content hash: {h[:8]}")
                seen_hashes[h] = True
                
                # We still need a unique ID for DB constraints, so we use hash + row info as fallback
                # But heavily encourage PK usage in logs
                raw_id = f"gsheet_auto_{h[:12]}_{i}"

            rows.append({
                'id': raw_id,
                'payload': r
            })
        
        if duplicates_count > 0:
             logger.warning(f"⚠️ Всего найдено дубликатов хешей данных: {duplicates_count}. Это может привести к проблемам. Рекомендуется добавить колонку 'id' в Google Sheet.")

        await load_raw(source, rows)
        logger.info(f"💾 Загружено {len(rows)} строк.")
    finally:
        await close_db_pool()


async def run_check_env():
    """Check environment, .env, and DB connection."""
    logger.info("Проверка окружения...")
    
    import os
    if not os.path.exists('.env'):
        logger.error("❌ .env not found")
    else:
        logger.info("✅ .env found")
    
    if not settings.POSTGRES_URI:
        logger.error("❌ POSTGRES_URI not set")
    else:
        logger.info("✅ POSTGRES_URI set")
    
    try:
        await init_db_pool()
        res = await fetch("SELECT 1 as val")
        if res and res[0]['val'] == 1:
            logger.info("✅ DB Connection successful")
        else:
            logger.error("❌ DB Connection failed")
    except Exception as e:
        logger.error(f"❌ DB Connection failed: {e}")
    finally:
        await close_db_pool()



def main():
    """Точка входа CLI."""
    parser = argparse.ArgumentParser(
        description="ChileKids ETL Pipeline: raw.source_events → staging.records"
    )
    parser.add_argument("--debug", action="store_true", help="Set log level to DEBUG")
    parser.add_argument("--json-logs", action="store_true", help="Enable JSON logging format")

    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Run command
    p_run = subparsers.add_parser('run', help='Run incremental ELT')
    p_run.add_argument(
        '--test',
        action='store_true',
        help='Test mode: process only first 100 records and show examples'
    )
    p_run.add_argument("--source", default="google_sheets", help="Raw data source name")
    p_run.add_argument("--source-type", default="live", help="Target staging source_type tag")
    
    # Load command
    p_load = subparsers.add_parser('load', help='Load from Google Sheets')
    p_load.add_argument('spreadsheet_id', help='Google Spreadsheet ID')
    p_load.add_argument('range', nargs='?', default='Sheet1!A:AF', help='Range (default: Sheet1!A:AF)')
    p_load.add_argument('--source', default='google_sheets', help='Store as this source in raw.data')
    
    # Check command
    p_check = subparsers.add_parser('check', help='Check environment')
    
    args = parser.parse_args()
    
    # Configure logging based on args
    log_level = "DEBUG" if getattr(args, 'debug', False) else settings.LOG_LEVEL
    json_format = getattr(args, 'json_logs', False)
    setup_logging(level=log_level, json_format=json_format)
    
    try:
        if args.command == 'run':
            asyncio.run(run_incremental_elt(test_mode=args.test, source=args.source, source_type=args.source_type))
        elif args.command == 'load':
            asyncio.run(run_load_sheets(args.spreadsheet_id, args.range, source=args.source))
        elif args.command == 'check':
            asyncio.run(run_check_env())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
