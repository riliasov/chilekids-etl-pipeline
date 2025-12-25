import asyncio
import json
from decimal import Decimal
import pytest
import asyncpg
from testcontainers.postgres import PostgresContainer
from src.config import settings
from src.db import init_db_pool, close_db_pool, fetch
from src.transform import normalize_record, upsert_staging_records_batch
from datetime import datetime, timezone


@pytest.fixture(scope="module")
def postgres_container():
    """Запускает контейнер Postgres для интеграционных тестов."""
    with PostgresContainer("postgres:15-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="module")
async def setup_db(postgres_container):
    """Инициализирует схему БД в тестовом контейнере."""
    # Override settings.POSTGRES_URI to point to the test container
    original_uri = settings.POSTGRES_URI
    settings.POSTGRES_URI = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")

    conn = await asyncpg.connect(settings.POSTGRES_URI)
    try:
        # Create schemas
        await conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        await conn.execute("CREATE SCHEMA IF NOT EXISTS staging")

        # Create tables
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.data (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                payload JSONB NOT NULL,
                payload_hash TEXT NOT NULL,
                extracted_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS staging.records (
                raw_id TEXT PRIMARY KEY,
                sheet_row_number INTEGER,
                received_at TIMESTAMPTZ,
                source_type TEXT,
                date TIMESTAMPTZ,
                payment_date TIMESTAMPTZ,
                client TEXT,
                total_rub DECIMAL,
                category TEXT,
                payload_hash TEXT NOT NULL,
                raw_payload JSONB,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                updated_by TEXT,
                -- Dummy columns for test stability
                payment_date_orig TIMESTAMPTZ,
                task TEXT,
                type TEXT,
                vendor TEXT,
                cashier TEXT,
                service TEXT,
                approver TEXT,
                currency TEXT,
                subcategory TEXT,
                description TEXT,
                direct_indirect TEXT,
                cat_new TEXT,
                cat_final TEXT,
                subcat_new TEXT,
                subcat_final TEXT,
                kategoriya TEXT,
                podstatya TEXT,
                statya TEXT,
                vidy_raskhodov TEXT,
                paket TEXT,
                package_secondary TEXT,
                year INTEGER,
                month INTEGER,
                quarter INTEGER,
                count_vendor INTEGER,
                hours DECIMAL,
                fx_rub DECIMAL,
                fx_usd DECIMAL,
                total_usd DECIMAL,
                sum_total_rub DECIMAL,
                total_in_currency DECIMAL,
                rub_summa DECIMAL,
                usd_summa DECIMAL
            )
        """)
    finally:
        await conn.close()

    yield settings.POSTGRES_URI

    # Restore original URI
    settings.POSTGRES_URI = original_uri


@pytest.mark.asyncio
async def test_full_elt_flow(setup_db):
    """Проверяет полный цикл ELT от вставки в raw до появления в staging."""
    # 1. Insert into raw.data
    conn = await asyncpg.connect(setup_db)
    payload = {"Date": "25.12.2023", "Client": "Integration Test Client", "Total RUB": "1000.50", "Type": "Income"}
    payload_hash_val = "test_hash_123"
    await conn.execute(
        "INSERT INTO raw.data (id, source, payload, payload_hash) VALUES ($1, $2, $3, $4)",
        "test_id_1", "test_source", json.dumps(payload), payload_hash_val
    )

    # 2. Extract and Normalize
    row = await conn.fetchrow("SELECT * FROM raw.data WHERE id = 'test_id_1'")
    raw_payload = json.loads(row['payload'])

    normalized = normalize_record(
        raw_id=row['id'],
        sheet_row_number=1,
        received_at=row['extracted_at'],
        payload=raw_payload,
        source_type="test"
    )

    assert normalized['client'] == "Integration Test Client"
    assert normalized['total_rub'] == Decimal("1000.50")

    # 3. Load into Staging
    await init_db_pool()
    try:
        count = await upsert_staging_records_batch([normalized])
        assert count == 1

        # 4. Verify in Staging
        staging_row = await conn.fetchrow("SELECT * FROM staging.records WHERE raw_id = 'test_id_1'")
        assert staging_row is not None
        assert staging_row['client'] == "Integration Test Client"
        assert staging_row['total_rub'] == Decimal("1000.50")
    finally:
        await close_db_pool()
        await conn.close()
