"""Initial schema

Revision ID: b3e0a3b2fa5d
Revises: 
Create Date: 2025-12-25 01:51:33.306118

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3e0a3b2fa5d'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- RAW LAYER ---
    op.execute("CREATE SCHEMA IF NOT EXISTS raw")
    op.execute("""
        CREATE TABLE IF NOT EXISTS raw.data (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            payload JSONB,
            payload_hash TEXT,
            extracted_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now())
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_raw_payload_hash ON raw.data (payload_hash)")

    # --- STAGING LAYER ---
    op.execute("CREATE SCHEMA IF NOT EXISTS staging")
    
    op.execute("""
        CREATE TABLE IF NOT EXISTS staging.records (
            raw_id TEXT PRIMARY KEY,
            sheet_row_number INTEGER,
            received_at TIMESTAMP WITH TIME ZONE,
            date TIMESTAMP WITH TIME ZONE,
            payment_date TIMESTAMP WITH TIME ZONE,
            type TEXT,
            category TEXT,
            subcategory TEXT,
            client TEXT,
            vendor TEXT,
            description TEXT,
            total_rub NUMERIC,
            total_usd NUMERIC,
            currency TEXT,
            fx_rub NUMERIC,
            fx_usd NUMERIC,
            task TEXT,
            service TEXT,
            approver TEXT,
            cashier TEXT,
            payload_hash TEXT,
            raw_payload JSONB,
            payment_date_orig TIMESTAMP WITH TIME ZONE,
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
            hours NUMERIC,
            sum_total_rub NUMERIC,
            total_in_currency NUMERIC,
            rub_summa NUMERIC,
            usd_summa NUMERIC
        )
    """)

    # Идемпотентное добавление колонки source_type
    op.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='staging' AND table_name='records' AND column_name='source_type') THEN
                ALTER TABLE staging.records ADD COLUMN source_type TEXT DEFAULT 'live';
            END IF;
        END
        $$;
    """)

    op.execute("CREATE INDEX IF NOT EXISTS idx_staging_type ON staging.records (type)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_staging_source ON staging.records (source_type)")

    # --- MARTS LAYER ---
    op.execute("CREATE SCHEMA IF NOT EXISTS marts")


def downgrade() -> None:
    pass
