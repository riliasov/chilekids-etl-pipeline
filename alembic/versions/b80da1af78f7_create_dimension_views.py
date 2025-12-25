"""Create dimension views

Revision ID: b80da1af78f7
Revises: 129f09ac6c14
Create Date: 2025-12-25 02:35:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'b80da1af78f7'
down_revision: Union[str, Sequence[str], None] = '129f09ac6c14'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Dimension: Clients
    # Combines explicit reference data (if source_type='ref_clients') with implicit distinct clients
    op.execute("""
        CREATE OR REPLACE VIEW marts.dim_clients_v AS
        WITH explicit AS (
            SELECT 
                client as name, 
                received_at as updated_at, 
                'manual' as origin 
            FROM staging.records 
            WHERE source_type = 'ref_clients'
        ),
        implicit AS (
            SELECT DISTINCT 
                client as name, 
                NULL::timestamp as updated_at, 
                'transaction' as origin 
            FROM staging.records 
            WHERE client IS NOT NULL AND client != ''
        )
        SELECT DISTINCT ON (name) name, updated_at, origin
        FROM (SELECT * FROM explicit UNION ALL SELECT * FROM implicit) all_clients
        ORDER BY name, origin DESC; -- manual preferred over transaction
    """)

    # 2. Dimension: Categories
    op.execute("""
        CREATE OR REPLACE VIEW marts.dim_categories_v AS
        SELECT DISTINCT 
            COALESCE(category, 'Uncategorized') as name
        FROM staging.records
        WHERE category IS NOT NULL AND category != ''
        ORDER BY 1;
    """)

    # 3. Dimension: Vendors
    op.execute("""
        CREATE OR REPLACE VIEW marts.dim_vendors_v AS
        SELECT DISTINCT 
            vendor as name
        FROM staging.records
        WHERE vendor IS NOT NULL AND vendor != ''
        ORDER BY 1;
    """)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS marts.dim_clients_v")
    op.execute("DROP VIEW IF EXISTS marts.dim_categories_v")
    op.execute("DROP VIEW IF EXISTS marts.dim_vendors_v")
