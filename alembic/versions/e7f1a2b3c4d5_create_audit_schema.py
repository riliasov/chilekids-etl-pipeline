"""Create audit schema

Revision ID: e7f1a2b3c4d5
Revises: b80da1af78f7
Create Date: 2025-12-25 04:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'e7f1a2b3c4d5'
down_revision: Union[str, Sequence[str], None] = 'b80da1af78f7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create audit schema
    op.execute("CREATE SCHEMA IF NOT EXISTS audit")
    
    # 2. Create audit.logs table
    op.execute("""
        CREATE TABLE IF NOT EXISTS audit.logs (
            id SERIAL PRIMARY KEY,
            record_id TEXT NOT NULL,
            field_name TEXT,
            old_value JSONB,
            new_value JSONB,
            changed_at TIMESTAMPTZ DEFAULT now(),
            changed_by TEXT
        )
    """)
    
    # 3. Create index for fast lookup
    op.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_record_id ON audit.logs (record_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_changed_at ON audit.logs (changed_at)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS audit.logs")
    op.execute("DROP SCHEMA IF EXISTS audit")
