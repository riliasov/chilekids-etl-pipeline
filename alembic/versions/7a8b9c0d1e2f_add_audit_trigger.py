"""Add audit trigger

Revision ID: 7a8b9c0d1e2f
Revises: 0d732ee101a9
Create Date: 2025-12-25 04:15:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '7a8b9c0d1e2f'
down_revision: Union[str, Sequence[str], None] = '0d732ee101a9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create audit function
    op.execute("""
        CREATE OR REPLACE FUNCTION staging.fn_audit_record_changes()
        RETURNS TRIGGER AS $$
        BEGIN
            IF (OLD.payload_hash IS DISTINCT FROM NEW.payload_hash) THEN
                INSERT INTO audit.logs (record_id, field_name, old_value, new_value, changed_by)
                VALUES (
                    NEW.raw_id,
                    'payload',
                    OLD.raw_payload,
                    NEW.raw_payload,
                    COALESCE(NEW.updated_by, 'system')
                );
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # 2. Create trigger
    op.execute("""
        CREATE TRIGGER trg_audit_staging_records
        AFTER UPDATE ON staging.records
        FOR EACH ROW
        EXECUTE FUNCTION staging.fn_audit_record_changes();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_audit_staging_records ON staging.records")
    op.execute("DROP FUNCTION IF EXISTS staging.fn_audit_record_changes")
