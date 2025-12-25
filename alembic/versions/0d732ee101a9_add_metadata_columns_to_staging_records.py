"""Add metadata columns to staging.records

Revision ID: 0d732ee101a9
Revises: e7f1a2b3c4d5
Create Date: 2025-12-25 04:05:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '0d732ee101a9'
down_revision: Union[str, Sequence[str], None] = 'e7f1a2b3c4d5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add metadata columns to staging.records
    op.add_column('records', sa.Column('created_at', sa.DateTime(timezone=True), nullable=True), schema='staging')
    op.add_column('records', sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True), schema='staging')
    op.add_column('records', sa.Column('updated_by', sa.Text(), nullable=True), schema='staging')


def downgrade() -> None:
    op.drop_column('records', 'updated_by', schema='staging')
    op.drop_column('records', 'updated_at', schema='staging')
    op.drop_column('records', 'created_at', schema='staging')
