"""Create analytical views

Revision ID: 129f09ac6c14
Revises: b3e0a3b2fa5d
Create Date: 2025-12-25 02:01:40.095763

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '129f09ac6c14'
down_revision: Union[str, Sequence[str], None] = 'b3e0a3b2fa5d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Сводная витрина финансов по месяцам
    op.execute("""
        CREATE OR REPLACE VIEW marts.financials_v AS
        SELECT 
            to_char(date_trunc('month', COALESCE(payment_date, date)), 'YYYY-MM') AS year_month,
            type,
            ROUND(SUM(total_rub)) AS total_rub,
            COUNT(*) as record_count,
            now() as last_updated
        FROM staging.records
        WHERE type IN ('Доход', 'Расход', 'Income', 'Expense')
          AND COALESCE(payment_date, date) >= '2005-01-01'::timestamptz
        GROUP BY 1, 2
        ORDER BY 1 DESC, 2;
    """)

    # 2. Витрина расходов по категориям
    op.execute("""
        CREATE OR REPLACE VIEW marts.expenses_by_category_v AS
        SELECT 
            COALESCE(category, 'Uncategorized') AS category,
            ROUND(SUM(total_rub)) AS total_rub,
            COUNT(*) as record_count,
            now() as last_updated
        FROM staging.records
        WHERE (type = 'Расход' OR type = 'Expense')
        GROUP BY 1
        ORDER BY 2 DESC;
    """)

    # 3. Витрина для Web App (чистые данные без технических полей)
    op.execute("""
        CREATE OR REPLACE VIEW marts.web_transactions_v AS
        SELECT 
            raw_id,
            date,
            payment_date,
            type,
            client,
            vendor,
            category,
            total_rub,
            currency,
            description,
            source_type
        FROM staging.records
        ORDER BY date DESC;
    """)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS marts.financials_v")
    op.execute("DROP VIEW IF EXISTS marts.expenses_by_category_v")
    op.execute("DROP VIEW IF EXISTS marts.web_transactions_v")
