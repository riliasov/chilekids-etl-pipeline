"""Alembic env that runs SQL from configs/schema.sql using sync engine.

For simplicity this script executes the SQL file during upgrade.
"""
from __future__ import annotations
import os
from logging.config import fileConfig
from sqlalchemy import create_engine, text
from alembic import context

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

db_url = os.getenv('POSTGRES_URI') or config.get_main_option('sqlalchemy.url')
if not db_url:
    raise RuntimeError('POSTGRES_URI not set for alembic')


def run_migrations_online():
    engine = create_engine(db_url)
    sql_path = os.path.join(os.getcwd(), 'configs', 'schema.sql')
    with engine.connect() as conn:
        with open(sql_path, 'r', encoding='utf-8') as fh:
            conn.execute(text(fh.read()))


if context.is_offline_mode():
    raise RuntimeError('Offline mode not supported in this simple env.py')
else:
    run_migrations_online()
