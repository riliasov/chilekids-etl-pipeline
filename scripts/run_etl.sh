#!/bin/bash
set -e

# Run migrations (using the simple env.py logic for now)
echo "Running database migrations..."
alembic upgrade head

# Start the ELT process
echo "Starting ELT pipeline..."
python main.py
