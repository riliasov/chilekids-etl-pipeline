#!/usr/bin/env python3
"""
Data Audit Script for staging.records
Analyzes data quality, type mismatches, and logical inconsistencies.
"""
import asyncio
import json
import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pandas as pd
from src.utils.db import init_db_pool, close_db_pool, fetch
from src.utils.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = "reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)

async def fetch_data(limit: int = 2000) -> List[Dict[str, Any]]:
    """Fetch sample data from staging.records."""
    await init_db_pool()
    try:
        sql = f"SELECT * FROM staging.records LIMIT {limit}"
        logger.info(f"Fetching up to {limit} rows from staging.records...")
        rows = await fetch(sql)
        logger.info(f"Fetched {len(rows)} rows.")
        return [dict(row) for row in rows]
    finally:
        await close_db_pool()

def analyze_field(df: pd.DataFrame, field: str, expected_type: str) -> Dict[str, Any]:
    """Analyze a single field for quality metrics."""
    series = df[field]
    total = len(series)
    nulls = series.isnull().sum()
    null_pct = (nulls / total) * 100 if total > 0 else 0
    
    # Type check
    observed_types = series.dropna().apply(type).unique()
    observed_types_str = [t.__name__ for t in observed_types]
    
    # Value stats
    examples = series.dropna().head(3).tolist()
    unique_count = series.nunique()
    
    errors = 0
    error_examples = []
    
    # Basic type validation logic (simplified)
    if expected_type == 'NUMERIC' or expected_type == 'INTEGER':
        # Check if non-numeric
        for val in series.dropna():
            if not isinstance(val, (int, float, Decimal)):
                errors += 1
                if len(error_examples) < 3:
                    error_examples.append(str(val))
    elif expected_type == 'TIMESTAMPTZ':
        for val in series.dropna():
            if not isinstance(val, datetime):
                errors += 1
                if len(error_examples) < 3:
                    error_examples.append(str(val))

    return {
        "field": field,
        "schema_type": expected_type,
        "observed_types": observed_types_str,
        "null_pct": round(null_pct, 2),
        "unique_count": unique_count,
        "errors": errors,
        "examples": [str(e) for e in examples],
        "error_examples": error_examples
    }

def generate_report(analysis_results: List[Dict[str, Any]], row_count: int):
    """Generate Markdown report."""
    report_path = os.path.join(OUTPUT_DIR, "data_audit_report.md")
    
    with open(report_path, "w") as f:
        f.write("# Data Audit Report\n\n")
        f.write(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Rows Sampled:** {row_count}\n\n")
        
        f.write("## Field Analysis\n\n")
        f.write("| Field | Schema Type | Observed Types | % Null | Unique | Errors | Examples |\n")
        f.write("|-------|-------------|----------------|--------|--------|--------|----------|\n")
        
        for res in analysis_results:
            obs_types = ", ".join(res['observed_types'])
            examples = ", ".join(res['examples'])
            f.write(f"| {res['field']} | {res['schema_type']} | {obs_types} | {res['null_pct']}% | {res['unique_count']} | {res['errors']} | {examples} |\n")
        
        f.write("\n## Recommendations\n\n")
        f.write("- [ ] Review fields with high NULL percentage.\n")
        f.write("- [ ] Fix type mismatches where observed types differ from schema.\n")

    logger.info(f"Report generated at {report_path}")

def generate_fixes(analysis_results: List[Dict[str, Any]]):
    """Generate SQL fixes."""
    fixes_path = os.path.join(OUTPUT_DIR, "fixes.sql")
    with open(fixes_path, "w") as f:
        f.write("-- Recommended SQL Fixes\n\n")
        for res in analysis_results:
            if res['errors'] > 0:
                f.write(f"-- Fix errors in {res['field']}\n")
                f.write(f"-- UPDATE staging.records SET {res['field']} = NULL WHERE ...;\n\n")
    logger.info(f"Fixes generated at {fixes_path}")

def generate_snippets():
    """Generate Python cleaning snippets."""
    snippets_path = os.path.join(OUTPUT_DIR, "cleaning_snippets.py")
    with open(snippets_path, "w") as f:
        f.write("def clean_currency(val):\n")
        f.write("    if not val: return None\n")
        f.write("    return str(val).upper().strip()\n\n")
    logger.info(f"Snippets generated at {snippets_path}")

async def main():
    # Define schema mapping based on schema.sql
    schema_map = {
        'raw_id': 'BIGINT',
        'sheet_row_number': 'INTEGER',
        'received_at': 'TIMESTAMPTZ',
        'date': 'TIMESTAMPTZ',
        'payment_date': 'TIMESTAMPTZ',
        'task': 'TEXT',
        'type': 'TEXT',
        'year': 'INTEGER',
        'hours': 'NUMERIC',
        'month': 'INTEGER',
        'client': 'TEXT',
        'fx_rub': 'NUMERIC',
        'fx_usd': 'NUMERIC',
        'vendor': 'TEXT',
        'cashier': 'TEXT',
        'cat_new': 'TEXT',
        'quarter': 'INTEGER',
        'service': 'TEXT',
        'approver': 'TEXT',
        'category': 'TEXT',
        'currency': 'TEXT',
        'cat_final': 'TEXT',
        'total_rub': 'NUMERIC',
        'total_usd': 'NUMERIC',
        'subcat_new': 'TEXT',
        'paket': 'TEXT',
        'description': 'TEXT',
        'subcategory': 'TEXT',
        'payment_date_orig': 'TIMESTAMPTZ',
        'subcat_final': 'TEXT',
        'count_vendor': 'INTEGER',
        'statya': 'TEXT',
        'sum_total_rub': 'NUMERIC',
        'usd_summa': 'NUMERIC',
        'direct_indirect': 'TEXT',
        'package_secondary': 'TEXT',
        'total_in_currency': 'NUMERIC',
        'rub_summa': 'NUMERIC',
        'kategoriya': 'TEXT',
        'podstatya': 'TEXT',
        'vidy_raskhodov': 'TEXT',
        'payload_hash': 'TEXT',
        'raw_payload': 'JSONB'
    }

    data = await fetch_data()
    if not data:
        logger.warning("No data found in staging.records")
        return

    df = pd.DataFrame(data)
    
    results = []
    for field, dtype in schema_map.items():
        if field in df.columns:
            results.append(analyze_field(df, field, dtype))
        else:
            logger.warning(f"Field {field} not found in data columns")

    generate_report(results, len(df))
    generate_fixes(results)
    generate_snippets()
    
    # Output summary to stdout
    summary = {
        "rows_sampled": len(df),
        "fields_checked": len(results),
        "problems_found": sum(r['errors'] for r in results)
    }
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
