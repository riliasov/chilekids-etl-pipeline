import logging
from .db import fetch, execute

logger = logging.getLogger(__name__)

async def build_campaigns_summary():
    rows = await fetch("SELECT payload FROM staging.data")
    items = [r['payload'] for r in rows]
    if not items: return
    
    groups = {}
    for it in items:
        if not isinstance(it, dict): continue
        cid = it.get('campaign_id')
        if not cid: continue
        agg = groups.setdefault(cid, {'impressions': 0, 'clicks': 0, 'cost': 0.0})
        agg['impressions'] += int(it.get('impressions') or 0)
        agg['clicks'] += int(it.get('clicks') or 0)
        agg['cost'] += float(it.get('cost') or 0)
        
    for cid, vals in groups.items():
        try:
            await execute(
                "INSERT INTO marts.campaigns_summary (campaign_id, impressions, clicks, cost, last_updated) VALUES ($1,$2,$3,$4,now()) ON CONFLICT (campaign_id) DO UPDATE SET impressions=EXCLUDED.impressions, clicks=EXCLUDED.clicks, cost=EXCLUDED.cost, last_updated=now()",
                cid, vals['impressions'], vals['clicks'], vals['cost']
            )
        except Exception: pass

async def build_financials():
    await execute('''
        CREATE TABLE IF NOT EXISTS marts.financials (
            year_month TEXT, type TEXT, total_rub NUMERIC, last_updated TIMESTAMP WITH TIME ZONE,
            PRIMARY KEY (year_month, type)
        )
    ''')
    sql = (
        "WITH s AS ("
        " SELECT COALESCE(NULLIF(payload->>'Payment date','')::timestamptz, NULLIF(payload->>'Date','')::timestamptz) AS dt,"
        " trim(payload->>'Type') AS type,"
        " (payload->>'Total RUB')::numeric AS total_rub"
        " FROM staging.data WHERE payload ? 'Total RUB'"
        " ) SELECT to_char(date_trunc('month', dt), 'YYYY-MM') AS year_month, type, SUM(total_rub) AS total_rub"
        " FROM s WHERE dt IS NOT NULL AND (type = 'Доход' OR type = 'Расход') AND dt >= '2005-01-01'::timestamptz"
        " GROUP BY year_month, type"
    )
    rows = await fetch(sql)
    for r in rows:
        try:
            total_rounded = int(round(float(r['total_rub'] or 0)))
            await execute(
                "INSERT INTO marts.financials (year_month, type, total_rub, last_updated) VALUES ($1,$2,$3,now()) ON CONFLICT (year_month, type) DO UPDATE SET total_rub = EXCLUDED.total_rub, last_updated = now()",
                r['year_month'], r['type'], total_rounded
            )
        except Exception: pass

async def build_expenses_by_category():
    await execute('''
        CREATE TABLE IF NOT EXISTS marts.expenses_by_category (
            category TEXT, total_rub NUMERIC, last_updated TIMESTAMP WITH TIME ZONE,
            PRIMARY KEY (category)
        )
    ''')
    sql = (
        "WITH s AS ("
        " SELECT COALESCE(payload->>'Category', payload->>'Категория', 'Uncategorized') AS category,"
        " (payload->>'Total RUB')::numeric AS total_rub, trim(payload->>'Type') AS type"
        " FROM staging.data WHERE payload ? 'Total RUB'"
        " ) SELECT category, SUM(total_rub) AS total_rub FROM s WHERE (type = 'Расход' OR type = 'Expense') GROUP BY category"
    )
    rows = await fetch(sql)
    for r in rows:
        try:
            total_rounded = int(round(float(r['total_rub'] or 0)))
            await execute(
                "INSERT INTO marts.expenses_by_category (category, total_rub, last_updated) VALUES ($1,$2,now()) ON CONFLICT (category) DO UPDATE SET total_rub = EXCLUDED.total_rub, last_updated = now()",
                r['category'], total_rounded
            )
        except Exception: pass

async def build_all():
    await build_campaigns_summary()
    await build_financials()
    await build_expenses_by_category()
