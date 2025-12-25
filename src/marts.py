import logging
from typing import Any

from .db import execute, fetch

logger = logging.getLogger(__name__)


async def build_campaigns_summary() -> None:
    rows = await fetch("SELECT payload FROM staging.records")
    items = [r["payload"] for r in rows]
    if not items:
        return

    groups: dict[str, dict[str, Any]] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        cid = it.get("campaign_id")
        if not cid:
            continue
        agg = groups.setdefault(cid, {"impressions": 0, "clicks": 0, "cost": 0.0})
        agg["impressions"] += int(it.get("impressions") or 0)
        agg["clicks"] += int(it.get("clicks") or 0)
        agg["cost"] += float(it.get("cost") or 0)

    for cid, vals in groups.items():
        try:
            sql = (
                "INSERT INTO marts.campaigns_summary (campaign_id, impressions, clicks, cost, last_updated) "
                "VALUES ($1, $2, $3, $4, now()) ON CONFLICT (campaign_id) DO UPDATE SET "
                "impressions=EXCLUDED.impressions, clicks=EXCLUDED.clicks, cost=EXCLUDED.cost, last_updated=now()"
            )
            await execute(
                sql,
                cid,
                vals["impressions"],
                vals["clicks"],
                vals["cost"],
            )
        except Exception:
            pass


async def build_all() -> None:
    """legacy: витрины теперь рассчитываются на уровне SQL Views в реальном времени."""
    pass
