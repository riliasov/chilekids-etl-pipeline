import aiohttp
import logging
from typing import Any, Dict, List
from ..utils.config import settings
from ..utils.auth import upload_to_supabase_storage
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


async def fetch_meta_ads(account_id: str) -> List[Dict[str, Any]]:
    token = settings.META_TOKEN
    if not token:
        logger.warning('META_TOKEN not configured')
        return []
    url = f"https://graph.facebook.com/v16.0/{account_id}/campaigns"
    params = {"access_token": token}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as resp:
                j = await resp.json()
        except Exception as exc:
            logger.warning('Meta Ads fetch failed: %s', exc)
            return []

    items = j.get('data', [])

    df = pd.DataFrame(items)
    date_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    out_dir = Path(settings.ARCHIVE_PATH) / 'csv' / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"meta_ads_{account_id}_{date_str}.csv"
    df.to_csv(csv_path, index=False)
    try:
        with open(csv_path, 'rb') as fh:
            await upload_to_supabase_storage('archives', f"{date_str}/meta_ads_{account_id}.csv", fh.read(), 'text/csv')
    except Exception as exc:
        logger.warning('Supabase upload failed: %s', exc)

    return items
