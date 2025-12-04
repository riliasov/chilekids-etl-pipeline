import aiohttp
import logging
from typing import Any, Dict, List
from ..utils.config import settings
from ..utils.auth import upload_to_supabase_storage
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


async def fetch_google_ads(customer_id: str) -> List[Dict[str, Any]]:
    token = settings.GOOGLE_ADS_TOKEN
    if not token:
        logger.warning('⚠️ GOOGLE_ADS_TOKEN не настроен')
        return []
    url = f"https://googleads.googleapis.com/v12/customers/{customer_id}/campaigns"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers) as resp:
                j = await resp.json()
        except Exception as exc:
            logger.warning('⚠️ Ошибка получения данных Google Ads: %s', exc)
            return []

    items = j.get('results') or j.get('campaigns') or []

    df = pd.DataFrame(items)
    date_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    out_dir = Path(settings.ARCHIVE_PATH) / 'csv' / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"google_ads_{customer_id}_{date_str}.csv"
    df.to_csv(csv_path, index=False)
    try:
        with open(csv_path, 'rb') as fh:
            await upload_to_supabase_storage('archives', f"{date_str}/google_ads_{customer_id}.csv", fh.read(), 'text/csv')
    except Exception as exc:
        logger.warning('⚠️ Загрузка в Supabase не удалась: %s', exc)

    return items
