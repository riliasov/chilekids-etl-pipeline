import aiohttp
import logging
from typing import Any, Dict, List
from ..utils.config import settings
from ..utils.auth import upload_to_supabase_storage
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


async def fetch_bitrix24(method: str = 'crm.contact.list', params: dict | None = None) -> List[Dict[str, Any]]:
    params = params or {}
    base = settings.BITRIX_WEBHOOK
    if not base:
        logger.error('❌ BITRIX_WEBHOOK не настроен')
        return []
    url = f"{base}/{method}"
    results: List[Dict[str, Any]] = []
    async with aiohttp.ClientSession() as session:
        page = 1
        while True:
            try:
                async with session.get(url, params={**params, 'page': page}) as resp:
                    j = await resp.json()
            except Exception as exc:
                logger.warning('⚠️ Запрос к Bitrix не удался: %s', exc)
                break
            items = j.get('result') or j.get('items') or []
            if not items:
                break
            results.extend(items)
            if len(items) < 50:
                break
            page += 1

    df = pd.DataFrame(results)
    date_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    out_dir = Path(settings.ARCHIVE_PATH) / 'csv' / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"bitrix24_{date_str}.csv"
    df.to_csv(csv_path, index=False)
    try:
        with open(csv_path, 'rb') as fh:
            await upload_to_supabase_storage('archives', f"{date_str}/bitrix24_{date_str}.csv", fh.read(), 'text/csv')
    except Exception as exc:
        logger.warning('⚠️ Загрузка в Supabase не удалась: %s', exc)

    return results
