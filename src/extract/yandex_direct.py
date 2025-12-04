import aiohttp
import logging
from typing import Any, Dict, List
from ..utils.config import settings
from ..utils.auth import upload_to_supabase_storage
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


async def fetch_yandex_direct(client_login: str) -> List[Dict[str, Any]]:
    token = settings.YANDEX_DIRECT_TOKEN
    if not token:
        logger.warning('⚠️ YANDEX_DIRECT_TOKEN не настроен')
        return []
    url = f"https://api.direct.yandex.com/json/v5/campaigns"
    headers = {"Authorization": f"Bearer {token}", "Accept-Language": "ru"}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, headers=headers, json={"method": "get", "params": {}}) as resp:
                j = await resp.json()
        except Exception as exc:
            logger.warning('⚠️ Ошибка получения данных Yandex Direct: %s', exc)
            return []

    items = j.get('result') or []

    df = pd.DataFrame(items)
    date_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    out_dir = Path(settings.ARCHIVE_PATH) / 'csv' / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"yandex_direct_{client_login}_{date_str}.csv"
    df.to_csv(csv_path, index=False)
    try:
        with open(csv_path, 'rb') as fh:
            await upload_to_supabase_storage('archives', f"{date_str}/yandex_direct_{client_login}.csv", fh.read(), 'text/csv')
    except Exception as exc:
        logger.warning('⚠️ Загрузка в Supabase не удалась: %s', exc)

    return items
