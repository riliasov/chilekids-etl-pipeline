import aiohttp
import logging
from typing import Any, Dict, List
from ..utils.config import settings
from ..utils.auth import upload_to_supabase_storage
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


async def fetch_youtube_analytics(channel_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    key = settings.YOUTUBE_KEY
    if not key:
        logger.warning('⚠️ YOUTUBE_KEY не настроен')
        return []
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {"part": "snippet", "channelId": channel_id, "maxResults": 50, "key": key}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as resp:
                j = await resp.json()
        except Exception as exc:
            logger.warning('⚠️ Ошибка получения данных YouTube: %s', exc)
            return []

    items = j.get('items', [])
    df = pd.json_normalize(items)
    date_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    out_dir = Path(settings.ARCHIVE_PATH) / 'csv' / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"youtube_{channel_id}_{date_str}.csv"
    df.to_csv(csv_path, index=False)
    try:
        with open(csv_path, 'rb') as fh:
            await upload_to_supabase_storage('archives', f"{date_str}/youtube_{channel_id}.csv", fh.read(), 'text/csv')
    except Exception as exc:
        logger.warning('⚠️ Загрузка в Supabase не удалась: %s', exc)

    return items
