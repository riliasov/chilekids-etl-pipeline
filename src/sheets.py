import aiohttp
import pandas as pd
import logging
from typing import List, Dict, Any
from pathlib import Path
from .db import get_google_access_token, upload_to_supabase_storage
from .config import settings

logger = logging.getLogger(__name__)

async def fetch_google_sheets(spreadsheet_id: str, range_name: str = 'Sheet1!A:AF') -> List[Dict[str, Any]]:
    token = get_google_access_token()
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{range_name}"

    headers = None
    params = None
    if token:
        headers = {"Authorization": f"Bearer {token}"}
    elif settings.SHEETS_API_KEY:
        params = {"key": settings.SHEETS_API_KEY}
    else:
        logger.error("❌ Токен сервисного аккаунта Google недоступен и SHEETS_API_KEY не настроен")
        return []

    async with aiohttp.ClientSession() as session:
        logger.info(f"📥 Загрузка URL: {url}")
        async with session.get(url, headers=headers, params=params) as resp:
            data = await resp.json()
            logger.info("✅ Данные получены")
    
    values = data.get('values', [])
    if not values:
        return []
    
    # Ensure we have headers for all 32 columns (A-AF)
    raw_headers = values[0]
    expected_col_count = 32
    
    if len(raw_headers) < expected_col_count:
        raw_headers += [f"Column_{i+1}" for i in range(len(raw_headers), expected_col_count)]
    elif len(raw_headers) > expected_col_count:
         raw_headers = raw_headers[:expected_col_count]

    headers_row = raw_headers
    rows = values[1:]
    
    records = [dict(zip(headers_row, r + [""] * (len(headers_row) - len(r)))) for r in rows]

    df = pd.DataFrame(records)
    date_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    out_dir = Path(settings.ARCHIVE_PATH) / 'csv' / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"google_sheets_{spreadsheet_id}.csv"
    df.to_csv(csv_path, index=False)
    try:
        with open(csv_path, 'rb') as fh:
            await upload_to_supabase_storage('archives', f"{date_str}/google_sheets_{spreadsheet_id}.csv", fh.read(), 'text/csv')
    except Exception as exc:
        logger.warning("⚠️ Загрузка в Supabase не удалась: %s", exc)

    return records

async def push_df_to_sheet(spreadsheet_id: str, sheet_name: str, df: pd.DataFrame) -> dict:
    token = get_google_access_token()
    if not token:
        raise RuntimeError('❌ Отсутствует токен доступа Google')
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{sheet_name}!A1:append?valueInputOption=RAW"
    values = [list(df.columns)] + df.fillna('').astype(str).values.tolist()
    body = {"values": values}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=body) as resp:
            try:
                return await resp.json()
            except Exception:
                text = await resp.text()
                return {"status": resp.status, "text": text}
