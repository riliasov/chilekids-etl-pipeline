import aiohttp
import pandas as pd
from ..utils.auth import get_google_access_token
import logging

logger = logging.getLogger(__name__)


async def push_df_to_sheet(spreadsheet_id: str, sheet_name: str, df: pd.DataFrame) -> dict:
    token = get_google_access_token()
    if not token:
        raise RuntimeError('Google access token missing')
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
