import aiohttp
import pandas as pd
from typing import List, Dict, Any
from ..utils.auth import load_service_account_info, get_google_access_token, upload_to_supabase_storage
from ..utils.config import settings
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


async def fetch_google_sheets(spreadsheet_id: str, range_name: str = 'Sheet1!A:AF') -> List[Dict[str, Any]]:
    token = get_google_access_token()
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{range_name}"

    # Prefer service-account OAuth token. If not available, fall back to an
    # API key (useful for public sheets) if present in settings.
    headers = None
    params = None
    if token:
        headers = {"Authorization": f"Bearer {token}"}
    elif settings.SHEETS_API_KEY:
        params = {"key": settings.SHEETS_API_KEY}
    else:
        logger.error("Google service account token not available and no SHEETS_API_KEY configured")
        return []

    async with aiohttp.ClientSession() as session:
        logger.info(f"Fetching URL: {url}")
        async with session.get(url, headers=headers, params=params) as resp:
            logger.info(f"Response status: {resp.status}")
            data = await resp.json()
            logger.info("Data received")
    values = data.get('values', [])
    if not values:
        return []
    
    # Ensure we have headers for all 32 columns (A-AF)
    # If the sheet has fewer columns, we'll pad the headers and rows.
    raw_headers = values[0]
    expected_col_count = 32
    
    # Pad headers if necessary
    if len(raw_headers) < expected_col_count:
        raw_headers += [f"Column_{i+1}" for i in range(len(raw_headers), expected_col_count)]
    elif len(raw_headers) > expected_col_count:
         # If more, we slice (though A:AF request should limit this, sometimes manual edits mess it up)
         raw_headers = raw_headers[:expected_col_count]

    headers_row = raw_headers
    rows = values[1:]
    
    # Pad rows to match header length
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
        logger.warning("Upload to Supabase failed: %s", exc)

    return records
