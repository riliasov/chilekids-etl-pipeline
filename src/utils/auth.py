import json
import aiohttp
from typing import Optional
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from ..utils.config import settings


def load_service_account_info() -> Optional[dict]:
    path = settings.SHEETS_SA_JSON
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def get_google_access_token() -> Optional[str]:
    info = load_service_account_info()
    if not info:
        return None
    creds = service_account.Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    creds.refresh(Request())
    return creds.token


async def upload_to_supabase_storage(bucket: str, path: str, file_bytes: bytes, content_type: str = "application/octet-stream") -> dict:
    if not settings.SUPABASE_URL or not settings.SUPABASE_SERVICE_KEY:
        raise RuntimeError("Supabase storage not configured")
    url = f"{settings.SUPABASE_URL}/storage/v1/object/{bucket}/{path}"
    headers = {
        "apikey": settings.SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {settings.SUPABASE_SERVICE_KEY}",
        "Content-Type": content_type,
    }
    async with aiohttp.ClientSession() as session:
        async with session.put(url, data=file_bytes, headers=headers) as resp:
            try:
                return await resp.json()
            except Exception:
                return {"status": resp.status, "text": await resp.text()}
