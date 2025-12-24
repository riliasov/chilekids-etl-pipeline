import asyncpg
import json
import aiohttp
from typing import Any, Iterable, Optional
from contextlib import asynccontextmanager
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from .config import settings

# --- DB Section ---

_pool: asyncpg.Pool | None = None


async def init_db_pool(min_size: int | None = None, max_size: int | None = None) -> asyncpg.Pool:
    """Инициализирует глобальный пул соединений asyncpg."""
    global _pool
    if _pool is None:
        if min_size is None:
            min_size = int(getattr(settings, 'DB_POOL_MIN', 1))
        if max_size is None:
            max_size = int(getattr(settings, 'DB_POOL_MAX', 4))
        if min_size < 1: min_size = 1
        if max_size < 1: max_size = 1

        import asyncio
        import re
        try:
            _pool = await asyncio.wait_for(
                asyncpg.create_pool(
                    dsn=str(settings.POSTGRES_URI), min_size=min_size, max_size=max_size
                ),
                timeout=30.0
            )
        except Exception as e:
            # Mask password in DSN for logging
            masked_dsn = re.sub(r':([^@]+)@', ':***@', str(settings.POSTGRES_URI))
            logger.error(f"Failed to connect to DB. DSN: {masked_dsn} | Error: {e}")
            raise
    return _pool

async def close_db_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None

def get_db_pool() -> asyncpg.Pool | None:
    return _pool

@asynccontextmanager
async def acquire():
    pool = await init_db_pool()
    async with pool.acquire() as conn:
        yield conn

async def execute(sql: str, *args: Any) -> str:
    async with acquire() as conn:
        return await conn.execute(sql, *args)

async def executemany(sql: str, args_iter: Iterable[Iterable[Any]]) -> None:
    async with acquire() as conn:
        async with conn.transaction():
            await conn.executemany(sql, args_iter)

async def executemany_one_off(sql: str, args_iter: Iterable[Iterable[Any]]) -> None:
    """Запускает executemany, используя одноразовое соединение."""
    conn = await asyncpg.connect(dsn=str(settings.POSTGRES_URI))
    try:
        async with conn.transaction():
            await conn.executemany(sql, args_iter)
    finally:
        await conn.close()

async def fetch(sql: str, *args: Any):
    async with acquire() as conn:
        return await conn.fetch(sql, *args)

async def fetch_one_off(sql: str, *args: Any):
    """Запускает запрос с одноразовым соединением."""
    conn = await asyncpg.connect(dsn=str(settings.POSTGRES_URI))
    try:
        result = await conn.fetch(sql, *args)
        return result
    finally:
        await conn.close()

async def execute_one_off(sql: str, *args: Any):
    conn = await asyncpg.connect(dsn=str(settings.POSTGRES_URI))
    try:
        return await conn.execute(sql, *args)
    finally:
        await conn.close()

# --- Auth Section ---

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
