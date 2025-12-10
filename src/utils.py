import json
import hashlib
import aiohttp
import asyncio
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

# --- Hash Utils ---

def payload_hash(payload: Dict[str, Any]) -> str:
    """
    Вычисляет детерминированный MD5 хеш словаря payload.
    """
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':')
    )
    return hashlib.md5(serialized.encode('utf-8')).hexdigest()

# --- HTTP Utils ---

async def request_with_retries(method: str, url: str, retries: int = 3, backoff: float = 1.0, **kwargs):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(method, url, **kwargs) as resp:
                    if resp.status in (429, 500, 502, 503, 504):
                        text = await resp.text()
                        raise RuntimeError(f"HTTP {resp.status}: {text}")
                    return resp
        except Exception as exc:
            last_exc = exc
            logger.warning("HTTP request failed (attempt %s/%s) %s: %s", attempt, retries, url, exc)
            if attempt == retries:
                raise
            await asyncio.sleep(backoff * attempt)
    raise last_exc
