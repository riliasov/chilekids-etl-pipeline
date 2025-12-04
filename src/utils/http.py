import aiohttp
import asyncio
import logging

logger = logging.getLogger(__name__)

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
