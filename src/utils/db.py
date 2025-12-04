import asyncpg
from typing import Any, Iterable
from contextlib import asynccontextmanager
from .config import settings

_pool: asyncpg.Pool | None = None


async def init_db_pool(min_size: int | None = None, max_size: int | None = None) -> asyncpg.Pool:
    """Initialize a global asyncpg pool if not already present.

    Defaults to values from settings.DB_POOL_MIN / DB_POOL_MAX to avoid
    exhausting connections on hosted Postgres (Supabase) plans.
    """
    global _pool
    if _pool is None:
        # use provided args or fall back to configured settings
        if min_size is None:
            min_size = int(getattr(settings, 'DB_POOL_MIN', 1))
        if max_size is None:
            max_size = int(getattr(settings, 'DB_POOL_MAX', 4))
        # defensive: ensure sensible bounds
        if min_size < 1:
            min_size = 1
        if max_size < 1:
            max_size = 1

        import asyncio
        _pool = await asyncio.wait_for(
            asyncpg.create_pool(
                dsn=str(settings.POSTGRES_URI), min_size=min_size, max_size=max_size
            ),
            timeout=30.0
        )
    return _pool

async def close_db_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_db_pool() -> asyncpg.Pool | None:
    """Get the current database pool (must be initialized first)."""
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
    """Run executemany using a one-off connection (no persistent pool).

    Useful for bulk-loading scripts that should not keep a pool open.
    """
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
    """Run a single-query connection (no pool) and close it immediately.

    Useful for short-lived CLI scripts or maintenance tasks to avoid
    keeping many persistent connections open.
    """
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
