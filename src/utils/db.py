import asyncpg
from typing import Any, Iterable
from contextlib import asynccontextmanager
from .config import settings

_pool: asyncpg.Pool | None = None


async def init_db_pool(min_size: int | None = None, max_size: int | None = None) -> asyncpg.Pool:
    """Инициализирует глобальный пул соединений asyncpg, если он еще не создан.

    Использует значения по умолчанию из settings.DB_POOL_MIN / DB_POOL_MAX, чтобы избежать
    исчерпания соединений на хостинговых планах Postgres (Supabase).
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
    """Возвращает текущий пул базы данных (должен быть сначала инициализирован)."""
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
    """Запускает executemany, используя одноразовое соединение (без постоянного пула).

    Полезно для скриптов массовой загрузки, которые не должны держать пул открытым.
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
    """Запускает запрос с одноразовым соединением (без пула) и сразу закрывает его.

    Полезно для короткоживущих CLI скриптов или задач обслуживания, чтобы избежать
    удержания множества постоянных соединений.
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
