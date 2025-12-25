import asyncio

import asyncpg
import pytest

from src.config import settings


@pytest.mark.asyncio
async def test_connection():
    print(f"Testing connection to: {settings.POSTGRES_URI}")
    try:
        print("Creating connection...")
        conn = await asyncio.wait_for(asyncpg.connect(dsn=str(settings.POSTGRES_URI)), timeout=10.0)
        print("Connection successful!")
        await conn.close()
        print("Connection closed.")
    except TimeoutError:
        print("ERROR: Connection timed out after 10 seconds")
    except Exception as e:
        pytest.fail(f"Connection failed: {e}")


if __name__ == "__main__":
    asyncio.run(test_connection())
