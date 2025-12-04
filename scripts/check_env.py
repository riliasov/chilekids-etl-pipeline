import asyncio
import os
import sys
from src.utils.db import init_db_pool, close_db_pool, fetch

async def check_env():
    print("Checking environment configuration...")
    
    # Check .env file existence
    if not os.path.exists('.env'):
        print("❌ .env file not found!")
        # Don't exit yet, maybe env vars are set in shell
    else:
        print("✅ .env file found.")

    # Check critical vars
    postgres_uri = os.getenv('POSTGRES_URI')
    if not postgres_uri:
        # Try loading from .env manually if not in env
        try:
            from dotenv import load_dotenv
            load_dotenv()
            postgres_uri = os.getenv('POSTGRES_URI')
        except ImportError:
            pass
            
    if not postgres_uri:
        print("❌ POSTGRES_URI not set.")
        return False
    else:
        print("✅ POSTGRES_URI is set.")

    # Test DB Connection
    print("Testing DB connection...")
    try:
        await init_db_pool()
        res = await fetch("SELECT 1 as val")
        if res and res[0]['val'] == 1:
            print("✅ DB Connection successful!")
        else:
            print("❌ DB Connection failed (unexpected result).")
            return False
    except Exception as e:
        print(f"❌ DB Connection failed: {e}")
        return False
    finally:
        await close_db_pool()
        
    return True

if __name__ == "__main__":
    try:
        success = asyncio.run(check_env())
        if not success:
            sys.exit(1)
    except Exception as e:
        print(f"❌ Script failed: {e}")
        sys.exit(1)
