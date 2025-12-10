import asyncio
from src.db import get_google_access_token

def test_auth():
    print("Getting token...")
    try:
        token = get_google_access_token()
        if token:
            print("Token received (length):", len(token))
        else:
            print("No token received.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_auth()
