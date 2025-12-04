import asyncio
from src.utils.db import fetch_one_off

async def verify_marts():
    print("=== Checking raw.data ===")
    raw_count = await fetch_one_off("SELECT COUNT(*) as count FROM raw.data WHERE source = 'google_sheets'")
    print(f"Raw records: {raw_count[0]['count']}")
    
    print("\n=== Checking staging.data ===")
    staging_count = await fetch_one_off("SELECT COUNT(*) as count FROM staging.data WHERE source = 'google_sheets'")
    print(f"Staging records: {staging_count[0]['count']}")
    
    print("\n=== Checking marts.expenses_by_category ===")
    try:
        expenses = await fetch_one_off("""
            SELECT category, total_rub, last_updated 
            FROM marts.expenses_by_category 
            ORDER BY total_rub DESC 
            LIMIT 10
        """)
        print(f"Total categories: {len(expenses) if expenses else 0}")
        if expenses:
            print("\nTop 10 categories by expense:")
            for row in expenses:
                print(f"  {row['category']}: {row['total_rub']:.2f} RUB (updated: {row['last_updated']})")
    except Exception as e:
        print(f"Error querying marts: {e}")

if __name__ == "__main__":
    asyncio.run(verify_marts())
