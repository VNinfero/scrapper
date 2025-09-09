import asyncio
import sys
import os

# Add parent directory to path to import database module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.mongodb_manager import get_mongodb_manager

async def main():
    """
    Main function to trigger the unified leads enrichment process.
    """
    print("🚀 Starting unified leads enrichment process...")
    mongodb_manager = None
    try:
        mongodb_manager = get_mongodb_manager()
        stats = mongodb_manager.enrich_unified_leads_from_sources()
        print("\n📊 Enrichment process summary:")
        print(f"   - Total unified leads: {stats.get('total_unified_leads', 0)}")
        print(f"   - Leads enriched: {stats.get('leads_enriched', 0)}")
        print(f"   - Leads skipped: {stats.get('leads_skipped', 0)}")
        print("\n✅ Enrichment process completed.")
    except Exception as e:
        print(f"❌ An error occurred during enrichment: {e}")
    finally:
        if mongodb_manager:
            mongodb_manager.close_connection()
            print("🔌 MongoDB connection closed.")

if __name__ == "__main__":
    asyncio.run(main())