import os
from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime

# MongoDB connection details
CONNECTION_STRING = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
DATABASE_NAME = "lead_generation_db"
COLLECTION_NAME = "unified_leads"

def cleanup_duplicates():
    client = None
    try:
        client = MongoClient(CONNECTION_STRING)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Aggregate to find duplicates based on 'url'
        pipeline = [
            {"$group": {
                "_id": "$url",
                "count": {"$sum": 1},
                "ids": {"$push": "$_id"},
                "latest_doc": {"$last": "$$ROOT"} # Get the last document in the group
            }},
            {"$match": {"count": {"$gt": 1}}}
        ]

        duplicate_urls = list(collection.aggregate(pipeline))

        print(f"Found {len(duplicate_urls)} URLs with duplicate entries.")

        for doc in duplicate_urls:
            url = doc["_id"]
            ids_to_remove = doc["ids"]
            latest_doc = doc["latest_doc"]

            print(f"Processing duplicates for URL: {url}")
            print(f"  Total duplicates: {doc['count']}")
            
            # Remove all duplicates except the latest one
            # The latest_doc['_id'] might be one of the ids_to_remove, so we exclude it
            ids_to_delete = [oid for oid in ids_to_remove if oid != latest_doc['_id']]

            if ids_to_delete:
                print(f"  Deleting {len(ids_to_delete)} older duplicate(s) for URL: {url}")
                collection.delete_many({"_id": {"$in": ids_to_delete}})
            else:
                print(f"  No older duplicates to delete for URL: {url} (only one unique _id for this URL).")
            
            # Ensure the latest_doc has the correct _id and update it if necessary
            # This handles cases where _id might have been re-generated but URL is same
            collection.replace_one({"url": url}, latest_doc, upsert=True)
            print(f"  Ensured latest version of {url} is present.")

        print("\nDuplicate cleanup complete.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    cleanup_duplicates()