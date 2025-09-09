from database.mongodb_manager import MongoDBManager
import json

def query_social_media_data():
    manager = None
    all_found_data = []
    try:
        manager = MongoDBManager()
        
        # Define collections and potential social media fields
        collections_to_check = {
            'instagram': ['url', 'username'],
            'linkedin': ['url', 'username'],
            'web': ['source_url', 'social_media'],
            'youtube': ['url', 'channel_name'],
            'facebook': ['url', 'username'],
            'twitter': ['url', 'username'],
            'unified': ['url', 'profile.username', 'contact.social_media_handles']
        }

        for platform, fields in collections_to_check.items():
            collection_name = manager.collections.get(platform)
            if not collection_name:
                continue

            collection = manager.db[collection_name]
            
            print(f"Checking collection: {collection_name}")
            
            # Construct query to find documents with social media related fields
            query = {"$or": []}
            for field in fields:
                if field == 'contact.social_media_handles':
                    query["$or"].append({field: {"$ne": {}, "$exists": True}})
                elif field == 'social_media':
                    query["$or"].append({field: {"$ne": {}, "$exists": True}})
                else:
                    query["$or"].append({field: {"$ne": None, "$ne": "", "$exists": True}})
            
            # If no fields are added to query (e.g., for some platforms, the fields might be empty initially)
            if not query["$or"]:
                continue

            cursor = collection.find(query).limit(100) # Limit to 100 results per collection for brevity

            for doc in cursor:
                entry = {
                    'platform': platform,
                    'url': doc.get('url') or doc.get('source_url', 'N/A'),
                    'username': doc.get('username') or doc.get('channel_name') or doc.get('profile', {}).get('username', 'N/A'),
                    'social_media_handles': doc.get('contact', {}).get('social_media_handles', {}) or doc.get('social_media', {})
                }
                # Clean up empty social_media_handles
                if not entry['social_media_handles']:
                    del entry['social_media_handles']
                all_found_data.append(entry)
        
        if all_found_data:
            print(json.dumps(all_found_data, indent=2))
        else:
            print("No social media URLs or usernames found in any collection.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if manager:
            manager.close_connection()

if __name__ == '__main__':
    query_social_media_data()
