#!/usr/bin/env python3
"""
Script to analyze the structure and content of web leads collection
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.mongodb_manager import get_mongodb_manager
import json
from datetime import datetime

def analyze_web_leads():
    """Analyze the actual structure and content of web leads"""
    try:
        # Get MongoDB manager
        db_manager = get_mongodb_manager()

        if not db_manager:
            print("❌ Failed to connect to MongoDB")
            return

        # Get web leads collection
        web_collection = db_manager.db['web_leads']

        # Get total count
        total_count = web_collection.count_documents({})
        print(f"🌐 Total web leads in database: {total_count}")

        # Get one document to see its structure
        sample_doc = web_collection.find_one()
        if sample_doc:
            print("\n📋 Sample Web Lead Document Structure:")
            print("=" * 80)
            print(json.dumps(sample_doc, indent=2, default=str))
            print("=" * 80)

        # Get a few more documents to analyze patterns
        web_leads = list(web_collection.find().limit(10))

        print(f"\n🔍 Analysis of {len(web_leads)} sample web leads:")

        # Analyze what fields are actually populated
        field_counts = {}
        total_docs = len(web_leads)

        for lead in web_leads:
            for key in lead.keys():
                if key not in field_counts:
                    field_counts[key] = 0
                field_counts[key] += 1

        print("   Field population across documents:")
        for field, count in sorted(field_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_docs) * 100
            print(".1f")

        # Check for actual content
        print("\n📄 Content Analysis:")
        docs_with_content = 0
        docs_with_emails = 0
        docs_with_phones = 0

        for lead in web_leads:
            if lead.get('content') and lead.get('content', {}).get('text'):
                docs_with_content += 1

            if lead.get('emails'):
                docs_with_emails += 1

            if lead.get('phone_numbers'):
                docs_with_phones += 1

        print(f"   Documents with text content: {docs_with_content}")
        print(f"   Documents with emails: {docs_with_emails}")
        print(f"   Documents with phone numbers: {docs_with_phones}")

        # Show some actual data if available
        if docs_with_content > 0:
            print("\n   Sample content preview:")
            for i, lead in enumerate(web_leads):
                content = lead.get('content', {})
                if content and content.get('text'):
                    text = content['text'][:150].replace('\n', ' ').strip()
                    print(f"     Lead #{i+1}: {text}...")
                    break

        if docs_with_emails > 0:
            print("\n   Sample emails found:")
            for lead in web_leads:
                emails = lead.get('emails', [])
                if emails:
                    print(f"     {lead.get('domain', 'unknown')}: {emails}")
                    break

        # Show domains and industries
        print("\n🏢 Domain and Industry Analysis:")
        domains = {}
        industries = {}

        for lead in web_leads:
            domain = lead.get('domain', 'unknown')
            industry = lead.get('industry', 'unknown')

            domains[domain] = domains.get(domain, 0) + 1
            industries[industry] = industries.get(industry, 0) + 1

        print("   Top domains:")
        for domain, count in sorted(domains.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"     {domain}: {count}")

        print("   Top industries:")
        for industry, count in sorted(industries.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"     {industry}: {count}")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    analyze_web_leads()
