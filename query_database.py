#!/usr/bin/env python3
"""
Script to query and display detailed data from the web leads collection
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.mongodb_manager import get_mongodb_manager
import json
from datetime import datetime

def display_web_leads_data():
    """Display detailed data from the web_leads collection"""
    try:
        # Get MongoDB manager
        db_manager = get_mongodb_manager()

        if not db_manager:
            print("❌ Failed to connect to MongoDB")
            return

        # Get web leads collection
        web_collection = db_manager.db['web_leads']

        # Get all web leads (limit to 20 for display)
        web_leads = list(web_collection.find().sort('scraped_at', -1).limit(20))

        print(f"🌐 Found {len(web_leads)} web leads in the database (showing first 20):")
        print("=" * 100)

        for i, lead in enumerate(web_leads, 1):
            print(f"\n🔹 Web Lead #{i}")
            print(f"   Source URL: {lead.get('source_url', 'N/A')}")
            print(f"   Domain: {lead.get('domain', 'N/A')}")
            print(f"   Title: {lead.get('title', 'N/A')[:80]}{'...' if lead.get('title') and len(lead.get('title', '')) > 80 else ''}")

            # Contact information
            emails = lead.get('emails', [])
            phones = lead.get('phone_numbers', [])
            if emails:
                print(f"   Emails ({len(emails)}): {', '.join(emails[:3])}{'...' if len(emails) > 3 else ''}")
            if phones:
                print(f"   Phones ({len(phones)}): {', '.join(phones[:3])}{'...' if len(phones) > 3 else ''}")

            # Content information
            content = lead.get('content', {})
            if content:
                text_content = content.get('text', '')
                if text_content:
                    preview = text_content[:100].replace('\n', ' ').strip()
                    print(f"   Content Preview: {preview}{'...' if len(text_content) > 100 else ''}")

            # Metadata
            if 'scraped_at' in lead:
                scraped_at = lead['scraped_at']
                if isinstance(scraped_at, datetime):
                    print(f"   Scraped At: {scraped_at.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    print(f"   Scraped At: {scraped_at}")

            # Additional fields
            if 'company_name' in lead:
                print(f"   Company: {lead['company_name']}")
            if 'industry' in lead:
                print(f"   Industry: {lead['industry']}")
            if 'lead_category' in lead:
                print(f"   Category: {lead['lead_category']}")

            print("-" * 50)

        # Get statistics about web leads
        print("\n� Web Leads Statistics:")

        # Count by domain
        domain_counts = {}
        for lead in web_leads:
            domain = lead.get('domain', 'unknown')
            domain_counts[domain] = domain_counts.get(domain, 0) + 1

        print("   Top domains:")
        for domain, count in sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"     {domain}: {count} leads")

        # Count by industry
        industry_counts = {}
        for lead in web_leads:
            industry = lead.get('industry', 'unknown')
            industry_counts[industry] = industry_counts.get(industry, 0) + 1

        print("   Top industries:")
        for industry, count in sorted(industry_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"     {industry}: {count} leads")

        # Total counts
        total_emails = sum(len(lead.get('emails', [])) for lead in web_leads)
        total_phones = sum(len(lead.get('phone_numbers', [])) for lead in web_leads)

        print(f"   Total emails found: {total_emails}")
        print(f"   Total phone numbers found: {total_phones}")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    display_web_leads_data()
