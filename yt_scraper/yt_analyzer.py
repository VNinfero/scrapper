import json
from collections import Counter
from typing import List, Dict, Any

def analyze_youtube_leads(leads: List[Dict[str, Any]]):
    """
    Analyzes a list of YouTube leads and prints various statistics.

    Args:
        leads (List[Dict[str, Any]]): A list of dictionaries, where each dictionary
                                      represents a scraped YouTube lead.
    """
    total_leads = len(leads)
    if total_leads == 0:
        print("\nNo leads to analyze.")
        return

    print("\n--- YouTube Leads Analysis Report ---")
    print(f"Total Leads: {total_leads}")

    # Field Population Analysis
    print("\n--- Field Population (%) ---")
    fields_to_check = ['title', 'description', 'emails', 'channel_name', 'category']
    field_counts = Counter()
    for lead in leads:
        for field in fields_to_check:
            if lead.get(field) and (isinstance(lead[field], str) and lead[field].strip() or \
                                    isinstance(lead[field], list) and len(lead[field]) > 0):
                field_counts[field] += 1
    
    for field in fields_to_check:
        percentage = (field_counts[field] / total_leads) * 100 if total_leads > 0 else 0
        print(f"{field.ljust(15)}: {percentage:.2f}%")

    # Sample Preview
    print("\n--- Sample Leads Preview (First 3) ---")
    for i, lead in enumerate(leads[:3]):
        print(f"\nLead {i+1}:")
        print(f"  Title      : {lead.get('title', 'N/A')[:70]}...")
        print(f"  Description: {lead.get('description', 'N/A')[:70]}...")
        emails = lead.get('emails', [])
        print(f"  Emails     : {', '.join(emails) if emails else 'N/A'}")
        print(f"  Channel    : {lead.get('channel_name', 'N/A')}")
        print(f"  Category   : {lead.get('category', 'N/A')}")

    # Top Channels and Categories
    print("\n--- Top Channels ---")
    channel_counts = Counter(lead.get('channel_name') for lead in leads if lead.get('channel_name'))
    for channel, count in channel_counts.most_common(5):
        print(f"- {channel}: {count} leads")

    print("\n--- Top Categories ---")
    category_counts = Counter(lead.get('category') for lead in leads if lead.get('category'))
    for category, count in category_counts.most_common(5):
        print(f"- {category}: {count} leads")

    print("\n--- End of Report ---")

if __name__ == "__main__":
    # Example Usage (dummy data)
    sample_leads_data = [
        {
            "title": "Travel Vlog: Exploring the Amazon Rainforest",
            "description": "An adventurous journey through the heart of the Amazon, encountering unique wildlife. Contact us at info@amazonexplorers.com",
            "emails": ["info@amazonexplorers.com"],
            "channel_name": "Wilderness Treks",
            "category": "Travel"
        },
        {
            "title": "Best Beaches in Thailand - A Complete Guide",
            "description": "Discover hidden gems and popular spots in Thailand's beautiful coastline. Reach out for collabs: travelwithme@gmail.com",
            "emails": ["travelwithme@gmail.com"],
            "channel_name": "Global Nomads",
            "category": "Travel"
        },
        {
            "title": "Coding Tutorial: Python for Beginners",
            "description": "Learn Python from scratch with this comprehensive tutorial. No prior experience needed.",
            "emails": [],
            "channel_name": "Code Academy",
            "category": "Education"
        },
        {
            "title": "Food Tour in Italy: A Culinary Adventure",
            "description": "Indulge in the delicious flavors of Italy. For business inquiries, email: foodtravel@example.com",
            "emails": ["foodtravel@example.com"],
            "channel_name": "Taste Explorers",
            "category": "Food & Travel"
        },
        {
            "title": "Top 10 Ancient Wonders of the World",
            "description": "A historical journey to the most magnificent structures ever built.",
            "emails": [],
            "channel_name": "History Buffs",
            "category": "History"
        },
        {
            "title": "Hiking in Patagonia: A Breathtaking Experience",
            "description": "Experience the stunning landscapes of Patagonia. Join our next trip!",
            "emails": [],
            "channel_name": "Wilderness Treks",
            "category": "Travel"
        },
        {
            "title": "Digital Marketing Strategies for Small Businesses",
            "description": "Boost your online presence with these effective marketing tips.",
            "emails": ["contact@digitalgrow.com"],
            "channel_name": "Digital Grow",
            "category": "Business"
        },
        {
            "title": "Exploring Japan: Culture, Food, and Traditions",
            "description": "An immersive experience into the rich culture of Japan. Collaborate with us: japanadventures@email.com",
            "emails": ["japanadventures@email.com"],
            "channel_name": "Global Nomads",
            "category": "Travel"
        }
    ]
    analyze_youtube_leads(sample_leads_data)