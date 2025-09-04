import requests
import pandas as pd
import re
import json
import argparse
from urllib.parse import urlparse
import os
import asyncio
from dotenv import load_dotenv

# Add parent directory to path to import linkedin_scraper module
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from linkedin_scraper.main import LinkedInScraperMain
from database.mongodb_manager import get_mongodb_manager # Import MongoDBManager

load_dotenv() # Load environment variables from .env file

# API Keys
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")

# --- Existing Scrapers (Instagram and LinkedIn) ---
def scrape_instagram_profile(username):
    url = f"https://api.apify.com/v2/acts/apify~instagram-profile-scraper/run-sync-get-dataset-items?token={APIFY_TOKEN}"
    payload = {"usernames": [username]}
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        if data:
            return data[0]
    except requests.exceptions.RequestException as e:
        print(f"Instagram Scraper Error for '{username}': {e}")
    return None

async def scrape_linkedin_company(company_name):
    """
    Scrapes LinkedIn company data using the project's LinkedInScraperMain.
    """
    scraper = LinkedInScraperMain(headless=True, enable_anti_detection=True)
    try:
        # LinkedInScraperMain expects URLs, so we'll construct a dummy URL for company name search
        # The scraper.scrape_async method handles its own internal start/stop of the extractor
        dummy_url = f"https://www.linkedin.com/company/{company_name.lower().replace(' ', '-')}/"
        results = await scraper.scrape_async([dummy_url], output_filename="temp_linkedin_company.json")
        
        if results and results.get("scraped_data"):
            # Return the first scraped company data
            return results["scraped_data"][0]
        else:
            print(f"No data found for LinkedIn company: {company_name}")
            return None
    except Exception as e:
        print(f"LinkedIn Scraper Error for '{company_name}': {e}")
        return None
    finally:
        # No need to call scraper.stop() here, it's handled by scrape_async internally
        pass

# --- Hunter.io APIs ---
def get_emails_from_domain(domain):
    url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={HUNTER_API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if "data" in data and "emails" in data["data"]:
            return [email["value"] for email in data["data"]["emails"]]
    except requests.exceptions.RequestException as e:
        print(f"Hunter.io Domain Search Error for '{domain}': {e}")
    return []

def resolve_shortened_url(url):
    """Resolves a shortened URL to its final destination URL."""
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        response.raise_for_status()
        return response.url
    except requests.exceptions.RequestException as e:
        return url # Return original URL if resolution fails

# --- New Scrapers (Facebook and Twitter - with limitations) ---
def scrape_facebook_profile(name):
    
    print(f"Note: Direct email/phone scraping from Facebook profiles for '{name}' is limited.")
    return None

def scrape_twitter_profile(name):
   
    print(f"Note: Direct email/phone scraping from Twitter profiles for '{name}' is limited.")
    return None

# --- Generalized Contact Retrieval Function ---

def format_output(data, source_platform):
    emails = []
    phone_numbers = []
    
    if source_platform == "instagram":
        if data:
            if data.get("emails"):
                emails.extend(data["emails"])
            if data.get("phone_number"):
                phone_numbers.append(data["phone_number"])
    elif source_platform == "linkedin":
        if data:
            if data.get("emails"):
                emails.extend(data["emails"])
            if data.get("phone_number"):
                phone_numbers.append(data["phone_number"])
    elif source_platform == "facebook":
        if data:
            # Facebook data is limited, but if any contact info is found, add it
            pass
    elif source_platform == "twitter":
        if data:
            # Twitter data is limited, but if any contact info is found, add it
            pass
    
    return {
        "emails": emails,
        "phone_numbers": phone_numbers,
        "source": source_platform
    }

async def get_social_media_contacts(name):
    contacts = []

    # Instagram
    insta_data = scrape_instagram_profile(name)
    if insta_data:
        org_name = insta_data.get("fullName") or insta_data.get("username") or name
        website = insta_data.get("externalUrl")
        emails = []
        if website:
            domain = website.replace("http://", "").replace("https://", "").split("/")[0]
            emails = get_emails_from_domain(domain)

        phone_number = None
        if insta_data.get("biography"):
            phone_match = re.search(r'(\+\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', insta_data["biography"])
            if phone_match:
                phone_number = phone_match.group(0)
        
        if not phone_number and insta_data.get("businessContact"):
            phone_number = insta_data["businessContact"].get("phoneNumber")

        contacts.append(format_output({
            "emails": emails,
            "phone_number": phone_number
        }, "instagram"))

    # LinkedIn
    linkedin_data = await scrape_linkedin_company(name) # Await the async function
    if linkedin_data:
        # LinkedInScraperMain returns structured data, adapt it to format_output
        company_website = linkedin_data.get("website")
        emails = []
        if company_website:
            resolved_url = resolve_shortened_url(company_website)
            domain = urlparse(resolved_url).netloc
            # Check if domain is a LinkedIn domain, if so, try to derive a better one
            if "linkedin.com" in domain:
                # If the LinkedIn scraper provided a website, use that for Hunter.io
                # Otherwise, we can't reliably get a domain for Hunter.io from a generic LinkedIn URL
                if linkedin_data.get("website"):
                    resolved_company_website = resolve_shortened_url(linkedin_data["website"])
                    domain = urlparse(resolved_company_website).netloc
                else:
                    domain = None # Cannot proceed with Hunter.io without a proper domain
            
            if domain:
                emails = get_emails_from_domain(domain)
            else:
                emails = [] # No domain to search with
        
        # Extract phone numbers if available from the structured data
        phone_number = None
        if linkedin_data.get("contact_info") and linkedin_data["contact_info"].get("phone"):
            phone_number = linkedin_data["contact_info"]["phone"]

        contacts.append(format_output({
            "emails": emails,
            "phone_number": phone_number
        }, "linkedin"))

    # Facebook (placeholder)
    facebook_data = scrape_facebook_profile(name)
    if facebook_data:
        contacts.append(format_output(facebook_data, "facebook"))
    else:
        contacts.append(format_output(None, "facebook"))


    # Twitter (placeholder)
    twitter_data = scrape_twitter_profile(name)
    if twitter_data:
        contacts.append(format_output(twitter_data, "twitter"))
    else:
        contacts.append(format_output(None, "twitter"))

    return contacts

async def scrape_from_url(profile_url):
    parsed_url = urlparse(profile_url)
    domain = parsed_url.netloc
    path_segments = [segment for segment in parsed_url.path.strip('/').split('/') if segment] # Filter out empty strings

    if "instagram.com" in domain:
        if len(path_segments) > 0:
            username = path_segments[0]
            print(f"Scraping Instagram profile: {username}")
            data = scrape_instagram_profile(username)
            return format_output(data, "instagram")
        else:
            print("Could not extract Instagram username from URL.")
            return None
    elif "linkedin.com" in domain: # Simplified condition as we're using our own scraper
        print(f"Scraping LinkedIn URL: {profile_url}")
        # Use LinkedInScraperMain directly for URLs
        scraper = LinkedInScraperMain(headless=True, enable_anti_detection=True)
        try:
            results = await scraper.scrape_async([profile_url], output_filename="temp_linkedin_profile.json")
            if results and results.get("scraped_data"):
                linkedin_data = results["scraped_data"][0]
                
                emails = []
                phone_number = None

                # Prioritize company website extracted by LinkedIn scraper for Hunter.io lookup
                company_website_from_linkedin = linkedin_data.get("website")
                if company_website_from_linkedin:
                    resolved_company_url = resolve_shortened_url(company_website_from_linkedin)
                    company_domain = urlparse(resolved_company_url).netloc
                    emails.extend(get_emails_from_domain(company_domain))
                
                # Extract phone numbers if available from the structured data
                if linkedin_data.get("contact_info") and linkedin_data["contact_info"].get("phone"):
                    phone_number = linkedin_data["contact_info"]["phone"]

                return format_output({
                    "emails": emails,
                    "phone_number": phone_number
                }, "linkedin")
            else:
                print(f"No data found for LinkedIn URL: {profile_url}")
                return None
        except Exception as e:
            print(f"LinkedIn Scraper Error for '{profile_url}': {e}")
            return None
        finally:
            # No need to call scraper.stop() here, it's handled by scrape_async internally
            pass
    elif "facebook.com" in domain:
        # Facebook URLs can be complex, often with user IDs or vanity URLs
        # This is a simplified approach, might need more robust parsing for real-world scenarios
        if len(path_segments) > 0:
            profile_name = path_segments[0]
            print(f"Scraping Facebook profile: {profile_name}")
            data = scrape_facebook_profile(profile_name)
            return format_output(data, "facebook")
        else:
            print("Could not extract Facebook profile name from URL.")
            return None
    elif "twitter.com" in domain:
        if len(path_segments) > 0:
            username = path_segments[0]
            print(f"Scraping Twitter profile: {username}")
            data = scrape_twitter_profile(username)
            return format_output(data, "twitter")
        else:
            print("Could not extract Twitter username from URL.")
            return None
    else:
        print(f"Unsupported social media platform or URL format: {profile_url}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Social Media Scraper")
    parser.add_argument("-n", "--name", type=str, help="Organization name to search for")
    parser.add_argument("-u", "--url", type=str, help="Social media profile URL")
    args = parser.parse_args()

    async def main():
        # Variables to store raw scraped data
        instagram_scraped_data = None
        linkedin_scraped_data = None
        facebook_scraped_data = None
        twitter_scraped_data = None

        if args.name:
            search_name = args.name
            print(f"\nSearching for contact details for: {search_name}\n")
            
            instagram_scraped_data = scrape_instagram_profile(search_name)
            linkedin_scraped_data = await scrape_linkedin_company(search_name)
            facebook_scraped_data = scrape_facebook_profile(search_name)
            twitter_scraped_data = scrape_twitter_profile(search_name)

        elif args.url:
            profile_url = args.url
            print(f"\nScraping from URL: {profile_url}")
            
            parsed_url = urlparse(profile_url)
            domain = parsed_url.netloc
            
            # Call the specific scraper function based on URL
            if "instagram.com" in domain:
                username = [segment for segment in parsed_url.path.strip('/').split('/') if segment][0]
                instagram_scraped_data = scrape_instagram_profile(username)
            elif "linkedin.com" in domain:
                scraper = LinkedInScraperMain(headless=True, enable_anti_detection=True)
                results = await scraper.scrape_async([profile_url], output_filename="temp_linkedin_profile.json")
                if results and results.get("scraped_data"):
                    linkedin_scraped_data = results["scraped_data"][0]
            elif "facebook.com" in domain:
                profile_name = [segment for segment in parsed_url.path.strip('/').split('/') if segment][0]
                facebook_scraped_data = scrape_facebook_profile(profile_name)
            elif "twitter.com" in domain:
                username = [segment for segment in parsed_url.path.strip('/').split('/') if segment][0]
                twitter_scraped_data = scrape_twitter_profile(username)
            else:
                print(f"Unsupported social media platform or URL format: {profile_url}")
                return # Exit main() if unsupported

        else:
            print("Please provide either an organization name using -n or a URL using -u.")
            return # Exit main() if no args

        # Now, process and insert data into MongoDB
        db_manager = get_mongodb_manager()
        
        inserted_any = False

        if instagram_scraped_data:
            print(f"\nInstagram Scraped Data:\n{json.dumps(instagram_scraped_data, indent=4)}")
            result = db_manager.insert_and_transform_to_unified([instagram_scraped_data], "instagram")
            if result['success_count'] > 0:
                inserted_any = True
        
        if linkedin_scraped_data:
            print(f"\nLinkedIn Scraped Data:\n{json.dumps(linkedin_scraped_data, indent=4)}")
            result = db_manager.insert_and_transform_to_unified([linkedin_scraped_data], "linkedin")
            if result['success_count'] > 0:
                inserted_any = True

        if facebook_scraped_data:
            print(f"\nFacebook Scraped Data (Note: No unified transformation yet):\n{json.dumps(facebook_scraped_data, indent=4)}")
            # TODO: Implement transform_facebook_to_unified in mongodb_manager.py
            # For now, this data will not be inserted into the unified collection.
        
        if twitter_scraped_data:
            print(f"\nTwitter Scraped Data (Note: No unified transformation yet):\n{json.dumps(twitter_scraped_data, indent=4)}")
            # TODO: Implement transform_twitter_to_unified in mongodb_manager.py
            # For now, this data will not be inserted into the unified collection.

        if inserted_any:
            print("\nResults processed. Check MongoDB for inserted leads.")
        else:
            print("No contact details found or inserted into MongoDB.")
    
    asyncio.run(main())