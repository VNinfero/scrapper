"""
Facebook Data Extractor 
Uses browser automation with JSON-LD extraction and custom selectors
"""

import asyncio
import json
import re
import time
from typing import Dict, Any, Optional, List
from bs4 import BeautifulSoup
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from web_scraper.scrapers.scraper_dynamic import fetch_dynamic_async # Import the async function
from database.mongodb_manager import get_mongodb_manager # Import the MongoDB manager

class FacebookDataExtractor:
    """Facebook data extractor"""
    
    def __init__(self, proxy: Optional[Dict[str, str]] = None):
        # Parameters like headless, enable_anti_detection, is_mobile are handled internally by fetch_dynamic_async
        self.mongodb_manager = get_mongodb_manager()
        self.proxy = proxy

    async def extract_facebook_data(self, url: str) -> Dict[str, Any]:
        """Extract Facebook data from a specific URL"""
        print(f"Extracting Facebook data from: {url}")
        
        try:
            # Use fetch_dynamic_async directly; it manages headless and anti-detection internally
            page_content = await fetch_dynamic_async(url, proxy=self.proxy)
            
            html_content = page_content.html
            rendered_text = page_content.text
            
            extracted_data = {
                'url': url,
                'url_type': 'unknown', # Initialize as unknown, will be detected later
                'html_length': len(html_content),
                'text_length': len(rendered_text),
                'json_ld_data': {},
                'meta_data': {},
                'extracted_data': {},
                'page_analysis': {}
            }
            
            json_ld_data = await self._extract_json_ld_data(html_content, 'unknown') # Pass 'unknown' for now
            extracted_data['json_ld_data'] = json_ld_data
            
            meta_data = await self._extract_meta_data(html_content)
            extracted_data['meta_data'] = meta_data
            print(f"DEBUG: Extracted meta_data: {json.dumps(meta_data, indent=2)}")
            
            # Detect URL type after meta_data is available
            url_type = self._detect_url_type(url, meta_data)
            extracted_data['url_type'] = url_type # Update url_type in extracted_data
            
            combined_data = await self._combine_data_sources(json_ld_data, meta_data, url_type)
            extracted_data['extracted_data'] = combined_data
            
            page_analysis = await self._analyze_page_content(rendered_text, html_content, url_type)
            extracted_data['page_analysis'] = page_analysis
            
            # network_analysis is not directly available from fetch_dynamic_async, so it's an empty dict
            
            return extracted_data
            
        except Exception as e:
            print(f"❌ Error extracting data from {url}: {e}")
            return {
                'url': url,
                'error': str(e),
                'success': False
            }

    def _detect_url_type(self, url: str, meta_data: Dict[str, Any]) -> str:
        """Detect the type of Facebook URL (profile, page, post, etc.) using URL patterns and Open Graph meta tags"""
        og_type = meta_data.get('open_graph', {}).get('og:type', '').lower()

        # 1. Post URLs (most specific URL patterns)
        if re.search(r'facebook\.com/story\.php\?story_fbid=\d+&id=\d+', url) or \
           re.search(r'facebook\.com/[^/]+/posts/\d+', url):
            return 'post'
        
        # 2. Profile URLs (specific URL patterns or og:type "profile")
        if re.search(r'facebook\.com/profile\.php\?id=\d+', url) or \
           og_type == 'profile':
            return 'profile'
        
        # 3. Page URLs (specific URL patterns)
        # Look for /pages/, /pg/, or /about sub-paths, or if og_type is clearly a page type
        if re.search(r'facebook\.com/(?:pages|pg)/[^/]+', url) or \
           re.search(r'facebook\.com/[^/]+/about/?', url) or \
           og_type in ['website', 'article', 'business.business', 'books.book', 'music.song', 'video.movie', 'video.tv_show', 'video.episode']:
            return 'page'
        
        # 4. Generic username URLs (e.g., facebook.com/facebook)
        # If it's a simple facebook.com/username and not caught by above,
        # try to infer based on common page titles or if it's not a known profile-like pattern.
        if re.search(r'facebook\.com/[a-zA-Z0-9_.]+$', url):
            # Exclude known profile-like sub-paths and system paths
            if not re.search(r'facebook\.com/(?:friends|messages|bookmarks|saved|notifications|settings|help|privacy|terms|login|recover|marketplace|games|gaming|watch|live|stories|search|groups|events|findfriends|developers|business|creators|community|gamingvideo|jobs|weather|coronavirus|news|shops|offers|donations)/?$', url):
                # If og_type is not profile, and it's a generic username, it's likely a page
                if og_type != 'profile':
                    return 'page'

        return 'unknown'

    async def _extract_json_ld_data(self, html_content: str, url_type: str) -> Dict[str, Any]:
        """Extract JSON-LD data"""
        print("🔍 Extracting JSON-LD data (primary source)...")
        json_ld_data = {
            'found': False,
            'raw_json': None,
            'parsed_data': {},
            'data_type': None,
            'extraction_success': False
        }
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            json_ld_scripts = soup.find_all('script', type='application/ld+json')
            
            if not json_ld_scripts:
                print("❌ No JSON-LD scripts found")
                return json_ld_data
            
            print(f"✅ Found {len(json_ld_scripts)} JSON-LD script(s)")
            
            for script in json_ld_scripts:
                if script.string:
                    try:
                        json_data = json.loads(script.string)
                        json_ld_data['raw_json'] = json_data
                        json_ld_data['found'] = True
                        
                        if url_type == 'profile':
                            parsed_data = await self._parse_profile_json_ld(json_data)
                            json_ld_data['data_type'] = 'profile'
                        elif url_type == 'page':
                            parsed_data = await self._parse_page_json_ld(json_data)
                            json_ld_data['data_type'] = 'page'
                        elif url_type == 'post':
                            parsed_data = await self._parse_post_json_ld(json_data)
                            json_ld_data['data_type'] = 'post'
                        else:
                            parsed_data = await self._parse_generic_json_ld(json_data)
                            json_ld_data['data_type'] = 'generic'
                        
                        json_ld_data['parsed_data'] = parsed_data
                        json_ld_data['extraction_success'] = True
                        print(f"✅ Successfully parsed JSON-LD for {url_type}")
                        break
                    except json.JSONDecodeError as e:
                        print(f"❌ JSON-LD parsing error: {e}")
                        continue
                    except Exception as e:
                        print(f"❌ Error parsing JSON-LD: {e}")
                        continue
        except Exception as e:
            print(f"❌ Error extracting JSON-LD: {e}")
        return json_ld_data

    async def _parse_profile_json_ld(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse profile JSON-LD data"""
        profile_data = {}
        if json_data.get('@type') == 'Person':
            profile_data['name'] = json_data.get('name')
            profile_data['description'] = json_data.get('description')
            profile_data['url'] = json_data.get('url')
            if 'image' in json_data and isinstance(json_data['image'], dict):
                profile_data['image'] = json_data['image'].get('url')
            if 'mainEntityOfPage' in json_data and isinstance(json_data['mainEntityOfPage'], dict):
                profile_data['mainEntityOfPage'] = json_data['mainEntityOfPage'].get('@id')
            print(f"✅ Extracted profile data from JSON-LD: {profile_data.get('name', 'Unknown')}")
        return profile_data

    async def _parse_page_json_ld(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse page JSON-LD data"""
        page_data = {}
        # Handle cases where Facebook might embed multiple JSON-LD objects in an array
        if isinstance(json_data, list):
            for item in json_data:
                if item.get('@type') in ['Organization', 'LocalBusiness', 'WebSite']:
                    json_data = item # Use the first relevant object
                    break
            else: # No relevant object found in list
                return page_data

        if json_data.get('@type') in ['Organization', 'LocalBusiness', 'WebSite']:
            page_data['name'] = json_data.get('name')
            page_data['description'] = json_data.get('description')
            page_data['url'] = json_data.get('url')
            page_data['address'] = json_data.get('address')
            page_data['telephone'] = json_data.get('telephone')
            page_data['priceRange'] = json_data.get('priceRange')
            page_data['aggregateRating'] = json_data.get('aggregateRating')
            if 'image' in json_data and isinstance(json_data['image'], dict):
                page_data['image'] = json_data['image'].get('url')
            if 'logo' in json_data and isinstance(json_data['logo'], dict):
                page_data['logo'] = json_data['logo'].get('url')
            if 'sameAs' in json_data:
                page_data['sameAs'] = json_data['sameAs']
            if 'numberOfFollowers' in json_data:
                page_data['numberOfFollowers'] = json_data['numberOfFollowers']
            if 'member' in json_data and isinstance(json_data['member'], list):
                page_data['members'] = [m.get('name') for m in json_data['member'] if isinstance(m, dict)]
            print(f"✅ Extracted page data from JSON-LD: {page_data.get('name', 'Unknown')}")
        return page_data

    async def _parse_post_json_ld(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse post JSON-LD data"""
        post_data = {}
        if json_data.get('@type') in ['Article', 'SocialMediaPosting', 'NewsArticle', 'BlogPosting']:
            post_data['headline'] = json_data.get('headline')
            post_data['articleBody'] = json_data.get('articleBody')
            post_data['datePublished'] = json_data.get('datePublished')
            post_data['dateModified'] = json_data.get('dateModified')
            post_data['url'] = json_data.get('url') or json_data.get('mainEntityOfPage', {}).get('@id')
            if 'author' in json_data and isinstance(json_data['author'], dict):
                post_data['author_name'] = json_data['author'].get('name')
                post_data['author_url'] = json_data['author'].get('url')
            if 'publisher' in json_data and isinstance(json_data['publisher'], dict):
                post_data['publisher_name'] = json_data['publisher'].get('name')
                post_data['publisher_url'] = json_data['publisher'].get('url')
            if 'image' in json_data and isinstance(json_data['image'], dict):
                post_data['image'] = json_data['image'].get('url')
            if 'interactionStatistic' in json_data and isinstance(json_data['interactionStatistic'], list):
                for stat in json_data['interactionStatistic']:
                    if stat.get('interactionType') == 'http://schema.org/LikeAction':
                        post_data['likes'] = stat.get('userInteractionCount')
                    elif stat.get('interactionType') == 'http://schema.org/CommentAction':
                        post_data['comments'] = stat.get('userInteractionCount')
            print(f"✅ Extracted post data from JSON-LD: {post_data.get('headline', 'Unknown')[:50]}...")
        return post_data

    async def _parse_generic_json_ld(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse generic JSON-LD data"""
        generic_data = {}
        # Handle cases where Facebook might embed multiple JSON-LD objects in an array
        if isinstance(json_data, list):
            # Try to find the most descriptive object (e.g., Article, Organization, Person)
            for item in json_data:
                if item.get('@type') in ['Article', 'SocialMediaPosting', 'NewsArticle', 'BlogPosting', 'Organization', 'LocalBusiness', 'Person', 'WebSite']:
                    json_data = item
                    break
            else: # No specific type found, use the first object if available
                if json_data:
                    json_data = json_data[0]

        generic_data['type'] = json_data.get('@type')
        generic_data['name'] = json_data.get('name')
        generic_data['headline'] = json_data.get('headline') # Often found in generic articles
        generic_data['description'] = json_data.get('description')
        generic_data['url'] = json_data.get('url') or json_data.get('@id')
        if 'image' in json_data and isinstance(json_data['image'], dict):
            generic_data['image'] = json_data['image'].get('url')
        if 'datePublished' in json_data:
            generic_data['datePublished'] = json_data['datePublished']
        if 'author' in json_data and isinstance(json_data['author'], dict):
            generic_data['author_name'] = json_data['author'].get('name')
        print(f"✅ Extracted generic data from JSON-LD: {generic_data.get('type', 'Unknown')}")
        return generic_data

    async def _extract_meta_data(self, html_content: str) -> Dict[str, Any]:
        """Extract meta data from HTML content"""
        print("🔍 Extracting meta data (secondary source)...")
        soup = BeautifulSoup(html_content, 'html.parser')
        meta_data = {
            'open_graph': {},
            'twitter': {},
            'other_meta': {},
            'title': '',
            'description': ''
        }
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            name = meta.get('name') or meta.get('property')
            content = meta.get('content')
            if name and content:
                if name.startswith('og:'):
                    meta_data['open_graph'][name] = content
                elif name.startswith('twitter:'):
                    meta_data['twitter'][name] = content
                else:
                    meta_data['other_meta'][name] = content
        
        title_tag = soup.find('title')
        if title_tag:
            meta_data['title'] = title_tag.text
        
        description_tag = soup.find('meta', attrs={'name': 'description'})
        if description_tag:
            meta_data['description'] = description_tag.get('content', '')
        
        print(f"✅ Extracted meta data: {len(meta_data['open_graph'])} OpenGraph, {len(meta_data['twitter'])} Twitter")
        return meta_data

    async def _combine_data_sources(self, json_ld_data: Dict[str, Any], meta_data: Dict[str, Any], url_type: str) -> Dict[str, Any]:
        """Combine data from JSON-LD and meta sources"""
        print("🔍 Combining data sources...")
        combined_data = {}
        if json_ld_data.get('extraction_success'):
            combined_data.update(json_ld_data.get('parsed_data', {}))
        
        if meta_data:
            og_data = meta_data.get('open_graph', {})
            if og_data:
                combined_data['og_title'] = og_data.get('og:title', '')
                combined_data['og_description'] = og_data.get('og:description', '')
                combined_data['og_image'] = og_data.get('og:image', '')
                combined_data['og_url'] = og_data.get('og:url', '')
            
            twitter_data = meta_data.get('twitter', {})
            if twitter_data:
                combined_data['twitter_title'] = twitter_data.get('twitter:title', '')
                combined_data['twitter_description'] = twitter_data.get('twitter:description', '')
                combined_data['twitter_image'] = twitter_data.get('twitter:image', '')
            
            combined_data['page_title'] = meta_data.get('title', '')
            combined_data['page_description'] = meta_data.get('description', '')
        
        print(f"✅ Combined data sources: {len(combined_data)} fields")
        return combined_data

    async def _analyze_page_content(self, rendered_text: str, html_content: str, url_type: str) -> Dict[str, Any]:
        """Analyze page content for Facebook-specific data"""
        print("🔍 Analyzing page content...")
        analysis = {
            'facebook_keywords': [],
            'content_type': url_type,
            'text_summary': ''
        }
        
        facebook_keywords = [
            'likes', 'comments', 'shares', 'friends', 'followers', 'posts', 'photos',
            'videos', 'about', 'community', 'reviews', 'events', 'groups', 'marketplace',
            'facebook', 'meta'
        ]
        
        found_keywords = []
        for keyword in facebook_keywords:
            if keyword.lower() in rendered_text.lower():
                found_keywords.append(keyword)
        
        analysis['facebook_keywords'] = found_keywords
        
        lines = rendered_text.split('\n')
        non_empty_lines = [line.strip() for line in lines if line.strip()]
        analysis['text_summary'] = ' | '.join(non_empty_lines[:10])
        
        print(f"✅ Page content analysis completed. Found {len(found_keywords)} keywords.")
        return analysis

    async def save_facebook_data_to_json(self, extracted_data: Dict[str, Any], filename: str = "facebook_data.json") -> None:
        """Save Facebook data to a structured JSON file"""
        facebook_data = {
            "metadata": {
                "scraping_timestamp": time.time(),
                "scraping_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "extractor_version": "facebook_data_extractor_v1.0",
                "url": extracted_data.get('url'),
                "url_type": extracted_data.get('url_type'),
                "platform": "facebook",
                "data_sources": ["json_ld", "meta_tags"]
            },
            "extraction_summary": {
                "success": not extracted_data.get('error'),
                "json_ld_found": extracted_data.get('json_ld_data', {}).get('found', False),
                "json_ld_success": extracted_data.get('json_ld_data', {}).get('extraction_success', False),
                "meta_data_found": bool(extracted_data.get('meta_data', {}).get('open_graph')),
                "html_content_length": extracted_data.get('html_length', 0),
                "text_content_length": extracted_data.get('text_length', 0)
            },
            "extracted_data": {
                "json_ld_data": extracted_data.get('json_ld_data', {}),
                "meta_data": extracted_data.get('meta_data', {}),
                "combined_data": extracted_data.get('extracted_data', {}),
                "page_analysis": extracted_data.get('page_analysis', {})
            }
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(facebook_data, f, indent=2, ensure_ascii=False, default=str)
            print(f"\n✅ Facebook data saved to: {filename}")
        except Exception as e:
            print(f"❌ Error saving Facebook data to {filename}: {e}")

async def run_facebook_scraper_interactively():
    """Run the Facebook scraper interactively, allowing user to input URLs."""
    print("=" * 80)
    print("FACEBOOK DATA EXTRACTOR - INTERACTIVE MODE")
    print("=" * 80)

    example_urls = [
        
        "https://www.facebook.com/AirtelIndia/"

    ]

    print("\nSupported URL types:")
    print("  - Profile: https://www.facebook.com/username")
    print("  - Page: https://www.facebook.com/pagename")
    print("  - Post: https://www.facebook.com/username/posts/postid (Note: post scraping is less reliable due to Facebook's public access policies)")
    
    choice = ''
    try:
        choice = input("\nUse example URLs? (y/n, default: y): ").strip().lower()
    except EOFError:
        print("\nNon-interactive environment detected. Using example URLs.")
        choice = 'y' # Default to 'y' for non-interactive environments

    if choice == 'y' or choice == '':
        urls_to_scrape = example_urls
        print("\nUsing example URLs:")
        for url in example_urls:
            print(f"  - {url}")
    else:
        print("\nEnter Facebook URLs (one per line, press Enter twice when done):")
        urls_to_scrape = []
        while True:
            try:
                url = input().strip()
            except EOFError:
                print("\nNon-interactive environment detected. Stopping URL input.")
                break
            if not url:
                break
            if 'facebook.com' in url:
                urls_to_scrape.append(url)
            else:
                print("⚠️  Please enter a valid Facebook URL")
        
        if not urls_to_scrape:
            print("No valid URLs provided. Exiting.")
            return

    # For interactive mode, we don't have a proxy from the main orchestrator,
    # so we initialize without one or add a prompt for it if desired.
    # For now, let's assume no proxy for interactive testing.
    extractor = FacebookDataExtractor(proxy=None)

    try:
        for i, url in enumerate(urls_to_scrape, 1):
            print(f"\n{'='*60}")
            print(f"SCRAPING URL {i}/{len(urls_to_scrape)}: {url}")
            print(f"{'='*60}")
            
            try:
                extracted_data = await extractor.extract_facebook_data(url)
                
                if extracted_data.get('error'):
                    print(f"❌ Failed to extract data: {extracted_data['error']}")
                    continue
                
                # Determine a suitable filename
                url_type = extracted_data.get('url_type', 'unknown')
                filename_prefix = url_type if url_type != 'unknown' else 'facebook_data'
                filename = f"facebook_{filename_prefix}_{i}.json" # Append index to ensure unique filenames
                
                await extractor.save_facebook_data_to_json(extracted_data, filename)
                
                print(f"✓ URL Type: {extracted_data.get('url_type', 'unknown')}")
                print(f"✓ JSON-LD Found: {extracted_data.get('json_ld_data', {}).get('found', False)}")
                print(f"✓ Extracted Data:")
                for key, value in extracted_data.get('extracted_data', {}).items():
                    if isinstance(value, dict):
                        print(f"    - {key}:")
                        for sub_key, sub_value in value.items():
                            print(f"        - {sub_key}: {sub_value}")
                    else:
                        print(f"    - {key}: {value}")
                
                # Save raw extracted data to facebook_leads collection
                print(f"\n💾 Attempting to insert raw Facebook data into facebook_leads collection...")
                raw_data_insert_success = extractor.mongodb_manager.insert_facebook_lead(extracted_data)
                if raw_data_insert_success:
                    print(f"✅ Raw Facebook data inserted into facebook_leads collection.")
                else:
                    print(f"❌ Failed to insert raw Facebook data into facebook_leads collection (may already exist).")

                # Insert into unified collection
                print(f"\n💾 Attempting to insert into unified_leads collection...")
                
                # Prepare data for unified transformation.
                # The extracted_data from Facebook scraper is already somewhat processed.
                # Need to map it to the expected input for transform_facebook_to_unified.
                # The transform_facebook_to_unified expects specific keys which might not directly match
                # the top-level keys in extracted_data.
                # Let's create a dictionary that mimics the structure expected by the transformer.
                
                data_for_unified_transform = {
                    "url": extracted_data.get('url'),
                    "username": extracted_data.get('extracted_data', {}).get('og_url', '').split('/')[-1] if extracted_data.get('extracted_data', {}).get('og_url') else None,
                    "full_name": extracted_data.get('extracted_data', {}).get('og_title'),
                    "about": extracted_data.get('extracted_data', {}).get('og_description'),
                    "location": None, # Not directly available from current extraction
                    "email": extracted_data.get('extracted_data', {}).get('business_email'), # From meta, if available
                    "phone": extracted_data.get('extracted_data', {}).get('business_phone_number'), # From meta, if available
                    "address": extracted_data.get('extracted_data', {}).get('address'), # From JSON-LD, if available
                    "website": extracted_data.get('extracted_data', {}).get('og_url'),
                    "description": extracted_data.get('extracted_data', {}).get('page_description'),
                    "scraped_at": extracted_data.get('metadata', {}).get('scraping_timestamp')
                }
                
                # Check if the URL type is profile or page to populate full_name and username more accurately
                if extracted_data.get('url_type') == 'profile' or extracted_data.get('url_type') == 'page':
                    if extracted_data.get('extracted_data', {}).get('og_title'):
                        data_for_unified_transform['full_name'] = extracted_data['extracted_data']['og_title'].replace(' | Facebook', '')
                    if extracted_data.get('url'):
                        data_for_unified_transform['username'] = extracted_data['url'].split('/')[-1]

                unified_stats = extractor.mongodb_manager.insert_and_transform_to_unified(
                    [data_for_unified_transform], 'facebook'
                )
                
                print(f"✅ Unified insertion result: Success: {unified_stats['success_count']}, Updated: {unified_stats['updated_count']}, Failed: {unified_stats['failure_count']}")
                
            except Exception as e:
                print(f"❌ Error processing {url}: {e}")
                import traceback
                traceback.print_exc()
                
    except Exception as e:
        print(f"\n❌ Critical error during interactive scraping: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n✓ Facebook data extractor interactive session completed")

if __name__ == "__main__":
    asyncio.run(run_facebook_scraper_interactively())