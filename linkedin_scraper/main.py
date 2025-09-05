"""
LinkedIn Scraper - Main Interface
Simple one-line usage: linkedin_scraper(urls)

Usage:
    from main import linkedin_scraper
    
    urls = [
        "https://www.linkedin.com/in/williamhgates/",
        "https://www.linkedin.com/company/microsoft/",
        "https://www.linkedin.com/posts/aiqod_inside-aiqod-how-were-building-enterprise-ready-activity-7348224698146541568-N7oQ",
        "https://www.linkedin.com/newsletters/aiqod-insider-7325820451622940672"
    ]
    
    results = linkedin_scraper(urls)
"""

import asyncio
import json
import time
import re
import sys
import os
from typing import List, Dict, Any, Optional
import random

# Add parent directory to path to import database module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from linkedin_scraper.linkedin_data_extractor import LinkedInDataExtractor
from database.mongodb_manager import get_mongodb_manager


class LinkedInScraperMain:
    """Main LinkedIn Scraper class with simplified interface"""
    
    def __init__(self, headless: bool = True, enable_anti_detection: bool = True, use_mongodb: bool = True):
        self.headless = headless
        self.enable_anti_detection = enable_anti_detection
        self.use_mongodb = use_mongodb
        self.extractor = None
        
        # Initialize MongoDB manager if needed
        if self.use_mongodb:
            try:
                self.mongodb_manager = get_mongodb_manager()
                print("✅ MongoDB connection initialized")
            except Exception as e:
                print(f"⚠️ Failed to initialize MongoDB: {e}")
                self.use_mongodb = False
    
    def _is_signup_data(self, structured_data: Dict[str, Any]) -> bool:
        """Detect if scraped data is from a sign-up page"""
        if not structured_data:
            return False
        
        # Check for common sign-up indicators
        signup_indicators = [
            "sign up", "signup", "join linkedin", "create account",
            "register", "get started", "welcome to linkedin",
            "member login", "log in", "continue with", "create profile"
        ]
        # Normalize fields
        def normalize(value: Any) -> str:
            """Convert value to lowercase string, handle lists gracefully"""
            if isinstance(value, list):
                return " ".join([str(v).lower().strip() for v in value])
            elif isinstance(value, str):
                return value.lower().strip()
            return ""
            
        full_name = normalize(structured_data.get('full_name', ''))
        job_title = normalize(structured_data.get('job_title', ''))
        title = normalize(structured_data.get('title', ''))
        about = normalize(structured_data.get('about', ''))
        
        # Check if any field contains signup indicators
        fields_to_check = [full_name, job_title, title, about]
        
        for field in fields_to_check:
            if field:
                for indicator in signup_indicators:
                    if indicator in field:
                        return True
        
        # Additional checks for specific patterns
        if full_name == "sign up" or job_title == "linkedin":
            return True
        
        # Check if about contains LinkedIn's default signup description
        if "million+ members" in about and "manage your professional identity" in about:
            return True
        
        return False
    
    async def _retry_with_enhanced_anti_detection(self, url: str) -> Optional[Dict[str, Any]]:
        """Retry scraping with enhanced anti-detection measures"""
        print(f"🔄 Retrying with enhanced anti-detection: {url}")
        
        try:
            # Create new extractor with enhanced settings for retry
            enhanced_extractor = LinkedInDataExtractor(
                headless=self.headless, 
                enable_anti_detection=True,
                # Enhanced anti-detection settings
                is_mobile=True,  # Try mobile user agent
            )
            
            await enhanced_extractor.start()
            
            # Add random delay before retry
            await asyncio.sleep(random.uniform(2.0, 4.0))
            
            # Detect URL type and prepare Google referer only for profiles
            url_type = enhanced_extractor.browser_manager.detect_url_type(url)
            google_referer: Optional[str] = None
            if url_type == 'profile':
                # Simulate coming from Google search results for this profile
                username_match = re.search(r'linkedin\.com/in/([^/?]+)', url)
                search_query = username_match.group(1) if username_match else ''
                if search_query:
                    google_referer = f"https://www.google.com/search?q=site%3Alinkedin.com%2Fin%2F+{search_query}"
                else:
                    google_referer = 'https://www.google.com/'
                print("🔎 Using Google referer for profile retry")
            
            # Extract data with enhanced settings and optional referer
            raw_data = await enhanced_extractor.extract_linkedin_data(url, referer=google_referer)
            
            if raw_data.get('error'):
                print(f"❌ Enhanced retry failed: {raw_data['error']}")
                return None
            
            # Structure the data
            structured_data = self._structure_linkedin_data(raw_data)
            
            await enhanced_extractor.stop()
            return structured_data
        
        except Exception as e:
            print(f"❌ Enhanced retry error for {url}: {str(e)}")
            return None

    async def scrape_async(self, urls: List[str], output_filename: str = "linkedin_scraped_data.json") -> Dict[str, Any]:
        """Async method to scrape LinkedIn URLs"""
        
        print("=" * 80)
        print("🚀 LINKEDIN SCRAPER - STARTING")
        print("=" * 80)
        print(f"📋 URLs to scrape: {len(urls)}")
        print(f"📁 Output file: {output_filename}")
        print("=" * 80)
        
        # Initialize extractor
        self.extractor = LinkedInDataExtractor(
            headless=self.headless, 
            enable_anti_detection=self.enable_anti_detection
        )
        
        results = {
            "scraping_metadata": {
                "timestamp": time.time(),
                "date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "total_urls": len(urls),
                "successful_scrapes": 0,
                "failed_scrapes": 0,
                "signup_pages_detected": 0,
                "signup_pages_retried": 0,
                "signup_pages_skipped": 0,
                "scraper_version": "linkedin_scraper_main_v1.0"
            },
            "scraped_data": [],
            "signup_urls_flagged": [],  # Track signup URLs for retry
            "signup_urls_skipped": [],   # Track URLs skipped after retry
            "failed_urls": []
        }
        
        try:
            await self.extractor.start()
            print("✅ LinkedIn scraper initialized successfully")
            
            for i, url in enumerate(urls, 1):
                print(f"\n{'='*60}")
                print(f"🔍 SCRAPING {i}/{len(urls)}: {url}")
                print(f"{'='*60}")
                
                # PRE-CHECK: Detect URL type before processing
                url_type = self.extractor.browser_manager.detect_url_type(url)
                print(f"📋 Detected URL type: {url_type}")
                
                # SKIP unknown URLs
                if url_type == 'unknown':
                    print(f"⚠️  SKIPPING unknown URL type: {url}")
                    continue

                try:
                    # Extract raw data
                    raw_data = await self.extractor.extract_linkedin_data(url)
                    
                    if raw_data.get('error'):
                        print(f"❌ Failed to scrape {url}: {raw_data['error']}")
                        results["failed_urls"].append({
                            "url": url,
                            "error": raw_data['error']
                        })
                        results["scraping_metadata"]["failed_scrapes"] += 1
                        continue
                    
                    # Process and structure data
                    structured_data = self._structure_linkedin_data(raw_data)
                    
                    if structured_data:
                        # Check if this is sign-up data
                        if self._is_signup_data(structured_data):
                            print(f"🚫 SIGN-UP PAGE DETECTED: {url}")
                            results["signup_urls_flagged"].append({
                                "url": url,
                                "detected_data": structured_data
                            })
                            results["scraping_metadata"]["signup_pages_detected"] += 1
                        else:
                            results["scraped_data"].append(structured_data)
                            results["scraping_metadata"]["successful_scrapes"] += 1
                            print(f"✅ Successfully scraped: {structured_data.get('full_name', 'Unknown')}")
                    else:
                        print(f"❌ Failed to structure data for {url}")
                        results["failed_urls"].append({
                            "url": url,
                            "error": "Failed to structure data"
                        })
                        results["scraping_metadata"]["failed_scrapes"] += 1
                
                except Exception as e:
                    print(f"❌ Error scraping {url}: {str(e)}")
                    results["failed_urls"].append({
                        "url": url,
                        "error": str(e)
                    })
                    results["scraping_metadata"]["failed_scrapes"] += 1
                
                # Brief pause between requests
                await asyncio.sleep(2)
            
            # Phase 2: Retry sign-up flagged URLs with enhanced anti-detection
            if results["signup_urls_flagged"]:
                print("\n" + "="*60)
                print("🔄 PHASE 2: RETRYING SIGN-UP FLAGGED URLs")
                print("="*60)
                print(f"📊 Found {len(results['signup_urls_flagged'])} sign-up pages to retry")
                
                for i, signup_item in enumerate(results["signup_urls_flagged"], 1):
                    url = signup_item["url"]
                    print(f"\n🔄 RETRY {i}/{len(results['signup_urls_flagged'])}: {url}")
                    
                    try:
                        retry_data = await self._retry_with_enhanced_anti_detection(url)
                        results["scraping_metadata"]["signup_pages_retried"] += 1
                        
                        if retry_data and not self._is_signup_data(retry_data):
                            # Success! Got real data
                            results["scraped_data"].append(retry_data)
                            results["scraping_metadata"]["successful_scrapes"] += 1
                            print(f"✅ RETRY SUCCESS: {retry_data.get('full_name', 'Unknown')}")
                        else:
                            # Still sign-up data or failed, skip it
                            print(f"🚫 RETRY FAILED - Still sign-up data, skipping: {url}")
                            results["signup_urls_skipped"].append({
                                "url": url,
                                "reason": "Still shows sign-up page after retry"
                            })
                            results["scraping_metadata"]["signup_pages_skipped"] += 1
                    
                    except Exception as e:
                        print(f"❌ RETRY ERROR for {url}: {str(e)}")
                        results["signup_urls_skipped"].append({
                            "url": url,
                            "reason": f"Retry error: {str(e)}"
                        })
                        results["scraping_metadata"]["signup_pages_skipped"] += 1
                    
                    # Longer pause between retries
                    await asyncio.sleep(random.uniform(2.0, 5.0))

            # Phase 3: Filter out sign-up data and save results
            print("\n" + "="*60)
            print("💾 PHASE 3: FILTERING AND SAVING RESULTS")
            print("="*60)
            
            # Final filter to ensure no sign-up data gets through
            filtered_data = []
            for item in results["scraped_data"]:
                if not self._is_signup_data(item):
                    filtered_data.append(item)
                else:
                    print(f"🚫 FINAL FILTER: Removing sign-up data for {item.get('url', 'Unknown URL')}")
            
            results["scraped_data"] = filtered_data
            
            # Update final counts
            results["scraping_metadata"]["successful_scrapes"] = len(filtered_data)

            # Save results to file
            self._save_results_to_file(results, output_filename)
            
            # Print summary
            self._print_summary(results)
            
            return results
        
        except Exception as e:
            print(f"❌ Critical error in LinkedIn scraper: {e}")
            raise
        
        finally:
            if self.extractor:
                await self.extractor.stop()
                print("🔧 LinkedIn scraper cleanup completed")
    
    def _structure_linkedin_data(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Structure raw LinkedIn data according to requirements"""
        
        url = raw_data.get('url', '')
        url_type = raw_data.get('url_type', 'unknown')
        
        # Get combined data (primary source)
        combined_data = raw_data.get('extracted_data', {})
        
        # Get fallback data
        json_ld_data = raw_data.get('json_ld_data', {}).get('parsed_data', {})
        meta_data = raw_data.get('meta_data', {})
        
        # Base structure
        structured = {
            "url": url,
            "url_type": url_type,
            "scraping_timestamp": time.time(),
            "scraping_date": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        # print("="*100)
        # print(f"Combined Data: {combined_data}")
        # print("="*100)
        # print(f"JSON-LD Data: {json_ld_data}")
        # print("="*100)
        # print(f"Meta Data: {meta_data}")
        # print("="*100)
        # print(f"URL: {url}")
        # print("="*100)
        # Structure data based on URL type
        if url_type == "profile":
            structured.update(self._structure_profile_data(combined_data, json_ld_data, meta_data, url))
        
        elif url_type == "company":
            structured.update(self._structure_company_data(combined_data, json_ld_data, meta_data, url))
        
        elif url_type == "post":
            structured.update(self._structure_post_data(combined_data, json_ld_data, meta_data, url))
        
        elif url_type == "newsletter":
            structured.update(self._structure_newsletter_data(combined_data, json_ld_data, meta_data, url))
        
        else:
            # Generic structure
            structured.update(self._structure_generic_data(combined_data, json_ld_data, meta_data, url))
        
        return structured if self._has_meaningful_data(structured) else None
    
    def _structure_profile_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure profile data"""
        
        # Extract username from URL
        username_match = re.search(r'linkedin\.com/in/([^/?]+)', url)
        username = username_match.group(1) if username_match else ""
        
        return {
            "username": username,
            "full_name": self._get_reliable_value([
                combined.get('name'),
                json_ld.get('name'),
                meta.get('open_graph', {}).get('og:title', '').split(' | ')[0] if ' | ' in meta.get('open_graph', {}).get('og:title', '') else meta.get('open_graph', {}).get('og:title'),
                meta.get('title', '').split(' | ')[0] if ' | ' in meta.get('title', '') else meta.get('title')
            ]),
            "job_title": self._get_reliable_value([
                combined.get('job_title'),
                json_ld.get('job_title'),
                self._extract_title_from_meta(meta)
            ]),
            "title": self._get_reliable_value([
                meta.get("title").split(" - ", 1)[-1].split(" | ", 1)[0]
            ]),
            "followers": self._get_reliable_value([
                combined.get('followers'),
                json_ld.get('followers'),
                combined.get('author_followers')
            ], convert_to_int=True),
            "connections": self._get_reliable_value([
                combined.get('connections')
            ], convert_to_int=True),
            "about": self._get_reliable_value([
                combined.get('description'),
                json_ld.get('description'),
                meta.get('open_graph', {}).get('og:description'),
                meta.get('description')
            ]),
            "location": self._get_reliable_value([
                combined.get('location'),
                json_ld.get('location'),
                combined.get('country')
            ]),
            "website": self._get_reliable_value([
                combined.get('same_as'),
                json_ld.get('same_as'),
                combined.get('url')
            ]),
            "contact_info": {}  # Not typically available in public data
        }
    
    def _structure_company_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure company data"""
        
        # Extract username from URL
        username_match = re.search(r'linkedin\.com/company/([^/?]+)', url)
        username = username_match.group(1) if username_match else ""
        return {
            "username": username,
            "full_name": self._get_reliable_value([
                json_ld.get('name'),
                combined.get('name'),
                meta.get('title', '').split(' | ')[0] if ' | ' in meta.get('title', '') else meta.get('title')
            ]),
            "address": self._format_company_address(json_ld.get('address', {})),
            "website": self._get_reliable_value([
                combined.get('same_as'),
                json_ld.get('same_as')
            ]),
            "about_us": self._get_reliable_value([
                json_ld.get('description')
            ]),
            "employee_count": self._get_reliable_value([
                json_ld.get('employee_count')
            ], convert_to_int=True)
        }
    
    def _structure_post_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure post data"""
        
        return {
            "url": url,
            "headline": self._get_reliable_value([
                combined.get('headline'),
                json_ld.get('headline'),
                meta.get('open_graph', {}).get('og:title'),
                meta.get('title')
            ]),
            "author_url": self._get_reliable_value([
                combined.get('author', {}).get('url') if isinstance(combined.get('author'), dict) else None,
                json_ld.get('author', {}).get('url') if isinstance(json_ld.get('author'), dict) else None
            ]),
            "author_name": self._get_reliable_value([
                combined.get('author', {}).get('name') if isinstance(combined.get('author'), dict) else None,
                json_ld.get('author', {}).get('name') if isinstance(json_ld.get('author'), dict) else None
            ]),
            "full_name": self._get_reliable_value([
                combined.get('author', {}).get('name') if isinstance(combined.get('author'), dict) else None,
                json_ld.get('author', {}).get('name') if isinstance(json_ld.get('author'), dict) else None
            ]),
            "comment_count": self._get_reliable_value([
                combined.get('comment_count'),
                combined.get('comments_count'),
                json_ld.get('comment_count')
            ], convert_to_int=True),
            "likes_count": self._get_reliable_value([
                combined.get('likes'),
                json_ld.get('likes')
            ], convert_to_int=True),
            "followers": self._get_reliable_value([
                combined.get('author_followers'),
                json_ld.get('author_followers')
            ], convert_to_int=True),
            "date_published": self._get_reliable_value([
                combined.get('date_published'),
                json_ld.get('date_published')
            ])
        }
    
    def _structure_newsletter_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure newsletter data"""
        
        # Extract username from URL
        username_match = re.search(r'linkedin\.com/newsletters/([^/?]+)', url)
        username = username_match.group(1) if username_match else ""
        
        return {
            "username": username,
            "full_name": self._get_reliable_value([
                combined.get('name'),
                json_ld.get('name'),
                meta.get('open_graph', {}).get('og:title', '').split(' | ')[0] if ' | ' in meta.get('open_graph', {}).get('og:title', '') else meta.get('open_graph', {}).get('og:title'),
                meta.get('title', '').split(' | ')[0] if ' | ' in meta.get('title', '') else meta.get('title')
            ]),
            "description": self._get_reliable_value([
                combined.get('description'),
                json_ld.get('description'),
                meta.get('open_graph', {}).get('og:description'),
                meta.get('description')
            ]),
            "author_name": self._get_reliable_value([
                combined.get('name'),
                json_ld.get('name'),
                meta.get('open_graph', {}).get('og:title', '').split(' | ')[0] if ' | ' in meta.get('open_graph', {}).get('og:title', '') else meta.get('open_graph', {}).get('og:title'),
                meta.get('title', '').split(' | ')[0] if ' | ' in meta.get('title', '') else meta.get('title')
            ]),
            "date_published": self._get_reliable_value([
                combined.get('date_published'),
                json_ld.get('date_published')
            ])
        }
    
    def _structure_generic_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure generic data for unknown URL types"""
        
        return {
            "full_name": self._get_reliable_value([
                combined.get('name'),
                json_ld.get('name'),
                meta.get('open_graph', {}).get('og:title'),
                meta.get('title')
            ]),
            "description": self._get_reliable_value([
                combined.get('description'),
                json_ld.get('description'),
                meta.get('open_graph', {}).get('og:description'),
                meta.get('description')
            ]),
            "url": url,
            "image_url": self._get_reliable_value([
                combined.get('image_url'),
                json_ld.get('image_url'),
                meta.get('open_graph', {}).get('og:image')
            ])
        }
    
    def _get_reliable_value(self, values: List[Any], convert_to_int: bool = False) -> Any:
        """Get the most reliable non-empty value from a list"""
        
        for value in values:
            if value is not None and value != '' and value != 'N/A':
                if convert_to_int:
                    try:
                        if isinstance(value, str):
                            # Remove commas and convert
                            clean_value = value.replace(',', '').replace(' ', '')
                            return int(clean_value)
                        elif isinstance(value, (int, float)):
                            return int(value)
                    except (ValueError, TypeError):
                        continue
                else:
                    return value
        
        return None if not convert_to_int else 0
    
    def _extract_title_from_meta(self, meta: Dict) -> Optional[str]:
        """Extract job title from meta data"""
        
        og_title = meta.get('open_graph', {}).get('og:title', '')
        if ' | ' in og_title:
            parts = og_title.split(' | ')
            if len(parts) > 1:
                return parts[1]  # Usually job title comes after name
        
        return None
    
    def _format_company_address(self, address_dict: Dict) -> str:
        """Format company address from dictionary"""
        
        if not address_dict:
            return ""
        
        address_parts = []
        
        if address_dict.get('street'):
            address_parts.append(address_dict['street'])
        if address_dict.get('city'):
            address_parts.append(address_dict['city'])
        if address_dict.get('region'):
            address_parts.append(address_dict['region'])
        if address_dict.get('postal_code'):
            address_parts.append(address_dict['postal_code'])
        if address_dict.get('country'):
            address_parts.append(address_dict['country'])
        
        return ', '.join(address_parts) if address_parts else ""
    
    def _has_meaningful_data(self, structured: Dict[str, Any]) -> bool:
        """Check if structured data has meaningful content"""
        
        # Must have at least a name or title
        key_fields = ['full_name', 'name', 'headline', 'title']
        
        for field in key_fields:
            if structured.get(field) and structured[field].strip():
                return True
        
        return False
    
    def _save_results_to_file(self, results: Dict[str, Any], filename: str) -> None:
        """Save results to JSON file and MongoDB"""
        
        # Save to MongoDB if enabled
        mongodb_stats = None
        unified_stats = None
        if self.use_mongodb and results.get("scraped_data"):
            try:
                # Save to original LinkedIn collection
                mongodb_stats = self.mongodb_manager.insert_batch_leads(results["scraped_data"], 'linkedin')
                print(f"\n💾 Results saved to MongoDB (linkedin_leads):")
                print(f"   - Successfully inserted: {mongodb_stats['success_count']}")
                print(f"   - Duplicates skipped: {mongodb_stats['duplicate_count']}")
                print(f"   - Failed insertions: {mongodb_stats['failure_count']}")
                
                # Also save to unified collection
                unified_stats = self.mongodb_manager.insert_and_transform_to_unified(results["scraped_data"], 'linkedin')
                print(f"\n💾 Results also saved to unified_leads collection:")
                print(f"   - Successfully transformed & inserted: {unified_stats['success_count']}")
                print(f"   - Duplicates skipped: {unified_stats['duplicate_count']}")
                print(f"   - Failed transformations: {unified_stats['failure_count']}")
                
            except Exception as e:
                print(f"❌ Error saving to MongoDB: {e}")
        elif self.use_mongodb and not clean_scraped_data:
            print("\n⚠️ No clean data to save to MongoDB (all data was sign-up pages)")
        # Save to file as backup
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"\n💾 Results also saved to file: {filename}")
            print(f"   File size: {len(json.dumps(results, indent=2, ensure_ascii=False, default=str)):,} characters")
        
        except Exception as e:
            print(f"❌ Error saving results to {filename}: {e}")
        
        # Add MongoDB stats to results
        if mongodb_stats:
            results['mongodb_stats'] = mongodb_stats
        if unified_stats:
            results['unified_stats'] = unified_stats
    
    def _print_summary(self, results: Dict[str, Any]) -> None:
        """Print scraping summary"""
        
        metadata = results.get("scraping_metadata", {})
        successful = metadata.get("successful_scrapes", 0)
        failed = metadata.get("failed_scrapes", 0)
        signup_detected = metadata.get("signup_pages_detected", 0)
        signup_retried = metadata.get("signup_pages_retried", 0)
        signup_skipped = metadata.get("signup_pages_skipped", 0)
        total = metadata.get("total_urls", 0)
        
        print(f"\n{'='*80}")
        print("🎯 LINKEDIN SCRAPING SUMMARY")
        print(f"{'='*80}")
        print(f"✅ Successful: {successful}/{total} ({successful/total*100 if total > 0 else 0:.1f}%)")
        print(f"❌ Failed: {failed}/{total} ({failed/total*100 if total > 0 else 0:.1f}%)")
        print(f"🚫 Sign-up pages detected: {signup_detected}")
        print(f"🔄 Sign-up pages retried: {signup_retried}")
        print(f"⏭️ Sign-up pages skipped: {signup_skipped}")
        
        if results.get("scraped_data"):
            print(f"\n📊 Successfully scraped:")
            for item in results["scraped_data"]:
                name = item.get('full_name', 'Unknown')
                url_type = item.get('url_type', 'unknown')
                print(f"  ✓ {name} ({url_type})")
        
        if results.get("failed_urls"):
            print(f"\n❌ Failed URLs:")
            for item in results["failed_urls"]:
                print(f"  ✗ {item['url']}: {item['error']}")
        
        if results.get("signup_urls_skipped"):
            print(f"\n🚫 Sign-up URLs skipped after retry:")
            for item in results["signup_urls_skipped"]:
                print(f"  ⏭️ {item['url']}: {item['reason']}")
        print(f"{'='*80}")


# Global instance
_scraper_instance = None


def linkedin_scraper(urls: List[str], output_filename: str = "linkedin_scraper/linkedin_scraped_data.json", headless: bool = True) -> Dict[str, Any]:
    """
    Simple one-line LinkedIn scraper function
    
    Args:
        urls: List of LinkedIn URLs to scrape
        output_filename: Name of output JSON file (default: "linkedin_scraped_data.json")
        headless: Run browser in headless mode (default: True)
    
    Returns:
        Dict containing scraped data and metadata
    
    Usage:
        from main import linkedin_scraper
        
        urls = [
            "https://www.linkedin.com/in/williamhgates/",
            "https://www.linkedin.com/company/microsoft/"
        ]
        
        results = linkedin_scraper(urls)
    """
    
    global _scraper_instance
    
    if not urls:
        print("❌ No URLs provided")
        return {"error": "No URLs provided"}
    
    try:
        # Create scraper instance
        _scraper_instance = LinkedInScraperMain(headless=headless, enable_anti_detection=True)
        
        # Run async scraping
        results = asyncio.run(_scraper_instance.scrape_async(urls, output_filename))
        
        return results
    
    except Exception as e:
        print(f"❌ LinkedIn scraper failed: {e}")
        return {"error": str(e)}


# Alternative class-based approach (if preferred)
class LinkedInScraper:
    """Alternative class-based interface"""
    
    def __init__(self, headless: bool = True):
        self.scraper = LinkedInScraperMain(headless=headless, enable_anti_detection=True)
    
    def scrape(self, urls: List[str], output_filename: str = "linkedin_scraper/linkedin_scraped_data.json") -> Dict[str, Any]:
        """
        Scrape LinkedIn URLs
        
        Usage:
            from main import LinkedInScraper
            
            scraper = LinkedInScraper()
            results = scraper.scrape(urls)
        """
        
        if not urls:
            print("❌ No URLs provided")
            return {"error": "No URLs provided"}
        
        try:
            return asyncio.run(self.scraper.scrape_async(urls, output_filename))
        except Exception as e:
            print(f"❌ LinkedIn scraper failed: {e}")
            return {"error": str(e)}


if __name__ == "__main__":
    # Example usage
    # test_urls = [
    #     "https://www.linkedin.com/in/williamhgates/", #Profile URL Type
    #     "https://www.linkedin.com/company/microsoft/", #Company URL Type
    #     "https://www.linkedin.com/newsletters/aiqod-insider-7325820451622940672", #Newsletter URL type
    #     "https://www.linkedin.com/pulse/10-offbeat-destinations-india-corporate…", #Unknown URL type
    #     "https://careers.linkedin.com/", #Unknown URL type
    #     "https://www.linkedin.com/legal/user-agreement", #Unknown URL type
    #     "https://economicgraph.linkedin.com/workforce-data", #Unknown URL type
        #     "https://www.linkedin.com/posts/mehar-labana_the-empowered-coach-retreat-2024-is-here-activity-7255548691091005440-Zcoi", #Post URL type '@type': 'VideoObject'
        # "https://www.linkedin.com/posts/manojsatishkumar_below-is-my-experience-booking-a-trip-to-activity-7090924640289632256-jgSc", #Post URL type @type': 'DiscussionForumPosting'
        # "https://www.linkedin.com/posts/harishbali_ep-5-nusa-penida-island-bali-everything-activity-7200356196912963584-V8mV" #Post URL type '@type': 'DiscussionForumPosting'
    # ]
    test_urls = [
        "https://www.linkedin.com/pulse/just-finished-travelling-around-world-one-year-now-what-guimar%C3%A3es",
        "https://careers.linkedin.com/",
        "https://www.linkedin.com/legal/user-agreement",
"https://in.linkedin.com/in/rishabhmariwala",
      "https://in.linkedin.com/in/ruchi-aggarwal",
      "https://www.linkedin.com/posts/mohesh-mohan_a-hackers-travel-diaries-episode-1-hotels-activity-7139268391843921920-DW8u",
      "https://in.linkedin.com/in/mohansundaram",
      "https://www.linkedin.com/posts/aazam-ali-mir-26aab2135_i-recently-travelled-to-belgrade-serbia-activity-7350519219886653440-n0IY",
      "https://in.linkedin.com/company/odysseytravels"
    ]
    print("Testing LinkedIn Scraper...")
    print("Method 1: Function approach")
    results = linkedin_scraper(test_urls, "linkedin_scraper/test_results.json", headless=False)
    
    # print("\nMethod 2: Class approach")
    # scraper = LinkedInScraper(headless=False)
    # results2 = scraper.scrape(test_urls, "test_results_class.json")