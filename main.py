#!/usr/bin/env python3
"""
Lead Generation Application Orchestrator
Coordinates all scrapers based on ICP (Ideal Customer Profile) and user preferences.

Flow:
1. Define ICP (hardcoded for now)
2. User selects which scrapers to use
3. Generate queries using Gemini AI
4. Collect URLs using web_url_scraper
5. Classify URLs and route to appropriate scrapers
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse
import logging

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import scrapers
from web_url_scraper.main import main as web_url_scraper_main, initialize_application
from web_url_scraper.database_service import get_urls_by_type, get_url_type_statistics
from web_scraper.main_app import WebScraperOrchestrator
from instagram_scraper.main import InstagramScraper
from linkedin_scraper.main import LinkedInScraperMain
from yt_scraper.main import YouTubeScraperInterface
from database.mongodb_manager import get_mongodb_manager
from filter_web_lead import MongoDBLeadProcessor
from contact_scraper import scrape_from_url, get_social_media_contacts

# Import Gemini AI (assuming it's available)
try:
    import google.generativeai as genai
    from dotenv import load_dotenv
    load_dotenv()
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    print("⚠️ Gemini AI not available. Install google-generativeai package.")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LeadGenerationOrchestrator:
    """Main orchestrator for the lead generation application"""
    
    def __init__(self):
        """Initialize the orchestrator"""
        self.mongodb_manager = None
        self.available_scrapers = {
            'web_scraper': True,
            'instagram': True,
            'linkedin': True,
            'youtube': True
        }
        
        # Initialize MongoDB
        try:
            self.mongodb_manager = get_mongodb_manager()
            logger.info("✅ MongoDB connection initialized")
        except Exception as e:
            logger.warning(f"⚠️ Failed to initialize MongoDB: {e}")
        
        # Initialize Gemini AI if available
        if GEMINI_AVAILABLE and os.getenv('GEMINI_API_KEY'):
            try:
                genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
                self.gemini_model = genai.GenerativeModel('gemini-2.0-flash')
                logger.info("✅ Gemini AI initialized")
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize Gemini AI: {e}")
                self.gemini_model = None
        else:
            self.gemini_model = None
            logger.warning("⚠️ Gemini AI not available")
    
    def get_hardcoded_icp(self) -> Dict[str, Any]:
        """
        Get hardcoded ICP (Ideal Customer Profile) data
        In future versions, this will come from user forms
        """
        return {
            "product_details": {
                "product_name": "Premium Bus Travel & Group Tour Services",
                "product_category": "Travel & Tourism/Transportation Services",
                "usps": [
                    "Luxury bus fleet with premium amenities",
                    "Custom corporate group travel packages",
                    "Exclusive high-end travel experiences",
                    "Professional tour planning and coordination",
                    "Cost-effective group travel solutions",
                    "24/7 customer support during travel"
                ],
                "pain_points_solved": [
                    "Complicated group travel logistics",
                    "Expensive individual travel arrangements",
                    "Lack of customized corporate travel options",
                    "Poor coordination for large group events",
                    "Safety concerns in group transportation",
                    "Time-consuming travel planning process"
                ]
            },
            "icp_information": {
                "target_industry": [
                    "Corporate Companies",
                    "Educational Institutions",
                    "Wedding Planners",
                    "Event Management",
                    "Religious Organizations",
                    "Sports Teams/Clubs",
                    "Family Reunion Organizers",
                    "Travel Influencers"
                ],
                "competitor_companies": [
                    "RedBus",
                    "MakeMyTrip",
                    "Yatra",
                    "Local tour operators",
                    "Private bus operators",
                    "Luxury Bus Company", 
                    "Premium Tour Operator", 
                    "Corporate Travel Agency"
                ],
                "company_size": "10-1000+ employees/members",
                "decision_maker_persona": [
                    "HR Manager",
                    "Event Coordinator",
                    "Travel Manager",
                    "Family Head/Organizer",
                    "Wedding Planner",
                    "School/College Administrator",
                    "Corporate Executive",
                    "Travel Influencer",
                    "Religious Leader/Organizer"
                ],
                "region": ["India", "Major Cities", "Tourist Destinations"],
                "budget_range": "$5,000-$50,000 annually",
                "travel_occasions": [
                    "Corporate offsites",
                    "Wedding functions",
                    "Family vacations",
                    "Educational tours",
                    "Religious pilgrimages",
                    "Adventure trips",
                    "Destination weddings",
                    "Sports events"
                ]
            }
        }
    
    def get_user_scraper_selection(self) -> List[str]:
        """
        Get user's scraper selection
        For now, returns default selection with web_scraper
        """
        print("\n🎯 SCRAPER SELECTION")
        print("=" * 50)
        print("Available scrapers:")
        print("1. web_scraper (default) - General web scraping")
        print("2. instagram - Instagram profiles and posts")
        print("3. linkedin - LinkedIn profiles and companies")
        print("4. youtube - YouTube channels and videos")
        
        # For automated execution, return default selection without user input
        return ['web_scraper']
    
    async def generate_search_queries(self, icp_data: Dict[str, Any], selected_scrapers: List[str]) -> List[str]:
        """
        Generate search queries using Gemini AI based on ICP data
        Then add platform-specific queries based on selected scrapers
        """
        if not self.gemini_model:
            # Fallback queries if Gemini is not available
            logger.warning("Using fallback queries - Gemini AI not available")
            return self._get_fallback_queries(icp_data)
        
        try:
            # Create prompt for Gemini
            prompt = self._create_gemini_prompt(icp_data)
            
            logger.info("🤖 Generating search queries with Gemini AI...")
            response = await asyncio.to_thread(self.gemini_model.generate_content, prompt)
            
            # Parse the response to extract queries
            base_queries = self._parse_gemini_response(response.text)
        
            # Add platform-specific queries based on selected scrapers
            all_queries = self._add_platform_specific_queries(base_queries, selected_scrapers)
        
            logger.info(f"✅ Generated {len(all_queries)} total search queries ({len(base_queries)} base + {len(all_queries) - len(base_queries)} platform-specific)")
            return all_queries
            
        except Exception as e:
            logger.error(f"❌ Error generating queries with Gemini: {e}")
            return self._get_fallback_queries(icp_data)
    
    def _add_platform_specific_queries(self, base_queries: List[str], selected_scrapers: List[str]) -> List[str]:
        """
        Add platform-specific versions of base queries based on selected scrapers
        """
        all_queries = base_queries.copy()
        
        # Platform keywords mapping
        platform_keywords = {
            'instagram': 'instagram',
            'linkedin': 'linkedin', 
            'youtube': 'youtube'
        }
        
        # Add platform-specific queries
        for scraper in selected_scrapers:
            if scraper in platform_keywords:
                platform_keyword = platform_keywords[scraper]
                logger.info(f"🔍 Adding {platform_keyword} specific queries...")
                
                for query in base_queries:
                    # Add platform keyword to the query
                    platform_query = f"{query} {platform_keyword}"
                    all_queries.append(platform_query)
        
        logger.info(f"📊 Query breakdown:")
        logger.info(f"  - Base queries: {len(base_queries)}")
        for scraper in selected_scrapers:
            if scraper in platform_keywords:
                logger.info(f"  - {scraper} queries: {len(base_queries)}")
        
        return all_queries

    def _create_gemini_prompt(self, icp_data: Dict[str, Any]) -> str:
        """Create a prompt for Gemini AI to generate search queries"""
        product = icp_data["product_details"]
        icp = icp_data["icp_information"]
        
        prompt = f"""
        Based on the following Ideal Customer Profile (ICP) for a premium bus travel agency, generate 15 specific Google search queries that would help find potential customers planning group travel:

        BUSINESS DETAILS:
        - Service: {product["product_name"]}
        - Category: {product["product_category"]}
        - Key Benefits: {', '.join(product["usps"])}
        - Problems Solved: {', '.join(product["pain_points_solved"])}

        TARGET CUSTOMER PROFILE:
        - Target Segments: {', '.join(icp["target_industry"])}
        - Group Size: {icp["company_size"]}
        - Decision Makers: {', '.join(icp["decision_maker_persona"])}
        - Service Areas: {', '.join(icp["region"])}
        - Budget Range: {icp["budget_range"]}
        - Travel Occasions: {', '.join(icp["travel_occasions"])}

        Generate search queries that would find:
        1. Companies/organizations planning group trips or corporate outings
        2. Families planning vacation trips or reunions
        3. Wedding planners organizing destination weddings
        4. Educational institutions planning tours
        5. Travel influencers and content creators
        6. People discussing upcoming travel plans on social media
        7. Corporate HR departments organizing team building events
        8. Religious organizations planning pilgrimages
        9. Sports teams/clubs planning trips
        10. Event management companies handling large groups

        Focus on search terms that indicate:
        - Active travel planning ("planning trip", "looking for", "organizing")
        - Budget considerations ("affordable group travel", "premium travel packages")
        - Specific destinations popular in India
        - Group size indicators ("corporate outing", "family reunion", "wedding party")
        - Timeline indicators ("2024", "2025", "upcoming", "next month")

        Format: Return only the search queries, one per line, without numbering or additional text.
        """
        
        return prompt
    
    def _parse_gemini_response(self, response_text: str) -> List[str]:
        """Parse Gemini response to extract search queries"""
        queries = []
        lines = response_text.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            # Remove numbering, bullets, quotation marks, etc.
            line = line.lstrip('0123456789.-• "\'')
            line = line.rstrip('"\'')
            
            # Basic validation - check for minimum length and travel-related keywords
            travel_keywords = [
                'travel', 'trip', 'tour', 'vacation', 'holiday', 'outing', 'wedding',
                'corporate', 'group', 'family', 'pilgrimage', 'destination', 'bus',
                'transport', 'planning', 'organizing', 'visiting', 'visit', 'travelling',
                'journey', 'excursion', 'adventure','sightseeing', 'backpacking', 'trekking', 'hiking',
                'roadtrip', 'road trip', 'picnic', 'camping', 'booking', 'reservation', 'package', 'deal', 'offer',
                'explore', 'exploring', 'discover', 'discovering', 'wanderlust','company trip', 'staff outing',
                'event', 'gathering', 'yatra','reunion', 'get-together', 'meetup'
            ]
            
            if line and len(line) > 15:  # Increased minimum length
                # Check if the query contains at least one travel-related keyword
                if any(keyword.lower() in line.lower() for keyword in travel_keywords):
                    queries.append(line)
            print('*' * 80)
            print(queries)
            print('*' * 80)
        return queries[:3]  # Limit to 3 queries
    
    def _get_fallback_queries(self, icp_data: Dict[str, Any]) -> List[str]:
        """Fallback search queries when Gemini is not available"""
        logger.info("Using fallback queries - Gemini AI not available")
        industries = icp_data["icp_information"]["target_industry"]
        
        base_queries = [
            "Corporations planning team outings",
            "Families organizing reunions or vacations"
        ]
        
        return base_queries
    
    async def collect_urls_from_queries(self, queries: List[str]) -> Dict[str, List[str]]:
        """
        Use web_url_scraper to collect URLs for each query
        """
        logger.info(f"🔍 Collecting URLs for {len(queries)} queries...")
        
        # Initialize web_url_scraper
        if not initialize_application():
            logger.error("❌ Failed to initialize web_url_scraper")
            return {}
        
        all_urls = []
        
        for i, query in enumerate(queries, 1):
            logger.info(f"[{i}/{len(queries)}] Processing query: {query}")
            
            try:
                # Run web_url_scraper for this query
                success = web_url_scraper_main(query)
                if success:
                    logger.info(f"✅ Successfully processed query: {query}")
                else:
                    logger.warning(f"⚠️ Failed to process query: {query}")
                
                # Add delay between queries to avoid rate limiting
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Error processing query '{query}': {e}")

        try:
            # Get URL type statistics first to see what's available
            stats = get_url_type_statistics()
            logger.info(f"📊 Database contains {stats['total_urls']} URLs across {stats['unique_url_types']} types")
            
            # Initialize classified_urls dictionary
            classified_urls = {
                'instagram': [],
                'linkedin': [],
                'youtube': [],
                'general': []
            }
            
            # Get URLs for each type directly from database
            for url_type in ['instagram', 'linkedin', 'youtube', 'general']:
                try:
                    urls_data = get_urls_by_type(url_type)
                    # Extract just the URLs from the database documents
                    urls = [doc['url'] for doc in urls_data if 'url' in doc]
                    classified_urls[url_type] = urls
                    
                    if urls:
                        logger.info(f"📊 {url_type.title()}: {len(urls)} URLs")
                        
                except Exception as e:
                    logger.error(f"❌ Error getting {url_type} URLs: {e}")
                    classified_urls[url_type] = []
            
            total_urls = sum(len(urls) for urls in classified_urls.values())
            logger.info(f"✅ Collected and classified {total_urls} URLs")
            return classified_urls
            
        except Exception as e:
            logger.error(f"❌ Error retrieving URLs from database: {e}")
            return {
                'instagram': [],
                'linkedin': [],
                'youtube': [],
                'general': []
            }
    
    def _classify_urls(self, urls_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Classify URLs by type (instagram, linkedin, youtube, general)
        """
        classified = {
            'instagram': [],
            'linkedin': [],
            'youtube': [],
            'general': []
        }
        
        for url_data in urls_data:
            url = url_data.get('url', '')
            domain = urlparse(url).netloc.lower()
            
            if 'instagram.com' in domain:
                classified['instagram'].append(url)
            elif 'linkedin.com' in domain:
                classified['linkedin'].append(url)
            elif 'youtube.com' in domain or 'youtu.be' in domain:
                classified['youtube'].append(url)
            else:
                classified['general'].append(url)
        
        # Log classification results
        for url_type, urls in classified.items():
            if urls:
                logger.info(f"📊 {url_type.title()}: {len(urls)} URLs")
        
        return classified
    
    async def run_selected_scrapers(self, classified_urls: Dict[str, List[str]], 
                                  selected_scrapers: List[str]) -> Dict[str, Any]:
        """
        Run the selected scrapers on their respective URL collections
        """
        results = {}
        
        logger.info(f"🚀 Running {len(selected_scrapers)} selected scrapers...")
        
        # Run web_scraper (general URLs)
        if 'web_scraper' in selected_scrapers and classified_urls.get('general'):
            logger.info("🌐 Running web_scraper...")
            try:
                web_scraper = WebScraperOrchestrator(
                    enable_ai=False,
                    enable_quality_engine=False,
                    use_mongodb=True
                )
                
                web_results = web_scraper.run_complete_pipeline(
                    urls=classified_urls['general'][:10],  # Limit to 10 URLs
                    export_format="json",
                    generate_final_leads=True
                )
                
                results['web_scraper'] = web_results
                logger.info(f"✅ Web scraper completed: {web_results.get('summary', {}).get('successful_leads', 0)} leads")
                
            except Exception as e:
                logger.error(f"❌ Web scraper failed: {e}")
                results['web_scraper'] = {'error': str(e)}
        
        # Run Instagram scraper
        if 'instagram' in selected_scrapers and classified_urls.get('instagram'):
            logger.info("📸 Running Instagram scraper...")
            try:
                instagram_scraper = InstagramScraper(
                    headless=True,
                    enable_anti_detection=True,
                    use_mongodb=True
                )
                
                instagram_results = await instagram_scraper.scrape(classified_urls['instagram'][:5]) # Limit to 5 URLs
                results['instagram'] = instagram_results
                logger.info(f"✅ Instagram scraper completed: {len(instagram_results.get('data', []))} profiles")
                
            except Exception as e:
                logger.error(f"❌ Instagram scraper failed: {e}")
                results['instagram'] = {'error': str(e)}
        
        # Run LinkedIn scraper
        if 'linkedin' in selected_scrapers and classified_urls.get('linkedin'):
            logger.info("💼 Running LinkedIn scraper...")
            try:
                linkedin_scraper = LinkedInScraperMain(
                    headless=True,
                    enable_anti_detection=True,
                    use_mongodb=True
                )
                
                linkedin_results = await linkedin_scraper.scrape_async(
                    classified_urls['linkedin'][:5], # Limit to 5 URLs
                    "linkedin_orchestrator_results.json"
                )
                results['linkedin'] = linkedin_results
                logger.info(f"✅ LinkedIn scraper completed: {linkedin_results.get('scraping_metadata', {}).get('successful_scrapes', 0)} profiles")
                
            except Exception as e:
                logger.error(f"❌ LinkedIn scraper failed: {e}")
                results['linkedin'] = {'error': str(e)}
        
        # Run YouTube scraper
        if 'youtube' in selected_scrapers and classified_urls.get('youtube'):
            logger.info("🎥 Running YouTube scraper...")
            try:
                youtube_scraper = YouTubeScraperInterface(
                    headless=True,
                    enable_anti_detection=True,
                    use_mongodb=True
                )
                
                youtube_success = await youtube_scraper.scrape_multiple_urls(
                    classified_urls['youtube'][:5], # Limit to 5 URLs
                    "youtube_orchestrator_results.json"
                )
                results['youtube'] = {'success': youtube_success}
                logger.info(f"✅ YouTube scraper completed: {'Success' if youtube_success else 'Failed'}")
                
            except Exception as e:
                logger.error(f"❌ YouTube scraper failed: {e}")
                results['youtube'] = {'error': str(e)}
        
        return results

    async def enrich_leads_with_contact_scraper(self):
        """
        Enrich leads in MongoDB with missing contact information using contact_scraper.py
        """
        if not self.mongodb_manager:
            logger.warning("MongoDB manager not initialized. Skipping lead enrichment.")
            return

        db = self.mongodb_manager.db
        leadgen_leads_collection = db.leadgen_leads

        # Find leads that are missing email or phone
        # Check for both 'contacts.emails' and 'contacts.phones' being empty or not existing
        query = {
            "$or": [
                {"contacts.emails": {"$exists": False}},
                {"contacts.emails": {"$size": 0}},
                {"contacts.phones": {"$exists": False}},
                {"contacts.phones": {"$size": 0}}
            ]
        }
        
        leads_to_enrich = list(leadgen_leads_collection.find(query))
        logger.info(f"Found {len(leads_to_enrich)} leads to enrich.")

        for lead in leads_to_enrich:
            lead_id = lead['_id']
            updated_contacts = []
            
            # Prioritize social media links if available
            social_media_urls = lead.get('social_media', [])
            company_website = lead.get('website')
            company_name = lead.get('company_name')

            found_contact_info = False

            if social_media_urls:
                for social_url_entry in social_media_urls:
                    social_url = social_url_entry.get('url')
                    if social_url:
                        try:
                            scraped_data = await scrape_from_url(social_url)
                            if scraped_data and (scraped_data.get('emails') or scraped_data.get('phone_numbers')):
                                updated_contacts.append(scraped_data)
                                found_contact_info = True
                        except Exception as e:
                            logger.error(f"Error scraping {social_url}: {e}")
            
            if not found_contact_info and (company_name or company_website):
                try:
                    scraped_data_by_name = await get_social_media_contacts(company_name if company_name else company_website)
                    if scraped_data_by_name:
                        for data in scraped_data_by_name:
                            if data.get('emails') or data.get('phone_numbers'):
                                updated_contacts.append(data)
                                found_contact_info = True
                                break
                except Exception as e:
                    logger.error(f"Error scraping for company {company_name}: {e}")

            if updated_contacts:
                existing_contacts_emails = lead.get('contact', {}).get('emails', [])
                existing_contacts_phones = lead.get('contact', {}).get('phone_numbers', [])

                all_emails = set(e['value'] if isinstance(e, dict) and 'value' in e else e for e in existing_contacts_emails)
                all_phones = set(p['value'] if isinstance(p, dict) and 'value' in p else p for p in existing_contacts_phones)

                for contact_entry in updated_contacts:
                    if contact_entry and isinstance(contact_entry, dict):
                        for email in contact_entry.get('emails', []):
                            if isinstance(email, dict) and 'value' in email:
                                all_emails.add(email['value'])
                            elif isinstance(email, str):
                                all_emails.add(email)
                        for phone in contact_entry.get('phone_numbers', []):
                            if isinstance(phone, dict) and 'value' in phone:
                                all_phones.add(phone['value'])
                            elif isinstance(phone, str):
                                all_phones.add(phone)
                
                new_contact_data = lead.get('contact', {})
                new_contact_data['emails'] = list(all_emails)
                new_contact_data['phone_numbers'] = list(all_phones)

                if new_contact_data['emails'] or new_contact_data['phone_numbers']:
                    leadgen_leads_collection.update_one(
                        {'_id': lead_id},
                        {'$set': {'contact': new_contact_data}}
                    )
                    logger.info(f"Updated lead {lead_id} with new contact information.")
                else:
                    logger.info(f"No new contact information found or merged for lead {lead_id}.")
            else:
                logger.info(f"No additional contact information found for lead {lead_id}.")

    async def export_enriched_leads_to_json(self, filename: str = "enriched_leads.json"):
        """
        Exports all leads from the leadgen_leads collection to a JSON file.
        """
        if not self.mongodb_manager:
            logger.warning("MongoDB manager not initialized. Cannot export leads.")
            return

        try:
            leads = self.mongodb_manager.get_unified_leads(limit=0) # Get all leads
            
            # Convert ObjectId to string for JSON serialization
            for lead in leads:
                if '_id' in lead:
                    lead['_id'] = str(lead['_id'])
                if 'metadata' in lead and 'scraped_at' in lead['metadata']:
                    lead['metadata']['scraped_at'] = str(lead['metadata']['scraped_at'])

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(leads, f, indent=2, ensure_ascii=False)
            logger.info(f"✅ Exported {len(leads)} enriched leads to {filename}")
        except Exception as e:
            logger.error(f"❌ Error exporting enriched leads to JSON: {e}")

    def generate_final_report(self, icp_data: Dict[str, Any], selected_scrapers: List[str],
                            results: Dict[str, Any]) -> str:
        """
        Generate a final report of the orchestration results
        """
        report_data = {
            "orchestration_metadata": {
                "timestamp": datetime.now().isoformat(),
                "icp_data": icp_data,
                "selected_scrapers": selected_scrapers,
                "total_scrapers_run": len([r for r in results.values() if not r.get('error')])
            },
            "results_summary": {},
            "detailed_results": results
        }
        
        # Generate summary for each scraper
        for scraper, result in results.items():
            if scraper == 'lead_filtering':
                # Handle lead filtering results separately
                if result.get('error'):
                    report_data["results_summary"][scraper] = {"status": "failed", "error": result['error']}
                else:
                    filtering_stats = result.get('filtering_stats', {})
                    report_data["results_summary"][scraper] = {
                        "status": "success",
                        "leads_processed": filtering_stats.get('total', 0),
                        "leads_filtered": filtering_stats.get('filtered', 0),
                        "leads_extracted": filtering_stats.get('extracted', 0),
                        "leads_inserted": filtering_stats.get('inserted', 0),
                        "email_based_leads": filtering_stats.get('email_based', 0),
                        "phone_based_leads": filtering_stats.get('phone_based', 0)
                    }
            elif result.get('error'):
                report_data["results_summary"][scraper] = {"status": "failed", "error": result['error']}
            else:
                if scraper == 'web_scraper':
                    summary = result.get('summary', {})
                    report_data["results_summary"][scraper] = {
                        "status": "success",
                        "leads_found": summary.get('successful_leads', 0),
                        "urls_processed": summary.get('urls_processed', 0)
                    }
                elif scraper == 'instagram':
                    report_data["results_summary"][scraper] = {
                        "status": "success",
                        "profiles_found": len(result.get('data', [])),
                        "success_rate": result.get('summary', {}).get('success_rate', 0)
                    }
                elif scraper == 'linkedin':
                    metadata = result.get('scraping_metadata', {})
                    report_data["results_summary"][scraper] = {
                        "status": "success",
                        "profiles_found": metadata.get('successful_scrapes', 0),
                        "failed_scrapes": metadata.get('failed_scrapes', 0)
                    }
                elif scraper == 'youtube':
                    report_data["results_summary"][scraper] = {
                        "status": "success" if result.get('success') else "failed"
                    }
        
        # Save report
        report_filename = f"orchestration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"📊 Final report saved: {report_filename}")
            return report_filename
            
        except Exception as e:
            logger.error(f"❌ Failed to save report: {e}")
            return ""

    async def run_complete_orchestration(self):
        """
        Run the complete lead generation orchestration
        """
        print("\n" + "=" * 80)
        print("🚀 LEAD GENERATION ORCHESTRATOR")
        print("=" * 80)
        
        try:
            # Step 1: Get ICP data (hardcoded for now)
            logger.info("📋 Step 1: Loading ICP data...")
            icp_data = self.get_hardcoded_icp()
            
            print(f"\n📊 ICP SUMMARY:")
            print(f"Product: {icp_data['product_details']['product_name']}")
            print(f"Target Industries: {', '.join(icp_data['icp_information']['target_industry'])}")
            print(f"Company Size: {icp_data['icp_information']['company_size']}")
            
            # Step 2: Get user scraper selection
            logger.info("🎯 Step 2: Getting scraper selection...")
            selected_scrapers = self.get_user_scraper_selection()
            print(f"\n✅ Selected scrapers: {', '.join(selected_scrapers)}")
            
            # Step 3: Generate search queries with Gemini AI
            logger.info("🤖 Step 3: Generating search queries...")
            queries = await self.generate_search_queries(icp_data, selected_scrapers)
            print(f"\n📝 Generated {len(queries)} search queries:")
            print(queries)
            print("\n")
            
            # Step 4: Collect URLs using web_url_scraper
            logger.info("🔍 Step 4: Collecting URLs...")
            classified_urls = await self.collect_urls_from_queries(queries)
            
            total_urls = sum(len(urls) for urls in classified_urls.values())
            print(f"\n📊 URL COLLECTION SUMMARY:")
            print(f"Total URLs collected: {total_urls}")
            
            if total_urls == 0:
                logger.warning("⚠️ No URLs collected. Exiting.")
                return
            
            # Step 5: Run selected scrapers
            logger.info("🚀 Step 5: Running scrapers...")
            results = await self.run_selected_scrapers(classified_urls, selected_scrapers)
            
            # Step 6: Filter and process leads using MongoDBLeadProcessor
            logger.info("🧹 Step 6: Filtering and processing leads...")
            try:
                lead_processor = MongoDBLeadProcessor()
                
                # Create indexes for the target collection
                lead_processor.create_indexes()
                
                # Process all leads from web_leads collection to leadgen_leads collection
                filtering_results = lead_processor.process_leads(batch_size=50)
                
                # Get processing statistics
                processing_stats = lead_processor.get_processing_stats()
                
                print(f"\n📊 LEAD FILTERING SUMMARY:")
                print(f"Total web_leads processed: {filtering_results['total']}")
                print(f"Leads with valid emails or phones: {filtering_results['filtered']}")
                print(f"Individual leads extracted: {filtering_results['extracted']}")
                print(f"Leads inserted to leadgen_leads: {filtering_results['inserted']}")
                print(f"Email-based leads: {filtering_results.get('email_based', 0)}")
                print(f"Phone-based leads: {filtering_results.get('phone_based', 0)}")
                print(f"Unique companies: {processing_stats.get('unique_companies', 'N/A')}")
                print(f"Unique industries: {processing_stats.get('unique_industries', 'N/A')}")
                
                # Add filtering results to the main results
                results['lead_filtering'] = {
                    'filtering_stats': filtering_results,
                    'processing_stats': processing_stats
                }
                
                lead_processor.close_connection()
                
            except Exception as e:
                logger.error(f"❌ Error in lead filtering: {e}")
                results['lead_filtering'] = {'error': str(e)}

            # Step 7: Enrich leads with missing contact information
            logger.info("✨ Step 7: Enriching leads with contact details...")
            await self.enrich_leads_with_contact_scraper()

            # Step 8: Export enriched leads to JSON
            logger.info("📄 Step 8: Exporting enriched leads to JSON...")
            await self.export_enriched_leads_to_json()

            # Step 9: Generate final report
            logger.info("📊 Step 9: Generating final report...")
            report_file = self.generate_final_report(icp_data, selected_scrapers, results)
            
            # Final summary
            print(f"\n" + "=" * 80)
            print("🎉 ORCHESTRATION COMPLETED")
            print("=" * 80)
            print(f"\n📊 URL COLLECTION SUMMARY:")
            print(f"Total URLs collected: {total_urls}")
            for key, urls in classified_urls.items():
                print(f"{key}: {len(urls)}")

            successful_scrapers = len([r for r in results.values() if not r.get('error') and r != results.get('lead_filtering')])
            print(f"✅ Successful scrapers: {successful_scrapers}/{len(selected_scrapers)}")
            
            # Show lead filtering results if available
            if 'lead_filtering' in results and not results['lead_filtering'].get('error'):
                filtering_stats = results['lead_filtering']['filtering_stats']
                print(f"\n🧹 LEAD FILTERING RESULTS:")
                print(f"✅ Leads processed: {filtering_stats['inserted']} leads extracted and stored")
                print(f"📧 Email-based leads: {filtering_stats.get('email_based', 0)}")
                print(f"📞 Phone-based leads: {filtering_stats.get('phone_based', 0)}")
            
            if report_file:
                print(f"📊 Final report: {report_file}")
            
            print("=" * 80)
            
        except KeyboardInterrupt:
            logger.info("⚠️ Orchestration interrupted by user")
        except Exception as e:
            logger.error(f"❌ Critical error in orchestration: {e}")
            raise


async def main():
    """Main entry point"""
    orchestrator = LeadGenerationOrchestrator()
    await orchestrator.run_complete_orchestration()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        sys.exit(1)