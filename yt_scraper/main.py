"""
YouTube Scraper Main Interface
"""

import asyncio
import argparse
import sys
import os
from typing import List, Optional, Dict, Any
from asyncio import Semaphore
from playwright.async_api import Error # For Playwright errors

# Add parent directory to path to import database module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from yt_scraper.yt_data_extractor import AdvancedYouTubeExtractor
from yt_scraper.browser_manager import YouTubeBrowserManager # Import YouTubeBrowserManager
from database.mongodb_manager import get_mongodb_manager
from yt_scraper.yt_analyzer import analyze_youtube_leads # Import the new analysis function

# Configuration constants
CONCURRENCY_LIMIT = 3  # Number of concurrent workers (3-5)
BATCH_SIZE = 5         # Number of URLs to process in each batch (5-10)
RETRY_ATTEMPTS = 3     # Number of retry attempts for failed scrapes
RETRY_BACKOFF_FACTOR = 2 # Exponential backoff factor

class YouTubeScraperInterface:
    """Simple interface for YouTube data extraction"""
    
    def __init__(self, browser_manager: YouTubeBrowserManager, use_mongodb: bool = True):
        """Initialize the scraper interface"""
        self.browser_manager = browser_manager
        self.extractor = AdvancedYouTubeExtractor(browser_manager=self.browser_manager)
        self.use_mongodb = use_mongodb
        self.semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT) # Global rate limiter
        
        # Initialize MongoDB manager if needed
        if self.use_mongodb:
            try:
                self.mongodb_manager = get_mongodb_manager()
                print("✅ MongoDB connection initialized")
            except Exception as e:
                print(f"⚠️ Failed to initialize MongoDB: {e}")
                self.use_mongodb = False
    
    async def _process_url_with_retries(self, url: str) -> Optional[Dict[str, Any]]:
        """Process a single URL with retry logic and exponential backoff."""
        for attempt in range(RETRY_ATTEMPTS):
            try:
                async with self.semaphore:
                    data = await self.extractor.extract_youtube_data(url)
                    if not data.get('error'):
                        return data
                    else:
                        print(f"❌ Attempt {attempt + 1} failed for {url}: {data['error']}")
            except Error as e:
                print(f"❌ Playwright error on attempt {attempt + 1} for {url}: {e}")
            except Exception as e:
                print(f"❌ Unexpected error on attempt {attempt + 1} for {url}: {e}")
            
            if attempt < RETRY_ATTEMPTS - 1:
                wait_time = RETRY_BACKOFF_FACTOR ** attempt
                print(f"Retrying {url} in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
        return None

    async def scrape_single_url(self, url: str, output_file: str = "youtube_data.json") -> bool:
        """
        Scrape a single YouTube URL and save to file
        
        Args:
            url: YouTube URL to scrape
            output_file: Output file name
            
        Returns:
            bool: Success status
        """
        print(f"🎯 Scraping single URL: {url}")
        
        data = await self._process_url_with_retries(url)
        
        if not data:
            print(f"❌ Failed to extract data for {url} after {RETRY_ATTEMPTS} attempts.")
            return False
        
        # Save to MongoDB if enabled
        if self.use_mongodb:
            try:
                # Save to original YouTube collection
                mongodb_stats = self.mongodb_manager.insert_batch_leads([data], 'youtube') # Changed collection name
                print(f"✅ Successfully scraped and saved to MongoDB (youtube_leads):")
                print(f"   - Successfully inserted: {mongodb_stats['success_count']}")
                print(f"   - Duplicates skipped: {mongodb_stats['duplicate_count']}")
                print(f"   - Failed insertions: {mongodb_stats['failure_count']}")
                
                # Also save to unified collection
                unified_stats = self.mongodb_manager.insert_and_transform_to_unified([data], 'youtube')
                print(f"✅ Results also saved to unified_leads collection:")
                print(f"   - Successfully transformed & inserted: {unified_stats['success_count']}")
                print(f"   - Duplicates skipped: {unified_stats['updated_count']}") # Changed from duplicate_count
                print(f"   - Failed transformations: {unified_stats['failure_count']}")
                
            except Exception as e:
                print(f"❌ Error saving to MongoDB: {e}")
        
        # Save clean output to file as backup
        await self.extractor.save_clean_final_output([data], output_file)
        
        print(f"✅ Successfully scraped and saved to {output_file}")
        return True
            
    async def scrape_multiple_urls(self, urls: List[str], output_file: str = "youtube_batch_data.json") -> bool:
        """
        Scrape multiple YouTube URLs concurrently in batches and save to file.
        
        Args:
            urls: List of YouTube URLs to scrape
            output_file: Output file name
            
        Returns:
            bool: Success status
        """
        print(f"🎯 Scraping {len(urls)} URLs in batches of {BATCH_SIZE} with {CONCURRENCY_LIMIT} concurrent workers...")
        
        all_extracted_data = []
        
        # Process URLs in batches
        for i in range(0, len(urls), BATCH_SIZE):
            batch_urls = urls[i:i + BATCH_SIZE]
            print(f"\nProcessing batch {int(i/BATCH_SIZE) + 1}/{(len(urls) + BATCH_SIZE - 1) // BATCH_SIZE} ({len(batch_urls)} URLs)...")
            
            tasks = [self._process_url_with_retries(url) for url in batch_urls]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    print(f"❌ An exception occurred during scraping: {result}")
                elif result:
                    all_extracted_data.append(result)
        
        # Save to file as backup
        final_output = await self.extractor.save_clean_final_output(all_extracted_data, output_file)
        
        # Save to MongoDB if enabled
        if self.use_mongodb and final_output:
            try:
                mongodb_stats = self.mongodb_manager.insert_batch_leads(final_output, 'youtube') # Changed collection name
                print(f"✅ Successfully saved to MongoDB:")
                print(f"   - Successfully inserted: {mongodb_stats['success_count']}")
                print(f"   - Duplicates skipped: {mongodb_stats['duplicate_count']}")
                print(f"   - Failed insertions: {mongodb_stats['failure_count']}")

                # Also save to unified collection
                unified_stats = self.mongodb_manager.insert_and_transform_to_unified(final_output, 'youtube')
                print(f"✅ Results also saved to unified_leads collection:")
                print(f"   - Successfully transformed & inserted: {unified_stats['success_count']}")
                print(f"   - Duplicates skipped: {unified_stats['updated_count']}") # Changed from duplicate_count
                print(f"   - Failed transformations: {unified_stats['failure_count']}")
            except Exception as e:
                print(f"❌ Error saving to MongoDB: {e}")
        
        print(f"✅ Successfully scraped {len(all_extracted_data)} URLs and saved to {output_file}")
        return True
    
    async def scrape_from_file(self, file_path: str, output_file: str = "youtube_file_data.json") -> bool:
        """
        Scrape URLs from a text file
        
        Args:
            file_path: Path to file containing URLs (one per line)
            output_file: Output file name
            
        Returns:
            bool: Success status
        """
        try:
            if not os.path.exists(file_path):
                print(f"❌ File not found: {file_path}")
                return False
            
            with open(file_path, 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f.readlines() if line.strip() and line.strip().startswith('http')]
            
            if not urls:
                print(f"❌ No valid YouTube URLs found in {file_path}")
                return False
            
            print(f"📄 Found {len(urls)} URLs in {file_path}")
            return await self.scrape_multiple_urls(urls, output_file)
            
        except Exception as e:
            print(f"❌ Error reading file: {e}")
            return False
    
    async def interactive_mode(self):
        """Interactive mode for easy URL input"""
        print("🔥 YouTube Scraper - Interactive Mode")
        print("=" * 50)
        
        while True:
            print("\nOptions:")
            print("1. Scrape single URL")
            print("2. Scrape multiple URLs (comma-separated)")
            print("3. Scrape from file")
            print("4. Exit")
            
            choice = input("\nEnter your choice (1-4): ").strip()
            
            if choice == '1':
                url = input("Enter YouTube URL: ").strip()
                if url:
                    output = input("Output file name (press Enter for default): ").strip() or "youtube_data.json"
                    await self.scrape_single_url(url, output)
                
            elif choice == '2':
                urls_input = input("Enter URLs (comma-separated): ").strip()
                if urls_input:
                    urls = [url.strip() for url in urls_input.split(',') if url.strip()]
                    output = input("Output file name (press Enter for default): ").strip() or "youtube_batch_data.json"
                    await self.scrape_multiple_urls(urls, output)
                
            elif choice == '3':
                file_path = input("Enter file path: ").strip()
                if file_path:
                    output = input("Output file name (press Enter for default): ").strip() or "youtube_file_data.json"
                    await self.scrape_from_file(file_path, output)
                
            elif choice == '4':
                print("👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice. Please enter 1-4.")
        
    async def perform_analysis(self):
        """Fetches data from MongoDB and performs analysis."""
        if self.use_mongodb:
            try:
                print("\n--- Performing Data Analysis ---")
                # Fetch all leads from the 'youtube_leads' collection
                all_leads_raw = self.mongodb_manager.get_leads_by_source('youtube', limit=0)
                
                # Extract the 'final_data' from each lead for analysis
                all_leads_for_analysis = [
                    lead.get('final_data', {}) for lead in all_leads_raw if lead.get('final_data')
                ]
                
                analyze_youtube_leads(all_leads_for_analysis)
            except Exception as e:
                print(f"❌ Error during data analysis: {e}")
        else:
            print("\nSkipping data analysis: MongoDB is not enabled.")

# Convenience functions for 1-2 line usage (Updated to use browser_manager)
async def quick_scrape(url: str, output: str = "yt_scraper/youtube_data.json", headless: bool = True, enable_anti_detection: bool = True) -> bool:
    """
    Quick single URL scraping in 1 line
    
    Usage:
        await quick_scrape("https://youtube.com/watch?v=VIDEO_ID")
    """
    browser_manager = YouTubeBrowserManager(headless=headless, enable_anti_detection=enable_anti_detection)
    await browser_manager.start()
    scraper = YouTubeScraperInterface(browser_manager=browser_manager)
    try:
        return await scraper.scrape_single_url(url, output)
    finally:
        await browser_manager.stop()

async def quick_batch_scrape(urls: List[str], output: str = "yt_scraper/youtube_batch_data.json", headless: bool = True, enable_anti_detection: bool = True) -> bool:
    """
    Quick multiple URLs scraping in 1 line
    
    Usage:
        await quick_batch_scrape(["url1", "url2", "url3"])
    """
    browser_manager = YouTubeBrowserManager(headless=headless, enable_anti_detection=enable_anti_detection)
    await browser_manager.start()
    scraper = YouTubeScraperInterface(browser_manager=browser_manager)
    try:
        return await scraper.scrape_multiple_urls(urls, output)
    finally:
        await browser_manager.stop()

async def quick_file_scrape(file_path: str, output: str = "yt_scraper/youtube_file_data.json", headless: bool = True, enable_anti_detection: bool = True) -> bool:
    """
    Quick file-based scraping in 1 line
    
    Usage:
        await quick_file_scrape("urls.txt")
    """
    browser_manager = YouTubeBrowserManager(headless=headless, enable_anti_detection=enable_anti_detection)
    await browser_manager.start()
    scraper = YouTubeScraperInterface(browser_manager=browser_manager)
    try:
        return await scraper.scrape_from_file(file_path, output)
    finally:
        await browser_manager.stop()


def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(
        description="YouTube Data Scraper - Extract data from YouTube videos, shorts, and channels",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --url "https://www.youtube.com/watch?v=VIDEO_ID"
  python main.py --urls "url1,url2,url3" --output my_data.json
  python main.py --file urls.txt --headless
  python main.py --interactive
        """
    )
    
    # Mutually exclusive group for input methods
    input_group = parser.add_mutually_exclusive_group(required=False)
    input_group.add_argument('--url', help='Single YouTube URL to scrape')
    input_group.add_argument('--urls', help='Multiple YouTube URLs (comma-separated)')
    input_group.add_argument('--file', help='File containing YouTube URLs (one per line)')
    input_group.add_argument('--interactive', action='store_true', help='Start interactive mode')
    
    # Optional arguments
    parser.add_argument('--output', '-o', default='youtube_scraped_data.json', 
                       help='Output file name (default: youtube_scraped_data.json)')
    parser.add_argument('--headless', action='store_true', default=True,
                       help='Run in headless mode (default: True)')
    parser.add_argument('--show-browser', action='store_true',
                       help='Show browser window (opposite of headless)')
    parser.add_argument('--no-anti-detection', action='store_true',
                       help='Disable anti-detection features')
    parser.add_argument('--analyze', action='store_true', help='Perform analysis after scraping') # New argument for analysis
    
    args = parser.parse_args()
    
    # Handle show-browser flag
    headless_mode = args.headless and not args.show_browser
    anti_detection = not args.no_anti_detection
    
    async def run_scraper():
        browser_manager = YouTubeBrowserManager(headless=headless_mode, enable_anti_detection=anti_detection)
        await browser_manager.start() # Start browser manager once
        
        scraper = YouTubeScraperInterface(browser_manager=browser_manager)
        
        try:
            if args.interactive:
                await scraper.interactive_mode()
            elif args.url:
                success = await scraper.scrape_single_url(args.url, args.output)
                if success and args.analyze: # Perform analysis if successful and requested
                    await scraper.perform_analysis()
                sys.exit(0 if success else 1)
            elif args.urls:
                urls = [url.strip() for url in args.urls.split(',') if url.strip()]
                success = await scraper.scrape_multiple_urls(urls, args.output)
                if success and args.analyze: # Perform analysis if successful and requested
                    await scraper.perform_analysis()
                sys.exit(0 if success else 1)
            elif args.file:
                success = await scraper.scrape_from_file(args.file, args.output)
                if success and args.analyze: # Perform analysis if successful and requested
                    await scraper.perform_analysis()
                sys.exit(0 if success else 1)
            else:
                # No arguments provided, show help and start interactive mode
                parser.print_help()
                print("\n🔥 Starting interactive mode...")
                await scraper.interactive_mode()
        finally:
            await browser_manager.stop() # Stop browser manager once at the end
    
    # Run the async function
    try:
        asyncio.run(run_scraper())
    except KeyboardInterrupt:
        print("\n👋 Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()