"""
YouTube Browser Manager - Task 1: Basic Infrastructure with Anti-Detection
Handles browser automation with comprehensive stealth configuration for YouTube
"""

import asyncio
import random
import time
from typing import Optional, Dict, Any, List
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Request
from fake_useragent import UserAgent
from yt_scraper.anti_detection import AntiDetectionManager, create_stealth_browser_context, execute_human_behavior


class YouTubeBrowserManager:
    """Manages browser automation with comprehensive anti-detection features for YouTube"""

    def __init__(self, headless: bool = True, enable_anti_detection: bool = True, is_mobile: bool = False,
                 max_contexts: int = 3, max_operations_per_context: int = 20):
        self.headless = headless
        self.enable_anti_detection = enable_anti_detection
        self.is_mobile = is_mobile
        self.browser: Optional[Browser] = None
        self._context_pool: List[BrowserContext] = []
        self._context_usage_counts: Dict[BrowserContext, int] = {}
        self._context_idx: int = 0
        self._max_contexts = max_contexts
        self._max_operations_per_context = max_operations_per_context
        self.ua = UserAgent()
        self._lock = asyncio.Lock() # For safe access to context pool

        # Initialize anti-detection manager
        if self.enable_anti_detection:
            self.anti_detection = AntiDetectionManager(
                enable_fingerprint_evasion=True,
                enable_behavioral_mimicking=True,
                enable_network_obfuscation=True
            )
        else:
            self.anti_detection = None

    async def start(self) -> None:
        """Initialize browser and populate context pool with comprehensive anti-detection configuration"""
        self.playwright = await async_playwright().start()

        browser_args = [
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor',
            '--disable-extensions',
            '--disable-plugins',
            # Optimizations for speed
            '--disable-images',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-notifications',
            '--disable-gpu' # Disable GPU for headless environments
        ]
        
        # Launch a single browser instance
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=browser_args
        )

        # Populate context pool
        for _ in range(self._max_contexts):
            context = await self._create_new_context()
            self._context_pool.append(context)
            self._context_usage_counts[context] = 0

    async def _create_new_context(self) -> BrowserContext:
        """Create a new browser context with anti-detection and optimization settings."""
        if self.enable_anti_detection and self.anti_detection:
            # Use advanced anti-detection configuration
            _, context = await create_stealth_browser_context(
                self.playwright, self.anti_detection, browser_instance=self.browser, is_mobile=self.is_mobile
            )
        else:
            # Fallback to basic stealth configuration
            context = await self.browser.new_context(
                user_agent=self.ua.random,
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York',
                permissions=['geolocation', 'notifications'],
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9'
                }
            )

            # Add basic stealth scripts for YouTube
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });
                
                // YouTube-specific optimizations
                Object.defineProperty(navigator, 'maxTouchPoints', {
                    get: () => 0,
                });
                
                Object.defineProperty(screen, 'colorDepth', {
                    get: () => 24,
                });
            """)
        
        # Optimize resource loading: disable CSS and images for speed
        await context.route("**/*", self._block_aggressively)

        # Set additional headers for YouTube
        await context.set_extra_http_headers({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        return context

    async def _block_aggressively(self, route):
        """Block unnecessary resources like images and CSS for faster loading."""
        if route.request.resource_type in ["image", "stylesheet", "font"]:
            await route.abort()
        else:
            await route.continue_()

    async def get_context(self) -> BrowserContext:
        """Get a browser context from the pool (round-robin). Recycle if needed."""
        async with self._lock:
            # Get the next context in a round-robin fashion
            context = self._context_pool[self._context_idx]
            self._context_idx = (self._context_idx + 1) % len(self._context_pool)

            # Check if context needs recycling
            self._context_usage_counts[context] += 1
            if self._context_usage_counts[context] >= self._max_operations_per_context:
                print(f"Recycling context {context} after {self._context_usage_counts[context]} operations.")
                await context.close()
                new_context = await self._create_new_context()
                self._context_pool[self._context_pool.index(context)] = new_context
                self._context_usage_counts[new_context] = 0
                return new_context
            
            return context

    async def release_context(self, context: BrowserContext):
        """Release a browser context back to the pool (no-op as contexts are reused)."""
        # For now, this is a no-op as contexts are reused.
        # This method is primarily for API consistency if a more complex pooling
        # mechanism (e.g., with explicit release) is introduced later.
        pass

    async def stop(self) -> None:
        """Clean up all browser contexts and the browser instance."""
        async with self._lock:
            for context in self._context_pool:
                try:
                    await context.close()
                except Exception as e:
                    print(f"Error closing context: {e}")
            self._context_pool.clear()
            self._context_usage_counts.clear()

            if self.browser:
                await self.browser.close()
            if hasattr(self, 'playwright'):
                await self.playwright.stop()

    async def recycle_all_contexts(self) -> None:
        """Closes all existing contexts and creates new ones.
        Useful for preventing memory leaks or clearing states."""
        async with self._lock:
            for context in self._context_pool:
                try:
                    await context.close()
                except Exception as e:
                    print(f"Error closing context during recycling: {e}")
            self._context_pool.clear()
            self._context_usage_counts.clear()

            for _ in range(self._max_contexts):
                context = await self._create_new_context()
                self._context_pool.append(context)
                self._context_usage_counts[context] = 0
            print(f"Recycled and recreated {self._max_contexts} browser contexts.")

if __name__ == "__main__":
    # Removed test_youtube_browser_manager as it relied on self.page directly
    print("Browser manager is now context-based and should be tested via AdvancedYouTubeExtractor.")