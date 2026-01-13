"""Browser context management with anti-detection capabilities."""

import asyncio
import logging
import random
from pathlib import Path
from typing import Dict, Any, Optional, List

from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright

logger = logging.getLogger(__name__)


class BrowserManager:
    """Singleton browser context manager with anti-detection features.
    
    This class manages a single Playwright browser context that can be reused
    across multiple page requests. It provides anti-detection capabilities including:
    - Stealth script injection
    - User-Agent rotation
    - Resource blocking for performance
    - Configurable viewport and locale settings
    """

    _instance: Optional['BrowserManager'] = None
    _lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls):
        """Prevent direct instantiation."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize browser manager (only once due to singleton)."""
        if not hasattr(self, '_initialized'):
            self.playwright: Optional[Playwright] = None
            self.browser: Optional[Browser] = None
            self.context: Optional[BrowserContext] = None
            self.config: Dict[str, Any] = {}
            self.user_agents: List[str] = []
            self.stealth_script: Optional[str] = None
            self._initialized = False

    @classmethod
    async def get_instance(cls, config: Dict[str, Any]) -> 'BrowserManager':
        """Get or create singleton instance.
        
        Args:
            config: Configuration dictionary containing browser settings
            
        Returns:
            BrowserManager instance
        """
        async with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            
            # Initialize if not already done or if config changed
            if not cls._instance._initialized:
                await cls._instance.initialize(config)
            
            return cls._instance

    async def initialize(self, config: Dict[str, Any]) -> bool:
        """Initialize Playwright and browser context.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            True if initialization successful, False otherwise
        """
        if self._initialized:
            logger.info("Browser manager already initialized")
            return True

        self.config = config
        browser_config = config.get('browser', {})

        try:
            # Load User-Agent pool
            await self._load_user_agents(browser_config.get('user_agents_file'))
            
            # Load stealth script
            await self._load_stealth_script(browser_config.get('stealth_script'))

            # Start Playwright
            logger.info("Starting Playwright...")
            self.playwright = await async_playwright().start()

            # Launch browser
            headless = browser_config.get('headless', False)
            user_data_dir = browser_config.get('user_data_dir')

            launch_options = {
                'headless': headless,
                'args': [
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                ]
            }

            if user_data_dir:
                # Use persistent context with user data
                logger.info(f"Launching persistent context with user data: {user_data_dir}")
                self.context = await self.playwright.chromium.launch_persistent_context(
                    user_data_dir=user_data_dir,
                    **launch_options,
                    viewport=browser_config.get('viewport'),
                    locale=browser_config.get('locale', 'zh-CN'),
                    timezone_id=browser_config.get('timezone', 'Asia/Shanghai'),
                    ignore_https_errors=True,
                )
            else:
                # Regular browser launch
                logger.info("Launching browser...")
                self.browser = await self.playwright.chromium.launch(**launch_options)
                
                # Create context
                self.context = await self.browser.new_context(
                    viewport=browser_config.get('viewport'),
                    locale=browser_config.get('locale', 'zh-CN'),
                    timezone_id=browser_config.get('timezone', 'Asia/Shanghai'),
                    user_agent=self.get_random_user_agent(),
                )

            # Set up resource blocking
            block_resources = browser_config.get('block_resources', [])
            if block_resources:
                await self.context.route(
                    "**/*",
                    lambda route: (
                        route.abort()
                        if route.request.resource_type in block_resources
                        else route.continue_()
                    )
                )
                logger.info(f"Blocking resources: {block_resources}")

            self._initialized = True
            logger.info("Browser manager initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize browser manager: {e}", exc_info=True)
            await self.close()
            return False

    async def _load_user_agents(self, file_path: Optional[str]) -> None:
        """Load User-Agent strings from file.
        
        Args:
            file_path: Path to User-Agent file
        """
        if not file_path:
            # Default User-Agents
            self.user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            ]
            logger.info("Using default User-Agent pool")
            return

        try:
            ua_path = Path(file_path)
            if ua_path.exists():
                with open(ua_path, 'r', encoding='utf-8') as f:
                    self.user_agents = [
                        line.strip() for line in f
                        if line.strip() and not line.startswith('#')
                    ]
                logger.info(f"Loaded {len(self.user_agents)} User-Agents from {file_path}")
            else:
                logger.warning(f"User-Agent file not found: {file_path}, using defaults")
                await self._load_user_agents(None)
        except Exception as e:
            logger.error(f"Error loading User-Agents: {e}")
            await self._load_user_agents(None)

    async def _load_stealth_script(self, file_path: Optional[str]) -> None:
        """Load stealth.min.js script.
        
        Args:
            file_path: Path to stealth script
        """
        if not file_path:
            logger.warning("No stealth script configured")
            return

        try:
            script_path = Path(file_path)
            if script_path.exists():
                with open(script_path, 'r', encoding='utf-8') as f:
                    self.stealth_script = f.read()
                logger.info(f"Loaded stealth script from {file_path}")
            else:
                logger.warning(f"Stealth script not found: {file_path}")
        except Exception as e:
            logger.error(f"Error loading stealth script: {e}")

    async def get_page(self) -> Page:
        """Get a new page from the managed context.
        
        Returns:
            New Page instance
            
        Raises:
            RuntimeError: If browser manager not initialized
        """
        if not self._initialized or not self.context:
            raise RuntimeError("Browser manager not initialized. Call initialize() first.")

        page = await self.context.new_page()
        
        # Inject stealth script if available
        if self.stealth_script:
            await self.inject_stealth(page)

        return page

    async def inject_stealth(self, page: Page) -> None:
        """Inject stealth.min.js into page.
        
        Args:
            page: Page to inject script into
        """
        if not self.stealth_script:
            return

        try:
            await page.add_init_script(self.stealth_script)
            logger.debug("Stealth script injected into page")
        except Exception as e:
            logger.error(f"Failed to inject stealth script: {e}")

    def get_random_user_agent(self) -> str:
        """Get random User-Agent from pool.
        
        Returns:
            Random User-Agent string
        """
        if not self.user_agents:
            return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        
        return random.choice(self.user_agents)

    async def close(self) -> None:
        """Close browser context and cleanup resources."""
        logger.info("Closing browser manager...")
        
        try:
            if self.context:
                await self.context.close()
                self.context = None
            
            if self.browser:
                await self.browser.close()
                self.browser = None
            
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
            
            self._initialized = False
            logger.info("Browser manager closed successfully")
            
        except Exception as e:
            logger.error(f"Error closing browser manager: {e}", exc_info=True)

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance (mainly for testing)."""
        cls._instance = None
