"""Anti-detection middleware for handling anti-bot mechanisms."""

import asyncio
import random
import logging
import time
from typing import Callable, Any, Optional, Dict, List
from functools import wraps

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

logger = logging.getLogger(__name__)


class AntiDetectionMiddleware:
    """Middleware for handling anti-bot mechanisms.
    
    Provides reusable decorators and functions for:
    - Dynamic cookie handling
    - Random delays
    - Retry with exponential backoff
    - CAPTCHA detection
    - Error recovery
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize middleware with configuration.
        
        Args:
            config: Configuration dictionary containing anti-detection settings
        """
        self.config = config
        anti_detection_config = config.get('anti_detection', {})
        
        # Concurrency control
        max_concurrent = anti_detection_config.get('max_concurrent', 3)
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Delay configuration
        delay_config = anti_detection_config.get('request_delay', {})
        self.min_delay = delay_config.get('min', 1.0)
        self.max_delay = delay_config.get('max', 3.0)
        
        # Retry configuration
        retry_config = anti_detection_config.get('retry', {})
        self.max_retries = retry_config.get('max_attempts', 3)
        self.backoff_factor = retry_config.get('backoff_factor', 2.0)
        
        logger.info(f"Middleware initialized: max_concurrent={max_concurrent}, "
                   f"delay={self.min_delay}-{self.max_delay}s, "
                   f"max_retries={self.max_retries}")

    async def with_retry(
        self,
        func: Callable,
        *args,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
        **kwargs
    ) -> Any:
        """Execute function with retry logic and exponential backoff.
        
        Args:
            func: Async function to execute
            *args: Positional arguments for func
            max_retries: Maximum number of retry attempts (overrides config)
            backoff_factor: Backoff multiplier (overrides config)
            **kwargs: Keyword arguments for func
            
        Returns:
            Result from successful function execution
            
        Raises:
            Exception: Last exception if all retries fail
        """
        max_retries = max_retries or self.max_retries
        backoff_factor = backoff_factor or self.backoff_factor
        
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"Attempt {attempt + 1}/{max_retries} for {func.__name__}")
                result = await func(*args, **kwargs)
                
                if attempt > 0:
                    logger.info(f"Success on attempt {attempt + 1} for {func.__name__}")
                
                return result
                
            except Exception as e:
                last_exception = e
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}: {e}")
                
                if attempt < max_retries - 1:
                    # Calculate backoff delay
                    delay = (backoff_factor ** attempt) + random.uniform(0, 1)
                    logger.info(f"Retrying in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"All {max_retries} attempts failed for {func.__name__}")
        
        raise last_exception

    async def random_delay(
        self,
        min_seconds: Optional[float] = None,
        max_seconds: Optional[float] = None
    ) -> None:
        """Add random delay between requests.
        
        Args:
            min_seconds: Minimum delay in seconds (overrides config)
            max_seconds: Maximum delay in seconds (overrides config)
        """
        min_seconds = min_seconds or self.min_delay
        max_seconds = max_seconds or self.max_delay
        
        delay = random.uniform(min_seconds, max_seconds)
        logger.debug(f"Random delay: {delay:.2f}s")
        await asyncio.sleep(delay)

    async def handle_dynamic_cookies(self, page: Page, timeout: int = 10000) -> bool:
        """Handle dynamic cookie acquisition.
        
        Waits for cookie-setting scripts to execute and validates cookie presence.
        
        Args:
            page: Playwright Page object
            timeout: Maximum wait time in milliseconds
            
        Returns:
            True if cookies were successfully set, False otherwise
        """
        try:
            logger.debug("Waiting for dynamic cookies...")
            
            # Wait for network to be idle (cookies usually set during initial load)
            await page.wait_for_load_state('networkidle', timeout=timeout)
            
            # Get cookies
            cookies = await page.context.cookies()
            
            if cookies:
                logger.debug(f"Found {len(cookies)} cookies")
                return True
            else:
                logger.warning("No cookies found after waiting")
                return False
                
        except PlaywrightTimeoutError:
            logger.warning("Timeout waiting for dynamic cookies")
            return False
        except Exception as e:
            logger.error(f"Error handling dynamic cookies: {e}")
            return False

    async def detect_captcha(self, page: Page) -> bool:
        """Detect if CAPTCHA is present on the page.
        
        Args:
            page: Playwright Page object
            
        Returns:
            True if CAPTCHA detected, False otherwise
        """
        try:
            # Common CAPTCHA selectors
            captcha_selectors = [
                'iframe[src*="recaptcha"]',
                'iframe[src*="hcaptcha"]',
                'div[class*="captcha"]',
                'div[id*="captcha"]',
                '#captcha',
                '.g-recaptcha',
                '.h-captcha',
            ]
            
            for selector in captcha_selectors:
                try:
                    element = await page.query_selector(selector)
                    if element and await element.is_visible():
                        logger.warning(f"CAPTCHA detected: {selector}")
                        return True
                except Exception:
                    continue
            
            # Check for CAPTCHA-related text
            content = await page.content()
            captcha_keywords = ['captcha', 'recaptcha', 'hcaptcha', 'verify you are human']
            
            for keyword in captcha_keywords:
                if keyword.lower() in content.lower():
                    logger.warning(f"CAPTCHA keyword detected: {keyword}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error detecting CAPTCHA: {e}")
            return False

    async def wait_for_stable_dom(
        self,
        page: Page,
        timeout: int = 30000,
        check_interval: int = 500
    ) -> None:
        """Wait for DOM to stabilize after dynamic loading.
        
        Args:
            page: Playwright Page object
            timeout: Maximum wait time in milliseconds
            check_interval: Interval between stability checks in milliseconds
        """
        try:
            logger.debug("Waiting for DOM to stabilize...")
            
            # First wait for network idle
            await page.wait_for_load_state('networkidle', timeout=timeout)
            
            # Then check for DOM stability
            start_time = time.time()
            previous_html_length = 0
            stable_count = 0
            required_stable_checks = 3
            
            while (time.time() - start_time) * 1000 < timeout:
                current_html = await page.content()
                current_length = len(current_html)
                
                if current_length == previous_html_length:
                    stable_count += 1
                    if stable_count >= required_stable_checks:
                        logger.debug("DOM stabilized")
                        return
                else:
                    stable_count = 0
                
                previous_html_length = current_length
                await asyncio.sleep(check_interval / 1000)
            
            logger.warning("DOM did not stabilize within timeout")
            
        except PlaywrightTimeoutError:
            logger.warning("Timeout waiting for stable DOM")
        except Exception as e:
            logger.error(f"Error waiting for stable DOM: {e}")

    def with_concurrency_limit(self, func: Callable) -> Callable:
        """Decorator to limit concurrent executions.
        
        Args:
            func: Async function to wrap
            
        Returns:
            Wrapped function with concurrency control
        """
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async with self.semaphore:
                logger.debug(f"Acquired semaphore for {func.__name__}")
                try:
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    logger.debug(f"Released semaphore for {func.__name__}")
        
        return wrapper

    def with_random_delay(self, func: Callable) -> Callable:
        """Decorator to add random delay before function execution.
        
        Args:
            func: Async function to wrap
            
        Returns:
            Wrapped function with random delay
        """
        @wraps(func)
        async def wrapper(*args, **kwargs):
            await self.random_delay()
            return await func(*args, **kwargs)
        
        return wrapper

    def with_retry_decorator(
        self,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None
    ) -> Callable:
        """Decorator factory for retry logic.
        
        Args:
            max_retries: Maximum number of retry attempts
            backoff_factor: Backoff multiplier
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            async def wrapper(*args, **kwargs):
                return await self.with_retry(
                    func,
                    *args,
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                    **kwargs
                )
            return wrapper
        return decorator

    async def handle_popup(self, page: Page, close_selectors: Optional[List[str]] = None) -> bool:
        """Handle and close popup dialogs.
        
        Args:
            page: Playwright Page object
            close_selectors: List of selectors for close buttons
            
        Returns:
            True if popup was closed, False otherwise
        """
        if close_selectors is None:
            close_selectors = [
                'button:has-text("关闭")',
                'button:has-text("Close")',
                'button.close',
                '[aria-label*="close" i]',
                '.modal-close',
                'div[role="dialog"] button',
            ]
        
        try:
            for selector in close_selectors:
                try:
                    button = page.locator(selector).first
                    if await button.is_visible(timeout=2000):
                        await button.click()
                        logger.info(f"Closed popup using selector: {selector}")
                        await asyncio.sleep(1)
                        return True
                except Exception:
                    continue
            
            return False
            
        except Exception as e:
            logger.error(f"Error handling popup: {e}")
            return False
