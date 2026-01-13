"""Core modules for Playwright-Async-Crawler-Suite."""

from .browser import BrowserManager
from .middleware import AntiDetectionMiddleware
from .base_spider import BaseSpider

__all__ = ["BrowserManager", "AntiDetectionMiddleware", "BaseSpider"]
