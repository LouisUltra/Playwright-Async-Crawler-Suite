"""Abstract base class for all spiders."""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from pathlib import Path

from core.browser import BrowserManager
from core.middleware import AntiDetectionMiddleware

logger = logging.getLogger(__name__)


class BaseSpider(ABC):
    """Abstract base class defining spider interface.
    
    All concrete spiders must inherit from this class and implement
    the abstract methods: search, parse_detail, and save_results.
    
    The base class provides:
    - Dependency injection for browser and middleware
    - Test mode support for offline testing
    - Common run() orchestration method
    """

    def __init__(
        self,
        browser_manager: BrowserManager,
        middleware: AntiDetectionMiddleware,
        config: Dict[str, Any]
    ):
        """Initialize spider with dependencies.
        
        Args:
            browser_manager: Singleton browser context manager
            middleware: Anti-detection middleware instance
            config: Configuration dictionary
        """
        self.browser = browser_manager
        self.middleware = middleware
        self.config = config
        self.test_mode = config.get('test_mode', False)
        
        logger.info(f"Initialized {self.__class__.__name__} (test_mode={self.test_mode})")

    @abstractmethod
    async def search(self, keyword: str, **kwargs) -> List[Dict]:
        """Perform search and return results.
        
        Args:
            keyword: Search keyword
            **kwargs: Additional search parameters
            
        Returns:
            List of dictionaries containing search results
            
        Raises:
            NotImplementedError: Must be implemented by subclass
        """
        raise NotImplementedError("Subclass must implement search()")

    @abstractmethod
    async def parse_detail(self, item: Dict) -> Dict:
        """Parse detail page and extract data.
        
        Args:
            item: Dictionary containing item information (e.g., URL, ID)
            
        Returns:
            Dictionary containing extracted data
            
        Raises:
            NotImplementedError: Must be implemented by subclass
        """
        raise NotImplementedError("Subclass must implement parse_detail()")

    @abstractmethod
    async def save_results(self, data: List[Dict], output_path: str) -> None:
        """Save results to file.
        
        Args:
            data: List of dictionaries containing extracted data
            output_path: Path to output file
            
        Raises:
            NotImplementedError: Must be implemented by subclass
        """
        raise NotImplementedError("Subclass must implement save_results()")

    async def run(
        self,
        keywords: List[str],
        output_dir: str = "output",
        **kwargs
    ) -> Dict[str, Any]:
        """Main execution flow orchestrating search -> parse -> save.
        
        This method provides the common workflow for all spiders:
        1. For each keyword, perform search
        2. For each search result, parse detail page
        3. Save all results to output file
        
        Args:
            keywords: List of keywords to search
            output_dir: Directory to save output files
            **kwargs: Additional parameters passed to search()
            
        Returns:
            Dictionary containing execution statistics:
            - total_keywords: Number of keywords processed
            - total_items: Total items found
            - successful: Number of successfully processed items
            - failed: Number of failed items
            - errors: List of error messages
        """
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        stats = {
            'total_keywords': len(keywords),
            'total_items': 0,
            'successful': 0,
            'failed': 0,
            'errors': []
        }
        
        all_results = []
        
        try:
            for keyword in keywords:
                logger.info(f"Processing keyword: {keyword}")
                
                try:
                    # Step 1: Search
                    search_results = await self.search(keyword, **kwargs)
                    stats['total_items'] += len(search_results)
                    logger.info(f"Found {len(search_results)} items for '{keyword}'")
                    
                    # Step 2: Parse details
                    for item in search_results:
                        try:
                            detail_data = await self.parse_detail(item)
                            detail_data['keyword'] = keyword
                            all_results.append(detail_data)
                            stats['successful'] += 1
                            
                        except Exception as e:
                            logger.error(f"Failed to parse detail for item {item}: {e}")
                            stats['failed'] += 1
                            stats['errors'].append({
                                'keyword': keyword,
                                'item': item,
                                'error': str(e)
                            })
                    
                except Exception as e:
                    logger.error(f"Failed to search for keyword '{keyword}': {e}")
                    stats['errors'].append({
                        'keyword': keyword,
                        'error': str(e)
                    })
            
            # Step 3: Save results
            if all_results:
                output_file = output_path / f"results_{self.__class__.__name__}.xlsx"
                await self.save_results(all_results, str(output_file))
                logger.info(f"Saved {len(all_results)} results to {output_file}")
            else:
                logger.warning("No results to save")
            
        except Exception as e:
            logger.error(f"Fatal error in run(): {e}", exc_info=True)
            stats['errors'].append({
                'error': f"Fatal error: {str(e)}"
            })
        
        # Log summary
        logger.info(f"Execution complete: {stats['successful']} successful, "
                   f"{stats['failed']} failed out of {stats['total_items']} total items")
        
        return stats

    def _get_mock_data_path(self, filename: str) -> Path:
        """Get path to mock data file for test mode.
        
        Args:
            filename: Name of mock data file
            
        Returns:
            Path to mock data file
        """
        return Path(__file__).parent.parent / "tests" / "mock_data" / filename

    async def cleanup(self) -> None:
        """Cleanup resources.
        
        This method can be overridden by subclasses to perform
        additional cleanup (e.g., closing OCR engines, temp files).
        """
        logger.info(f"Cleaning up {self.__class__.__name__}")
