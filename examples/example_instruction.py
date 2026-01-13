"""Example script for using InstructionSpider."""

import asyncio
import logging
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core import BrowserManager, AntiDetectionMiddleware
from spiders import InstructionSpider
from config import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Main execution function."""
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        
        # Override with example settings
        config['test_mode'] = False  # Set to True for testing without network requests
        config['spiders'] = {
            'instruction': {
                'base_url': 'https://www.cde.org.cn',
                'list_page_url': 'https://www.cde.org.cn/hymlj/listpage/9cd8db3b7530c6fa0c86485e563f93c7',
                'items_per_page': 10
            }
        }
        
        # Initialize components
        logger.info("Initializing browser and middleware...")
        browser = await BrowserManager.get_instance(config)
        middleware = AntiDetectionMiddleware(config)
        
        # Create spider
        logger.info("Creating InstructionSpider...")
        spider = InstructionSpider(browser, middleware, config)
        
        # Define keywords to search
        keywords = [
            '阿司匹林',
            '布洛芬',
        ]
        
        logger.info(f"Starting scraping for {len(keywords)} keywords...")
        
        # Run spider
        stats = await spider.run(
            keywords=keywords,
            output_dir='output/instruction',
            start_page=1,
            end_page=2  # Limit to 2 pages for example
        )
        
        # Print results
        logger.info("=" * 60)
        logger.info("Scraping Complete!")
        logger.info("=" * 60)
        logger.info(f"Total keywords processed: {stats['total_keywords']}")
        logger.info(f"Total items found: {stats['total_items']}")
        logger.info(f"Successful downloads: {stats['successful']}")
        logger.info(f"Failed downloads: {stats['failed']}")
        
        if stats['errors']:
            logger.warning(f"Encountered {len(stats['errors'])} errors")
            for error in stats['errors'][:5]:  # Show first 5 errors
                logger.warning(f"  - {error}")
        
        # Cleanup
        logger.info("Cleaning up...")
        await browser.close()
        
        logger.info("Done!")
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        # Ensure cleanup
        try:
            await browser.close()
        except:
            pass


if __name__ == '__main__':
    asyncio.run(main())
