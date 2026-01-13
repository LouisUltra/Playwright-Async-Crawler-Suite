"""Example script for using IngredientSpider."""

import asyncio
import logging
from pathlib import Path
import sys
import pandas as pd

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core import BrowserManager, AntiDetectionMiddleware
from spiders import IngredientSpider
from config import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_keywords_from_excel(excel_path: str, sheet_name: str = 'Sheet1', column_name: str = '成分名称') -> list:
    """Load keywords from Excel file.
    
    Args:
        excel_path: Path to Excel file
        sheet_name: Sheet name
        column_name: Column name containing keywords
        
    Returns:
        List of keywords
    """
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        keywords = df[column_name].dropna().astype(str).str.strip().tolist()
        keywords = [k for k in keywords if k]  # Remove empty strings
        logger.info(f"Loaded {len(keywords)} keywords from {excel_path}")
        return keywords
    except Exception as e:
        logger.error(f"Error loading keywords: {e}")
        return []


async def main():
    """Main execution function."""
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        
        # Override with example settings
        config['test_mode'] = False  # Set to True for testing
        config['spiders'] = {
            'ingredient': {
                'base_url': 'https://www.nmpa.gov.cn',
                'search_url': 'https://www.nmpa.gov.cn/datasearch/home-index.html',
                'search_type': 'domestic',  # or 'overseas'
                'batch_size': 50
            }
        }
        
        # Initialize components
        logger.info("Initializing browser and middleware...")
        browser = await BrowserManager.get_instance(config)
        middleware = AntiDetectionMiddleware(config)
        
        # Create spider
        logger.info("Creating IngredientSpider...")
        spider = IngredientSpider(browser, middleware, config)
        
        # Load keywords from Excel (or use hardcoded list)
        # keywords = load_keywords_from_excel('keywords.xlsx', column_name='成分名称')
        
        # For this example, use hardcoded keywords
        keywords = [
            '阿司匹林',
            '对乙酰氨基酚',
            '布洛芬',
        ]
        
        logger.info(f"Starting scraping for {len(keywords)} keywords...")
        
        # Process each keyword
        for idx, keyword in enumerate(keywords, 1):
            logger.info(f"Processing keyword {idx}/{len(keywords)}: {keyword}")
            
            try:
                # Search
                results = await spider.search(keyword, search_type='domestic')
                logger.info(f"Found {len(results)} results for '{keyword}'")
                
                # Parse details (limit to first 10 for example)
                detailed_results = []
                for item in results[:10]:
                    detail = await spider.parse_detail(item)
                    detailed_results.append(detail)
                
                # Save incremental batch
                if detailed_results:
                    await spider.save_incremental_batch(
                        detailed_results,
                        keyword=keyword,
                        batch_num=idx
                    )
                
                # Random delay between keywords
                await middleware.random_delay(2.0, 5.0)
                
            except Exception as e:
                logger.error(f"Error processing keyword '{keyword}': {e}")
                continue
        
        # Merge temp files for each keyword
        logger.info("Merging temporary files...")
        for keyword in keywords:
            merged_file = await spider.merge_temp_files(keyword, 'domestic')
            if merged_file:
                logger.info(f"Merged file created: {merged_file}")
        
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
