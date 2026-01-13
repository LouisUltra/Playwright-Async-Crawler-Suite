"""Spider for scraping pharmaceutical instruction PDFs."""

import asyncio
import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin
from datetime import datetime

from playwright.async_api import Page, Download
from bs4 import BeautifulSoup
import pandas as pd

from core.base_spider import BaseSpider
from core.browser import BrowserManager
from core.middleware import AntiDetectionMiddleware

logger = logging.getLogger(__name__)


class InstructionSpider(BaseSpider):
    """Spider for scraping pharmaceutical instruction PDFs.
    
    This spider searches for drug instruction manuals, downloads PDF files,
    and optionally performs OCR on image-based PDFs.
    
    Features:
    - Pagination support
    - PDF download with structured naming
    - OCR integration for image-based PDFs
    - Test mode support for offline testing
    """

    def __init__(
        self,
        browser_manager: BrowserManager,
        middleware: AntiDetectionMiddleware,
        config: Dict[str, Any]
    ):
        """Initialize instruction spider.
        
        Args:
            browser_manager: Browser context manager
            middleware: Anti-detection middleware
            config: Configuration dictionary
        """
        super().__init__(browser_manager, middleware, config)
        
        # Spider-specific configuration
        spider_config = config.get('spiders', {}).get('instruction', {})
        self.base_url = spider_config.get('base_url', 'https://www.cde.org.cn')
        self.list_page_url = spider_config.get('list_page_url', '')
        self.items_per_page = spider_config.get('items_per_page', 10)
        self.save_dir = Path(config.get('output', {}).get('pdf_dir', 'output/pdfs'))
        
        # Create output directory
        self.save_dir.mkdir(parents=True, exist_ok=True)
        
        # OCR engine (lazy initialization)
        self.ocr_engine = None
        
        logger.info(f"InstructionSpider initialized with base_url={self.base_url}")

    async def search(
        self,
        keyword: str,
        start_page: int = 1,
        end_page: Optional[int] = None,
        **kwargs
    ) -> List[Dict]:
        """Search for drugs and return list with PDF links.
        
        Args:
            keyword: Search keyword (not used in list-based scraping)
            start_page: Starting page number
            end_page: Ending page number (None = all pages)
            **kwargs: Additional parameters
            
        Returns:
            List of dictionaries containing drug information:
            - sequence: Item sequence number
            - approval_number: Drug approval number
            - name: Drug name
            - url: Detail page URL
        """
        if self.test_mode:
            return await self._search_mock(keyword, start_page, end_page)
        
        all_results = []
        
        try:
            # Get page for searching
            page = await self.browser.get_page()
            
            # Navigate to list page
            logger.info(f"Navigating to list page: {self.list_page_url}")
            await page.goto(self.list_page_url, wait_until="networkidle", timeout=60000)
            
            # Get total pages
            total_pages = await self._get_total_pages(page)
            if end_page is None or end_page > total_pages:
                end_page = total_pages
            
            logger.info(f"Total pages: {total_pages}, scraping pages {start_page} to {end_page}")
            
            # Scrape each page
            for page_num in range(start_page, end_page + 1):
                logger.info(f"Processing page {page_num}/{end_page}")
                
                # Navigate to page
                if page_num > start_page:
                    success = await self._navigate_to_page(page, page_num)
                    if not success:
                        logger.error(f"Failed to navigate to page {page_num}, skipping")
                        continue
                
                # Wait for table to update
                await self._wait_for_table_update(page, page_num)
                
                # Extract drug links from current page
                page_results = await self._extract_drug_links(page)
                all_results.extend(page_results)
                
                logger.info(f"Extracted {len(page_results)} items from page {page_num}")
                
                # Random delay between pages
                if page_num < end_page:
                    await self.middleware.random_delay(2.0, 5.0)
            
            await page.close()
            
        except Exception as e:
            logger.error(f"Error during search: {e}", exc_info=True)
        
        return all_results

    async def _get_total_pages(self, page: Page) -> int:
        """Get total number of pages.
        
        Args:
            page: Playwright Page object
            
        Returns:
            Total number of pages
        """
        try:
            # Wait for pagination info
            pagination_info = await page.wait_for_selector(
                ".layui-laypage-count",
                timeout=15000
            )
            
            if pagination_info:
                text = await pagination_info.text_content()
                # Extract total count
                match = re.search(r'共\s+(\d+)\s+条', text)
                if match:
                    total_count = int(match.group(1))
                    
                    # Get items per page from dropdown
                    try:
                        limit_select = await page.query_selector(".layui-laypage-limits select")
                        if limit_select:
                            selected_option = await limit_select.query_selector("option[selected]")
                            if selected_option:
                                self.items_per_page = int(await selected_option.get_attribute("value"))
                            else:
                                self.items_per_page = int(await limit_select.evaluate("select => select.value"))
                    except Exception as e:
                        logger.warning(f"Could not get items per page, using default: {e}")
                    
                    total_pages = (total_count + self.items_per_page - 1) // self.items_per_page
                    logger.info(f"Total: {total_count} items, {self.items_per_page} per page, {total_pages} pages")
                    return total_pages
            
            logger.warning("Could not get total pages, using default")
            return 1173  # Default fallback
            
        except Exception as e:
            logger.error(f"Error getting total pages: {e}")
            return 1173

    async def _navigate_to_page(self, page: Page, page_num: int) -> bool:
        """Navigate to specific page number.
        
        Args:
            page: Playwright Page object
            page_num: Target page number
            
        Returns:
            True if navigation successful, False otherwise
        """
        max_retries = 2
        
        for attempt in range(max_retries):
            try:
                # Try page input jump first
                page_input_selector = ".layui-laypage-skip .layui-input"
                jump_button_selector = ".layui-laypage-skip .layui-laypage-btn"
                
                logger.info(f"Attempting to jump to page {page_num} (attempt {attempt + 1})")
                await page.locator(page_input_selector).fill(str(page_num))
                await page.locator(jump_button_selector).click()
                
                # Wait for page load
                await page.wait_for_load_state("networkidle", timeout=30000)
                
                # Verify page number updated
                try:
                    await page.locator('.layui-laypage-curr em').filter(has_text=str(page_num)).wait_for(timeout=15000)
                    logger.info(f"Successfully navigated to page {page_num}")
                    return True
                except Exception:
                    logger.warning(f"Page number verification failed for page {page_num}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2)
                        continue
                    return False
                    
            except Exception as e:
                logger.error(f"Navigation error (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    return False
        
        return False

    async def _wait_for_table_update(self, page: Page, page_num: int) -> None:
        """Wait for table content to sync with current page.
        
        Args:
            page: Playwright Page object
            page_num: Expected page number
        """
        try:
            # Calculate expected first sequence number
            target_first_seq = str((page_num - 1) * self.items_per_page + 1)
            logger.info(f"Waiting for table to update, target first sequence: {target_first_seq}")
            
            first_row_seq_locator = page.locator('table tbody tr:first-child td[data-field="0"]')
            await first_row_seq_locator.filter(has_text=target_first_seq).wait_for(timeout=20000)
            
            logger.info(f"Table content synced for page {page_num}")
            
        except Exception as e:
            logger.error(f"Error waiting for table update: {e}")
            raise

    async def _extract_drug_links(self, page: Page) -> List[Dict]:
        """Extract drug links from current page.
        
        Args:
            page: Playwright Page object
            
        Returns:
            List of drug information dictionaries
        """
        drug_details = []
        
        try:
            # Wait for table rows
            await page.wait_for_selector("table tbody tr", timeout=15000)
            
            # Get all rows
            rows = await page.query_selector_all("table tbody tr")
            logger.debug(f"Found {len(rows)} rows")
            
            for row in rows:
                try:
                    # Get sequence number (first column)
                    sequence_elem = await row.query_selector('td[data-field="0"]')
                    sequence = await sequence_elem.text_content() if sequence_elem else "UNKNOWN_SEQ"
                    
                    # Get approval number (second column)
                    approval_elem = await row.query_selector('td[data-field="pzwh"]')
                    approval_number = await approval_elem.text_content() if approval_elem else "UNKNOWN_APPROVAL"
                    
                    # Get name and link (third column)
                    name_cell = await row.query_selector('td[data-field="ypmc"] a')
                    if name_cell:
                        drug_name = await name_cell.text_content()
                        drug_url = await name_cell.get_attribute("href")
                        if drug_url:
                            full_url = urljoin(self.base_url, drug_url)
                            drug_details.append({
                                "sequence": sequence.strip(),
                                "approval_number": approval_number.strip(),
                                "name": drug_name.strip(),
                                "url": full_url
                            })
                            
                except Exception as row_e:
                    logger.warning(f"Error processing row: {row_e}")
                    continue
            
            return drug_details
            
        except Exception as e:
            logger.error(f"Error extracting drug links: {e}")
            return []

    async def parse_detail(self, item: Dict) -> Dict:
        """Parse detail page for PDF link and download.
        
        Args:
            item: Dictionary containing drug information
            
        Returns:
            Dictionary with download result
        """
        if self.test_mode:
            return await self._parse_mock_detail(item)
        
        sequence = item.get('sequence', 'UNKNOWN')
        approval_number = item.get('approval_number', 'UNKNOWN')
        drug_name = item.get('name', 'UNKNOWN')
        detail_url = item.get('url', '')
        
        logger.info(f"Processing: seq={sequence}, approval={approval_number}, name={drug_name}")
        
        # Generate filename
        safe_name = re.sub(r'[\\/*?:"<>|]', "", drug_name)
        target_filename = f"{sequence}_{approval_number}_{safe_name}.pdf"
        download_path = self.save_dir / target_filename
        
        # Skip if file exists
        if download_path.exists():
            logger.info(f"File already exists, skipping: {target_filename}")
            return {
                "status": "skipped",
                "sequence": sequence,
                "approval_number": approval_number,
                "name": drug_name,
                "path": str(download_path),
                "message": "File already exists"
            }
        
        # Download PDF
        result = await self._download_pdf_from_detail(
            detail_url,
            download_path,
            sequence,
            approval_number,
            drug_name
        )
        
        result.update({
            "sequence": sequence,
            "approval_number": approval_number,
            "name": drug_name
        })
        
        return result

    async def _download_pdf_from_detail(
        self,
        detail_url: str,
        download_path: Path,
        sequence: str,
        approval_number: str,
        drug_name: str
    ) -> Dict:
        """Download PDF from detail page.
        
        Args:
            detail_url: URL of detail page
            download_path: Path to save PDF
            sequence: Drug sequence number
            approval_number: Drug approval number
            drug_name: Drug name
            
        Returns:
            Dictionary with download result
        """
        detail_page = None
        
        try:
            # Open detail page in new tab
            detail_page = await self.browser.context.new_page()
            await detail_page.goto(detail_url, wait_until="networkidle", timeout=60000)
            logger.info(f"Detail page loaded: {detail_url}")
            
            # Check for "暂无" (no PDF available)
            sms_cell_selector = 'tr:has(td:text-is("说明书")) td:nth-child(2)'
            try:
                sms_content_element = detail_page.locator(sms_cell_selector)
                await sms_content_element.wait_for(state="visible", timeout=5000)
                actual_text = await sms_content_element.text_content()
                
                if "暂无" in actual_text:
                    logger.warning(f"No PDF available for {drug_name}")
                    return {
                        "status": "no_pdf",
                        "message": "Page shows '暂无' (no PDF available)"
                    }
            except Exception:
                logger.debug("Could not check for '暂无', continuing to look for download link")
            
            # Find download link
            try:
                attachment_link_locator = detail_page.locator(
                    'tr:has(td:text-is("说明书")) a:text-is("下载附件")'
                )
                await attachment_link_locator.wait_for(state="visible", timeout=10000)
                logger.info("Found '下载附件' link")
                
                # Download PDF
                async with detail_page.expect_download(timeout=60000) as download_info:
                    await attachment_link_locator.click()
                    logger.info("Clicked download link, waiting for download...")
                
                download = await download_info.value
                await download.save_as(str(download_path))
                logger.info(f"Successfully downloaded: {download_path.name}")
                
                return {
                    "status": "success",
                    "path": str(download_path),
                    "message": "Download successful"
                }
                
            except Exception as download_e:
                logger.warning(f"Could not find or download PDF: {download_e}")
                return {
                    "status": "no_pdf",
                    "message": f"Download link not found or download failed: {download_e}"
                }
                
        except Exception as e:
            logger.error(f"Error processing detail page {detail_url}: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Detail page error: {e}"
            }
            
        finally:
            if detail_page:
                await detail_page.close()

    async def save_results(self, data: List[Dict], output_path: str) -> None:
        """Save results to Excel file.
        
        Args:
            data: List of result dictionaries
            output_path: Path to output Excel file
        """
        try:
            # Separate successful and failed downloads
            successful = [d for d in data if d.get('status') == 'success']
            failed = [d for d in data if d.get('status') != 'success']
            
            logger.info(f"Saving results: {len(successful)} successful, {len(failed)} failed")
            
            # Save failed downloads to Excel with formatting
            if failed:
                await self._save_failed_downloads(failed, output_path)
            
            # Log summary
            logger.info(f"Results saved to {output_path}")
            logger.info(f"Total: {len(data)}, Success: {len(successful)}, Failed: {len(failed)}")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}", exc_info=True)

    async def _save_failed_downloads(self, failures: List[Dict], base_path: str) -> None:
        """Save failed downloads to formatted Excel file.
        
        Args:
            failures: List of failed download dictionaries
            base_path: Base path for output file
        """
        try:
            # Create DataFrame
            df = pd.DataFrame(failures)
            
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_path = Path(base_path).parent / f"failed_downloads_{timestamp}.xlsx"
            
            # Write with formatting
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='Failed Downloads', index=False, header=False, startrow=1)
                
                workbook = writer.book
                worksheet = writer.sheets['Failed Downloads']
                
                # Define formats
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'vcenter',
                    'align': 'center',
                    'fg_color': '#808080',
                    'font_color': 'white',
                    'border': 1
                })
                
                content_format_white = workbook.add_format({
                    'text_wrap': True,
                    'valign': 'top',
                    'border': 1,
                    'fg_color': 'white'
                })
                
                content_format_gray = workbook.add_format({
                    'text_wrap': True,
                    'valign': 'top',
                    'border': 1,
                    'fg_color': '#F0F0F0'
                })
                
                # Write headers
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # Write data with alternating colors
                for row_num in range(len(df)):
                    row_format = content_format_gray if row_num % 2 == 0 else content_format_white
                    for col_num in range(len(df.columns)):
                        value = df.iloc[row_num, col_num]
                        if pd.isna(value) or value is None:
                            worksheet.write(row_num + 1, col_num, "", row_format)
                        else:
                            worksheet.write(row_num + 1, col_num, value, row_format)
                
                # Auto-adjust column widths
                for i, col in enumerate(df.columns):
                    header_width = len(str(col))
                    content_width = df[col].astype(str).map(len).max()
                    if pd.isna(content_width):
                        content_width = 0
                    column_width = max(header_width, int(content_width)) + 3
                    worksheet.set_column(i, i, column_width)
            
            logger.info(f"Failed downloads saved to: {excel_path}")
            
        except Exception as e:
            logger.error(f"Error saving failed downloads: {e}", exc_info=True)

    async def _search_mock(
        self,
        keyword: str,
        start_page: int,
        end_page: Optional[int]
    ) -> List[Dict]:
        """Mock search for test mode.
        
        Args:
            keyword: Search keyword
            start_page: Starting page
            end_page: Ending page
            
        Returns:
            List of mock drug information
        """
        logger.info("Using mock search data (test mode)")
        return [
            {
                "sequence": "1",
                "approval_number": "TEST001",
                "name": "Test Drug 1",
                "url": "http://example.com/drug1"
            },
            {
                "sequence": "2",
                "approval_number": "TEST002",
                "name": "Test Drug 2",
                "url": "http://example.com/drug2"
            }
        ]

    async def _parse_mock_detail(self, item: Dict) -> Dict:
        """Mock detail parsing for test mode.
        
        Args:
            item: Drug information dictionary
            
        Returns:
            Mock parse result
        """
        logger.info(f"Using mock detail parsing (test mode) for {item.get('name')}")
        return {
            "status": "success",
            "sequence": item.get('sequence'),
            "approval_number": item.get('approval_number'),
            "name": item.get('name'),
            "path": f"/mock/path/{item.get('name')}.pdf",
            "message": "Mock download successful"
        }
