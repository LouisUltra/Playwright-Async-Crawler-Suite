"""Spider for scraping structured drug data by ingredient."""

import asyncio
import logging
import re
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import glob

from playwright.async_api import Page
import pandas as pd

from core.base_spider import BaseSpider
from core.browser import BrowserManager
from core.middleware import AntiDetectionMiddleware

logger = logging.getLogger(__name__)


class IngredientSpider(BaseSpider):
    """Spider for scraping structured drug data by ingredient name.
    
    This spider searches for drugs by ingredient/component name,
    extracts structured table data, validates completeness,
    and supports incremental saving with temp file merging.
    
    Features:
    - Domestic and overseas drug search
    - Field mapping and validation
    - Incremental batch saving
    - Temp file merging
    - Data completeness checking
    """

    def __init__(
        self,
        browser_manager: BrowserManager,
        middleware: AntiDetectionMiddleware,
        config: Dict[str, Any]
    ):
        """Initialize ingredient spider.
        
        Args:
            browser_manager: Browser context manager
            middleware: Anti-detection middleware
            config: Configuration dictionary
        """
        super().__init__(browser_manager, middleware, config)
        
        # Spider-specific configuration
        spider_config = config.get('spiders', {}).get('ingredient', {})
        self.base_url = spider_config.get('base_url', 'https://www.nmpa.gov.cn')
        self.search_url = spider_config.get('search_url', '')
        self.search_type = spider_config.get('search_type', 'domestic')  # domestic or overseas
        self.batch_size = spider_config.get('batch_size', 50)
        
        # Output directories
        output_config = config.get('output', {})
        self.data_dir = Path(output_config.get('data_dir', 'output/data'))
        self.temp_dir = self.data_dir / 'temp'
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Field mappings
        self.domestic_fields = [
            '序号', '药品名称', '批准文号', '生产单位', '产品类别',
            '药品本位码', '药品本位码备注', '批准日期', '药品类型'
        ]
        
        self.overseas_fields = [
            '序号', '药品名称', '批准文号', '生产单位', '产品类别',
            '药品本位码', '药品本位码备注', '批准日期', '药品类型',
            '包装规格', '剂型', '生产地址'
        ]
        
        self.required_fields = ['药品名称', '批准文号', '生产单位']
        
        logger.info(f"IngredientSpider initialized with search_type={self.search_type}")

    async def search(
        self,
        keyword: str,
        search_type: Optional[str] = None,
        **kwargs
    ) -> List[Dict]:
        """Search for drugs by ingredient name.
        
        Args:
            keyword: Ingredient/component name to search
            search_type: 'domestic' or 'overseas' (overrides config)
            **kwargs: Additional parameters
            
        Returns:
            List of dictionaries containing basic drug information
        """
        if self.test_mode:
            return await self._search_mock(keyword)
        
        search_type = search_type or self.search_type
        all_results = []
        
        try:
            page = await self.browser.get_page()
            
            # Navigate to search page
            logger.info(f"Navigating to search page: {self.search_url}")
            await page.goto(self.search_url, wait_until="networkidle", timeout=60000)
            
            # Select drug type (domestic/overseas)
            await self._select_drug_type(page, search_type)
            
            # Input keyword and search
            await self._input_keyword_and_search(page, keyword)
            
            # Wait for results
            await self.middleware.wait_for_stable_dom(page, timeout=30000)
            
            # Extract table rows
            results = await self._extract_table_rows(page, search_type)
            all_results.extend(results)
            
            logger.info(f"Found {len(results)} results for keyword '{keyword}'")
            
            await page.close()
            
        except Exception as e:
            logger.error(f"Error during search for '{keyword}': {e}", exc_info=True)
        
        return all_results

    async def _select_drug_type(self, page: Page, search_type: str) -> None:
        """Select domestic or overseas drug type.
        
        Args:
            page: Playwright Page object
            search_type: 'domestic' or 'overseas'
        """
        try:
            if search_type == 'domestic':
                selector = 'input[value="domestic"]'  # Adjust based on actual page
            else:
                selector = 'input[value="overseas"]'
            
            await page.locator(selector).click()
            logger.info(f"Selected drug type: {search_type}")
            
        except Exception as e:
            logger.warning(f"Could not select drug type: {e}")

    async def _input_keyword_and_search(self, page: Page, keyword: str) -> None:
        """Input keyword and trigger search.
        
        Args:
            page: Playwright Page object
            keyword: Search keyword
        """
        try:
            # Input keyword
            search_input_selector = 'input[name="keyword"]'  # Adjust based on actual page
            await page.locator(search_input_selector).fill(keyword)
            
            # Click search button
            search_button_selector = 'button[type="submit"]'  # Adjust based on actual page
            await page.locator(search_button_selector).click()
            
            logger.info(f"Submitted search for keyword: {keyword}")
            
        except Exception as e:
            logger.error(f"Error inputting keyword and searching: {e}")
            raise

    async def _extract_table_rows(self, page: Page, search_type: str) -> List[Dict]:
        """Extract table rows from search results.
        
        Args:
            page: Playwright Page object
            search_type: 'domestic' or 'overseas'
            
        Returns:
            List of dictionaries with basic drug info
        """
        results = []
        
        try:
            # Wait for table
            await page.wait_for_selector('table tbody tr', timeout=15000)
            
            # Get all rows
            rows = await page.query_selector_all('table tbody tr')
            logger.info(f"Found {len(rows)} table rows")
            
            for idx, row in enumerate(rows):
                try:
                    # Extract basic info from row
                    cells = await row.query_selector_all('td')
                    
                    if len(cells) >= 3:
                        sequence = await cells[0].text_content()
                        drug_name = await cells[1].text_content()
                        approval_number = await cells[2].text_content()
                        
                        # Check if detail button exists
                        detail_button = await row.query_selector('button.detail-btn')
                        has_detail = detail_button is not None
                        
                        results.append({
                            'sequence': sequence.strip() if sequence else '',
                            'drug_name': drug_name.strip() if drug_name else '',
                            'approval_number': approval_number.strip() if approval_number else '',
                            'has_detail': has_detail,
                            'row_index': idx,
                            'search_type': search_type
                        })
                        
                except Exception as row_e:
                    logger.warning(f"Error extracting row {idx}: {row_e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error extracting table rows: {e}")
        
        return results

    async def parse_detail(self, item: Dict) -> Dict:
        """Parse detail page and extract all fields.
        
        Args:
            item: Dictionary containing basic drug information
            
        Returns:
            Dictionary with complete drug data
        """
        if self.test_mode:
            return await self._parse_mock_detail(item)
        
        drug_name = item.get('drug_name', 'UNKNOWN')
        search_type = item.get('search_type', 'domestic')
        
        logger.info(f"Parsing detail for: {drug_name}")
        
        try:
            page = await self.browser.get_page()
            
            # Navigate back to search results if needed
            # Then click detail button for this item
            # This is simplified - actual implementation would need to maintain page state
            
            # Extract all fields
            detail_data = await self._extract_detail_fields(page, search_type)
            
            # Map fields to standard names
            mapped_data = self._map_fields(detail_data, search_type)
            
            # Validate data
            validation_result = self._validate_data(mapped_data)
            mapped_data.update(validation_result)
            
            await page.close()
            
            return mapped_data
            
        except Exception as e:
            logger.error(f"Error parsing detail for {drug_name}: {e}", exc_info=True)
            return {
                'drug_name': drug_name,
                'error': str(e),
                'completeness': '0%',
                'missing_fields': 'All fields'
            }

    async def _extract_detail_fields(self, page: Page, search_type: str) -> Dict:
        """Extract all fields from detail page.
        
        Args:
            page: Playwright Page object
            search_type: 'domestic' or 'overseas'
            
        Returns:
            Dictionary with extracted fields
        """
        fields = {}
        
        try:
            # Wait for detail content
            await page.wait_for_selector('.detail-content', timeout=10000)
            
            # Extract fields from detail table
            rows = await page.query_selector_all('.detail-table tr')
            
            for row in rows:
                try:
                    label_cell = await row.query_selector('td.label')
                    value_cell = await row.query_selector('td.value')
                    
                    if label_cell and value_cell:
                        label = await label_cell.text_content()
                        value = await value_cell.text_content()
                        
                        if label and value:
                            fields[label.strip()] = value.strip()
                            
                except Exception:
                    continue
            
        except Exception as e:
            logger.error(f"Error extracting detail fields: {e}")
        
        return fields

    def _map_fields(self, raw_data: Dict, search_type: str) -> Dict:
        """Map extracted fields to standard field names.
        
        Args:
            raw_data: Raw extracted data
            search_type: 'domestic' or 'overseas'
            
        Returns:
            Dictionary with mapped field names
        """
        # Field mapping dictionary (handles variations)
        field_mapping = {
            '药品名称': ['药品名称', '产品名称', '名称'],
            '批准文号': ['批准文号', '批准号', '文号'],
            '生产单位': ['生产单位', '生产企业', '企业名称'],
            '产品类别': ['产品类别', '类别'],
            '药品本位码': ['药品本位码', '本位码'],
            '批准日期': ['批准日期', '批准时间'],
            '药品类型': ['药品类型', '类型'],
        }
        
        if search_type == 'overseas':
            field_mapping.update({
                '包装规格': ['包装规格', '规格'],
                '剂型': ['剂型', '药品剂型'],
                '生产地址': ['生产地址', '地址'],
            })
        
        mapped_data = {}
        
        for standard_name, variations in field_mapping.items():
            for variation in variations:
                # Try with and without colon
                for key in [variation, variation + ':', variation + '：']:
                    if key in raw_data:
                        mapped_data[standard_name] = raw_data[key]
                        break
                if standard_name in mapped_data:
                    break
            
            # Set empty string if not found
            if standard_name not in mapped_data:
                mapped_data[standard_name] = ''
        
        return mapped_data

    def _validate_data(self, data: Dict) -> Dict:
        """Validate data completeness.
        
        Args:
            data: Data dictionary to validate
            
        Returns:
            Dictionary with validation results
        """
        missing_fields = []
        
        for field in self.required_fields:
            if not data.get(field):
                missing_fields.append(field)
        
        total_fields = len(self.required_fields)
        present_fields = total_fields - len(missing_fields)
        completeness = f"{(present_fields / total_fields * 100):.0f}%"
        
        return {
            'completeness': completeness,
            'missing_fields': ', '.join(missing_fields) if missing_fields else 'None'
        }

    async def save_results(self, data: List[Dict], output_path: str) -> None:
        """Save results to Excel file with formatting.
        
        Args:
            data: List of result dictionaries
            output_path: Path to output Excel file
        """
        try:
            if not data:
                logger.warning("No data to save")
                return
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Determine field order based on search type
            search_type = data[0].get('search_type', 'domestic')
            field_order = self.domestic_fields if search_type == 'domestic' else self.overseas_fields
            
            # Add validation columns
            field_order = field_order + ['completeness', 'missing_fields']
            
            # Reorder columns
            for col in field_order:
                if col not in df.columns:
                    df[col] = ''
            df = df[field_order]
            
            # Save with formatting
            await self._save_formatted_excel(df, output_path)
            
            logger.info(f"Results saved to {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}", exc_info=True)

    async def _save_formatted_excel(self, df: pd.DataFrame, filename: str) -> None:
        """Save DataFrame to formatted Excel file.
        
        Args:
            df: DataFrame to save
            filename: Output filename
        """
        try:
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='Drug Data', index=False, header=False, startrow=1)
                
                workbook = writer.book
                worksheet = writer.sheets['Drug Data']
                
                # Define formats
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'vcenter',
                    'align': 'center',
                    'fg_color': '#D3D3D3',
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
                    'fg_color': '#F2F2F2'
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
                    column_width = min(column_width, 70)  # Max width
                    worksheet.set_column(i, i, column_width)
                
                # Freeze first row
                worksheet.freeze_panes(1, 0)
            
            logger.info(f"Formatted Excel saved: {filename}")
            
        except Exception as e:
            logger.error(f"Error saving formatted Excel: {e}", exc_info=True)

    async def save_incremental_batch(
        self,
        batch_data: List[Dict],
        keyword: str,
        batch_num: int
    ) -> None:
        """Save a batch of results to temporary file.
        
        Args:
            batch_data: Batch of result dictionaries
            keyword: Search keyword
            batch_num: Batch number
        """
        if not batch_data:
            logger.warning(f"No data in batch {batch_num} for keyword '{keyword}'")
            return
        
        try:
            # Generate safe filename
            safe_keyword = re.sub(r'[\\/*?:"<>|\s]+', '_', keyword)
            safe_keyword = safe_keyword[:60]  # Limit length
            
            timestamp = datetime.now().strftime("%Y%m%d")
            search_type = batch_data[0].get('search_type', 'domestic')
            
            temp_filename = self.temp_dir / f"{search_type}_{safe_keyword}_{timestamp}_batch{batch_num}.xlsx"
            
            logger.info(f"Saving incremental batch to: {temp_filename}")
            
            df = pd.DataFrame(batch_data)
            await self._save_formatted_excel(df, str(temp_filename))
            
        except Exception as e:
            logger.error(f"Error saving incremental batch: {e}", exc_info=True)

    async def merge_temp_files(self, keyword: str, search_type: str) -> Optional[str]:
        """Merge temporary files for a keyword into final file.
        
        Args:
            keyword: Search keyword
            search_type: 'domestic' or 'overseas'
            
        Returns:
            Path to merged file, or None if no files found
        """
        try:
            # Generate safe keyword
            safe_keyword = re.sub(r'[\\/*?:"<>|\s]+', '_', keyword)
            safe_keyword = safe_keyword[:60]
            
            timestamp = datetime.now().strftime("%Y%m%d")
            
            # Find temp files
            pattern = str(self.temp_dir / f"{search_type}_{safe_keyword}_{timestamp}_*.xlsx")
            temp_files = glob.glob(pattern)
            
            if not temp_files:
                logger.warning(f"No temp files found for keyword '{keyword}'")
                return None
            
            logger.info(f"Found {len(temp_files)} temp files to merge for '{keyword}'")
            
            # Read and concatenate
            dfs = []
            for temp_file in temp_files:
                try:
                    df = pd.read_excel(temp_file)
                    dfs.append(df)
                except Exception as e:
                    logger.error(f"Error reading temp file {temp_file}: {e}")
            
            if not dfs:
                logger.warning("No data frames to merge")
                return None
            
            # Concatenate
            merged_df = pd.concat(dfs, ignore_index=True)
            
            # Save merged file
            final_filename = self.data_dir / f"{search_type}_{safe_keyword}_{timestamp}_merged.xlsx"
            await self._save_formatted_excel(merged_df, str(final_filename))
            
            # Delete temp files
            for temp_file in temp_files:
                try:
                    os.remove(temp_file)
                    logger.debug(f"Deleted temp file: {temp_file}")
                except Exception as e:
                    logger.warning(f"Could not delete temp file {temp_file}: {e}")
            
            logger.info(f"Merged {len(temp_files)} files into: {final_filename}")
            return str(final_filename)
            
        except Exception as e:
            logger.error(f"Error merging temp files: {e}", exc_info=True)
            return None

    async def _search_mock(self, keyword: str) -> List[Dict]:
        """Mock search for test mode.
        
        Args:
            keyword: Search keyword
            
        Returns:
            List of mock drug information
        """
        logger.info(f"Using mock search data (test mode) for keyword: {keyword}")
        return [
            {
                'sequence': '1',
                'drug_name': f'Test Drug for {keyword}',
                'approval_number': 'TEST001',
                'has_detail': True,
                'row_index': 0,
                'search_type': 'domestic'
            }
        ]

    async def _parse_mock_detail(self, item: Dict) -> Dict:
        """Mock detail parsing for test mode.
        
        Args:
            item: Drug information dictionary
            
        Returns:
            Mock parse result
        """
        logger.info(f"Using mock detail parsing (test mode) for {item.get('drug_name')}")
        return {
            'drug_name': item.get('drug_name'),
            'approval_number': item.get('approval_number'),
            'manufacturer': 'Test Manufacturer',
            'category': 'Test Category',
            'completeness': '100%',
            'missing_fields': 'None'
        }
