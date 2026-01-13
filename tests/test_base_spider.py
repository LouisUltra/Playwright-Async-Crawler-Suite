"""Tests for BaseSpider abstract class."""

import pytest
from pathlib import Path
from typing import Dict, List

from core.base_spider import BaseSpider
from core.browser import BrowserManager
from core.middleware import AntiDetectionMiddleware


class ConcreteSpider(BaseSpider):
    """Concrete implementation of BaseSpider for testing."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_called = False
        self.parse_detail_called = False
        self.save_results_called = False
        self.search_results = []
        self.detail_results = []
    
    async def search(self, keyword: str, **kwargs) -> List[Dict]:
        """Mock search implementation."""
        self.search_called = True
        return self.search_results
    
    async def parse_detail(self, item: Dict) -> Dict:
        """Mock parse_detail implementation."""
        self.parse_detail_called = True
        return {'item_id': item.get('id'), 'data': 'test_data'}
    
    async def save_results(self, data: List[Dict], output_path: str) -> None:
        """Mock save_results implementation."""
        self.save_results_called = True
        self.detail_results = data


class IncompleteSpider(BaseSpider):
    """Spider that doesn't implement abstract methods."""
    pass


class TestBaseSpider:
    """Test suite for BaseSpider abstract class."""
    
    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return {
            'browser': {
                'headless': True,
                'viewport': {'width': 1920, 'height': 1080}
            },
            'anti_detection': {
                'max_concurrent': 3,
                'request_delay': {'min': 0.1, 'max': 0.2},
                'retry': {'max_attempts': 2, 'backoff_factor': 1.5}
            },
            'test_mode': False
        }
    
    @pytest.fixture
    async def browser_manager(self, config):
        """Create browser manager instance."""
        manager = await BrowserManager.get_instance(config)
        yield manager
        await manager.close()
        BrowserManager.reset_instance()
    
    @pytest.fixture
    def middleware(self, config):
        """Create middleware instance."""
        return AntiDetectionMiddleware(config)
    
    @pytest.fixture
    def concrete_spider(self, browser_manager, middleware, config):
        """Create concrete spider instance for testing."""
        return ConcreteSpider(browser_manager, middleware, config)
    
    def test_abstract_method_enforcement(self, browser_manager, middleware, config):
        """Test that abstract methods must be implemented."""
        # Should not be able to instantiate IncompleteSpider
        with pytest.raises(TypeError) as exc_info:
            IncompleteSpider(browser_manager, middleware, config)
        
        assert "Can't instantiate abstract class" in str(exc_info.value)
    
    def test_initialization(self, concrete_spider, browser_manager, middleware, config):
        """Test spider initialization."""
        assert concrete_spider.browser == browser_manager
        assert concrete_spider.middleware == middleware
        assert concrete_spider.config == config
        assert concrete_spider.test_mode == config.get('test_mode', False)
    
    def test_test_mode_flag(self, browser_manager, middleware):
        """Test that test_mode flag is correctly set."""
        config_with_test_mode = {
            'test_mode': True,
            'anti_detection': {'max_concurrent': 3}
        }
        spider = ConcreteSpider(browser_manager, middleware, config_with_test_mode)
        assert spider.test_mode is True
        
        config_without_test_mode = {
            'anti_detection': {'max_concurrent': 3}
        }
        spider2 = ConcreteSpider(browser_manager, middleware, config_without_test_mode)
        assert spider2.test_mode is False
    
    @pytest.mark.asyncio
    async def test_run_orchestration(self, concrete_spider, tmp_path):
        """Test run() method orchestrates search -> parse -> save flow."""
        # Set up mock data
        concrete_spider.search_results = [
            {'id': 1, 'name': 'Item 1'},
            {'id': 2, 'name': 'Item 2'}
        ]
        
        keywords = ['test_keyword']
        stats = await concrete_spider.run(keywords, output_dir=str(tmp_path))
        
        # Verify all methods were called
        assert concrete_spider.search_called
        assert concrete_spider.parse_detail_called
        assert concrete_spider.save_results_called
        
        # Verify stats
        assert stats['total_keywords'] == 1
        assert stats['total_items'] == 2
        assert stats['successful'] == 2
        assert stats['failed'] == 0
        
        # Verify results were processed
        assert len(concrete_spider.detail_results) == 2
        assert all('keyword' in result for result in concrete_spider.detail_results)
    
    @pytest.mark.asyncio
    async def test_run_with_multiple_keywords(self, concrete_spider, tmp_path):
        """Test run() with multiple keywords."""
        concrete_spider.search_results = [{'id': 1}]
        
        keywords = ['keyword1', 'keyword2', 'keyword3']
        stats = await concrete_spider.run(keywords, output_dir=str(tmp_path))
        
        assert stats['total_keywords'] == 3
        assert stats['total_items'] == 3  # 1 item per keyword
        assert stats['successful'] == 3
    
    @pytest.mark.asyncio
    async def test_run_handles_search_errors(self, concrete_spider, tmp_path):
        """Test run() handles errors during search."""
        # Make search raise an exception
        async def failing_search(keyword: str, **kwargs):
            raise ValueError("Search failed")
        
        concrete_spider.search = failing_search
        
        keywords = ['test_keyword']
        stats = await concrete_spider.run(keywords, output_dir=str(tmp_path))
        
        assert stats['total_keywords'] == 1
        assert stats['total_items'] == 0
        assert len(stats['errors']) > 0
        assert 'Search failed' in str(stats['errors'][0])
    
    @pytest.mark.asyncio
    async def test_run_handles_parse_errors(self, concrete_spider, tmp_path):
        """Test run() handles errors during detail parsing."""
        concrete_spider.search_results = [{'id': 1}, {'id': 2}]
        
        # Make parse_detail raise an exception
        async def failing_parse(item: Dict):
            raise ValueError("Parse failed")
        
        concrete_spider.parse_detail = failing_parse
        
        keywords = ['test_keyword']
        stats = await concrete_spider.run(keywords, output_dir=str(tmp_path))
        
        assert stats['total_items'] == 2
        assert stats['successful'] == 0
        assert stats['failed'] == 2
        assert len(stats['errors']) == 2
    
    @pytest.mark.asyncio
    async def test_run_creates_output_directory(self, concrete_spider, tmp_path):
        """Test run() creates output directory if it doesn't exist."""
        output_dir = tmp_path / "new_output_dir"
        assert not output_dir.exists()
        
        concrete_spider.search_results = []
        await concrete_spider.run(['keyword'], output_dir=str(output_dir))
        
        assert output_dir.exists()
        assert output_dir.is_dir()
    
    @pytest.mark.asyncio
    async def test_run_with_no_results(self, concrete_spider, tmp_path):
        """Test run() handles case with no search results."""
        concrete_spider.search_results = []
        
        keywords = ['test_keyword']
        stats = await concrete_spider.run(keywords, output_dir=str(tmp_path))
        
        assert stats['total_items'] == 0
        assert stats['successful'] == 0
        assert stats['failed'] == 0
        assert not concrete_spider.save_results_called
    
    @pytest.mark.asyncio
    async def test_run_continues_after_partial_failure(self, concrete_spider, tmp_path):
        """Test run() continues processing after some items fail."""
        concrete_spider.search_results = [
            {'id': 1},
            {'id': 2},
            {'id': 3}
        ]
        
        # Make parse_detail fail for item with id=2
        original_parse = concrete_spider.parse_detail
        async def selective_failing_parse(item: Dict):
            if item.get('id') == 2:
                raise ValueError("Parse failed for item 2")
            return await original_parse(item)
        
        concrete_spider.parse_detail = selective_failing_parse
        
        keywords = ['test_keyword']
        stats = await concrete_spider.run(keywords, output_dir=str(tmp_path))
        
        assert stats['total_items'] == 3
        assert stats['successful'] == 2
        assert stats['failed'] == 1
        assert len(concrete_spider.detail_results) == 2
    
    def test_get_mock_data_path(self, concrete_spider):
        """Test _get_mock_data_path() returns correct path."""
        filename = "test_mock.html"
        path = concrete_spider._get_mock_data_path(filename)
        
        assert isinstance(path, Path)
        assert path.name == filename
        assert "tests/mock_data" in str(path)
    
    @pytest.mark.asyncio
    async def test_cleanup(self, concrete_spider):
        """Test cleanup() method can be called."""
        # Should not raise any exceptions
        await concrete_spider.cleanup()
    
    @pytest.mark.asyncio
    async def test_run_adds_keyword_to_results(self, concrete_spider, tmp_path):
        """Test run() adds keyword field to each result."""
        concrete_spider.search_results = [{'id': 1}]
        
        keyword = 'test_keyword'
        await concrete_spider.run([keyword], output_dir=str(tmp_path))
        
        assert len(concrete_spider.detail_results) == 1
        assert concrete_spider.detail_results[0]['keyword'] == keyword
