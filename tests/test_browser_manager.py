"""Tests for BrowserManager singleton and functionality."""

import pytest
import asyncio
from hypothesis import given, strategies as st, settings
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.browser import BrowserManager


class TestBrowserManagerSingleton:
    """Property tests for BrowserManager singleton pattern."""

    @pytest.fixture(autouse=True)
    async def reset_singleton(self):
        """Reset singleton before each test."""
        BrowserManager.reset_instance()
        yield
        # Cleanup after test
        instance = BrowserManager._instance
        if instance and instance._initialized:
            await instance.close()
        BrowserManager.reset_instance()

    @pytest.mark.asyncio
    @settings(max_examples=100, deadline=None)
    @given(st.integers(min_value=1, max_value=10))
    async def test_singleton_property_multiple_calls(self, num_calls):
        """Property 1: Browser Context Singleton.
        
        For any sequence of calls to BrowserManager.get_instance(),
        all calls should return the same instance object.
        
        Feature: playwright-async-crawler-suite, Property 1: Browser Context Singleton
        Validates: Requirements 2.1
        """
        config = {
            'browser': {
                'headless': True,
                'user_agents_file': None,
                'stealth_script': None,
            }
        }

        instances = []
        for _ in range(num_calls):
            instance = await BrowserManager.get_instance(config)
            instances.append(instance)

        # All instances should be the same object
        first_instance = instances[0]
        for instance in instances[1:]:
            assert instance is first_instance, \
                "Multiple calls to get_instance() returned different objects"
        
        # Cleanup
        await first_instance.close()

    @pytest.mark.asyncio
    async def test_singleton_across_different_configs(self):
        """Test that singleton persists even with different configs."""
        config1 = {'browser': {'headless': True}}
        config2 = {'browser': {'headless': False}}

        instance1 = await BrowserManager.get_instance(config1)
        instance2 = await BrowserManager.get_instance(config2)

        assert instance1 is instance2, \
            "Singleton should return same instance regardless of config"
        
        await instance1.close()

    @pytest.mark.asyncio
    async def test_singleton_thread_safety(self):
        """Test singleton creation is thread-safe."""
        config = {'browser': {'headless': True}}
        
        async def get_instance():
            return await BrowserManager.get_instance(config)
        
        # Create multiple concurrent requests
        tasks = [get_instance() for _ in range(5)]
        instances = await asyncio.gather(*tasks)
        
        # All should be the same instance
        first = instances[0]
        for instance in instances[1:]:
            assert instance is first, "Concurrent calls created multiple instances"
        
        await first.close()


class TestBrowserManagerInitialization:
    """Tests for browser manager initialization."""

    @pytest.fixture(autouse=True)
    async def reset_singleton(self):
        """Reset singleton before each test."""
        BrowserManager.reset_instance()
        yield
        instance = BrowserManager._instance
        if instance and instance._initialized:
            await instance.close()
        BrowserManager.reset_instance()

    @pytest.mark.asyncio
    async def test_initialization_success(self):
        """Test successful initialization."""
        config = {
            'browser': {
                'headless': True,
                'user_agents_file': None,
                'stealth_script': None,
            }
        }

        manager = await BrowserManager.get_instance(config)
        
        assert manager._initialized is True
        assert manager.context is not None
        assert len(manager.user_agents) > 0
        
        await manager.close()

    @pytest.mark.asyncio
    async def test_initialization_failure_handling(self):
        """Test that initialization failures are handled gracefully."""
        config = {
            'browser': {
                'headless': True,
                'user_data_dir': '/nonexistent/path/that/does/not/exist',
            }
        }

        manager = BrowserManager()
        success = await manager.initialize(config)
        
        # Should fail gracefully
        assert success is False
        assert manager._initialized is False

    @pytest.mark.asyncio
    async def test_get_page_before_initialization(self):
        """Test that get_page raises error if not initialized."""
        manager = BrowserManager()
        
        with pytest.raises(RuntimeError, match="not initialized"):
            await manager.get_page()


class TestBrowserManagerUserAgents:
    """Tests for User-Agent management."""

    @pytest.fixture(autouse=True)
    async def reset_singleton(self):
        """Reset singleton before each test."""
        BrowserManager.reset_instance()
        yield
        instance = BrowserManager._instance
        if instance and instance._initialized:
            await instance.close()
        BrowserManager.reset_instance()

    @pytest.mark.asyncio
    async def test_default_user_agents_loaded(self):
        """Test that default User-Agents are loaded."""
        config = {'browser': {'headless': True}}
        
        manager = await BrowserManager.get_instance(config)
        
        assert len(manager.user_agents) > 0
        assert all(isinstance(ua, str) for ua in manager.user_agents)
        
        await manager.close()

    @pytest.mark.asyncio
    @settings(max_examples=50, deadline=None)
    @given(st.integers(min_value=1, max_value=20))
    async def test_user_agent_rotation_property(self, num_requests):
        """Property 3: User-Agent Rotation.
        
        For any two consecutive page creations, the User-Agent strings
        should be different (assuming pool size > 1).
        
        Feature: playwright-async-crawler-suite, Property 3: User-Agent Rotation
        Validates: Requirements 2.3
        """
        config = {'browser': {'headless': True}}
        manager = await BrowserManager.get_instance(config)
        
        # Ensure we have multiple User-Agents
        if len(manager.user_agents) < 2:
            manager.user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/119.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/119.0",
                "Mozilla/5.0 (X11; Linux x86_64) Chrome/119.0",
            ]
        
        user_agents = [manager.get_random_user_agent() for _ in range(num_requests)]
        
        # Check that we get variety (not all the same)
        unique_uas = set(user_agents)
        assert len(unique_uas) > 1 or num_requests == 1, \
            "User-Agent rotation should provide variety"
        
        await manager.close()

    def test_get_random_user_agent_returns_string(self):
        """Test that get_random_user_agent always returns a string."""
        manager = BrowserManager()
        manager.user_agents = ["UA1", "UA2", "UA3"]
        
        for _ in range(10):
            ua = manager.get_random_user_agent()
            assert isinstance(ua, str)
            assert len(ua) > 0


class TestBrowserManagerCleanup:
    """Tests for browser manager cleanup."""

    @pytest.fixture(autouse=True)
    async def reset_singleton(self):
        """Reset singleton before each test."""
        BrowserManager.reset_instance()
        yield
        instance = BrowserManager._instance
        if instance and instance._initialized:
            await instance.close()
        BrowserManager.reset_instance()

    @pytest.mark.asyncio
    async def test_close_cleans_up_resources(self):
        """Test that close() properly cleans up all resources."""
        config = {'browser': {'headless': True}}
        manager = await BrowserManager.get_instance(config)
        
        assert manager._initialized is True
        assert manager.context is not None
        
        await manager.close()
        
        assert manager._initialized is False
        assert manager.context is None
        assert manager.browser is None
        assert manager.playwright is None

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self):
        """Test that calling close() multiple times is safe."""
        config = {'browser': {'headless': True}}
        manager = await BrowserManager.get_instance(config)
        
        await manager.close()
        await manager.close()  # Should not raise error
        await manager.close()  # Should not raise error
        
        assert manager._initialized is False
