"""Tests for AntiDetectionMiddleware."""

import pytest
import asyncio
from hypothesis import given, strategies as st, settings
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.middleware import AntiDetectionMiddleware


class TestMiddlewareConcurrency:
    """Property tests for concurrency control."""

    @pytest.mark.asyncio
    @settings(max_examples=100, deadline=None)
    @given(
        max_concurrent=st.integers(min_value=1, max_value=5),
        num_tasks=st.integers(min_value=1, max_value=20)
    )
    async def test_concurrency_limit_property(self, max_concurrent, num_tasks):
        """Property 4: Concurrency Limit Enforcement.
        
        For any set of concurrent requests, the number of simultaneously
        executing requests should never exceed the configured semaphore limit.
        
        Feature: playwright-async-crawler-suite, Property 4: Concurrency Limit Enforcement
        Validates: Requirements 6.1, 6.2, 6.3
        """
        config = {
            'anti_detection': {
                'max_concurrent': max_concurrent
            }
        }
        
        middleware = AntiDetectionMiddleware(config)
        
        # Track concurrent executions
        current_concurrent = 0
        max_observed = 0
        lock = asyncio.Lock()
        
        async def tracked_task():
            nonlocal current_concurrent, max_observed
            
            async with middleware.semaphore:
                async with lock:
                    current_concurrent += 1
                    max_observed = max(max_observed, current_concurrent)
                
                # Simulate work
                await asyncio.sleep(0.01)
                
                async with lock:
                    current_concurrent -= 1
        
        # Run tasks concurrently
        tasks = [tracked_task() for _ in range(num_tasks)]
        await asyncio.gather(*tasks)
        
        # Verify concurrency limit was never exceeded
        assert max_observed <= max_concurrent, \
            f"Observed {max_observed} concurrent tasks, limit was {max_concurrent}"

    @pytest.mark.asyncio
    async def test_semaphore_releases_on_error(self):
        """Test that semaphore is released even when function raises error."""
        config = {'anti_detection': {'max_concurrent': 1}}
        middleware = AntiDetectionMiddleware(config)
        
        @middleware.with_concurrency_limit
        async def failing_func():
            raise ValueError("Test error")
        
        # First call should fail
        with pytest.raises(ValueError):
            await failing_func()
        
        # Semaphore should be released, so second call should also execute
        with pytest.raises(ValueError):
            await failing_func()


class TestMiddlewareRetry:
    """Tests for retry logic."""

    @pytest.mark.asyncio
    async def test_retry_success_on_first_attempt(self):
        """Test that successful function doesn't retry."""
        config = {'anti_detection': {'retry': {'max_attempts': 3}}}
        middleware = AntiDetectionMiddleware(config)
        
        call_count = 0
        
        async def success_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = await middleware.with_retry(success_func)
        
        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retry_eventual_success(self):
        """Test that function succeeds after retries."""
        config = {'anti_detection': {'retry': {'max_attempts': 3}}}
        middleware = AntiDetectionMiddleware(config)
        
        call_count = 0
        
        async def eventually_succeeds():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Not yet")
            return "success"
        
        result = await middleware.with_retry(eventually_succeeds)
        
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_retry_all_attempts_fail(self):
        """Test that exception is raised after all retries fail."""
        config = {'anti_detection': {'retry': {'max_attempts': 3}}}
        middleware = AntiDetectionMiddleware(config)
        
        call_count = 0
        
        async def always_fails():
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fails")
        
        with pytest.raises(ValueError, match="Always fails"):
            await middleware.with_retry(always_fails)
        
        assert call_count == 3

    @pytest.mark.asyncio
    @settings(max_examples=50, deadline=None)
    @given(st.integers(min_value=1, max_value=5))
    async def test_retry_idempotence_property(self, num_failures):
        """Property 5: Retry Idempotence.
        
        For any failed request that is retried, the final result should be
        equivalent to a successful first attempt (for idempotent operations).
        
        Feature: playwright-async-crawler-suite, Property 5: Retry Idempotence
        Validates: Requirements 3.5
        """
        config = {'anti_detection': {'retry': {'max_attempts': num_failures + 1}}}
        middleware = AntiDetectionMiddleware(config)
        
        call_count = 0
        expected_result = "final_result"
        
        async def idempotent_func():
            nonlocal call_count
            call_count += 1
            if call_count < num_failures + 1:
                raise ValueError("Temporary failure")
            return expected_result
        
        result = await middleware.with_retry(idempotent_func)
        
        # Result should be the same regardless of number of retries
        assert result == expected_result


class TestMiddlewareDelay:
    """Tests for random delay functionality."""

    @pytest.mark.asyncio
    async def test_random_delay_within_range(self):
        """Test that random delay is within configured range."""
        config = {
            'anti_detection': {
                'request_delay': {'min': 0.1, 'max': 0.2}
            }
        }
        middleware = AntiDetectionMiddleware(config)
        
        import time
        start = time.time()
        await middleware.random_delay()
        elapsed = time.time() - start
        
        assert 0.1 <= elapsed <= 0.3  # Allow small margin

    @pytest.mark.asyncio
    async def test_random_delay_custom_range(self):
        """Test that custom delay range overrides config."""
        config = {
            'anti_detection': {
                'request_delay': {'min': 1.0, 'max': 2.0}
            }
        }
        middleware = AntiDetectionMiddleware(config)
        
        import time
        start = time.time()
        await middleware.random_delay(min_seconds=0.05, max_seconds=0.1)
        elapsed = time.time() - start
        
        assert elapsed < 0.2  # Should use custom range, not config


class TestMiddlewareDecorators:
    """Tests for middleware decorators."""

    @pytest.mark.asyncio
    async def test_with_concurrency_limit_decorator(self):
        """Test concurrency limit decorator."""
        config = {'anti_detection': {'max_concurrent': 1}}
        middleware = AntiDetectionMiddleware(config)
        
        @middleware.with_concurrency_limit
        async def limited_func():
            await asyncio.sleep(0.01)
            return "done"
        
        result = await limited_func()
        assert result == "done"

    @pytest.mark.asyncio
    async def test_with_random_delay_decorator(self):
        """Test random delay decorator."""
        config = {
            'anti_detection': {
                'request_delay': {'min': 0.05, 'max': 0.1}
            }
        }
        middleware = AntiDetectionMiddleware(config)
        
        @middleware.with_random_delay
        async def delayed_func():
            return "done"
        
        import time
        start = time.time()
        result = await delayed_func()
        elapsed = time.time() - start
        
        assert result == "done"
        assert elapsed >= 0.05

    @pytest.mark.asyncio
    async def test_with_retry_decorator(self):
        """Test retry decorator."""
        config = {'anti_detection': {'retry': {'max_attempts': 3}}}
        middleware = AntiDetectionMiddleware(config)
        
        call_count = 0
        
        @middleware.with_retry_decorator(max_retries=3)
        async def retryable_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Fail")
            return "success"
        
        result = await retryable_func()
        assert result == "success"
        assert call_count == 2


class TestMiddlewareCaptchaDetection:
    """Tests for CAPTCHA detection."""

    def test_captcha_detection_initialization(self):
        """Test that middleware initializes correctly."""
        config = {'anti_detection': {}}
        middleware = AntiDetectionMiddleware(config)
        
        assert middleware.config is not None
        assert middleware.semaphore is not None
