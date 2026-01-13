"""Tests for configuration management."""

import pytest
import os
import tempfile
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import load_config, validate_config


class TestConfigLoading:
    """Tests for configuration loading."""

    def test_load_default_config(self):
        """Test loading default config.yaml."""
        config = load_config()
        
        assert config is not None
        assert 'browser' in config
        assert 'anti_detection' in config
        assert 'output' in config
        assert 'logging' in config

    def test_load_custom_config(self):
        """Test loading custom config file."""
        # Create temporary config
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
browser:
  headless: true
anti_detection:
  max_concurrent: 5
output:
  directory: "test_output"
logging:
  level: "DEBUG"
""")
            temp_path = f.name
        
        try:
            config = load_config(temp_path)
            assert config['browser']['headless'] is True
            assert config['anti_detection']['max_concurrent'] == 5
        finally:
            os.unlink(temp_path)

    def test_load_nonexistent_config(self):
        """Test that loading nonexistent config raises error."""
        with pytest.raises(FileNotFoundError):
            load_config('/nonexistent/config.yaml')

    def test_environment_variable_substitution(self):
        """Test that environment variables are substituted."""
        # Set test environment variable
        os.environ['TEST_MAX_CONCURRENT'] = '10'
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
browser:
  headless: true
anti_detection:
  max_concurrent: ${TEST_MAX_CONCURRENT}
output:
  directory: "output"
logging:
  level: "INFO"
""")
            temp_path = f.name
        
        try:
            config = load_config(temp_path)
            assert config['anti_detection']['max_concurrent'] == '10'
        finally:
            os.unlink(temp_path)
            del os.environ['TEST_MAX_CONCURRENT']

    def test_environment_variable_with_default(self):
        """Test environment variable with default value."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
browser:
  headless: true
anti_detection:
  max_concurrent: ${NONEXISTENT_VAR:3}
output:
  directory: "output"
logging:
  level: "INFO"
""")
            temp_path = f.name
        
        try:
            config = load_config(temp_path)
            assert config['anti_detection']['max_concurrent'] == '3'
        finally:
            os.unlink(temp_path)


class TestConfigValidation:
    """Tests for configuration validation."""

    def test_validate_complete_config(self):
        """Test validation of complete config."""
        config = {
            'browser': {'headless': True},
            'anti_detection': {'max_concurrent': 3},
            'output': {'directory': 'output'},
            'logging': {'level': 'INFO'}
        }
        
        assert validate_config(config) is True

    def test_validate_missing_section(self):
        """Test validation fails with missing section."""
        config = {
            'browser': {'headless': True},
            'anti_detection': {'max_concurrent': 3},
            # Missing 'output' and 'logging'
        }
        
        with pytest.raises(ValueError, match="Missing required config section"):
            validate_config(config)

    def test_validate_missing_browser_headless(self):
        """Test validation fails with missing browser.headless."""
        config = {
            'browser': {},  # Missing 'headless'
            'anti_detection': {'max_concurrent': 3},
            'output': {'directory': 'output'},
            'logging': {'level': 'INFO'}
        }
        
        with pytest.raises(ValueError, match="Missing required browser.headless"):
            validate_config(config)

    def test_validate_missing_max_concurrent(self):
        """Test validation fails with missing max_concurrent."""
        config = {
            'browser': {'headless': True},
            'anti_detection': {},  # Missing 'max_concurrent'
            'output': {'directory': 'output'},
            'logging': {'level': 'INFO'}
        }
        
        with pytest.raises(ValueError, match="Missing required anti_detection.max_concurrent"):
            validate_config(config)


class TestConfigStructure:
    """Tests for config structure and content."""

    def test_default_config_has_browser_settings(self):
        """Test that default config has all browser settings."""
        config = load_config()
        browser = config['browser']
        
        assert 'headless' in browser
        assert 'viewport' in browser
        assert 'locale' in browser
        assert 'timezone' in browser

    def test_default_config_has_anti_detection_settings(self):
        """Test that default config has anti-detection settings."""
        config = load_config()
        anti_detection = config['anti_detection']
        
        assert 'max_concurrent' in anti_detection
        assert 'request_delay' in anti_detection
        assert 'retry' in anti_detection

    def test_default_config_has_spider_settings(self):
        """Test that default config has spider settings."""
        config = load_config()
        
        assert 'spiders' in config
        assert 'instruction' in config['spiders']
        assert 'ingredient' in config['spiders']

    def test_config_urls_are_externalized(self):
        """Property 7: Configuration Externalization.
        
        For any target URL or sensitive configuration, it should be loaded
        from config.yaml and not hardcoded in source files.
        
        Feature: playwright-async-crawler-suite, Property 7: Configuration Externalization
        Validates: Requirements 9.3, 11.3
        """
        config = load_config()
        
        # Check that URLs are in config
        assert 'target_url' in config['spiders']['instruction']
        assert 'target_url' in config['spiders']['ingredient']
        
        # URLs should be strings
        assert isinstance(config['spiders']['instruction']['target_url'], str)
        assert isinstance(config['spiders']['ingredient']['target_url'], str)
