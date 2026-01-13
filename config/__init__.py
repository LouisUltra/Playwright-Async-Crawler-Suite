"""Configuration management for Playwright-Async-Crawler-Suite."""

import yaml
import os
from pathlib import Path
from typing import Dict, Any


def load_config(config_path: str = None) -> Dict[str, Any]:
    """Load configuration from YAML file.
    
    Args:
        config_path: Path to config file. If None, uses default config.yaml
        
    Returns:
        Configuration dictionary
        
    Raises:
        FileNotFoundError: If config file not found
        yaml.YAMLError: If config file is invalid
    """
    if config_path is None:
        # Default to config.yaml in same directory
        config_path = Path(__file__).parent / "config.yaml"
    
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Substitute environment variables
    config = _substitute_env_vars(config)
    
    return config


def _substitute_env_vars(config: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively substitute environment variables in config.
    
    Supports ${VAR_NAME} and ${VAR_NAME:default} syntax.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Configuration with environment variables substituted
    """
    if isinstance(config, dict):
        return {k: _substitute_env_vars(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_substitute_env_vars(item) for item in config]
    elif isinstance(config, str):
        # Simple environment variable substitution
        if config.startswith('${') and config.endswith('}'):
            var_expr = config[2:-1]
            
            # Check for default value
            if ':' in var_expr:
                var_name, default = var_expr.split(':', 1)
                return os.getenv(var_name, default)
            else:
                return os.getenv(var_expr, config)
        return config
    else:
        return config


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration has required fields.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if valid, False otherwise
    """
    required_sections = ['browser', 'anti_detection', 'output', 'logging']
    
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required config section: {section}")
    
    # Validate browser config
    browser_config = config['browser']
    if 'headless' not in browser_config:
        raise ValueError("Missing required browser.headless setting")
    
    # Validate anti_detection config
    anti_detection = config['anti_detection']
    if 'max_concurrent' not in anti_detection:
        raise ValueError("Missing required anti_detection.max_concurrent setting")
    
    return True


__all__ = ['load_config', 'validate_config']
