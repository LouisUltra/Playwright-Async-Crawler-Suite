"""Tests for project structure validation."""

import os
import pytest
from pathlib import Path


class TestProjectStructure:
    """Test that project structure is correctly set up."""

    @pytest.fixture
    def project_root(self):
        """Get project root directory."""
        return Path(__file__).parent.parent

    def test_required_directories_exist(self, project_root):
        """Test that all required directories exist."""
        required_dirs = [
            "core",
            "spiders",
            "utils",
            "config",
            "tests",
            "tests/mock_data",
            "examples",
            "docs",
            "output",
            "logs",
        ]
        
        for dir_name in required_dirs:
            dir_path = project_root / dir_name
            assert dir_path.exists(), f"Required directory '{dir_name}' does not exist"
            assert dir_path.is_dir(), f"'{dir_name}' exists but is not a directory"

    def test_required_files_exist(self, project_root):
        """Test that all required files exist."""
        required_files = [
            "requirements.txt",
            "setup.py",
            "pyproject.toml",
            ".gitignore",
            "core/__init__.py",
            "spiders/__init__.py",
            "utils/__init__.py",
        ]
        
        for file_name in required_files:
            file_path = project_root / file_name
            assert file_path.exists(), f"Required file '{file_name}' does not exist"
            assert file_path.is_file(), f"'{file_name}' exists but is not a file"

    def test_gitignore_patterns(self, project_root):
        """Test that .gitignore contains essential patterns."""
        gitignore_path = project_root / ".gitignore"
        with open(gitignore_path, "r") as f:
            content = f.read()
        
        essential_patterns = [
            "__pycache__",
            "*.pyc",
            ".venv",
            "chrome_profile",
            "*.log",
            "output/",
            "logs/",
            ".env",
        ]
        
        for pattern in essential_patterns:
            assert pattern in content, f"Essential pattern '{pattern}' not in .gitignore"

    def test_requirements_file_not_empty(self, project_root):
        """Test that requirements.txt is not empty."""
        req_path = project_root / "requirements.txt"
        with open(req_path, "r") as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
        
        assert len(lines) > 0, "requirements.txt is empty"
        assert "playwright" in " ".join(lines).lower(), "playwright not in requirements"

    def test_package_init_files(self, project_root):
        """Test that package __init__.py files are not empty."""
        init_files = [
            "core/__init__.py",
            "spiders/__init__.py",
            "utils/__init__.py",
        ]
        
        for init_file in init_files:
            init_path = project_root / init_file
            with open(init_path, "r") as f:
                content = f.read().strip()
            
            assert len(content) > 0, f"{init_file} is empty"
            assert "__all__" in content or "import" in content, \
                f"{init_file} should contain imports or __all__"
