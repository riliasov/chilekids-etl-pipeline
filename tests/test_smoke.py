import os
import sys


def test_imports():
    """Simple smoke test: import core modules to ensure import-time errors don't occur.

    Ensure the repository root is on sys.path so `src` can be imported when pytest
    runs from the tests directory.
    """
    # Add project root to PYTHONPATH for test discovery
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    """Test that all core modules can be imported without errors."""
    # Core utilities
    # Core utilities
    from src import db, config, utils
    # ETL modules
    from src import sheets, transform, marts
    assert True  # импорты прошли успешно
    assert True  # импорты прошли успешно
