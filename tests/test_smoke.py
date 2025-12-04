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
    from src.utils import db, config, hash, auth
    # ETL modules
    from src.extract import google_sheets
    from src.transform import normalizer, loader, transformer
    from src.load import postgres_loader
    from src.marts import build_marts
    from src.export import sheets_export
    assert True  # импорты прошли успешно
