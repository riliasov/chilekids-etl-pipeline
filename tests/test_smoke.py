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

    modules = [
        "src.utils.config",
        "src.utils.db",
        "src.utils.http",
        "src.utils.auth",
        "src.extract.google_sheets",
        "src.extract.bitrix24",
        "src.extract.google_ads",
        "src.extract.meta_ads",
        "src.extract.yandex_direct",
        "src.extract.youtube",
        "src.archive.archival",
        "src.load.postgres_loader",
        "src.transform.transformer",
        "src.marts.build_marts",
        "src.export.sheets_export",
    ]
    for m in modules:
        __import__(m)
    assert True
