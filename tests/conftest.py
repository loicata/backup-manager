"""Shared test fixtures for Backup Manager v3."""

import sys
from pathlib import Path

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture(scope="session")
def tk_root():
    """Single Tk instance shared across the entire test session.

    Using session scope avoids Tcl corruption when multiple test
    modules each create and destroy their own Tk root.
    """
    import tkinter as tk

    root = tk.Tk()
    root.withdraw()
    yield root
    root.destroy()


@pytest.fixture
def tmp_config_dir(tmp_path):
    """Provide a temporary config directory for ConfigManager."""
    config_dir = tmp_path / "BackupManager"
    config_dir.mkdir()
    (config_dir / "profiles").mkdir()
    (config_dir / "logs").mkdir()
    (config_dir / "manifests").mkdir()
    return config_dir


@pytest.fixture
def sample_files(tmp_path):
    """Create sample files for testing."""
    files_dir = tmp_path / "source"
    files_dir.mkdir()
    (files_dir / "file1.txt").write_text("Hello World", encoding="utf-8")
    (files_dir / "file2.txt").write_text("Test content", encoding="utf-8")
    sub = files_dir / "subdir"
    sub.mkdir()
    (sub / "file3.txt").write_text("Nested file", encoding="utf-8")
    return files_dir
