"""Shared test fixtures for Backup Manager v3.

Provides session-scoped and function-scoped fixtures shared across
all test modules.  Import these by name in test functions — pytest
discovers them automatically from this conftest.py.

Fixtures:
    tk_root: Session-scoped Tkinter root for UI tests (avoids Tcl errors).
    tmp_config_dir: Function-scoped temp config directory with profiles/logs/manifests.
    sample_files: Function-scoped temp directory with sample source files.
"""

import sys
from pathlib import Path

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture(scope="session")
def tk_root():
    """Single Tk instance shared across the entire test session.

    Using session scope avoids Tcl corruption when multiple test
    modules each create and destroy their own Tk root.  The root
    is withdrawn immediately (hidden) and destroyed at session end.

    Used by: unit/test_recovery_tab_autofill, unit/test_run_tab_progress,
    unit/test_bandwidth_percent_ui, unit/test_clear_tabs_on_delete,
    unit/test_sv_ttk_theme.
    """
    import tkinter as tk

    root = tk.Tk()
    root.withdraw()
    yield root
    root.destroy()


@pytest.fixture
def tmp_config_dir(tmp_path):
    """Provide a temporary config directory for ConfigManager.

    Creates the standard subdirectory structure expected by ConfigManager:
    profiles/, logs/, and manifests/.  Each test gets its own isolated
    directory (function-scoped via tmp_path).
    """
    config_dir = tmp_path / "BackupManager"
    config_dir.mkdir()
    (config_dir / "profiles").mkdir()
    (config_dir / "logs").mkdir()
    (config_dir / "manifests").mkdir()
    return config_dir


@pytest.fixture
def sample_files(tmp_path):
    """Create sample source files for backup testing.

    Structure:
        source/
        ├── file1.txt   ("Hello World")
        ├── file2.txt   ("Test content")
        └── subdir/
            └── file3.txt  ("Nested file")

    Returns the source directory Path.
    """
    files_dir = tmp_path / "source"
    files_dir.mkdir()
    (files_dir / "file1.txt").write_text("Hello World", encoding="utf-8")
    (files_dir / "file2.txt").write_text("Test content", encoding="utf-8")
    sub = files_dir / "subdir"
    sub.mkdir()
    (sub / "file3.txt").write_text("Nested file", encoding="utf-8")
    return files_dir
