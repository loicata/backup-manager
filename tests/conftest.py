"""Shared test fixtures for Backup Manager v3.

Provides session-scoped and function-scoped fixtures shared across
all test modules.  Import these by name in test functions — pytest
discovers them automatically from this conftest.py.

Fixtures:
    tk_root: Session-scoped Tkinter root for UI tests (avoids Tcl errors).
    tmp_config_dir: Function-scoped temp config directory with profiles/logs/manifests.
    sample_files: Function-scoped temp directory with sample source files.
    _isolate_hmac_key (autouse): Replaces the per-install HMAC key with
        a fixed test value for the entire test session. Without this,
        any test that goes through the backup pipeline ends up calling
        the real ``_get_hmac_key`` which touches Windows DPAPI and
        ``%APPDATA%/BackupManager/.integrity_key``. That has two bad
        effects: (1) DPAPI calls serialise + slow down hundreds of
        tests when Defender is active, (2) writing/regenerating the
        real key file races with the installed Backup Manager app
        which may be running in parallel.
"""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# Stable test-only HMAC key. Any 32 bytes will do; this is consistent
# across the whole session so a marker written in one test can be
# verified in another (e.g. round-trip through the engine).
_SESSION_TEST_HMAC_KEY = b"\xab" * 32


@pytest.fixture(autouse=True)
def _isolate_hmac_key():
    """Block every test from touching the real HMAC key on disk.

    Patches the source-of-truth ``_get_hmac_key`` plus its public
    alias ``get_app_hmac_key`` so neither the integrity-check module
    nor any consumer (commit_marker, backup_engine, etc) reads or
    rewrites the user's real ``%APPDATA%/BackupManager/.integrity_key``.

    Applied automatically to every test in the suite — there's no
    legitimate test reason to exercise the real DPAPI key, and
    forgetting to apply this in even one test silently slows pytest
    by minutes (DPAPI is a system call) and risks corrupting the
    installed app's key file.
    """
    with (
        patch(
            "src.security.integrity_check._get_hmac_key",
            return_value=_SESSION_TEST_HMAC_KEY,
        ),
        patch(
            "src.security.integrity_check.get_app_hmac_key",
            return_value=_SESSION_TEST_HMAC_KEY,
        ),
        patch(
            "src.core.phases.commit_marker.get_app_hmac_key",
            return_value=_SESSION_TEST_HMAC_KEY,
        ),
    ):
        yield


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
