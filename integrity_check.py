"""
Backup Manager - Application Integrity Check
===============================================
Self-verification of application files using SHA-256 checksums.
Detects corruption or tampering of .py files (especially useful for .exe builds).

Workflow:
  First run:  compute_checksums() → save to %APPDATA%/BackupManager/app_checksums.json
  Next runs:  verify_integrity()  → compare stored vs current checksums

If a mismatch is detected (e.g., after an update), reset_checksums() regenerates
the baseline. The GUI auto-resets silently — no blocking prompt.

Files checked: all .py modules listed in APP_FILES constant.
For PyInstaller builds, uses sys._MEIPASS as the app directory.
"""

import hashlib
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Stored in %APPDATA%/BackupManager/ — survives app updates
CHECKSUM_FILE = "app_checksums.json"

# All .py modules that should be integrity-checked.
# NOTE: build_pyinstaller.py and setup_msi.py are excluded
# because they are build-only tools (not shipped in the .exe).
APP_FILES = [
    "gui.py", "backup_engine.py", "config.py", "encryption.py",
    "verification.py", "storage.py", "scheduler.py", "wizard.py",
    "installer.py", "tray.py", "email_notifier.py",
]


def _get_app_dir() -> Path:
    """Get the application directory.
    In a PyInstaller .exe, files are extracted to a temp folder (sys._MEIPASS).
    In development, uses the directory containing this script.
    """
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        return Path(sys._MEIPASS)  # PyInstaller temp extraction folder
    return Path(__file__).parent    # Development: same dir as this file


def _get_checksum_path() -> Path:
    """Get the path where checksums are stored.
    Uses %APPDATA%/BackupManager/ (persistent across app updates).
    """
    import os
    config_dir = Path(os.environ.get("APPDATA", "~")) / "BackupManager"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / CHECKSUM_FILE


def _compute_file_hash(filepath: Path) -> Optional[str]:
    """Compute SHA-256 hash of a file.
    Reads in 8KB chunks to handle large files without loading into memory.
    """
    try:
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
    except (OSError, IOError):
        return None


def compute_checksums() -> dict[str, str]:
    """Compute SHA-256 checksums of all application files.
    Returns: {"gui.py": "abc123...", "config.py": "def456...", ...}
    Skips files that don't exist (e.g., optional modules).
    """
    app_dir = _get_app_dir()
    checksums = {}
    for filename in APP_FILES:
        filepath = app_dir / filename
        if filepath.exists():
            h = _compute_file_hash(filepath)
            if h:
                checksums[filename] = h
    return checksums


def save_checksums(checksums: dict[str, str]):
    """Save checksums to disk as JSON.
    Includes version and timestamp for debugging.
    """
    data = {
        "version": "2.2.8",
        "created": datetime.now().isoformat(),
        "files": checksums,
    }
    try:
        path = _get_checksum_path()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Checksums saved: {len(checksums)} files")
    except OSError as e:
        logger.warning(f"Cannot save checksums: {e}")


def load_checksums() -> Optional[dict[str, str]]:
    """Load previously stored checksums from disk.
    Returns None if file doesn't exist (first run) or is corrupted.
    """
    path = _get_checksum_path()
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("files", {})
    except (json.JSONDecodeError, OSError):
        return None


def verify_integrity() -> tuple[bool, str]:
    """
    Verify application integrity by comparing current file hashes
    against the stored baseline.

    Returns:
        (passed, message)
        - passed=True: all files match or first run (checksums saved)
        - passed=False: one or more files are corrupted/modified
    """
    stored = load_checksums()
    current = compute_checksums()

    if not current:
        return True, "No application files found to verify."

    # First run: no stored checksums → save current as baseline
    if stored is None:
        save_checksums(current)
        return True, f"First run — checksums recorded for {len(current)} files."

    # Compare stored vs current hashes
    modified = []   # Files whose hash has changed
    missing = []    # Files that existed before but are now gone

    for filename, expected_hash in stored.items():
        actual_hash = current.get(filename)
        if actual_hash is None:
            missing.append(filename)
        elif actual_hash != expected_hash:
            modified.append(filename)

    if not modified and not missing:
        return True, f"Integrity OK — {len(stored)} files verified."

    # Build human-readable warning message
    issues = []
    if modified:
        issues.append(f"{len(modified)} modified: {', '.join(modified)}")
    if missing:
        issues.append(f"{len(missing)} missing: {', '.join(missing)}")

    msg = "INTEGRITY WARNING — " + "; ".join(issues)
    logger.warning(msg)
    return False, msg


def reset_checksums():
    """Reset stored checksums to current file state.
    Called after an update to establish a new baseline.
    """
    current = compute_checksums()
    save_checksums(current)
    return f"Checksums reset for {len(current)} files."
