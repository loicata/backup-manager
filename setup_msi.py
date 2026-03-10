"""
Backup Manager — MSI Installer Build Script
Uses cx_Freeze to bundle the application + Python runtime into a .msi installer.

Usage (on Windows 10/11):
    pip install cx_Freeze
    cd backup_app
    python build/setup_msi.py bdist_msi
"""

import sys
import os
from pathlib import Path
from cx_Freeze import setup, Executable

# ── Project paths ──
# Try parent directory first, then check if gui.py exists
PROJECT_DIR = Path(__file__).parent.parent.resolve()
if not (PROJECT_DIR / "gui.py").exists():
    # Maybe we're running from inside the build folder itself
    PROJECT_DIR = Path.cwd()
    if not (PROJECT_DIR / "gui.py").exists():
        PROJECT_DIR = Path.cwd().parent
    if not (PROJECT_DIR / "gui.py").exists():
        print(f"ERROR: Cannot find gui.py. Run this script from the backup_app folder:")
        print(f"  cd backup_app")
        print(f"  python build/setup_msi.py bdist_msi")
        sys.exit(1)

print(f"  Project directory: {PROJECT_DIR}")

# Ensure source dir is in sys.path so cx_Freeze can find modules
sys.path.insert(0, str(PROJECT_DIR))
SRC_FILES = [
    "gui.py", "wizard.py", "installer.py", "config.py",
    "backup_engine.py", "verification.py", "encryption.py",
    "storage.py", "scheduler.py",
]

# ── Version ──
APP_NAME = "Backup Manager"
APP_VERSION = "2.2.8"
APP_DESCRIPTION = "Complete backup application for Windows 10/11"
APP_AUTHOR = "Backup Manager Project"
APP_ICON = None  # Set to "icon.ico" if you have one

# ── cx_Freeze build options ──
build_exe_options = {
    "packages": [
        "tkinter", "json", "hashlib", "zipfile", "shutil",
        "threading", "subprocess", "logging", "dataclasses",
        "pathlib", "fnmatch", "uuid", "locale", "importlib",
        "struct", "secrets", "tempfile",
    ],
    "excludes": [
        "test", "unittest", "distutils", "setuptools",
        "numpy", "pandas", "matplotlib", "scipy",
    ],
    "include_files": [
        # Include all source modules
        *[(str(PROJECT_DIR / f), f) for f in SRC_FILES],
        # Include docs if present
        *([(str(PROJECT_DIR / "docs"), "docs")]
          if (PROJECT_DIR / "docs").exists() else []),
        # Include README
        *([(str(PROJECT_DIR / "README.md"), "README.md")]
          if (PROJECT_DIR / "README.md").exists() else []),
    ],
    "optimize": 2,
}

# ── MSI-specific options ──
bdist_msi_options = {
    "upgrade_code": "{B4CKU9-M4N4-G3R1-0000-MSI1NST4LL}",
    "add_to_path": False,
    "initial_target_dir": r"[ProgramFilesFolder]\BackupManager",
    "all_users": True,
}

# ── Shortcut table for Start Menu ──
shortcut_table = [
    (
        "DesktopShortcut",          # Shortcut
        "DesktopFolder",            # Directory
        APP_NAME,                   # Name
        "TARGETDIR",                # Component
        "[TARGETDIR]BackupManager.exe",       # Target
        None,                       # Arguments
        APP_DESCRIPTION,            # Description
        None,                       # Hotkey
        None,                       # Icon
        None,                       # IconIndex
        None,                       # ShowCmd
        "TARGETDIR",                # WkDir
    ),
    (
        "StartMenuShortcut",
        "StartMenuFolder",
        APP_NAME,
        "TARGETDIR",
        "[TARGETDIR]BackupManager.exe",
        None,
        APP_DESCRIPTION,
        None,
        None,
        None,
        None,
        "TARGETDIR",
    ),
]

msi_data = {
    "Shortcut": shortcut_table,
}

bdist_msi_options["data"] = msi_data

# ── Detect correct base name for cx_Freeze version ──
# cx_Freeze >= 7.0 uses "gui" instead of legacy "Win32GUI"
import cx_Freeze as _cxf
_cxf_version = tuple(int(x) for x in _cxf.__version__.split(".")[:2])
GUI_BASE = "gui" if _cxf_version >= (7, 0) else "Win32GUI"

# ── Main executable ──
gui_exe = Executable(
    script=str(PROJECT_DIR / "gui.py"),
    base=GUI_BASE,  # No console window
    target_name="BackupManager.exe",
    shortcut_name=APP_NAME,
    shortcut_dir="StartMenuFolder",
)

# ── Setup ──
setup(
    name=APP_NAME,
    version=APP_VERSION,
    description=APP_DESCRIPTION,
    author=APP_AUTHOR,
    options={
        "build_exe": build_exe_options,
        "bdist_msi": bdist_msi_options,
    },
    executables=[gui_exe],
)
