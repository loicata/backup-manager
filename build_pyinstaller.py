"""
Backup Manager - PyInstaller Build Script
===========================================
Builds a standalone Windows .exe from the Python source.

Output: build/dist/BackupManager/BackupManager.exe + _internal/

Steps:
  1. Kill any running BackupManager.exe (taskkill)
  2. Clean old dist folder
  3. Copy all .py source files to build/src/
  4. Generate version_info.txt (Windows file properties: "Backup Manager")
  5. Run PyInstaller with --windowed, hidden imports, and version file
  6. Output ready for Inno Setup packaging

Hidden imports: tkinter, pystray, PIL, plyer (optional dependencies
that PyInstaller can't detect automatically).

The version_info.txt ensures Windows shows "Backup Manager" in taskbar
and system tray settings, instead of "BackupManager.exe".
"""

import subprocess
import sys
import shutil
from pathlib import Path

# When all files are in the same directory (flat structure)
PROJECT_DIR = Path(__file__).parent
BUILD_DIR = PROJECT_DIR / "build"
DIST_DIR = PROJECT_DIR / "dist"

# Entry point for the application
ENTRY_POINT = PROJECT_DIR / "src" / "__main__.py"


APP_VERSION = "2.2.9"


def build():
    print("=" * 60)
    print("  Backup Manager — PyInstaller Build")
    print("=" * 60)

    # Kill any running BackupManager.exe to release locked files
    if sys.platform == "win32":
        print("\n  Closing running BackupManager instances...")
        result = subprocess.run(
            ["taskkill", "/F", "/IM", "BackupManager.exe"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            print("  Closed running instance. Waiting 3 seconds...")
            import time
            time.sleep(3)
        else:
            print("  No running instance found.")

    # Clean old dist folder if it exists (avoids PermissionError)
    dist_target = DIST_DIR / "BackupManager"
    if dist_target.exists():
        print(f"  Cleaning old build: {dist_target}")
        try:
            shutil.rmtree(dist_target)
        except PermissionError as e:
            print(f"\n  ❌ Cannot delete {dist_target}")
            print(f"     {e}")
            print(f"\n  Fix: close BackupManager.exe, or delete the folder manually:")
            print(f"       rmdir /s /q \"{dist_target}\"")
            sys.exit(1)

    # Check PyInstaller
    try:
        import PyInstaller
        print(f"  PyInstaller {PyInstaller.__version__} found.")
    except ImportError:
        print("  Installing PyInstaller...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller", "--quiet"])

    # Collect data files — entire src package
    add_data = [
        f"--add-data={PROJECT_DIR / 'src'};src",
    ]

    # Include docs
    docs_dir = PROJECT_DIR / "docs"
    if docs_dir.exists():
        add_data.append(f"--add-data={docs_dir};docs")

    # This file embeds metadata into the .exe so Windows shows
    # 'Backup Manager' instead of 'BackupManager.exe' in taskbar/tray.
    # Generate Windows version info file (so Windows shows "Backup Manager" instead of "BackupManager.exe")
    version_file = BUILD_DIR / "version_info.txt"
    version_file.parent.mkdir(parents=True, exist_ok=True)
    version_file.write_text(f"""# UTF-8
VSVersionInfo(
  ffi=FixedFileInfo(
    filevers=(2, 2, 8, 0),
    prodvers=(2, 2, 8, 0),
    mask=0x3f,
    flags=0x0,
    OS=0x40004,
    fileType=0x1,
    subtype=0x0,
    date=(0, 0)
  ),
  kids=[
    StringFileInfo([
      StringTable('040904B0', [
        StringStruct('CompanyName', 'Loic Ader'),
        StringStruct('FileDescription', 'Backup Manager'),
        StringStruct('FileVersion', '{APP_VERSION}'),
        StringStruct('InternalName', 'BackupManager'),
        StringStruct('OriginalFilename', 'BackupManager.exe'),
        StringStruct('ProductName', 'Backup Manager'),
        StringStruct('ProductVersion', '{APP_VERSION}'),
      ])
    ]),
    VarFileInfo([VarStruct('Translation', [1033, 1200])])
  ]
)
""")

    # Build command
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--name=BackupManager",
        "--windowed",              # No console window for GUI
        "--noconfirm",             # Overwrite without asking
        "--clean",                 # Clean PyInstaller cache before building
        f"--distpath={DIST_DIR}",
        f"--workpath={BUILD_DIR / 'pyinstaller_build'}",
        f"--specpath={BUILD_DIR}",
        f"--version-file={version_file}",
        f"--paths={PROJECT_DIR}",  # So PyInstaller finds the src package
        # Hidden imports for optional dependencies
        "--hidden-import=tkinter",
        "--hidden-import=tkinter.ttk",
        "--hidden-import=json",
        "--hidden-import=hashlib",
        "--hidden-import=zipfile",
        "--hidden-import=locale",
        "--hidden-import=pystray",
        "--hidden-import=pystray._win32",
        "--hidden-import=PIL",
        "--hidden-import=PIL.Image",
        "--hidden-import=PIL.ImageDraw",
        "--hidden-import=PIL.ImageFont",
        "--hidden-import=plyer.platforms.win.notification",
        # Hidden imports for src subpackages
        "--hidden-import=src",
        "--hidden-import=src.core",
        "--hidden-import=src.storage",
        "--hidden-import=src.security",
        "--hidden-import=src.ui",
        "--hidden-import=src.ui.tabs",
        "--hidden-import=src.notifications",
        # Exclude unnecessary modules to reduce exe size
        "--exclude-module=test",
        "--exclude-module=unittest",
        "--exclude-module=distutils",
        "--exclude-module=setuptools",
        "--exclude-module=pip",
        "--exclude-module=ensurepip",
        "--exclude-module=tkinter.test",
        "--exclude-module=lib2to3",
        "--collect-all=tkinter",
        *add_data,
        str(ENTRY_POINT),
    ]

    print(f"\n  Running PyInstaller...")
    print(f"  Command: {' '.join(cmd[:8])}...\n")

    result = subprocess.run(cmd)

    if result.returncode == 0:
        print("\n" + "=" * 60)
        print("  BUILD SUCCESSFUL!")
        print("=" * 60)
        print(f"\n  Output: {DIST_DIR / 'BackupManager'}")
        print(f"  Main executable: {DIST_DIR / 'BackupManager' / 'BackupManager.exe'}")
        print(f"\n  Next steps:")
        print(f"    1. Test: run {DIST_DIR / 'BackupManager' / 'BackupManager.exe'}")
        print(f"    2. Create installer: compile build/innosetup.iss with Inno Setup")
        print(f"       Download: https://jrsoftware.org/isdl.php")
    else:
        print("\n  BUILD FAILED. Check the output above.")
        sys.exit(1)


if __name__ == "__main__":
    build()
