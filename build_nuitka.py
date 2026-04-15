"""Build Backup Manager v3 executable with Nuitka.

Usage: python build_nuitka.py
Requires: Nuitka, MSVC (cl.exe), ordered-set.
Output: dist/BackupManager/BackupManager.exe

Nuitka compiles Python to C, then to native binary. The resulting
executable is much harder to decompile than PyInstaller output —
no extractable .pyc files, no readable source code.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

# Project root (resolve to main repo if running from a git worktree)
ROOT = Path(__file__).resolve().parent
_git_common = ROOT / ".git"
if _git_common.is_file():
    _main_root = Path(
        _git_common.read_text(encoding="utf-8").split("gitdir: ")[1].strip()
    ).resolve()
    while _main_root.name != ".git":
        _main_root = _main_root.parent
    DIST = _main_root.parent / "dist"
else:
    DIST = ROOT / "dist"
SRC = ROOT / "src"
ASSETS = ROOT / "assets"
ICON = ASSETS / "backup_manager.ico"
BUILD_DIR = DIST / "BackupManager"


def get_version() -> str:
    """Read version from src/__init__.py."""
    import re

    init = SRC / "__init__.py"
    for line in init.read_text(encoding="utf-8").splitlines():
        m = re.search(r'__version__\s*=\s*["\'](\d+\.\d+\.\d+)["\']', line)
        if m:
            return m.group(1)
    return "0.0.0"


def build():
    """Build the executable with Nuitka."""
    version = get_version()
    print(f"Building Backup Manager v{version} with Nuitka...")

    # Clean previous build output
    nuitka_build = ROOT / "BackupManager.build"
    nuitka_dist = ROOT / "BackupManager.dist"
    for d in (nuitka_build, nuitka_dist):
        if d.exists():
            shutil.rmtree(d)

    cmd = [
        sys.executable,
        "-m",
        "nuitka",
        # Output mode: standalone directory (like PyInstaller --onedir)
        "--standalone",
        # Output directory name
        "--output-dir=" + str(ROOT),
        "--output-filename=BackupManager.exe",
        # Windows GUI app (no console)
        "--windows-console-mode=disable",
        # Auto-download dependency walker
        "--assume-yes-for-downloads",
        # Tkinter plugin (required for standalone)
        "--enable-plugin=tk-inter",
        # Icon
    ]

    if ICON.exists():
        cmd.append(f"--windows-icon-from-ico={ICON}")

    # Company/product info for the .exe properties
    cmd += [
        "--windows-product-name=Backup Manager",
        f"--windows-product-version={version}",
        "--windows-company-name=loicata.com",
        f"--windows-file-description=Backup Manager v{version}",
    ]

    # Include packages that Nuitka might not detect automatically
    packages = [
        "paramiko",
        "boto3",
        "botocore",
        "pystray",
        "PIL",
        "cryptography",
        "sv_ttk",
        "pyotp",
    ]
    for pkg in packages:
        cmd.append(f"--include-package={pkg}")

    # Include data files (assets)
    if ICON.exists():
        cmd.append(f"--include-data-files={ICON}=assets/backup_manager.ico")

    license_rtf = ASSETS / "License.rtf"
    if license_rtf.exists():
        cmd.append(f"--include-data-files={license_rtf}=assets/License.rtf")

    # Launch script
    launch_vbs = ASSETS / "launch.vbs"
    if launch_vbs.exists():
        cmd.append(f"--include-data-files={launch_vbs}=launch.vbs")

    # Bug report signing key (Ed25519 private key, gitignored)
    signing_key = ASSETS / "report_signing_key.pem"
    if signing_key.exists():
        cmd.append(f"--include-data-files={signing_key}=assets/report_signing_key.pem")
    else:
        print("WARNING: report_signing_key.pem not found " "— reports won't be signed")

    # Python path for imports
    cmd.append("--include-package-data=sv_ttk")

    # Enable anti-bloat to reduce size
    cmd.append("--nofollow-import-to=pytest")
    cmd.append("--nofollow-import-to=setuptools")
    cmd.append("--nofollow-import-to=pip")

    # Entry point
    cmd.append(str(SRC / "__main__.py"))

    # Set working directory
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)

    print("Running Nuitka (this may take several minutes)...")
    print(f"Command: {' '.join(cmd[:6])}...")
    result = subprocess.run(cmd, cwd=str(ROOT), env=env)

    if result.returncode != 0:
        print(f"Nuitka build failed with exit code {result.returncode}")
        sys.exit(1)

    # Nuitka outputs to __main__.dist — rename to dist/BackupManager
    nuitka_output = ROOT / "__main__.dist"
    if not nuitka_output.exists():
        # Try alternative name
        nuitka_output = ROOT / "BackupManager.dist"

    if not nuitka_output.exists():
        print("ERROR: Nuitka output directory not found")
        sys.exit(1)

    # Move to dist/BackupManager
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    DIST.mkdir(parents=True, exist_ok=True)
    shutil.move(str(nuitka_output), str(BUILD_DIR))

    # Rename exe if needed (Nuitka names it after the entry point)
    for name in ("__main__.exe", "BackupManager.exe"):
        exe = BUILD_DIR / name
        if exe.exists():
            target = BUILD_DIR / "BackupManager.exe"
            if exe != target:
                exe.rename(target)
            break

    # Clean build artifacts
    for d in (ROOT / "__main__.build", ROOT / "BackupManager.build"):
        if d.exists():
            shutil.rmtree(d)

    exe = BUILD_DIR / "BackupManager.exe"
    if exe.exists():
        size_mb = exe.stat().st_size / (1024 * 1024)
        print("\nBuild successful!")
        print(f"  Executable: {exe}")
        print(f"  Size: {size_mb:.1f} MB")
        print(f"  Version: {version}")
        print("  Compiler: Nuitka (native C binary)")
    else:
        print("Build completed but exe not found")
        sys.exit(1)


if __name__ == "__main__":
    build()
