"""Build Backup Manager v3 executable with PyInstaller.

Usage: python build_pyinstaller.py
Output: dist/BackupManager/BackupManager.exe
"""

import os
import subprocess
import sys
from pathlib import Path

# Project root (resolve to main repo if running from a git worktree)
ROOT = Path(__file__).resolve().parent
_git_common = ROOT / ".git"
if _git_common.is_file():
    # Worktree: .git is a file pointing to the main repo
    _main_root = Path(
        _git_common.read_text(encoding="utf-8").split("gitdir: ")[1].strip()
    ).resolve()
    # Walk up from .git/worktrees/<name> to the repo root
    while _main_root.name != ".git":
        _main_root = _main_root.parent
    DIST = _main_root.parent / "dist"
else:
    DIST = ROOT / "dist"
SRC = ROOT / "src"
ASSETS = ROOT / "assets"
ICON = ASSETS / "backup_manager.ico"


def get_version() -> str:
    """Read version from src/__init__.py (fallback line with quoted string)."""
    import re

    init = SRC / "__init__.py"
    for line in init.read_text(encoding="utf-8").splitlines():
        m = re.search(r'__version__\s*=\s*["\'](\d+\.\d+\.\d+)["\']', line)
        if m:
            return m.group(1)
    return "0.0.0"


def build():
    version = get_version()
    print(f"Building Backup Manager v{version}...")

    # Hidden imports for optional dependencies
    hidden_imports = [
        "paramiko",
        "paramiko.transport",
        "paramiko.sftp_client",
        "paramiko.ed25519key",
        "paramiko.ecdsakey",
        "paramiko.rsakey",
        "paramiko.hostkeys",
        "paramiko.auth_handler",
        "paramiko.channel",
        "boto3",
        "botocore",
        "botocore.config",
        "botocore.exceptions",
        "pyotp",
        "pystray",
        "pystray._win32",
        "PIL",
        "PIL.Image",
        "PIL.ImageDraw",
        "PIL.ImageFont",
        "cryptography",
        "cryptography.hazmat",
        "cryptography.hazmat.primitives",
        "cryptography.hazmat.primitives.ciphers",
        "cryptography.hazmat.primitives.ciphers.aead",
    ]

    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--name",
        "BackupManager",
        "--windowed",
        "--onedir",
        "--noconfirm",
        "--clean",
        "--distpath",
        str(DIST),
        "--paths",
        str(ROOT),
    ]

    # Icon
    if ICON.exists():
        cmd += ["--icon", str(ICON)]

    # Hidden imports
    for hi in hidden_imports:
        cmd += ["--hidden-import", hi]

    # Collect submodules
    cmd += ["--collect-submodules", "paramiko"]
    cmd += ["--collect-submodules", "pystray"]
    cmd += ["--collect-submodules", "botocore"]

    # Add assets
    if ICON.exists():
        cmd += ["--add-data", f"{ICON};assets"]

    license_rtf = ASSETS / "License.rtf"
    if license_rtf.exists():
        cmd += ["--add-data", f"{license_rtf};assets"]

    # Launch script (for MSI post-install, placed at root next to exe)
    launch_vbs = ASSETS / "launch.vbs"
    if launch_vbs.exists():
        cmd += ["--add-data", f"{launch_vbs};."]

    # Entry point
    cmd.append(str(SRC / "__main__.py"))

    # Set working directory
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(ROOT), env=env)

    if result.returncode == 0:
        exe = DIST / "BackupManager" / "BackupManager.exe"
        if exe.exists():
            # Copy launch.vbs to root (PyInstaller puts add-data in _internal)
            launch_src = DIST / "BackupManager" / "_internal" / "launch.vbs"
            launch_dst = DIST / "BackupManager" / "launch.vbs"
            if launch_src.exists() and not launch_dst.exists():
                import shutil

                shutil.copy2(launch_src, launch_dst)

            size_mb = exe.stat().st_size / (1024 * 1024)
            print("\nBuild successful!")
            print(f"  Executable: {exe}")
            print(f"  Size: {size_mb:.1f} MB")
            print(f"  Version: {version}")
        else:
            print("Build completed but exe not found at expected path")
    else:
        print(f"Build failed with exit code {result.returncode}")
        sys.exit(1)


if __name__ == "__main__":
    build()
