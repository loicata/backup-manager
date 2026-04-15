"""Build MSI installer with WiX Toolset.

Usage: python build_msi.py
Requires: WiX Toolset v3 (heat.exe, candle.exe, light.exe).
Output: dist/BackupManager-{version}.msi

Uses heat.exe to auto-harvest files from the PyInstaller output,
avoiding manual ID generation and collision issues.
"""

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
    while _main_root.name != ".git":
        _main_root = _main_root.parent
    DIST = _main_root.parent / "dist"
else:
    DIST = ROOT / "dist"
SRC = ROOT / "src"
BUILD_DIR = DIST / "BackupManager"
ASSETS = ROOT / "assets"

UPGRADE_CODE = "E8F2A1B3-4C5D-6E7F-8A9B-0C1D2E3F4A5B"
WIX_BIN = Path(r"C:\Program Files (x86)\WiX Toolset v3.14\bin")


def get_version() -> str:
    """Read version from src/__init__.py (fallback line with quoted string)."""
    import re

    init = SRC / "__init__.py"
    for line in init.read_text(encoding="utf-8").splitlines():
        m = re.search(r'__version__\s*=\s*["\'](\d+\.\d+\.\d+)["\']', line)
        if m:
            return m.group(1)
    return "0.0.0"


def _patch_license_version(version: str) -> None:
    """Update the version number in License.rtf to match __version__.

    Single source of truth: src/__init__.py defines the version,
    this function patches License.rtf so they never diverge.
    """
    import re

    license_rtf = ASSETS / "License.rtf"
    if not license_rtf.exists():
        return

    content = license_rtf.read_text(encoding="utf-8")
    updated = re.sub(r"Version \d+\.\d+\.\d+", f"Version {version}", content, count=1)
    if updated != content:
        license_rtf.write_text(updated, encoding="utf-8")
        print(f"  License.rtf version updated to {version}")


def run(cmd: list[str], label: str):
    """Run a command and exit on failure."""
    print(f"  {label}...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  FAILED: {result.stderr[:2000]}")
        sys.exit(1)
    return result


def _build_wxs(version: str) -> str:
    """Generate the main Product.wxs content."""
    icon_path = ASSETS / "backup_manager.ico"
    license_rtf = ASSETS / "License.rtf"

    license_line = ""
    if license_rtf.exists():
        license_line = f'<WixVariable Id="WixUILicenseRtf" Value="{license_rtf}" />'

    icon_lines = ""
    icon_attr = ""
    if icon_path.exists():
        icon_lines = (
            f'<Icon Id="BackupManagerIcon" SourceFile="{icon_path}" />\n'
            f'    <Property Id="ARPPRODUCTICON" Value="BackupManagerIcon" />'
        )
        icon_attr = 'Icon="BackupManagerIcon"'

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Wix xmlns="http://schemas.microsoft.com/wix/2006/wi">
  <Product Id="*"
           Name="Backup Manager"
           Language="1033"
           Version="{version}.0"
           Manufacturer="Loic Ader — loicata.com"
           UpgradeCode="{UPGRADE_CODE}">

    <Package InstallerVersion="500"
             Compressed="yes"
             InstallScope="perMachine"
             Description="Backup Manager v{version}" />

    <MajorUpgrade DowngradeErrorMessage="A newer version is already installed."
                  AllowSameVersionUpgrades="yes" />

    <MediaTemplate EmbedCab="yes" CompressionLevel="high" />

    <Property Id="ARPURLINFOABOUT" Value="https://loicata.com" />
    <Property Id="ARPHELPLINK" Value="https://loicata.com" />

    {license_line}
    {icon_lines}

    <!-- Directory structure -->
    <Directory Id="TARGETDIR" Name="SourceDir">
      <Directory Id="ProgramFilesFolder">
        <Directory Id="INSTALLFOLDER" Name="Backup Manager" />
      </Directory>
      <Directory Id="ProgramMenuFolder">
        <Directory Id="ApplicationProgramsFolder" Name="Backup Manager" />
      </Directory>
    </Directory>

    <!-- Start Menu shortcut -->
    <DirectoryRef Id="ApplicationProgramsFolder">
      <Component Id="C_StartMenuShortcut" Guid="*">
        <Shortcut Id="StartMenuShortcut"
                  Name="Backup Manager"
                  Target="[INSTALLFOLDER]BackupManager.exe"
                  WorkingDirectory="INSTALLFOLDER"
                  {icon_attr} />
        <RemoveFolder Id="RemoveStartMenu" On="uninstall" />
        <RegistryValue Root="HKCU" Key="Software\\BackupManager"
                       Name="StartMenu" Type="integer" Value="1"
                       KeyPath="yes" />
      </Component>
    </DirectoryRef>

    <!-- Clean up registry keys on uninstall -->
    <Component Id="C_CleanupRegistry" Directory="INSTALLFOLDER" Guid="B2C3D4E5-F6A7-8901-BCDE-F12345678901">
      <RegistryValue Root="HKCU" Key="Software\\BackupManager"
                     Name="Installed" Type="integer" Value="1"
                     KeyPath="yes" />
      <RemoveRegistryKey Id="RemoveRegKey" Root="HKCU"
                         Key="Software\\BackupManager"
                         Action="removeOnUninstall" />
      <RemoveRegistryValue Id="RemoveAutoStartRun" Root="HKCU"
                           Key="Software\\Microsoft\\Windows\\CurrentVersion\\Run"
                           Name="BackupManager" />
    </Component>

    <!-- Features -->
    <Feature Id="Complete" Title="Backup Manager" Level="1">
      <ComponentGroupRef Id="ProductFiles" />
      <ComponentRef Id="C_StartMenuShortcut" />
      <ComponentRef Id="C_CleanupRegistry" />
    </Feature>

    <!-- UI -->
    <UIRef Id="WixUI_InstallDir" />
    <Property Id="WIXUI_INSTALLDIR" Value="INSTALLFOLDER" />

    <!-- Launch app after user clicks Finish (not during install) -->
    <Property Id="WIXUI_EXITDIALOGOPTIONALCHECKBOXTEXT"
              Value="Launch Backup Manager" />
    <Property Id="WIXUI_EXITDIALOGOPTIONALCHECKBOX" Value="1" />

    <CustomAction Id="LaunchApplication"
                  Directory="INSTALLFOLDER"
                  ExeCommand='[SystemFolder]wscript.exe "[INSTALLFOLDER]launch.vbs"'
                  Impersonate="yes"
                  Return="asyncNoWait" />

    <UI>
      <Publish Dialog="ExitDialog" Control="Finish" Event="DoAction"
               Value="LaunchApplication">
        WIXUI_EXITDIALOGOPTIONALCHECKBOX = 1 AND NOT Installed
      </Publish>
    </UI>

  </Product>
</Wix>"""


def build():
    """Build the MSI installer."""
    version = get_version()
    print(f"Building MSI for Backup Manager v{version}...")

    _patch_license_version(version)

    if not BUILD_DIR.exists():
        print(f"Error: {BUILD_DIR} not found. Run build_pyinstaller.py first.")
        sys.exit(1)

    # Create launch.vbs for post-install launch from MSI exit dialog
    launch_vbs = BUILD_DIR / "launch.vbs"
    launch_vbs.write_text(
        'Set WshShell = CreateObject("WScript.Shell")\n'
        'WshShell.Run """" & Replace(WScript.ScriptFullName, '
        '"launch.vbs", "BackupManager.exe") & """", 1, False\n',
        encoding="utf-8",
    )

    heat = str(WIX_BIN / "heat.exe")
    candle = str(WIX_BIN / "candle.exe")
    light = str(WIX_BIN / "light.exe")

    # Step 1: Harvest files with heat.exe
    heat_wxs = DIST / "HeatFiles.wxs"
    run(
        [
            heat,
            "dir",
            str(BUILD_DIR),
            "-cg",
            "ProductFiles",
            "-dr",
            "INSTALLFOLDER",
            "-srd",
            "-ke",
            "-gg",
            "-sfrag",
            "-sreg",
            "-var",
            "var.SourceDir",
            "-ag",
            "-template",
            "fragment",
            "-indent",
            "2",
            "-out",
            str(heat_wxs),
        ],
        "Harvesting files with heat.exe",
    )

    # Step 2: Write main product WXS
    main_wxs = DIST / "Product.wxs"
    main_wxs.write_text(_build_wxs(version), encoding="utf-8")

    # Step 3: Compile WXS files
    product_obj = DIST / "Product.wixobj"
    heat_obj = DIST / "HeatFiles.wixobj"

    run(
        [
            candle,
            str(main_wxs),
            "-o",
            str(product_obj),
            f"-dSourceDir={BUILD_DIR}",
        ],
        "Compiling Product.wxs",
    )

    run(
        [
            candle,
            str(heat_wxs),
            "-o",
            str(heat_obj),
            f"-dSourceDir={BUILD_DIR}",
        ],
        "Compiling HeatFiles.wxs",
    )

    # Step 4: Link into MSI
    msi_path = DIST / f"BackupManager-{version}.msi"
    run(
        [
            light,
            "-ext",
            "WixUIExtension",
            str(product_obj),
            str(heat_obj),
            "-o",
            str(msi_path),
            "-b",
            str(BUILD_DIR),
        ],
        "Linking MSI",
    )

    size_mb = msi_path.stat().st_size / (1024 * 1024)
    print("\nMSI build successful!")
    print(f"  Installer: {msi_path}")
    print(f"  Size: {size_mb:.1f} MB")
    print(f"  Version: {version}")


if __name__ == "__main__":
    build()
