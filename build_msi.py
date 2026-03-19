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

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
DIST = ROOT / "dist"
BUILD_DIR = DIST / "BackupManager"
ASSETS = ROOT / "assets"

UPGRADE_CODE = "E8F2A1B3-4C5D-6E7F-8A9B-0C1D2E3F4A5B"
WIX_BIN = Path(r"C:\Program Files (x86)\WiX Toolset v3.14\bin")


def get_version() -> str:
    """Read version from src/__init__.py."""
    init = SRC / "__init__.py"
    for line in init.read_text(encoding="utf-8").splitlines():
        if line.startswith("__version__"):
            return line.split("=")[1].strip().strip('"').strip("'")
    return "3.0"


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
        license_line = (
            f'<WixVariable Id="WixUILicenseRtf" Value="{license_rtf}" />'
        )

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
           Manufacturer="Loic Ader"
           UpgradeCode="{UPGRADE_CODE}">

    <Package InstallerVersion="500"
             Compressed="yes"
             InstallScope="perMachine"
             Description="Backup Manager v{version}" />

    <MajorUpgrade DowngradeErrorMessage="A newer version is already installed."
                  AllowSameVersionUpgrades="yes" />

    <MediaTemplate EmbedCab="yes" CompressionLevel="high" />
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

    <!-- Features -->
    <Feature Id="Complete" Title="Backup Manager" Level="1">
      <ComponentGroupRef Id="ProductFiles" />
      <ComponentRef Id="C_StartMenuShortcut" />
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

    if not BUILD_DIR.exists():
        print(f"Error: {BUILD_DIR} not found. Run build_pyinstaller.py first.")
        sys.exit(1)

    heat = str(WIX_BIN / "heat.exe")
    candle = str(WIX_BIN / "candle.exe")
    light = str(WIX_BIN / "light.exe")

    # Step 1: Harvest files with heat.exe
    heat_wxs = DIST / "HeatFiles.wxs"
    run([
        heat, "dir", str(BUILD_DIR),
        "-cg", "ProductFiles",
        "-dr", "INSTALLFOLDER",
        "-srd",
        "-ke",
        "-gg",
        "-sfrag",
        "-sreg",
        "-var", "var.SourceDir",
        "-ag",
        "-template", "fragment",
        "-indent", "2",
        "-out", str(heat_wxs),
    ], "Harvesting files with heat.exe")

    # Step 2: Write main product WXS
    main_wxs = DIST / "Product.wxs"
    main_wxs.write_text(_build_wxs(version), encoding="utf-8")

    # Step 3: Compile WXS files
    product_obj = DIST / "Product.wixobj"
    heat_obj = DIST / "HeatFiles.wixobj"

    run([
        candle, str(main_wxs), "-o", str(product_obj),
        f"-dSourceDir={BUILD_DIR}",
    ], "Compiling Product.wxs")

    run([
        candle, str(heat_wxs), "-o", str(heat_obj),
        f"-dSourceDir={BUILD_DIR}",
    ], "Compiling HeatFiles.wxs")

    # Step 4: Link into MSI
    msi_path = DIST / f"BackupManager-{version}.msi"
    run([
        light,
        "-ext", "WixUIExtension",
        str(product_obj), str(heat_obj),
        "-o", str(msi_path),
        "-b", str(BUILD_DIR),
    ], "Linking MSI")

    size_mb = msi_path.stat().st_size / (1024 * 1024)
    print(f"\nMSI build successful!")
    print(f"  Installer: {msi_path}")
    print(f"  Size: {size_mb:.1f} MB")
    print(f"  Version: {version}")


if __name__ == "__main__":
    build()
