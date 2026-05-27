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
<Wix xmlns="http://schemas.microsoft.com/wix/2006/wi"
     xmlns:util="http://schemas.microsoft.com/wix/UtilExtension">
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

    <!--
      Close any running BackupManager.exe BEFORE replacing the binary
      (since 3.7.36). Without this, re-installing the MSI on top of a
      running instance left the old process holding the single-instance
      mutex in memory; the user's next launch of the new binary saw the
      mutex, wrote ``.show_signal`` and exited without raising the
      window (the old instance might be in the system tray, busy in a
      callback, or simply unresponsive — see v3.7.35 user report).

      ``CloseMessage="yes"`` sends WM_CLOSE to every top-level window
      of the target process so the app gets the chance to save state
      cleanly. ``TerminateProcess="10000"`` then force-kills any window
      that did not close within 10 s (e.g. a modal dialog that was
      waiting on user input).
    -->
    <util:CloseApplication Id="CloseBackupManagerExe"
                           Target="BackupManager.exe"
                           CloseMessage="yes"
                           PromptToContinue="no"
                           TerminateProcess="10000"
                           RebootPrompt="no"
                           EndSessionMessage="no" />

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

    <!--
      Defender exclusion for the install folder.

      PyInstaller-packaged Python applications are routinely flagged as
      false positives by Microsoft Defender heuristics (cloud lookup,
      ML model). Defender then silently quarantines the main .exe
      AFTER a successful MSI install — Programs &amp; Features still shows
      the app installed, but the binary is gone and the launcher fails
      with cryptic missing-file errors.

      We add the install path to ExclusionPath at install time and
      remove it at uninstall time (symmetry). Both actions are
      Return="ignore" so a missing/disabled Defender (other AV in use,
      Tamper Protection blocking) never breaks the install or uninstall.
      Execute="deferred" + Impersonate="no" runs them as SYSTEM, which
      is the only context that may modify Defender preferences.

      Scheduling: AddDefenderExclusion runs BEFORE InstallFiles so the
      copy of BackupManager.exe lands inside an already-excluded path —
      otherwise a real-time scan can fire between file creation and the
      exclusion taking effect, causing the same disappearance bug.
    -->
    <!--
      ``WixQuietExec64`` from WixUtilExtension launches the process via
      ``CreateProcessEx`` with ``CREATE_NO_WINDOW`` set, so no conhost
      is created and no window can flash on the desktop.

      The earlier ``-WindowStyle Hidden`` flag on a plain ExeCommand
      CustomAction was not enough on Windows 10/11: the console host
      (conhost.exe) is spawned by the OS BEFORE PowerShell has a chance
      to process ``-WindowStyle Hidden`` and hide it, producing a
      ~150 ms black rectangle that users perceived as the installer
      crashing.

      Deferred CustomActions cannot read installer properties directly,
      so we use the canonical pattern: an immediate "setter" CA
      populates ``WixQuietExec64CmdLine`` with the resolved command
      string (incl. the ``[INSTALLFOLDER]`` resolution), then the
      deferred CA reads that property and runs the command silently.
      ``Return="ignore"`` on the executor keeps a disabled / blocked
      Defender from breaking the install or uninstall.
    -->
    <CustomAction Id="SetAddDefenderCmd"
                  Property="WixQuietExec64CmdLine"
                  Value="powershell.exe -NoProfile -ExecutionPolicy Bypass -Command &quot;Add-MpPreference -ExclusionPath '[INSTALLFOLDER]' -ErrorAction SilentlyContinue&quot;"
                  Execute="immediate" />

    <CustomAction Id="AddDefenderExclusion"
                  BinaryKey="WixCA"
                  DllEntry="WixQuietExec64"
                  Execute="deferred"
                  Impersonate="no"
                  Return="ignore" />

    <CustomAction Id="SetRemoveDefenderCmd"
                  Property="WixQuietExec64CmdLine"
                  Value="powershell.exe -NoProfile -ExecutionPolicy Bypass -Command &quot;Remove-MpPreference -ExclusionPath '[INSTALLFOLDER]' -ErrorAction SilentlyContinue&quot;"
                  Execute="immediate" />

    <CustomAction Id="RemoveDefenderExclusion"
                  BinaryKey="WixCA"
                  DllEntry="WixQuietExec64"
                  Execute="deferred"
                  Impersonate="no"
                  Return="ignore" />

    <InstallExecuteSequence>
      <!-- Setter CAs must run BEFORE their paired deferred CA so the
           property value is in the execution script when the deferred
           CA fires. -->
      <Custom Action="SetAddDefenderCmd" Before="AddDefenderExclusion">NOT Installed AND NOT REMOVE</Custom>
      <!-- Add the exclusion before files land, so the install path is
           already trusted when BackupManager.exe is created. -->
      <Custom Action="AddDefenderExclusion" Before="InstallFiles">NOT Installed AND NOT REMOVE</Custom>
      <!-- v3.7.3 fix: skip the Remove CA when an upgrade is in flight.

           During a MajorUpgrade WiX runs RemoveExistingProducts which
           *uninstalls* the old product silently to make room for the
           new one. That uninstall has REMOVE="ALL" set, so without
           the NOT UPGRADINGPRODUCTCODE guard, the Defender exclusion
           added by the previous version was being silently removed
           — and the new version's AddDefenderExclusion did not
           reliably re-add it (Tamper Protection or timing races on
           Win10/11), leaving the install folder un-excluded.
           Symptom: 3.7.0+ first launch after upgrade froze for 5-10
           minutes while Defender real-time scanned the 800+ embedded
           data files of the Nuitka binary on every module load.
           UPGRADINGPRODUCTCODE is set ONLY during the upgrade-driven
           uninstall, never on a real user-initiated uninstall, so
           the Remove CA still fires on genuine uninstalls. -->
      <Custom Action="SetRemoveDefenderCmd" Before="RemoveDefenderExclusion">REMOVE="ALL" AND NOT UPGRADINGPRODUCTCODE</Custom>
      <!-- Symmetric cleanup on full uninstall only (not repair, not upgrade). -->
      <Custom Action="RemoveDefenderExclusion" Before="RemoveFiles">REMOVE="ALL" AND NOT UPGRADINGPRODUCTCODE</Custom>
    </InstallExecuteSequence>

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
            "-ext",
            "WixUtilExtension",
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
            "-ext",
            "WixUtilExtension",
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
