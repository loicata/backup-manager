"""Tests for build_msi.py — MSI WXS generation."""

import importlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def build_msi():
    """Import build_msi module."""
    spec = importlib.util.spec_from_file_location("build_msi", ROOT / "build_msi.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestBuildWxs:
    """Verify the generated WXS contains required cleanup components."""

    def test_wxs_contains_registry_cleanup(self, build_msi):
        """Uninstall must remove HKCU\\Software\\BackupManager registry key."""
        wxs = build_msi._build_wxs("1.0.0")
        assert "RemoveRegKey" in wxs
        assert 'Key="Software\\BackupManager"' in wxs
        assert 'Action="removeOnUninstall"' in wxs

    def test_wxs_removes_autostart_run_key(self, build_msi):
        """Uninstall must remove auto-start entry from HKCU\\...\\Run."""
        wxs = build_msi._build_wxs("1.0.0")
        assert "RemoveAutoStartRun" in wxs
        assert "CurrentVersion\\Run" in wxs
        assert 'Name="BackupManager"' in wxs

    def test_wxs_no_legacy_vbs_references(self, build_msi):
        """WXS must not contain legacy VBS/StartupFolder references."""
        wxs = build_msi._build_wxs("1.0.0")
        assert "StartupFolder" not in wxs
        assert "BackupManager.vbs" not in wxs
        assert "CA_RemoveStartupVbs" not in wxs

    def test_wxs_cleanup_components_in_feature(self, build_msi):
        """Cleanup components must be referenced in the Complete feature."""
        wxs = build_msi._build_wxs("1.0.0")
        assert 'ComponentRef Id="C_CleanupRegistry"' in wxs

    def test_wxs_contains_start_menu_cleanup(self, build_msi):
        """Uninstall must remove Start Menu folder."""
        wxs = build_msi._build_wxs("1.0.0")
        assert 'RemoveFolder Id="RemoveStartMenu"' in wxs
        assert 'On="uninstall"' in wxs

    def test_wxs_version_substitution(self, build_msi):
        """Version string must appear in Product and Package elements."""
        wxs = build_msi._build_wxs("3.2.1")
        assert 'Version="3.2.1.0"' in wxs
        assert "Backup Manager v3.2.1" in wxs

    def test_wxs_launch_action_only_on_fresh_install(self, build_msi):
        """LaunchApplication must only fire on first install, not upgrades."""
        wxs = build_msi._build_wxs("1.0.0")
        assert "NOT Installed" in wxs

    def test_wxs_no_custom_action_for_vbs(self, build_msi):
        """No CustomAction for VBS cleanup should exist (registry is used)."""
        wxs = build_msi._build_wxs("1.0.0")
        assert "mshta" not in wxs
        assert "CA_RemoveStartupVbs" not in wxs

    def test_wxs_major_upgrade_configured(self, build_msi):
        """MajorUpgrade must be configured for clean upgrades."""
        wxs = build_msi._build_wxs("1.0.0")
        assert "MajorUpgrade" in wxs
        assert "AllowSameVersionUpgrades" in wxs


class TestDefenderExclusion:
    """Verify the WXS auto-excludes the install folder from Microsoft Defender.

    Background: PyInstaller-packaged binaries trigger Defender false
    positives. After a successful install, Defender silently quarantines
    BackupManager.exe — Programs & Features still shows the app installed
    but the launcher fails with cryptic missing-file errors. Adding the
    install path to the Defender ExclusionPath list at install time avoids
    the heuristic scan; we remove it on full uninstall by symmetry.
    """

    def _add_block(self, wxs: str) -> str:
        """Slice of the WXS containing the AddDefenderExclusion CustomAction.

        The slice is delimited by the next CustomAction so attribute
        assertions don't accidentally match the Remove counterpart.
        """
        start = wxs.index('Id="AddDefenderExclusion"')
        end = wxs.index('Id="RemoveDefenderExclusion"')
        return wxs[start:end]

    def _remove_block(self, wxs: str) -> str:
        """Slice for the RemoveDefenderExclusion CustomAction."""
        start = wxs.index('Id="RemoveDefenderExclusion"')
        end = wxs.index("<InstallExecuteSequence>")
        return wxs[start:end]

    def test_wxs_contains_add_defender_exclusion(self, build_msi):
        """Install must add INSTALLFOLDER to Defender exclusions."""
        wxs = build_msi._build_wxs("1.0.0")
        assert 'Id="AddDefenderExclusion"' in wxs
        assert "Add-MpPreference" in wxs
        assert "ExclusionPath" in wxs
        assert "[INSTALLFOLDER]" in wxs

    def test_wxs_contains_remove_defender_exclusion(self, build_msi):
        """Full uninstall must remove the exclusion (symmetry)."""
        wxs = build_msi._build_wxs("1.0.0")
        assert 'Id="RemoveDefenderExclusion"' in wxs
        assert "Remove-MpPreference" in wxs

    def test_add_exclusion_runs_before_install_files(self, build_msi):
        """The exclusion must be in place BEFORE BackupManager.exe lands.

        If we scheduled it after InstallFiles, Defender's real-time scan
        would fire on file creation and could quarantine the binary
        before the exclusion list updates — exactly the bug we are
        defending against.
        """
        wxs = build_msi._build_wxs("1.0.0")
        import re

        match = re.search(
            r'<Custom\s+Action="AddDefenderExclusion"[^>]*Before="([^"]+)"',
            wxs,
        )
        assert match is not None, "AddDefenderExclusion must have a Before= anchor"
        assert match.group(1) == "InstallFiles"

    def test_add_exclusion_only_on_fresh_install(self, build_msi):
        """Don't re-trigger PowerShell on upgrade or repair."""
        wxs = build_msi._build_wxs("1.0.0")
        import re

        match = re.search(
            r'<Custom Action="AddDefenderExclusion"[^>]*>([^<]+)</Custom>',
            wxs,
        )
        assert match is not None, "AddDefenderExclusion <Custom> not found"
        condition = match.group(1)
        assert "NOT Installed" in condition
        assert "NOT REMOVE" in condition

    def test_remove_exclusion_only_on_full_uninstall(self, build_msi):
        """Repair must not delete the exclusion (REMOVE != ALL)."""
        wxs = build_msi._build_wxs("1.0.0")
        import re

        match = re.search(
            r'<Custom Action="RemoveDefenderExclusion"[^>]*>([^<]+)</Custom>',
            wxs,
        )
        assert match is not None, "RemoveDefenderExclusion <Custom> not found"
        condition = match.group(1)
        assert 'REMOVE="ALL"' in condition

    def test_defender_actions_run_as_system(self, build_msi):
        """Add/Remove-MpPreference need elevation.

        Deferred + Impersonate="no" runs the action under the SYSTEM
        account from the elevated MSI context — the only context that
        can modify Defender preferences (user accounts cannot, even with
        admin token, when Tamper Protection is on).
        """
        wxs = build_msi._build_wxs("1.0.0")

        for block in (self._add_block(wxs), self._remove_block(wxs)):
            assert 'Execute="deferred"' in block
            assert 'Impersonate="no"' in block

    def test_defender_actions_never_fail_install(self, build_msi):
        """Both actions must use Return="ignore" so they never fail.

        Real-world failure modes we silently swallow:
        - PowerShell missing or blocked by AppLocker
        - Defender disabled (third-party AV in use)
        - Tamper Protection blocking the modification
        """
        wxs = build_msi._build_wxs("1.0.0")

        for block in (self._add_block(wxs), self._remove_block(wxs)):
            assert 'Return="ignore"' in block

    def test_defender_actions_use_silentlycontinue(self, build_msi):
        """PowerShell command must swallow runtime errors too.

        Return="ignore" only handles the wrapper exit code. The
        PowerShell -Command itself must use -ErrorAction
        SilentlyContinue so a Defender API error doesn't surface as a
        non-zero exit and pollute the MSI log with red lines.
        """
        wxs = build_msi._build_wxs("1.0.0")

        for block in (self._add_block(wxs), self._remove_block(wxs)):
            assert "SilentlyContinue" in block
