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

    def test_wxs_contains_startup_cleanup(self, build_msi):
        """Uninstall must remove BackupManager.vbs from Startup folder."""
        wxs = build_msi._build_wxs("1.0.0")
        assert "RemoveAutoStartVbs" in wxs
        assert 'Name="BackupManager.vbs"' in wxs
        assert 'On="uninstall"' in wxs

    def test_wxs_contains_startup_folder(self, build_msi):
        """WXS must reference the Windows Startup folder for cleanup."""
        wxs = build_msi._build_wxs("1.0.0")
        assert "StartupFolder" in wxs

    def test_wxs_contains_registry_cleanup(self, build_msi):
        """Uninstall must remove HKCU\\Software\\BackupManager registry key."""
        wxs = build_msi._build_wxs("1.0.0")
        assert "RemoveRegKey" in wxs
        assert 'Key="Software\\BackupManager"' in wxs
        assert 'Action="removeOnUninstall"' in wxs

    def test_wxs_cleanup_components_in_feature(self, build_msi):
        """Cleanup components must be referenced in the Complete feature."""
        wxs = build_msi._build_wxs("1.0.0")
        assert 'ComponentRef Id="C_CleanupAutoStart"' in wxs
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

    def test_wxs_major_upgrade_configured(self, build_msi):
        """MajorUpgrade must be configured for clean upgrades."""
        wxs = build_msi._build_wxs("1.0.0")
        assert "MajorUpgrade" in wxs
        assert "AllowSameVersionUpgrades" in wxs
