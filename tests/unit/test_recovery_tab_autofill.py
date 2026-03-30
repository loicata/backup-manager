"""Tests for RecoveryTab auto-fill when switching storage type.

Verifies that S3 credentials are pre-filled from the profile when the
user selects S3 in the retrieve section, just like SFTP fields are.
"""

import tkinter as tk
from unittest.mock import patch

import pytest

from src.core.config import (
    BackupProfile,
    EncryptionConfig,
    StorageConfig,
    StorageType,
)
from src.ui.tabs.recovery_tab import RecoveryTab


@pytest.fixture(scope="module")
def tk_root():
    """Create a Tk root for the entire module, destroy after."""
    root = tk.Tk()
    root.withdraw()
    yield root
    root.destroy()


@pytest.fixture()
def recovery_tab(tk_root):
    """Create a fresh RecoveryTab for each test."""
    with patch("src.ui.tabs.recovery_tab.get_available_features", return_value={"sftp", "s3"}):
        tab = RecoveryTab(tk_root)
    yield tab
    tab.destroy()


def _make_profile(
    storage_type: StorageType = StorageType.SFTP,
    mirrors: list[StorageConfig] | None = None,
    **storage_kwargs,
) -> BackupProfile:
    """Build a minimal BackupProfile for testing.

    Args:
        storage_type: Type for the main storage config.
        mirrors: Optional list of mirror StorageConfigs.
        **storage_kwargs: Extra fields for the main StorageConfig.

    Returns:
        Configured BackupProfile.
    """
    storage = StorageConfig(storage_type=storage_type, **storage_kwargs)
    profile = BackupProfile.__new__(BackupProfile)
    profile.name = "test"
    profile.storage = storage
    profile.mirror_destinations = mirrors or []
    profile.encryption = EncryptionConfig()
    profile.sources = []
    profile.exclusions = []
    profile.schedule_frequency = "manual"
    profile.retention_policy = "gfs"
    profile.retention_days = 30
    profile.retention_max_backups = 10
    profile.retention_gfs_daily = 7
    profile.retention_gfs_weekly = 4
    profile.retention_gfs_monthly = 6
    return profile


class TestRetrieveAutoFillS3:
    """S3 fields should auto-fill from profile when switching to S3."""

    def test_switch_to_s3_fills_from_main_storage(self, recovery_tab):
        """Switching to S3 fills fields when main storage is S3."""
        profile = _make_profile(
            storage_type=StorageType.S3,
            s3_bucket="my-bucket",
            s3_region="eu-west-3",
            s3_access_key="AKID123",
            s3_secret_key="secret456",
            s3_endpoint_url="https://s3.example.com",
            s3_provider="scaleway",
        )
        recovery_tab.load_profile(profile)

        # Simulate user switching to S3 radio
        recovery_tab.retrieve_type_var.set(StorageType.S3.value)
        recovery_tab._on_retrieve_type_changed()

        assert recovery_tab._ret_s3_vars["s3_bucket"].get() == "my-bucket"
        assert recovery_tab._ret_s3_vars["s3_region"].get() == "eu-west-3"
        assert recovery_tab._ret_s3_vars["s3_access_key"].get() == "AKID123"
        assert recovery_tab._ret_s3_vars["s3_secret_key"].get() == "secret456"
        assert recovery_tab._ret_s3_vars["s3_endpoint_url"].get() == "https://s3.example.com"
        assert recovery_tab._ret_s3_provider_var.get() == "scaleway"

    def test_switch_to_s3_fills_from_mirror(self, recovery_tab):
        """Switching to S3 fills fields from a mirror when main storage is SFTP."""
        s3_mirror = StorageConfig(
            storage_type=StorageType.S3,
            s3_bucket="mirror-bucket",
            s3_region="us-east-1",
            s3_access_key="MIRROR_KEY",
            s3_secret_key="MIRROR_SECRET",
            s3_provider="aws",
        )
        profile = _make_profile(
            storage_type=StorageType.SFTP,
            sftp_host="server.example.com",
            sftp_username="user",
            mirrors=[s3_mirror],
        )
        recovery_tab.load_profile(profile)

        # Profile storage is SFTP, so SFTP is pre-selected.
        # User switches to S3 — should auto-fill from the mirror.
        recovery_tab.retrieve_type_var.set(StorageType.S3.value)
        recovery_tab._on_retrieve_type_changed()

        assert recovery_tab._ret_s3_vars["s3_bucket"].get() == "mirror-bucket"
        assert recovery_tab._ret_s3_vars["s3_access_key"].get() == "MIRROR_KEY"
        assert recovery_tab._ret_s3_vars["s3_secret_key"].get() == "MIRROR_SECRET"

    def test_switch_to_sftp_fills_from_profile(self, recovery_tab):
        """Switching to SFTP still auto-fills correctly."""
        profile = _make_profile(
            storage_type=StorageType.SFTP,
            sftp_host="backup.example.com",
            sftp_port=2222,
            sftp_username="admin",
            sftp_remote_path="/backups",
        )
        recovery_tab.load_profile(profile)

        # Switch away then back to SFTP
        recovery_tab.retrieve_type_var.set(StorageType.S3.value)
        recovery_tab._on_retrieve_type_changed()
        recovery_tab.retrieve_type_var.set(StorageType.SFTP.value)
        recovery_tab._on_retrieve_type_changed()

        assert recovery_tab._ret_sftp_vars["sftp_host"].get() == "backup.example.com"
        assert recovery_tab._ret_sftp_vars["sftp_port"].get() == "2222"
        assert recovery_tab._ret_sftp_vars["sftp_username"].get() == "admin"

    def test_no_matching_type_leaves_fields_empty(self, recovery_tab):
        """If no config matches the selected type, fields stay at defaults."""
        profile = _make_profile(storage_type=StorageType.LOCAL)
        recovery_tab.load_profile(profile)

        recovery_tab.retrieve_type_var.set(StorageType.S3.value)
        recovery_tab._on_retrieve_type_changed()

        # No S3 config in profile — fields should be empty/default
        assert recovery_tab._ret_s3_vars["s3_bucket"].get() == ""
        assert recovery_tab._ret_s3_vars["s3_access_key"].get() == ""

    def test_find_profile_config_priority(self, recovery_tab):
        """Main storage is preferred over mirrors when both match."""
        s3_mirror = StorageConfig(
            storage_type=StorageType.S3,
            s3_bucket="mirror-bucket",
        )
        profile = _make_profile(storage_type=StorageType.S3, s3_bucket="main-bucket")
        profile.mirror_destinations = [s3_mirror]
        recovery_tab.load_profile(profile)

        recovery_tab.retrieve_type_var.set(StorageType.S3.value)
        recovery_tab._on_retrieve_type_changed()

        assert recovery_tab._ret_s3_vars["s3_bucket"].get() == "main-bucket"

    def test_source_combo_mirror1_fills_s3_provider(self, recovery_tab):
        """Changing source to Mirror 1 (S3 Scaleway) fills correct provider."""
        s3_mirror = StorageConfig(
            storage_type=StorageType.S3,
            s3_bucket="backup-manager-cipango56",
            s3_region="fr-par",
            s3_access_key="SCWKEY",
            s3_secret_key="SCWSECRET",
            s3_provider="scaleway",
        )
        profile = _make_profile(
            storage_type=StorageType.SFTP,
            sftp_host="192.168.2.101",
            sftp_username="cipango56",
            mirrors=[s3_mirror],
        )
        recovery_tab.load_profile(profile)

        # Simulate user selecting "Mirror 1" in source combo
        recovery_tab.source_var.set("Mirror 1")
        recovery_tab._on_source_changed()

        assert recovery_tab.retrieve_type_var.get() == StorageType.S3.value
        assert recovery_tab._ret_s3_provider_var.get() == "scaleway"
        assert recovery_tab._ret_s3_vars["s3_bucket"].get() == "backup-manager-cipango56"
        assert recovery_tab._ret_s3_vars["s3_region"].get() == "fr-par"
        assert recovery_tab._ret_s3_vars["s3_access_key"].get() == "SCWKEY"
        assert recovery_tab._ret_s3_vars["s3_secret_key"].get() == "SCWSECRET"
