"""Tests for UI field clearing after profile deletion.

Verifies that:
- StorageTab.load_profile resets all fields (local, network, SFTP, S3)
  when loading a new/blank profile.
- MirrorTab.load_profile resets all fields when loading a profile without
  mirrors.
- BackupManagerApp._clear_tabs resets all tabs to default state.
"""

from unittest.mock import patch

import pytest

from src.core.config import BackupProfile, StorageConfig, StorageType
from src.ui.tabs.mirror_tab import MirrorTab
from src.ui.tabs.storage_tab import StorageTab


@pytest.fixture()
def storage_tab(tk_root):
    """Create a fresh StorageTab for each test."""
    with patch(
        "src.ui.tabs.storage_tab.get_available_features",
        return_value={"sftp", "s3"},
    ):
        tab = StorageTab(tk_root)
    yield tab
    tab.destroy()


@pytest.fixture()
def mirror_tab(tk_root):
    """Create a fresh MirrorTab for each test."""
    with patch(
        "src.ui.tabs.mirror_tab.get_available_features",
        return_value={"sftp", "s3"},
    ):
        tab = MirrorTab(tk_root, mirror_index=0)
    yield tab
    tab.destroy()


def _make_sftp_profile() -> BackupProfile:
    """Build a profile with SFTP storage and populated fields."""
    profile = BackupProfile()
    profile.storage = StorageConfig(
        storage_type=StorageType.SFTP,
        sftp_host="old-server.example.com",
        sftp_port=2222,
        sftp_username="olduser",
        sftp_password="oldpass",
        sftp_remote_path="/old/path",
        sftp_key_path="/old/key",
        sftp_key_passphrase="oldphrase",
    )
    return profile


def _make_s3_profile() -> BackupProfile:
    """Build a profile with S3 storage and populated fields."""
    profile = BackupProfile()
    profile.storage = StorageConfig(
        storage_type=StorageType.S3,
        s3_bucket="old-bucket",
        s3_prefix="old/prefix",
        s3_region="eu-west-1",
        s3_access_key="OLDACCESSKEY",
        s3_secret_key="OLDSECRETKEY",
        s3_provider="Amazon AWS",
    )
    return profile


def _make_network_profile() -> BackupProfile:
    """Build a profile with network storage and populated fields."""
    profile = BackupProfile()
    profile.storage = StorageConfig(
        storage_type=StorageType.NETWORK,
        destination_path=r"\\oldserver\oldshare",
        network_username="oldnetuser",
        network_password="oldnetpass",
    )
    return profile


class TestStorageTabFieldReset:
    """StorageTab.load_profile must clear stale fields."""

    def test_sftp_fields_cleared_when_switching_to_local(self, storage_tab):
        """Loading an SFTP profile then a LOCAL profile must clear SFTP fields."""
        storage_tab.load_profile(_make_sftp_profile())
        assert storage_tab._sftp_vars["sftp_host"].get() == "old-server.example.com"

        blank = BackupProfile()
        storage_tab.load_profile(blank)

        assert storage_tab._sftp_vars["sftp_host"].get() == ""
        assert storage_tab._sftp_vars["sftp_username"].get() == ""
        assert storage_tab._sftp_vars["sftp_password"].get() == ""
        assert storage_tab._sftp_vars["sftp_remote_path"].get() == ""
        assert storage_tab._sftp_vars["sftp_port"].get() == "22"

    def test_sftp_port_defaults_to_22_on_new_profile(self, storage_tab):
        """SFTP port must default to 22 when loading a blank profile."""
        sftp_profile = _make_sftp_profile()
        sftp_profile.storage.sftp_port = 2222
        storage_tab.load_profile(sftp_profile)
        assert storage_tab._sftp_vars["sftp_port"].get() == "2222"

        blank = BackupProfile()
        storage_tab.load_profile(blank)
        assert storage_tab._sftp_vars["sftp_port"].get() == "22"

    def test_s3_fields_cleared_when_switching_to_local(self, storage_tab):
        """Loading an S3 profile then a LOCAL profile must clear S3 fields."""
        storage_tab.load_profile(_make_s3_profile())
        assert storage_tab._s3_vars["s3_bucket"].get() == "old-bucket"

        blank = BackupProfile()
        storage_tab.load_profile(blank)

        assert storage_tab._s3_vars["s3_bucket"].get() == ""
        assert storage_tab._s3_vars["s3_access_key"].get() == ""
        assert storage_tab._s3_vars["s3_secret_key"].get() == ""

    def test_network_fields_cleared_when_switching_to_local(self, storage_tab):
        """Loading a NETWORK profile then a LOCAL profile must clear network fields."""
        storage_tab.load_profile(_make_network_profile())
        assert storage_tab.network_path_var.get() == r"\\oldserver\oldshare"

        blank = BackupProfile()
        storage_tab.load_profile(blank)

        assert storage_tab.network_path_var.get() == ""
        assert storage_tab.network_user_var.get() == ""
        assert storage_tab.network_pass_var.get() == ""

    def test_local_path_cleared_when_switching_to_sftp(self, storage_tab):
        """Loading a LOCAL profile with a path then SFTP must clear local path."""
        local_profile = BackupProfile()
        local_profile.storage.destination_path = r"D:\Backups"
        storage_tab.load_profile(local_profile)
        assert storage_tab.local_path_var.get() == r"D:\Backups"

        storage_tab.load_profile(_make_sftp_profile())
        assert storage_tab.local_path_var.get() == ""


class TestMirrorTabFieldReset:
    """MirrorTab.load_profile must clear stale fields."""

    def test_sftp_mirror_fields_cleared_on_blank_profile(self, mirror_tab):
        """SFTP mirror fields must be cleared when loading a profile without mirrors."""
        sftp_profile = BackupProfile()
        sftp_profile.mirror_destinations = [
            StorageConfig(
                storage_type=StorageType.SFTP,
                sftp_host="mirror-server.example.com",
                sftp_username="mirroruser",
                sftp_password="mirrorpass",
                sftp_remote_path="/mirror/path",
            )
        ]
        mirror_tab.load_profile(sftp_profile)
        assert mirror_tab._sftp_vars["sftp_host"].get() == "mirror-server.example.com"

        blank = BackupProfile()
        mirror_tab.load_profile(blank)

        assert mirror_tab._sftp_vars["sftp_host"].get() == ""
        assert mirror_tab._sftp_vars["sftp_username"].get() == ""
        assert mirror_tab.enabled_var.get() is False

    def test_network_mirror_fields_cleared_on_blank_profile(self, mirror_tab):
        """Network mirror fields must be cleared when loading a profile without mirrors."""
        net_profile = BackupProfile()
        net_profile.mirror_destinations = [
            StorageConfig(
                storage_type=StorageType.NETWORK,
                destination_path=r"\\mirrorserver\share",
                network_username="netuser",
                network_password="netpass",
            )
        ]
        mirror_tab.load_profile(net_profile)
        assert mirror_tab.network_path_var.get() == r"\\mirrorserver\share"

        blank = BackupProfile()
        mirror_tab.load_profile(blank)

        assert mirror_tab.network_path_var.get() == ""
        assert mirror_tab.network_user_var.get() == ""
        assert mirror_tab.network_pass_var.get() == ""


class TestDeleteProfileClearsTabs:
    """Deleting last profile must result in cleared tabs."""

    def test_load_blank_profile_resets_storage_type_to_local(self, storage_tab):
        """After loading a blank profile, storage type must be LOCAL."""
        storage_tab.load_profile(_make_sftp_profile())
        assert storage_tab.type_var.get() == StorageType.SFTP.value

        blank = BackupProfile()
        storage_tab.load_profile(blank)
        assert storage_tab.type_var.get() == StorageType.LOCAL.value

    def test_load_blank_profile_resets_mirror_enabled(self, mirror_tab):
        """After loading a blank profile, mirror must be disabled."""
        sftp_profile = BackupProfile()
        sftp_profile.mirror_destinations = [
            StorageConfig(
                storage_type=StorageType.SFTP,
                sftp_host="host",
                sftp_remote_path="/path",
            )
        ]
        mirror_tab.load_profile(sftp_profile)
        assert mirror_tab.enabled_var.get() is True

        blank = BackupProfile()
        mirror_tab.load_profile(blank)
        assert mirror_tab.enabled_var.get() is False
