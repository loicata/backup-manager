"""Regression tests: switching storage type must not wipe the other
type's saved configuration (audit 2026-06-10, medium).

Before the fix, _build_storage_config started from a default
StorageConfig and populated only the selected type's branch — an
SFTP profile switched to S3 persisted every sftp_* field as empty,
and the second save rotated the loss into .bak (irreversible).
"""

from unittest.mock import patch

import pytest

from src.core.config import BackupProfile, StorageConfig, StorageType
from src.ui.tabs.storage_tab import StorageTab


@pytest.fixture()
def storage_tab(tk_root):
    """Fresh StorageTab with all backend features enabled."""
    with patch(
        "src.ui.tabs.storage_tab.get_available_features",
        return_value={"sftp", "s3"},
    ):
        tab = StorageTab(tk_root)
    yield tab
    tab.destroy()


def _sftp_profile() -> BackupProfile:
    profile = BackupProfile(name="SwitchMe")
    profile.storage = StorageConfig(
        storage_type=StorageType.SFTP,
        sftp_host="pi.example.com",
        sftp_port=2222,
        sftp_username="pi",
        sftp_password="secret",
        sftp_remote_path="/backups",
        sftp_key_path="C:/keys/id_ed25519",
        sftp_key_passphrase="phrase",
    )
    return profile


class TestTypeSwitchPreservesOtherConfig:
    def test_switch_sftp_to_s3_keeps_sftp_fields(self, storage_tab):
        storage_tab.load_profile(_sftp_profile())

        # User switches to S3 and fills in a valid bucket.
        storage_tab.type_var.set(StorageType.S3.value)
        storage_tab._s3_vars["s3_bucket"].set("my-bucket")
        storage_tab._s3_vars["s3_access_key"].set("AK")
        storage_tab._s3_vars["s3_secret_key"].set("SK")

        config = storage_tab._build_storage_config()

        assert config.storage_type == StorageType.S3
        assert config.s3_bucket == "my-bucket"
        # The SFTP block must survive the switch.
        assert config.sftp_host == "pi.example.com"
        assert config.sftp_port == 2222
        assert config.sftp_username == "pi"
        assert config.sftp_password == "secret"
        assert config.sftp_remote_path == "/backups"
        assert config.sftp_key_path == "C:/keys/id_ed25519"
        assert config.sftp_key_passphrase == "phrase"

    def test_switch_back_restores_sftp_without_retyping(self, storage_tab):
        """Round-trip: SFTP → S3 → save → reload → SFTP fields intact."""
        profile = _sftp_profile()
        storage_tab.load_profile(profile)
        storage_tab.type_var.set(StorageType.S3.value)
        storage_tab._s3_vars["s3_bucket"].set("bkt")

        profile.storage = storage_tab._build_storage_config()

        # Simulate reopening the tab on the saved-as-S3 profile and
        # switching back to SFTP without retyping anything.
        storage_tab.load_profile(profile)
        storage_tab.type_var.set(StorageType.SFTP.value)
        for key, var in storage_tab._sftp_vars.items():
            var.set(str(getattr(profile.storage, key)))

        back = storage_tab._build_storage_config()
        assert back.storage_type == StorageType.SFTP
        assert back.sftp_host == "pi.example.com"
        assert back.s3_bucket == "bkt"  # and S3 survives the reverse switch

    def test_selected_type_fields_still_come_from_ui(self, storage_tab):
        """Carry-over must not shadow live UI edits of the active type."""
        storage_tab.load_profile(_sftp_profile())
        storage_tab._sftp_vars["sftp_host"].set("new-host.example.com")

        config = storage_tab._build_storage_config()

        assert config.storage_type == StorageType.SFTP
        assert config.sftp_host == "new-host.example.com"

    def test_object_lock_fields_still_preserved(self, storage_tab):
        """The historical Object-Lock preservation keeps working."""
        profile = BackupProfile(name="OL")
        profile.storage = StorageConfig(
            storage_type=StorageType.S3,
            s3_bucket="locked",
            s3_object_lock=True,
            s3_object_lock_mode="COMPLIANCE",
            s3_object_lock_days=120,
        )
        storage_tab.load_profile(profile)
        storage_tab._s3_vars["s3_bucket"].set("locked")

        config = storage_tab._build_storage_config()

        assert config.s3_object_lock is True
        assert config.s3_object_lock_mode == "COMPLIANCE"
        assert config.s3_object_lock_days == 120

    def test_fresh_tab_without_loaded_profile_builds_default(self, storage_tab):
        """No load_profile yet → carry-over is skipped, no crash."""
        storage_tab.local_path_var.set("D:/Backups")

        config = storage_tab._build_storage_config()

        assert config.storage_type == StorageType.LOCAL
        assert config.destination_path == "D:/Backups"
        assert config.sftp_host == ""
