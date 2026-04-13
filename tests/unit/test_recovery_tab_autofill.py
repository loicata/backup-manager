"""Tests for RecoveryTab: auto-fill, visibility, selection, helpers, S3 scan.

Covers the unified recovery tab with 4 source types: External drive,
Network folder, SFTP, S3 cloud.
"""

import time
from unittest.mock import patch

import pytest

from src.core.config import (
    BackupProfile,
    EncryptionConfig,
    StorageConfig,
    StorageType,
)
from src.ui.tabs.recovery_tab import (
    RecoveryTab,
    _format_date,
    _human_size,
    _is_backup_object,
    _is_encrypted_name,
    _parse_backup_type,
)

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture()
def recovery_tab(tk_root):
    """Create a fresh RecoveryTab packed into the root window."""
    with patch("src.ui.tabs.recovery_tab.get_available_features", return_value={"sftp", "s3"}):
        tab = RecoveryTab(tk_root)
    tab.pack(fill="both", expand=True)
    tk_root.update_idletasks()
    yield tab
    tab.destroy()


def _make_config(**kwargs) -> StorageConfig:
    """Build a StorageConfig bypassing __post_init__ validation.

    Args:
        **kwargs: Fields to set on the config.

    Returns:
        Configured StorageConfig.
    """
    config = StorageConfig.__new__(StorageConfig)
    config.storage_type = StorageType.LOCAL
    config.destination_path = ""
    config.device_serial = ""
    config.network_username = ""
    config.network_password = ""
    config.sftp_host = ""
    config.sftp_port = 22
    config.sftp_username = ""
    config.sftp_password = ""
    config.sftp_key_path = ""
    config.sftp_key_passphrase = ""
    config.sftp_remote_path = ""
    config.s3_bucket = ""
    config.s3_prefix = ""
    config.s3_region = "eu-west-1"
    config.s3_access_key = ""
    config.s3_secret_key = ""
    config.s3_endpoint_url = ""
    config.s3_provider = "Amazon AWS"
    config.s3_object_lock = False
    config.s3_object_lock_mode = "COMPLIANCE"
    config.s3_object_lock_days = 30
    config.s3_object_lock_full_extra_days = 30
    config.s3_speedtest_bucket = ""
    config.mirror_encrypt = False
    for key, val in kwargs.items():
        setattr(config, key, val)
    return config


def _make_profile(
    storage_type: StorageType = StorageType.LOCAL,
    mirrors: list[StorageConfig] | None = None,
    stored_password: str = "",
    **storage_kwargs,
) -> BackupProfile:
    """Build a minimal BackupProfile for testing.

    Args:
        storage_type: Type for the main storage config.
        mirrors: Optional list of mirror StorageConfigs.
        stored_password: Encryption password stored in profile.
        **storage_kwargs: Extra fields for the main StorageConfig.

    Returns:
        Configured BackupProfile.
    """
    storage = _make_config(storage_type=storage_type, **storage_kwargs)
    profile = BackupProfile.__new__(BackupProfile)
    profile.name = "test"
    profile.storage = storage
    profile.mirror_destinations = mirrors or []
    profile.encryption = EncryptionConfig(stored_password=stored_password)
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


# ------------------------------------------------------------------
# Helper function tests
# ------------------------------------------------------------------


class TestParseBackupType:
    """Tests for _parse_backup_type."""

    def test_full_detected(self):
        assert _parse_backup_type("loicata_FULL_2026-04-10_120000") == "FULL"

    def test_diff_detected(self):
        assert _parse_backup_type("loicata_DIFF_2026-04-09_080000") == "DIFF"

    def test_full_at_start(self):
        assert _parse_backup_type("FULL_2026-04-10") == "FULL"

    def test_diff_at_start(self):
        assert _parse_backup_type("DIFF_2026-04-10") == "DIFF"

    def test_unknown_type(self):
        assert _parse_backup_type("some_random_backup") == ""

    def test_case_insensitive(self):
        assert _parse_backup_type("loicata_full_2026-04-10") == "FULL"
        assert _parse_backup_type("loicata_Diff_2026-04-10") == "DIFF"


class TestIsEncryptedName:
    """Tests for _is_encrypted_name."""

    def test_wbenc_extension(self):
        assert _is_encrypted_name("backup_FULL_2026-04-10.tar.wbenc") is True

    def test_no_encryption(self):
        assert _is_encrypted_name("backup_FULL_2026-04-10") is False

    def test_case_insensitive(self):
        assert _is_encrypted_name("backup.tar.WBENC") is True


class TestIsBackupObject:
    """Tests for _is_backup_object."""

    def test_full_backup(self):
        assert _is_backup_object("loicata_FULL_2026-04-10/file.txt") is True

    def test_diff_backup(self):
        assert _is_backup_object("loicata_DIFF_2026-04-09.tar.wbenc") is True

    def test_wbverify(self):
        assert _is_backup_object("loicata_FULL_2026-04-10.wbverify") is True

    def test_random_file(self):
        assert _is_backup_object("photos/vacation.jpg") is False

    def test_case_insensitive(self):
        assert _is_backup_object("loicata_full_2026-04-10/data") is True


class TestHumanSize:
    """Tests for _human_size."""

    def test_bytes(self):
        assert _human_size(500) == "500 B"

    def test_kilobytes(self):
        assert _human_size(1024) == "1.0 KB"

    def test_megabytes(self):
        assert "MB" in _human_size(350 * 1024 * 1024)

    def test_gigabytes(self):
        assert "GB" in _human_size(2 * 1024 * 1024 * 1024)

    def test_negative(self):
        assert _human_size(-1) == "?"

    def test_zero(self):
        assert _human_size(0) == "0 B"


class TestFormatDate:
    """Tests for _format_date."""

    def test_valid_timestamp(self):
        ts = 1775952000.0
        result = _format_date(ts)
        assert "/" in result and "2026" in result

    def test_zero(self):
        assert _format_date(0) == ""

    def test_negative(self):
        assert _format_date(-1) == ""


# ------------------------------------------------------------------
# Source type switching
# ------------------------------------------------------------------


class TestSourceTypeSwitching:
    """Tests for storage type radio button switching."""

    def test_default_type_is_local(self, recovery_tab):
        assert recovery_tab.source_type_var.get() == StorageType.LOCAL.value

    def test_switch_to_sftp_shows_list_button(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.SFTP.value)
        recovery_tab.winfo_toplevel().update_idletasks()
        assert recovery_tab._list_btn.winfo_ismapped()

    def test_switch_to_s3_shows_scan_button(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.S3.value)
        recovery_tab.winfo_toplevel().update_idletasks()
        assert "Scan" in recovery_tab._list_btn.cget("text")

    def test_scan_button_accent_style(self, recovery_tab):
        assert recovery_tab._list_btn.cget("style") == "Accent.TButton"

    def test_switch_to_local_hides_list_button(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.SFTP.value)
        recovery_tab.source_type_var.set(StorageType.LOCAL.value)
        recovery_tab.winfo_toplevel().update_idletasks()
        assert not recovery_tab._list_btn.winfo_ismapped()

    def test_switch_to_network_hides_list_button(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.NETWORK.value)
        recovery_tab.winfo_toplevel().update_idletasks()
        assert not recovery_tab._list_btn.winfo_ismapped()

    def test_switch_type_clears_listing(self, recovery_tab):
        recovery_tab._listed_backups = [{"name": "test", "size": 1, "modified": 1}]
        recovery_tab._selected_backups = {"test"}
        recovery_tab.source_type_var.set(StorageType.LOCAL.value)
        assert recovery_tab._listed_backups == []
        assert recovery_tab._selected_backups == set()


class TestS3ProviderRegionLabel:
    """Tests for Region label changing based on S3 provider."""

    def test_amazon_region_optional(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.S3.value)
        recovery_tab._ret_s3_provider_var.set("Amazon AWS")
        assert "optional" in recovery_tab._s3_region_label.cget("text").lower()

    def test_scaleway_region_required(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.S3.value)
        recovery_tab._ret_s3_provider_var.set("scaleway")
        label = recovery_tab._s3_region_label.cget("text")
        assert "Region:" in label
        assert "optional" not in label.lower()

    def test_wasabi_region_required(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.S3.value)
        recovery_tab._ret_s3_provider_var.set("wasabi")
        assert "optional" not in recovery_tab._s3_region_label.cget("text").lower()

    def test_other_region_required(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.S3.value)
        recovery_tab._ret_s3_provider_var.set("other")
        assert "optional" not in recovery_tab._s3_region_label.cget("text").lower()

    def test_switch_back_to_amazon(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.S3.value)
        recovery_tab._ret_s3_provider_var.set("scaleway")
        recovery_tab._ret_s3_provider_var.set("Amazon AWS")
        assert "optional" in recovery_tab._s3_region_label.cget("text").lower()


# ------------------------------------------------------------------
# Auto-fill from profile
# ------------------------------------------------------------------


class TestAutoFill:
    """Tests for auto-fill from profile."""

    def test_load_profile_fills_s3_scaleway(self, recovery_tab):
        """Non-Amazon S3: all fields pre-filled including bucket/region."""
        profile = _make_profile(
            storage_type=StorageType.S3,
            s3_access_key="AKID123",
            s3_secret_key="secret456",
            s3_provider="scaleway",
            s3_bucket="my-bucket",
            s3_region="fr-par",
        )
        recovery_tab.load_profile(profile)
        assert recovery_tab.source_type_var.get() == StorageType.S3.value
        assert recovery_tab._ret_s3_vars["s3_access_key"].get() == "AKID123"
        assert recovery_tab._ret_s3_provider_var.get() == "scaleway"
        assert recovery_tab._ret_s3_vars["s3_bucket"].get() == "my-bucket"
        assert recovery_tab._ret_s3_vars["s3_region"].get() == "fr-par"

    def test_load_profile_fills_s3_amazon_no_bucket(self, recovery_tab):
        """Amazon AWS: bucket/prefix/region NOT pre-filled to encourage scan."""
        profile = _make_profile(
            storage_type=StorageType.S3,
            s3_access_key="AKID123",
            s3_secret_key="secret456",
            s3_provider="Amazon AWS",
            s3_bucket="some-bucket",
            s3_region="eu-west-1",
            s3_prefix="backups/",
        )
        recovery_tab.load_profile(profile)
        assert recovery_tab._ret_s3_vars["s3_access_key"].get() == "AKID123"
        assert recovery_tab._ret_s3_vars["s3_bucket"].get() == ""
        assert recovery_tab._ret_s3_vars["s3_prefix"].get() == ""
        assert recovery_tab._ret_s3_vars["s3_region"].get() == ""

    def test_load_profile_fills_sftp(self, recovery_tab):
        profile = _make_profile(
            storage_type=StorageType.SFTP,
            sftp_host="backup.example.com",
            sftp_port=2222,
            sftp_username="admin",
        )
        recovery_tab.load_profile(profile)
        assert recovery_tab.source_type_var.get() == StorageType.SFTP.value
        assert recovery_tab._ret_sftp_vars["sftp_host"].get() == "backup.example.com"
        assert recovery_tab._ret_sftp_vars["sftp_port"].get() == "2222"

    def test_load_profile_fills_local(self, recovery_tab):
        profile = _make_profile(
            storage_type=StorageType.LOCAL,
            destination_path="D:\\Backups",
        )
        recovery_tab.load_profile(profile)
        assert recovery_tab.source_type_var.get() == StorageType.LOCAL.value
        assert recovery_tab.backup_path_var.get() == "D:\\Backups"

    def test_load_profile_fills_network(self, recovery_tab):
        profile = _make_profile(
            storage_type=StorageType.NETWORK,
            destination_path=r"\\server\backups",
            network_username="admin",
            network_password="pass123",
        )
        recovery_tab.load_profile(profile)
        assert recovery_tab.source_type_var.get() == StorageType.NETWORK.value
        assert recovery_tab._net_path_var.get() == r"\\server\backups"
        assert recovery_tab._net_user_var.get() == "admin"

    def test_combo_mirror1_fills_s3(self, recovery_tab):
        s3_mirror = _make_config(
            storage_type=StorageType.S3,
            s3_access_key="MIRROR_KEY",
            s3_secret_key="MIRROR_SECRET",
            s3_provider="wasabi",
        )
        profile = _make_profile(
            storage_type=StorageType.SFTP,
            sftp_host="192.168.2.101",
            mirrors=[s3_mirror],
        )
        recovery_tab.load_profile(profile)

        recovery_tab.source_var.set("Mirror 1")
        recovery_tab._on_source_changed()

        assert recovery_tab.source_type_var.get() == StorageType.S3.value
        assert recovery_tab._ret_s3_vars["s3_access_key"].get() == "MIRROR_KEY"
        assert recovery_tab._ret_s3_provider_var.get() == "wasabi"

    def test_manual_type_change_does_not_autofill(self, recovery_tab):
        """Manually switching storage type should NOT overwrite fields."""
        profile = _make_profile(
            storage_type=StorageType.S3,
            s3_access_key="PROFILE_KEY",
            s3_provider="scaleway",
        )
        recovery_tab.load_profile(profile)

        # User manually switches to SFTP and fills host
        recovery_tab.source_type_var.set(StorageType.SFTP.value)
        recovery_tab._ret_sftp_vars["sftp_host"].set("my-manual-host")

        # Switch back to S3 — fields should NOT be re-filled from profile
        recovery_tab.source_type_var.set(StorageType.S3.value)
        # The access key should still be what profile set, not overwritten
        # But more importantly, the switch itself should not trigger auto-fill
        # (we verify by checking that no _filling flag caused overwrites)
        assert recovery_tab._ret_sftp_vars["sftp_host"].get() == "my-manual-host"

    def test_reload_same_profile_preserves_user_edits(self, recovery_tab):
        """Reloading the same profile (silent save) should not reset fields."""
        profile = _make_profile(
            storage_type=StorageType.S3,
            s3_access_key="PROFILE_KEY",
            s3_provider="Amazon AWS",
        )
        profile.id = "test-profile-123"
        recovery_tab.load_profile(profile)

        # User manually changes provider to scaleway
        recovery_tab._ret_s3_provider_var.set("scaleway")

        # Reload same profile (simulates silent save + reload)
        recovery_tab.load_profile(profile)

        # Provider should still be scaleway, not reset to Amazon AWS
        assert recovery_tab._ret_s3_provider_var.get() == "scaleway"

    def test_load_different_profile_resets_fields(self, recovery_tab):
        """Loading a different profile should reset all fields."""
        profile1 = _make_profile(
            storage_type=StorageType.S3,
            s3_access_key="KEY1",
            s3_provider="Amazon AWS",
        )
        profile1.id = "profile-1"
        recovery_tab.load_profile(profile1)
        recovery_tab._ret_s3_provider_var.set("scaleway")

        profile2 = _make_profile(
            storage_type=StorageType.SFTP,
            sftp_host="new-host.com",
        )
        profile2.id = "profile-2"
        recovery_tab.load_profile(profile2)

        assert recovery_tab.source_type_var.get() == StorageType.SFTP.value
        assert recovery_tab._ret_sftp_vars["sftp_host"].get() == "new-host.com"

    def test_autofill_frame_hidden_without_profile(self, recovery_tab):
        recovery_tab.load_no_profile()
        recovery_tab.winfo_toplevel().update_idletasks()
        assert not recovery_tab._autofill_frame.winfo_ismapped()

    def test_autofill_frame_visible_with_profile(self, recovery_tab):
        profile = _make_profile()
        recovery_tab.load_profile(profile)
        recovery_tab.winfo_toplevel().update_idletasks()
        assert recovery_tab._autofill_frame.winfo_ismapped()


# ------------------------------------------------------------------
# Treeview selection
# ------------------------------------------------------------------


class TestTreeviewSelection:
    """Tests for backup selection in the treeview."""

    def _populate(self, tab, backups, grouped=False):
        tab._listed_backups = backups
        tab._selected_backups.clear()
        tab._populate_tree(grouped=grouped)

    def test_select_all(self, recovery_tab):
        self._populate(
            recovery_tab,
            [
                {"name": "a_FULL", "size": 1000, "modified": 1.0},
                {"name": "b_DIFF", "size": 500, "modified": 0.5},
            ],
        )
        recovery_tab._select_all()
        assert len(recovery_tab._selected_backups) == 2

    def test_select_none(self, recovery_tab):
        self._populate(
            recovery_tab,
            [
                {"name": "a_FULL", "size": 1000, "modified": 1.0},
            ],
        )
        recovery_tab._select_all()
        recovery_tab._select_none()
        assert len(recovery_tab._selected_backups) == 0

    def test_summary_no_backups(self, recovery_tab):
        recovery_tab._listed_backups = []
        recovery_tab._selected_backups = set()
        recovery_tab._update_selection_summary()
        assert "No backups found" in recovery_tab._selection_summary.cget("text")

    def test_summary_with_selection(self, recovery_tab):
        self._populate(
            recovery_tab,
            [
                {"name": "a_FULL", "size": 2_000_000_000, "modified": 1.0},
            ],
        )
        recovery_tab._selected_backups.add("a_FULL")
        recovery_tab._update_selection_summary()
        text = recovery_tab._selection_summary.cget("text")
        assert "1 backup" in text and "GB" in text

    def test_sort_by_date_descending(self, recovery_tab):
        self._populate(
            recovery_tab,
            [
                {"name": "old_FULL", "size": 100, "modified": 1000.0},
                {"name": "new_FULL", "size": 200, "modified": 9000.0},
                {"name": "mid_DIFF", "size": 150, "modified": 5000.0},
            ],
        )
        items = recovery_tab._tree.get_children()
        assert items[0] == "new_FULL"
        assert items[2] == "old_FULL"

    def test_grouped_by_bucket(self, recovery_tab):
        backups = [
            {"name": "a_FULL", "size": 100, "modified": 2.0, "_bucket": "bucket-1"},
            {"name": "b_DIFF", "size": 50, "modified": 1.0, "_bucket": "bucket-2"},
        ]
        self._populate(recovery_tab, backups, grouped=True)

        top_items = recovery_tab._tree.get_children()
        assert any("bucket-1" in str(i) for i in top_items)
        assert any("bucket-2" in str(i) for i in top_items)

    def test_bucket_headers_not_selectable(self, recovery_tab):
        backups = [
            {"name": "a_FULL", "size": 100, "modified": 1.0, "_bucket": "bucket-1"},
        ]
        self._populate(recovery_tab, backups, grouped=True)
        recovery_tab._select_all()
        assert "a_FULL" in recovery_tab._selected_backups
        assert not any(s.startswith("_bucket_") for s in recovery_tab._selected_backups)

    def test_encrypted_column_shown(self, recovery_tab):
        """Encrypted column shows 'Yes' for encrypted backups."""
        backups = [
            {"name": "enc_FULL", "size": 100, "modified": 2.0, "encrypted": True},
            {"name": "plain_FULL", "size": 100, "modified": 1.0, "encrypted": False},
        ]
        self._populate(recovery_tab, backups)
        enc_values = recovery_tab._tree.item("enc_FULL", "values")
        plain_values = recovery_tab._tree.item("plain_FULL", "values")
        assert enc_values[1] == "Yes"
        assert plain_values[1] == ""


# ------------------------------------------------------------------
# Password visibility
# ------------------------------------------------------------------


class TestPasswordVisibility:
    """Tests for conditional password section display."""

    def test_hidden_for_plain_local(self, recovery_tab, tmp_path):
        src = tmp_path / "plain_backup"
        src.mkdir()
        (src / "file.txt").write_text("data")

        recovery_tab.source_type_var.set(StorageType.LOCAL.value)
        recovery_tab.backup_path_var.set(str(src))
        recovery_tab.winfo_toplevel().update_idletasks()
        assert not recovery_tab._pw_frame.winfo_ismapped()

    def test_shown_for_encrypted_local(self, recovery_tab, tmp_path):
        src = tmp_path / "enc_backup"
        src.mkdir()
        (src / "archive.tar.wbenc").write_bytes(b"\x00" * 100)

        profile = _make_profile(stored_password="secret")
        recovery_tab.load_profile(profile)
        recovery_tab.source_type_var.set(StorageType.LOCAL.value)
        recovery_tab.backup_path_var.set(str(src))
        recovery_tab.winfo_toplevel().update_idletasks()
        assert recovery_tab._pw_frame.winfo_ismapped()

    def test_hidden_for_remote_non_encrypted(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.SFTP.value)
        recovery_tab._listed_backups = [
            {"name": "backup_FULL_2026-04-10", "size": 1000, "modified": 1.0, "encrypted": False},
        ]
        recovery_tab._selected_backups = {"backup_FULL_2026-04-10"}
        recovery_tab._update_post_source_sections()
        recovery_tab.winfo_toplevel().update_idletasks()
        assert not recovery_tab._pw_frame.winfo_ismapped()

    def test_shown_for_remote_encrypted(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.S3.value)
        recovery_tab._listed_backups = [
            {"name": "backup_FULL_2026-04-10", "size": 1000, "modified": 1.0, "encrypted": True},
        ]
        recovery_tab._selected_backups = {"backup_FULL_2026-04-10"}
        recovery_tab._update_post_source_sections()
        recovery_tab.winfo_toplevel().update_idletasks()
        assert recovery_tab._pw_frame.winfo_ismapped()


# ------------------------------------------------------------------
# Post-source visibility
# ------------------------------------------------------------------


class TestPostSourceVisibility:
    """Tests for destination / execute section visibility."""

    def test_dest_hidden_no_local_path(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.LOCAL.value)
        recovery_tab.backup_path_var.set("")
        recovery_tab._update_post_source_sections()
        recovery_tab.winfo_toplevel().update_idletasks()
        assert not recovery_tab._dest_frame.winfo_ismapped()

    def test_dest_shown_with_local_path(self, recovery_tab, tmp_path):
        src = tmp_path / "backup"
        src.mkdir()
        (src / "file.txt").write_text("data")
        recovery_tab.source_type_var.set(StorageType.LOCAL.value)
        recovery_tab.backup_path_var.set(str(src))
        recovery_tab.winfo_toplevel().update_idletasks()
        assert recovery_tab._dest_frame.winfo_ismapped()

    def test_dest_shown_with_remote_selection(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.SFTP.value)
        recovery_tab._listed_backups = [
            {"name": "backup_FULL", "size": 1000, "modified": 1.0},
        ]
        recovery_tab._selected_backups = {"backup_FULL"}
        recovery_tab._update_post_source_sections()
        recovery_tab.winfo_toplevel().update_idletasks()
        assert recovery_tab._dest_frame.winfo_ismapped()


# ------------------------------------------------------------------
# Build storage config
# ------------------------------------------------------------------


class TestBuildStorageConfig:
    """Tests for _build_storage_config."""

    def test_local_config(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.LOCAL.value)
        recovery_tab.backup_path_var.set("D:\\Backups")
        config = recovery_tab._build_storage_config()
        assert config.storage_type == StorageType.LOCAL
        assert config.destination_path == "D:\\Backups"

    def test_network_config(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.NETWORK.value)
        recovery_tab._net_path_var.set(r"\\server\share")
        recovery_tab._net_user_var.set("admin")
        recovery_tab._net_pass_var.set("pass")
        config = recovery_tab._build_storage_config()
        assert config.storage_type == StorageType.NETWORK
        assert config.network_username == "admin"

    def test_sftp_config(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.SFTP.value)
        recovery_tab._ret_sftp_vars["sftp_host"].set("myhost.com")
        recovery_tab._ret_sftp_vars["sftp_port"].set("2222")
        config = recovery_tab._build_storage_config()
        assert config.storage_type == StorageType.SFTP
        assert config.sftp_host == "myhost.com"
        assert config.sftp_port == 2222

    def test_s3_config(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.S3.value)
        # Set provider first (changing provider clears fields)
        recovery_tab._ret_s3_provider_var.set("wasabi")
        recovery_tab._ret_s3_vars["s3_access_key"].set("AKID")
        recovery_tab._ret_s3_vars["s3_secret_key"].set("SECRET")
        config = recovery_tab._build_storage_config()
        assert config.storage_type == StorageType.S3
        assert config.s3_access_key == "AKID"
        assert config.s3_provider == "wasabi"


# ------------------------------------------------------------------
# Listing callbacks
# ------------------------------------------------------------------


class TestListingCallback:
    """Tests for _on_list_done and _on_list_error."""

    def test_on_list_done_flat(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.SFTP.value)
        recovery_tab._on_list_done(
            [
                {"name": "a_FULL", "size": 100, "modified": 2000.0},
                {"name": "b_DIFF", "size": 50, "modified": 3000.0},
            ],
            grouped=False,
        )
        recovery_tab.winfo_toplevel().update_idletasks()
        assert len(recovery_tab._tree.get_children()) == 2
        assert recovery_tab._list_frame.winfo_ismapped()

    def test_on_list_done_grouped(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.S3.value)
        recovery_tab._on_list_done(
            [
                {"name": "a_FULL", "size": 100, "modified": 2.0, "_bucket": "b1"},
                {"name": "b_DIFF", "size": 50, "modified": 1.0, "_bucket": "b2"},
            ],
            grouped=True,
        )
        recovery_tab.winfo_toplevel().update_idletasks()
        assert recovery_tab._list_frame.winfo_ismapped()
        assert "Scan complete" in recovery_tab._scan_label.cget("text")

    def test_on_list_error(self, recovery_tab):
        recovery_tab.source_type_var.set(StorageType.S3.value)
        recovery_tab._on_list_error("Connection refused")
        recovery_tab.winfo_toplevel().update_idletasks()
        assert "Connection refused" in recovery_tab._scan_label.cget("text")


# ------------------------------------------------------------------
# Decrypt and extract
# ------------------------------------------------------------------


class TestDecryptAndExtract:
    """Tests for _decrypt_and_extract static method."""

    def test_wrong_password(self, tmp_path):
        from src.core.phases.collector import FileInfo
        from src.core.phases.local_writer import write_encrypted_tar

        src = tmp_path / "source"
        src.mkdir()
        (src / "test.txt").write_bytes(b"hello")
        fi = FileInfo(
            source_path=src / "test.txt",
            relative_path="test.txt",
            size=5,
            mtime=time.time(),
            source_root=str(src),
        )
        dest = tmp_path / "backup"
        dest.mkdir()
        archive = write_encrypted_tar([fi], dest, "Test", "correct-password")

        restore_dir = tmp_path / "restored"
        restore_dir.mkdir()
        with pytest.raises(RuntimeError, match="password you provided is incorrect"):
            RecoveryTab._decrypt_and_extract(archive, restore_dir, "wrong-password")

    def test_successful_extraction(self, tmp_path):
        from src.core.phases.collector import FileInfo
        from src.core.phases.local_writer import write_encrypted_tar

        src = tmp_path / "source"
        src.mkdir()
        (src / "a.txt").write_bytes(b"aaa")
        (src / "b.txt").write_bytes(b"bbb")

        infos = []
        for name in ("a.txt", "b.txt"):
            p = src / name
            infos.append(
                FileInfo(
                    source_path=p,
                    relative_path=name,
                    size=p.stat().st_size,
                    mtime=p.stat().st_mtime,
                    source_root=str(src),
                )
            )

        dest = tmp_path / "backup"
        dest.mkdir()
        archive = write_encrypted_tar(infos, dest, "Test", "password123")

        restore_dir = tmp_path / "restored"
        restore_dir.mkdir()
        count = RecoveryTab._decrypt_and_extract(archive, restore_dir, "password123")
        assert count == 2


# ------------------------------------------------------------------
# collect_config
# ------------------------------------------------------------------


class TestCollectConfig:
    def test_returns_empty_dict(self, recovery_tab):
        assert recovery_tab.collect_config() == {}
