"""Tests for the Recovery tab's backup-source Browse initialdir.

Without an explicit ``initialdir``, Tk's file dialog falls back to the
OS "last used directory" — after the user's first successful restore,
clicking Browse for a second restore opens the dialog at the PREVIOUS
destination (where the first backup was extracted) instead of where
the backups actually live. The helper
``RecoveryTab._default_backup_initialdir`` picks a meaningful
starting directory based on three signals, in order of preference.
"""

import pytest

from src.core.config import (
    BackupProfile,
    StorageConfig,
    StorageType,
)
from src.ui.tabs.recovery_tab import RecoveryTab


@pytest.fixture()
def recovery_tab(tk_root):
    tab = RecoveryTab(tk_root)
    yield tab
    tab.destroy()


def _make_profile(
    storage_path: str, storage_type: StorageType = StorageType.LOCAL
) -> BackupProfile:
    if storage_type == StorageType.LOCAL:
        storage = StorageConfig(
            storage_type=storage_type,
            destination_path=storage_path,
        )
    elif storage_type == StorageType.SFTP:
        storage = StorageConfig(
            storage_type=storage_type,
            sftp_host="example.com",
            sftp_username="user",
            sftp_remote_path=storage_path,
            sftp_password="pw",
        )
    else:  # S3 etc.
        storage = StorageConfig(
            storage_type=storage_type,
            s3_bucket="b",
            s3_region="us-east-1",
            s3_access_key="a",
            s3_secret_key="s",
        )
    return BackupProfile(name="T", storage=storage)


class TestDefaultBackupInitialDir:
    """Three-tier priority for the backup-source Browse dialog's initialdir."""

    def test_uses_parent_of_existing_path_when_set(self, recovery_tab, tmp_path):
        """Tier 1: an already-selected backup path wins — dialog opens at its
        parent so the user can pick a sibling backup immediately."""
        backup_dir = tmp_path / "G_root" / "Backup Manager"
        backup_dir.mkdir(parents=True)
        previous = backup_dir / "BackupTest_FULL_2026-04-17_204141"
        previous.mkdir()

        # Profile points elsewhere — existing path should still win
        recovery_tab._profile = _make_profile(str(tmp_path / "something_else"))
        recovery_tab.backup_path_var.set(str(previous))

        assert recovery_tab._default_backup_initialdir() == str(backup_dir)

    def test_uses_profile_storage_path_when_field_empty(self, recovery_tab, tmp_path):
        """Tier 2: no user-selected path yet — default to the profile's
        primary storage path (where backups for this profile live)."""
        storage_dir = tmp_path / "G_root" / "Backup Manager"
        storage_dir.mkdir(parents=True)
        recovery_tab._profile = _make_profile(str(storage_dir))
        recovery_tab.backup_path_var.set("")

        assert recovery_tab._default_backup_initialdir() == str(storage_dir)

    def test_returns_none_when_profile_storage_is_not_local(self, recovery_tab, tmp_path):
        """Tier 3: non-local storage (SFTP/S3) has no meaningful filesystem
        path the OS can open — let Tk fall back to its default."""
        recovery_tab._profile = _make_profile("/remote/path", StorageType.SFTP)
        recovery_tab.backup_path_var.set("")

        assert recovery_tab._default_backup_initialdir() is None

    def test_returns_none_when_no_profile_loaded(self, recovery_tab):
        """Tier 3 bis: no profile context at all."""
        recovery_tab._profile = None
        recovery_tab.backup_path_var.set("")

        assert recovery_tab._default_backup_initialdir() is None

    def test_parent_is_used_even_when_existing_is_a_directory(self, recovery_tab, tmp_path):
        """When the field holds a backup folder itself (e.g.
        ``G:\\Backup Manager\\BackupTest_FULL_...``) the dialog must open
        at the SIBLING level so other backups are visible — opening
        inside the current backup would hide the alternatives."""
        parent_dir = tmp_path / "Backup Manager"
        parent_dir.mkdir()
        selected = parent_dir / "BackupTest_FULL_folder"
        selected.mkdir()
        recovery_tab.backup_path_var.set(str(selected))

        assert recovery_tab._default_backup_initialdir() == str(parent_dir)

    def test_falls_through_to_profile_if_existing_path_does_not_exist(self, recovery_tab, tmp_path):
        """A stale/invalid existing path must not block the profile fallback."""
        storage_dir = tmp_path / "live_storage"
        storage_dir.mkdir()
        recovery_tab._profile = _make_profile(str(storage_dir))
        recovery_tab.backup_path_var.set("C:/nonexistent/stale/path")

        assert recovery_tab._default_backup_initialdir() == str(storage_dir)
