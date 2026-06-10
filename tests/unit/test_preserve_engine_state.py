"""Tests for BackupManagerApp._preserve_engine_owned_state.

Regression guard for the 10/06/2026 incident: a profile-switch auto-save
collected user-editable tab values into a stale cached profile instance
and wrote it to disk, regressing run-state fields (last_backup,
last_full_backup, ...) that the scheduler had just advanced on its own
instance. The UI save must refresh those engine-owned fields from disk
first so it never overwrites the scheduler's work.
"""

from __future__ import annotations

from src.core.config import BackupProfile, ConfigManager, StorageConfig, StorageType
from src.ui.app import BackupManagerApp


def _app(config_manager):
    # Bypass the heavy Tk constructor — the method under test only uses
    # self.config_manager.
    app = BackupManagerApp.__new__(BackupManagerApp)
    app.config_manager = config_manager
    return app


def _local_storage(tmp_path):
    return StorageConfig(
        storage_type=StorageType.LOCAL, destination_path=str(tmp_path / "backups")
    )


def test_preserves_run_state_from_disk(tmp_path):
    mgr = ConfigManager(config_dir=tmp_path)
    profile = BackupProfile(name="P", storage=_local_storage(tmp_path))
    profile.last_backup = "2026-06-10T10:02:02"
    profile.last_full_backup = "2026-06-10T10:02:02"
    profile.last_full_files_count = 2356
    profile.crash_recovery_attempts = 0
    mgr.save_profile(profile)

    # Stale in-memory copy: simulates the UI cache from BEFORE a scheduled
    # run advanced the on-disk run-state.
    stale = BackupProfile(id=profile.id, name="P", storage=_local_storage(tmp_path))
    stale.last_backup = "2026-06-08T10:01:17"  # two days old
    stale.last_full_files_count = 0

    _app(mgr)._preserve_engine_owned_state(stale)

    assert stale.last_backup == "2026-06-10T10:02:02"
    assert stale.last_full_backup == "2026-06-10T10:02:02"
    assert stale.last_full_files_count == 2356


def test_preserves_recovery_flags(tmp_path):
    mgr = ConfigManager(config_dir=tmp_path)
    profile = BackupProfile(name="P", storage=_local_storage(tmp_path))
    profile.last_backup_completed = False
    profile.incomplete_backup_name = "P_FULL_inflight"
    profile.crash_recovery_attempts = 2
    mgr.save_profile(profile)

    stale = BackupProfile(id=profile.id, name="P", storage=_local_storage(tmp_path))
    # Stale cache thinks everything is clean.
    stale.last_backup_completed = True
    stale.incomplete_backup_name = ""
    stale.crash_recovery_attempts = 0

    _app(mgr)._preserve_engine_owned_state(stale)

    assert stale.last_backup_completed is False
    assert stale.incomplete_backup_name == "P_FULL_inflight"
    assert stale.crash_recovery_attempts == 2


def test_noop_for_new_unsaved_profile(tmp_path):
    mgr = ConfigManager(config_dir=tmp_path)
    fresh = BackupProfile(name="New", storage=_local_storage(tmp_path))
    fresh.last_backup = "keep-me"
    # Not on disk yet → in-memory value must be left untouched.
    _app(mgr)._preserve_engine_owned_state(fresh)
    assert fresh.last_backup == "keep-me"


def test_does_not_touch_user_editable_fields(tmp_path):
    # The merge must only copy run-state — user edits (name, sources)
    # collected by _save_profile must survive.
    mgr = ConfigManager(config_dir=tmp_path)
    profile = BackupProfile(name="OldName", storage=_local_storage(tmp_path))
    profile.source_paths = ["/old/path"]
    mgr.save_profile(profile)

    edited = BackupProfile(id=profile.id, name="NewName", storage=_local_storage(tmp_path))
    edited.source_paths = ["/new/path"]

    _app(mgr)._preserve_engine_owned_state(edited)

    assert edited.name == "NewName"
    assert edited.source_paths == ["/new/path"]
