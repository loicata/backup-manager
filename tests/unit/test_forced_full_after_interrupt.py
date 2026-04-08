"""Tests for backup interrupt detection and cleanup.

Verifies that when any backup (full or differential) is interrupted,
the next run detects the incomplete backup, cleans it up, and forces
a new full only if the interrupted backup was a full.
"""

import json

from src.core.backup_result import BackupResult
from src.core.config import BackupProfile, BackupType, ConfigManager
from src.core.events import EventBus


class TestLastBackupCompletedFlag:
    def test_default_is_true(self):
        """New profiles default to last_backup_completed=True."""
        p = BackupProfile()
        assert p.last_backup_completed is True

    def test_default_incomplete_name_empty(self):
        p = BackupProfile()
        assert p.incomplete_backup_name == ""

    def test_default_incomplete_was_full_false(self):
        p = BackupProfile()
        assert p.incomplete_backup_was_full is False

    def test_roundtrip_false(self, tmp_config_dir):
        """Flag False is persisted and loaded correctly."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(
            name="Test",
            last_backup_completed=False,
            incomplete_backup_name="test_FULL_123",
            incomplete_backup_was_full=True,
        )
        mgr.save_profile(profile)

        loaded = mgr.get_all_profiles()[0]
        assert loaded.last_backup_completed is False
        assert loaded.incomplete_backup_name == "test_FULL_123"
        assert loaded.incomplete_backup_was_full is True

    def test_roundtrip_true(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Test", last_backup_completed=True)
        mgr.save_profile(profile)

        loaded = mgr.get_all_profiles()[0]
        assert loaded.last_backup_completed is True

    def test_old_profile_without_flag_defaults_true(self, tmp_config_dir):
        """Profiles saved before this feature default to True."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Legacy")
        mgr.save_profile(profile)

        filepath = mgr.profiles_dir / f"{profile.id}.json"
        data = json.loads(filepath.read_text(encoding="utf-8"))
        data.pop("last_backup_completed", None)
        data.pop("incomplete_backup_name", None)
        data.pop("incomplete_backup_was_full", None)
        filepath.write_text(json.dumps(data, indent=2), encoding="utf-8")

        loaded = mgr.get_all_profiles()[0]
        assert loaded.last_backup_completed is True

    def test_migrate_last_full_completed(self, tmp_config_dir):
        """Old last_full_completed field migrates to last_backup_completed."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Legacy")
        mgr.save_profile(profile)

        filepath = mgr.profiles_dir / f"{profile.id}.json"
        data = json.loads(filepath.read_text(encoding="utf-8"))
        data["last_full_completed"] = False
        data.pop("last_backup_completed", None)
        filepath.write_text(json.dumps(data, indent=2), encoding="utf-8")

        loaded = mgr.get_all_profiles()[0]
        assert loaded.last_backup_completed is False


class TestForceFullAfterInterrupt:
    def _make_ctx(self, profile, mgr):
        from src.core.phases.base import PipelineContext

        return PipelineContext(
            profile=profile,
            config_manager=mgr,
            events=EventBus(),
            result=BackupResult(),
        )

    def test_interrupted_full_forces_new_full(self, tmp_config_dir):
        """Differential after interrupted full must be promoted to full."""
        from src.core.backup_engine import BackupEngine

        profile = BackupProfile(
            name="Test",
            backup_type=BackupType.DIFFERENTIAL,
            last_backup_completed=False,
            incomplete_backup_was_full=True,
            incomplete_backup_name="test_FULL_123",
        )
        mgr = ConfigManager(config_dir=tmp_config_dir)
        mgr.save_profile(profile)
        engine = BackupEngine(mgr, events=EventBus())
        ctx = self._make_ctx(profile, mgr)

        engine._maybe_force_full(ctx)

        assert ctx.forced_full is True
        assert ctx.profile.backup_type == BackupType.FULL

    def test_interrupted_diff_does_not_force_full(self, tmp_config_dir):
        """Differential after interrupted diff should NOT force full
        (when no other condition triggers a full)."""
        from src.core.backup_engine import BackupEngine
        from src.core.config import StorageConfig, StorageType, compute_profile_hash

        # Create a valid local destination with a fake full backup
        backups_dir = tmp_config_dir / "backups"
        backups_dir.mkdir()
        (backups_dir / "Test_FULL_2026-01-01_000000").mkdir()

        profile = BackupProfile(
            name="Test",
            backup_type=BackupType.DIFFERENTIAL,
            last_backup_completed=False,
            incomplete_backup_was_full=False,
            incomplete_backup_name="test_DIFF_123",
            differential_count=0,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(backups_dir),
            ),
        )
        # Set profile_hash to current so "config changed" doesn't trigger
        profile.profile_hash = compute_profile_hash(profile)

        mgr = ConfigManager(config_dir=tmp_config_dir)
        mgr.save_profile(profile)

        # Create a manifest so "no_manifest" doesn't trigger a full
        manifest_path = mgr.get_manifest_path(profile.id)
        manifest_path.write_text("{}", encoding="utf-8")

        engine = BackupEngine(mgr, events=EventBus())
        ctx = self._make_ctx(profile, mgr)

        engine._maybe_force_full(ctx)

        # Should NOT be forced to full (diff was interrupted, not full)
        assert ctx.forced_full is False
        assert ctx.profile.backup_type == BackupType.DIFFERENTIAL
        # Cleanup should have cleared the incomplete name
        assert ctx.profile.incomplete_backup_name == ""

    def test_completed_backup_allows_differential(self, tmp_config_dir):
        """Differential after completed backup should not be forced."""
        from src.core.backup_engine import BackupEngine

        profile = BackupProfile(
            name="Test",
            backup_type=BackupType.DIFFERENTIAL,
            last_backup_completed=True,
            differential_count=0,
            profile_hash="",
        )
        mgr = ConfigManager(config_dir=tmp_config_dir)
        mgr.save_profile(profile)
        manifest_path = mgr.get_manifest_path(profile.id)
        manifest_path.write_text("{}", encoding="utf-8")

        engine = BackupEngine(mgr, events=EventBus())
        ctx = self._make_ctx(profile, mgr)

        engine._maybe_force_full(ctx)

        assert profile.last_backup_completed is True
