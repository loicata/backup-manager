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


class TestCleanupIncompleteBackup:
    def _make_ctx(self, profile, mgr):
        from src.core.phases.base import PipelineContext

        return PipelineContext(
            profile=profile,
            config_manager=mgr,
            events=EventBus(),
            result=BackupResult(),
        )

    def test_logs_nothing_to_clean_when_not_found(self, tmp_config_dir):
        """Destinations where the incomplete backup doesn't exist log a message."""
        from unittest.mock import MagicMock, patch

        from src.core.backup_engine import BackupEngine
        from src.core.config import StorageConfig, StorageType

        storage_dir = tmp_config_dir / "storage"
        storage_dir.mkdir()

        profile = BackupProfile(
            name="Test",
            incomplete_backup_name="test_FULL_interrupted",
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(storage_dir),
            ),
            mirror_destinations=[
                StorageConfig(
                    storage_type=StorageType.S3,
                    s3_bucket="bucket",
                    s3_region="eu-west-1",
                    s3_access_key="key",
                    s3_secret_key="secret",
                ),
            ],
        )
        mgr = ConfigManager(config_dir=tmp_config_dir)

        mock_backend = MagicMock()
        mock_backend.delete_backup.side_effect = FileNotFoundError("not found")

        engine = BackupEngine(mgr, events=EventBus())
        ctx = self._make_ctx(profile, mgr)

        logged = []
        engine._log = lambda msg, **kw: logged.append(msg)

        with patch(
            "src.core.backup_engine.create_backend",
            return_value=mock_backend,
        ):
            engine._cleanup_incomplete_backup(ctx)

        # Both Storage and Mirror 1 should report nothing to clean up
        clean_msgs = [m for m in logged if "nothing to clean up" in m]
        assert len(clean_msgs) == 2
        assert any("Storage" in m for m in clean_msgs)
        assert any("Mirror 1" in m for m in clean_msgs)

    def test_mixed_found_and_not_found(self, tmp_config_dir):
        """Storage has the backup, Mirror does not."""
        from unittest.mock import MagicMock, patch

        from src.core.backup_engine import BackupEngine
        from src.core.config import StorageConfig, StorageType

        storage_dir = tmp_config_dir / "storage"
        storage_dir.mkdir()

        profile = BackupProfile(
            name="Test",
            incomplete_backup_name="test_FULL_123",
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(storage_dir),
            ),
            mirror_destinations=[
                StorageConfig(
                    storage_type=StorageType.S3,
                    s3_bucket="bucket",
                    s3_region="eu-west-1",
                    s3_access_key="key",
                    s3_secret_key="secret",
                ),
            ],
        )
        mgr = ConfigManager(config_dir=tmp_config_dir)

        # Storage backend: plain dir found, .tar.wbenc not found
        storage_backend = MagicMock()
        storage_backend.delete_backup.side_effect = [
            None,  # plain dir deleted
            FileNotFoundError(),  # no .tar.wbenc
        ]
        # Mirror backend: nothing found
        mirror_backend = MagicMock()
        mirror_backend.delete_backup.side_effect = FileNotFoundError("nope")

        backends = [storage_backend, mirror_backend]

        engine = BackupEngine(mgr, events=EventBus())
        ctx = self._make_ctx(profile, mgr)

        logged = []
        engine._log = lambda msg, **kw: logged.append(msg)

        with patch(
            "src.core.backup_engine.create_backend",
            side_effect=backends,
        ):
            engine._cleanup_incomplete_backup(ctx)

        assert any("Storage: deleted incomplete" in m for m in logged)
        assert any("Mirror 1: nothing to clean up" in m for m in logged)


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


class TestMarkCompletedPersistence:
    """Regression tests: _mark_completed must persist flags to disk.

    Before the fix, last_backup_completed was set in memory but never
    saved, so every subsequent run treated the previous backup as
    interrupted and forced a full.
    """

    def _make_ctx(self, profile, mgr):
        from src.core.phases.base import PipelineContext

        return PipelineContext(
            profile=profile,
            config_manager=mgr,
            events=EventBus(),
            result=BackupResult(),
        )

    def test_mark_completed_persists_flags(self, tmp_config_dir):
        """After _mark_completed, reloading the profile from disk must
        show last_backup_completed=True and empty incomplete fields."""
        from src.core.backup_engine import BackupEngine

        profile = BackupProfile(
            name="Persist",
            last_backup_completed=False,
            incomplete_backup_name="Persist_FULL_123",
            incomplete_backup_was_full=True,
        )
        mgr = ConfigManager(config_dir=tmp_config_dir)
        mgr.save_profile(profile)

        engine = BackupEngine(mgr, events=EventBus())
        ctx = self._make_ctx(profile, mgr)

        engine._mark_completed(ctx)

        # Reload from disk to verify persistence
        loaded = mgr.get_all_profiles()[0]
        assert loaded.last_backup_completed is True
        assert loaded.incomplete_backup_name == ""
        assert loaded.incomplete_backup_was_full is False

    def test_mark_completed_restores_differential_type(self, tmp_config_dir):
        """When a differential was auto-promoted to full, _mark_completed
        must restore the profile type back to DIFFERENTIAL on disk."""
        from src.core.backup_engine import BackupEngine

        profile = BackupProfile(
            name="Restore",
            backup_type=BackupType.FULL,
            last_backup_completed=False,
            incomplete_backup_name="Restore_FULL_123",
            incomplete_backup_was_full=True,
        )
        mgr = ConfigManager(config_dir=tmp_config_dir)
        mgr.save_profile(profile)

        engine = BackupEngine(mgr, events=EventBus())
        ctx = self._make_ctx(profile, mgr)
        ctx.forced_full = True  # simulate auto-promotion

        engine._mark_completed(ctx)

        loaded = mgr.get_all_profiles()[0]
        assert loaded.backup_type == BackupType.DIFFERENTIAL

    def test_successful_backup_allows_next_differential(self, tmp_config_dir):
        """End-to-end: after a successful full backup, the next run should
        NOT force a full (the bug that triggered this fix)."""
        from src.core.backup_engine import BackupEngine
        from src.core.config import StorageConfig, StorageType, compute_profile_hash

        backups_dir = tmp_config_dir / "backups"
        backups_dir.mkdir()

        profile = BackupProfile(
            name="E2E",
            backup_type=BackupType.DIFFERENTIAL,
            last_backup_completed=False,
            incomplete_backup_name="E2E_FULL_old",
            incomplete_backup_was_full=True,
            differential_count=0,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(backups_dir),
            ),
        )
        mgr = ConfigManager(config_dir=tmp_config_dir)
        mgr.save_profile(profile)

        engine = BackupEngine(mgr, events=EventBus())

        # Simulate a successful backup completing
        ctx = self._make_ctx(profile, mgr)
        engine._mark_completed(ctx)

        # Now reload from disk and run _maybe_force_full
        reloaded = mgr.get_all_profiles()[0]
        assert reloaded.last_backup_completed is True

        # Create a manifest so "no_manifest" doesn't trigger
        manifest_path = mgr.get_manifest_path(reloaded.id)
        manifest_path.write_text("{}", encoding="utf-8")

        # Create a fake full backup so destination check passes
        (backups_dir / "E2E_FULL_2026-01-01_000000").mkdir()

        reloaded.profile_hash = compute_profile_hash(reloaded)
        ctx2 = self._make_ctx(reloaded, mgr)
        engine._maybe_force_full(ctx2)

        assert ctx2.forced_full is False
        assert reloaded.backup_type == BackupType.DIFFERENTIAL
