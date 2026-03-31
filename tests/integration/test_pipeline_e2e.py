"""End-to-end integration tests for the full backup pipeline.

Tests the complete backup workflow: collect → filter → write → verify → rotate.
Uses local storage with temporary directories.
"""

from pathlib import Path

import pytest

from src.core.backup_engine import BackupEngine
from src.core.config import (
    BackupProfile,
    BackupType,
    ConfigManager,
    RetentionConfig,
    RetentionPolicy,
    StorageConfig,
    StorageType,
    VerificationConfig,
)
from src.core.events import EventBus
from src.core.exceptions import CancelledError


@pytest.fixture
def e2e_env(tmp_path):
    """Set up a complete E2E environment with source files and config."""
    # Create source directory with test files
    source = tmp_path / "source"
    source.mkdir()
    (source / "document.txt").write_text("Important document", encoding="utf-8")
    (source / "data.csv").write_text("col1,col2\n1,2\n3,4", encoding="utf-8")
    sub = source / "subdir"
    sub.mkdir()
    (sub / "nested.txt").write_text("Nested content", encoding="utf-8")

    # Create destination
    dest = tmp_path / "backups"
    dest.mkdir()

    # Create config directory
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "profiles").mkdir()
    (config_dir / "logs").mkdir()
    (config_dir / "manifests").mkdir()

    config_manager = ConfigManager(config_dir=config_dir)

    return {
        "source": source,
        "dest": dest,
        "config_dir": config_dir,
        "config_manager": config_manager,
    }


@pytest.fixture
def full_profile(e2e_env):
    """Create a full backup profile pointing at the test source/dest."""
    return BackupProfile(
        id="e2e_test",
        name="E2E Test",
        source_paths=[str(e2e_env["source"])],
        exclude_patterns=["*.tmp", "*.log"],
        backup_type=BackupType.FULL,
        storage=StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path=str(e2e_env["dest"]),
        ),
        verification=VerificationConfig(auto_verify=True, alert_on_failure=True),
        retention=RetentionConfig(
            policy=RetentionPolicy.GFS,
        ),
    )


class TestFullBackupE2E:
    """Test complete full backup pipeline."""

    def test_full_backup_creates_files(self, e2e_env, full_profile):
        """A full backup should copy all source files to destination."""
        full_profile.retention = RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=99,
            gfs_weekly=99,
            gfs_monthly=99,
        )
        engine = BackupEngine(e2e_env["config_manager"])
        stats = engine.run_backup(full_profile)

        assert stats.files_found == 3
        assert stats.files_processed == 3
        assert stats.errors == 0
        assert stats.backup_path != ""

        # Verify backup directory was created
        backup_path = Path(stats.backup_path)
        assert backup_path.exists()

    def test_full_backup_preserves_content(self, e2e_env, full_profile):
        """Backup should preserve exact file content."""
        full_profile.retention = RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=99,
            gfs_weekly=99,
            gfs_monthly=99,
        )
        engine = BackupEngine(e2e_env["config_manager"])
        stats = engine.run_backup(full_profile)

        backup_path = Path(stats.backup_path)
        # Find the backed-up document
        backed_up_files = list(backup_path.rglob("document.txt"))
        assert len(backed_up_files) == 1
        assert backed_up_files[0].read_text(encoding="utf-8") == "Important document"

    def test_full_backup_events(self, e2e_env, full_profile):
        """Backup should emit events throughout pipeline."""
        events = EventBus()
        log_messages = []
        events.subscribe("log", lambda message="", **kw: log_messages.append(message))

        engine = BackupEngine(e2e_env["config_manager"], events=events)
        engine.run_backup(full_profile)

        assert len(log_messages) > 0
        assert any("Collecting" in m for m in log_messages)
        assert any("complete" in m.lower() for m in log_messages)

    def test_log_lines_capture_phase_messages(self, e2e_env, full_profile):
        """BackupResult.log_lines must include messages from all phases."""
        full_profile.retention = RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=99,
            gfs_weekly=99,
            gfs_monthly=99,
        )
        engine = BackupEngine(e2e_env["config_manager"])
        stats = engine.run_backup(full_profile)

        logs = stats.log_lines
        # Engine messages
        assert any("Collecting" in m for m in logs)
        # Phase messages (from PhaseLogger, not engine._log)
        assert any("Collected" in m for m in logs)
        assert any("Manifest created" in m for m in logs)
        assert any("Verification OK" in m for m in logs)
        assert any("Backup complete" in m for m in logs)


class TestDifferentialBackupE2E:
    """Test differential backup pipeline."""

    def test_differential_skips_unchanged(self, e2e_env, full_profile):
        """Full then differential with no changes should skip all."""
        engine = BackupEngine(e2e_env["config_manager"])

        # Full backup first (writes the manifest)
        full_profile.backup_type = BackupType.FULL
        stats1 = engine.run_backup(full_profile)
        assert stats1.files_processed == 3

        # Differential: no changes
        full_profile.backup_type = BackupType.DIFFERENTIAL
        stats2 = engine.run_backup(full_profile)
        assert stats2.files_skipped == 3
        assert stats2.files_processed == 0

    def test_differential_detects_new_file(self, e2e_env, full_profile):
        """Differential should detect newly added files."""
        engine = BackupEngine(e2e_env["config_manager"])

        # Full backup first (writes the manifest)
        full_profile.backup_type = BackupType.FULL
        engine.run_backup(full_profile)

        # Add new file
        new_file = e2e_env["source"] / "new_file.txt"
        new_file.write_text("New content", encoding="utf-8")

        # Differential should detect the new file
        full_profile.backup_type = BackupType.DIFFERENTIAL
        stats2 = engine.run_backup(full_profile)
        assert stats2.files_processed >= 1


class TestCancellationE2E:
    """Test backup cancellation."""

    def test_cancel_during_backup(self, e2e_env, full_profile):
        """Cancelling during backup should raise CancelledError."""
        events = EventBus()
        engine = BackupEngine(e2e_env["config_manager"], events=events)

        # Cancel when the first phase event fires (before collect completes)
        events.subscribe("phase_changed", lambda **kw: engine.cancel())

        with pytest.raises(CancelledError):
            engine.run_backup(full_profile)


class TestRotationE2E:
    """Test backup rotation as part of full pipeline."""

    def test_gfs_rotation_runs_without_error(self, e2e_env, full_profile):
        """GFS rotation should run without error during pipeline."""
        full_profile.retention = RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=7,
            gfs_weekly=4,
            gfs_monthly=12,
        )
        engine = BackupEngine(e2e_env["config_manager"])

        # Run 2 backups — rotation should execute without error
        stats1 = engine.run_backup(full_profile)
        assert stats1.files_processed > 0

        stats2 = engine.run_backup(full_profile)
        assert stats2.files_processed > 0


class TestExcludePatterns:
    """Test file exclusion patterns."""

    def test_excludes_tmp_files(self, e2e_env, full_profile):
        """Tmp files should be excluded from backup."""
        # Add a .tmp file to source
        (e2e_env["source"] / "temp.tmp").write_text("Temp file", encoding="utf-8")

        engine = BackupEngine(e2e_env["config_manager"])
        stats = engine.run_backup(full_profile)

        # Should still be 3 files (tmp excluded)
        assert stats.files_found == 3


class TestBackupTypeLogs:
    """Test that backup type and reference info appear in log messages."""

    def test_full_backup_logs_type(self, e2e_env, full_profile):
        """Full backup should log 'Backup type: full'."""
        full_profile.retention = RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=99,
            gfs_weekly=99,
            gfs_monthly=99,
        )
        engine = BackupEngine(e2e_env["config_manager"])
        stats = engine.run_backup(full_profile)

        assert any("Backup type: full" in m for m in stats.log_lines)

    def test_differential_logs_reference(self, e2e_env, full_profile):
        """Differential backup should log reference to the last full backup."""
        full_profile.retention = RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=99,
            gfs_weekly=99,
            gfs_monthly=99,
        )
        engine = BackupEngine(e2e_env["config_manager"])

        # Run a full backup first to create the manifest with metadata
        full_profile.backup_type = BackupType.FULL
        stats_full = engine.run_backup(full_profile)
        (
            stats_full.backup_path.rsplit("\\", 1)[-1]
            if "\\" in stats_full.backup_path
            else stats_full.backup_path.rsplit("/", 1)[-1]
        )

        # Now run a differential
        full_profile.backup_type = BackupType.DIFFERENTIAL
        (e2e_env["source"] / "changed.txt").write_text("new", encoding="utf-8")
        stats_diff = engine.run_backup(full_profile)

        # Should mention backup type and reference
        type_logs = [m for m in stats_diff.log_lines if "Backup type:" in m]
        assert len(type_logs) == 1
        assert "differential" in type_logs[0]
        assert "reference:" in type_logs[0]
        assert "_FULL_" in type_logs[0]

    def test_auto_promoted_full_logs_correctly(self, e2e_env, full_profile):
        """Auto-promoted differential should log 'full (auto-promoted)'."""
        full_profile.retention = RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=99,
            gfs_weekly=99,
            gfs_monthly=99,
        )
        engine = BackupEngine(e2e_env["config_manager"])

        # Set to differential with no prior manifest → auto-promotes to full
        full_profile.backup_type = BackupType.DIFFERENTIAL
        stats = engine.run_backup(full_profile)

        assert any("Backup type: full (auto-promoted)" in m for m in stats.log_lines)

    def test_manifest_stores_metadata(self, e2e_env, full_profile):
        """Full backup manifest should contain __metadata__ with backup_name."""
        full_profile.retention = RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=99,
            gfs_weekly=99,
            gfs_monthly=99,
        )
        engine = BackupEngine(e2e_env["config_manager"])

        full_profile.backup_type = BackupType.FULL
        engine.run_backup(full_profile)

        from src.core.phases.filter import load_manifest

        manifest_path = e2e_env["config_manager"].get_manifest_path(full_profile.id)
        manifest = load_manifest(manifest_path)

        assert "__metadata__" in manifest
        assert "backup_name" in manifest["__metadata__"]
        assert "_FULL_" in manifest["__metadata__"]["backup_name"]
        assert "created_at" in manifest["__metadata__"]

    def test_differential_without_metadata_logs_gracefully(self, e2e_env, full_profile):
        """Differential with old manifest (no metadata) should still log type."""
        full_profile.retention = RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=99,
            gfs_weekly=99,
            gfs_monthly=99,
        )
        engine = BackupEngine(e2e_env["config_manager"])

        # Create a manifest without metadata (simulating old format)
        from src.core.phases.collector import collect_files
        from src.core.phases.filter import build_updated_manifest, save_manifest

        files = collect_files([str(e2e_env["source"])])
        manifest = build_updated_manifest(files)
        manifest_path = e2e_env["config_manager"].get_manifest_path(full_profile.id)
        save_manifest(manifest, manifest_path)

        # Run a full backup first so the destination is not empty.
        from src.core.config import compute_profile_hash

        full_profile.backup_type = BackupType.FULL
        full_profile.profile_hash = compute_profile_hash(full_profile)
        engine.run_backup(full_profile)

        # Now run differential — destination has a backup, manifest exists.
        full_profile.backup_type = BackupType.DIFFERENTIAL
        full_profile.profile_hash = compute_profile_hash(full_profile)
        (e2e_env["source"] / "extra.txt").write_text("extra", encoding="utf-8")
        stats = engine.run_backup(full_profile)

        type_logs = [m for m in stats.log_lines if "Backup type:" in m]
        assert len(type_logs) == 1
        assert "differential" in type_logs[0]
