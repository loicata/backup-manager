"""End-to-end integration tests for the full backup pipeline.

Tests the complete backup workflow: collect → filter → write → verify → rotate.
Uses local storage with temporary directories.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.core.backup_engine import BackupEngine, BackupStats
from src.core.config import (
    BackupProfile,
    BackupType,
    ConfigManager,
    EncryptionConfig,
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
            policy=RetentionPolicy.GFS, gfs_daily=99, gfs_weekly=99, gfs_monthly=99,
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
            policy=RetentionPolicy.GFS, gfs_daily=99, gfs_weekly=99, gfs_monthly=99,
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


class TestIncrementalBackupE2E:
    """Test incremental backup pipeline."""

    def test_incremental_skips_unchanged(self, e2e_env, full_profile):
        """Second incremental run should detect no changes."""
        full_profile.backup_type = BackupType.INCREMENTAL
        engine = BackupEngine(e2e_env["config_manager"])

        # First run: all files
        stats1 = engine.run_backup(full_profile)
        assert stats1.files_processed == 3

        # Second run: no changes
        stats2 = engine.run_backup(full_profile)
        assert stats2.files_skipped == 3
        assert stats2.files_processed == 0

    def test_incremental_detects_new_file(self, e2e_env, full_profile):
        """Incremental should detect newly added files."""
        full_profile.backup_type = BackupType.INCREMENTAL
        engine = BackupEngine(e2e_env["config_manager"])

        # First run
        engine.run_backup(full_profile)

        # Add new file
        new_file = e2e_env["source"] / "new_file.txt"
        new_file.write_text("New content", encoding="utf-8")

        # Second run should detect the new file
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
            gfs_daily=7, gfs_weekly=4, gfs_monthly=12,
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
