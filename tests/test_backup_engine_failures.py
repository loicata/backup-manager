"""Tests for backup engine failure scenarios.

Covers collection, write, manifest, verify, encrypt, mirror, rotate
failures, cancellation at each phase, empty backups, and non-fatal
error propagation through the pipeline.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.core.backup_engine import BackupEngine
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

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def env(tmp_path):
    """Minimal backup environment with source files and config."""
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_text("aaa", encoding="utf-8")
    (source / "b.txt").write_text("bbb", encoding="utf-8")

    dest = tmp_path / "backups"
    dest.mkdir()

    config_dir = tmp_path / "config"
    for sub in ("profiles", "logs", "manifests"):
        (config_dir / sub).mkdir(parents=True, exist_ok=True)

    return {
        "source": source,
        "dest": dest,
        "config_manager": ConfigManager(config_dir=config_dir),
    }


@pytest.fixture
def profile(env):
    """Standard full-backup profile."""
    return BackupProfile(
        id="fail_test",
        name="FailTest",
        source_paths=[str(env["source"])],
        exclude_patterns=[],
        backup_type=BackupType.FULL,
        storage=StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path=str(env["dest"]),
        ),
        verification=VerificationConfig(auto_verify=True, alert_on_failure=True),
        retention=RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=99,
            gfs_weekly=99,
            gfs_monthly=99,
        ),
    )


def _engine(env):
    """Create a BackupEngine with a silent EventBus."""
    return BackupEngine(env["config_manager"], events=EventBus())


# ---------------------------------------------------------------------------
# 1. Collection phase failures
# ---------------------------------------------------------------------------


class TestCollectionFailures:

    def test_source_directory_does_not_exist(self, env, profile):
        """Collector should return zero files when source is missing."""
        profile.source_paths = [str(env["dest"] / "nonexistent")]
        engine = _engine(env)
        result = engine.run_backup(profile)
        assert result.files_found == 0
        assert result.files_processed == 0

    def test_permission_denied_on_source(self, env, profile):
        """Collector should skip unreadable directories gracefully."""
        with patch("os.scandir", side_effect=PermissionError("denied")):
            engine = _engine(env)
            result = engine.run_backup(profile)
            assert result.files_found == 0


# ---------------------------------------------------------------------------
# 2. Write phase failures
# ---------------------------------------------------------------------------


class TestWriteFailures:

    def test_disk_full_during_copy(self, env, profile):
        """Write phase must raise when copy2 fails — zero tolerance for errors."""
        engine = _engine(env)
        with (
            patch("shutil.copy2", side_effect=OSError("No space left on device")),
            pytest.raises(Exception, match="No space left"),
        ):
            engine.run_backup(profile)


# ---------------------------------------------------------------------------
# 3. Manifest phase failures
# ---------------------------------------------------------------------------


class TestManifestFailures:

    def test_cannot_write_manifest_file(self, env, profile):
        """Pipeline should raise when integrity manifest write fails."""
        engine = _engine(env)
        with (
            patch(
                "src.core.backup_engine.save_integrity_manifest",
                side_effect=OSError("Permission denied"),
            ),
            pytest.raises(OSError, match="Permission denied"),
        ):
            engine.run_backup(profile)


# ---------------------------------------------------------------------------
# 4. Verify phase failures
# ---------------------------------------------------------------------------


class TestVerifyFailures:

    def test_verification_mismatch_fails_backup(self, env, profile):
        """Verification failure must fail the entire backup."""
        engine = _engine(env)
        with (
            patch(
                "src.core.backup_engine.verify_backup",
                return_value=(False, "Verification failed: 1/2 errors\n  - Mismatch: a.txt"),
            ),
            pytest.raises(RuntimeError, match="Verification failed"),
        ):
            engine.run_backup(profile)


# ---------------------------------------------------------------------------
# 5. Encrypt phase failures
# ---------------------------------------------------------------------------


class TestEncryptFailures:

    def test_encryption_failure_propagates(self, env, profile):
        """Encryption error should bubble up from the pipeline."""
        profile.encrypt_primary = True
        profile.encryption = EncryptionConfig(enabled=True, stored_password="secret")

        engine = _engine(env)
        with (
            patch(
                "src.core.backup_engine.encrypt_backup",
                side_effect=OSError("disk full"),
            ),
            pytest.raises(OSError, match="disk full"),
        ):
            engine.run_backup(profile)


# ---------------------------------------------------------------------------
# 6. Mirror phase failures — isolation between mirrors
# ---------------------------------------------------------------------------


class TestMirrorFailures:

    def test_mirror1_fails_both_attempted_then_raises(self, env, profile):
        """Mirror 1 fails, Mirror 2 still attempted, then backup fails."""
        mirror1_dir = env["dest"] / "mirror1"
        mirror2_dir = env["dest"] / "mirror2"
        mirror1_dir.mkdir()
        mirror2_dir.mkdir()

        mirror1 = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path=str(mirror1_dir),
        )
        mirror2 = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path=str(mirror2_dir),
        )
        profile.mirror_destinations = [mirror1, mirror2]

        copy_calls = {"count": 0}
        original_copy = __import__(
            "src.core.phases.mirror", fromlist=["_copy_local_mirror"]
        )._copy_local_mirror

        def patched_copy(backup_path, backend, backup_name, phase_log, cancel_check=None):
            copy_calls["count"] += 1
            if copy_calls["count"] == 1:
                raise RuntimeError("mirror1 down")
            original_copy(backup_path, backend, backup_name, phase_log, cancel_check)

        engine = _engine(env)
        with (
            patch("src.core.phases.mirror._copy_local_mirror", patched_copy),
            pytest.raises(RuntimeError, match="Mirror upload failed"),
        ):
            engine.run_backup(profile)

        # Both mirrors were attempted
        assert copy_calls["count"] == 2


# ---------------------------------------------------------------------------
# 7. Rotate phase failures
# ---------------------------------------------------------------------------


class TestRotateFailures:

    def test_permission_denied_on_delete(self, env, profile):
        """Rotation should log errors but not crash if delete fails."""
        mock_backend = MagicMock()
        mock_backend.list_backups.return_value = [
            {"name": "old_backup", "modified": 1000000},
        ]
        mock_backend.delete_backup.side_effect = PermissionError("denied")

        engine = _engine(env)
        with patch.object(
            BackupEngine,
            "_get_backend",
            return_value=mock_backend,
        ):
            result = engine.run_backup(profile)
            # Pipeline completes; rotation simply could not delete
            assert result.files_processed == 2


# ---------------------------------------------------------------------------
# 8. Cancellation at each phase
# ---------------------------------------------------------------------------


class TestCancellation:

    @pytest.mark.parametrize(
        "phase_method",
        [
            "_phase_collect",
            "_phase_write",
            "_phase_verify",
            "_phase_encrypt",
            "_phase_mirror",
            "_phase_rotate",
        ],
    )
    def test_cancel_at_phase(self, env, profile, phase_method):
        """Cancelling at any phase should raise CancelledError."""
        # Enable encryption so _phase_encrypt is reached
        profile.encrypt_primary = True
        profile.encryption = EncryptionConfig(enabled=True, stored_password="pw")
        profile.mirror_destinations = [
            StorageConfig(storage_type=StorageType.LOCAL, destination_path=str(env["dest"] / "m")),
        ]

        engine = _engine(env)
        original = getattr(engine, phase_method)

        def cancel_then_run(ctx):
            engine.cancel()
            return original(ctx)

        with (
            patch.object(engine, phase_method, side_effect=cancel_then_run),
            pytest.raises(CancelledError),
        ):
            engine.run_backup(profile)


# ---------------------------------------------------------------------------
# 9. Empty backup — all files filtered out
# ---------------------------------------------------------------------------


class TestEmptyBackup:

    def test_all_files_excluded(self, env, profile):
        """If all files are excluded, pipeline should finish with zero
        files processed and no backup created."""
        profile.exclude_patterns = ["*.txt"]
        engine = _engine(env)
        result = engine.run_backup(profile)
        assert result.files_found == 0
        assert result.files_processed == 0

    def test_differential_no_changes(self, env, profile):
        """Full then differential with no changes should skip all."""
        profile.backup_type = BackupType.FULL
        engine = _engine(env)
        engine.run_backup(profile)  # Full writes the manifest

        profile.backup_type = BackupType.DIFFERENTIAL
        result = engine.run_backup(profile)
        assert result.files_processed == 0
        assert result.files_skipped == 2


# ---------------------------------------------------------------------------
# 10. Verify mismatch stops the entire pipeline (no mirror, no rotate)
# ---------------------------------------------------------------------------


class TestVerifyStopsPipeline:

    def test_verify_mismatch_prevents_mirror_and_rotate(self, env, profile):
        """A verify mismatch must stop the pipeline before mirror/rotate."""
        profile.mirror_destinations = [
            StorageConfig(
                storage_type=StorageType.LOCAL, destination_path=str(env["dest"] / "mirror")
            ),
        ]
        mock_backend = MagicMock()
        mock_backend.list_backups.return_value = []
        mock_backend.upload.return_value = None

        engine = _engine(env)
        with (
            patch(
                "src.core.backup_engine.verify_backup",
                return_value=(False, "Verification failed: 1/2 errors\n  - Mismatch: a.txt"),
            ),
            patch.object(
                BackupEngine,
                "_get_backend",
                return_value=mock_backend,
            ),
            pytest.raises(RuntimeError, match="Verification failed"),
        ):
            engine.run_backup(profile)

        # Mirror was NOT reached — upload never called
        mock_backend.upload.assert_not_called()
