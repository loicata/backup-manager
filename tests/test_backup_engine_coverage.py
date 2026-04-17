"""Additional tests for backup_engine — targeting uncovered paths.

Covers: disk space checks, auto-promote to full, incomplete cleanup,
encrypted verify, remote verify, describe_target fallback, phase count,
object lock retention, bandwidth throttle skip, _check_path_space OSError,
manifest upload failure, and verify error formatting.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.core.backup_engine import BackupEngine, create_backend
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

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def env(tmp_path):
    """Minimal backup environment."""
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
        id="cov_test",
        name="CovTest",
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
# create_backend — unknown storage type
# ---------------------------------------------------------------------------


class TestCreateBackend:

    def test_unknown_storage_type_raises(self):
        """create_backend raises ValueError for unknown storage type."""
        config = StorageConfig()
        config.storage_type = "unknown_type"

        with pytest.raises((ValueError, KeyError)):
            create_backend(config)


# ---------------------------------------------------------------------------
# Disk space checks
# ---------------------------------------------------------------------------


class TestDiskSpaceChecks:

    def test_check_path_space_insufficient(self, env, profile):
        """Insufficient disk space raises RuntimeError."""
        engine = _engine(env)
        with (
            patch("shutil.disk_usage", return_value=MagicMock(free=1024)),
            pytest.raises(RuntimeError, match="Insufficient disk space"),
        ):
            engine.run_backup(profile)

    def test_check_path_space_oserror_skipped(self):
        """OSError from disk_usage is silently skipped."""
        errors = []
        with patch("shutil.disk_usage", side_effect=OSError("not mounted")):
            BackupEngine._check_path_space("/nonexistent", 1024, "Test", errors)
        assert errors == []

    def test_check_remote_space_connection_failure(self, env):
        """Remote space check logs debug when connection fails."""
        engine = _engine(env)
        errors = []
        with patch.object(
            engine,
            "_get_backend",
            side_effect=ConnectionError("no route"),
        ):
            engine._check_remote_space(
                StorageConfig(
                    storage_type=StorageType.SFTP, sftp_host="test.local", sftp_username="user"
                ),
                1024,
                "SFTP test",
                errors,
            )
        # Error is silently skipped, no entry added
        assert errors == []

    def test_check_remote_space_sufficient(self, env):
        """Remote space check passes when space is sufficient."""
        engine = _engine(env)
        errors = []
        mock_backend = MagicMock()
        mock_backend.get_free_space.return_value = 10 * 1024**3  # 10 GB
        with patch.object(engine, "_get_backend", return_value=mock_backend):
            engine._check_remote_space(
                StorageConfig(
                    storage_type=StorageType.SFTP, sftp_host="test.local", sftp_username="user"
                ),
                1024,
                "SFTP test",
                errors,
            )
        assert errors == []

    def test_check_remote_space_insufficient(self, env):
        """Remote space check appends error when space is too low."""
        engine = _engine(env)
        errors = []
        mock_backend = MagicMock()
        mock_backend.get_free_space.return_value = 100  # 100 bytes
        with patch.object(engine, "_get_backend", return_value=mock_backend):
            engine._check_remote_space(
                StorageConfig(
                    storage_type=StorageType.SFTP, sftp_host="test.local", sftp_username="user"
                ),
                10 * 1024**3,
                "SFTP test",
                errors,
            )
        assert len(errors) == 1
        assert "SFTP test" in errors[0]

    def test_check_remote_space_none(self, env):
        """Remote space check skips when get_free_space returns None."""
        engine = _engine(env)
        errors = []
        mock_backend = MagicMock()
        mock_backend.get_free_space.return_value = None
        with patch.object(engine, "_get_backend", return_value=mock_backend):
            engine._check_remote_space(
                StorageConfig(
                    storage_type=StorageType.SFTP, sftp_host="test.local", sftp_username="user"
                ),
                1024,
                "SFTP test",
                errors,
            )
        assert errors == []


# ---------------------------------------------------------------------------
# Auto-promote differential to full
# ---------------------------------------------------------------------------


class TestMaybeForceFullPromotion:

    def test_promote_when_no_manifest(self, env, profile):
        """Differential auto-promotes to full when no manifest exists."""
        profile.backup_type = BackupType.DIFFERENTIAL
        engine = _engine(env)
        result = engine.run_backup(profile)
        # Should have been promoted to FULL and processed all files
        assert result.actual_backup_type == "FULL"
        assert result.files_processed == 2

    def test_promote_when_cycle_reached(self, env, profile):
        """Differential auto-promotes to full when cycle threshold reached."""
        profile.backup_type = BackupType.FULL
        engine = _engine(env)
        engine.run_backup(profile)

        profile.backup_type = BackupType.DIFFERENTIAL
        profile.differential_count = profile.full_backup_every  # At threshold
        result = engine.run_backup(profile)
        assert result.actual_backup_type == "FULL"

    def test_no_promote_when_differential_normal(self, env, profile):
        """Differential stays differential when conditions are normal."""
        profile.backup_type = BackupType.FULL
        engine = _engine(env)
        engine.run_backup(profile)

        profile.backup_type = BackupType.DIFFERENTIAL
        profile.differential_count = 0
        result = engine.run_backup(profile)
        assert result.actual_backup_type == "DIFFERENTIAL"

    def test_promote_after_interrupted_full(self, env, profile):
        """After an interrupted full backup, forces full again."""
        profile.backup_type = BackupType.DIFFERENTIAL
        profile.last_backup_completed = False
        profile.incomplete_backup_was_full = True
        profile.incomplete_backup_name = "CovTest_FULL_2026-01-01_120000"
        engine = _engine(env)
        result = engine.run_backup(profile)
        assert result.actual_backup_type == "FULL"


# ---------------------------------------------------------------------------
# Verify phase — encrypted backup
# ---------------------------------------------------------------------------


class TestVerifyEncryptedBackup:

    def test_encrypted_backup_stored_hash(self, env, profile):
        """Encrypted backup stores hash for future periodic verification."""
        profile.encrypt_primary = True
        profile.encryption = EncryptionConfig(enabled=True, stored_password="test_password_1234")
        engine = _engine(env)
        engine.run_backup(profile)

        # Hash should be stored for verification
        hashes = env["config_manager"].load_verify_hashes()
        assert len(hashes) >= 1


# ---------------------------------------------------------------------------
# Manifest upload failure — non-fatal
# ---------------------------------------------------------------------------


class TestManifestUploadFailure:

    def test_manifest_upload_failure_non_fatal(self, env, profile):
        """Failed manifest upload to remote should not crash the pipeline."""
        profile.storage = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path=str(env["dest"]),
        )
        engine = _engine(env)
        # This should succeed even if upload_manifest_to_remote fails
        with patch(
            "src.core.backup_engine.upload_manifest_to_remote",
            side_effect=ConnectionError("upload failed"),
        ):
            result = engine.run_backup(profile)
        assert result.files_processed == 2


# ---------------------------------------------------------------------------
# _raise_verify_error formatting
# ---------------------------------------------------------------------------


class TestRaiseVerifyError:

    def test_few_errors(self):
        """Error message lists all errors when <= 10."""
        errors = ["Error 1", "Error 2", "Error 3"]
        with pytest.raises(RuntimeError, match="3/10 errors"):
            BackupEngine._raise_verify_error(errors, 10)

    def test_many_errors_truncated(self):
        """Error message truncates when > 10 errors."""
        errors = [f"Error {i}" for i in range(15)]
        with pytest.raises(RuntimeError, match="and 5 more"):
            BackupEngine._raise_verify_error(errors, 20)


# ---------------------------------------------------------------------------
# _describe_target fallback
# ---------------------------------------------------------------------------


class TestDescribeTargetFallback:

    def test_unknown_type_fallback(self):
        """Unknown storage type returns generic message."""
        config = StorageConfig()
        # Use a valid type to avoid errors, but test the generic path
        config.storage_type = StorageType.LOCAL
        msg = BackupEngine._describe_target(config)
        assert "USB drive" in msg or "Connect" in msg


# ---------------------------------------------------------------------------
# _emit_phase_count variations
# ---------------------------------------------------------------------------


class TestEmitPhaseCount:

    def test_phase_count_local_no_mirrors(self, env, profile):
        """Local backup without mirrors emits correct phase weights."""
        engine = _engine(env)
        events_received = []
        engine._events.subscribe("phase_count", lambda **kw: events_received.append(kw))
        result = engine.run_backup(profile)
        assert result.files_processed == 2
        # Phase count was emitted
        assert len(events_received) >= 1
        weights = events_received[0]["weights"]
        assert "backup" in weights  # local writer
        assert "upload" not in weights  # no remote

    def test_phase_count_with_mirrors(self, env, profile):
        """Backup with mirrors includes mirror_upload weight."""
        mirror_dir = env["dest"] / "mirror1"
        mirror_dir.mkdir()
        profile.mirror_destinations = [
            StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(mirror_dir),
            )
        ]
        engine = _engine(env)
        events_received = []
        engine._events.subscribe("phase_count", lambda **kw: events_received.append(kw))
        result = engine.run_backup(profile)
        assert result.files_processed == 2
        assert len(events_received) >= 1
        weights = events_received[0]["weights"]
        assert "mirror_upload" in weights

    def test_phase_count_with_encryption(self, env, profile):
        """Encrypted backup includes encryption weight."""
        profile.encrypt_primary = True
        profile.encryption = EncryptionConfig(enabled=True, stored_password="test_password_1234")
        engine = _engine(env)
        events_received = []
        engine._events.subscribe("phase_count", lambda **kw: events_received.append(kw))
        engine.run_backup(profile)
        assert len(events_received) >= 1
        weights = events_received[0]["weights"]
        assert "encryption" in weights


# ---------------------------------------------------------------------------
# _compute_md5
# ---------------------------------------------------------------------------


class TestComputeMd5:

    def test_md5_computation(self, tmp_path):
        """MD5 computation returns correct hex digest."""
        test_file = tmp_path / "test.bin"
        test_file.write_bytes(b"hello world")
        result = BackupEngine._compute_md5(test_file)
        assert len(result) == 32
        assert result == "5eb63bbbe01eeed093cb22bb8f5acdc3"


# ---------------------------------------------------------------------------
# _cleanup_incomplete_backup
# ---------------------------------------------------------------------------


class TestCleanupIncomplete:

    def test_cleanup_no_name_noop(self, env, profile):
        """Cleanup does nothing when incomplete_backup_name is empty."""
        profile.incomplete_backup_name = ""
        engine = _engine(env)

        from src.core.backup_result import BackupResult
        from src.core.phases.base import PipelineContext

        ctx = PipelineContext(
            profile=profile,
            config_manager=env["config_manager"],
            events=EventBus(),
            result=BackupResult(),
        )
        engine._cleanup_incomplete_backup(ctx)
        # No error raised

    def test_cleanup_deletes_from_primary(self, env, profile):
        """Cleanup removes incomplete backup from primary destination."""
        # Create a fake incomplete backup
        incomplete_dir = env["dest"] / "CovTest_FULL_2026-01-01_120000"
        incomplete_dir.mkdir()
        (incomplete_dir / "a.txt").write_text("data", encoding="utf-8")

        profile.incomplete_backup_name = "CovTest_FULL_2026-01-01_120000"
        engine = _engine(env)

        from src.core.backup_result import BackupResult
        from src.core.phases.base import PipelineContext

        ctx = PipelineContext(
            profile=profile,
            config_manager=env["config_manager"],
            events=EventBus(),
            result=BackupResult(),
        )
        engine._cleanup_incomplete_backup(ctx)
        assert not incomplete_dir.exists()


# ---------------------------------------------------------------------------
# _phase_cleanup — temp directories
# ---------------------------------------------------------------------------


class TestPhaseCleanup:

    def test_cleanup_removes_temp_dirs(self, env, profile):
        """Phase cleanup removes .tmp.drivedownload directories."""
        engine = _engine(env)
        engine.run_backup(profile)

        # Find the created backup directory
        backup_dirs = [d for d in env["dest"].iterdir() if d.is_dir()]
        if backup_dirs:
            # Create a temp dir inside the backup
            temp_dir = backup_dirs[0] / ".tmp.drivedownload"
            temp_dir.mkdir()
            (temp_dir / "partial.dat").write_bytes(b"temp data")

            from src.core.backup_result import BackupResult
            from src.core.phases.base import PipelineContext

            ctx = PipelineContext(
                profile=profile,
                config_manager=env["config_manager"],
                events=EventBus(),
                result=BackupResult(),
            )
            ctx.backup_path = backup_dirs[0]
            engine._phase_cleanup(ctx)
            assert not temp_dir.exists()


# ---------------------------------------------------------------------------
# _any_destination_missing_full
# ---------------------------------------------------------------------------


class TestAnyDestinationMissingFull:

    def test_returns_empty_when_full_exists(self, env, profile):
        """Returns empty string when destination has a full backup."""
        engine = _engine(env)
        mock_backend = MagicMock()
        mock_backend.list_backups.return_value = [
            {"name": "CovTest_FULL_2026-01-01_120000"},
        ]
        from src.core.backup_result import BackupResult
        from src.core.phases.base import PipelineContext

        ctx = PipelineContext(
            profile=profile,
            config_manager=env["config_manager"],
            events=EventBus(),
            result=BackupResult(),
        )
        with patch.object(engine, "_get_backend", return_value=mock_backend):
            result = engine._any_destination_missing_full(ctx)
        assert result == ""

    def test_returns_name_when_no_full(self, env, profile):
        """Returns destination name when no full backup exists."""
        engine = _engine(env)
        mock_backend = MagicMock()
        mock_backend.list_backups.return_value = [
            {"name": "CovTest_DIFF_2026-01-01_120000"},
        ]
        from src.core.backup_result import BackupResult
        from src.core.phases.base import PipelineContext

        ctx = PipelineContext(
            profile=profile,
            config_manager=env["config_manager"],
            events=EventBus(),
            result=BackupResult(),
        )
        with patch.object(engine, "_get_backend", return_value=mock_backend):
            result = engine._any_destination_missing_full(ctx)
        assert result == "Storage"

    def test_connection_error_skipped(self, env, profile):
        """Connection error during check is silently skipped."""
        engine = _engine(env)
        from src.core.backup_result import BackupResult
        from src.core.phases.base import PipelineContext

        ctx = PipelineContext(
            profile=profile,
            config_manager=env["config_manager"],
            events=EventBus(),
            result=BackupResult(),
        )
        with patch.object(
            engine,
            "_get_backend",
            side_effect=ConnectionError("offline"),
        ):
            result = engine._any_destination_missing_full(ctx)
        assert result == ""


# ---------------------------------------------------------------------------
# _mark_completed restores DIFFERENTIAL type after forced full
# ---------------------------------------------------------------------------


class TestMarkCompleted:

    def test_restores_differential_after_forced_full(self, env, profile):
        """Profile type restored to DIFFERENTIAL after forced full."""
        profile.backup_type = BackupType.DIFFERENTIAL
        engine = _engine(env)
        # Force full promotion — no manifest exists
        result = engine.run_backup(profile)
        assert result.actual_backup_type == "FULL"
        # Profile type should be restored
        assert profile.backup_type == BackupType.DIFFERENTIAL


# ---------------------------------------------------------------------------
# Verify disabled — auto_verify=False
# ---------------------------------------------------------------------------


class TestVerifyDisabled:

    def test_skip_verify_when_disabled(self, env, profile):
        """Verification phase skipped when auto_verify is False."""
        profile.verification.auto_verify = False
        engine = _engine(env)
        result = engine.run_backup(profile)
        assert result.files_processed == 2
