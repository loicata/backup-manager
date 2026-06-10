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

    def test_all_sources_missing_raises(self, env, profile):
        """Every source unreachable must FAIL loudly, not report a green
        0-file success — a dead source drive would otherwise produce
        'successful' empty runs forever, masking total data loss."""
        profile.source_paths = [str(env["dest"] / "nonexistent")]
        engine = _engine(env)
        with pytest.raises(RuntimeError, match="No backup source is available"):
            engine.run_backup(profile)

    def test_empty_existing_source_succeeds(self, env, profile):
        """A source that EXISTS but is genuinely empty is a legitimate
        no-op success (distinct from an unreachable source)."""
        empty = env["dest"].parent / "empty_src"
        empty.mkdir()
        profile.source_paths = [str(empty)]
        engine = _engine(env)
        result = engine.run_backup(profile)
        assert result.files_found == 0
        assert result.success is True

    def test_partial_missing_source_warns_but_succeeds(self, env, profile):
        """One missing source among several present ones: the run backs up
        what it can and surfaces a warning for the missing one."""
        profile.source_paths = [str(env["source"]), str(env["dest"] / "ghost")]
        engine = _engine(env)
        result = engine.run_backup(profile)
        assert result.files_found == 2  # a.txt + b.txt from the present source
        assert result.success is True
        assert result.warnings >= 1

    def test_permission_denied_on_source(self, env, profile):
        """Collector should skip unreadable directories gracefully.

        The source path EXISTS (only scandir fails), so this is not the
        all-sources-missing case — the run completes with zero files
        rather than raising."""
        with patch("os.scandir", side_effect=PermissionError("denied")):
            engine = _engine(env)
            result = engine.run_backup(profile)
            assert result.files_found == 0


# ---------------------------------------------------------------------------
# 1b. Concurrent-run protection
# ---------------------------------------------------------------------------


class TestConcurrentRunProtection:
    """Only one backup at a time per profile must be permitted.

    Without the per-profile lock, a scheduled run firing while the user
    has also clicked "Run now" would read ``last_backup_completed=False``
    and delete the in-flight backup via the incomplete-cleanup path.
    """

    def test_second_run_rejected_while_lock_held(self, env, profile):
        """A stale-but-alive lock blocks the run with ProfileLockError."""
        from src.core.profile_lock import ProfileLockError

        engine = _engine(env)
        lock_path = engine._profile_lock_path(profile.id)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        # Simulate another live run holding the lock.
        foreign_pid = 424242
        lock_path.write_text(str(foreign_pid))

        with (
            patch("src.core.profile_lock._pid_alive", return_value=True),
            pytest.raises(ProfileLockError, match="Another backup"),
        ):
            engine.run_backup(profile)

        # The foreign lock must remain untouched so the legitimate
        # holder's release keeps working.
        assert int(lock_path.read_text()) == foreign_pid

    def test_lock_released_after_successful_run(self, env, profile):
        """A completed backup removes its own lock file."""
        engine = _engine(env)
        lock_path = engine._profile_lock_path(profile.id)
        engine.run_backup(profile)
        assert not lock_path.exists()

    def test_lock_released_after_failed_run(self, env, profile):
        """A failed backup still releases its lock (finally block)."""
        engine = _engine(env)
        lock_path = engine._profile_lock_path(profile.id)
        with (
            patch("shutil.copy2", side_effect=OSError("disk full")),
            pytest.raises(Exception, match="disk full"),
        ):
            engine.run_backup(profile)
        assert not lock_path.exists()

    def test_stale_lock_from_dead_pid_is_taken_over(self, env, profile):
        """A crashed prior run leaves a lock; a new run must proceed."""
        engine = _engine(env)
        lock_path = engine._profile_lock_path(profile.id)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text("99999999")  # Simulated dead PID.

        with patch("src.core.profile_lock._pid_alive", return_value=False):
            result = engine.run_backup(profile)

        assert result.files_processed > 0
        assert not lock_path.exists()


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
        """Pipeline should NOT raise when integrity manifest write fails.

        Previously, an OSError from ``save_integrity_manifest`` aborted
        the run AFTER the backup bytes were already on disk; the orphan
        scan would then delete the just-written backup at the next
        startup, losing the user's data over a transient I/O glitch
        (locked file, antivirus, NAS hiccup).

        The contract since v3.5.6 mirrors the remote-upload path: log a
        structured warning on the result so the user knows
        post-restore verification is no longer available, but keep the
        backup. This test pins that contract — a regression toward
        "raise and lose the backup" would be a data-loss bug.
        """
        engine = _engine(env)
        with patch(
            "src.core.backup_engine.save_integrity_manifest",
            side_effect=OSError("Permission denied"),
        ):
            # MUST NOT raise — the backup is intact, only the
            # manifest sidecar failed.
            result = engine.run_backup(profile)

        # The warning must be surfaced on the result for the report
        # generator and the UI to display.
        assert result.warnings >= 1
        manifest_warnings = [
            w for w in result.phase_errors if w.phase == "manifest" and "verification" in w.message
        ]
        assert manifest_warnings, (
            "Expected a structured 'manifest could not be saved' warning "
            f"on the result, got: {[w.message for w in result.phase_errors]}"
        )


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
        """Encryption error during write should bubble up from the pipeline."""
        profile.encrypt_primary = True
        profile.encryption = EncryptionConfig(enabled=True, stored_password="secret1234567890")

        engine = _engine(env)
        with (
            patch(
                "src.security.encryption.EncryptingWriter",
                side_effect=OSError("disk full"),
            ),
            pytest.raises(Exception, match="disk full"),
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


class TestCancelClearsCrashRecoveryFlags:
    """v3.7.11 regression guard.

    Pre-v3.7.11, a user-initiated cancel left
    ``profile.last_backup_completed=False`` and
    ``profile.incomplete_backup_name`` populated. On the next app
    launch ``InAppScheduler._check_startup_missed`` saw those flags
    and re-fired the backup as crash-recovery — the 17/05/2026 case
    where v3.7.10's install was preceded by a cancelled run and the
    user found the backup auto-running on the next launch.

    The fix lives in the ``except CancelledError`` block of
    ``run_backup``: ``_mark_cancelled`` clears the flags so the
    persistent state matches the user's intent (no pending work).
    """

    def test_cancel_clears_last_backup_completed_flag(self, env, profile):
        """``last_backup_completed`` must be True after a clean cancel."""
        engine = _engine(env)
        original = engine._phase_write

        def cancel_then_run(ctx):
            engine.cancel()
            return original(ctx)

        with (
            patch.object(engine, "_phase_write", side_effect=cancel_then_run),
            pytest.raises(CancelledError),
        ):
            engine.run_backup(profile)

        # In-memory flag (the engine just flipped it)
        assert profile.last_backup_completed is True, (
            "User cancel must clear the interrupt-recovery flag — otherwise "
            "the next app launch will treat the run as a crash and auto-fire "
            "the backup again."
        )

    def test_cancel_clears_incomplete_backup_name(self, env, profile):
        """``incomplete_backup_name`` must be empty after a clean cancel."""
        engine = _engine(env)
        original = engine._phase_write

        def cancel_then_run(ctx):
            engine.cancel()
            return original(ctx)

        with (
            patch.object(engine, "_phase_write", side_effect=cancel_then_run),
            pytest.raises(CancelledError),
        ):
            engine.run_backup(profile)

        assert profile.incomplete_backup_name == "", (
            "User cancel must clear the incomplete-backup pointer — "
            "_check_startup_missed reads this field to decide whether to "
            "auto-fire a crash-recovery backup."
        )

    def test_cancel_resets_crash_recovery_attempts(self, env, profile):
        """The crash-recovery circuit breaker counter must be cleared.

        Without this reset, a sequence of user-cancels would leave the
        counter incremented from prior crash-recovery attempts forever,
        eventually tripping ``MAX_CRASH_RECOVERY_ATTEMPTS`` and
        permanently disabling auto-recovery for real crashes.
        """
        profile.crash_recovery_attempts = 2
        engine = _engine(env)
        original = engine._phase_write

        def cancel_then_run(ctx):
            engine.cancel()
            return original(ctx)

        with (
            patch.object(engine, "_phase_write", side_effect=cancel_then_run),
            pytest.raises(CancelledError),
        ):
            engine.run_backup(profile)

        assert profile.crash_recovery_attempts == 0

    def test_cancel_persists_cleared_flags_on_disk(self, env, profile):
        """The reset must be written to the profile JSON, not only to
        the in-memory object — ``_check_startup_missed`` reads the
        next session's freshly-loaded profile, which lives on disk.
        """
        env["config_manager"].save_profile(profile)  # baseline on disk
        engine = _engine(env)
        original = engine._phase_write

        def cancel_then_run(ctx):
            engine.cancel()
            return original(ctx)

        with (
            patch.object(engine, "_phase_write", side_effect=cancel_then_run),
            pytest.raises(CancelledError),
        ):
            engine.run_backup(profile)

        # Read back the saved profile from disk to assert persistence.
        all_profiles = env["config_manager"].get_all_profiles()
        reloaded = next((p for p in all_profiles if p.id == profile.id), None)
        assert reloaded is not None
        assert reloaded.last_backup_completed is True
        assert reloaded.incomplete_backup_name == ""


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


# ---------------------------------------------------------------------------
# 11. Auto-promotion rollback on failure
# ---------------------------------------------------------------------------


class TestBackupTypeRollbackOnFailure:
    """When the pipeline auto-promotes a DIFF to FULL and then crashes,
    the on-disk profile must not keep the promoted ``FULL`` value —
    otherwise the next run would skip the DIFF → FULL evaluation and
    produce a FULL backup indefinitely."""

    def test_diff_promoted_to_full_rolled_back_on_crash(self, env, profile):
        profile.backup_type = BackupType.DIFFERENTIAL
        profile.last_full_backup = None  # No prior full forces promotion on first run
        env["config_manager"].save_profile(profile)

        engine = _engine(env)

        # Make the write phase crash AFTER _maybe_force_full ran
        def _boom(_ctx):
            raise RuntimeError("simulated write failure")

        with (
            patch.object(BackupEngine, "_phase_write", side_effect=_boom),
            pytest.raises(RuntimeError, match="simulated"),
        ):
            engine.run_backup(profile)

        # Reload profile from disk and confirm backup_type is restored.
        loaded = next(p for p in env["config_manager"].get_all_profiles() if p.id == profile.id)
        assert loaded.backup_type == BackupType.DIFFERENTIAL, (
            "backup_type must be rolled back to DIFFERENTIAL after a " "failed auto-promoted run"
        )
