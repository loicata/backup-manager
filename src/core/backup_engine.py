"""Backup engine — orchestrates the pipeline.

Delegates each phase to its dedicated module. Supports cancellation
between phases and emits events for UI progress tracking.

Uses PipelineContext to pass state between phases, and BackupResult
for error accumulation.
"""

import contextlib
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from src.core.backup_result import BackupResult
from src.core.bandwidth_tester import compute_throttle_kbps, measure_bandwidth
from src.core.config import (
    BackupProfile,
    BackupType,
    StorageConfig,
    StorageType,
    compute_profile_hash,
)
from src.core.events import (
    BACKUP_DONE,
    BACKUP_TYPE_DETERMINED,
    ERROR,
    LOG,
    PHASE_CHANGED,
    PHASE_COUNT,
    PROGRESS,
    STATUS,
    EventBus,
)
from src.core.exceptions import CancelledError
from src.core.phases.base import PipelineContext
from src.core.phases.collector import collect_files
from src.core.phases.filter import (
    build_updated_manifest,
    filter_changed_files,
    load_manifest,
    save_manifest,
)
from src.core.phases.local_writer import generate_backup_name, sanitize_profile_name
from src.core.phases.manifest import (
    build_integrity_manifest,
    save_integrity_manifest,
    upload_manifest_to_remote,
)
from src.core.phase_logger import PhaseLogger
from src.core.phases.mirror import mirror_backup
from src.core.phases.rotator import rotate_backups
from src.core.phases.verifier import verify_backup
from src.core.phases.writer import write_backup
from src.core.profile_lock import ProfileLockError, acquire, release
from src.security.secure_memory import SecurePassword
from src.storage.base import StorageBackend


def _parse_iso_datetime(value: str | None) -> datetime | None:
    """Parse an ISO-formatted datetime string, returning None on failure."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def _is_full_due_by_schedule(profile: BackupProfile, now: datetime) -> bool:
    """Return True if the calendar schedule requires a FULL run now.

    Compares the last recorded FULL backup against the current time using
    the profile's ``full_schedule_mode``. Returns True when:

    - monthly: the last FULL was in a different calendar month
    - weekly: the last FULL was in a different ISO week
    - daily: the last FULL was on a different calendar date
    - no previous FULL has ever been recorded

    Missed windows are caught automatically on the next run after the
    target date passes — there is no "skip" mode.
    """
    last_full = _parse_iso_datetime(profile.last_full_backup)
    if last_full is None:
        return True

    mode = profile.full_schedule_mode
    if mode == "monthly":
        return (last_full.year, last_full.month) != (now.year, now.month)
    if mode == "weekly":
        return last_full.isocalendar()[:2] != now.isocalendar()[:2]
    if mode == "daily":
        return last_full.date() != now.date()
    return False


logger = logging.getLogger(__name__)


def _effective_auto_verify(profile: BackupProfile) -> bool:
    """Compute whether post-copy verification runs for this profile.

    Resolves the user's ``verification.auto_verify`` toggle against
    two force-on overrides:
    1. Remote primary storage (SFTP / S3) — time saved by skipping
       is negligible (~17 s for SFTP via PoC C sidecar, ~30 s for
       S3 ETag check) and silent corruption is harder to detect on
       a remote backend, so the safer default is to always verify.
       Network shares are NOT included: they go through the local
       pipeline (mounted as a drive letter), so the user-toggle
       applies to them like to any local destination.
    2. Object Lock (anti-ransomware) profiles — verification is part
       of the security contract; the anti-ransomware mode promises
       integrity guarantees that depend on post-copy verification.

    The UI greys out the "Verify integrity after backup" checkbox
    whenever either override applies, so the user is not surprised.

    Args:
        profile: BackupProfile to evaluate.

    Returns:
        True if post-copy verification should run, False if it
        should be skipped.
    """
    if profile.storage.is_remote():
        return True
    if profile.object_lock_enabled:
        return True
    return profile.verification.auto_verify


def _resolve_local_destination(storage: StorageConfig) -> str:
    """Resolve the local destination path using drive serial if needed.

    If the configured path is inaccessible and a device serial is saved,
    scans all drive letters to find the drive and rewrites the path.

    Args:
        storage: Storage configuration (may be mutated in-place).

    Returns:
        Resolved destination path.
    """
    from src.storage.drive_serial import resolve_local_path

    resolved = resolve_local_path(storage.destination_path, storage.device_serial)
    if resolved != storage.destination_path:
        storage.destination_path = resolved
    return resolved


def create_backend(storage: StorageConfig) -> StorageBackend:
    """Create a storage backend from a StorageConfig.

    Args:
        storage: Storage configuration.

    Returns:
        Configured StorageBackend instance.

    Raises:
        ValueError: If the storage type is unknown.
    """
    from src.storage.local import LocalStorage
    from src.storage.network import NetworkStorage
    from src.storage.s3 import S3Storage
    from src.storage.sftp import SFTPStorage

    builders = {
        StorageType.LOCAL: lambda s: LocalStorage(_resolve_local_destination(s)),
        StorageType.NETWORK: lambda s: NetworkStorage(
            destination_path=s.destination_path,
            username=s.network_username,
            password=s.network_password,
        ),
        StorageType.SFTP: lambda s: SFTPStorage(
            host=s.sftp_host,
            port=s.sftp_port,
            username=s.sftp_username,
            password=s.sftp_password,
            key_path=s.sftp_key_path,
            key_passphrase=s.sftp_key_passphrase,
            remote_path=s.sftp_remote_path,
        ),
        StorageType.S3: lambda s: S3Storage(
            bucket=s.s3_bucket,
            prefix=s.s3_prefix,
            region=s.s3_region,
            access_key=s.s3_access_key,
            secret_key=s.s3_secret_key,
            endpoint_url=s.s3_endpoint_url,
            provider=s.s3_provider,
        ),
    }
    builder = builders.get(storage.storage_type)
    if builder is None:
        raise ValueError(f"Unknown storage type: {storage.storage_type}")
    return builder(storage)


def delete_profile_backups(
    profile_name: str,
    storage_configs: list[StorageConfig],
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> tuple[int, list[str]]:
    """Delete all backups created by a profile across all destinations.

    Operates in two passes so the optional ``progress_callback`` can
    drive a determinate progress bar:

    1. Enumerate matching backups across every reachable backend
       (``list_backups`` per backend, filter by prefix).  Backends that
       fail to list are skipped silently and their share of the total
       is omitted; the failure surfaces in the returned error list.
    2. Delete each matching backup. Before each ``delete_backup`` call
       the callback is invoked with ``(current, total, name)`` so the
       UI can paint a 0..total bar instead of an indeterminate
       "deleting…" placeholder.

    Args:
        profile_name: Human-readable profile name.
        storage_configs: List of storage configurations to clean.
        progress_callback: Optional callable receiving
            ``(current, total, name)`` where ``current`` is the 1-based
            index of the backup being deleted across all destinations
            and ``total`` is the precomputed grand total.

    Returns:
        Tuple of (total_deleted, error_messages).
    """
    prefix = sanitize_profile_name(profile_name) + "_"

    # Pass 1: build a flat plan of (backend, name) pairs across all
    # configs. Listing failures are recorded but do not abort — a
    # mirror that is offline should not block primary cleanup.
    plan: list[tuple[StorageBackend, str]] = []
    all_errors: list[str] = []

    for config in storage_configs:
        try:
            backend = create_backend(config)
            backups = backend.list_backups()
        except Exception as e:
            all_errors.append(f"{config.storage_type.value}: {e}")
            logger.warning("Backend listing error during cleanup: %s", e)
            continue

        for backup in backups:
            name = backup.get("name", "")
            if name.startswith(prefix):
                plan.append((backend, name))

    total = len(plan)
    total_deleted = 0
    current = 0

    # Pass 2: delete with progress reporting.  ``progress_callback`` is
    # called BEFORE the actual delete so the UI shows the file that is
    # currently being processed (matches user expectation of "Deleting
    # X" rather than the just-finished name).
    for backend, name in plan:
        current += 1
        if progress_callback is not None:
            try:
                progress_callback(current, total, name)
            except Exception as cb_err:
                # A misbehaving callback (Tk shutdown, etc.) must not
                # abort the deletion sweep — log and keep going.
                logger.debug("delete progress_callback raised: %s", cb_err)
        try:
            backend.delete_backup(name)
            total_deleted += 1
        except Exception as e:
            all_errors.append(f"{name}: {e}")
            logger.warning("Failed to delete %s: %s", name, e)

    return total_deleted, all_errors


class BackupEngine:
    """Orchestrates the backup pipeline.

    Creates a PipelineContext at the start of each run and passes it
    through all phases. Each phase reads from and writes to the context.
    """

    def __init__(
        self,
        config_manager,
        events: EventBus | None = None,
    ):
        self._config = config_manager
        self._events = events or EventBus()
        self._cancelled = False
        self._current_result: BackupResult | None = None
        self._events.subscribe(LOG, self._capture_log)

    def cancel(self) -> None:
        """Request cancellation of the current backup."""
        self._cancelled = True

    def run_backup(self, profile: BackupProfile) -> BackupResult:
        """Execute the full backup pipeline.

        Args:
            profile: Backup profile to execute.

        Returns:
            BackupResult with metrics and accumulated errors.

        Raises:
            CancelledError: If backup is cancelled by the user.
            RuntimeError: If backup fails.
        """
        self._cancelled = False
        # Apply any pending rollback left by a double-crash on the
        # previous run BEFORE the pipeline examines ``backup_type``.
        self._apply_pending_rollback(profile)
        # Wrap the events bus so every PROGRESS / LOG / STATUS / PHASE
        # event emitted downstream carries ``profile_id`` without each
        # call site having to know. The wrapper restores the original
        # bus in the ``finally`` block so a long-lived engine reused
        # across profiles does not bleed the previous profile's id
        # into the next run. The UI side filters PROGRESS / LOG /
        # STATUS / PHASE events whose ``profile_id`` does not match
        # the currently-selected profile in the sidebar — that's how
        # the Run tab stops conflating a background scheduler run on
        # profile A with the user looking at profile B in the
        # foreground (17/05/2026 user report).
        from src.core.events import ProfileTaggingEventBus

        original_events = self._events
        tagged_events = ProfileTaggingEventBus(original_events, profile.id)
        self._events = tagged_events
        ctx = PipelineContext(
            profile=profile,
            config_manager=self._config,
            events=tagged_events,
            result=BackupResult(),
        )
        self._current_result = ctx.result
        start_time = time.monotonic()

        # Acquire the per-profile run lock BEFORE any state mutation so
        # a concurrent run (scheduler + manual) cannot delete the
        # in-flight backup via the incomplete-cleanup path.
        lock_path = self._profile_lock_path(profile.id)
        try:
            acquire(lock_path)
        except ProfileLockError as e:
            self._log(f"Backup rejected: {e}")
            self._emit_status("error")
            self._events.emit(ERROR, exception=e, context="backup")
            raise

        # Remember the user-configured backup_type so we can roll back
        # if the pipeline crashes after ``_maybe_force_full`` has
        # temporarily flipped it to FULL. Without the rollback, a
        # crash during an auto-promoted run would persist
        # ``backup_type = FULL`` on disk, and the profile would stay
        # permanently promoted until the user manually edited it.
        original_backup_type = profile.backup_type
        try:
            # Validate storage configuration before starting the pipeline
            profile.storage.validate()
            for mirror in profile.mirror_destinations:
                mirror.validate()

            self._emit_status("running")
            self._run_pipeline(ctx)
            ctx.result.duration_seconds = time.monotonic() - start_time
            self._emit_status("success")
            self._events.emit(BACKUP_DONE, stats=ctx.result)
            # Final summary. The destination list was previously appended
            # after a ``→`` arrow — useful only on the very first run, and
            # noisy on every subsequent one since the destinations are
            # visible in the profile / Storage tab. The duration is shown
            # in minutes (with one decimal) because ``7831.8s`` is harder
            # to read at a glance than ``130.5 min``.
            self._log(
                f"Backup complete: {ctx.result.files_processed} files "
                f"in {ctx.result.duration_seconds / 60:.1f} min"
            )
            return ctx.result

        except CancelledError:
            ctx.result.duration_seconds = time.monotonic() - start_time
            self._log("Backup cancelled by user")
            self._emit_status("idle")
            self._rollback_backup_type_on_failure(ctx, original_backup_type)
            self._best_effort_cleanup(ctx)
            # User cancel is intentional, not a crash. Clear the
            # interrupt-recovery flags so ``_check_startup_missed``
            # does not auto-fire this backup again on the next launch
            # (see ``_mark_cancelled`` docstring for the 17/05/2026 case).
            self._mark_cancelled(ctx)
            raise

        except Exception as e:
            ctx.result.duration_seconds = time.monotonic() - start_time
            self._log(f"Backup failed: {e}")
            self._emit_status("error")
            self._events.emit(ERROR, exception=e, context="backup")
            self._rollback_backup_type_on_failure(ctx, original_backup_type)
            self._best_effort_cleanup(ctx)
            raise
        finally:
            release(lock_path)
            # Restore the unwrapped bus so a subsequent run on a
            # different profile gets its own tagging scope (the
            # next ``run_backup`` rewraps it).
            self._events = original_events

    def _best_effort_cleanup(self, ctx: PipelineContext) -> None:
        """Remove the partial backup that was just created, if possible.

        Called from the ``except`` block of ``run_backup`` so disk
        space is reclaimed immediately on the common case of a
        recoverable failure (verify mismatch, network blip, cancel)
        rather than waiting for the next run's orphan scan to do it.

        This is **best-effort**: any error here is swallowed — the
        original failure is what matters, and the persistent
        ``incomplete_backup_name`` flag plus the orphan scan are the
        safety nets that catch anything we cannot clean up here
        (disk unmounted, S3 Object Lock, etc.).

        Without a ``.wbcommit`` the backup is invisible to ``list_backups``
        either way, so leaving the bytes on disk is correctness-safe;
        the cleanup just frees space sooner.
        """
        # Without a backup_name there is nothing to delete.
        backup_name = getattr(ctx, "backup_name", "")
        if not backup_name:
            return

        # Primary destination
        if ctx.backend is not None:
            self._try_delete(ctx.backend, backup_name, "primary")
            self._try_delete(ctx.backend, f"{backup_name}.tar.wbenc", "primary")

        # Mirror destinations: each one might have its own backend.
        for i, config in enumerate(ctx.profile.mirror_destinations):
            try:
                backend = self._get_backend(config)
            except Exception:
                continue
            label = f"mirror {i + 1}"
            self._try_delete(backend, backup_name, label)
            self._try_delete(backend, f"{backup_name}.tar.wbenc", label)

    @staticmethod
    def _try_delete(backend, name: str, label: str) -> None:
        """Best-effort backend delete that swallows every error.

        Logs at INFO level on success so the cleanup is visible in
        the run log; logs at DEBUG on FileNotFoundError (the artefact
        was never created — common when the failure happened during
        write of the very first file). All other errors get a single
        WARNING line so they do not drown the original failure.
        """
        try:
            backend.delete_backup(name)
            logger.info("Best-effort cleanup: deleted %s on %s", name, label)
        except FileNotFoundError:
            logger.debug(
                "Best-effort cleanup: nothing to delete (%s on %s)",
                name,
                label,
            )
        except Exception as e:
            logger.warning(
                "Best-effort cleanup failed for %s on %s: %s "
                "(orphan scan will retry at next run)",
                name,
                label,
                e,
            )

    def _rollback_backup_type_on_failure(
        self,
        ctx: PipelineContext,
        original_type: BackupType,
    ) -> None:
        """Restore the profile's user-configured backup_type after a failed run.

        When the pipeline auto-promotes a DIFF to FULL (profile
        changed, cycle reached, etc.) and then crashes, the on-disk
        profile would otherwise carry the promoted ``FULL`` value
        permanently — the next ``_maybe_force_full`` call would exit
        early at line 910 (``if backup_type != DIFFERENTIAL``) and
        every subsequent run would keep producing FULL backups.

        If the in-place save fails (double-fault: disk full during the
        rollback itself), a sentinel file ``<config>/<profile>.rollback``
        is written so the next launch can complete the rollback.
        Without this sentinel, a one-in-a-million unlucky double-crash
        would permanently strand the profile in FULL mode.
        """
        if not getattr(ctx, "forced_full", False):
            return
        if ctx.profile.backup_type == original_type:
            return
        ctx.profile.backup_type = original_type
        try:
            ctx.config_manager.save_profile(ctx.profile)
            logger.info(
                "Rolled back backup_type to %s after pipeline failure",
                original_type.value,
            )
        except Exception as rollback_err:
            logger.error(
                "Failed to persist backup_type rollback — writing sentinel: %s",
                rollback_err,
            )
            try:
                sentinel = self._rollback_sentinel_path(ctx.profile.id)
                sentinel.parent.mkdir(parents=True, exist_ok=True)
                sentinel.write_text(original_type.value, encoding="utf-8")
            except Exception as sentinel_err:
                logger.warning(
                    "Sentinel write also failed — profile may stay in FULL "
                    "mode until manually corrected: %s",
                    sentinel_err,
                )

    def _rollback_sentinel_path(self, profile_id: str) -> Path:
        """Return the path to the rollback-sentinel file for a profile."""
        manifest_path = self._config.get_manifest_path(profile_id)
        return manifest_path.parent / f"{profile_id}.rollback"

    def _apply_pending_rollback(self, profile: BackupProfile) -> None:
        """If a rollback sentinel exists, apply it and delete the sentinel.

        Called at the start of ``run_backup`` so any rollback that
        failed to persist on the previous run completes before we
        evaluate ``_maybe_force_full``. A one-time best-effort:
        reading/writing errors are logged but do not block the run.
        """
        sentinel = self._rollback_sentinel_path(profile.id)
        if not sentinel.exists():
            return
        try:
            target_value = sentinel.read_text(encoding="utf-8").strip()
            target = BackupType(target_value)
            if profile.backup_type != target:
                profile.backup_type = target
                self._config.save_profile(profile)
                logger.info(
                    "Applied pending backup_type rollback: profile '%s' → %s",
                    profile.name,
                    target.value,
                )
            sentinel.unlink()
        except Exception as e:
            logger.warning("Could not apply rollback sentinel for %s: %s", profile.name, e)

    def _run_pipeline(self, ctx: PipelineContext) -> None:
        """Execute all pipeline phases sequentially."""
        # Auto-promote differential to full when the cycle threshold is reached
        self._maybe_force_full(ctx)

        # Generate backup name AFTER promotion so the tag is correct
        type_tag = "DIFF" if ctx.profile.backup_type == BackupType.DIFFERENTIAL else "FULL"
        ctx.backup_name = generate_backup_name(ctx.profile.name, type_tag)

        # Record actual type for email report (before restore to DIFFERENTIAL)
        ctx.result.actual_backup_type = ctx.profile.backup_type.value.upper()

        # Notify the UI of the effective backup_type NOW so the Run tab
        # header can switch from the configured "differential" to
        # "full (auto-promoted)" for the duration of this run.
        self._events.emit(
            BACKUP_TYPE_DETERMINED,
            backup_type=ctx.profile.backup_type.value,
            forced_full=bool(getattr(ctx, "forced_full", False)),
        )

        # Log backup type and reference for differential
        if ctx.profile.backup_type == BackupType.DIFFERENTIAL:
            manifest_path = ctx.config_manager.get_manifest_path(ctx.profile.id)
            manifest = load_manifest(manifest_path)
            meta = manifest.get("__metadata__", {})
            ref_name = meta.get("backup_name")
            if ref_name:
                self._log(f"Backup type: differential (reference: {ref_name})")
            else:
                self._log("Backup type: differential")
        else:
            if ctx.forced_full:
                self._log("Backup type: full (auto-promoted)")
            else:
                self._log("Backup type: full")

        # Mark backup as in-progress (persisted immediately so that
        # a crash or shutdown leaves the flag as False, enabling cleanup).
        ctx.profile.last_backup_completed = False
        ctx.profile.incomplete_backup_name = ctx.backup_name
        ctx.profile.incomplete_backup_was_full = ctx.profile.backup_type == BackupType.FULL
        ctx.config_manager.save_profile(ctx.profile)

        # Phase 1: Collect
        self._phase_collect(ctx)
        if not ctx.files:
            self._mark_completed(ctx)
            self._emit_status("success")
            return

        # Phase 2: Filter (differential only)
        self._phase_filter(ctx)
        if not ctx.files:
            self._mark_completed(ctx)
            self._emit_status("success")
            return

        ctx.result.files_processed = len(ctx.files)

        # Check disk space on all destinations before writing
        self._check_disk_space(ctx)

        # Tell the UI how many progress-emitting phases to expect
        self._emit_phase_count(ctx)

        ctx.backend = self._get_backend(ctx.profile.storage)
        ctx.backend.set_cancel_check(self._check_cancel)
        self._apply_bandwidth_throttle(ctx.backend, ctx.profile)
        self._apply_object_lock_retention(ctx)

        # Phase 0: Scan and remove orphans (anything without a valid
        # ``.wbcommit``) on every destination. This catches leftovers
        # from failed runs that the synchronous best-effort cleanup
        # could not delete (disk was unmounted, profile was edited,
        # different machine), as well as foreign artefacts on shared
        # destinations.
        self._phase_orphan_scan(ctx)

        # Phase 3 (integrity) ALWAYS runs before Phase 4 (write).
        #
        # ``_phase_integrity`` hashes every source file in parallel via
        # the ``ThreadPoolExecutor`` in ``manifest.py`` (introduced in
        # v3.3.13). The write phase then runs as a pure ``shutil.copy2``
        # loop with no hashing in the inner pass, so the kernel copy
        # primitive (``CopyFileExW`` on Windows) saturates the pipe.
        #
        # An earlier shape (v3.3.15–3.3.18) interleaved hash + copy
        # per file inside ``_phase_write``. That was visually opaque
        # ("Copying to Storage..." with no preceding manifest log) and
        # serialised the hash with the copy, so on a 30 k-small-file
        # workload the sequential per-file SHA-256 alone capped USB
        # throughput at ~8 MB/s. The hash-then-copy split below
        # restores the v3.3.14 model: parallel hash, then native copy.
        self._phase_integrity(ctx)
        self._phase_write(ctx)
        ctx.result.backup_path = str(ctx.backup_path or ctx.backup_remote_name)

        # Phase 5: Save integrity manifest
        self._phase_save_manifest(ctx)

        # Phase 6: Verify
        self._phase_verify(ctx)

        # Phase 6.5: Commit marker for the primary destination.
        # Writing the marker is the only way for ``list_backups`` and
        # restore to recognise this backup as complete; running it
        # AFTER verify guarantees the marker is never present on a
        # backup that has not been integrity-checked end-to-end.
        self._phase_commit_primary(ctx)

        # Phase 8: Update delta manifest
        self._phase_update_delta(ctx)

        # Phase 9: Mirror (per-destination write + verify + commit)
        self._phase_mirror(ctx)

        # Phase 10: Verify mirrors
        self._phase_verify_mirrors(ctx)

        # Phase 11: Rotate
        self._phase_rotate(ctx)

        # Phase 11: Cleanup temp artifacts
        self._phase_cleanup(ctx)

        self._mark_completed(ctx)

    def _mark_completed(self, ctx: PipelineContext) -> None:
        """Reset interrupt-recovery flags and persist to disk.

        Must be called on every successful exit path (including early
        returns when there are no files to process) so the next run
        does not mistake a completed backup for an interrupted one.
        """
        ctx.profile.last_backup_completed = True
        ctx.profile.incomplete_backup_name = ""
        ctx.profile.incomplete_backup_was_full = False
        # A successful run clears the crash-recovery circuit breaker.
        # Otherwise after e.g. 3 transient NAS outages the counter
        # would stay at 3 forever, permanently disabling auto-recovery.
        ctx.profile.crash_recovery_attempts = 0

        # Restore profile type if it was temporarily promoted to full
        if getattr(ctx, "forced_full", False):
            ctx.profile.backup_type = BackupType.DIFFERENTIAL

        ctx.config_manager.save_profile(ctx.profile)

    def _mark_cancelled(self, ctx: PipelineContext) -> None:
        """Reset interrupt-recovery flags after a clean user cancel.

        A user-initiated cancel is *not* a crash. The pre-v3.7.11
        behaviour left ``last_backup_completed=False`` and
        ``incomplete_backup_name`` set on the profile, which then made
        ``_check_startup_missed`` re-trigger the backup as
        crash-recovery the next time the app booted. The user reported
        the case on 17/05/2026: install of v3.7.10 cancelled an
        in-flight backup, and the next launch auto-fired it again.

        Side effects mirror ``_mark_completed`` because the persistent
        crash-recovery semantics is "no pending interrupted state",
        not "the backup succeeded" — the success/failure verdict lives
        in the ``ScheduleJournal``, which is updated independently.
        ``_best_effort_cleanup`` is called before this from the
        ``except CancelledError`` block, so the partial bytes are gone
        by the time the flags are cleared.
        """
        ctx.profile.last_backup_completed = True
        ctx.profile.incomplete_backup_name = ""
        ctx.profile.incomplete_backup_was_full = False
        ctx.profile.crash_recovery_attempts = 0

        if getattr(ctx, "forced_full", False):
            ctx.profile.backup_type = BackupType.DIFFERENTIAL

        try:
            ctx.config_manager.save_profile(ctx.profile)
        except OSError as exc:
            # Persisting the reset is best-effort: even if it fails the
            # in-memory flag flip is what matters this session, and
            # crash-recovery already has a circuit breaker (cf.
            # MAX_CRASH_RECOVERY_ATTEMPTS) on the next launch.
            logger.warning("Failed to persist mark_cancelled state: %s", exc)

    def _phase_cleanup(self, ctx: PipelineContext) -> None:
        """Phase 11: Remove temporary artifacts from backup directory."""
        if ctx.backup_path and ctx.backup_path.exists() and ctx.backup_path.is_dir():
            temp_dirs = list(ctx.backup_path.rglob(".tmp.drivedownload"))
            for temp_dir in temp_dirs:
                try:
                    import shutil

                    shutil.rmtree(temp_dir)
                    self._log(f"Cleaned up temp directory: {temp_dir.name}")
                except OSError as e:
                    self._log(f"Could not remove {temp_dir.name}: {e}")

    def _phase_collect(self, ctx: PipelineContext) -> None:
        """Phase 1: Collect source files."""
        self._phase("Collecting files...")
        self._check_cancel()
        ctx.files = collect_files(
            ctx.profile.source_paths,
            ctx.profile.exclude_patterns,
            self._events,
        )
        ctx.all_files = list(ctx.files)  # Preserve full list for manifest
        ctx.result.files_found = len(ctx.files)
        ctx.result.bytes_source = sum(f.size for f in ctx.files)
        if not ctx.files:
            self._log("No files to back up")

    def _phase_filter(self, ctx: PipelineContext) -> None:
        """Phase 2: Filter changed files for differential backup.

        Differential compares against the manifest written by the last
        full backup.  If no manifest exists, all files are included
        (equivalent to a full backup).
        """
        self._phase("Filtering changed files...")
        self._check_cancel()
        if ctx.profile.backup_type == BackupType.DIFFERENTIAL:
            manifest_path = ctx.config_manager.get_manifest_path(ctx.profile.id)
            ctx.files, ctx.filter_hashes = filter_changed_files(
                ctx.files, manifest_path, self._events, cancel_check=self._check_cancel
            )
            ctx.result.files_skipped = ctx.result.files_found - len(ctx.files)
            if not ctx.files:
                self._log("No changes detected — backup skipped")

    def _check_disk_space(self, ctx: PipelineContext) -> None:
        """Verify sufficient disk space on all destinations before writing.

        Checks local/network destinations, SFTP via get_free_space(),
        and the temp drive for S3 encrypted uploads.

        Raises:
            RuntimeError: If any destination has insufficient space.
        """
        import tempfile

        backup_size = sum(f.size for f in ctx.files)
        local_required = backup_size + 100 * 1024 * 1024  # backup + 100 MB margin
        s3_temp_required = backup_size + 2 * 1024 * 1024 * 1024  # backup + 2 GB margin
        errors = []

        is_encrypted = (
            ctx.profile.encrypt_primary
            and ctx.profile.encryption.enabled
            and ctx.profile.encryption.stored_password
        )

        # --- Primary destination ---
        primary = ctx.profile.storage
        if primary.storage_type in (StorageType.LOCAL, StorageType.NETWORK):
            self._check_path_space(
                primary.destination_path,
                local_required,
                "Storage",
                errors,
            )
        elif primary.storage_type == StorageType.SFTP:
            self._check_remote_space(
                primary,
                local_required,
                "Storage (SFTP)",
                errors,
            )
        elif primary.storage_type == StorageType.S3 and is_encrypted:
            temp_dir = tempfile.gettempdir()
            self._check_path_space(
                temp_dir,
                s3_temp_required,
                f"Temp drive ({temp_dir[:3]}) for encrypted S3 upload",
                errors,
            )

        # --- Mirror destinations ---
        encrypt_flags = [ctx.profile.encrypt_mirror1, ctx.profile.encrypt_mirror2]
        for i, config in enumerate(ctx.profile.mirror_destinations):
            mirror_name = f"Mirror {i + 1}"
            mirror_encrypted = (
                i < len(encrypt_flags)
                and encrypt_flags[i]
                and ctx.profile.encryption.enabled
                and ctx.profile.encryption.stored_password
            )

            if config.storage_type in (StorageType.LOCAL, StorageType.NETWORK):
                self._check_path_space(
                    config.destination_path,
                    local_required,
                    mirror_name,
                    errors,
                )
            elif config.storage_type == StorageType.SFTP:
                self._check_remote_space(
                    config,
                    local_required,
                    f"{mirror_name} (SFTP)",
                    errors,
                )
            elif config.storage_type == StorageType.S3 and mirror_encrypted:
                temp_dir = tempfile.gettempdir()
                self._check_path_space(
                    temp_dir,
                    s3_temp_required,
                    f"Temp drive ({temp_dir[:3]}) for {mirror_name} encrypted S3",
                    errors,
                )

        if errors:
            detail = "\n".join(f"  - {e}" for e in errors)
            raise RuntimeError(f"Insufficient disk space:\n{detail}")

    def _check_remote_space(
        self,
        config: object,
        required: int,
        label: str,
        errors: list[str],
    ) -> None:
        """Check free space on a remote SFTP destination.

        Uses the backend's get_free_space() method (SFTP statvfs).
        Silently skips if the check fails (connection issue, etc.).
        """
        try:
            backend = self._get_backend(config)
            free = backend.get_free_space()
            if free is not None and free < required:
                free_gb = free / (1024**3)
                needed_gb = required / (1024**3)
                errors.append(f"{label}: {free_gb:.1f} GB free, need {needed_gb:.1f} GB")
        except Exception:
            logger.debug("Remote space check skipped for %s", label, exc_info=True)

    @staticmethod
    def _check_path_space(
        path: str,
        required: int,
        label: str,
        errors: list[str],
    ) -> None:
        """Check free space at *path* and append to *errors* if insufficient."""
        import shutil

        try:
            free = shutil.disk_usage(path).free
            if free < required:
                free_gb = free / (1024**3)
                needed_gb = required / (1024**3)
                errors.append(f"{label}: {free_gb:.1f} GB free, need {needed_gb:.1f} GB")
        except OSError:
            pass  # Path not accessible yet (USB not plugged, etc.)

    def _phase_integrity(self, ctx: PipelineContext) -> None:
        """Phase 3: Build integrity manifest by hashing every source.

        Runs BEFORE the write phase (since v3.3.19) so that:

        1. The user sees an explicit "Building integrity manifest..."
           log line before the long "Copying to Storage..." pass —
           previously the integrity phase ran AFTER write and was
           invisible until the copy was done.
        2. The hashing is parallelised by ``manifest.py`` over the
           ``_HASH_WORKERS_MAX`` thread pool, so 30 k small files
           hash in O(N / workers) wallclock instead of O(N) — the
           reason the v3.3.18 sequential hash-then-copy collapsed
           to ~8 MB/s on a USB SSD.
        3. The write phase becomes a pure ``shutil.copy2`` loop with
           nothing in the inner pass except the kernel copy primitive,
           which is what makes USB throughput match v3.3.14.

        ``filter_hashes`` (populated during the differential filter
        when a file's mtime/size already matched the previous manifest)
        is still honoured as a cache so a clean differential run
        avoids re-hashing files that did not change.
        """
        self._phase("Building integrity manifest...")
        self._check_cancel()

        # Differential runs may already carry hashes for unchanged
        # files in ``ctx.filter_hashes``. Use them as a hint; the rest
        # is hashed fresh from the source.
        cached: dict[str, str] | None = getattr(ctx, "filter_hashes", None) or None

        ctx.integrity_manifest = build_integrity_manifest(
            ctx.files,
            self._events,
            cancel_check=self._check_cancel,
            cached_hashes=cached,
        )

        # Cache file hashes for reuse in Phase 8 (delta manifest).
        ctx.file_hashes = {
            rel_path: info["hash"]
            for rel_path, info in ctx.integrity_manifest.get("files", {}).items()
        }

    def _phase_write(self, ctx: PipelineContext) -> None:
        """Phase 4: Write backup to primary destination."""
        target = self._describe_target(ctx.profile.storage)
        if ctx.profile.storage.is_remote():
            self._phase(f"Uploading to Storage — {target}...")
        else:
            self._phase(f"Copying to Storage — {target}...")
        write_backup(ctx, cancel_check=self._check_cancel)

    def _phase_save_manifest(self, ctx: PipelineContext) -> None:
        """Phase 5: Save integrity manifest alongside backup.

        Skipped for encrypted backups — the manifest is embedded inside
        the .tar.wbenc archive to avoid leaking file metadata.

        Local unencrypted: writes .wbverify next to the backup directory.
        Remote unencrypted: uploads .wbverify to the remote backend.
        """
        is_encrypted = (
            ctx.profile.encrypt_primary
            and ctx.profile.encryption.enabled
            and ctx.profile.encryption.stored_password
        )
        if is_encrypted:
            self._log("Manifest embedded in encrypted archive")
            return

        self._phase("Saving manifest...")
        if ctx.backup_path and ctx.backup_path.exists():
            try:
                save_integrity_manifest(ctx.integrity_manifest, ctx.backup_path)
            except (OSError, PermissionError) as e:
                # Local manifest write failure (disk full, read-only,
                # permission denied) used to abort the whole pipeline
                # AFTER the backup bytes were already on disk — the
                # orphan scan would then delete the backup at the next
                # run because no ``.wbcommit`` had been written. Match
                # the remote-upload path's behaviour: record a warning,
                # let the run continue, surface the loss of verifiability
                # in the result.
                message = (
                    f"Integrity manifest could not be saved locally next to "
                    f"{ctx.backup_path} ({type(e).__name__}: {e}); "
                    f"post-restore verification for this backup will not be "
                    f"available."
                )
                self._log(f"Warning: {message}")
                logger.warning("Failed to save local manifest: %s", e)
                ctx.result.add_warning(
                    phase="manifest",
                    file_path=f"{ctx.backup_name}.wbverify",
                    message=message,
                    exception=e,
                )

        if ctx.backup_remote_name and ctx.backend is not None:
            try:
                upload_manifest_to_remote(ctx.integrity_manifest, ctx.backend, ctx.backup_name)
            except Exception as e:
                # Manifest upload failure means post-restore verification
                # is no longer possible for this backup: the user believes
                # they have an integrity guarantee they do not.  Record it
                # on the result so the report surfaces the warning rather
                # than silently dropping it into the log.
                message = (
                    f"Integrity manifest could not be uploaded to remote "
                    f"({type(e).__name__}: {e}); post-restore verification "
                    f"for this backup will not be available."
                )
                self._log(f"Warning: {message}")
                logger.warning("Failed to upload manifest to remote: %s", e)
                ctx.result.add_warning(
                    phase="manifest",
                    file_path=f"{ctx.backup_name}.wbverify",
                    message=message,
                    exception=e,
                )

    def _phase_orphan_scan(self, ctx: PipelineContext) -> None:
        """Phase 0: Delete any backup without a valid ``.wbcommit``.

        Called at the very start of every run, before write phases
        consume disk space. Iterates over the primary destination and
        every configured mirror, asking each backend for its orphan
        list (``list_orphan_backups`` — backends that don't implement
        it are skipped silently). Each orphan is delete-best-effort:
        a single failure does not abort the scan or the run.

        Skipped destinations:
            * Backends that do not expose ``list_orphan_backups``
              (legacy SFTP/S3 — they will be retrofitted).
            * S3 Object Lock destinations: the lock prevents deletion
              before retention expiry; the lifecycle rule on the
              bucket reclaims those objects when they expire.
        """
        from src.core.config import StorageType

        destinations: list[tuple[str, object, object]] = [
            ("Storage", ctx.profile.storage, ctx.backend),
        ]
        for i, mirror_config in enumerate(ctx.profile.mirror_destinations):
            try:
                backend = self._get_backend(mirror_config)
            except Exception as e:
                logger.warning(
                    "Orphan scan: cannot reach mirror %d backend (%s) " "— skipping",
                    i + 1,
                    e,
                )
                continue
            destinations.append((f"Mirror {i + 1}", mirror_config, backend))

        for label, config, backend in destinations:
            # Object Lock buckets refuse delete before retention expiry;
            # let the bucket lifecycle rule reclaim those orphans.
            if config.storage_type == StorageType.S3 and getattr(config, "s3_object_lock", False):
                continue

            list_orphans = getattr(backend, "list_orphan_backups", None)
            if list_orphans is None:
                # Legacy backend — phase B will retrofit. Skip silently
                # so the missing capability is not noisy at every run.
                continue

            try:
                orphans = list_orphans()
            except Exception as e:
                logger.warning(
                    "Orphan scan: list_orphan_backups failed on %s: %s",
                    label,
                    e,
                )
                continue

            for orphan in orphans:
                name = orphan["name"]
                try:
                    backend.delete_backup(name)
                    self._log(
                        f"Orphan removed on {label}: {name} " f"({orphan.get('size', 0):,} bytes)"
                    )
                except FileNotFoundError:
                    # Concurrent removal — fine.
                    pass
                except Exception as e:
                    logger.warning(
                        "Orphan scan: failed to delete %s on %s: %s",
                        name,
                        label,
                        e,
                    )

    def _phase_commit_primary(self, ctx: PipelineContext) -> None:
        """Phase 6.5: Write commit marker for the primary destination.

        The commit marker (``.wbcommit``) is the destination-side proof
        that a backup is complete and integrity-verified. It is the
        sole authority used by ``list_backups``, restore, and the
        orphan scan to decide whether a backup is "real" or just
        leftover bytes from a failed run.

        Must run only after ``_phase_verify`` has succeeded. A failed
        write/upload here is fatal: without a marker the backup is
        invisible to the rest of the system and will be cleaned up at
        the next run, which would silently lose the run that just
        completed.

        Raises:
            RuntimeError: If the integrity manifest is missing the
                ``total_checksum`` field (defensive — should never
                happen if ``_phase_integrity`` ran).
            OSError / Exception: If the marker write/upload fails;
                the run is aborted before the profile is marked
                completed.
        """
        from src.core.phases.commit_marker import (
            DESTINATION_STORAGE,
            build_commit_marker,
            serialise_commit_marker,
            write_commit_marker,
        )

        manifest_sha = ctx.integrity_manifest.get("total_checksum", "")
        if not manifest_sha:
            # Without the manifest checksum the marker would have
            # nothing to bind to → an attacker could later swap the
            # ``.wbverify`` and the marker would still validate.
            # Refuse outright rather than write a useless commit.
            raise RuntimeError(
                "Cannot write commit marker: integrity manifest has no total_checksum"
            )

        files_count = len(ctx.integrity_manifest.get("files", {}))

        # Local destination (plain directory or encrypted .tar.wbenc)
        if ctx.backup_path is not None and ctx.backup_path.exists():
            self._phase("Writing commit marker...")
            try:
                write_commit_marker(
                    backup_path=ctx.backup_path,
                    manifest_sha256=manifest_sha,
                    files_count=files_count,
                    destination_label=DESTINATION_STORAGE,
                )
            except OSError as e:
                self._log(
                    f"Commit marker write failed for primary destination: {e}. "
                    f"Backup will be treated as orphaned at the next run."
                )
                raise
            return

        # Remote destination
        if ctx.backup_remote_name and ctx.backend is not None:
            self._phase("Uploading commit marker...")
            from io import BytesIO

            payload = build_commit_marker(
                manifest_sha256=manifest_sha,
                files_count=files_count,
                destination_label=DESTINATION_STORAGE,
            )
            data = serialise_commit_marker(payload)
            commit_remote_name = f"{ctx.backup_remote_name}.wbcommit"
            try:
                ctx.backend.upload_file(BytesIO(data), commit_remote_name, size=len(data))
            except Exception as e:
                self._log(
                    f"Commit marker upload failed for primary destination: {e}. "
                    f"Backup will be treated as orphaned at the next run."
                )
                raise

    def _phase_verify(self, ctx: PipelineContext) -> None:
        """Phase 6: Post-backup verification.

        Local backups: re-hash files and compare to manifest.
        Remote backups: verify file count and sizes on the server.

        Since v3.7.0 the user can disable post-copy verification on
        a per-profile basis via the "Verify integrity after backup"
        toggle in the General tab. Two cases bypass the toggle and
        force verification on:
        1. Remote primary storage (SFTP / S3 / Network) — the time
           saved by skipping is negligible (under 30 s) and silent
           corruption is harder to detect on a remote backend.
        2. Object Lock (anti-ransomware) profiles — verification is
           part of the security contract.

        When the toggle is off and the backup is local plain or local
        encrypted, this phase returns silently. The UI dispatcher
        (run tab for manual runs, email notifier for scheduled runs)
        handles the user-facing "Verify now?" prompt or the adapted
        success email.

        Raises:
            RuntimeError: If any file fails integrity verification.
        """
        if not _effective_auto_verify(ctx.profile):
            return

        is_local_dir = (
            ctx.backup_path is not None and ctx.backup_path.exists() and ctx.backup_path.is_dir()
        )
        is_local_encrypted = (
            ctx.backup_path is not None
            and ctx.backup_path.exists()
            and ctx.backup_path.name.endswith(".tar.wbenc")
        )

        if is_local_dir:
            self._phase("Verifying backup (hash)...")
            self._check_cancel()
            manifest_file = ctx.backup_path.parent / f"{ctx.backup_path.name}.wbverify"
            ok, msg = verify_backup(
                ctx.backup_path, manifest_file, self._events, cancel_check=self._check_cancel
            )
            if not ok:
                raise RuntimeError(msg)

        elif is_local_encrypted:
            from src.core.hashing import compute_sha256

            self._phase("Verifying encrypted backup...")
            self._check_cancel()
            size = ctx.backup_path.stat().st_size
            if size == 0:
                raise RuntimeError(f"Encrypted archive is empty: {ctx.backup_path.name}")
            # Store SHA-256 hash of the archive for future periodic verification
            archive_hash = compute_sha256(ctx.backup_path)
            ctx.config_manager.save_verify_hash(ctx.backup_path.name, archive_hash, size)
            self._log(
                f"Verification OK: {ctx.backup_path.name} " f"({size:,} bytes, GCM-authenticated)"
            )

        elif ctx.backup_remote_name and ctx.backend is not None:
            self._phase("Verifying remote backup (file count + sizes)...")
            self._check_cancel()
            self._verify_remote(ctx)

    def _verify_remote(self, ctx: PipelineContext) -> None:
        """Verify a remote backup by checking files on the server.

        Verification levels (best available per backend):
        - SFTP: SHA-256 computed server-side via exec channel
        - S3: MD5 from ETag (simple uploads < 5GB)
        - Other: file count + sizes only

        Args:
            ctx: Pipeline context with backend and files.

        Raises:
            RuntimeError: If any file fails verification.
        """
        # Try hash-based verification first
        verified_files = ctx.backend.verify_backup_files(ctx.backup_remote_name)
        has_checksums = verified_files and any(checksum for _, _, checksum in verified_files)

        if has_checksums:
            self._verify_remote_checksums(ctx, verified_files)
        else:
            # Fall back to size-only verification
            remote_files = ctx.backend.list_backup_files(ctx.backup_remote_name)
            if not remote_files:
                self._log("Remote verification skipped: backend does not " "support file listing")
                return
            self._verify_remote_sizes(ctx, remote_files)

    def _verify_remote_checksums(
        self,
        ctx: PipelineContext,
        remote_files: list[tuple[str, int, str]],
    ) -> None:
        """Verify remote files using checksums (SHA-256 or MD5).

        Compares the remote checksum to the hash already captured in the
        integrity manifest. The manifest is the canonical reference of
        "what was backed up"; re-hashing ``f.source_path`` here would
        race with any writer that touches the live source between the
        manifest phase and this verify phase (a long-running mirror
        upload of a volatile file like ``.claude/settings.local.json``
        is enough to fail every backup).

        For files without remote checksums: fall back to size comparison.

        Args:
            ctx: Pipeline context with files and integrity_manifest.
            remote_files: List of (relative_path, size, checksum) tuples.

        Raises:
            RuntimeError: If any file fails verification.
        """
        manifest_files = ctx.integrity_manifest.get("files", {})
        remote_map = {path: (size, checksum) for path, size, checksum in remote_files}

        errors = []
        hash_verified = 0
        size_verified = 0
        total = len(ctx.files)
        # Throttle progress emissions to 10 Hz the same way every other
        # pipeline phase does. Previously this loop emitted one PROGRESS
        # event per file; on a 231 k-file backup the resulting flood of
        # Tk.after(0) callbacks dominated the verify phase wall time
        # (~22 ms per widget update × 231908 ≈ 87 min for a loop that
        # is otherwise just dict lookups). The throttler in PhaseLogger
        # collapses that to ~10 emits/s -- the verify phase reverts to
        # CPU-bound and finishes in well under a second.
        phase_log = PhaseLogger("verify_remote", self._events)

        for i, f in enumerate(ctx.files):
            self._check_cancel()

            if f.relative_path not in remote_map:
                errors.append(f"Missing on remote: {f.relative_path}")
                continue

            remote_size, remote_checksum = remote_map[f.relative_path]

            if remote_checksum:
                manifest_entry = manifest_files.get(f.relative_path)
                manifest_hash = manifest_entry.get("hash") if manifest_entry else None
                # Hash-based verification
                if len(remote_checksum) == 64:
                    # SHA-256 (SFTP) — compare against manifest hash
                    if manifest_hash and manifest_hash != remote_checksum:
                        errors.append(
                            f"Hash mismatch: {f.relative_path} "
                            f"(manifest={manifest_hash[:16]}... "
                            f"remote={remote_checksum[:16]}...)"
                        )
                        continue
                    if not manifest_hash:
                        # Manifest entry missing or hashless — fall back
                        # to size to avoid blindly accepting the file.
                        if remote_size != f.size:
                            errors.append(
                                f"Size mismatch: {f.relative_path} "
                                f"(expected {f.size}, got {remote_size})"
                            )
                            continue
                        size_verified += 1
                        continue
                    hash_verified += 1
                elif len(remote_checksum) == 32:
                    # MD5 (S3 ETag) — manifest only stores SHA-256, so
                    # we still need a one-shot MD5 of the source here.
                    # MD5-only mirrors are rare (S3 simple uploads < 5GB);
                    # the race-condition class of bug is much smaller.
                    local_md5 = self._compute_md5(f.source_path)
                    if local_md5 != remote_checksum:
                        errors.append(
                            f"MD5 mismatch: {f.relative_path} "
                            f"(local={local_md5[:16]}... "
                            f"remote={remote_checksum[:16]}...)"
                        )
                        continue
                    hash_verified += 1
                else:
                    # Unknown checksum format — fall back to size
                    if remote_size != f.size:
                        errors.append(
                            f"Size mismatch: {f.relative_path} "
                            f"(expected {f.size}, got {remote_size})"
                        )
                        continue
                    size_verified += 1
            else:
                # No checksum available — size only
                if remote_size != f.size:
                    errors.append(
                        f"Size mismatch: {f.relative_path} "
                        f"(expected {f.size}, got {remote_size})"
                    )
                    continue
                size_verified += 1

            phase_log.progress(
                current=i + 1,
                total=total,
                filename=f.relative_path,
                phase="verification",
            )

        if errors:
            self._raise_verify_error(errors, len(ctx.files))
        parts = []
        if hash_verified:
            parts.append(f"{hash_verified} by checksum")
        if size_verified:
            parts.append(f"{size_verified} by size")
        method = ", ".join(parts)
        self._log(f"Remote verification OK: {total}/{total} files verified " f"({method})")

    def _verify_remote_sizes(
        self,
        ctx: PipelineContext,
        remote_files: list[tuple[str, int]],
    ) -> None:
        """Verify remote files by count and size only.

        Args:
            ctx: Pipeline context with files.
            remote_files: List of (relative_path, size) tuples.

        Raises:
            RuntimeError: If any file fails verification.
        """
        remote_map = {path: size for path, size in remote_files}
        errors = []
        total = len(ctx.files)
        # Same throttle reason as ``_verify_remote_checksums`` above.
        phase_log = PhaseLogger("verify_remote", self._events)

        for i, f in enumerate(ctx.files):
            self._check_cancel()

            if f.relative_path not in remote_map:
                errors.append(f"Missing on remote: {f.relative_path}")
            elif remote_map[f.relative_path] != f.size:
                errors.append(
                    f"Size mismatch: {f.relative_path} "
                    f"(expected {f.size}, "
                    f"got {remote_map[f.relative_path]})"
                )

            phase_log.progress(
                current=i + 1,
                total=total,
                filename=f.relative_path,
                phase="verification",
            )

        if errors:
            self._raise_verify_error(errors, len(ctx.files))
        self._log(f"Remote verification OK: {total}/{total} files verified " f"(by size)")

    @staticmethod
    def _compute_md5(file_path: Path) -> str:
        """Compute MD5 hex digest of a local file.

        Args:
            file_path: Path to the file.

        Returns:
            MD5 hex digest string.
        """
        import hashlib

        md5 = hashlib.md5()  # nosec B303 — used for S3 ETag comparison
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                md5.update(chunk)
        return md5.hexdigest()

    @staticmethod
    def _raise_verify_error(errors: list[str], total: int) -> None:
        """Raise RuntimeError with formatted verification errors.

        Args:
            errors: List of error messages.
            total: Total number of files expected.
        """
        detail = "\n  - ".join(errors[:10])
        extra = ""
        if len(errors) > 10:
            extra = f"\n  ... and {len(errors) - 10} more"
        raise RuntimeError(
            f"Remote verification failed: {len(errors)}/{total} " f"errors\n  - {detail}{extra}"
        )

    def _maybe_force_full(self, ctx: PipelineContext) -> None:
        """Auto-promote differential to full when needed.

        A full backup is forced when:
        - No manifest exists (first run or manifest deleted).
        - The differential cycle threshold is reached.
        - The profile configuration has changed (any setting except email).
        - Any destination has no full backup.

        Sets ``ctx.forced_full`` to True when promotion happens.  The
        profile's ``backup_type`` is changed to FULL for this run only
        and restored after the pipeline completes.
        """
        ctx.forced_full = False
        if ctx.profile.backup_type != BackupType.DIFFERENTIAL:
            return

        # Previous backup was interrupted — clean up and decide
        if not ctx.profile.last_backup_completed:
            if ctx.profile.object_lock_enabled:
                self._log(
                    "Previous backup was incomplete — skipping cleanup "
                    "(Object Lock prevents deletion)"
                )
            else:
                self._cleanup_incomplete_backup(ctx)
            if ctx.profile.incomplete_backup_was_full:
                # Interrupted full → must redo a full backup
                ctx.forced_full = True
                ctx.profile.backup_type = BackupType.FULL
                self._log("Forcing full backup (previous full was interrupted)")
                return
            # Interrupted differential → clean up, then continue checking
            # other conditions below (config change, cycle, etc.)
            self._log("Previous differential was interrupted — cleaned up")

        manifest_path = ctx.config_manager.get_manifest_path(ctx.profile.id)
        no_manifest = not manifest_path.exists()
        schedule_due = _is_full_due_by_schedule(ctx.profile, datetime.now())

        current_hash = compute_profile_hash(ctx.profile)
        profile_changed = ctx.profile.profile_hash != current_hash

        dest_missing_full = self._any_destination_missing_full(ctx)

        if no_manifest or schedule_due or profile_changed or dest_missing_full:
            ctx.forced_full = True
            ctx.profile.backup_type = BackupType.FULL
            if profile_changed:
                reason = "profile configuration changed"
            elif no_manifest:
                reason = "no manifest"
            elif dest_missing_full:
                reason = f"no full backup on {dest_missing_full}"
            else:
                reason = f"calendar schedule due ({ctx.profile.full_schedule_mode})"
            self._log(f"Forcing full backup ({reason})")

    def _any_destination_missing_full(self, ctx: PipelineContext) -> str:
        """Check if any configured destination is missing a full backup.

        Checks all destinations (local, network, SFTP, S3).
        A full backup is required on every destination for differential
        backups to be restorable.  If any destination is empty or has
        no FULL backup, a full is forced on all destinations.

        Args:
            ctx: Current pipeline context.

        Returns:
            Name of the first destination without a full backup,
            or empty string if all destinations have at least one full.
        """
        destinations = [("Storage", ctx.profile.storage)]
        for i, mirror in enumerate(ctx.profile.mirror_destinations):
            destinations.append((f"Mirror {i + 1}", mirror))

        # Exclude the incomplete-backup name from the has_full check.
        # On storage backends that cannot delete the partial (S3 Object
        # Lock mirror in particular), _cleanup_incomplete_backup leaves
        # the partial FULL in place. Counting it as "has_full" would
        # make every subsequent DIFF reference a broken FULL and corrupt
        # the restore chain silently.
        incomplete = ctx.profile.incomplete_backup_name
        # Match both the plain directory name and its .tar.wbenc file.
        excluded = {incomplete, f"{incomplete}.tar.wbenc"} if incomplete else set()

        for name, config in destinations:
            try:
                backend = self._get_backend(config)
                backups = backend.list_backups()
                has_full = any("_FULL_" in b["name"] and b["name"] not in excluded for b in backups)
                if not has_full:
                    logger.info(
                        "Destination %s has no usable full backup " "(excluded incomplete: %s)",
                        name,
                        excluded or "none",
                    )
                    return name
            except Exception as e:
                logger.warning("Could not check %s: %s", name, e)
        return ""

    def _phase_update_delta(self, ctx: PipelineContext) -> None:
        """Phase 8: Update manifest for differential tracking.

        After a full backup: writes the manifest and resets the
        differential counter.  After a differential backup:
        increments the counter.  The manifest is never overwritten
        by a differential, so it always reflects the last full.

        Relies on ``ctx.forced_full`` as a fallback source of truth
        because ``ctx.profile.backup_type`` can be overwritten mid-pipeline
        when the UI saves the profile while a backup is in progress (the
        engine and UI share the same ``BackupProfile`` instance). The flag
        is set once in ``_maybe_force_full`` and never mutated by the UI.
        """
        manifest_path = ctx.config_manager.get_manifest_path(ctx.profile.id)

        is_full = ctx.profile.backup_type == BackupType.FULL or getattr(ctx, "forced_full", False)
        if is_full:
            self._phase("Updating manifest...")
            full_manifest = build_updated_manifest(
                ctx.all_files, ctx.file_hashes, cancel_check=self._check_cancel
            )
            full_manifest["__metadata__"] = {
                "backup_name": ctx.backup_name,
                "created_at": datetime.now().isoformat(),
            }
            save_manifest(full_manifest, manifest_path)
            ctx.profile.profile_hash = compute_profile_hash(ctx.profile)
            ctx.profile.last_full_backup = datetime.now().isoformat()
            ctx.profile.last_full_files_count = ctx.result.files_processed

    def _phase_mirror(self, ctx: PipelineContext) -> None:
        """Phase 9: Mirror upload to secondary destinations."""
        if ctx.profile.mirror_destinations:
            self._phase("Uploading to mirrors...")
            self._check_cancel()
            mirror_path = ctx.backup_path if ctx.backup_path else Path(".")

            # Per-mirror encryption flags
            encrypt_flags = [
                ctx.profile.encrypt_mirror1,
                ctx.profile.encrypt_mirror2,
            ]
            secure_pw = None
            if ctx.profile.encryption.enabled and ctx.profile.encryption.stored_password:
                secure_pw = SecurePassword(ctx.profile.encryption.stored_password)
            try:
                encrypt_pw = secure_pw.get() if secure_pw else ""
                logger.info(
                    "Mirror phase: encrypt_flags=%s, encryption_enabled=%s, "
                    "has_stored_password=%s, encrypt_pw_set=%s",
                    encrypt_flags,
                    ctx.profile.encryption.enabled,
                    bool(ctx.profile.encryption.stored_password),
                    bool(encrypt_pw),
                )

                ctx.result.mirror_results = mirror_backup(
                    mirror_path,
                    ctx.files,
                    ctx.profile.mirror_destinations,
                    ctx.backup_name,
                    self._get_backend,
                    self._events,
                    encrypt_password=encrypt_pw,
                    encrypt_flags=encrypt_flags,
                    cancel_check=self._check_cancel,
                    integrity_manifest=ctx.integrity_manifest,
                    apply_throttle=lambda backend, label: (
                        self._apply_bandwidth_throttle(backend, ctx.profile, label)
                    ),
                    allow_partial=ctx.profile.object_lock_enabled,
                )
            finally:
                if secure_pw:
                    secure_pw.clear()

    def _phase_verify_mirrors(self, ctx: PipelineContext) -> None:
        """Phase 10: Verify mirror uploads.

        Runs the same verification as _verify_remote for each
        mirror destination that was successfully uploaded.

        Raises:
            RuntimeError: If any mirror file fails verification.
        """
        if not ctx.profile.mirror_destinations:
            return
        if not ctx.profile.verification.auto_verify:
            return

        encrypt_flags = [
            ctx.profile.encrypt_mirror1,
            ctx.profile.encrypt_mirror2,
        ]
        for i, config in enumerate(ctx.profile.mirror_destinations):
            mirror_name = f"Mirror {i + 1}"
            self._check_cancel()
            mirror_encrypted = (
                ctx.profile.encryption.enabled and i < len(encrypt_flags) and encrypt_flags[i]
            )
            logger.info(
                "Verify %s: encrypted=%s (enc_enabled=%s, flag=%s)",
                mirror_name,
                mirror_encrypted,
                ctx.profile.encryption.enabled,
                encrypt_flags[i] if i < len(encrypt_flags) else "N/A",
            )

            try:
                backend = self._get_backend(config)

                if mirror_encrypted:
                    # Encrypted mirrors produce a single .tar.wbenc file.
                    # Verify it exists with plausible size. GCM tags
                    # guarantee integrity at decryption time.
                    self._phase(f"Verifying {mirror_name} (encrypted)...")
                    self._verify_encrypted_archive(
                        backend,
                        config,
                        ctx.backup_name,
                        mirror_name,
                    )
                elif config.is_remote():
                    self._phase(f"Verifying {mirror_name}...")
                    verified = backend.verify_backup_files(ctx.backup_name)
                    has_checksums = verified and any(c for _, _, c in verified)

                    if has_checksums:
                        self._verify_mirror_checksums(
                            ctx,
                            verified,
                            mirror_name,
                        )
                    else:
                        remote_files = backend.list_backup_files(ctx.backup_name)
                        if remote_files:
                            self._verify_mirror_sizes(
                                ctx,
                                remote_files,
                                mirror_name,
                            )
                        else:
                            self._log(
                                f"{mirror_name}: verification skipped "
                                f"(file listing not supported)"
                            )
                else:
                    # Local unencrypted mirror — hash verification.
                    mirror_path = Path(config.destination_path) / ctx.backup_name
                    if mirror_path.exists() and mirror_path.is_dir():
                        self._phase(f"Verifying {mirror_name} (hash)...")
                        manifest_file = mirror_path.parent / f"{mirror_path.name}.wbverify"
                        if manifest_file.exists():
                            ok, msg = verify_backup(mirror_path, manifest_file, self._events)
                            if not ok:
                                raise RuntimeError(f"{mirror_name}: {msg}")

                # Mirror passed verification — write its commit marker
                # so list_backups / restore on this mirror's destination
                # recognise the artefact as committed. Each mirror has
                # its own marker so a failure on mirror 2 does not
                # invalidate mirror 1.
                self._commit_mirror(ctx, config, i, mirror_name, mirror_encrypted)

            except RuntimeError:
                raise
            except Exception as e:
                raise RuntimeError(f"{mirror_name} verification failed: {e}") from e

    def _commit_mirror(
        self,
        ctx: PipelineContext,
        config,
        mirror_idx: int,
        mirror_name: str,
        is_encrypted: bool,
    ) -> None:
        """Write a commit marker on a mirror destination after its verify.

        Each mirror gets its own ``.wbcommit`` so per-destination state
        is honest: a mirror that did not pass verification has no
        marker and is invisible to the orphan scan / restore.

        Args:
            ctx: Pipeline context (already-built integrity manifest).
            config: This mirror's storage configuration.
            mirror_idx: Zero-based mirror index for the destination
                label (``"mirror_1"`` for index 0, etc).
            mirror_name: Human-readable label for log messages.
            is_encrypted: Whether the mirror artefact is a
                ``.tar.wbenc`` archive (vs a plain directory).
        """
        from src.core.phases.commit_marker import (
            DESTINATION_MIRROR_PREFIX,
            build_commit_marker,
            serialise_commit_marker,
            write_commit_marker,
        )

        manifest_sha = ctx.integrity_manifest.get("total_checksum", "")
        if not manifest_sha:
            raise RuntimeError(
                f"Cannot commit {mirror_name}: integrity manifest has no total_checksum"
            )
        files_count = len(ctx.integrity_manifest.get("files", {}))
        label = f"{DESTINATION_MIRROR_PREFIX}{mirror_idx + 1}"

        artefact_relname = f"{ctx.backup_name}.tar.wbenc" if is_encrypted else ctx.backup_name

        if config.is_remote():
            from io import BytesIO

            self._phase(f"Uploading commit marker for {mirror_name}...")
            payload = build_commit_marker(
                manifest_sha256=manifest_sha,
                files_count=files_count,
                destination_label=label,
            )
            data = serialise_commit_marker(payload)
            backend = self._get_backend(config)
            commit_remote_name = f"{artefact_relname}.wbcommit"
            try:
                backend.upload_file(BytesIO(data), commit_remote_name, size=len(data))
            except Exception as e:
                self._log(
                    f"Commit marker upload failed for {mirror_name}: {e}. "
                    f"This mirror will be treated as orphaned at the next run."
                )
                raise
            return

        # Local mirror
        artefact_path = Path(config.destination_path) / artefact_relname
        if not artefact_path.exists():
            # Should not happen — verify just succeeded — but refuse
            # to write a marker pointing at a non-existent artefact.
            raise RuntimeError(f"{mirror_name}: artefact missing after verify: {artefact_path}")
        self._phase(f"Writing commit marker for {mirror_name}...")
        try:
            write_commit_marker(
                backup_path=artefact_path,
                manifest_sha256=manifest_sha,
                files_count=files_count,
                destination_label=label,
            )
        except OSError as e:
            self._log(
                f"Commit marker write failed for {mirror_name}: {e}. "
                f"This mirror will be treated as orphaned at the next run."
            )
            raise

    def _verify_mirror_checksums(
        self,
        ctx: PipelineContext,
        remote_files: list[tuple[str, int, str]],
        mirror_name: str,
    ) -> None:
        """Verify unencrypted mirror files using checksums.

        Compares the remote checksum to the hash already captured in the
        integrity manifest, NOT a fresh re-hash of ``f.source_path``.
        See ``_verify_remote_checksums`` for the full rationale: live
        source files (editor autosaves, ``.claude/settings.local.json``)
        would otherwise race with the verify phase and produce
        guaranteed false positives.
        """
        manifest_files = ctx.integrity_manifest.get("files", {})
        remote_map = {path: (size, checksum) for path, size, checksum in remote_files}
        errors = []
        hash_verified = 0
        size_verified = 0

        for f in ctx.files:
            self._check_cancel()
            expected_path = f.relative_path
            if expected_path not in remote_map:
                errors.append(f"Missing on {mirror_name}: {expected_path}")
                continue

            remote_size, remote_checksum = remote_map[expected_path]

            if remote_checksum and len(remote_checksum) == 64:
                manifest_entry = manifest_files.get(expected_path)
                manifest_hash = manifest_entry.get("hash") if manifest_entry else None
                if manifest_hash and manifest_hash != remote_checksum:
                    errors.append(f"Hash mismatch on {mirror_name}: " f"{expected_path}")
                    continue
                if not manifest_hash:
                    if remote_size != f.size:
                        errors.append(f"Size mismatch on {mirror_name}: " f"{f.relative_path}")
                        continue
                    size_verified += 1
                    continue
                hash_verified += 1
            elif remote_checksum and len(remote_checksum) == 32:
                local_md5 = self._compute_md5(f.source_path)
                if local_md5 != remote_checksum:
                    errors.append(f"MD5 mismatch on {mirror_name}: " f"{expected_path}")
                    continue
                hash_verified += 1
            else:
                if remote_size != f.size:
                    errors.append(f"Size mismatch on {mirror_name}: " f"{f.relative_path}")
                    continue
                size_verified += 1

        if errors:
            self._raise_verify_error(errors, len(ctx.files))

        total = len(ctx.files)
        parts = []
        if hash_verified:
            parts.append(f"{hash_verified} by checksum")
        if size_verified:
            parts.append(f"{size_verified} by size")
        method = ", ".join(parts)
        self._log(f"{mirror_name} verification OK: {total}/{total} files " f"({method})")

    def _verify_mirror_sizes(
        self,
        ctx: PipelineContext,
        remote_files: list[tuple[str, int]],
        mirror_name: str,
    ) -> None:
        """Verify unencrypted mirror files by size."""
        remote_map = {path: size for path, size in remote_files}
        errors = []

        for f in ctx.files:
            self._check_cancel()
            expected_path = f.relative_path
            if expected_path not in remote_map:
                errors.append(f"Missing on {mirror_name}: {expected_path}")
            elif remote_map[expected_path] != f.size:
                errors.append(f"Size mismatch on {mirror_name}: {expected_path}")

        if errors:
            self._raise_verify_error(errors, len(ctx.files))

        total = len(ctx.files)
        self._log(f"{mirror_name} verification OK: {total}/{total} files " f"(by size)")

    def _verify_encrypted_archive(
        self,
        backend: object,
        config: object,
        backup_name: str,
        mirror_name: str,
    ) -> None:
        """Verify that a .tar.wbenc archive exists on the destination.

        For encrypted backups, individual file verification is impossible
        without decryption.  GCM authentication tags guarantee integrity
        at restore time, so we only check that the archive exists and
        has a plausible size (> header size).

        Args:
            backend: Storage backend instance.
            config: Storage configuration.
            backup_name: Backup name (without extension).
            mirror_name: Human-readable mirror label for logging.

        Raises:
            RuntimeError: If the archive is missing or empty.
        """
        archive_name = f"{backup_name}.tar.wbenc"

        if config.is_remote():
            # Use get_file_size for a direct check (works for both SFTP and S3)
            size = None
            if hasattr(backend, "get_file_size"):
                size = backend.get_file_size(archive_name)
            if size is None or size == 0:
                raise RuntimeError(
                    f"{mirror_name}: encrypted archive {archive_name} " f"not found on remote"
                )
        else:
            local_path = Path(config.destination_path) / archive_name
            if not local_path.exists() or local_path.stat().st_size == 0:
                raise RuntimeError(
                    f"{mirror_name}: encrypted archive {archive_name} "
                    f"not found at {config.destination_path}"
                )

        self._log(f"{mirror_name} verification OK: {archive_name} present")

    def _phase_rotate(self, ctx: PipelineContext) -> None:
        """Phase 11: Rotation — delete old backups."""
        self._phase("Rotating old backups...")
        self._check_cancel()
        ctx.result.rotated_count = rotate_backups(
            ctx.backend,
            ctx.profile.retention,
            self._events,
            current_backup_name=ctx.backup_name,
            profile_name=ctx.profile.name,
        )

        # Count remaining backups on primary after rotation
        with contextlib.suppress(Exception):
            ctx.result.backups_available = len(ctx.backend.list_backups())

        # Rotate mirrors with the same retention policy. Each mirror
        # backend needs its own cancel-check wiring so a user Cancel
        # aborts the rotation loop promptly instead of waiting for
        # every mirror's list+delete cycle to finish.
        for i, config in enumerate(ctx.profile.mirror_destinations):
            mirror_name = f"Mirror {i + 1}"
            try:
                backend = self._get_backend(config)
                backend.set_cancel_check(self._check_cancel)
                deleted = rotate_backups(
                    backend,
                    ctx.profile.retention,
                    self._events,
                    current_backup_name=ctx.backup_name,
                    profile_name=ctx.profile.name,
                )
                if deleted:
                    self._log(f"{mirror_name}: rotated {deleted} old backup(s)")
            except Exception as e:
                self._log(f"{mirror_name}: rotation failed — {e}")

    def _profile_lock_path(self, profile_id: str) -> Path:
        """Return the filesystem path for a profile's per-run lock.

        Stored next to the profile's manifest so it follows the config
        directory on disk and is visible to any other BackupEngine
        instance working on the same profile.
        """
        manifest_path = self._config.get_manifest_path(profile_id)
        return manifest_path.parent / f"{profile_id}.lock"

    def _cleanup_incomplete_backup(self, ctx: PipelineContext) -> None:
        """Delete the incomplete full backup from all destinations.

        Only deletes the exact backup name recorded when the interrupted
        full started.  Skips silently if the backup does not exist on
        a destination (it may not have been written there yet).

        Args:
            ctx: Pipeline context with profile containing the
                 incomplete_backup_name field.
        """
        name = ctx.profile.incomplete_backup_name
        if not name:
            return

        self._log(f"Cleaning up incomplete backup: {name}")

        # Build list of all destinations: primary + mirrors
        destinations: list[tuple[str, StorageConfig]] = [
            ("Storage", ctx.profile.storage),
        ]
        for i, mirror in enumerate(ctx.profile.mirror_destinations):
            destinations.append((f"Mirror {i + 1}", mirror))

        for label, config in destinations:
            # A destination under S3 Object Lock COMPLIANCE cannot
            # have its objects deleted until retention expires, so
            # calling delete_backup would only surface a confusing
            # error. Skip explicitly and let the provider lifecycle
            # rule (AbortIncompleteMultipartUpload + Expiration after
            # lock) reclaim the space at the correct time.
            if config.storage_type == StorageType.S3 and getattr(config, "s3_object_lock", False):
                self._log(f"{label}: skipping cleanup (Object Lock prevents deletion)")
                continue
            try:
                backend = create_backend(config)
                # Try both plain directory and encrypted archive names
                deleted = False
                for suffix in ("", ".tar.wbenc"):
                    target = f"{name}{suffix}"
                    try:
                        backend.delete_backup(target)
                        self._log(f"{label}: deleted incomplete {target}")
                        deleted = True
                    except FileNotFoundError:
                        pass
                if not deleted:
                    self._log(f"{label}: nothing to clean up")
            except Exception as exc:
                self._log(f"{label}: cleanup failed — {exc}")

        # Persist the cleared name immediately. If the pipeline crashes
        # between this point and _mark_completed, a second interruption
        # could otherwise overwrite ``incomplete_backup_name`` in memory
        # with the NEW partial's name, leaving the OLD (already-deleted)
        # name lost from disk — and more importantly leaving the new
        # partial untracked, so the next run would believe there is
        # nothing to clean up.
        ctx.profile.incomplete_backup_name = ""
        try:
            self._config.save_profile(ctx.profile)
        except Exception as exc:
            # save_profile failure here is recoverable: the in-memory
            # state is consistent, and _mark_completed (or the next
            # _cleanup call) will try again. Just log it.
            logger.warning("Failed to persist cleared incomplete_backup_name: %s", exc)

    def _apply_bandwidth_throttle(
        self,
        backend: StorageBackend,
        profile: BackupProfile,
        label: str = "Storage",
    ) -> None:
        """Measure bandwidth and apply throttle to a backend.

        Skips measurement for LOCAL destinations (always 100%).
        Skips when the user has selected 100%.
        For S3 Object Lock backends with a speedtest bucket configured,
        measures on the speedtest bucket to avoid locked test files.

        Args:
            backend: Storage backend to throttle.
            profile: Backup profile with bandwidth_percent setting.
            label: Human-readable destination name for logging.
        """
        from src.storage.local import LocalStorage
        from src.storage.s3 import S3Storage

        if isinstance(backend, LocalStorage):
            self._log(f"{label}: local destination — bandwidth unlimited")
            return

        percent = profile.bandwidth_percent
        if percent >= 100:
            self._log(f"{label}: bandwidth usage set to 100% — no throttle")
            return

        self._phase(f"Measuring bandwidth ({label})...")
        self._check_cancel()

        # Use speedtest bucket for S3 Object Lock to avoid locked test files
        test_backend = backend
        if isinstance(backend, S3Storage) and profile.storage.s3_object_lock:
            if not profile.storage.s3_speedtest_bucket:
                raise ValueError("Object Lock profile is missing s3_speedtest_bucket")
            test_backend = S3Storage(
                bucket=profile.storage.s3_speedtest_bucket,
                prefix="",
                region=profile.storage.s3_region,
                access_key=profile.storage.s3_access_key,
                secret_key=profile.storage.s3_secret_key,
                endpoint_url=profile.storage.s3_endpoint_url,
                provider=profile.storage.s3_provider,
            )
            self._log(f"{label}: using speedtest bucket for bandwidth measurement")

        measured_bps = measure_bandwidth(test_backend)
        if measured_bps <= 0:
            self._log(f"{label}: bandwidth test failed — no throttle applied")
            return

        throttle_kbps = compute_throttle_kbps(measured_bps, percent)
        backend.set_bandwidth_limit(throttle_kbps)

        measured_mbps = measured_bps / (1024 * 1024)
        throttle_mbps = (throttle_kbps * 1024) / (1024 * 1024)
        self._log(
            f"{label}: {measured_mbps:.1f} MB/s measured → "
            f"throttle {percent}% = {throttle_mbps:.1f} MB/s"
        )

    def _apply_object_lock_retention(self, ctx: PipelineContext) -> None:
        """Set per-object Object Lock retention on the backend.

        Full backups are locked for retention + full_extra_days to ensure
        they outlive all differential backups that reference them.
        Differential backups are locked for the base retention period.

        Does nothing if the profile does not have Object Lock enabled.

        Args:
            ctx: Current pipeline context.
        """
        if not ctx.profile.object_lock_enabled:
            return

        from datetime import timedelta

        storage = ctx.profile.storage
        lock_days = storage.s3_object_lock_days

        is_full = ctx.profile.backup_type == BackupType.FULL or getattr(ctx, "forced_full", False)
        if is_full:
            lock_days += storage.s3_object_lock_full_extra_days

        retain_until = datetime.now(UTC) + timedelta(days=lock_days)

        if hasattr(ctx.backend, "set_retain_until"):
            ctx.backend.set_retain_until(retain_until)
            tag = "full" if is_full else "differential"
            self._log(
                f"Object Lock: {tag} backup locked for {lock_days} days "
                f"(until {retain_until.strftime('%Y-%m-%d')})"
            )

    def _get_backend(self, storage: StorageConfig) -> StorageBackend:
        """Create a storage backend from config.

        Args:
            storage: Storage configuration.

        Returns:
            Configured StorageBackend instance.

        Raises:
            ValueError: If the storage type is unknown.
        """
        return create_backend(storage)

    def precheck_targets(self, profile: BackupProfile) -> list[tuple[str, str, bool, str]]:
        """Test connectivity of all configured destinations before backup.

        Tests the primary storage and all mirror destinations.

        Args:
            profile: Backup profile with storage and mirror configs.

        Returns:
            List of (role, action, success, detail) for each target.
            role: "Storage", "Mirror 1", "Mirror 2"
            action: Human-readable action, e.g. "Connect USB drive D:\\Backups"
            success: True if reachable, False otherwise.
            detail: Message from test_connection() or error string.
        """
        results = []
        targets = [("Storage", profile.storage)]
        for i, mirror in enumerate(profile.mirror_destinations):
            targets.append((f"Mirror {i + 1}", mirror))

        for role, config in targets:
            action = self._describe_target(config)
            try:
                backend = self._get_backend(config)
                ok, msg = backend.test_connection()
                results.append((role, action, ok, msg))
            except Exception as e:
                results.append((role, action, False, str(e)))

        return results

    @staticmethod
    def _describe_target(config: StorageConfig) -> str:
        """Build a human-readable action for a storage target.

        Args:
            config: Storage configuration.

        Returns:
            Action string like "Connect USB drive D:\\Backups".
        """
        st = config.storage_type
        if st == StorageType.LOCAL:
            return f"Connect USB drive {config.destination_path}"
        if st == StorageType.NETWORK:
            return f"Connect network share {config.destination_path}"
        if st == StorageType.SFTP:
            return (
                f"Start SSH server " f"{config.sftp_username}@{config.sftp_host}:{config.sftp_port}"
            )
        if st == StorageType.S3:
            return (
                f"Check S3 bucket {config.s3_bucket} " f"({config.s3_provider} {config.s3_region})"
            )
        return f"Check {st.value} destination"

    def _check_cancel(self) -> None:
        """Check if cancellation was requested."""
        if self._cancelled:
            raise CancelledError("Backup cancelled")

    def _phase(self, message: str) -> None:
        """Announce a new pipeline phase."""
        self._log(message)
        self._events.emit(PHASE_CHANGED, phase=message)

    def _log(self, message: str) -> None:
        logger.info(message)
        self._events.emit(LOG, message=message, level="info")

    def _capture_log(self, message: str, **_kwargs) -> None:
        """Capture all LOG events (engine + phases) into BackupResult."""
        if self._current_result is not None:
            self._current_result.log_lines.append(message)

    def _emit_status(self, state: str) -> None:
        self._events.emit(STATUS, state=state)

    def _emit_phase_count(self, ctx: PipelineContext) -> None:
        """Calculate and emit phase weights for progress bar.

        Weights reflect relative duration of each phase:
        - hashing: 1 (local disk read, fast)
        - backup/upload: 2 (local) or 5 (remote network)
        - verification: 1 (local hash or remote size check)
        - upload (mirror): 5 (network upload)
        - encryption: 1 (CPU-bound, fast)
        - rotation: 1 (delete old backups, can be slow on remote)
        """
        is_remote = ctx.profile.storage.is_remote()

        weights = {
            "hashing": 1,
            "verification": 1,
            "rotation": 1,
        }

        if is_remote:
            weights["upload"] = 5  # remote_writer phase name
        else:
            weights["backup"] = 2  # local_writer phase name

        if ctx.profile.mirror_destinations:
            weights["mirror_upload"] = 5

        if ctx.profile.encrypt_primary and ctx.profile.encryption.enabled:
            weights["encryption"] = 1

        self._events.emit(PHASE_COUNT, weights=weights)


# Re-export for backward compatibility
__all__ = ["BackupEngine", "BackupResult", "CancelledError"]
