"""In-app backup scheduler with journal and auto-start.

Runs a daemon thread that checks every 30s if a backup is due.
Detects system sleep/hibernation and triggers missed backups.
"""

import json
import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from src.core.config import BackupProfile, ScheduleFrequency

logger = logging.getLogger(__name__)

CHECK_INTERVAL = 30  # seconds
MAX_JOURNAL_ENTRIES = 500
# Stop auto-triggering "crash recovery" after this many consecutive
# failures. Beyond that the user must explicitly run the profile to
# acknowledge the problem (NAS offline, credentials expired, etc.).
MAX_CRASH_RECOVERY_ATTEMPTS = 3


@dataclass
class ScheduleLogEntry:
    timestamp: str = ""
    profile_id: str = ""
    profile_name: str = ""
    trigger: str = "in_app"  # in_app, missed_recovery
    status: str = "started"  # started, success, failed, skipped
    detail: str = ""
    files_count: int = 0
    duration_seconds: float = 0.0
    # Total size of all source files that were backed up, in bytes.
    # Surfaces in the Run-tab "Last backup" card so the user can
    # eyeball the workload without opening the success email. 0 means
    # "unknown" (older entries from before this field existed, or a
    # failed run that never reached the collect phase).
    bytes_source: int = 0


class ScheduleJournal:
    """Persistent schedule execution log.

    Thread-safe: all read/write operations are protected by an
    internal lock to prevent race conditions when the scheduler
    daemon thread and the main thread access journal data
    concurrently.
    """

    def __init__(self, config_dir: Path):
        self._path = config_dir / "schedule_journal.json"
        self._entries: list[dict] = []
        self._lock = threading.Lock()
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                self._entries = json.loads(self._path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                self._entries = []

    def _save(self) -> None:
        # Trim to max entries
        if len(self._entries) > MAX_JOURNAL_ENTRIES:
            self._entries = self._entries[-MAX_JOURNAL_ENTRIES:]
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(self._entries, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def add(self, entry: ScheduleLogEntry) -> None:
        """Add a new journal entry (thread-safe).

        Args:
            entry: Schedule log entry to append.
        """
        with self._lock:
            if not entry.timestamp:
                entry.timestamp = datetime.now().isoformat()
            self._entries.append(asdict(entry))
            self._save()

    def update_last(self, profile_id: str = "", **kwargs) -> None:
        """Update the most recent journal entry for a profile (thread-safe).

        Targets the most recent entry whose ``profile_id`` matches —
        NOT the global last entry. Concurrent runs on *different*
        profiles interleave their ``add()`` / ``update_last()`` calls,
        so the global last entry may belong to another profile. Updating
        it blindly wrote one profile's ``success`` (and its file counts)
        onto another profile's row and left the real run stuck on
        ``started`` — the 30/05/2026 "crypter shows Failed but the
        backup actually succeeded" bug.

        Args:
            profile_id: Profile whose latest entry to update. Empty
                (legacy callers without a profile context) falls back to
                the global last entry.
            **kwargs: Fields to set on the matched entry.
        """
        with self._lock:
            if not self._entries:
                return
            if profile_id:
                for entry in reversed(self._entries):
                    if entry.get("profile_id") == profile_id:
                        entry.update(kwargs)
                        self._save()
                        return
                return
            self._entries[-1].update(kwargs)
            self._save()

    def get_entries(self, limit: int = 50, profile_id: str = "") -> list[dict]:
        """Retrieve journal entries (thread-safe).

        Args:
            limit: Maximum number of entries to return.
            profile_id: Filter to a specific profile (empty = all).

        Returns:
            List of entry dicts, most recent last.
        """
        with self._lock:
            entries = self._entries
            if profile_id:
                entries = [e for e in entries if e.get("profile_id") == profile_id]
            return entries[-limit:]

    def clear(self) -> None:
        """Remove all journal entries (thread-safe)."""
        with self._lock:
            self._entries.clear()
            self._save()

    def get_last_run(self, profile_id: str) -> dict | None:
        """Get the most recent FINISHED backup run for a profile (thread-safe).

        Skips verify triggers and non-terminal entries (``started`` /
        ``waiting``): a ``started`` row is either a run still in flight
        (the live progress bar already conveys that) or an orphan left
        by a crash / by the pre-3.7.47 ``update_last`` bug — neither
        should be painted as a *failed* backup on the dashboard card.
        Returns the last entry with a terminal status.

        Args:
            profile_id: Profile to look up.

        Returns:
            The last terminal backup entry dict, or None.
        """
        terminal = {"success", "failed", "cancelled", "skipped"}
        with self._lock:
            for entry in reversed(self._entries):
                if entry.get("profile_id") != profile_id:
                    continue
                if entry.get("trigger") == "verify":
                    continue
                if entry.get("status") not in terminal:
                    continue
                return entry
            return None


class SchedulerState:
    """Tracks last trigger time per profile to prevent duplicates.

    Thread-safe: all read/write operations are protected by an
    internal lock to prevent race conditions between the scheduler
    daemon thread and the main thread.
    """

    def __init__(self, config_dir: Path):
        self._path = config_dir / "scheduler_state.json"
        self._state: dict[str, str] = {}
        self._lock = threading.Lock()
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                self._state = json.loads(self._path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                self._state = {}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._state, indent=2), encoding="utf-8")

    def get_last_trigger(self, profile_id: str) -> datetime | None:
        """Get the last trigger time for a profile (thread-safe).

        Args:
            profile_id: Profile to look up.

        Returns:
            The datetime of the last trigger, or None.
        """
        with self._lock:
            ts = self._state.get(profile_id)
            if ts:
                try:
                    return datetime.fromisoformat(ts)
                except ValueError:
                    pass
            return None

    def set_last_trigger(self, profile_id: str, dt: datetime) -> None:
        """Record a trigger time for a profile (thread-safe).

        Args:
            profile_id: Profile that was triggered.
            dt: Timestamp of the trigger.
        """
        with self._lock:
            self._state[profile_id] = dt.isoformat()
            self._save()

    def get_last_verify(self, profile_id: str) -> datetime | None:
        """Get the last verify time for a profile (thread-safe).

        Args:
            profile_id: Profile to look up.

        Returns:
            The datetime of the last verify, or None.
        """
        with self._lock:
            ts = self._state.get(f"verify_{profile_id}")
            if ts:
                try:
                    return datetime.fromisoformat(ts)
                except ValueError:
                    pass
            return None

    def set_last_verify(self, profile_id: str, dt: datetime) -> None:
        """Record a verify time for a profile (thread-safe).

        Args:
            profile_id: Profile that was verified.
            dt: Timestamp of the verification.
        """
        with self._lock:
            self._state[f"verify_{profile_id}"] = dt.isoformat()
            self._save()


class InAppScheduler:
    """Daemon thread that checks for due backups."""

    def __init__(
        self,
        config_dir: Path,
        get_profiles: Callable[[], list[BackupProfile]],
        backup_callback: Callable[[BackupProfile], None],
        config_manager=None,
    ):
        self._config_dir = config_dir
        self._get_profiles = get_profiles
        self._backup_callback = backup_callback
        self._config_manager = config_manager
        self._journal = ScheduleJournal(config_dir)
        self._state = SchedulerState(config_dir)
        self._op_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._running = False
        self._last_check_time = time.monotonic()
        self.skip_startup_check = False
        # Track profiles currently being backed up so a long-running
        # backup cannot be re-triggered by a "sleep detected" pass
        # (long backup > 3× CHECK_INTERVAL looks like an OS sleep).
        # Guarded by ``_in_progress_lock`` for thread-safe add/discard.
        self._profile_in_progress: set[str] = set()
        self._in_progress_lock = threading.Lock()

    @property
    def journal(self) -> ScheduleJournal:
        return self._journal

    @property
    def op_lock(self) -> threading.Lock:
        """Lock for compound state+journal operations.

        External callers (e.g. UI thread updating journal after a
        scheduled backup) should acquire this lock to participate
        in the same atomicity scheme as the scheduler daemon thread.
        """
        return self._op_lock

    def mark_triggered_now(self, profile_id: str, dt: datetime | None = None) -> None:
        """Record an out-of-band "triggered now" event for a profile.

        Lets the UI bump a profile's ``last_trigger`` when the user
        just ran it manually or just went through the wizard — so the
        scheduler's next ``is_due`` check does not fire again
        immediately. Callers were previously poking
        ``scheduler._state.set_last_trigger`` directly, which is a
        private attribute that can change shape between versions.

        Args:
            profile_id: The profile that was just triggered.
            dt: Timestamp of the trigger. Defaults to ``datetime.now()``.
        """
        self._state.set_last_trigger(profile_id, dt or datetime.now())

    def mark_verify_now(self, profile_id: str, dt: datetime | None = None) -> None:
        """Record an out-of-band "verified now" event for a profile.

        Symmetric counterpart of :meth:`mark_triggered_now` for the
        periodic verification clock.  Callers (wizard, profile import,
        manual verify) use this to seed ``last_verify`` so the next
        ``_check_verify_due`` tick does not fire immediately on a
        profile that has just been created or just been manually
        verified.

        Args:
            profile_id: The profile that was just verified.
            dt: Timestamp of the verification. Defaults to
                ``datetime.now()``.
        """
        self._state.set_last_verify(profile_id, dt or datetime.now())

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="Scheduler")
        self._thread.start()
        logger.info("Scheduler started")

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
        logger.info("Scheduler stopped")

    def _run(self) -> None:
        # On startup, check for missed backups (cold boot scenario)
        if self.skip_startup_check:
            logger.info("Skipping startup missed-backup check (first launch)")
            self.skip_startup_check = False
        else:
            try:
                self._check_startup_missed()
            except Exception:
                logger.exception("Startup missed-backup check error")

        # Reset the wake-from-sleep clock AFTER startup recovery, which
        # may have run a backup that blocked us for tens of minutes.
        # Without this, the first _check_schedules tick interprets the
        # backup duration as a sleep gap and emits a misleading
        # "Detected system wake from sleep" line.
        self._last_check_time = time.monotonic()

        while self._running:
            try:
                self._check_schedules()
            except Exception:
                logger.exception("Scheduler error")
            self._stop_event.wait(CHECK_INTERVAL)
            if self._stop_event.is_set():
                break

    def _check_startup_missed(self) -> None:
        """Check for missed backups on application startup (cold boot).

        Unlike sleep/wake detection which relies on monotonic time jumps,
        this method explicitly checks every active profile against the
        persistent scheduler state to catch backups missed while the PC
        was completely off.
        """
        now = datetime.now()
        profiles = self._get_profiles()
        logger.info("Startup missed-backup check: %d profiles loaded", len(profiles))

        for profile in profiles:
            if not profile.active:
                continue
            if not profile.schedule.enabled:
                continue
            if profile.schedule.frequency == ScheduleFrequency.MANUAL:
                continue

            # A backup that is still running from before the sleep
            # detection must not be re-triggered — double-triggers
            # on the same profile waste work and trip the profile
            # lock. The profile_lock catches it as a safety net but
            # the cleaner fix is to never issue the second trigger.
            with self._in_progress_lock:
                if profile.id in self._profile_in_progress:
                    logger.info(
                        "Skipping missed-backup trigger for '%s' " "(already running)",
                        profile.name,
                    )
                    continue

            last = self._state.get_last_trigger(profile.id)
            last_str = last.isoformat() if last else "never"
            logger.info(
                "Profile '%s': schedule=%s at %s, last_trigger=%s",
                profile.name,
                profile.schedule.frequency.value,
                profile.schedule.time,
                last_str,
            )

            # Force a catch-up when the previous run did not complete
            # (process crash, hard power-off mid-backup) even if the
            # current schedule window would normally suppress it.
            # Circuit breaker: after MAX_CRASH_RECOVERY_ATTEMPTS
            # consecutive failures we stop auto-retrying to avoid a
            # boot-loop on broken storage. The user can always re-run
            # manually from the UI.
            crash_recovery_due = (
                not profile.last_backup_completed
                and bool(profile.incomplete_backup_name)
                and profile.crash_recovery_attempts < MAX_CRASH_RECOVERY_ATTEMPTS
            )
            if (
                not profile.last_backup_completed
                and profile.crash_recovery_attempts >= MAX_CRASH_RECOVERY_ATTEMPTS
            ):
                logger.warning(
                    "Crash recovery circuit breaker TRIPPED for '%s' "
                    "after %d attempts — manual intervention required",
                    profile.name,
                    profile.crash_recovery_attempts,
                )

            if crash_recovery_due or self._is_due(profile, now):
                reason = "crash recovery" if crash_recovery_due else "missed schedule"
                logger.info(
                    "Missed backup detected on startup for '%s' (%s) — triggering",
                    profile.name,
                    reason,
                )
                if crash_recovery_due:
                    # Increment BEFORE the trigger so a crash during
                    # the trigger still bumps the counter on disk.
                    profile.crash_recovery_attempts += 1
                    if self._config_manager is not None:
                        try:
                            self._config_manager.save_profile(profile)
                        except Exception as exc:
                            logger.warning(
                                "Could not persist crash_recovery_attempts: %s",
                                exc,
                            )
                self._trigger_backup(profile, now, trigger="missed_recovery")

    def _check_schedules(self) -> None:
        now = datetime.now()
        elapsed = time.monotonic() - self._last_check_time

        # Detect sleep/hibernation (time jump > 3x check interval).
        # The clock is reset at the END of this method so that a long
        # _trigger_backup or _check_verify_due (each may block this
        # thread for tens of minutes) does not look like an OS sleep
        # to the next iteration.
        if elapsed > CHECK_INTERVAL * 3:
            logger.info("Detected system wake from sleep (%.0fs gap)", elapsed)
            self._check_missed_backups(now)

        for profile in self._get_profiles():
            if not profile.active:
                continue
            if not profile.schedule.enabled:
                continue
            if profile.schedule.frequency == ScheduleFrequency.MANUAL:
                continue
            # Skip profiles that are already running — avoids
            # double-triggering the same profile after a long backup
            # was interpreted as an OS sleep.
            with self._in_progress_lock:
                if profile.id in self._profile_in_progress:
                    continue
            if self._is_due(profile, now):
                self._trigger_backup(profile, now)

            # Periodic integrity verification
            if profile.schedule.verify_enabled:
                self._check_verify_due(profile, now)

        # Refresh AFTER the for-loop, not before. _trigger_backup and
        # _check_verify_due each run synchronously in this thread and
        # can take tens of minutes for large backups. Resetting at the
        # start (the previous behaviour) made that duration count
        # toward the next iteration's "wake from sleep" gap, producing
        # a cosmetic "Detected system wake from sleep (1350s gap)" log
        # right after every long backup.
        self._last_check_time = time.monotonic()

    def _is_due(self, profile: BackupProfile, now: datetime) -> bool:
        sched = profile.schedule
        last = self._state.get_last_trigger(profile.id)

        if last is None:
            return True

        if sched.frequency == ScheduleFrequency.HOURLY:
            return (now - last).total_seconds() >= 3600

        # Parse target time
        try:
            target_hour, target_minute = map(int, sched.time.split(":"))
        except (ValueError, AttributeError):
            target_hour, target_minute = 2, 0

        target_today = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)

        # The "due" check compares ``last`` to TODAY's scheduled
        # point (``target_today``), not to today's date. A manual run
        # earlier today (before the scheduled hour) used to be enough
        # to make ``last.date() == now.date()`` and suppress the
        # scheduled trigger for the whole day. With ``last < target_today``
        # the scheduled slot fires as long as no run has happened
        # since the cron point today — manual runs from earlier in
        # the same day no longer eat the slot.
        if sched.frequency == ScheduleFrequency.DAILY:
            return now >= target_today and last < target_today

        elif sched.frequency == ScheduleFrequency.WEEKLY:
            if now.weekday() != sched.day_of_week:
                return False
            return now >= target_today and last < target_today

        elif sched.frequency == ScheduleFrequency.MONTHLY:
            import calendar

            max_day = calendar.monthrange(now.year, now.month)[1]
            day = min(sched.day_of_month, max_day)
            if now.day != day:
                return False
            return now >= target_today and last < target_today

        return False

    def _trigger_backup(
        self,
        profile: BackupProfile,
        now: datetime,
        trigger: str = "in_app",
    ) -> None:
        """Trigger a backup with automatic retry on failure.

        If retry is enabled in the profile's schedule config, failed
        backups are retried after the configured delays (default:
        2, 10, 30, 90, 240 minutes). Retries run in the scheduler
        thread to avoid blocking the main thread.

        Args:
            profile: Backup profile to execute.
            now: Current timestamp.
            trigger: Trigger source for journal logging.
        """
        logger.info("Triggering scheduled backup: %s", profile.name)
        with self._op_lock:
            self._state.set_last_trigger(profile.id, now)
            self._journal.add(
                ScheduleLogEntry(
                    profile_id=profile.id,
                    profile_name=profile.name,
                    trigger=trigger,
                    status="started",
                )
            )

        # Claim the profile's run slot atomically BEFORE the callback.
        # If a UI "Start backup" (or another pass) already holds it, skip
        # this trigger entirely: calling the callback would invoke
        # run_backup, which logs the confusing "Backup rejected" line
        # before the ProfileLockError even propagates. The concurrent run
        # already satisfies this schedule window.
        if not self.try_acquire_profile(profile.id):
            logger.info("Scheduled trigger skipped (already running): %s", profile.name)
            with self._op_lock:
                self._journal.update_last(
                    profile_id=profile.id,
                    status="skipped",
                    detail="concurrent run already in progress",
                    timestamp=datetime.now().isoformat(),
                )
            return

        # Callback runs OUTSIDE the lock (can take minutes)
        try:
            self._backup_callback(profile)
            with self._op_lock:
                self._journal.update_last(
                    profile_id=profile.id,
                    status="success",
                    timestamp=datetime.now().isoformat(),
                )
            logger.info("Scheduled backup succeeded: %s", profile.name)
        except Exception as e:
            # Classify the exception so user-driven aborts are not
            # treated as backup failures (no crash-recovery bump, no
            # retry storm):
            #
            # - ProfileLockError: another run (UI "Run now", another
            #   scheduler instance) is already handling this profile —
            #   our trigger is effectively satisfied by the concurrent
            #   run. Retrying would produce a SECOND backup for the
            #   same schedule window once the other run releases the
            #   lock.
            # - PrecheckUserTimeoutError: the destinations-unavailable
            #   modal was left unanswered until the 30-min hard
            #   timeout. The backup never started because the user
            #   could not confirm the targets were back online —
            #   re-prompting in 2 minutes when the user is asleep is
            #   pure noise (18/05/2026 incident: three such timeouts
            #   in a row tripped the circuit breaker for TestNP).
            from src.core.exceptions import PrecheckUserTimeoutError
            from src.core.profile_lock import ProfileLockError

            is_concurrent = isinstance(e, ProfileLockError)
            is_user_timeout = isinstance(e, PrecheckUserTimeoutError)
            is_skip = is_concurrent or is_user_timeout

            level = logger.info if is_skip else logger.exception
            if is_concurrent:
                outcome_label = "skipped (concurrent)"
            elif is_user_timeout:
                outcome_label = "skipped (precheck user timeout)"
            else:
                outcome_label = "failed"
            level("Scheduled backup %s: %s", outcome_label, profile.name)
            with self._op_lock:
                self._journal.update_last(
                    profile_id=profile.id,
                    status="skipped" if is_skip else "failed",
                    detail=f"{type(e).__name__}: {e}",
                    timestamp=datetime.now().isoformat(),
                )

            # Retry logic — bypassed for skip-class exceptions
            if profile.schedule.retry_enabled and not is_skip:
                self._retry_backup(profile, trigger)
        finally:
            with self._in_progress_lock:
                self._profile_in_progress.discard(profile.id)

    def _retry_backup(self, profile: BackupProfile, trigger: str) -> None:
        """Retry a failed backup using configured delay intervals.

        Sleeps between attempts in the scheduler daemon thread.
        Stops retrying on success or after all delays are exhausted.

        Args:
            profile: Backup profile to retry.
            trigger: Original trigger source for journal logging.
        """
        delays = profile.schedule.retry_delay_minutes
        if not delays:
            return

        for attempt, delay_minutes in enumerate(delays, start=1):
            total_attempts = len(delays)
            logger.info(
                "Retry %d/%d for '%s' in %d minutes",
                attempt,
                total_attempts,
                profile.name,
                delay_minutes,
            )
            with self._op_lock:
                self._journal.add(
                    ScheduleLogEntry(
                        profile_id=profile.id,
                        profile_name=profile.name,
                        trigger=f"retry_{attempt}",
                        status="waiting",
                        detail=f"Retry {attempt}/{total_attempts} in {delay_minutes}min",
                    )
                )

            # Sleep in small increments to allow scheduler stop
            sleep_seconds = delay_minutes * 60
            slept = 0
            while slept < sleep_seconds and self._running:
                chunk = min(CHECK_INTERVAL, sleep_seconds - slept)
                self._stop_event.wait(chunk)
                if self._stop_event.is_set():
                    break
                slept += chunk

            if not self._running:
                logger.info("Scheduler stopped — aborting retry for '%s'", profile.name)
                return

            # Attempt the backup again
            logger.info(
                "Retry %d/%d executing for '%s'",
                attempt,
                total_attempts,
                profile.name,
            )
            with self._op_lock:
                self._journal.update_last(profile_id=profile.id, status="started")

            # Callback runs OUTSIDE the lock
            try:
                self._backup_callback(profile)
                with self._op_lock:
                    self._journal.update_last(
                        profile_id=profile.id,
                        status="success",
                        timestamp=datetime.now().isoformat(),
                    )
                logger.info(
                    "Retry %d/%d succeeded for '%s'",
                    attempt,
                    total_attempts,
                    profile.name,
                )
                return  # Success — stop retrying
            except Exception as e:
                logger.exception(
                    "Retry %d/%d failed for '%s'",
                    attempt,
                    total_attempts,
                    profile.name,
                )
                with self._op_lock:
                    self._journal.update_last(
                        profile_id=profile.id,
                        status="failed",
                        detail=f"Retry {attempt}/{total_attempts}: {type(e).__name__}: {e}",
                        timestamp=datetime.now().isoformat(),
                    )

        logger.error(
            "All %d retries exhausted for '%s'",
            len(delays),
            profile.name,
        )

    def _check_verify_due(self, profile: BackupProfile, now: datetime) -> None:
        """Check if periodic integrity verification is due for a profile.

        Args:
            profile: Profile with verify_enabled and verify_interval_days.
            now: Current timestamp.
        """
        interval_days = profile.schedule.verify_interval_days
        last_verify = self._state.get_last_verify(profile.id)
        # First observation of this profile: seed the timer at ``now``
        # and bail out. The expected semantics for a fresh profile is
        # "first periodic verify in N days from creation", not "verify
        # right now everything that already lives on the destination".
        # Without this guard, ``_check_verify_due`` fired on the first
        # scheduler tick (CHECK_INTERVAL ≈ 30 s after profile creation)
        # and re-hashed any pre-existing backup on the same destination
        # — including backups belonging to OTHER profiles — in parallel
        # with the user's first backup run. v3.7.3 case: a 56-s-old
        # ``TestLoic`` profile re-verified 39 873 + 3 339 foreign files
        # during its own hash phase. ``mark_verify_now`` is the public
        # API for callers that want to seed the clock at creation; this
        # branch is defence-in-depth for any creation path that forgot.
        if last_verify is None:
            with self._op_lock:
                self._state.set_last_verify(profile.id, now)
            return
        if (now - last_verify).days < interval_days:
            return

        logger.info(
            "Triggering periodic verification for '%s' (interval=%dd)",
            profile.name,
            interval_days,
        )
        with self._op_lock:
            self._state.set_last_verify(profile.id, now)
            self._journal.add(
                ScheduleLogEntry(
                    profile_id=profile.id,
                    profile_name=profile.name,
                    trigger="verify",
                    status="started",
                    detail="Periodic integrity verification",
                )
            )

        try:
            from src.core.config import ConfigManager
            from src.core.integrity_verifier import IntegrityVerifier

            cm = self._config_manager or ConfigManager(self._config_dir)
            verifier = IntegrityVerifier(profile, cm, events=None)
            result = verifier.verify_all()

            with self._op_lock:
                if result.success:
                    self._journal.update_last(
                        profile_id=profile.id,
                        status="success",
                        detail=f"Verified {result.ok_count} backups OK",
                    )
                else:
                    self._journal.update_last(
                        profile_id=profile.id,
                        status="failed",
                        detail=f"{result.error_count} error(s), {result.ok_count} OK",
                    )
            logger.info(
                "Verification for '%s': %d OK, %d errors",
                profile.name,
                result.ok_count,
                result.error_count,
            )
        except Exception as e:
            logger.exception("Verification failed for '%s'", profile.name)
            with self._op_lock:
                self._journal.update_last(
                    profile_id=profile.id,
                    status="failed",
                    detail=f"Verify error: {type(e).__name__}: {e}",
                )

    # ------------------------------------------------------------------
    # Public mark/unmark API for backups triggered OUTSIDE the scheduler
    # ------------------------------------------------------------------

    def try_acquire_profile(self, profile_id: str) -> bool:
        """Atomically claim a profile's run slot (test-and-set).

        Under ``_in_progress_lock``, add ``profile_id`` and return True
        if the slot was free, or return False if another run already
        holds it. This is the single point of coordination that stops a
        scheduled catch-up and a UI "Start backup" from both calling
        ``run_backup`` on the same profile at once — the race that
        surfaced as the confusing "Backup rejected" line when a
        freshly-activated profile became due at the same moment the user
        clicked Start.

        Whoever acquires MUST release via :meth:`unmark_profile_running`
        when the run finishes or is abandoned.

        Args:
            profile_id: Profile identifier (``BackupProfile.id``).

        Returns:
            True if the slot was free and is now held by the caller;
            False if it was already taken.
        """
        with self._in_progress_lock:
            if profile_id in self._profile_in_progress:
                return False
            self._profile_in_progress.add(profile_id)
            return True

    def mark_profile_running(self, profile_id: str) -> None:
        """Mark a profile as actively running.

        The scheduler tracks its own triggers in ``_profile_in_progress``
        so periodic checks (``_check_schedules``, ``_check_missed_backups``,
        ``_check_startup_missed``) can skip profiles that already have a
        backup in flight. When a backup is started OUTSIDE the scheduler
        — e.g. the user clicks "Start backup" in the Run tab — the
        scheduler is unaware of it; without this call the next periodic
        check happily fires a second trigger which then trips the
        engine's ``ProfileLockError``, producing a confusing
        "Backup rejected" line in the Run-tab log.

        Idempotent: calling twice for the same id is a no-op.

        Args:
            profile_id: Profile identifier (``BackupProfile.id``).
        """
        with self._in_progress_lock:
            self._profile_in_progress.add(profile_id)

    def unmark_profile_running(self, profile_id: str) -> None:
        """Symmetric to :meth:`mark_profile_running`.

        Idempotent: calling for an unknown id is a silent no-op so the
        UI's ``finally`` block can call it without first checking that
        ``mark_profile_running`` succeeded.
        """
        with self._in_progress_lock:
            self._profile_in_progress.discard(profile_id)

    def _check_missed_backups(self, now: datetime) -> None:
        """Check for missed backups after a wake-from-sleep event.

        Called from ``_check_schedules`` when the monotonic clock
        shows a gap larger than ``CHECK_INTERVAL * 3``.
        Two guards protect against spurious triggers:

        1. ``_profile_in_progress`` — skips profiles that are still
           running from before the sleep detection (a backup that
           takes longer than 3× CHECK_INTERVAL itself looks like a
           system sleep to this code).
        2. ``crash_recovery_due`` — forces a trigger when the last
           run did not complete even if the schedule window would
           normally suppress it (process crash mid-backup).
        """
        for profile in self._get_profiles():
            if not profile.active:
                continue
            if not profile.schedule.enabled:
                continue
            with self._in_progress_lock:
                if profile.id in self._profile_in_progress:
                    logger.info(
                        "Skipping missed-backup trigger for '%s' " "(already running)",
                        profile.name,
                    )
                    continue
            crash_recovery_due = (
                not profile.last_backup_completed
                and bool(profile.incomplete_backup_name)
                and profile.crash_recovery_attempts < MAX_CRASH_RECOVERY_ATTEMPTS
            )
            if crash_recovery_due or self._is_due(profile, now):
                reason = "crash recovery" if crash_recovery_due else "missed schedule"
                logger.info("Missed backup detected (%s): %s", reason, profile.name)
                if crash_recovery_due:
                    profile.crash_recovery_attempts += 1
                    if self._config_manager is not None:
                        try:
                            self._config_manager.save_profile(profile)
                        except Exception as exc:
                            logger.warning(
                                "Could not persist crash_recovery_attempts: %s",
                                exc,
                            )
                self._trigger_backup(profile, now)

    def get_next_run_info(self, profile: BackupProfile) -> str:
        """Get human-readable next run info."""
        sched = profile.schedule
        if not sched.enabled or sched.frequency == ScheduleFrequency.MANUAL:
            return "Manual"

        freq_labels = {
            ScheduleFrequency.HOURLY: "Every hour",
            ScheduleFrequency.DAILY: f"Daily at {sched.time}",
            ScheduleFrequency.WEEKLY: f"Weekly at {sched.time}",
            ScheduleFrequency.MONTHLY: f"Monthly at {sched.time}",
        }
        return freq_labels.get(sched.frequency, "Unknown")


class AutoStart:
    """Manages Windows auto-start via HKCU\\...\\Run registry key.

    Uses the standard Windows mechanism for per-user auto-start programs.
    The registry key is natively cleaned up by MSI uninstallers, unlike
    VBS scripts in the Startup folder which could persist after removal.
    """

    _REG_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"
    _REG_VALUE = "BackupManager"

    @classmethod
    def ensure_startup(cls, show_window: bool = True) -> None:
        """Create or update auto-start registry entry.

        Idempotent: queries the current value first and only writes
        when it differs from the desired command. ``ensure_startup``
        is invoked after every ``save_profile`` so a write-every-time
        path floods the run log with identical "Auto-start configured"
        lines; the read-before-write keeps the log readable and avoids
        registry churn.

        Args:
            show_window: If False, adds --minimized flag to the command.
        """
        import sys

        from src.__main__ import _is_nuitka

        if not (getattr(sys, "frozen", False) or _is_nuitka()):
            return

        exe_path = Path(sys.executable)
        args = "" if show_window else " --minimized"
        command = f'"{exe_path}"{args}'

        try:
            import winreg

            current: str | None = None
            try:
                with winreg.OpenKey(
                    winreg.HKEY_CURRENT_USER,
                    cls._REG_KEY,
                    0,
                    winreg.KEY_READ,
                ) as key:
                    current, _ = winreg.QueryValueEx(key, cls._REG_VALUE)
            except FileNotFoundError:
                pass

            if current == command:
                return

            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                cls._REG_KEY,
                0,
                winreg.KEY_SET_VALUE,
            ) as key:
                winreg.SetValueEx(key, cls._REG_VALUE, 0, winreg.REG_SZ, command)
            logger.info("Auto-start configured via registry: %s", command)
        except OSError as e:
            logger.warning("Could not set auto-start registry key: %s", e)

    @classmethod
    def disable(cls) -> tuple[bool, str]:
        """Remove auto-start registry entry."""
        try:
            import winreg

            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                cls._REG_KEY,
                0,
                winreg.KEY_SET_VALUE,
            ) as key:
                try:
                    winreg.DeleteValue(key, cls._REG_VALUE)
                except FileNotFoundError:
                    return True, "Auto-start was not enabled"
        except OSError as e:
            return False, f"Could not disable: {e}"

        return True, "Auto-start disabled"

    @classmethod
    def is_enabled(cls) -> bool:
        """Check if auto-start registry entry exists."""
        try:
            import winreg

            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, cls._REG_KEY, 0, winreg.KEY_READ) as key:
                winreg.QueryValueEx(key, cls._REG_VALUE)
                return True
        except (FileNotFoundError, OSError):
            return False

    @classmethod
    def is_show_window(cls) -> bool:
        """Check if startup is configured to show window."""
        try:
            import winreg

            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, cls._REG_KEY, 0, winreg.KEY_READ) as key:
                value, _ = winreg.QueryValueEx(key, cls._REG_VALUE)
                return "--minimized" not in value
        except (FileNotFoundError, OSError):
            return True

