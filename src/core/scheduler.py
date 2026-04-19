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

    def update_last(self, **kwargs) -> None:
        """Update the most recent journal entry (thread-safe).

        Args:
            **kwargs: Fields to update on the last entry.
        """
        with self._lock:
            if self._entries:
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
        """Get the most recent backup run for a given profile (thread-safe).

        Skips non-backup entries (e.g. verify triggers) so the
        dashboard only shows actual backup results.

        Args:
            profile_id: Profile to look up.

        Returns:
            The last matching backup entry dict, or None.
        """
        with self._lock:
            for entry in reversed(self._entries):
                if entry.get("profile_id") != profile_id:
                    continue
                if entry.get("trigger") == "verify":
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

        # Detect sleep/hibernation (time jump > 3x check interval)
        if elapsed > CHECK_INTERVAL * 3:
            logger.info("Detected system wake from sleep (%.0fs gap)", elapsed)
            self._check_missed_backups(now)

        self._last_check_time = time.monotonic()

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

        if sched.frequency == ScheduleFrequency.DAILY:
            return now >= target_today and last.date() < now.date()

        elif sched.frequency == ScheduleFrequency.WEEKLY:
            if now.weekday() != sched.day_of_week:
                return False
            return now >= target_today and (now - last).days >= 1

        elif sched.frequency == ScheduleFrequency.MONTHLY:
            import calendar

            max_day = calendar.monthrange(now.year, now.month)[1]
            day = min(sched.day_of_month, max_day)
            if now.day != day:
                return False
            return now >= target_today and (now - last).days >= 1

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

        # Mark as in-progress BEFORE the callback so a concurrent
        # "sleep detected" pass cannot re-trigger the same profile.
        with self._in_progress_lock:
            self._profile_in_progress.add(profile.id)

        # Callback runs OUTSIDE the lock (can take minutes)
        try:
            self._backup_callback(profile)
            with self._op_lock:
                self._journal.update_last(
                    status="success",
                    timestamp=datetime.now().isoformat(),
                )
            logger.info("Scheduled backup succeeded: %s", profile.name)
        except Exception as e:
            # Do NOT retry a ProfileLockError: another run (UI "Run now",
            # another scheduler instance) is already handling this
            # profile, so our trigger has effectively been satisfied
            # by the concurrent run. Retrying would produce a SECOND
            # backup for the same schedule window once the other run
            # releases the lock — wasteful and misleading in the
            # journal.
            from src.core.profile_lock import ProfileLockError

            is_concurrent = isinstance(e, ProfileLockError)

            level = logger.info if is_concurrent else logger.exception
            level(
                "Scheduled backup %s: %s",
                "skipped (concurrent)" if is_concurrent else "failed",
                profile.name,
            )
            with self._op_lock:
                self._journal.update_last(
                    status="skipped" if is_concurrent else "failed",
                    detail=f"{type(e).__name__}: {e}",
                    timestamp=datetime.now().isoformat(),
                )

            # Retry logic — skip for concurrent-run rejections
            if profile.schedule.retry_enabled and not is_concurrent:
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
                self._journal.update_last(status="started")

            # Callback runs OUTSIDE the lock
            try:
                self._backup_callback(profile)
                with self._op_lock:
                    self._journal.update_last(
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
        if last_verify and (now - last_verify).days < interval_days:
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
                        status="success",
                        detail=f"Verified {result.ok_count} backups OK",
                    )
                else:
                    self._journal.update_last(
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
                    status="failed",
                    detail=f"Verify error: {type(e).__name__}: {e}",
                )

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

    # Legacy VBS path — used only for migration cleanup.
    _LEGACY_VBS = Path(
        os.environ.get("APPDATA", ""),
        "Microsoft",
        "Windows",
        "Start Menu",
        "Programs",
        "Startup",
        "BackupManager.vbs",
    )

    @classmethod
    def ensure_startup(cls, show_window: bool = True) -> None:
        """Create or update auto-start registry entry.

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

        cls._cleanup_legacy_vbs()

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
                    cls._cleanup_legacy_vbs()
                    return True, "Auto-start was not enabled"
        except OSError as e:
            return False, f"Could not disable: {e}"

        cls._cleanup_legacy_vbs()
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

    @classmethod
    def _cleanup_legacy_vbs(cls) -> None:
        """Remove legacy VBS startup script if it exists."""
        try:
            if cls._LEGACY_VBS.exists():
                cls._LEGACY_VBS.unlink()
                logger.info("Removed legacy VBS auto-start: %s", cls._LEGACY_VBS)
        except OSError as e:
            logger.debug("Could not remove legacy VBS: %s", e)
