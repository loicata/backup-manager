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

    def get_last_run(self, profile_id: str) -> dict | None:
        """Get the most recent run for a given profile (thread-safe).

        Args:
            profile_id: Profile to look up.

        Returns:
            The last matching entry dict, or None.
        """
        with self._lock:
            for entry in reversed(self._entries):
                if entry.get("profile_id") == profile_id:
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


class InAppScheduler:
    """Daemon thread that checks for due backups."""

    def __init__(
        self,
        config_dir: Path,
        get_profiles: Callable[[], list[BackupProfile]],
        backup_callback: Callable[[BackupProfile], None],
    ):
        self._config_dir = config_dir
        self._get_profiles = get_profiles
        self._backup_callback = backup_callback
        self._journal = ScheduleJournal(config_dir)
        self._state = SchedulerState(config_dir)
        self._thread: threading.Thread | None = None
        self._running = False
        self._last_check_time = time.monotonic()

    @property
    def journal(self) -> ScheduleJournal:
        return self._journal

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="Scheduler")
        self._thread.start()
        logger.info("Scheduler started")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Scheduler stopped")

    def _run(self) -> None:
        # On startup, check for missed backups (cold boot scenario)
        try:
            self._check_startup_missed()
        except Exception:
            logger.exception("Startup missed-backup check error")

        while self._running:
            try:
                self._check_schedules()
            except Exception:
                logger.exception("Scheduler error")
            time.sleep(CHECK_INTERVAL)

    def _check_startup_missed(self) -> None:
        """Check for missed backups on application startup (cold boot).

        Unlike sleep/wake detection which relies on monotonic time jumps,
        this method explicitly checks every active profile against the
        persistent scheduler state to catch backups missed while the PC
        was completely off.
        """
        now = datetime.now()
        profiles = self._get_profiles()
        logger.info(
            "Startup missed-backup check: %d profiles loaded", len(profiles)
        )

        for profile in profiles:
            if not profile.active:
                continue
            if not profile.schedule.enabled:
                continue
            if profile.schedule.frequency == ScheduleFrequency.MANUAL:
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

            if self._is_due(profile, now):
                logger.info(
                    "Missed backup detected on startup for '%s' — triggering",
                    profile.name,
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
            if self._is_due(profile, now):
                self._trigger_backup(profile, now)

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
            day = min(sched.day_of_month, 28)
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
        self._state.set_last_trigger(profile.id, now)
        self._journal.add(
            ScheduleLogEntry(
                profile_id=profile.id,
                profile_name=profile.name,
                trigger=trigger,
                status="started",
            )
        )

        try:
            self._backup_callback(profile)
            self._journal.update_last(status="success")
            logger.info("Scheduled backup succeeded: %s", profile.name)
        except Exception as e:
            logger.exception("Scheduled backup failed: %s", profile.name)
            self._journal.update_last(
                status="failed",
                detail=f"{type(e).__name__}: {e}",
            )

            # Retry logic
            if profile.schedule.retry_enabled:
                self._retry_backup(profile, trigger)

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
                time.sleep(min(CHECK_INTERVAL, sleep_seconds - slept))
                slept += CHECK_INTERVAL

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
            self._journal.update_last(status="started")

            try:
                self._backup_callback(profile)
                self._journal.update_last(status="success")
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
                self._journal.update_last(
                    status="failed",
                    detail=f"Retry {attempt}/{total_attempts}: {type(e).__name__}: {e}",
                )

        logger.error(
            "All %d retries exhausted for '%s'",
            len(delays),
            profile.name,
        )

    def _check_missed_backups(self, now: datetime) -> None:
        for profile in self._get_profiles():
            if not profile.active:
                continue
            if not profile.schedule.enabled:
                continue
            if self._is_due(profile, now):
                logger.info("Missed backup detected: %s", profile.name)
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
    """Manages Windows auto-start via VBS in Startup folder."""

    STARTUP_DIR = Path(
        os.environ.get("APPDATA", ""),
        "Microsoft",
        "Windows",
        "Start Menu",
        "Programs",
        "Startup",
    )
    VBS_FILENAME = "BackupManager.vbs"

    @classmethod
    def ensure_startup(cls, show_window: bool = True) -> None:
        """Create or update VBS startup script."""
        import sys

        if not getattr(sys, "frozen", False):
            return  # Only for frozen exe

        exe_path = Path(sys.executable)
        args = "" if show_window else " --minimized"

        vbs_content = (
            f'Set WshShell = CreateObject("WScript.Shell")\n'
            f'WshShell.Run """{exe_path}""{args}", '
            f'{"1" if show_window else "0"}, False\n'
        )

        vbs_path = cls.STARTUP_DIR / cls.VBS_FILENAME
        try:
            vbs_path.write_text(vbs_content, encoding="utf-8")
            logger.info("Auto-start configured: %s", vbs_path)
        except OSError as e:
            logger.warning("Could not create startup script: %s", e)

    @classmethod
    def disable(cls) -> tuple[bool, str]:
        """Remove VBS startup script."""
        vbs_path = cls.STARTUP_DIR / cls.VBS_FILENAME
        try:
            if vbs_path.exists():
                vbs_path.unlink()
                return True, "Auto-start disabled"
            return True, "Auto-start was not enabled"
        except OSError as e:
            return False, f"Could not disable: {e}"

    @classmethod
    def is_enabled(cls) -> bool:
        """Check if auto-start is configured."""
        return (cls.STARTUP_DIR / cls.VBS_FILENAME).exists()

    @classmethod
    def is_show_window(cls) -> bool:
        """Check if startup is configured to show window."""
        vbs_path = cls.STARTUP_DIR / cls.VBS_FILENAME
        if vbs_path.exists():
            try:
                content = vbs_path.read_text(encoding="utf-8")
                return "--minimized" not in content
            except OSError:
                pass
        return True
