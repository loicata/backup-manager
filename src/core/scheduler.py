"""
Backup Manager - Scheduler
===========================
In-app backup scheduler running in a daemon thread.

Components:
  InAppScheduler    — checks every 30s if any profile is due for backup
  ScheduleJournal   — persistent log of all scheduled runs (JSON in APPDATA)
  ScheduleState     — tracks last trigger time per profile (prevents duplicates)
  AutoStart         — manages Windows Registry key for auto-start at login

Scheduling logic (_is_due):
  - Compares current time against profile.schedule.time
  - Checks last trigger to prevent duplicate runs within the same period
  - Detects missed backups after sleep/hibernation (runs immediately)
  - Supports: hourly, daily, weekly (specific day), monthly (specific date)

Threading: the scheduler thread calls backup_callback(profile) which
posts to gui.root.after(0, ...) to run the backup on the main thread.
"""

import json
import logging
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

from src.core.config import BackupProfile, ConfigManager, ScheduleFrequency

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
#  Schedule Execution Journal
# ──────────────────────────────────────────────
@dataclass
# ── Single entry in the schedule journal ──
# Recorded for every scheduled backup: started, success, failure, retry.
class ScheduleLogEntry:
    """A single entry in the schedule execution journal."""
    timestamp: str = ""
    profile_id: str = ""
    profile_name: str = ""
    trigger: str = ""           # "in_app" or "windows_task"
    status: str = ""            # "started", "success", "failed", "skipped"
    detail: str = ""
    files_count: int = 0
    duration_seconds: float = 0.0


# ── Persistent log of all scheduled backup runs ──
# Stored in %APPDATA%/BackupManager/schedule_journal.json
# Limited to MAX_ENTRIES to prevent unbounded growth.
class ScheduleJournal:
    """
    Persistent journal of all scheduled backup executions.
    Stored as a JSON file in the app config directory.
    """

    MAX_ENTRIES = 500  # Keep last N entries

    def __init__(self, config_manager: ConfigManager):
        self._path = config_manager.CONFIG_DIR / "schedule_journal.json"
        self._entries: list[dict] = self._load()

    def _load(self) -> list[dict]:
        if self._path.exists():
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data if isinstance(data, list) else []
            except (json.JSONDecodeError, OSError):
                pass
        return []

    def _save(self):
        # Trim to max entries
        self._entries = self._entries[-self.MAX_ENTRIES:]
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self._entries, f, indent=1, ensure_ascii=False)
        except OSError as e:
            logger.error(f"Cannot save schedule journal: {e}")

    def add(self, entry: ScheduleLogEntry):
        """Add an entry to the journal."""
        self._entries.append(asdict(entry))
        self._save()

    def update_last(self, **kwargs):
        """Update the most recent entry (e.g., set status after completion)."""
        if self._entries:
            self._entries[-1].update(kwargs)
            self._save()

    def get_entries(self, limit: int = 50, profile_id: str = "") -> list[dict]:
        """Get recent journal entries, optionally filtered by profile."""
        entries = self._entries
        if profile_id:
            entries = [e for e in entries if e.get("profile_id") == profile_id]
        return list(reversed(entries[-limit:]))

    def get_last_run(self, profile_id: str) -> Optional[dict]:
        """Get the last successful run for a profile."""
        for entry in reversed(self._entries):
            if (entry.get("profile_id") == profile_id
                    and entry.get("status") == "success"):
                return entry
        return None

    def clear(self):
        """Clear all entries."""
        self._entries = []
        self._save()


# ──────────────────────────────────────────────
#  Persistent Scheduler State
# ──────────────────────────────────────────────
class SchedulerState:
    """
    Persists the last trigger time for each profile across app restarts.
    Prevents duplicate triggers and detects missed runs.
    """

    def __init__(self, config_manager: ConfigManager):
        self._path = config_manager.CONFIG_DIR / "scheduler_state.json"
        self._state: dict[str, str] = self._load()  # profile_id -> ISO datetime

    def _load(self) -> dict[str, str]:
        if self._path.exists():
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                pass
        return {}

    def _save(self):
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self._state, f, indent=1)
        except OSError as e:
            logger.error(f"Cannot save scheduler state: {e}")

    def get_last_trigger(self, profile_id: str) -> Optional[datetime]:
        """Get the last trigger time for a profile."""
        iso_str = self._state.get(profile_id)
        if iso_str:
            try:
                return datetime.fromisoformat(iso_str)
            except ValueError:
                pass
        return None

    def set_last_trigger(self, profile_id: str, dt: datetime):
        """Record the trigger time for a profile."""
        self._state[profile_id] = dt.isoformat()
        self._save()

    def remove_profile(self, profile_id: str):
        """Remove state for a deleted profile."""
        self._state.pop(profile_id, None)
        self._save()


# ──────────────────────────────────────────────
#  In-App Scheduler
# ──────────────────────────────────────────────
class InAppScheduler:
    """
    Lightweight in-process scheduler that triggers backups while the app runs.
    Uses persistent state to avoid duplicates and detect missed runs.
    Thread-safe: all shared state access is protected by a Lock.
    """

    CHECK_INTERVAL = 30  # seconds between schedule checks

    def __init__(self, config_manager: ConfigManager, backup_callback: Callable):
        self.config = config_manager
        self.backup_callback = backup_callback
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self.state = SchedulerState(config_manager)
        self.journal = ScheduleJournal(config_manager)

    def start(self):
        """Start the scheduler background thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("Scheduler started.")

    def stop(self):
        """Stop the scheduler."""
        with self._lock:
            self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Scheduler stopped.")

    def _run_loop(self):
        """Main scheduler loop. Detects sleep/hibernation via time jumps."""
        time.sleep(2)
        # Check for missed backups at startup
        try:
            self.check_missed_backups()
        except Exception as e:
            logger.error(f"Missed backup check error: {e}")

        last_check = time.monotonic()
        while True:
            with self._lock:
                if not self._running:
                    break
            try:
                now = time.monotonic()
                elapsed = now - last_check
                last_check = now

                # If elapsed >> CHECK_INTERVAL, system was asleep
                if elapsed > self.CHECK_INTERVAL * 3:
                    logger.info(
                        f"Wake from sleep detected ({elapsed:.0f}s gap). "
                        f"Checking for missed backups..."
                    )
                    self.check_missed_backups()

                self._check_schedules()
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
            time.sleep(self.CHECK_INTERVAL)

    def _check_schedules(self):
        """Check all profiles and trigger backups that are due."""
        profiles = self.config.get_all_profiles()
        now = datetime.now()

        for profile in profiles:
            if not profile.schedule.enabled:
                continue
            if profile.schedule.frequency == ScheduleFrequency.MANUAL.value:
                continue

            if self._is_due(profile, now):
                logger.info(f"Scheduled backup triggered: {profile.name}")

                # Record in persistent state
                self.state.set_last_trigger(profile.id, now)

                # Journal: mark as started
                self.journal.add(ScheduleLogEntry(
                    timestamp=now.isoformat(),
                    profile_id=profile.id,
                    profile_name=profile.name,
                    trigger="in_app",
                    status="started",
                    detail=f"Schedule {profile.schedule.frequency} at {profile.schedule.time}",
                ))

                try:
                    self.backup_callback(profile)
                    # Success will be updated by the backup engine via update_journal_status()
                except Exception as e:
                    logger.error(f"Scheduled backup failed for {profile.name}: {e}")
                    self.journal.update_last(
                        status="failed",
                        detail=f"Error: {e}",
                    )

    def update_journal_status(self, status: str, detail: str = "",
                               files_count: int = 0, duration: float = 0.0):
        """Called by the backup engine to update the journal after completion. Thread-safe."""
        with self._lock:
            self.journal.update_last(
                status=status,
            detail=detail,
            files_count=files_count,
            duration_seconds=duration,
        )

    # ── Core scheduling logic ──
    # Checks if a profile's scheduled time has arrived.
    # Also detects missed backups (e.g., computer was asleep).
    def _is_due(self, profile: BackupProfile, now: datetime) -> bool:
        """Check if a profile's backup is due based on its schedule."""
        sched = profile.schedule

        try:
            scheduled_hour, scheduled_minute = map(int, sched.time.split(":"))
        except (ValueError, AttributeError):
            return False

        # Get last trigger time from persistent state
        last_trigger = self.state.get_last_trigger(profile.id)

        # Calculate the minimum interval to avoid re-triggers
        min_interval = self._get_min_interval(sched.frequency)
        if last_trigger and (now - last_trigger) < min_interval:
            return False

        # Check if we're within the check window of the scheduled time
        scheduled_time_today = now.replace(
            hour=scheduled_hour, minute=scheduled_minute, second=0, microsecond=0
        )
        time_diff = abs((now - scheduled_time_today).total_seconds())

        if time_diff > self.CHECK_INTERVAL:
            return False

        # Check frequency-specific conditions
        match sched.frequency:
            case ScheduleFrequency.HOURLY.value:
                if now.minute == scheduled_minute:
                    return True
            case ScheduleFrequency.DAILY.value:
                return True
            case ScheduleFrequency.WEEKLY.value:
                if now.weekday() == sched.day_of_week:
                    return True
            case ScheduleFrequency.MONTHLY.value:
                if now.day == sched.day_of_month:
                    return True

        return False

    @staticmethod
    def _get_min_interval(frequency: str) -> timedelta:
        """Get the minimum interval between triggers for a frequency."""
        match frequency:
            case ScheduleFrequency.HOURLY.value:
                return timedelta(minutes=50)
            case ScheduleFrequency.DAILY.value:
                return timedelta(hours=22)
            case ScheduleFrequency.WEEKLY.value:
                return timedelta(days=6)
            case ScheduleFrequency.MONTHLY.value:
                return timedelta(days=27)
            case _:
                return timedelta(hours=22)

    def get_next_run_info(self, profile: BackupProfile) -> str:
        """Get a human-readable string of when the next run is expected."""
        sched = profile.schedule
        if not sched.enabled or sched.frequency == ScheduleFrequency.MANUAL.value:
            return "Manual (not scheduled)"

        last = self.state.get_last_trigger(profile.id)
        last_str = last.strftime("%d/%m/%Y %H:%M") if last else "Never"

        freq_labels = {
            ScheduleFrequency.HOURLY.value: f"Every hour at :{sched.time.split(':')[1]}",
            ScheduleFrequency.DAILY.value: f"Every day at {sched.time}",
            ScheduleFrequency.WEEKLY.value: f"Every week at {sched.time}",
            ScheduleFrequency.MONTHLY.value: f"Every month on the {sched.day_of_month} at {sched.time}",
        }
        freq_str = freq_labels.get(sched.frequency, sched.frequency)

        return f"{freq_str}  |  Last: {last_str}"

    def check_missed_backups(self):
        """Check for backups that were missed during sleep/shutdown and run them."""
        profiles = self.config.get_all_profiles()
        now = datetime.now()

        for profile in profiles:
            if not profile.schedule.enabled:
                continue
            if profile.schedule.frequency == ScheduleFrequency.MANUAL.value:
                continue

            last_trigger = self.state.get_last_trigger(profile.id)
            if not last_trigger:
                continue

            min_interval = self._get_min_interval(profile.schedule.frequency)
            elapsed = now - last_trigger

            # If more than 1.5x the expected interval has passed, a backup was missed
            if elapsed > min_interval * 1.5:
                logger.info(
                    f"Missed backup detected for {profile.name}: "
                    f"last was {elapsed.total_seconds()/3600:.1f}h ago"
                )
                self.state.set_last_trigger(profile.id, now)
                self.journal.add(ScheduleLogEntry(
                    timestamp=now.isoformat(),
                    profile_id=profile.id,
                    profile_name=profile.name,
                    trigger="missed_recovery",
                    status="started",
                    detail=f"Missed backup recovery (last: {last_trigger.strftime('%d/%m %H:%M')})",
                ))
                try:
                    self.backup_callback(profile)
                except Exception as e:
                    logger.error(f"Missed backup recovery failed: {e}")
                    self.journal.update_last(status="failed", detail=f"Error: {e}")


# ──────────────────────────────────────────────
#  Auto-Start at Windows Boot
# ──────────────────────────────────────────────
# ══════════════════════════════════════════════
#  Windows auto-start: adds/removes a Registry key
#  HKCU\Software\Microsoft\Windows\CurrentVersion\Run
# ══════════════════════════════════════════════
class AutoStart:
    """
    Manages automatic application startup with Windows.
    Uses the Windows Startup folder (works for .exe and .py).
    """

    APP_NAME = "BackupManager"

    @classmethod
    def _get_startup_folder(cls) -> Path:
        """Get the Windows Startup folder path."""
        startup = Path(os.environ.get("APPDATA", "")) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
        return startup

    @classmethod
    def _get_app_path(cls) -> str:
        """Get the path to the current executable or script."""
        if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
            # Running as PyInstaller bundle
            return sys.executable
        else:
            # Running as Python script
            return f'"{sys.executable}" -m src'

    @classmethod
    def _get_shortcut_path(cls) -> Path:
        """Get the path to the startup shortcut."""
        return cls._get_startup_folder() / f"{cls.APP_NAME}.bat"

    @classmethod
    def is_enabled(cls) -> bool:
        """Check if auto-start is currently enabled."""
        return cls._get_shortcut_path().exists()

    @classmethod
    def enable(cls) -> tuple[bool, str]:
        """Enable auto-start at Windows boot."""
        try:
            shortcut = cls._get_shortcut_path()
            app_path = cls._get_app_path()

            # Create a .bat file in Startup folder
            shortcut.parent.mkdir(parents=True, exist_ok=True)
            with open(shortcut, "w", encoding="ascii", errors="replace") as f:
                f.write(f'@echo off\r\nstart "" {app_path}\r\n')

            logger.info(f"Auto-start enabled: {shortcut}")
            return True, f"✅ Backup Manager will start automatically with Windows."
        except Exception as e:
            logger.error(f"Failed to enable auto-start: {e}")
            return False, f"❌ Failed to enable auto-start: {e}"

    @classmethod
    def disable(cls) -> tuple[bool, str]:
        """Disable auto-start at Windows boot."""
        try:
            shortcut = cls._get_shortcut_path()
            if shortcut.exists():
                shortcut.unlink()
                logger.info("Auto-start disabled.")
            return True, "✅ Auto-start disabled."
        except Exception as e:
            logger.error(f"Failed to disable auto-start: {e}")
            return False, f"❌ Failed to disable auto-start: {e}"

    @classmethod
    def set_enabled(cls, enabled: bool) -> tuple[bool, str]:
        """Enable or disable auto-start."""
        if enabled:
            return cls.enable()
        else:
            return cls.disable()
