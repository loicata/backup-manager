"""Advanced scheduler tests — covers _check_schedules, _trigger_backup,
weekly/monthly is_due, missed backup detection, AutoStart, and journal trimming.
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.core.config import BackupProfile, ScheduleConfig, ScheduleFrequency
from src.core.scheduler import (
    AutoStart,
    InAppScheduler,
    ScheduleJournal,
    ScheduleLogEntry,
    SchedulerState,
    CHECK_INTERVAL,
    MAX_JOURNAL_ENTRIES,
)


class TestIsDueWeekly:
    """Test weekly schedule detection."""

    def test_due_on_correct_day(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        now = datetime.now()
        profile = BackupProfile(
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.WEEKLY,
                time="00:00",
                day_of_week=now.weekday(),
            )
        )
        # Set last trigger 2 days ago
        scheduler._state.set_last_trigger(profile.id, now - timedelta(days=2))
        assert scheduler._is_due(profile, now) is True

    def test_not_due_on_wrong_day(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        now = datetime.now()
        wrong_day = (now.weekday() + 3) % 7
        profile = BackupProfile(
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.WEEKLY,
                time="00:00",
                day_of_week=wrong_day,
            )
        )
        # Set a last trigger so it doesn't return True for "first run"
        scheduler._state.set_last_trigger(profile.id, now - timedelta(hours=1))
        assert scheduler._is_due(profile, now) is False


class TestIsDueMonthly:
    """Test monthly schedule detection."""

    def test_due_on_correct_day(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        now = datetime.now()
        profile = BackupProfile(
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.MONTHLY,
                time="00:00",
                day_of_month=now.day,
            )
        )
        scheduler._state.set_last_trigger(profile.id, now - timedelta(days=2))
        assert scheduler._is_due(profile, now) is True

    def test_not_due_on_wrong_day(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        now = datetime.now()
        wrong_day = (now.day % 28) + 1
        if wrong_day == now.day:
            wrong_day = (wrong_day % 28) + 1
        profile = BackupProfile(
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.MONTHLY,
                time="00:00",
                day_of_month=wrong_day,
            )
        )
        # Set a last trigger so it doesn't return True for "first run"
        scheduler._state.set_last_trigger(profile.id, now - timedelta(hours=1))
        assert scheduler._is_due(profile, now) is False


class TestIsDueEdgeCases:
    """Test edge cases for _is_due."""

    def test_invalid_time_format_uses_default(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        now = datetime.now()
        profile = BackupProfile(
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.DAILY,
                time="invalid",
            )
        )
        # Should not crash — uses default 02:00
        result = scheduler._is_due(profile, now)
        assert isinstance(result, bool)

    def test_hourly_not_due_if_recent(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        now = datetime.now()
        profile = BackupProfile(
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.HOURLY,
            )
        )
        scheduler._state.set_last_trigger(profile.id, now - timedelta(minutes=30))
        assert scheduler._is_due(profile, now) is False


class TestTriggerBackup:
    """Test _trigger_backup."""

    def test_trigger_calls_callback(self, tmp_path):
        triggered = []
        scheduler = InAppScheduler(
            tmp_path,
            lambda: [],
            lambda p: triggered.append(p.name),
        )
        profile = BackupProfile(name="TestTrigger")
        scheduler._trigger_backup(profile, datetime.now())
        assert "TestTrigger" in triggered

    def test_trigger_logs_to_journal(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        profile = BackupProfile(id="trig_id", name="Trig")
        scheduler._trigger_backup(profile, datetime.now())

        last = scheduler.journal.get_last_run("trig_id")
        assert last is not None
        assert last["status"] == "success"
        assert last["trigger"] == "in_app"

    def test_trigger_sets_state(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        profile = BackupProfile(id="state_id")
        now = datetime.now()
        scheduler._trigger_backup(profile, now)

        last = scheduler._state.get_last_trigger("state_id")
        assert last is not None

    def test_trigger_handles_callback_exception(self, tmp_path):
        """Callback exception should not crash the scheduler."""

        def failing_callback(p):
            raise RuntimeError("Backup exploded")

        scheduler = InAppScheduler(tmp_path, lambda: [], failing_callback)
        profile = BackupProfile(name="Boom")
        # Should not raise
        scheduler._trigger_backup(profile, datetime.now())


class TestCheckSchedules:
    """Test _check_schedules."""

    def test_skips_disabled_profiles(self, tmp_path):
        triggered = []
        profile = BackupProfile(
            schedule=ScheduleConfig(enabled=False, frequency=ScheduleFrequency.DAILY)
        )
        scheduler = InAppScheduler(
            tmp_path,
            lambda: [profile],
            lambda p: triggered.append(p),
        )
        scheduler._check_schedules()
        assert len(triggered) == 0

    def test_skips_manual_profiles(self, tmp_path):
        triggered = []
        profile = BackupProfile(
            schedule=ScheduleConfig(enabled=True, frequency=ScheduleFrequency.MANUAL)
        )
        scheduler = InAppScheduler(
            tmp_path,
            lambda: [profile],
            lambda p: triggered.append(p),
        )
        scheduler._check_schedules()
        assert len(triggered) == 0

    def test_triggers_due_profile(self, tmp_path):
        triggered = []
        profile = BackupProfile(
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.DAILY,
                time="00:00",
            )
        )
        scheduler = InAppScheduler(
            tmp_path,
            lambda: [profile],
            lambda p: triggered.append(p.name),
        )
        scheduler._check_schedules()
        assert len(triggered) == 1


class TestCheckMissedBackups:
    """Test missed backup detection after system sleep."""

    def test_detects_sleep_gap(self, tmp_path):
        triggered = []
        profile = BackupProfile(
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.DAILY,
                time="00:00",
            )
        )
        scheduler = InAppScheduler(
            tmp_path,
            lambda: [profile],
            lambda p: triggered.append(p.name),
        )
        # Simulate a sleep gap by setting last check time far in the past
        scheduler._last_check_time = time.monotonic() - CHECK_INTERVAL * 5
        scheduler._check_schedules()
        # Should detect sleep and check missed + regular → trigger at least once
        assert len(triggered) >= 1


class TestJournalTrimming:
    """Test journal stays within MAX_JOURNAL_ENTRIES."""

    def test_trims_old_entries(self, tmp_path):
        journal = ScheduleJournal(tmp_path)
        for i in range(MAX_JOURNAL_ENTRIES + 50):
            journal.add(
                ScheduleLogEntry(
                    profile_id="trim",
                    status="success",
                    detail=str(i),
                )
            )
        entries = journal.get_entries(limit=10000)
        assert len(entries) <= MAX_JOURNAL_ENTRIES


class TestSchedulerStateEdgeCases:
    """Test SchedulerState edge cases."""

    def test_corrupt_state_file(self, tmp_path):
        """Corrupt state file should not crash."""
        state_path = tmp_path / "scheduler_state.json"
        state_path.write_text("not json!", encoding="utf-8")
        state = SchedulerState(tmp_path)
        assert state.get_last_trigger("any") is None

    def test_invalid_timestamp_returns_none(self, tmp_path):
        """Invalid ISO timestamp should return None."""
        state_path = tmp_path / "scheduler_state.json"
        state_path.write_text(json.dumps({"p": "not-a-date"}), encoding="utf-8")
        state = SchedulerState(tmp_path)
        assert state.get_last_trigger("p") is None


class TestJournalEdgeCases:
    """Test ScheduleJournal edge cases."""

    def test_corrupt_journal_file(self, tmp_path):
        """Corrupt journal file should not crash."""
        journal_path = tmp_path / "schedule_journal.json"
        journal_path.write_text("corrupted!", encoding="utf-8")
        journal = ScheduleJournal(tmp_path)
        assert journal.get_entries() == []


class TestGetNextRunInfo:
    """Test get_next_run_info for all frequencies."""

    def test_hourly(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        profile = BackupProfile(
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.HOURLY,
            )
        )
        assert scheduler.get_next_run_info(profile) == "Every hour"

    def test_weekly(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        profile = BackupProfile(
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.WEEKLY,
                time="14:30",
            )
        )
        info = scheduler.get_next_run_info(profile)
        assert "Weekly" in info
        assert "14:30" in info

    def test_monthly(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        profile = BackupProfile(
            schedule=ScheduleConfig(
                enabled=True,
                frequency=ScheduleFrequency.MONTHLY,
                time="03:00",
            )
        )
        info = scheduler.get_next_run_info(profile)
        assert "Monthly" in info
        assert "03:00" in info

    def test_disabled_returns_manual(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        profile = BackupProfile(
            schedule=ScheduleConfig(
                enabled=False,
                frequency=ScheduleFrequency.DAILY,
            )
        )
        assert scheduler.get_next_run_info(profile) == "Manual"
