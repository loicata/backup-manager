"""Tests for scheduler retry logic."""

import time
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.core.config import BackupProfile, ScheduleConfig, ScheduleFrequency
from src.core.scheduler import InAppScheduler, ScheduleLogEntry


@pytest.fixture
def scheduler_env(tmp_path):
    """Create a scheduler with mocked dependencies."""
    profiles = []
    callback = MagicMock()

    scheduler = InAppScheduler(
        config_dir=tmp_path,
        get_profiles=lambda: profiles,
        backup_callback=callback,
    )
    # Keep it running so retry sleeps don't abort
    scheduler._running = True

    return {
        "scheduler": scheduler,
        "callback": callback,
        "profiles": profiles,
        "tmp_path": tmp_path,
    }


class TestTriggerBackupSuccess:
    """Test that successful backups don't trigger retry."""

    def test_success_logs_to_journal(self, scheduler_env):
        """Successful backup logs 'success' status."""
        s = scheduler_env["scheduler"]
        profile = BackupProfile(name="Test")
        now = datetime.now()

        s._trigger_backup(profile, now)

        entries = s.journal.get_entries()
        assert len(entries) == 1
        assert entries[0]["status"] == "success"

    def test_success_does_not_retry(self, scheduler_env):
        """Successful backup should not trigger any retries."""
        s = scheduler_env["scheduler"]
        callback = scheduler_env["callback"]
        profile = BackupProfile(
            name="Test",
            schedule=ScheduleConfig(retry_enabled=True),
        )
        now = datetime.now()

        s._trigger_backup(profile, now)

        # Callback called exactly once (no retries)
        assert callback.call_count == 1


class TestTriggerBackupFailureNoRetry:
    """Test failure behavior when retry is disabled."""

    def test_failure_without_retry_logs_failed(self, scheduler_env):
        """Failed backup with retry_enabled=False logs 'failed'."""
        s = scheduler_env["scheduler"]
        callback = scheduler_env["callback"]
        callback.side_effect = RuntimeError("disk full")

        profile = BackupProfile(
            name="NoRetry",
            schedule=ScheduleConfig(retry_enabled=False),
        )
        now = datetime.now()

        s._trigger_backup(profile, now)

        entries = s.journal.get_entries()
        assert entries[-1]["status"] == "failed"
        assert "disk full" in entries[-1]["detail"]

    def test_failure_without_retry_calls_once(self, scheduler_env):
        """Failed backup with retry disabled calls callback only once."""
        s = scheduler_env["scheduler"]
        callback = scheduler_env["callback"]
        callback.side_effect = RuntimeError("fail")

        profile = BackupProfile(
            name="NoRetry",
            schedule=ScheduleConfig(retry_enabled=False),
        )

        s._trigger_backup(profile, datetime.now())
        assert callback.call_count == 1


class TestRetryBackup:
    """Test the retry mechanism directly."""

    @patch("src.core.scheduler.time.sleep")
    def test_retry_succeeds_on_second_attempt(self, mock_sleep, scheduler_env):
        """Retry should stop after first successful attempt."""
        s = scheduler_env["scheduler"]
        callback = scheduler_env["callback"]
        # First call fails, second succeeds
        callback.side_effect = [RuntimeError("fail"), None]

        profile = BackupProfile(
            name="RetryOK",
            schedule=ScheduleConfig(
                retry_enabled=True,
                retry_delay_minutes=[1],  # Only 1 retry
            ),
        )

        s._trigger_backup(profile, datetime.now())

        # 1 initial + 1 retry = 2 calls
        assert callback.call_count == 2

        # Last journal entry should be success
        entries = s.journal.get_entries()
        assert entries[-1]["status"] == "success"

    @patch("src.core.scheduler.time.sleep")
    def test_retry_exhausts_all_delays(self, mock_sleep, scheduler_env):
        """When all retries fail, all delays are used."""
        s = scheduler_env["scheduler"]
        callback = scheduler_env["callback"]
        callback.side_effect = RuntimeError("always fails")

        profile = BackupProfile(
            name="AllFail",
            schedule=ScheduleConfig(
                retry_enabled=True,
                retry_delay_minutes=[1, 2, 3],
            ),
        )

        s._trigger_backup(profile, datetime.now())

        # 1 initial + 3 retries = 4 calls
        assert callback.call_count == 4

        # Last entry should be failed
        entries = s.journal.get_entries()
        assert entries[-1]["status"] == "failed"

    @patch("src.core.scheduler.time.sleep")
    def test_retry_logs_each_attempt(self, mock_sleep, scheduler_env):
        """Each retry attempt should be logged in the journal."""
        s = scheduler_env["scheduler"]
        callback = scheduler_env["callback"]
        callback.side_effect = RuntimeError("fail")

        profile = BackupProfile(
            name="Logged",
            schedule=ScheduleConfig(
                retry_enabled=True,
                retry_delay_minutes=[1, 2],
            ),
        )

        s._trigger_backup(profile, datetime.now())

        entries = s.journal.get_entries()
        # 1 initial (started→failed) + 2 retries (waiting→started→failed each)
        triggers = [e.get("trigger") for e in entries]
        assert "in_app" in triggers
        assert "retry_1" in triggers
        assert "retry_2" in triggers

    @patch("src.core.scheduler.time.sleep")
    def test_retry_stops_when_scheduler_stopped(self, mock_sleep, scheduler_env):
        """Retry should abort if scheduler is stopped during wait."""
        s = scheduler_env["scheduler"]
        callback = scheduler_env["callback"]
        callback.side_effect = RuntimeError("fail")

        # Stop scheduler after first sleep
        def stop_on_sleep(*args):
            s._running = False

        mock_sleep.side_effect = stop_on_sleep

        profile = BackupProfile(
            name="Stopped",
            schedule=ScheduleConfig(
                retry_enabled=True,
                retry_delay_minutes=[1, 2, 3],
            ),
        )

        s._trigger_backup(profile, datetime.now())

        # Only initial call, retry aborted during sleep
        assert callback.call_count == 1

    @patch("src.core.scheduler.time.sleep")
    def test_retry_with_default_delays(self, mock_sleep, scheduler_env):
        """Default retry delays are 2, 10, 30, 90, 240 minutes."""
        s = scheduler_env["scheduler"]
        callback = scheduler_env["callback"]
        # Fails initially, succeeds on 3rd retry
        callback.side_effect = [
            RuntimeError("fail"),  # initial
            RuntimeError("fail"),  # retry 1
            RuntimeError("fail"),  # retry 2
            None,                  # retry 3 succeeds
        ]

        profile = BackupProfile(
            name="Defaults",
            schedule=ScheduleConfig(retry_enabled=True),  # uses defaults
        )

        s._trigger_backup(profile, datetime.now())

        assert callback.call_count == 4
        entries = s.journal.get_entries()
        assert entries[-1]["status"] == "success"

    @patch("src.core.scheduler.time.sleep")
    def test_retry_empty_delays_does_not_retry(self, mock_sleep, scheduler_env):
        """Empty retry_delay_minutes means no retries even if enabled."""
        s = scheduler_env["scheduler"]
        callback = scheduler_env["callback"]
        callback.side_effect = RuntimeError("fail")

        profile = BackupProfile(
            name="Empty",
            schedule=ScheduleConfig(
                retry_enabled=True,
                retry_delay_minutes=[],
            ),
        )

        s._trigger_backup(profile, datetime.now())

        assert callback.call_count == 1
