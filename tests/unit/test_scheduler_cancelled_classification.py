"""Tests for the scheduler's classification of user-cancelled backups.

Regression guard for the 14/05/2026 incident: a scheduled backup the
user cancelled mid-run was journalled ``status=success``. ``_scheduled_backup``
caught ``CancelledError``, did not update the journal, and did NOT
re-raise — so ``_trigger_backup`` fell through to its success path and
wrote "Scheduled backup succeeded" right after "Backup cancelled by user".

The fix: ``_scheduled_backup`` re-raises the cancel, and ``_trigger_backup``
classifies ``CancelledError`` as a skip-class outcome — journal status
``cancelled`` (a terminal status, so the dashboard shows it correctly),
no crash-recovery bump, and no retry storm. This module pins both.
"""

from __future__ import annotations

import logging
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from src.core.config import BackupProfile, ScheduleConfig, ScheduleFrequency
from src.core.exceptions import CancelledError
from src.core.scheduler import InAppScheduler


def _make_scheduler(tmp_path, backup_callback):
    return InAppScheduler(
        tmp_path,
        get_profiles=lambda: [],
        backup_callback=backup_callback,
    )


def _profile_with_retry() -> BackupProfile:
    return BackupProfile(
        name="My Backup",
        schedule=ScheduleConfig(
            enabled=True,
            frequency=ScheduleFrequency.DAILY,
            time="03:00",
            retry_enabled=True,
            retry_delay_minutes=[2, 10],
        ),
    )


class TestCancelledClassification:
    def test_cancel_journal_status_is_cancelled_not_success(self, tmp_path):
        profile = _profile_with_retry()

        def callback_raises_cancel(p):
            raise CancelledError("Backup cancelled by user")

        scheduler = _make_scheduler(tmp_path, callback_raises_cancel)
        scheduler._trigger_backup(profile, datetime.now(), trigger="in_app")

        entries = scheduler.journal.get_entries()
        assert entries[-1]["status"] == "cancelled", (
            f"Expected 'cancelled' for a user cancel, got {entries[-1]['status']}"
        )

    def test_cancel_does_not_trigger_retry(self, tmp_path, monkeypatch):
        profile = _profile_with_retry()

        def callback_raises_cancel(p):
            raise CancelledError("Backup cancelled by user")

        scheduler = _make_scheduler(tmp_path, callback_raises_cancel)
        retry_spy = MagicMock()
        monkeypatch.setattr(scheduler, "_retry_backup", retry_spy)

        scheduler._trigger_backup(profile, datetime.now(), trigger="in_app")

        assert retry_spy.call_count == 0, (
            "A user cancel must bypass the retry budget — "
            f"_retry_backup was called {retry_spy.call_count} time(s)"
        )

    def test_genuine_failure_still_fails_and_retries(self, tmp_path, monkeypatch):
        # Contrast: a real error is still 'failed' and still retries — the
        # cancel handling must not have swallowed genuine failures.
        profile = _profile_with_retry()

        def callback_raises_error(p):
            raise RuntimeError("disk exploded")

        scheduler = _make_scheduler(tmp_path, callback_raises_error)
        retry_spy = MagicMock()
        monkeypatch.setattr(scheduler, "_retry_backup", retry_spy)

        scheduler._trigger_backup(profile, datetime.now(), trigger="in_app")

        entries = scheduler.journal.get_entries()
        assert entries[-1]["status"] == "failed"
        assert retry_spy.call_count == 1


@pytest.fixture(autouse=True)
def _silence_scheduler_logging(caplog):
    caplog.set_level(logging.WARNING, logger="src.core.scheduler")
