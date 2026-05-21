"""Tests for the scheduler's classification of precheck user-timeouts.

Regression guard for the 18/05/2026 incident: a destinations-unreachable
modal opened by ``_scheduled_precheck_prompt`` was never clicked by the
user, the 30-minute timeout fired, the scheduler thread surfaced
``RuntimeError`` from ``_scheduled_backup`` and the journal recorded
``status=failed`` for what is in fact a user-absence event. Two
collateral damages:

1. ``crash_recovery_attempts`` was incremented as if the backup itself
   had crashed — three such timeouts in a row trip the circuit breaker
   and the profile then needs manual intervention even though nothing
   was wrong with the backup pipeline.
2. The retry budget (default 2-10-30-90-240 min) kicked in, queueing
   four more pointless retries that all hit the same modal.

The fix introduces a dedicated ``PrecheckUserTimeoutError`` exception.
The scheduler now treats it like the existing concurrent-run case
(``ProfileLockError``): record ``status=skipped``, do not retry, do
not increment crash recovery. This module pins both contracts.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from src.core.config import BackupProfile, ScheduleConfig, ScheduleFrequency
from src.core.exceptions import PrecheckUserTimeoutError
from src.core.scheduler import InAppScheduler


def _make_scheduler(tmp_path, backup_callback):
    """Build an InAppScheduler wired with a controlled callback."""
    return InAppScheduler(
        tmp_path,
        get_profiles=lambda: [],
        backup_callback=backup_callback,
    )


def _profile_with_retry() -> BackupProfile:
    """Profile whose ``retry_enabled`` is True so we can prove the
    scheduler still does NOT call retry on a user-timeout.
    """
    return BackupProfile(
        name="TestNP",
        schedule=ScheduleConfig(
            enabled=True,
            frequency=ScheduleFrequency.DAILY,
            time="03:00",
            retry_enabled=True,
            retry_delay_minutes=[2, 10],
        ),
    )


class TestPrecheckUserTimeoutClassification:
    """The scheduler must treat ``PrecheckUserTimeoutError`` as a skip,
    not a failure.
    """

    def test_timeout_journal_status_is_skipped(self, tmp_path):
        """A precheck timeout records ``status=skipped`` in the journal."""
        profile = _profile_with_retry()

        def callback_raises_timeout(p):
            raise PrecheckUserTimeoutError(
                profile_name=p.name, timeout_seconds=1800
            )

        scheduler = _make_scheduler(tmp_path, callback_raises_timeout)
        scheduler._trigger_backup(profile, datetime.now(), trigger="in_app")

        entries = scheduler.journal.get_entries()
        # Last entry corresponds to the just-finished attempt.
        assert entries[-1]["status"] == "skipped", (
            f"Expected 'skipped' for precheck timeout, got {entries[-1]['status']}"
        )

    def test_timeout_does_not_trigger_retry(self, tmp_path, monkeypatch):
        """A precheck timeout must not pull on the retry budget."""
        profile = _profile_with_retry()

        def callback_raises_timeout(p):
            raise PrecheckUserTimeoutError(
                profile_name=p.name, timeout_seconds=1800
            )

        scheduler = _make_scheduler(tmp_path, callback_raises_timeout)
        retry_spy = MagicMock()
        monkeypatch.setattr(scheduler, "_retry_backup", retry_spy)

        scheduler._trigger_backup(profile, datetime.now(), trigger="in_app")

        assert retry_spy.call_count == 0, (
            "Precheck user-timeout must bypass the retry budget — "
            f"_retry_backup was called {retry_spy.call_count} time(s)"
        )

    def test_timeout_detail_distinguishes_from_concurrent_skip(
        self, tmp_path
    ) -> None:
        """The journal entry's ``detail`` must mention the timeout cause.

        Both the concurrent-run case (``ProfileLockError``) and the
        user-timeout case record ``status=skipped``, so the only way
        for the History tab to tell them apart is the ``detail`` field.
        """
        profile = _profile_with_retry()

        def callback_raises_timeout(p):
            raise PrecheckUserTimeoutError(
                profile_name=p.name, timeout_seconds=1800
            )

        scheduler = _make_scheduler(tmp_path, callback_raises_timeout)
        scheduler._trigger_backup(profile, datetime.now(), trigger="in_app")

        entries = scheduler.journal.get_entries()
        detail = entries[-1].get("detail", "")
        assert "PrecheckUserTimeoutError" in detail or "timeout" in detail.lower(), (
            f"Expected 'timeout' marker in detail, got: {detail!r}"
        )


class TestPrecheckUserTimeoutErrorShape:
    """The exception itself carries enough context for surface-level UX
    (journal detail, email subject) without needing additional plumbing.
    """

    def test_str_includes_profile_name_and_duration(self):
        exc = PrecheckUserTimeoutError(profile_name="BLoic", timeout_seconds=1800)
        msg = str(exc)
        assert "BLoic" in msg
        assert "1800" in msg or "30" in msg  # seconds or minutes is fine

    def test_pickling_round_trip(self):
        """Exception must survive serialisation (used in cross-thread
        propagation paths and email payloads).
        """
        import pickle

        exc = PrecheckUserTimeoutError(profile_name="BLoic", timeout_seconds=1800)
        restored = pickle.loads(pickle.dumps(exc))
        assert isinstance(restored, PrecheckUserTimeoutError)
        assert restored.profile_name == "BLoic"
        assert restored.timeout_seconds == 1800


@pytest.fixture(autouse=True)
def _silence_scheduler_logging(caplog):
    """The scheduler logs ``logger.exception(...)`` on failure paths,
    which pytest amplifies into a noisy traceback even though the
    behaviour under test is correct. Capture WARNING+ silently.
    """
    import logging

    caplog.set_level(logging.WARNING, logger="src.core.scheduler")
