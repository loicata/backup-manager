"""Regression tests for the 2026-06-10 audit precheck/retry findings.

Covers:
    - R06: ``_retry_backup`` aborts the ladder on skip/cancel-class
      exceptions instead of re-prompting up to 5 times (~9.5 h storm),
      and ``_trigger_backup`` classifies the new
      ``PrecheckUserCancelledError`` as "cancelled" with no retries.
    - R15: ``_enumerate_drive_serials`` retries once on a PowerShell
      timeout instead of faking an unplugged drive.
    - R08/R09/R10: app-level launch/queue state helpers (duck-typed —
      no full Tk app needed).
"""

import pickle
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

from src.core.config import BackupProfile
from src.core.exceptions import PrecheckUserCancelledError, PrecheckUserTimeoutError
from src.core.profile_lock import ProfileLockError
from src.core.scheduler import InAppScheduler
from src.storage import drive_serial
from src.ui.app import BackupManagerApp


def _profile_with_retries(delays: list[int]) -> BackupProfile:
    profile = BackupProfile(name="RetryMe")
    profile.schedule.retry_enabled = True
    profile.schedule.retry_delay_minutes = delays
    return profile


def _scheduler(tmp_path, callback) -> InAppScheduler:
    sched = InAppScheduler(tmp_path, lambda: [], callback)
    sched._running = True  # _retry_backup aborts immediately otherwise
    return sched


class TestRetryLadderClassification:
    def test_user_cancel_aborts_ladder(self, tmp_path):
        calls = []

        def _cb(profile):
            calls.append(profile.name)
            raise PrecheckUserCancelledError(profile_name=profile.name, details="Storage: x")

        sched = _scheduler(tmp_path, _cb)
        profile = _profile_with_retries([0, 0, 0])

        outcome = sched._retry_backup(profile, trigger="in_app")

        assert outcome is True  # definitive — in-flight marker cleared
        assert len(calls) == 1  # ladder abandoned after the first rung
        entries = sched._journal.get_entries()
        assert entries[0]["status"] == "cancelled"
        assert "PrecheckUserCancelledError" in entries[0]["detail"]

    def test_user_timeout_aborts_ladder_as_skipped(self, tmp_path):
        calls = []

        def _cb(profile):
            calls.append(1)
            raise PrecheckUserTimeoutError(profile_name=profile.name, timeout_seconds=1800)

        sched = _scheduler(tmp_path, _cb)
        outcome = sched._retry_backup(_profile_with_retries([0, 0]), trigger="in_app")

        assert outcome is True
        assert len(calls) == 1
        assert sched._journal.get_entries()[0]["status"] == "skipped"

    def test_profile_lock_aborts_ladder_as_skipped(self, tmp_path):
        def _cb(profile):
            raise ProfileLockError("already running")

        sched = _scheduler(tmp_path, _cb)
        outcome = sched._retry_backup(_profile_with_retries([0, 0]), trigger="in_app")

        assert outcome is True
        assert sched._journal.get_entries()[0]["status"] == "skipped"

    def test_real_failures_still_walk_the_full_ladder(self, tmp_path):
        calls = []

        def _cb(profile):
            calls.append(1)
            raise ValueError("disk on fire")

        sched = _scheduler(tmp_path, _cb)
        outcome = sched._retry_backup(_profile_with_retries([0, 0]), trigger="in_app")

        assert outcome is True  # exhausted = definitive
        assert len(calls) == 2  # every rung attempted
        assert sched._journal.get_entries()[0]["status"] == "failed"


class TestTriggerBackupCancelClassification:
    def test_precheck_cancel_journalled_cancelled_no_retry(self, tmp_path):
        from datetime import datetime

        calls = []

        def _cb(profile):
            calls.append(1)
            raise PrecheckUserCancelledError(profile_name=profile.name, details="Storage: down")

        sched = _scheduler(tmp_path, _cb)
        profile = _profile_with_retries([0, 0])

        sched._trigger_backup(profile, now=datetime.now())

        assert len(calls) == 1  # no retry ladder for a user decision
        entries = sched._journal.get_entries()
        assert entries[0]["status"] == "cancelled"
        # In-flight marker cleared — a user cancel is a definitive outcome.
        assert sched._state.get_inflight(profile.id) is None


class TestExceptionPayload:
    def test_message_carries_profile_and_details(self):
        exc = PrecheckUserCancelledError(profile_name="My Backup", details="Storage: timeout")
        assert "My Backup" in str(exc)
        assert "Storage: timeout" in str(exc)

    def test_pickle_round_trip(self):
        exc = PrecheckUserCancelledError(profile_name="P", details="d")
        clone = pickle.loads(pickle.dumps(exc))
        assert clone.profile_name == "P"
        assert clone.details == "d"


class TestDriveSerialEnumRetry:
    def test_retries_once_on_timeout_then_succeeds(self, monkeypatch):
        attempts = []

        def _fake_run(*args, **kwargs):
            attempts.append(1)
            if len(attempts) == 1:
                raise subprocess.TimeoutExpired(cmd="powershell", timeout=10)
            return SimpleNamespace(returncode=0, stdout="G\tSERIAL123\n")

        monkeypatch.setattr(drive_serial.subprocess, "run", _fake_run)
        monkeypatch.setattr(drive_serial.sys, "platform", "win32")

        mapping = drive_serial._enumerate_drive_serials()

        assert len(attempts) == 2
        assert mapping == {"G": "SERIAL123"}

    def test_returns_empty_after_retry_exhausted(self, monkeypatch):
        def _always_timeout(*args, **kwargs):
            raise subprocess.TimeoutExpired(cmd="powershell", timeout=10)

        monkeypatch.setattr(drive_serial.subprocess, "run", _always_timeout)
        monkeypatch.setattr(drive_serial.sys, "platform", "win32")

        assert drive_serial._enumerate_drive_serials() == {}


class TestAppLaunchStateHelpers:
    """Duck-typed unit tests of the app methods — no Tk instance."""

    def test_a_backup_is_active_truth_table(self):
        fake = SimpleNamespace(
            _active_engines={}, _backup_running=False, _launch_in_progress=False
        )
        assert BackupManagerApp._a_backup_is_active(fake) is False

        fake._active_engines = {"pid": object()}
        assert BackupManagerApp._a_backup_is_active(fake) is True

        # The audit scenario: first finisher cleared the boolean while
        # the other run is still registered.
        fake._backup_running = False
        assert BackupManagerApp._a_backup_is_active(fake) is True

        fake._active_engines = {}
        fake._launch_in_progress = True
        assert BackupManagerApp._a_backup_is_active(fake) is True

    def test_remove_profile_from_queue_drops_only_matching(self):
        p1, p2 = BackupProfile(name="A"), BackupProfile(name="B")
        fake = SimpleNamespace(
            _backup_queue=[p1, p2],
            tab_run=SimpleNamespace(_append_log=Mock()),
        )
        BackupManagerApp._remove_profile_from_queue(fake, p1.id)
        assert [p.name for p in fake._backup_queue] == ["B"]
        fake.tab_run._append_log.assert_called_once()

    def test_dequeue_skips_while_launch_in_progress(self):
        # Only the guard attribute exists: reaching any later statement
        # would raise AttributeError and fail the test.
        fake = SimpleNamespace(_launch_in_progress=True)
        assert BackupManagerApp._dequeue_next_backup(fake, False, "X") is None

    def test_on_precheck_cancel_releases_exact_slot_and_clears_queue(self):
        queued = [BackupProfile(name="Q1"), BackupProfile(name="Q2")]
        fake = SimpleNamespace(
            _hide_target_alert=Mock(),
            tray=SimpleNamespace(set_state=Mock()),
            _launch_in_progress=True,
            _backup_queue=list(queued),
            tab_run=SimpleNamespace(_append_log=Mock()),
            scheduler=SimpleNamespace(unmark_profile_running=Mock()),
            _launching_profile_id="other-profile",
        )

        BackupManagerApp._on_precheck_cancel(fake, "the-real-launch")

        # Releases the id bound into the closure — NOT the overwritten slot.
        fake.scheduler.unmark_profile_running.assert_called_once_with("the-real-launch")
        # The stale single-slot field is left for its own launch to manage.
        assert fake._launching_profile_id == "other-profile"
        assert fake._backup_queue == []
        assert fake._launch_in_progress is False

    def test_log_precheck_failures_emits_details_at_warning(self, caplog):
        failures = [
            ("Storage", "Write test", False, "Drive not ready after wake-up retries"),
            ("Mirror 1", "Connect", False, ""),
        ]
        with caplog.at_level("WARNING", logger="src.ui.app"):
            BackupManagerApp._log_precheck_failures("MyProf", failures, "unit test")

        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert "MyProf" in joined
        assert "Drive not ready after wake-up retries" in joined
        assert "Mirror 1" in joined

    def test_stop_aware_wait_returns_immediately_when_stopping(self):
        fake = SimpleNamespace(scheduler=SimpleNamespace(is_stopping=lambda: True))
        # Returns True (stopping) without sleeping the full duration.
        assert BackupManagerApp._stop_aware_precheck_wait(fake, 30.0) is True

    def test_stop_aware_wait_completes_when_not_stopping(self):
        fake = SimpleNamespace(scheduler=SimpleNamespace(is_stopping=lambda: False))
        assert BackupManagerApp._stop_aware_precheck_wait(fake, 0.0) is False

    def test_on_precheck_cancel_falls_back_to_slot_field(self):
        fake = SimpleNamespace(
            _hide_target_alert=Mock(),
            tray=SimpleNamespace(set_state=Mock()),
            _launch_in_progress=True,
            _backup_queue=[],
            tab_run=SimpleNamespace(_append_log=Mock()),
            scheduler=SimpleNamespace(unmark_profile_running=Mock()),
            _launching_profile_id="legacy-slot",
        )

        BackupManagerApp._on_precheck_cancel(fake)

        fake.scheduler.unmark_profile_running.assert_called_once_with("legacy-slot")
        assert fake._launching_profile_id is None
