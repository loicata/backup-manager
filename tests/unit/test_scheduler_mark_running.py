"""Tests for the public mark/unmark API on ``InAppScheduler``.

Regression: when the user clicked "Start backup" in the Run tab, the
backup ran on the engine but the scheduler had no idea it was in
flight. The next periodic ``_check_schedules`` tick happily re-triggered
the same profile, the engine rejected the duplicate via its
``ProfileLockError`` safety net, and the user saw a confusing
"Backup rejected: Another backup is already running" line in the
Run-tab log.

Fix: expose ``mark_profile_running`` / ``unmark_profile_running`` on
the scheduler. The UI calls ``mark`` before launching the backup
thread and ``unmark`` in the thread's ``finally`` block. The
scheduler's existing ``_check_schedules`` guard then sees the profile
in ``_profile_in_progress`` and skips it.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.core.config import (
    BackupProfile,
    ScheduleConfig,
    ScheduleFrequency,
    StorageConfig,
    StorageType,
)
from src.core.scheduler import InAppScheduler


def _make_profile(profile_id: str = "p1") -> BackupProfile:
    """Build a minimal valid BackupProfile that the scheduler will pick up."""
    return BackupProfile(
        id=profile_id,
        name="TestProfile",
        active=True,
        source_paths=["C:\\noop"],
        storage=StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path="C:\\noop_dest",
        ),
        schedule=ScheduleConfig(
            enabled=True,
            frequency=ScheduleFrequency.DAILY,
            time="10:00",
        ),
    )


@pytest.fixture()
def scheduler(tmp_path: Path):
    """Build an InAppScheduler that never actually runs the backup callback."""
    profiles: list[BackupProfile] = []
    callback = MagicMock()
    sched = InAppScheduler(
        config_dir=tmp_path,
        get_profiles=lambda: profiles,
        backup_callback=callback,
    )
    sched._test_profiles = profiles  # convenience handle
    sched._test_callback = callback
    yield sched
    # Defensive teardown — stop the daemon thread if a test forgot.
    if sched._running:
        sched.stop()


# ---------------------------------------------------------------------------
# Public mark/unmark contract
# ---------------------------------------------------------------------------


class TestMarkUnmarkContract:
    """Pure-state tests on the new public methods."""

    def test_mark_adds_to_in_progress_set(self, scheduler) -> None:
        scheduler.mark_profile_running("abc")
        # Internal set is exposed for tests by name (single underscore).
        assert "abc" in scheduler._profile_in_progress

    def test_unmark_removes_from_set(self, scheduler) -> None:
        scheduler.mark_profile_running("abc")
        scheduler.unmark_profile_running("abc")
        assert "abc" not in scheduler._profile_in_progress

    def test_mark_is_idempotent(self, scheduler) -> None:
        """A double-mark must NOT crash and must keep a single entry."""
        scheduler.mark_profile_running("abc")
        scheduler.mark_profile_running("abc")
        # set semantics — single occurrence regardless of mark count
        assert sum(1 for x in scheduler._profile_in_progress if x == "abc") == 1

    def test_unmark_unknown_id_is_noop(self, scheduler) -> None:
        """The UI's ``finally`` block calls unmark unconditionally;
        an unknown id must not raise."""
        scheduler.unmark_profile_running("never_marked")  # must not raise

    def test_mark_unmark_thread_safe(self, scheduler) -> None:
        """Concurrent mark/unmark from many threads must not corrupt
        the set or raise ``RuntimeError: set changed size during
        iteration``. We assert the set is empty at the end (every mark
        had a matching unmark)."""
        n = 200

        def worker(i):
            scheduler.mark_profile_running(f"p{i}")
            scheduler.unmark_profile_running(f"p{i}")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert scheduler._profile_in_progress == set()


# ---------------------------------------------------------------------------
# Integration with _check_schedules
# ---------------------------------------------------------------------------


class TestSchedulerSkipsMarkedProfile:
    """Once marked, a profile must NOT be triggered by ``_check_schedules``."""

    def test_marked_profile_is_skipped_by_check_schedules(self, scheduler) -> None:
        profile = _make_profile()
        scheduler._test_profiles.append(profile)

        # Force the profile to look "due" so the only thing that could
        # save us is the in-progress guard.
        scheduler._state.set_last_trigger(
            profile.id, datetime.now() - timedelta(days=10)
        )

        scheduler.mark_profile_running(profile.id)
        scheduler._check_schedules()

        # Callback must NOT have fired — the guard worked.
        scheduler._test_callback.assert_not_called()

    def test_unmarked_profile_can_be_triggered(self, scheduler) -> None:
        """Sanity check: after unmark, the same profile fires normally."""
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        scheduler._state.set_last_trigger(
            profile.id, datetime.now() - timedelta(days=10)
        )

        scheduler.mark_profile_running(profile.id)
        scheduler.unmark_profile_running(profile.id)
        scheduler._check_schedules()

        scheduler._test_callback.assert_called_once_with(profile)

    def test_marked_profile_is_skipped_by_missed_backups(self, scheduler) -> None:
        """The wake-from-sleep path uses the same guard."""
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        scheduler._state.set_last_trigger(
            profile.id, datetime.now() - timedelta(days=10)
        )

        scheduler.mark_profile_running(profile.id)
        scheduler._check_missed_backups(datetime.now())

        scheduler._test_callback.assert_not_called()

    def test_marked_profile_is_skipped_by_startup_missed(self, scheduler) -> None:
        """The cold-boot recovery path uses the same guard."""
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        scheduler._state.set_last_trigger(
            profile.id, datetime.now() - timedelta(days=10)
        )

        scheduler.mark_profile_running(profile.id)
        scheduler._check_startup_missed()

        scheduler._test_callback.assert_not_called()


# ---------------------------------------------------------------------------
# Integration with mark_triggered_now (post-backup re-fire prevention)
# ---------------------------------------------------------------------------


class TestPostBackupNoRefire:
    """After a manual UI backup ends, the scheduler must NOT immediately
    fire a second backup.

    Background: ``mark_profile_running`` only silences the duplicate
    WHILE the backup runs.  If the UI doesn't ALSO update
    ``last_trigger`` (via ``mark_triggered_now``), the next periodic
    check sees a stale timestamp, decides the profile is overdue, and
    fires a second full backup the user never asked for.  These tests
    simulate the full UI lifecycle (mark + mark_triggered_now → run →
    unmark) and assert the scheduler stays quiet afterwards.
    """

    def test_no_refire_after_unmark_when_triggered_now_was_called(
        self, scheduler
    ) -> None:
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        scheduler._state.set_last_trigger(
            profile.id, datetime.now() - timedelta(days=10)
        )

        # Simulate the full UI flow:
        #   1. user clicks Start backup → mark + mark_triggered_now
        #   2. backup runs (we don't actually run it)
        #   3. backup finishes → unmark
        scheduler.mark_profile_running(profile.id)
        scheduler.mark_triggered_now(profile.id)
        scheduler.unmark_profile_running(profile.id)

        # Next scheduler tick: must NOT fire because last_trigger is
        # now today — _is_due returns False for daily.
        scheduler._check_schedules()
        scheduler._test_callback.assert_not_called()

    def test_refire_DOES_happen_without_triggered_now_call(
        self, scheduler
    ) -> None:
        """Regression contract: removing ``mark_triggered_now`` from the
        UI flow must produce the bug — proves this test would catch
        a future revert.
        """
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        scheduler._state.set_last_trigger(
            profile.id, datetime.now() - timedelta(days=10)
        )

        # NOTE: we deliberately omit ``mark_triggered_now`` to simulate
        # the pre-fix behaviour. After unmark the scheduler should
        # fire — that's exactly the unwanted behaviour the fix prevents.
        scheduler.mark_profile_running(profile.id)
        scheduler.unmark_profile_running(profile.id)

        scheduler._check_schedules()
        # Without the timestamp bump, _is_due returns True and the
        # callback fires — proving the integration test would catch
        # a regression of the fix.
        scheduler._test_callback.assert_called_once()
