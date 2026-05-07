"""Tests for the wake-from-sleep gap detection in InAppScheduler.

Regression: ``_check_schedules`` used to refresh ``_last_check_time``
*before* the for-loop that calls ``_trigger_backup``. Because the
backup callback runs synchronously in the scheduler thread and can
take tens of minutes, the next iteration saw an apparent gap equal
to the backup duration and emitted a misleading ``Detected system
wake from sleep (1350s gap)`` line right after every long backup.

Fix: refresh ``_last_check_time`` at the END of ``_check_schedules``
(and once after ``_check_startup_missed`` in ``_run``), so the
elapsed window only spans the actual idle wait between ticks.
"""

from __future__ import annotations

import logging
import types
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
from src.core.scheduler import CHECK_INTERVAL, InAppScheduler


def _make_profile(profile_id: str = "p1") -> BackupProfile:
    """Build an HOURLY profile so ``_is_due`` is independent of the
    wall-clock hour at test execution time. ``DAILY`` would gate on
    ``now >= target_today`` which makes the test pass-or-fail
    depending on the hour the suite happens to run.
    """
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
            frequency=ScheduleFrequency.HOURLY,
            time="10:00",
            verify_enabled=False,
        ),
    )


@pytest.fixture()
def scheduler(tmp_path: Path):
    profiles: list[BackupProfile] = []
    callback = MagicMock()
    sched = InAppScheduler(
        config_dir=tmp_path,
        get_profiles=lambda: profiles,
        backup_callback=callback,
    )
    sched._test_profiles = profiles
    sched._test_callback = callback
    yield sched
    if sched._running:
        sched.stop()


class _FakeMonotonic:
    """Manually-advanced monotonic clock for time-jump simulation.

    A patched ``time.monotonic`` lets the tests pretend a long backup
    elapsed without the test process actually sleeping.
    """

    def __init__(self, start: float = 1000.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.fixture()
def fake_monotonic(monkeypatch):
    """Replace scheduler's view of ``time`` with a controllable clock.

    Patches the *module-level* ``time`` reference inside scheduler.py
    rather than ``time.monotonic`` directly. The latter would mutate
    the global ``time`` module and bleed into pytest internals or
    other concurrent tests; namespacing the patch keeps the fake
    visible only to scheduler code.
    """
    clock = _FakeMonotonic()
    fake_time = types.SimpleNamespace(monotonic=clock)
    monkeypatch.setattr("src.core.scheduler.time", fake_time)
    return clock


class TestNoFalseWakeAfterLongBackup:
    """The cosmetic regression we are fixing."""

    def test_no_false_wake_log_after_long_synchronous_backup(
        self,
        scheduler,
        fake_monotonic,
        caplog,
    ) -> None:
        """A 22-minute backup must not trip the wake-from-sleep log on
        the next ``_check_schedules`` tick.
        """
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        scheduler._state.set_last_trigger(
            profile.id, datetime.now() - timedelta(days=10)
        )
        # Align the scheduler's reference timestamp to our fake clock.
        scheduler._last_check_time = fake_monotonic.value

        # The "backup" simulates 22 minutes of synchronous work by
        # advancing the fake clock from inside the callback.
        long_backup_seconds = 22 * 60

        def slow_backup(_p):
            fake_monotonic.advance(long_backup_seconds)

        scheduler._test_callback.side_effect = slow_backup

        # First tick: should fire and run the long callback.
        scheduler._check_schedules()
        assert scheduler._test_callback.call_count == 1

        # Second tick: simulate the normal 30s wait between iterations
        # and check that no spurious "wake from sleep" log appears.
        fake_monotonic.advance(CHECK_INTERVAL)

        with caplog.at_level(logging.INFO, logger="src.core.scheduler"):
            scheduler._check_schedules()

        assert "Detected system wake from sleep" not in caplog.text

    def test_real_sleep_gap_still_detected(
        self,
        scheduler,
        fake_monotonic,
        caplog,
    ) -> None:
        """A genuine OS sleep (gap > 3x CHECK_INTERVAL with no backup
        running) must still emit the wake log — we must not silence
        the real signal while killing the false one.
        """
        scheduler._last_check_time = fake_monotonic.value

        # Simulate real OS suspend: clock jumps without any callback
        # execution between ticks.
        fake_monotonic.advance(CHECK_INTERVAL * 5)

        with caplog.at_level(logging.INFO, logger="src.core.scheduler"):
            scheduler._check_schedules()

        assert "Detected system wake from sleep" in caplog.text

    def test_last_check_time_refreshed_after_callback(
        self,
        scheduler,
        fake_monotonic,
    ) -> None:
        """Direct contract on the fix: after a long synchronous
        callback, ``_last_check_time`` must reflect the post-callback
        clock value, not the pre-callback one.
        """
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        scheduler._state.set_last_trigger(
            profile.id, datetime.now() - timedelta(days=10)
        )
        scheduler._last_check_time = fake_monotonic.value

        def slow_backup(_p):
            fake_monotonic.advance(22 * 60)

        scheduler._test_callback.side_effect = slow_backup
        scheduler._check_schedules()

        # The post-loop refresh must have happened — _last_check_time
        # is now the post-callback clock value, so the next tick's
        # elapsed measurement starts fresh.
        assert scheduler._last_check_time == fake_monotonic.value
