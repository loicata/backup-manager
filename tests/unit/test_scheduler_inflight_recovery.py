"""Tests for in-flight trigger recovery in ``InAppScheduler``.

Regression (02/06/2026 incident): three profiles shared an identical
``daily at 10:00`` schedule. At 13:05 the app launched and the startup
catch-up triggered the FIRST profile in the list ('crypter'). The
scheduler advanced its ``last_trigger`` to 13:05 *before* running the
backup (so the daily slot was marked consumed), then the PC was powered
off before the engine ran a single line — so ``last_backup_completed``
was never flipped to False. At 18:26 the app relaunched: 'crypter' was
no longer ``_is_due`` (last_trigger 13:05 >= today's 10:00 slot) and had
no crash-recovery flag armed, so it was silently skipped while the two
other profiles (whose ``last_trigger`` was still yesterday) ran fine.

Root cause: the schedule slot is consumed too early (before the run) and
the only safety net (``last_backup_completed=False``) is armed too late
(by the engine, after its first save). A process death in that gap burns
the slot with no recovery signal.

Fix: ``_trigger_backup`` writes a persistent in-flight marker the moment
it owns the run slot and clears it only in its ``finally`` (which runs
only if the process survives). A marker that survives a restart proves a
die-in-flight, so ``_check_startup_missed`` forces a catch-up
(``orphan_trigger_due``), bounded by the existing crash-recovery circuit
breaker.
"""

from __future__ import annotations

import logging
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
from src.core.scheduler import (
    MAX_CRASH_RECOVERY_ATTEMPTS,
    InAppScheduler,
    SchedulerState,
)


def _make_profile(
    profile_id: str = "p1",
    *,
    retry_enabled: bool = True,
    crash_recovery_attempts: int = 0,
) -> BackupProfile:
    """Build an HOURLY profile so ``_is_due`` is independent of the
    wall-clock hour at test execution time (the same reasoning as the
    other scheduler test modules — ``DAILY`` would gate on
    ``now >= target_today`` and flake before/after 10:00).

    The 02/06 incident was on a DAILY schedule, but ``orphan_trigger_due``
    is evaluated independently of frequency: a recent ``last_trigger``
    (slot consumed) plus a surviving in-flight marker reproduces the bug
    on HOURLY just as faithfully, without clock fragility.
    """
    profile = BackupProfile(
        id=profile_id,
        name=f"TestProfile-{profile_id}",
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
            retry_enabled=retry_enabled,
        ),
    )
    profile.crash_recovery_attempts = crash_recovery_attempts
    return profile


@pytest.fixture()
def scheduler(tmp_path: Path):
    """An InAppScheduler whose backup callback is a no-op mock."""
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


# ---------------------------------------------------------------------------
# SchedulerState — pure persistence contract for the in-flight marker
# ---------------------------------------------------------------------------


class TestInflightState:
    def test_set_get_roundtrip(self, tmp_path: Path) -> None:
        state = SchedulerState(tmp_path)
        dt = datetime(2026, 6, 2, 13, 5, 39)
        state.set_inflight("p1", dt)
        assert state.get_inflight("p1") == dt

    def test_get_absent_returns_none(self, tmp_path: Path) -> None:
        assert SchedulerState(tmp_path).get_inflight("nope") is None

    def test_clear_removes_marker(self, tmp_path: Path) -> None:
        state = SchedulerState(tmp_path)
        state.set_inflight("p1", datetime.now())
        state.clear_inflight("p1")
        assert state.get_inflight("p1") is None

    def test_clear_absent_is_noop(self, tmp_path: Path) -> None:
        SchedulerState(tmp_path).clear_inflight("never")  # must not raise

    def test_persists_across_instances(self, tmp_path: Path) -> None:
        dt = datetime(2026, 6, 2, 13, 5, 39)
        SchedulerState(tmp_path).set_inflight("p1", dt)
        # A fresh instance must read the same scheduler_state.json — the
        # marker has to survive the process restart that defines the bug.
        assert SchedulerState(tmp_path).get_inflight("p1") == dt

    def test_independent_from_last_trigger(self, tmp_path: Path) -> None:
        state = SchedulerState(tmp_path)
        trig = datetime(2026, 6, 1, 10, 0, 0)
        infl = datetime(2026, 6, 2, 13, 5, 0)
        state.set_last_trigger("p1", trig)
        state.set_inflight("p1", infl)
        # Distinct keys: clearing the in-flight marker must not disturb
        # the schedule slot (last_trigger) or vice versa.
        state.clear_inflight("p1")
        assert state.get_inflight("p1") is None
        assert state.get_last_trigger("p1") == trig

    def test_empty_profile_id_is_defensive(self, tmp_path: Path) -> None:
        state = SchedulerState(tmp_path)
        state.set_inflight("", datetime.now())  # no-op, must not raise
        state.clear_inflight("")  # no-op, must not raise
        assert state.get_inflight("") is None

    def test_corrupt_value_returns_none(self, tmp_path: Path) -> None:
        state = SchedulerState(tmp_path)
        state._state["inflight_p1"] = "not-an-iso-date"
        assert state.get_inflight("p1") is None


# ---------------------------------------------------------------------------
# _trigger_backup — the marker lifecycle (set on entry, clear in finally)
# ---------------------------------------------------------------------------


class TestTriggerBackupInflightLifecycle:
    def test_marker_present_during_run_and_cleared_after_success(self, scheduler) -> None:
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        seen: dict[str, object] = {}

        def callback(p: BackupProfile) -> None:
            seen["during"] = scheduler._state.get_inflight(p.id)

        scheduler._test_callback.side_effect = callback
        scheduler._trigger_backup(profile, datetime.now())

        # The marker must exist WHILE the callback runs ...
        assert seen["during"] is not None
        # ... and be gone once the run finishes cleanly.
        assert scheduler._state.get_inflight(profile.id) is None

    def test_marker_cleared_after_handled_exception(self, scheduler) -> None:
        # retry disabled so a non-skip failure does not spin _retry_backup
        # (which would sleep in this thread).
        profile = _make_profile(retry_enabled=False)
        scheduler._test_profiles.append(profile)
        scheduler._test_callback.side_effect = RuntimeError("boom")

        # _trigger_backup swallows the exception; the finally must still
        # clear the marker — a handled failure is NOT a die-in-flight.
        scheduler._trigger_backup(profile, datetime.now())
        assert scheduler._state.get_inflight(profile.id) is None

    def test_marker_not_written_when_slot_already_held(self, scheduler) -> None:
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        # A concurrent run (UI Start / another pass) already owns the slot.
        scheduler.mark_profile_running(profile.id)

        scheduler._trigger_backup(profile, datetime.now())

        # The trigger skips before the try-block, so our callback never
        # runs and we never stamp a marker over the live run's bookkeeping.
        scheduler._test_callback.assert_not_called()
        assert scheduler._state.get_inflight(profile.id) is None


# ---------------------------------------------------------------------------
# _check_startup_missed — orphaned-trigger recovery
# ---------------------------------------------------------------------------


class TestStartupOrphanRecovery:
    def test_orphan_marker_forces_trigger_when_slot_consumed(self, scheduler) -> None:
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        now = datetime.now()
        # Slot consumed (recent trigger → not due) but a previous run
        # died in flight (marker survives) and the engine never armed its
        # own crash flag (last_backup_completed stays True).
        scheduler._state.set_last_trigger(profile.id, now)
        scheduler._state.set_inflight(profile.id, now)
        assert profile.last_backup_completed is True

        scheduler._check_startup_missed()

        scheduler._test_callback.assert_called_once_with(profile)

    def test_orphan_recovery_increments_circuit_breaker(self, scheduler) -> None:
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        now = datetime.now()
        scheduler._state.set_last_trigger(profile.id, now)
        scheduler._state.set_inflight(profile.id, now)

        scheduler._check_startup_missed()

        # Bumped before the trigger so repeated die-in-flight cannot loop
        # forever (the engine resets it to 0 on a genuine success).
        assert profile.crash_recovery_attempts == 1

    def test_orphan_marker_cleared_after_successful_relaunch(self, scheduler) -> None:
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        now = datetime.now()
        scheduler._state.set_last_trigger(profile.id, now)
        scheduler._state.set_inflight(profile.id, now)

        scheduler._check_startup_missed()

        # The relaunch ran to its finally (mock callback succeeds), so the
        # marker is gone and a second restart will NOT recover again.
        assert scheduler._state.get_inflight(profile.id) is None

    def test_circuit_breaker_blocks_orphan_recovery(self, scheduler, caplog) -> None:
        profile = _make_profile(crash_recovery_attempts=MAX_CRASH_RECOVERY_ATTEMPTS)
        scheduler._test_profiles.append(profile)
        now = datetime.now()
        scheduler._state.set_last_trigger(profile.id, now)
        scheduler._state.set_inflight(profile.id, now)

        with caplog.at_level(logging.WARNING, logger="src.core.scheduler"):
            scheduler._check_startup_missed()

        scheduler._test_callback.assert_not_called()
        assert "circuit breaker TRIPPED" in caplog.text

    def test_orphan_skipped_when_run_already_in_progress(self, scheduler) -> None:
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        now = datetime.now()
        scheduler._state.set_last_trigger(profile.id, now)
        scheduler._state.set_inflight(profile.id, now)
        # A live run already owns the profile (it set the marker itself).
        scheduler.mark_profile_running(profile.id)

        scheduler._check_startup_missed()

        scheduler._test_callback.assert_not_called()
        # The live run owns the marker and will clear it in its own
        # finally — startup recovery must not touch it.
        assert scheduler._state.get_inflight(profile.id) is not None

    def test_not_due_without_marker_stays_quiet(self, scheduler) -> None:
        """Sanity: the fix must not re-fire a profile that simply ran
        recently and has no in-flight marker."""
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        scheduler._state.set_last_trigger(profile.id, datetime.now())  # not due

        scheduler._check_startup_missed()

        scheduler._test_callback.assert_not_called()
        assert scheduler._state.get_inflight(profile.id) is None


# ---------------------------------------------------------------------------
# Full reproduction of the 02/06/2026 incident
# ---------------------------------------------------------------------------


class TestCrypterIncidentRegression:
    """End-to-end shape of the incident: one burned-slot profile alongside
    two genuinely-due ones, all in a single startup catch-up pass."""

    def test_burned_slot_with_marker_is_recovered(self, scheduler) -> None:
        now = datetime.now()
        crypter = _make_profile("crypter")
        aws = _make_profile("aws")
        mybackup = _make_profile("mybackup")
        scheduler._test_profiles.extend([crypter, aws, mybackup])

        # crypter: slot consumed by a trigger that died in flight.
        scheduler._state.set_last_trigger(crypter.id, now)
        scheduler._state.set_inflight(crypter.id, now)
        # aws / mybackup: last real run >1h ago, genuinely due, no marker.
        scheduler._state.set_last_trigger(aws.id, now - timedelta(hours=2))
        scheduler._state.set_last_trigger(mybackup.id, now - timedelta(hours=2))

        scheduler._check_startup_missed()

        fired = {call.args[0].id for call in scheduler._test_callback.call_args_list}
        assert fired == {"crypter", "aws", "mybackup"}

    def test_burned_slot_without_marker_is_not_recovered(self, scheduler) -> None:
        """Inverse contract: WITHOUT the in-flight marker the bug must
        reproduce (crypter stays skipped). Proves this suite would catch a
        revert of the fix rather than passing vacuously."""
        now = datetime.now()
        crypter = _make_profile("crypter")
        aws = _make_profile("aws")
        scheduler._test_profiles.extend([crypter, aws])

        scheduler._state.set_last_trigger(crypter.id, now)  # burned, NO marker
        scheduler._state.set_last_trigger(aws.id, now - timedelta(hours=2))  # due

        scheduler._check_startup_missed()

        fired = {call.args[0].id for call in scheduler._test_callback.call_args_list}
        assert fired == {"aws"}
