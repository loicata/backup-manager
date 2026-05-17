"""Tests for the v3.7.4 fix to periodic-verify scheduling.

Regression (v3.7.3 and earlier): ``_check_verify_due`` treated a
profile with ``last_verify is None`` as "verification due right now"
and fired the periodic verify on the very first scheduler tick after
profile creation (~30 s, since ``CHECK_INTERVAL`` = 30). Combined with
``IntegrityVerifier.verify_iter`` not filtering by profile name, a
fresh profile pointing at a destination that already held *foreign*
backups (other profiles sharing the same drive) re-hashed those
foreign backups in parallel with its own first backup run.

Fix:
1. ``_check_verify_due`` seeds ``last_verify = now`` and returns when
   ``last_verify is None`` — the first periodic verify is now due
   ``interval_days`` after creation, not on the next tick.
2. ``mark_verify_now`` is the public API for seeding the timer
   from out-of-band callers (wizard, profile import).
"""

from __future__ import annotations

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


def _make_profile(profile_id: str = "p1", *, verify_enabled: bool = True) -> BackupProfile:
    """Build a verify-enabled profile pointing nowhere useful.

    The destination path is intentionally unreachable so a verify run
    that DID slip through would be cheap (no backups listed) but still
    visible as a side effect on ``set_last_verify``. The tests assert
    on state, not on the verify outcome itself.
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
            verify_enabled=verify_enabled,
            verify_interval_days=7,
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
    yield sched
    if sched._running:
        sched.stop()


class TestFirstObservationDoesNotTrigger:
    """``_check_verify_due`` must not fire on a profile whose timer
    has never been seeded (``last_verify is None``)."""

    def test_first_call_seeds_timer_without_invoking_verifier(
        self,
        scheduler,
        monkeypatch,
    ) -> None:
        """First tick on a fresh profile records ``last_verify`` at
        ``now`` and returns; no ``IntegrityVerifier`` is constructed."""
        profile = _make_profile()
        scheduler._test_profiles.append(profile)

        # Sentinel: if the code path that constructs the verifier runs,
        # the test fails loudly.  Patching the class at the import site
        # used inside ``_check_verify_due`` (lazy import) makes the
        # sentinel observable regardless of import order.
        construct_calls = MagicMock()
        monkeypatch.setattr(
            "src.core.integrity_verifier.IntegrityVerifier",
            construct_calls,
        )

        assert scheduler._state.get_last_verify(profile.id) is None

        now = datetime(2026, 5, 17, 15, 48, 47)
        scheduler._check_verify_due(profile, now)

        # Timer seeded at ``now``, no verifier was instantiated.
        seeded = scheduler._state.get_last_verify(profile.id)
        assert seeded == now
        construct_calls.assert_not_called()

    def test_second_call_within_interval_is_silent(
        self,
        scheduler,
        monkeypatch,
    ) -> None:
        """A second call shortly after seeding stays inside the
        ``interval_days`` window and does nothing."""
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        construct_calls = MagicMock()
        monkeypatch.setattr(
            "src.core.integrity_verifier.IntegrityVerifier",
            construct_calls,
        )

        now = datetime(2026, 5, 17, 15, 48, 47)
        scheduler._check_verify_due(profile, now)
        # 30 s later, still well under the 7-day interval.
        scheduler._check_verify_due(profile, now + timedelta(seconds=30))

        assert construct_calls.call_count == 0

    def test_call_after_interval_does_trigger(
        self,
        scheduler,
        monkeypatch,
    ) -> None:
        """Once ``interval_days`` have elapsed, the verify DOES fire.

        Without this assertion the fix could silently disable periodic
        verification altogether — the regression we want to avoid is
        firing too early, not never firing.
        """
        profile = _make_profile()
        scheduler._test_profiles.append(profile)

        # Stub the verifier so the test does not actually walk a disk;
        # we only care that the construct path is reached.
        fake_verifier = MagicMock()
        fake_verifier.verify_all.return_value = MagicMock(success=True, ok_count=0, error_count=0)
        construct_calls = MagicMock(return_value=fake_verifier)
        monkeypatch.setattr(
            "src.core.integrity_verifier.IntegrityVerifier",
            construct_calls,
        )
        # ConfigManager() is built lazily inside _check_verify_due when
        # no _config_manager was injected. Stub it out so the test does
        # not touch the real filesystem.
        monkeypatch.setattr("src.core.config.ConfigManager", MagicMock())

        seed = datetime(2026, 5, 17, 15, 48, 47)
        scheduler._state.set_last_verify(profile.id, seed)
        # 8 days later — past the 7-day interval.
        scheduler._check_verify_due(profile, seed + timedelta(days=8))

        construct_calls.assert_called_once()
        fake_verifier.verify_all.assert_called_once()


class TestMarkVerifyNow:
    """Public API symmetric to ``mark_triggered_now``."""

    def test_mark_verify_now_seeds_state(self, scheduler) -> None:
        """``mark_verify_now`` writes ``last_verify`` for later reads."""
        profile = _make_profile()
        now = datetime(2026, 5, 17, 15, 48, 47)

        scheduler.mark_verify_now(profile.id, now)
        assert scheduler._state.get_last_verify(profile.id) == now

    def test_mark_verify_now_defaults_to_now(self, scheduler) -> None:
        """Omitting ``dt`` records ``datetime.now()`` (within tolerance)."""
        profile = _make_profile()
        before = datetime.now()
        scheduler.mark_verify_now(profile.id)
        after = datetime.now()
        recorded = scheduler._state.get_last_verify(profile.id)
        assert recorded is not None
        assert before <= recorded <= after

    def test_mark_verify_now_prevents_immediate_trigger(
        self,
        scheduler,
        monkeypatch,
    ) -> None:
        """Wizard-style path: seed at creation, first tick stays silent.

        Mirrors the production code path in ``src/ui/app.py`` where the
        wizard calls both ``mark_triggered_now`` and ``mark_verify_now``
        on every fresh profile.
        """
        profile = _make_profile()
        scheduler._test_profiles.append(profile)
        construct_calls = MagicMock()
        monkeypatch.setattr(
            "src.core.integrity_verifier.IntegrityVerifier",
            construct_calls,
        )

        # The wizard would do this right after saving the profile.
        scheduler.mark_verify_now(profile.id, datetime(2026, 5, 17, 15, 47, 51))

        # Scheduler tick fires ~60 s later (CHECK_INTERVAL ≈ 30 s).
        scheduler._check_verify_due(
            profile,
            datetime(2026, 5, 17, 15, 48, 51),
        )

        construct_calls.assert_not_called()
