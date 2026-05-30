"""Coordination between the scheduler and the manual UI path.

3.7.45 still showed "Backup rejected: Another backup is already running"
in one situation: a freshly-activated profile (``last_trigger`` is None,
so ``_is_due`` returns True) was picked up by the scheduler's catch-up at
the same moment the user clicked "Start backup". Two ``run_backup`` calls
raced on the same profile; the loser logged the rejection.

Root cause: the manual path and the scheduler used two *separate* busy
registers and neither consulted the other in time. The fix is a single
atomic test-and-set on the scheduler's ``_profile_in_progress`` set,
used by BOTH paths:
  - scheduler ``_trigger_backup`` skips (no callback, no run_backup) if
    the slot is already held;
  - the manual ``_precheck_and_run`` claims the slot before launching and
    skips + chains to the next queued profile if it cannot.
"""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from src.core.config import (
    BackupProfile,
    ConfigManager,
    ScheduleConfig,
    ScheduleFrequency,
    StorageConfig,
    StorageType,
)
from src.core.scheduler import InAppScheduler

# ---------------------------------------------------------------------------
# Scheduler side — test-and-set + trigger skip
# ---------------------------------------------------------------------------


class TestTryAcquireProfile:
    def test_acquire_then_second_acquire_fails(self, tmp_path):
        sched = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        assert sched.try_acquire_profile("p1") is True
        assert sched.try_acquire_profile("p1") is False  # already held
        assert sched.try_acquire_profile("p2") is True  # different id is free

    def test_release_allows_reacquire(self, tmp_path):
        sched = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        assert sched.try_acquire_profile("p1") is True
        sched.unmark_profile_running("p1")
        assert sched.try_acquire_profile("p1") is True  # free again after release


class TestTriggerBackupCoordination:
    def _profile(self) -> BackupProfile:
        return BackupProfile(
            name="P",
            schedule=ScheduleConfig(
                enabled=True, frequency=ScheduleFrequency.DAILY, time="10:00"
            ),
        )

    def test_trigger_skips_when_slot_already_held(self, tmp_path):
        """A manual run holds the slot → the scheduler must NOT call the
        callback (which would invoke run_backup and log 'Backup rejected')."""
        calls = []
        sched = InAppScheduler(tmp_path, lambda: [], lambda p: calls.append(p.id))
        profile = self._profile()
        # Simulate the manual path having claimed the slot first.
        assert sched.try_acquire_profile(profile.id) is True

        sched._trigger_backup(profile, datetime.now())

        assert calls == []  # callback never fired — no second run_backup

    def test_trigger_runs_when_slot_free(self, tmp_path):
        calls = []
        sched = InAppScheduler(tmp_path, lambda: [], lambda p: calls.append(p.id))
        profile = self._profile()

        sched._trigger_backup(profile, datetime.now())

        assert calls == [profile.id]
        # Slot released in the finally so a later run can proceed.
        assert sched.try_acquire_profile(profile.id) is True


# ---------------------------------------------------------------------------
# Manual side — _precheck_and_run claims the slot / skips + chains
# ---------------------------------------------------------------------------


@pytest.fixture()
def app_two_active(tk_root, tmp_path, monkeypatch):
    """Hidden BackupManagerApp with two active LOCAL profiles."""
    monkeypatch.setenv("APPDATA", str(tmp_path))

    from src.ui.app import BackupManagerApp

    cfg = ConfigManager(config_dir=tmp_path / "BackupManager")
    p1 = BackupProfile(
        name="P1",
        storage=StorageConfig(
            storage_type=StorageType.LOCAL, destination_path=str(tmp_path / "d1")
        ),
    )
    p2 = BackupProfile(
        name="P2",
        storage=StorageConfig(
            storage_type=StorageType.LOCAL, destination_path=str(tmp_path / "d2")
        ),
    )
    cfg.save_profile(p1)
    cfg.save_profile(p2)

    import tkinter as tk

    toplevel = tk.Toplevel(tk_root)
    toplevel.withdraw()
    app = BackupManagerApp(toplevel)
    yield app, p1.id, p2.id
    toplevel.destroy()


def _get_profile(app, profile_id):
    for p in app._profiles:
        if p.id == profile_id:
            return p
    raise KeyError(profile_id)


class TestPrecheckSlotCoordination:
    def test_precheck_skips_and_chains_when_slot_taken(self, app_two_active):
        """Scheduler already holds the profile → manual launch must NOT
        call _start_backup_thread; it chains to the next queued profile."""
        app, p1_id, _ = app_two_active
        app.scheduler.try_acquire_profile = MagicMock(return_value=False)

        after_calls = []
        app.root.after = lambda delay, fn=None, *a: after_calls.append((delay, fn, a))
        started = []
        app._start_backup_thread = lambda profile, engine: started.append(profile.id)

        app._precheck_and_run(_get_profile(app, p1_id), MagicMock())

        assert started == []  # did NOT launch a duplicate run
        assert any(fn == app._dequeue_next_backup for (_, fn, _) in after_calls)

    def test_precheck_claims_slot_when_free(self, app_two_active, monkeypatch):
        app, p1_id, _ = app_two_active
        app.scheduler.try_acquire_profile = MagicMock(return_value=True)

        class _FakeThread:
            def __init__(self, *a, **k):
                pass

            def start(self):
                pass

        monkeypatch.setattr("src.ui.app.threading.Thread", _FakeThread)
        monkeypatch.setattr(app.root, "after", lambda *a, **k: None)
        monkeypatch.setattr(app, "_show_checking_message", lambda: None)

        app._precheck_and_run(_get_profile(app, p1_id), MagicMock())

        app.scheduler.try_acquire_profile.assert_called_once_with(p1_id)
        assert app._launching_profile_id == p1_id
        assert app._launch_in_progress is True

    def test_precheck_cancel_releases_slot(self, app_two_active):
        app, p1_id, _ = app_two_active
        app.scheduler.unmark_profile_running = MagicMock()
        app._launching_profile_id = p1_id

        app._on_precheck_cancel()

        app.scheduler.unmark_profile_running.assert_called_once_with(p1_id)
        assert app._launching_profile_id is None
