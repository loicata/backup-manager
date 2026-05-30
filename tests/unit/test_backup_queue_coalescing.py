"""Tests for queueing a backup instead of rejecting it (coalescing).

Before this change, clicking "Start backup" while a backup for the same
profile was already running (or double-clicking during the asynchronous
precheck) hit the engine's per-profile lock and was logged as
"Backup rejected: Another backup is already running...". The request was
lost.

Now ``_run_backup`` detects an in-flight (or launching) backup and queues
the request instead, with coalescing so a re-click never stacks a second
identical full backup. The running backup's ``finally`` drains the queue.

Two layers under test:
  1. ``select_profiles_to_queue`` — the pure coalescing helper.
  2. ``BackupManagerApp._run_backup`` / flag lifecycle — the UI wiring.
"""

from unittest.mock import MagicMock

import pytest

from src.core.backup_queue import select_profiles_to_queue
from src.core.config import (
    BackupProfile,
    ConfigManager,
    StorageConfig,
    StorageType,
)

# ---------------------------------------------------------------------------
# Layer 1 — pure helper
# ---------------------------------------------------------------------------


class TestSelectProfilesToQueue:
    """``select_profiles_to_queue`` partitions ids with coalescing."""

    def test_excludes_running_and_queued(self):
        to_queue, skipped = select_profiles_to_queue(
            ["a", "b", "c"], excluded_ids={"a", "c"}
        )
        assert to_queue == ["b"]
        assert skipped == ["a", "c"]

    def test_collapses_internal_duplicates(self):
        # The same id requested twice is queued once, not stacked.
        to_queue, skipped = select_profiles_to_queue(["a", "a", "b", "a"], excluded_ids=set())
        assert to_queue == ["a", "b"]
        assert skipped == ["a", "a"]

    def test_preserves_request_order(self):
        to_queue, _ = select_profiles_to_queue(["c", "a", "b"], excluded_ids=set())
        assert to_queue == ["c", "a", "b"]

    def test_all_excluded_yields_empty_queue(self):
        to_queue, skipped = select_profiles_to_queue(["a", "b"], excluded_ids={"a", "b"})
        assert to_queue == []
        assert skipped == ["a", "b"]

    def test_empty_request(self):
        to_queue, skipped = select_profiles_to_queue([], excluded_ids={"a"})
        assert to_queue == []
        assert skipped == []

    def test_excluded_accepts_any_iterable(self):
        # A list (not just a set) must work — the helper builds its own set.
        to_queue, _ = select_profiles_to_queue(["a", "b"], excluded_ids=["a"])
        assert to_queue == ["b"]

    def test_rejects_non_list_request(self):
        with pytest.raises(TypeError):
            select_profiles_to_queue("ab", excluded_ids=set())  # type: ignore[arg-type]

    def test_rejects_non_str_member(self):
        with pytest.raises(TypeError):
            select_profiles_to_queue(["a", 3], excluded_ids=set())  # type: ignore[list-item]

    def test_rejects_empty_str_member(self):
        with pytest.raises(TypeError):
            select_profiles_to_queue(["a", ""], excluded_ids=set())


# ---------------------------------------------------------------------------
# Layer 2 — UI wiring (real BackupManagerApp against a temp config)
# ---------------------------------------------------------------------------


@pytest.fixture()
def app_two_active(tk_root, tmp_path, monkeypatch):
    """Build a hidden BackupManagerApp with two active LOCAL profiles."""
    monkeypatch.setenv("APPDATA", str(tmp_path))

    from src.ui.app import BackupManagerApp

    cfg = ConfigManager(config_dir=tmp_path / "BackupManager")
    p1 = BackupProfile(
        name="P1",
        storage=StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path=str(tmp_path / "d1"),
        ),
    )
    p2 = BackupProfile(
        name="P2",
        storage=StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path=str(tmp_path / "d2"),
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


def _active_ids(app) -> list[str]:
    """Active profile ids in the order ``_run_backup`` iterates them."""
    return [p.id for p in app._profiles if p.active]


def _get_profile(app, profile_id: str) -> BackupProfile:
    for p in app._profiles:
        if p.id == profile_id:
            return p
    raise KeyError(profile_id)


class TestRunBackupQueues:
    """``_run_backup`` queues instead of rejecting when busy."""

    def test_click_while_running_queues_instead_of_rejecting(self, app_two_active):
        app, _, _ = app_two_active
        launched: list[str] = []
        app._precheck_and_run = lambda profile, engine, _retry_attempt=0: launched.append(
            profile.id
        )
        app._backup_running = True  # simulate an in-flight backup

        app._run_backup()

        # Nothing launched immediately; both actives are queued for later.
        assert launched == []
        assert [p.id for p in app._backup_queue] == _active_ids(app)

    def test_reclick_same_profile_does_not_stack(self, app_two_active):
        app, p1_id, p2_id = app_two_active
        app._precheck_and_run = lambda *a, **k: None
        app._backup_running = True
        # P1 already queued from a previous click.
        app._backup_queue = [_get_profile(app, p1_id)]

        app._run_backup()

        # P1 is NOT duplicated; P2 is appended once. Order preserved.
        queued = [p.id for p in app._backup_queue]
        assert queued == [p1_id, p2_id]
        assert queued.count(p1_id) == 1

    def test_running_profile_is_not_requeued(self, app_two_active):
        app, p1_id, p2_id = app_two_active
        app._precheck_and_run = lambda *a, **k: None
        # P1 is actively running.
        app._active_engines[p1_id] = MagicMock()
        app._backup_running = True

        app._run_backup()

        queued = [p.id for p in app._backup_queue]
        assert p1_id not in queued
        assert p2_id in queued

    def test_launch_in_progress_blocks_immediate_start(self, app_two_active):
        app, _, _ = app_two_active
        launched: list[str] = []
        app._precheck_and_run = lambda profile, engine, _retry_attempt=0: launched.append(
            profile.id
        )
        # No backup running yet, but a launch is mid-precheck.
        app._backup_running = False
        app._launch_in_progress = True

        app._run_backup()

        assert launched == []  # queued, not started — closes the double-click window
        assert [p.id for p in app._backup_queue] == _active_ids(app)

    def test_idle_click_launches_first_and_queues_rest(self, app_two_active):
        app, _, _ = app_two_active
        launched: list[str] = []
        app._precheck_and_run = lambda profile, engine, _retry_attempt=0: launched.append(
            profile.id
        )
        app._backup_running = False
        app._launch_in_progress = False
        app._active_engines.clear()

        app._run_backup()

        active = _active_ids(app)
        # First active profile is launched; the rest go to the queue.
        assert launched == [active[0]]
        assert [p.id for p in app._backup_queue] == active[1:]


class TestLaunchInProgressFlag:
    """The ``_launch_in_progress`` flag covers the async precheck window."""

    def test_precheck_sets_flag(self, app_two_active, monkeypatch):
        app, p1_id, _ = app_two_active

        class _FakeThread:
            def __init__(self, *a, **k):
                pass

            def start(self):
                pass

        # Stop the async precheck from actually running.
        monkeypatch.setattr("src.ui.app.threading.Thread", _FakeThread)
        monkeypatch.setattr(app.root, "after", lambda *a, **k: None)
        monkeypatch.setattr(app, "_show_checking_message", lambda: None)

        app._launch_in_progress = False
        app._precheck_and_run(_get_profile(app, p1_id), MagicMock())

        assert app._launch_in_progress is True

    def test_start_backup_thread_clears_flag(self, app_two_active, monkeypatch):
        app, p1_id, _ = app_two_active

        class _FakeThread:
            def __init__(self, *a, **k):
                pass

            def start(self):
                pass

        # Stop the real backup thread from running its pipeline.
        monkeypatch.setattr("src.ui.app.threading.Thread", _FakeThread)
        monkeypatch.setattr(app, "_repoll_destinations_after_backup_start", lambda: None)

        app._launch_in_progress = True
        app._start_backup_thread(_get_profile(app, p1_id), MagicMock())

        assert app._launch_in_progress is False
        assert app._backup_running is True
        app._backup_running = False  # tidy shared state

    def test_precheck_cancel_clears_flag(self, app_two_active):
        app, _, _ = app_two_active
        app._launch_in_progress = True

        app._on_precheck_cancel()

        assert app._launch_in_progress is False


class TestDequeueDrainsQueue:
    """The drain path used by both manual and scheduled ``finally`` blocks."""

    def test_dequeue_launches_next_queued_profile(self, app_two_active):
        app, _, p2_id = app_two_active
        launched: list[str] = []
        app._precheck_and_run = lambda profile, engine, _retry_attempt=0: launched.append(
            profile.id
        )
        app._backup_queue = [_get_profile(app, p2_id)]

        app._dequeue_next_backup(previous_failed=False, previous_name="P1")

        assert launched == [p2_id]
        assert app._backup_queue == []
