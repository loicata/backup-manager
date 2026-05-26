"""Tests for the post-backup-start health repoll.

Companion to the swallow guard tested in
``test_health_poll_race_swallow.py``: the swallow hides any NEW
spurious ``"read-only or locked"`` result that arrives during a
backup, but cannot CLEAR a card that was ALREADY painted red by an
earlier poll. The repoll fires fresh probes immediately after
``_backup_running`` flips to True; because the new probes read the
flag at thread start, they are guaranteed to take the lightweight
path and produce a green result that repaints the card.

User-visible bug this is regression-guarding:
On 26/05/2026 the v3.7.27 release shipped the swallow guard but
the card still showed red during a backup because the red had been
painted by a poll BEFORE the swallow guard had any reason to
trigger. Spawning fresh checks at backup-start replaces the stale
red with a current green.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class _BareApp:
    """Minimal stand-in for BackupManagerApp.

    Exposes only the surface the repoll helper touches:
    ``_health_configs`` and ``_check_single_destination``. Spawned
    threads are intercepted via the ``threading.Thread`` patch so
    nothing actually runs — we only verify ``start()`` is called
    with the right ``target`` and arguments.
    """


def _make_app_with_destinations(*labels: str) -> _BareApp:
    """Build a bare app stub whose ``_health_configs`` has N entries."""
    app = _BareApp()
    app._health_configs = {
        idx: (MagicMock(name=f"config-{label}"), label) for idx, label in enumerate(labels)
    }
    app._check_single_destination = MagicMock(name="_check_single_destination")
    return app


def _call_repoll(app) -> None:
    """Invoke the real ``_repoll_destinations_after_backup_start``."""
    from src.ui.app import BackupManagerApp

    BackupManagerApp._repoll_destinations_after_backup_start(app)


class TestSpawnsOnePerDestination:
    """One thread is spawned for every entry in ``_health_configs``."""

    def test_one_thread_per_destination_with_two_entries(self):
        app = _make_app_with_destinations("Storage", "Mirror 1")

        with patch("src.ui.app.threading.Thread") as mock_thread:
            _call_repoll(app)

        assert mock_thread.call_count == 2

        # Each spawn must target ``_check_single_destination`` bound to
        # the app stub, with the correct (index, config, label) tuple.
        observed_args = [call.kwargs["args"] for call in mock_thread.call_args_list]
        assert {args[0] for args in observed_args} == {0, 1}
        assert {args[2] for args in observed_args} == {"Storage", "Mirror 1"}

    def test_single_destination_spawns_one_thread(self):
        app = _make_app_with_destinations("Storage")
        with patch("src.ui.app.threading.Thread") as mock_thread:
            _call_repoll(app)
        assert mock_thread.call_count == 1

    def test_three_destinations_spawn_three_threads(self):
        app = _make_app_with_destinations("Storage", "Mirror 1", "Mirror 2")
        with patch("src.ui.app.threading.Thread") as mock_thread:
            _call_repoll(app)
        assert mock_thread.call_count == 3


class TestThreadDaemonAndName:
    """Spawned threads must be daemon + named so they show up in dumps."""

    def test_thread_is_daemon(self):
        app = _make_app_with_destinations("Storage")
        with patch("src.ui.app.threading.Thread") as mock_thread:
            _call_repoll(app)
        kwargs = mock_thread.call_args_list[0].kwargs
        assert kwargs["daemon"] is True

    def test_thread_name_carries_destination_label(self):
        """Name prefix makes the repoll distinguishable from regular polls."""
        app = _make_app_with_destinations("Mirror 1")
        with patch("src.ui.app.threading.Thread") as mock_thread:
            _call_repoll(app)
        kwargs = mock_thread.call_args_list[0].kwargs
        assert "Mirror 1" in kwargs["name"]
        assert "Repoll" in kwargs["name"], (
            "Thread name must say 'Repoll' so it is distinguishable from "
            "the regular HealthPoll-X / HealthCheck-X threads in a dump"
        )


class TestNoOpWhenEmpty:
    """Empty or absent ``_health_configs`` must not raise."""

    def test_empty_dict_is_noop(self):
        app = _BareApp()
        app._health_configs = {}
        app._check_single_destination = MagicMock()
        with patch("src.ui.app.threading.Thread") as mock_thread:
            _call_repoll(app)
        mock_thread.assert_not_called()

    def test_missing_attribute_is_noop(self):
        """A repoll fired before ``_update_health_dashboard`` ran must not raise."""
        app = _BareApp()
        # Deliberately do NOT set ``_health_configs``.
        app._check_single_destination = MagicMock()
        with patch("src.ui.app.threading.Thread") as mock_thread:
            _call_repoll(app)
        mock_thread.assert_not_called()


class TestWiredInBackupStartPaths:
    """The two flag-flip sites must call the repoll helper."""

    def test_start_backup_thread_source_contains_repoll_call(self):
        """``_start_backup_thread`` invokes the repoll after raising the flag."""
        import inspect

        from src.ui.app import BackupManagerApp

        source = inspect.getsource(BackupManagerApp._start_backup_thread)
        flag_set_idx = source.index("self._backup_running = True")
        repoll_idx = source.index("_repoll_destinations_after_backup_start")
        assert repoll_idx > flag_set_idx, (
            "Repoll MUST be called AFTER setting _backup_running=True so "
            "the spawned threads read the True value and take the "
            "lightweight path. Calling before would race the same bug "
            "the repoll is trying to fix."
        )

    def test_scheduled_backup_source_contains_repoll_call(self):
        """``_scheduled_backup`` invokes the repoll after raising the flag."""
        import inspect

        from src.ui.app import BackupManagerApp

        source = inspect.getsource(BackupManagerApp._scheduled_backup)
        flag_set_idx = source.index("self._backup_running = True")
        repoll_idx = source.index("_repoll_destinations_after_backup_start")
        assert repoll_idx > flag_set_idx


class TestRepollUsesSameCheckPath:
    """Repolled threads MUST go through ``_check_single_destination``.

    The whole point of using the same entry point is that the swallow
    guard in ``_on_health_result`` still applies — if for some reason
    the local drive is genuinely locked at this exact instant (AV
    grabbed the directory just after _backup_running flipped), the
    spurious result is still swallowed downstream rather than
    flashing red.
    """

    def test_target_is_check_single_destination(self):
        app = _make_app_with_destinations("Storage")
        with patch("src.ui.app.threading.Thread") as mock_thread:
            _call_repoll(app)
        target = mock_thread.call_args_list[0].kwargs["target"]
        # Must be the bound method of the SAME app object, not an
        # unrelated lambda or free function.
        assert target == app._check_single_destination


@pytest.mark.parametrize("count", [0, 1, 2, 5])
def test_thread_count_matches_destinations(count):
    app = _BareApp()
    app._health_configs = {i: (MagicMock(), f"Dest{i}") for i in range(count)}
    app._check_single_destination = MagicMock()

    with patch("src.ui.app.threading.Thread") as mock_thread:
        _call_repoll(app)

    assert mock_thread.call_count == count
