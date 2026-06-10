"""Tests for the health-poll race guard in ``_on_health_result``.

The bug being defended against (visible on every backup launch up to
v3.7.26): a ``HealthCheck-Storage`` thread spawned BEFORE
``_backup_running`` flips to True caches ``lightweight=False`` at
launch time. ``test_connection`` then runs its wake-up backoff (up
to ~15.8 s on a sleeping USB), and by the time it gets to the
``write_text(".backup_manager_test")`` probe the worker thread of
the just-started backup is already writing to the same drive. The
probe trips ``PermissionError``, the result reaches
``_on_health_result`` carrying ``"Destination is read-only or
locked"``, and the Destinations card shows red for ~60 s (until the
next poll captures ``_backup_running == True`` and switches to the
lightweight path).

These tests pin the guard's three-clause contract:

1. ``_backup_running == True`` AND ``health.online is False`` AND
   the error contains ``READ_ONLY_OR_LOCKED_MARKER`` → swallow.
2. Any clause not satisfied → pass through to the UI as before.

The narrow predicate (exact marker substring) is critical: a wider
guard would also swallow real "drive unplugged" / "network down"
errors during a backup, and the card would lie about a destination
that has truly gone dark.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.core.health_checker import DestinationHealth
from src.storage.local import READ_ONLY_OR_LOCKED_MARKER


def _make_app_stub(backup_running: bool):
    """Return a stub with the minimal surface ``_on_health_result`` reads.

    Avoids importing ``BackupManagerApp`` (which would pull tkinter,
    11 tab modules, and the scheduler at import time). The method
    under test only touches ``self._backup_running``, ``self.tab_run``,
    and the (unbound) ``logger`` — perfect candidate for stub-and-bind.
    """
    from src.ui.app import BackupManagerApp

    stub = MagicMock(spec=BackupManagerApp)
    stub._backup_running = backup_running
    stub._active_engines = {}
    stub._launch_in_progress = False
    stub._last_health_online = {}
    # The swallow guard now consults the derived predicate
    # ``_a_backup_is_active`` (so an overlapping run's stale boolean
    # cannot un-blind it). Bind the REAL predicate against this stub so
    # the test exercises production logic rather than an auto-mock.
    stub._a_backup_is_active = lambda: BackupManagerApp._a_backup_is_active(stub)
    # tab_run.after is the schedule-on-main-thread call we want to
    # observe (or NOT observe when the swallow path fires).
    stub.tab_run = MagicMock()
    return stub


def _call_on_health_result(stub, index: int, health: DestinationHealth) -> None:
    """Invoke the real ``_on_health_result`` against ``stub``.

    Goes through ``__func__`` so we are exercising the actual
    production code path, not a method on the MagicMock spec.
    """
    from src.ui.app import BackupManagerApp

    BackupManagerApp._on_health_result(stub, index, health)


class TestSwallowConditions:
    """The three guard clauses must ALL hold for the swallow to fire."""

    def test_race_pattern_is_swallowed_during_backup(self):
        stub = _make_app_stub(backup_running=True)
        health = DestinationHealth(
            label="Storage",
            backend_type="local",
            online=False,
            error=f"{READ_ONLY_OR_LOCKED_MARKER} (permission denied on E:\\): [Errno 13]",
        )

        _call_on_health_result(stub, 0, health)

        stub.tab_run.after.assert_not_called()

    def test_race_pattern_is_passed_through_when_no_backup_running(self):
        """Same error, but no backup in flight → real problem, show it."""
        stub = _make_app_stub(backup_running=False)
        health = DestinationHealth(
            label="Storage",
            backend_type="local",
            online=False,
            error=f"{READ_ONLY_OR_LOCKED_MARKER} (permission denied on E:\\): [Errno 13]",
        )

        _call_on_health_result(stub, 0, health)

        stub.tab_run.after.assert_called_once()

    def test_other_error_during_backup_is_passed_through(self):
        """A drive that genuinely disappeared mid-backup must STILL surface."""
        stub = _make_app_stub(backup_running=True)
        health = DestinationHealth(
            label="Storage",
            backend_type="local",
            online=False,
            error="Drive not ready after wake-up retries: E:\\",
        )

        _call_on_health_result(stub, 0, health)

        stub.tab_run.after.assert_called_once()

    def test_success_result_during_backup_is_passed_through(self):
        """The lightweight-path success result MUST update the card."""
        stub = _make_app_stub(backup_running=True)
        health = DestinationHealth(
            label="Storage",
            backend_type="local",
            online=True,
            free_bytes=123_456_789_012,
            error="",
        )

        _call_on_health_result(stub, 0, health)

        stub.tab_run.after.assert_called_once()

    def test_online_none_pending_state_is_passed_through(self):
        """An ``online=None`` pending state must reach the UI to clear stale red."""
        stub = _make_app_stub(backup_running=True)
        health = DestinationHealth(
            label="Storage",
            backend_type="local",
            online=None,
        )

        _call_on_health_result(stub, 0, health)

        stub.tab_run.after.assert_called_once()


class TestPartialMatch:
    """The marker must be matched as a substring, not as exact equality."""

    def test_marker_anywhere_in_error_is_swallowed_when_racing(self):
        """The marker is embedded inside a longer message — substring match."""
        stub = _make_app_stub(backup_running=True)
        embedded = (
            f"Pre-flight: backend reported '{READ_ONLY_OR_LOCKED_MARKER} "
            f"(permission denied on E:\\): [Errno 13] PermissionError'"
        )
        health = DestinationHealth(
            label="Storage",
            backend_type="local",
            online=False,
            error=embedded,
        )

        _call_on_health_result(stub, 0, health)

        stub.tab_run.after.assert_not_called()

    def test_empty_error_field_does_not_match(self):
        """``error=""`` must not accidentally satisfy ``in ""`` semantics."""
        stub = _make_app_stub(backup_running=True)
        health = DestinationHealth(
            label="Storage",
            backend_type="local",
            online=False,
            error="",
        )

        _call_on_health_result(stub, 0, health)

        # Empty-string error with online=False → still pass through so
        # the previous "online" state on the card is replaced (it would
        # otherwise be a silent lie that the destination is OK).
        stub.tab_run.after.assert_called_once()


class TestNoBackupAttribute:
    """If the backup-state fields are absent (early boot), guard MUST NOT fire.

    ``_a_backup_is_active`` reads ``_active_engines`` / ``_backup_running``
    / ``_launch_in_progress`` via ``getattr`` defaults, so an attribute
    lookup against a partially-initialised app returns False rather than
    raising. The very first health-check thread can fire before
    ``__init__`` reaches the lines that set those fields.
    """

    def test_missing_attribute_treated_as_false(self):
        """A bare object without the backup-state fields must NOT swallow."""
        from src.ui.app import BackupManagerApp

        # Plain object (no spec) with NONE of the backup-state fields:
        # ``_a_backup_is_active`` must return False via its getattr
        # defaults rather than raising AttributeError.
        class _BareStub:
            pass

        stub = _BareStub()
        stub.tab_run = MagicMock()
        stub._last_health_online = {}
        stub._a_backup_is_active = lambda: BackupManagerApp._a_backup_is_active(stub)

        health = DestinationHealth(
            label="Storage",
            backend_type="local",
            online=False,
            error=f"{READ_ONLY_OR_LOCKED_MARKER} (permission denied)",
        )

        BackupManagerApp._on_health_result(stub, 0, health)

        # Not in flight (fields missing → False) → real error → must
        # reach the UI so the user sees a destination problem.
        stub.tab_run.after.assert_called_once()


class TestMarkerStability:
    """``READ_ONLY_OR_LOCKED_MARKER`` is a UI contract — must stay stable."""

    def test_test_connection_error_message_contains_the_marker(self, tmp_path):
        """End-to-end: a write-failing destination produces the marker substring.

        Spins up a real ``LocalStorage`` against a directory whose
        ``write_text`` is monkey-patched to raise ``PermissionError``,
        then asserts the substring appears in the message returned by
        ``test_connection``. Catches drift where the constant value
        and the message format silently diverge.
        """
        from unittest.mock import patch

        from src.storage.local import LocalStorage

        dest = tmp_path / "dest"
        dest.mkdir()
        backend = LocalStorage(str(dest))

        original_write_text = type(dest).write_text

        def selective_failing_write(self, *args, **kwargs):
            if self.name == ".backup_manager_test":
                raise PermissionError("simulated AV lock")
            return original_write_text(self, *args, **kwargs)

        with patch.object(type(dest), "write_text", selective_failing_write):
            ok, msg = backend.test_connection()

        assert ok is False
        assert READ_ONLY_OR_LOCKED_MARKER in msg, (
            f"LocalStorage.test_connection no longer surfaces the "
            f"READ_ONLY_OR_LOCKED_MARKER substring (got {msg!r}). The "
            f"race guard in BackupManagerApp._on_health_result will "
            f"stop swallowing the spurious card. Restore the marker "
            f"in the PermissionError branch or update the constant."
        )

    def test_marker_is_a_non_empty_string(self):
        assert isinstance(READ_ONLY_OR_LOCKED_MARKER, str)
        assert READ_ONLY_OR_LOCKED_MARKER.strip()


@pytest.mark.parametrize(
    "backup_running,online,error,expect_swallowed",
    [
        # All three clauses true → swallow.
        (True, False, f"{READ_ONLY_OR_LOCKED_MARKER} blah", True),
        # backup_running False → pass through.
        (False, False, f"{READ_ONLY_OR_LOCKED_MARKER} blah", False),
        # online True (success) → pass through.
        (True, True, "", False),
        # online None (pending) → pass through.
        (True, None, "", False),
        # online False but unrelated error → pass through.
        (True, False, "Drive not ready", False),
        # online False but empty error → pass through (no marker to match).
        (True, False, "", False),
    ],
)
def test_swallow_truth_table(backup_running, online, error, expect_swallowed):
    """Cover every cell of the (backup_running, online, error) truth table."""
    stub = _make_app_stub(backup_running=backup_running)
    health = DestinationHealth(
        label="Storage",
        backend_type="local",
        online=online,
        error=error,
    )

    _call_on_health_result(stub, 0, health)

    if expect_swallowed:
        stub.tab_run.after.assert_not_called()
    else:
        stub.tab_run.after.assert_called_once()
