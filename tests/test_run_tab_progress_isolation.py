"""Regression guard for the 2026-05-14 v3.6.5 Run/Verify cross-talk bug.

The Run tab and the Verify tab share the same EventBus. Until 3.6.5
the Run tab's PROGRESS subscriber updated its own progress bar on
every event regardless of source. Clicking "Verify all backups" in
the Verify tab pushed PROGRESS events on the same bus, which the
Run tab interpreted as "a backup is in flight" and overwrote its
own header (which had been showing "Last backup: Success — 2h ago")
with the manifest path the verifier was currently walking.

v3.6.5 adds a ``_backup_active`` flag on RunTab. It flips True only
on STATUS=running (emitted by BackupEngine) and back to False on
STATUS=success / error / idle. PROGRESS events arriving while the
flag is False are dropped before any widget update.

These tests pin the contract without touching Tk: RunTab is built
via ``__new__`` and the methods are exercised directly.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from src.ui.tabs.run_tab import RunTab


def _make_tab_stub() -> RunTab:
    """Build a RunTab without touching Tk.

    Hand-attach the handful of attributes the methods under test
    read so we can call them without a Tk root.
    """
    tab = RunTab.__new__(RunTab)
    tab._backup_active = False
    tab.after = MagicMock(name="after")
    tab.start_btn = MagicMock(name="start_btn")
    tab.cancel_btn = MagicMock(name="cancel_btn")
    tab.status_label = MagicMock(name="status_label")
    tab.progress_bar = MagicMock(name="progress_bar")
    tab.percent_label = MagicMock(name="percent_label")
    return tab


class TestRunTabIgnoresProgressBetweenBackups:
    """PROGRESS events from the Verify tab must not move the Run-tab bar."""

    def test_progress_ignored_when_no_backup_is_active(self) -> None:
        """The default state (just opened the app) is "no backup active"."""
        tab = _make_tab_stub()
        assert tab._backup_active is False

        # A Verify-tab launched verify emits PROGRESS with phase="verification".
        tab._on_progress(
            current=37,
            total=100,
            filename="metadata.json",
            phase="verification",
        )
        # No widget update should be scheduled.
        assert tab.after.call_count == 0

    def test_progress_accepted_while_backup_is_running(self) -> None:
        """During an actual backup, PROGRESS must reach the widgets."""
        tab = _make_tab_stub()
        tab._update_status("running")
        assert tab._backup_active is True

        tab._on_progress(
            current=42,
            total=100,
            filename="manifest.json",
            phase="verification",
        )
        # One update_progress dispatch.
        assert tab.after.call_count == 1
        args = tab.after.call_args.args
        assert args[0] == 0
        assert args[1] == tab._update_progress
        # ``_on_progress`` forwards ``profile_id`` (v3.7.12 per-profile
        # event tagging) as the trailing argument to ``_update_progress``.
        assert args[2:] == (42, 100, "manifest.json", "verification", "")

    def test_progress_dropped_again_after_success(self) -> None:
        """STATUS=success closes the activity window."""
        tab = _make_tab_stub()
        tab._update_status("running")
        tab._update_status("success")
        assert tab._backup_active is False

        tab._on_progress(current=10, total=100, filename="x", phase="verification")
        assert tab.after.call_count == 0

    def test_progress_dropped_again_after_error(self) -> None:
        tab = _make_tab_stub()
        tab._update_status("running")
        tab._update_status("error")
        assert tab._backup_active is False

        tab._on_progress(current=10, total=100, filename="x", phase="verification")
        assert tab.after.call_count == 0

    def test_progress_dropped_again_after_idle(self) -> None:
        tab = _make_tab_stub()
        tab._update_status("running")
        tab._update_status("idle")
        assert tab._backup_active is False

        tab._on_progress(current=10, total=100, filename="x", phase="verification")
        assert tab.after.call_count == 0


class TestRunTabIgnoresLogBetweenBackups:
    """LOG events from the Verify tab must not append rows to the
    Run-tab Message panel either.

    Same root cause as the PROGRESS bug above: both tabs share the
    EventBus, so the manual ``Verify all backups`` action fired LOG
    events that ended up in this tab's Treeview (the user saw
    ``Verification OK: 262646/262646 files verified`` rows show up
    between backups).
    """

    def _make_log_stub(self) -> RunTab:
        tab = RunTab.__new__(RunTab)
        tab._backup_active = False
        tab._current_phase = ""
        # ``_on_log`` persists to the per-profile history store before
        # the cross-tab gate. ``None`` disables persistence so these
        # tests stay focused on the ``_backup_active`` gating contract.
        tab._history_store = None
        tab.after = MagicMock(name="after")
        tab.start_btn = MagicMock()
        tab.cancel_btn = MagicMock()
        tab.status_label = MagicMock()
        tab.progress_bar = MagicMock()
        tab.percent_label = MagicMock()
        return tab

    def test_log_dropped_when_no_backup_is_active(self) -> None:
        tab = self._make_log_stub()
        tab._on_log(
            message="Verification OK: 262646/262646 files verified",
            level="info",
            phase="verifier",
        )
        assert tab.after.call_count == 0

    def test_log_accepted_while_backup_is_running(self) -> None:
        tab = self._make_log_stub()
        tab._update_status("running")
        tab._on_log(message="Building integrity manifest...", level="info", phase="")
        # One dispatch. ``_on_log`` now defers to ``_dispatch_log_event``
        # (v3.7.12 per-profile guard), which calls ``_append_log`` itself.
        assert tab.after.call_count == 1
        args = tab.after.call_args.args
        assert args[0] == 0
        assert args[1] == tab._dispatch_log_event

    def test_log_dropped_after_terminal_status(self) -> None:
        tab = self._make_log_stub()
        tab._update_status("running")
        tab._update_status("success")
        tab._on_log(message="Late event", level="info", phase="")
        assert tab.after.call_count == 0

    def test_terminal_log_passes_through_even_after_status_success(self) -> None:
        """Regression for the 2026-05-15 UI bug.

        The engine emits ``STATUS=success`` immediately before the
        final ``Backup complete: N files in X min`` LOG. On Windows
        Tk can process ``_update_status`` (flipping ``_backup_active``
        to False) BEFORE the LOG's ``after(0, _append_log)`` is even
        scheduled — silently swallowing the only row that carries
        the run duration. ``_is_terminal_log_message`` matches the
        backup-engine's terminal patterns only, so an exemption is
        cross-tab safe.
        """
        tab = self._make_log_stub()
        tab._update_status("running")
        tab._update_status("success")  # `_backup_active` is now False
        tab._on_log(
            message="Backup complete: 265552 files in 73.1 min",
            level="info",
            phase="",
        )
        # The terminal line MUST be appended despite the gate. ``_on_log``
        # defers to ``_dispatch_log_event``, whose first payload arg is the
        # message (forwarded to ``_append_log`` after the per-profile check).
        assert tab.after.call_count == 1
        args = tab.after.call_args.args
        assert args[0] == 0
        assert args[1] == tab._dispatch_log_event
        assert args[2] == "Backup complete: 265552 files in 73.1 min"

    def test_failed_terminal_log_also_passes_through(self) -> None:
        """Same exemption applies to the failure variant."""
        tab = self._make_log_stub()
        tab._update_status("running")
        tab._update_status("error")
        tab._on_log(message="Backup failed: disk full", level="error", phase="")
        assert tab.after.call_count == 1
