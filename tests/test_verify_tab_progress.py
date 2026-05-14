"""Regression guard for the v3.6.2 Verify-tab progress bar fix.

In v3.6.0 the bar stayed at 0 % for the full ~10 min of a 260 k-file
local re-hash because the VerifyTab never subscribed to PROGRESS
events. v3.6.1 wired the EventBus into the IntegrityVerifier so the
underlying verify_backup phase emitted events, but no one in the tab
listened to them, so the symptom did not change.

v3.6.2 adds a PROGRESS subscriber on the tab. These tests pin the
filtering rules so a future refactor cannot silently re-introduce
the stuck-at-0% behaviour:

- Only ``phase == "verification"`` triggers a UI update; events from
  other pipeline phases (e.g. a Run-tab backup happening in parallel)
  must not move this tab's bar.
- Updates are no-op when ``_running`` is False so a stray late event
  cannot overwrite the final ``set_complete`` summary.
- The Tk widget mutation is dispatched via ``self.after(0, ...)`` to
  hop back onto the main thread, since PROGRESS events fire from the
  IntegrityVerifier worker thread.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from src.ui.tabs.verify_tab import VerifyTab


def _make_tab_stub() -> VerifyTab:
    """Build a VerifyTab without touching Tk.

    Bypassing ``__init__`` keeps the test pure-Python: no display
    server needed, no Tk root, no ttk widgets. We hand-attach the
    handful of attributes the methods under test read.
    """
    tab = VerifyTab.__new__(VerifyTab)
    tab._running = True
    tab.after = MagicMock(name="after")
    tab.progress_bar = MagicMock(name="progress_bar")
    tab.percent_label = MagicMock(name="percent_label")
    tab.status_label = MagicMock(name="status_label")
    return tab


class TestOnProgressEventFiltering:
    """Filtering rules on the incoming PROGRESS event."""

    def test_only_verification_phase_is_handled(self) -> None:
        tab = _make_tab_stub()

        tab._on_progress_event(current=10, total=100, filename="x.txt", phase="manifest")
        tab._on_progress_event(current=10, total=100, filename="x.txt", phase="write")
        tab._on_progress_event(current=10, total=100, filename="x.txt", phase="")
        assert tab.after.call_count == 0, "non-verification phases must NOT update the tab"

        tab._on_progress_event(current=10, total=100, filename="x.txt", phase="verification")
        assert tab.after.call_count == 1

    def test_ignored_when_not_running(self) -> None:
        """Stray late events must not overwrite the final summary."""
        tab = _make_tab_stub()
        tab._running = False
        tab._on_progress_event(current=10, total=100, filename="x.txt", phase="verification")
        assert tab.after.call_count == 0

    def test_ignored_when_total_is_zero_or_negative(self) -> None:
        """Heartbeat events with ``total == 0`` are not meaningful here."""
        tab = _make_tab_stub()
        tab._on_progress_event(current=0, total=0, filename="", phase="verification")
        tab._on_progress_event(current=5, total=-1, filename="x.txt", phase="verification")
        assert tab.after.call_count == 0

    def test_dispatches_to_main_thread_via_after(self) -> None:
        """The widget mutation is scheduled, not inline.

        PROGRESS is emitted from the verifier worker thread. Doing the
        widget mutation inline would race with Tk's main loop and the
        first ``progress_bar["value"] =`` call from off-thread would
        crash with ``TclError``. ``self.after(0, ...)`` queues the call
        onto the Tk thread instead.
        """
        tab = _make_tab_stub()
        tab._on_progress_event(current=37, total=100, filename="a.txt", phase="verification")

        # The call must be: self.after(0, self._apply_progress_event, 37, 100, "a.txt")
        args, _kwargs = tab.after.call_args
        assert args[0] == 0
        assert args[1] == tab._apply_progress_event
        assert args[2:] == (37, 100, "a.txt")


class TestApplyProgressEvent:
    """The actual widget mutation, once dispatched onto the Tk thread."""

    def test_caps_at_99_so_set_complete_owns_100(self) -> None:
        """Going to 100 % mid-stream would steal the "all done" signal."""
        tab = _make_tab_stub()

        # Halfway through
        tab._apply_progress_event(current=50, total=100, filename="x")
        tab.progress_bar.__setitem__.assert_called_with("value", 50)
        tab.percent_label.config.assert_called_with(text="50%")

        # Last file -- still 99 % because set_complete renders the final state
        tab.progress_bar.reset_mock()
        tab.percent_label.reset_mock()
        tab._apply_progress_event(current=100, total=100, filename="x")
        tab.progress_bar.__setitem__.assert_called_with("value", 99)
        tab.percent_label.config.assert_called_with(text="99%")

    def test_status_label_updated_with_filename(self) -> None:
        tab = _make_tab_stub()
        tab._apply_progress_event(current=3, total=10, filename="dir/sub/file.bin")
        tab.status_label.config.assert_called_with(text="Verifying dir/sub/file.bin")

    def test_empty_filename_does_not_clear_status(self) -> None:
        """An empty filename mid-loop (rare but possible) must not blank
        the status line -- keep the last known filename visible."""
        tab = _make_tab_stub()
        tab._apply_progress_event(current=3, total=10, filename="")
        tab.status_label.config.assert_not_called()
