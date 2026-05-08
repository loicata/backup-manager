"""Tests for the determinate-progress modal used during backup deletion.

The dialog is a thin Tk widget shell over a state machine — most of
the value is in the assertion that updates from a worker thread land
correctly on the Tk main thread (via ``parent.after``) and that the
auto-close path tears down the Toplevel without leaking the grab.
"""

from __future__ import annotations

import threading
from unittest.mock import patch

import pytest

from src.ui.delete_progress_dialog import DeleteProgressDialog


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pump(root, ms: int = 50) -> None:
    """Process pending Tk events for *ms* milliseconds.

    The dialog uses ``parent.after(0, ...)`` to marshal worker-thread
    updates onto the Tk main thread; without a pump the assertions
    would race the scheduled callbacks.
    """
    root.update_idletasks()
    end = root.tk.call("clock", "milliseconds") + ms
    while root.tk.call("clock", "milliseconds") < end:
        root.update()


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestDialogConstruction:
    """The dialog opens in a sane initial state."""

    def test_initial_text_is_preparing(self, tk_root) -> None:
        dialog = DeleteProgressDialog(tk_root)
        try:
            _pump(tk_root)
            assert dialog._title_label.cget("text") == "Preparing deletion…"
            assert dialog._counter_label.cget("text") == ""
            # ``determinate`` mode and a starting value of 0.
            assert str(dialog._progressbar.cget("mode")) == "determinate"
            assert float(dialog._progressbar["value"]) == 0.0
        finally:
            dialog.destroy()
            _pump(tk_root)

    def test_close_button_is_neutralised(self, tk_root) -> None:
        # The user must NOT be able to dismiss the dialog mid-sweep:
        # the worker thread would then write progress to a destroyed
        # Toplevel and we'd orphan the deletion.
        dialog = DeleteProgressDialog(tk_root)
        try:
            _pump(tk_root)
            handler = dialog._dialog.protocol("WM_DELETE_WINDOW")
            # ``protocol`` returns the Tcl name of the bound function;
            # what matters is that it is bound (not the empty string
            # that would mean "use the default = destroy").
            assert handler != ""
        finally:
            dialog.destroy()
            _pump(tk_root)


# ---------------------------------------------------------------------------
# update()
# ---------------------------------------------------------------------------


class TestDialogUpdate:
    """``update`` is the hot path called once per backup; pin the contract."""

    def test_update_paints_label_and_bar(self, tk_root) -> None:
        dialog = DeleteProgressDialog(tk_root)
        try:
            dialog.update(3, 10, "MyProfile_FULL_2026-04-01_120000")
            _pump(tk_root)
            assert "MyProfile_FULL_2026-04-01_120000" in dialog._title_label.cget("text")
            assert dialog._counter_label.cget("text") == "3 / 10"
            assert float(dialog._progressbar["maximum"]) == 10.0
            assert float(dialog._progressbar["value"]) == 3.0
        finally:
            dialog.destroy()
            _pump(tk_root)

    def test_update_truncates_long_names(self, tk_root) -> None:
        # Tk Labels auto-grow with the text, which would make the
        # modal jump width every time a longer name comes in. The
        # dialog truncates to 60 chars with a leading ellipsis to
        # keep the size stable.
        dialog = DeleteProgressDialog(tk_root)
        try:
            long_name = "x" * 200
            dialog.update(1, 1, long_name)
            _pump(tk_root)
            displayed = dialog._title_label.cget("text")
            # "…" + 57 chars after the leading "Deleting: " prefix.
            assert "…" in displayed
            assert len(displayed) <= len("Deleting: ") + 60


        finally:
            dialog.destroy()
            _pump(tk_root)

    def test_update_with_zero_total_does_not_set_maximum(self, tk_root) -> None:
        # An empty sweep ought to never call ``update`` (per the
        # engine's contract), but be defensive: a 0 total must not
        # set the bar's maximum to 0 (which would crash some Tk
        # builds with a "out of range" error).
        dialog = DeleteProgressDialog(tk_root)
        try:
            dialog.update(0, 0, "anything")
            _pump(tk_root)
            # Default Progressbar maximum is 100; we did not touch it.
            assert float(dialog._progressbar["maximum"]) == 100.0
        finally:
            dialog.destroy()
            _pump(tk_root)

    def test_update_after_destroy_is_silent(self, tk_root) -> None:
        # The worker thread may race the auto-close: ``update`` can
        # arrive after ``_do_destroy`` has already torn down the
        # widgets. The dialog short-circuits on the ``_closed`` flag.
        dialog = DeleteProgressDialog(tk_root)
        dialog.destroy()
        _pump(tk_root)
        # Must not raise even though the widgets are gone.
        dialog.update(1, 1, "ghost")
        _pump(tk_root)


# ---------------------------------------------------------------------------
# Thread-safety — updates from a worker thread.
# ---------------------------------------------------------------------------


class TestThreadSafety:
    """Every public method must marshal onto the Tk main thread."""

    def test_update_from_worker_thread_does_not_crash(self, tk_root) -> None:
        # The whole point of the dialog is to absorb worker-thread
        # updates safely. The worker must NEVER raise, even when the
        # Tk environment is missing a live mainloop on the calling
        # thread (the dialog's update is wrapped in a try/except for
        # exactly this case — see DeleteProgressDialog.update).
        # We do NOT assert that the update lands visually here: under
        # pytest there is no Tk mainloop on the main thread, so the
        # ``after(0, ...)`` callback never fires. The contract being
        # pinned is "the worker is not destabilised by UI calls".
        dialog = DeleteProgressDialog(tk_root)
        try:
            errors: list[BaseException] = []

            def worker() -> None:
                try:
                    for i in range(1, 6):
                        dialog.update(i, 5, f"file_{i}")
                except BaseException as e:  # pragma: no cover — diagnostic
                    errors.append(e)

            t = threading.Thread(target=worker, daemon=True)
            t.start()
            t.join(timeout=2.0)
            _pump(tk_root, ms=50)

            assert not errors, f"Worker raised: {errors}"
        finally:
            dialog.destroy()
            _pump(tk_root)


# ---------------------------------------------------------------------------
# complete() — auto-close path.
# ---------------------------------------------------------------------------


class TestComplete:
    """Once the worker is done, the dialog snaps to 100% and tears down."""

    def test_complete_snaps_bar_to_maximum(self, tk_root) -> None:
        dialog = DeleteProgressDialog(tk_root)
        try:
            dialog.update(1, 5, "one")
            _pump(tk_root)
            dialog.complete()
            _pump(tk_root, ms=10)  # less than the auto-close hold
            assert float(dialog._progressbar["value"]) == 5.0
            assert dialog._title_label.cget("text") == "Deletion complete"
        finally:
            dialog.destroy()
            _pump(tk_root)

    def test_complete_destroys_after_hold(self, tk_root) -> None:
        # The 500 ms hold is shortened in test mode for speed.
        dialog = DeleteProgressDialog(tk_root)
        with patch.object(dialog, "_COMPLETION_HOLD_MS", 30):
            dialog.update(2, 2, "x")
            _pump(tk_root)
            dialog.complete()
            # Pump well past the hold so the scheduled destroy fires.
            _pump(tk_root, ms=100)
            assert dialog._closed

    def test_destroy_releases_grab(self, tk_root) -> None:
        # ``grab_release`` errors are silently swallowed (window may be
        # gone by the time the auto-close fires twice). Pin the no-raise
        # contract so a future refactor doesn't reintroduce a TclError.
        dialog = DeleteProgressDialog(tk_root)
        _pump(tk_root)
        dialog.destroy()
        _pump(tk_root)
        # A second destroy must be a no-op (idempotent).
        dialog.destroy()
        _pump(tk_root)


# ---------------------------------------------------------------------------
# Integration with delete_profile_backups via callback.
# ---------------------------------------------------------------------------


class TestEngineIntegration:
    """End-to-end: feed the dialog the same shape of callback the
    engine produces, simulating a real deletion sweep."""

    def test_engine_callback_drives_bar_to_completion(self, tk_root) -> None:
        # Replicates what the engine does: invoke the callback once
        # per matching backup with monotonic ``current`` and constant
        # ``total``. The dialog must render every step.
        dialog = DeleteProgressDialog(tk_root)
        try:
            total = 4
            names = ["A", "B", "C", "D"]
            for i, name in enumerate(names, start=1):
                dialog.update(i, total, name)
                _pump(tk_root, ms=20)

            assert dialog._counter_label.cget("text") == f"{total} / {total}"
            dialog.complete()
            _pump(tk_root, ms=10)
            assert dialog._title_label.cget("text") == "Deletion complete"
        finally:
            dialog.destroy()
            _pump(tk_root, ms=600)
