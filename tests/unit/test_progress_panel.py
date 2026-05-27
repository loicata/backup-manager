"""Tests for the in-app inline progress panel.

Pins the contract of :class:`src.ui.progress_panel.InlineProgressPanel`
so a future refactor cannot silently break the long-running-operation
display path:

- ``update(current, total, name)`` refreshes the bar and labels.
- ``complete()`` snaps the bar to 100 % and schedules auto-destroy.
- ``destroy()`` is idempotent (safe even if already auto-destroyed).
- Worker-thread calls marshal onto the Tk main thread via ``after``.
- Long names truncate with a leading ellipsis (kept-tail) so the
  panel width does not jump per-update.
- ``hide_callback`` runs before the panel mounts; ``restore_callback``
  runs after the panel destroys.
- Validation errors are raised before any UI work.

These tests REPLACE the legacy ``test_delete_progress_dialog.py``
suite (removed in 3.7.33 along with the legacy ``Toplevel`` modal).
The contract being verified is equivalent — only the rendering
backend changed.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk

import pytest

from src.ui.progress_panel import _NAME_TRUNCATE_AT, InlineProgressPanel


@pytest.fixture
def panel_host(tk_root):
    """A blank frame inside the shared Tk root for one test."""
    frame = ttk.Frame(tk_root, width=900, height=600)
    frame.pack(fill="both", expand=True)
    tk_root.update_idletasks()
    yield frame
    frame.destroy()
    tk_root.update_idletasks()


def _walk(root: tk.Misc):
    """Depth-first iterator over a widget subtree."""
    stack: list[tk.Misc] = [root]
    while stack:
        widget = stack.pop()
        yield widget
        try:
            stack.extend(widget.winfo_children())
        except tk.TclError:
            pass


def _find_widget(root: tk.Misc, klass):
    for w in _walk(root):
        if isinstance(w, klass):
            return w
    return None


class TestConstructionValidation:
    """Bad constructor arguments must raise BEFORE touching the UI."""

    def test_none_parent_raises(self):
        with pytest.raises(TypeError):
            InlineProgressPanel(None)

    def test_empty_title_raises(self, panel_host):
        with pytest.raises(ValueError):
            InlineProgressPanel(panel_host, title="")

    def test_whitespace_title_raises(self, panel_host):
        with pytest.raises(ValueError):
            InlineProgressPanel(panel_host, title="   \t")

    def test_empty_completion_title_raises(self, panel_host):
        with pytest.raises(ValueError):
            InlineProgressPanel(panel_host, completion_title="")


class TestRenderedWidgets:
    """The panel renders a title label, a progress bar, and a counter."""

    def test_progressbar_is_built(self, panel_host):
        InlineProgressPanel(panel_host, title="Working")
        panel_host.update_idletasks()
        assert _find_widget(panel_host, ttk.Progressbar) is not None

    def test_title_appears_in_a_label(self, panel_host):
        InlineProgressPanel(panel_host, title="Custom title")
        panel_host.update_idletasks()
        labels = [w for w in _walk(panel_host) if isinstance(w, ttk.Label)]
        texts = [str(label.cget("text")) for label in labels]
        assert "Custom title" in texts


class TestUpdate:
    """``update`` refreshes the bar and the labels."""

    def test_update_sets_bar_value_and_max(self, panel_host):
        panel = InlineProgressPanel(panel_host, title="Working")
        panel.update(current=3, total=10, name="backup_007")
        panel_host.update_idletasks()
        # ``after(0, ...)`` schedules on the next idle slot; drain it.
        panel_host.update()

        bar = _find_widget(panel_host, ttk.Progressbar)
        assert bar is not None
        assert int(bar["value"]) == 3
        assert int(bar["maximum"]) == 10

    def test_update_with_zero_total_leaves_bar_untouched(self, panel_host):
        panel = InlineProgressPanel(panel_host, title="Working")
        panel.update(current=0, total=0, name="—")
        panel_host.update()
        bar = _find_widget(panel_host, ttk.Progressbar)
        # Initial 0/0 state — bar stays at 0/100 (Tk default).
        assert int(bar["value"]) == 0

    def test_update_shows_name_in_title(self, panel_host):
        panel = InlineProgressPanel(panel_host, title="Working")
        panel.update(current=2, total=5, name="backup_42")
        panel_host.update()
        labels = [str(w.cget("text")) for w in _walk(panel_host) if isinstance(w, ttk.Label)]
        assert any("backup_42" in t for t in labels)

    def test_update_shows_counter(self, panel_host):
        panel = InlineProgressPanel(panel_host, title="Working")
        panel.update(current=4, total=12, name="x")
        panel_host.update()
        labels = [str(w.cget("text")) for w in _walk(panel_host) if isinstance(w, ttk.Label)]
        assert any("4 / 12" in t for t in labels)


class TestNameTruncation:
    """Long names truncate with a leading ellipsis so width stays stable."""

    def test_long_name_is_truncated(self):
        long_name = "a/" * 80 + "tail.bin"
        truncated = InlineProgressPanel._truncate_name(long_name)
        # Always at most _NAME_TRUNCATE_AT chars (the ellipsis counts).
        assert len(truncated) <= _NAME_TRUNCATE_AT
        # The TAIL is kept (more discriminating than the head for paths).
        assert truncated.endswith("tail.bin")
        assert truncated.startswith("…")

    def test_short_name_is_returned_unchanged(self):
        name = "short_backup_001"
        assert InlineProgressPanel._truncate_name(name) == name

    def test_boundary_length_returned_unchanged(self):
        boundary = "x" * _NAME_TRUNCATE_AT
        assert InlineProgressPanel._truncate_name(boundary) == boundary


class TestComplete:
    """``complete`` snaps the bar to 100 % and schedules destroy."""

    def test_complete_snaps_bar_to_max(self, panel_host):
        panel = InlineProgressPanel(panel_host, title="Working")
        panel.update(current=2, total=8, name="x")
        panel_host.update()
        panel.complete()
        panel_host.update()

        bar = _find_widget(panel_host, ttk.Progressbar)
        assert int(bar["value"]) == int(bar["maximum"])

    def test_complete_swaps_title_to_completion_text(self, panel_host):
        panel = InlineProgressPanel(
            panel_host,
            title="Working",
            completion_title="All done!",
        )
        panel.update(current=1, total=1, name="x")
        panel_host.update()
        panel.complete()
        panel_host.update()

        labels = [str(w.cget("text")) for w in _walk(panel_host) if isinstance(w, ttk.Label)]
        assert any("All done!" in t for t in labels)


class TestDestroyIdempotence:
    """``destroy`` can fire from auto-timer + caller without raising."""

    def test_double_destroy_is_safe(self, panel_host):
        panel = InlineProgressPanel(panel_host, title="Working")
        panel.destroy()
        panel_host.update()
        # Second call must not raise.
        panel.destroy()
        panel_host.update()

    def test_destroy_removes_panel_from_host(self, panel_host):
        before = set(panel_host.winfo_children())
        panel = InlineProgressPanel(panel_host, title="Working")
        panel.destroy()
        panel_host.update()
        after = set(panel_host.winfo_children())
        assert (after - before) == set(), "Panel left a widget under the host after destroy()"

    def test_complete_eventually_destroys(self, panel_host):
        """After the COMPLETION_HOLD_MS delay, the panel is gone.

        ``panel_host.after(ms)`` without a callback is a no-op for the
        event loop: it just allocates a timer that nothing waits on.
        We instead pump the Tk event loop in a tight ``update()`` /
        ``sleep`` loop until the auto-destroy timer has had time to
        fire (its ``_do_destroy`` callback is what removes the panel).
        """
        import time

        panel = InlineProgressPanel(panel_host, title="Working")
        panel.update(current=1, total=1, name="x")
        panel_host.update()
        panel.complete()

        # Drive the Tk event loop for slightly longer than the auto-
        # destroy delay so the ``after`` callback actually runs.
        deadline = time.time() + (InlineProgressPanel.COMPLETION_HOLD_MS + 200) / 1000.0
        while time.time() < deadline:
            panel_host.update()
            time.sleep(0.01)

        residual_progress = _find_widget(panel_host, ttk.Progressbar)
        assert residual_progress is None, (
            "Progress bar still present after the auto-destroy timer "
            "fired — InlineProgressPanel.complete did not trigger "
            "the auto-close."
        )


class TestHideRestoreCallbacks:
    """``hide_callback`` runs at mount; ``restore_callback`` runs at destroy."""

    def test_callbacks_fired_in_order(self, panel_host):
        events: list[str] = []
        panel = InlineProgressPanel(
            panel_host,
            title="Working",
            hide_callback=lambda: events.append("hide"),
            restore_callback=lambda: events.append("restore"),
        )
        panel_host.update()
        assert events == ["hide"]
        panel.destroy()
        panel_host.update()
        assert events == ["hide", "restore"]

    def test_hide_callback_failure_does_not_abort_panel(self, panel_host):
        """A raising hide_callback is logged and the panel still mounts."""

        def failing_hide():
            raise RuntimeError("simulated")

        # Must not propagate.
        panel = InlineProgressPanel(
            panel_host,
            title="Working",
            hide_callback=failing_hide,
        )
        panel_host.update()
        # Panel widgets still present.
        assert _find_widget(panel_host, ttk.Progressbar) is not None
        panel.destroy()
        panel_host.update()

    def test_restore_callback_skipped_when_hide_callback_failed(self, panel_host):
        """If hide failed we never marked ``_hide_called`` → restore is skipped.

        Symmetric protection: a hide_callback that raised may have
        left the UI in an unknown state, calling its restore partner
        would assume a layout the app never reached.
        """
        events: list[str] = []

        def failing_hide():
            raise RuntimeError("simulated")

        panel = InlineProgressPanel(
            panel_host,
            title="Working",
            hide_callback=failing_hide,
            restore_callback=lambda: events.append("restore"),
        )
        panel_host.update()
        panel.destroy()
        panel_host.update()
        assert events == [], (
            "restore_callback fired even though hide_callback raised — "
            "the two callbacks must be matched: skipping the restore "
            "when the hide failed prevents the UI ending up in an "
            "inconsistent state."
        )


class TestWorkerThreadMarshalling:
    """Calls from a non-Tk thread must marshal via ``after`` without raising.

    The legacy ``DeleteProgressDialog`` was bitten by Tk's not-thread-
    safety; pin the contract that ``update`` / ``complete`` / ``destroy``
    can ALL be called from a background thread without crashing.
    """

    def test_update_from_worker_thread_does_not_raise(self, panel_host):
        import threading

        panel = InlineProgressPanel(panel_host, title="Working")
        errors: list[BaseException] = []

        def worker():
            try:
                panel.update(current=1, total=3, name="x")
            except BaseException as e:  # pragma: no cover
                errors.append(e)

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=5)
        panel_host.update()
        assert errors == []

    def test_complete_from_worker_thread_does_not_raise(self, panel_host):
        import threading

        panel = InlineProgressPanel(panel_host, title="Working")
        panel.update(1, 1, "x")
        panel_host.update()

        errors: list[BaseException] = []

        def worker():
            try:
                panel.complete()
            except BaseException as e:  # pragma: no cover
                errors.append(e)

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=5)
        panel_host.update()
        assert errors == []
