"""Tests for the inline confirmation panel.

``confirm_inline`` is synchronous (returns the user's decision as a
value, mirroring ``askyesno``) but driven by ``wait_variable`` under
the hood — so the test pattern is:

1. Schedule the click via ``root.after(delay_ms, simulate_click)``.
2. Call ``confirm_inline`` (blocks until the scheduled click fires).
3. Assert on the returned :class:`ConfirmResult`.

Validation tests and dataclass tests run without spinning up the
panel (they exercise pure helpers).
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk

import pytest

from src.ui.confirm_panel import (
    ConfirmExtra,
    ConfirmResult,
    _validate_args,
    confirm_inline,
)


@pytest.fixture
def panel_host(tk_root):
    """A blank frame inside the shared root, destroyed per-test."""
    frame = ttk.Frame(tk_root, width=900, height=700)
    frame.pack(fill="both", expand=True)
    tk_root.update_idletasks()
    yield frame
    frame.destroy()
    tk_root.update_idletasks()


def _click_first_button_with_text(root: tk.Misc, text: str) -> bool:
    """Walk the widget tree and invoke the first ttk.Button matching ``text``.

    Returns True on success, False if no button matched (caller is
    expected to assert).  Used by tests that need to simulate the
    user clicking Confirm / Cancel without manual coordinates.
    """
    stack: list[tk.Misc] = [root]
    while stack:
        widget = stack.pop()
        if isinstance(widget, ttk.Button) and widget.cget("text") == text:
            widget.invoke()
            return True
        try:
            stack.extend(widget.winfo_children())
        except tk.TclError:
            pass
    return False


class TestValidation:
    """``_validate_args`` rejects obviously-bad calls."""

    def test_none_parent_frame_raises(self):
        with pytest.raises(TypeError):
            _validate_args(None, "title", "body", "OK", "Cancel")

    def test_empty_title_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_args(panel_host, "", "body", "OK", "Cancel")

    def test_whitespace_title_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_args(panel_host, "   ", "body", "OK", "Cancel")

    def test_empty_body_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_args(panel_host, "Title", "", "OK", "Cancel")

    def test_empty_confirm_label_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_args(panel_host, "Title", "Body", "", "Cancel")

    def test_empty_cancel_label_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_args(panel_host, "Title", "Body", "OK", "")

    def test_non_string_title_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_args(panel_host, 42, "Body", "OK", "Cancel")  # type: ignore[arg-type]


class TestDataClasses:
    """``ConfirmExtra`` / ``ConfirmResult`` carry the expected fields."""

    def test_confirm_extra_defaults(self):
        extra = ConfirmExtra(key="delete_backups", label="Also delete backups")
        assert extra.key == "delete_backups"
        assert extra.label == "Also delete backups"
        assert extra.default is False
        assert extra.hint == ""

    def test_confirm_extra_with_hint(self):
        extra = ConfirmExtra(
            key="k",
            label="L",
            default=True,
            hint="Cannot be undone",
        )
        assert extra.default is True
        assert extra.hint == "Cannot be undone"

    def test_confirm_result_carries_state(self):
        result = ConfirmResult(confirmed=True, extras={"a": True, "b": False})
        assert result.confirmed is True
        assert result.extras == {"a": True, "b": False}


class TestEndToEndClickPaths:
    """Full ``confirm_inline`` round-trip via the real Tk event loop."""

    def test_confirm_click_returns_true(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "Delete"))

        result = confirm_inline(
            panel_host,
            title="Delete profile 'L2'?",
            body="This will remove the profile.",
            confirm_label="Delete",
            cancel_label="Cancel",
            destructive=True,
        )

        assert result.confirmed is True
        assert result.extras == {}

    def test_cancel_click_returns_false(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "Cancel"))

        result = confirm_inline(
            panel_host,
            title="Delete profile 'L2'?",
            body="This will remove the profile.",
            confirm_label="Delete",
            cancel_label="Cancel",
            destructive=True,
        )

        assert result.confirmed is False

    def test_confirm_with_extras_records_checkbox_state(self, panel_host):
        # The checkbox starts unchecked (default=False) and we click
        # Delete WITHOUT toggling it — extras should report False.
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "Delete"))

        result = confirm_inline(
            panel_host,
            title="Delete profile 'L2'?",
            body="Remove the profile.",
            confirm_label="Delete",
            extras=[
                ConfirmExtra(key="delete_backups", label="Also delete backups"),
            ],
            destructive=True,
        )

        assert result.confirmed is True
        assert result.extras == {"delete_backups": False}

    def test_confirm_with_extras_records_toggled_checkbox(self, panel_host):
        """Toggle the checkbox ON then click Delete — extras must reflect it."""

        def toggle_then_confirm():
            # Walk widgets to find the Checkbutton and invoke it
            # (which toggles its var).
            stack: list[tk.Misc] = [panel_host]
            while stack:
                widget = stack.pop()
                if isinstance(widget, ttk.Checkbutton):
                    widget.invoke()
                    break
                try:
                    stack.extend(widget.winfo_children())
                except tk.TclError:
                    pass
            _click_first_button_with_text(panel_host, "Delete")

        panel_host.after(50, toggle_then_confirm)

        result = confirm_inline(
            panel_host,
            title="Delete profile 'L2'?",
            body="Remove the profile.",
            confirm_label="Delete",
            extras=[
                ConfirmExtra(key="delete_backups", label="Also delete backups"),
            ],
            destructive=True,
        )

        assert result.confirmed is True
        assert result.extras == {"delete_backups": True}

    def test_extras_default_true_preserved_on_cancel(self, panel_host):
        """Even on Cancel, the (last-known) extras state is returned."""
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "Cancel"))

        result = confirm_inline(
            panel_host,
            title="Title",
            body="Body",
            confirm_label="OK",
            extras=[
                ConfirmExtra(key="opt", label="Toggle", default=True),
            ],
        )

        # confirmed=False but the extras snapshot is what was on
        # screen at dismissal time — the default-True checkbox.
        assert result.confirmed is False
        assert result.extras == {"opt": True}


class TestHideAndRestoreCallbacks:
    """``hide_callback`` runs before the panel; ``restore_callback`` after."""

    def test_callbacks_fired_in_order(self, panel_host):
        events: list[str] = []
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))

        confirm_inline(
            panel_host,
            title="Title",
            body="Body",
            confirm_label="OK",
            hide_callback=lambda: events.append("hide"),
            restore_callback=lambda: events.append("restore"),
        )

        assert events == ["hide", "restore"]

    def test_hide_callback_failure_does_not_abort_panel(self, panel_host):
        """A raising hide_callback is logged but the panel still shows."""
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))

        def failing_hide():
            raise RuntimeError("simulated")

        # Should not propagate — the user still gets the panel and
        # can click it.
        result = confirm_inline(
            panel_host,
            title="Title",
            body="Body",
            confirm_label="OK",
            hide_callback=failing_hide,
        )
        assert result.confirmed is True

    def test_restore_callback_failure_does_not_break_return(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))

        def failing_restore():
            raise RuntimeError("simulated")

        result = confirm_inline(
            panel_host,
            title="Title",
            body="Body",
            confirm_label="OK",
            restore_callback=failing_restore,
        )
        # ConfirmResult must still be the user's decision.
        assert result.confirmed is True


class TestPanelTeardown:
    """The panel is removed from the tree after the user decides."""

    def test_panel_destroyed_after_confirm(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))

        before_children = set(panel_host.winfo_children())
        confirm_inline(
            panel_host,
            title="Title",
            body="Body",
            confirm_label="OK",
        )
        after_children = set(panel_host.winfo_children())

        # No new children remain — the panel cleaned itself up.
        new_children = after_children - before_children
        assert new_children == set(), (
            f"confirm_inline left {len(new_children)} widget(s) "
            f"under the host after dismissal"
        )

    def test_panel_destroyed_after_cancel(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "Cancel"))

        before = set(panel_host.winfo_children())
        confirm_inline(
            panel_host,
            title="Title",
            body="Body",
            confirm_label="OK",
        )
        after = set(panel_host.winfo_children())

        assert (after - before) == set()
