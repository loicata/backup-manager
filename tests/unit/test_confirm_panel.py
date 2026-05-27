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
    """Walk the widget tree and invoke the first button matching ``text``.

    Accepts both ``ttk.Button`` (cancel + non-destructive confirm)
    AND ``tk.Button`` (destructive confirm uses the legacy tk widget
    because sv_ttk's ttk button layout ignores custom red backgrounds
    — see :mod:`src.ui.confirm_panel` for the rationale).

    Returns True on success, False if no button matched (caller is
    expected to assert).
    """
    stack: list[tk.Misc] = [root]
    while stack:
        widget = stack.pop()
        is_button = isinstance(widget, (ttk.Button, tk.Button))
        if is_button and widget.cget("text") == text:
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


class TestDestructiveButtonVisibility:
    """Regression: the destructive confirm button MUST paint red.

    Bug fixed in 3.7.31: under sv_ttk (Sun Valley theme), a
    ``ttk.Button`` with ``style="Danger.TButton"`` ignores the
    configured red ``background`` because sv_ttk's button layout
    uses image sprites. The button rendered with NO visible
    background — only on hover did the active state kick in and
    show colour. Users could not see the destructive action.

    The fix uses ``tk.Button`` (legacy widget) instead of
    ``ttk.Button`` for the destructive case so the red background
    actually paints. These tests pin that contract so a future
    refactor cannot silently re-introduce the bug.
    """

    def _find_button_with_text(self, root: tk.Misc, text: str):
        stack: list[tk.Misc] = [root]
        while stack:
            widget = stack.pop()
            if isinstance(widget, (ttk.Button, tk.Button)) and widget.cget("text") == text:
                return widget
            try:
                stack.extend(widget.winfo_children())
            except tk.TclError:
                pass
        return None

    def test_destructive_confirm_is_tk_button_not_ttk(self, panel_host):
        """Destructive confirm must be tk.Button (paints under sv_ttk)."""
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "Delete"))
        confirm_inline(
            panel_host,
            title="Delete profile 'X'?",
            body="Body",
            confirm_label="Delete",
            destructive=True,
        )
        # Re-build a fresh panel and inspect its tree before clicking.
        # (The previous panel was destroyed after confirm.)
        decision_received = {"value": None}

        def inspect_then_cancel():
            btn = self._find_button_with_text(panel_host, "Delete")
            decision_received["value"] = btn
            _click_first_button_with_text(panel_host, "Cancel")

        panel_host.after(50, inspect_then_cancel)
        confirm_inline(
            panel_host,
            title="Delete profile 'X'?",
            body="Body",
            confirm_label="Delete",
            destructive=True,
        )

        delete_btn = decision_received["value"]
        assert delete_btn is not None, "Delete button not found in the panel"
        assert isinstance(delete_btn, tk.Button) and not isinstance(delete_btn, ttk.Button), (
            f"Destructive confirm must be tk.Button (paints under sv_ttk), "
            f"got {type(delete_btn).__name__}. ttk.Button with Danger.TButton "
            f"style renders invisible under the Sun Valley theme."
        )

    def test_destructive_confirm_has_red_background(self, panel_host):
        """The red bg must be literally configured on the widget."""
        from src.ui.theme import Colors

        captured = {"bg": None}

        def inspect_then_cancel():
            btn = self._find_button_with_text(panel_host, "Delete")
            if btn is not None:
                captured["bg"] = btn.cget("bg")
            _click_first_button_with_text(panel_host, "Cancel")

        panel_host.after(50, inspect_then_cancel)
        confirm_inline(
            panel_host,
            title="Delete profile 'X'?",
            body="Body",
            confirm_label="Delete",
            destructive=True,
        )

        assert captured["bg"] == Colors.DANGER, (
            f"Destructive confirm bg must be Colors.DANGER ({Colors.DANGER}), "
            f"got {captured['bg']!r}. If this is empty / system default, the "
            f"button will render invisible on a white panel background."
        )

    def test_non_destructive_confirm_is_ttk_button(self, panel_host):
        """Non-destructive confirm stays ttk.Button (native sv_ttk Accent)."""
        captured = {"widget": None}

        def inspect_then_cancel():
            btn = self._find_button_with_text(panel_host, "Run next")
            captured["widget"] = btn
            _click_first_button_with_text(panel_host, "Cancel")

        panel_host.after(50, inspect_then_cancel)
        confirm_inline(
            panel_host,
            title="Run next?",
            body="Body",
            confirm_label="Run next",
            destructive=False,
        )

        btn = captured["widget"]
        assert btn is not None
        assert isinstance(btn, ttk.Button), (
            f"Non-destructive confirm should keep the native sv_ttk "
            f"ttk.Button (Accent.TButton style) — got {type(btn).__name__}"
        )


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
