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
    _NOTIFY_VARIANTS,
    _validate_args,
    _validate_notify_args,
    confirm_inline,
    notify_inline,
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


class TestButtonVisualParity:
    """Regression: Cancel and Confirm must have IDENTICAL visual weight.

    History of the bug this protects against:

    - **v3.7.30**: destructive confirm built as
      ``ttk.Button(style="Danger.TButton")`` rendered INVISIBLE at
      rest. sv_ttk's image-sprite layout for ``ttk.Button`` ignores
      ``style.configure(..., background=...)`` on custom styles.

    - **v3.7.31**: fixed the visibility by switching the destructive
      confirm to ``tk.Button``. BUT now Cancel was still ``ttk.Button``
      (height ~28px) and Delete was ``tk.Button`` (height ~46px). The
      asymmetry looked unprofessional.

    - **v3.7.32** (this): both Cancel and Confirm are ``tk.Button``
      with IDENTICAL ``padx`` / ``pady`` / ``font`` / ``relief``.
      Only the colours differ.

    These tests pin the contract so a future refactor cannot silently
    re-introduce either regression.
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

    def test_both_buttons_are_tk_buttons_not_ttk(self, panel_host):
        """Cancel + Confirm must both be tk.Button for size parity."""
        captured: dict = {"cancel": None, "confirm": None}

        def inspect_then_cancel():
            captured["cancel"] = self._find_button_with_text(panel_host, "Cancel")
            captured["confirm"] = self._find_button_with_text(panel_host, "Delete")
            _click_first_button_with_text(panel_host, "Cancel")

        panel_host.after(50, inspect_then_cancel)
        confirm_inline(
            panel_host,
            title="Delete profile 'X'?",
            body="Body",
            confirm_label="Delete",
            destructive=True,
        )

        for role in ("cancel", "confirm"):
            btn = captured[role]
            assert btn is not None, f"{role} button not found in panel"
            assert isinstance(btn, tk.Button) and not isinstance(btn, ttk.Button), (
                f"{role} must be tk.Button (not ttk.Button) so both buttons "
                f"share the same default padding — got {type(btn).__name__}. "
                f"Mixing ttk and tk gives them visibly different heights."
            )

    def test_cancel_and_confirm_share_identical_geometry(self, panel_host):
        """The two buttons must share every size-affecting option.

        Values are captured INSIDE the after-callback (before the panel
        is destroyed). Holding a widget reference after destruction
        then calling ``cget`` raises ``TclError: invalid command name``.
        """
        geometry_keys = ("padx", "pady", "relief", "borderwidth", "font")
        captured: dict = {"cancel": {}, "confirm": {}}

        def inspect_then_cancel():
            cancel_btn = self._find_button_with_text(panel_host, "Cancel")
            confirm_btn = self._find_button_with_text(panel_host, "Delete")
            if cancel_btn is not None:
                captured["cancel"] = {k: str(cancel_btn.cget(k)) for k in geometry_keys}
            if confirm_btn is not None:
                captured["confirm"] = {k: str(confirm_btn.cget(k)) for k in geometry_keys}
            _click_first_button_with_text(panel_host, "Cancel")

        panel_host.after(50, inspect_then_cancel)
        confirm_inline(
            panel_host,
            title="Delete profile 'X'?",
            body="Body",
            confirm_label="Delete",
            destructive=True,
        )

        assert captured["cancel"], "Cancel button geometry not captured"
        assert captured["confirm"], "Confirm button geometry not captured"
        for opt in geometry_keys:
            assert captured["cancel"][opt] == captured["confirm"][opt], (
                f"Cancel and Confirm differ on {opt!r}: "
                f"cancel={captured['cancel'][opt]!r} vs "
                f"confirm={captured['confirm'][opt]!r}. They must share "
                f"size-affecting options or one button will look smaller "
                f"than the other."
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

    def test_non_destructive_confirm_has_accent_background(self, panel_host):
        """Non-destructive confirm uses the blue accent (was Accent.TButton)."""
        from src.ui.theme import Colors

        captured = {"bg": None}

        def inspect_then_cancel():
            btn = self._find_button_with_text(panel_host, "Run next")
            if btn is not None:
                captured["bg"] = btn.cget("bg")
            _click_first_button_with_text(panel_host, "Cancel")

        panel_host.after(50, inspect_then_cancel)
        confirm_inline(
            panel_host,
            title="Run next?",
            body="Body",
            confirm_label="Run next",
            destructive=False,
        )

        assert captured["bg"] == Colors.ACCENT, (
            f"Non-destructive confirm bg must be Colors.ACCENT ({Colors.ACCENT}), "
            f"got {captured['bg']!r}."
        )

    def test_cancel_has_neutral_background(self, panel_host):
        """Cancel uses a white-ish bg with a border, never red or blue."""
        from src.ui.theme import Colors

        captured = {"bg": None}

        def inspect_then_cancel():
            btn = self._find_button_with_text(panel_host, "Cancel")
            if btn is not None:
                captured["bg"] = btn.cget("bg")
            _click_first_button_with_text(panel_host, "Cancel")

        panel_host.after(50, inspect_then_cancel)
        confirm_inline(
            panel_host,
            title="T",
            body="Body",
            confirm_label="OK",
            destructive=True,
        )

        assert captured["bg"] not in (Colors.DANGER, Colors.ACCENT), (
            f"Cancel must not share the destructive/accent colour. The user "
            f"should be able to tell at a glance which button is the safe one. "
            f"Got {captured['bg']!r}."
        )
        assert captured["bg"] == Colors.CARD_BG, (
            f"Cancel bg should be Colors.CARD_BG (white-ish), got {captured['bg']!r}"
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


# =====================================================================
# notify_inline tests (added 3.7.34 — single-button acknowledgement
# panel that replaced the toast system).
# =====================================================================


class TestNotifyValidation:
    """``_validate_notify_args`` rejects bad input before touching the UI."""

    def test_none_parent_raises(self):
        with pytest.raises(TypeError):
            _validate_notify_args(None, "T", "B", "OK", "info")

    def test_empty_title_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, "", "B", "OK", "info")

    def test_empty_body_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, "T", "", "OK", "info")

    def test_empty_button_label_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, "T", "B", "", "info")

    def test_unknown_level_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, "T", "B", "OK", "fatal")

    def test_non_string_title_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, 42, "B", "OK", "info")  # type: ignore[arg-type]


class TestNotifyEndToEnd:
    """``notify_inline`` returns only after the user clicks OK."""

    def test_ok_click_dismisses_and_returns(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))

        result = notify_inline(
            panel_host,
            title="Saved",
            body="Profile 'L2' was saved successfully.",
            level="success",
        )

        # Return value is intentionally None — the user has no
        # decision to make.
        assert result is None

    def test_custom_button_label_is_used(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "Got it"))

        notify_inline(
            panel_host,
            title="Notice",
            body="Body",
            level="info",
            button_label="Got it",
        )
        # Reaching here without a hang means the custom-labelled
        # button was found and clicked.

    def test_panel_destroyed_after_dismiss(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))

        before = set(panel_host.winfo_children())
        notify_inline(panel_host, title="T", body="B", level="info")
        after = set(panel_host.winfo_children())

        assert (after - before) == set(), (
            "notify_inline left a widget under the host — the panel must "
            "fully clean up on dismissal"
        )

    def test_escape_dismisses_too(self, panel_host):
        """Escape works as a dismissal accelerator."""

        def press_escape():
            panel_host.event_generate("<Escape>")

        panel_host.after(50, press_escape)
        notify_inline(panel_host, title="T", body="B", level="info")
        # If escape didn't dismiss, the test would hang.

    def test_return_dismisses_too(self, panel_host):
        """Return / Enter works as a dismissal accelerator."""

        def press_return():
            panel_host.event_generate("<Return>")

        panel_host.after(50, press_return)
        notify_inline(panel_host, title="T", body="B", level="info")


class TestNotifyVariantStyling:
    """Each level paints its icon with the level-specific colour."""

    @staticmethod
    def _find_icon_label(root: tk.Misc, expected_icon: str):
        """Locate the header icon label by its glyph (✓ / ℹ / ⚠ / ⛔)."""
        for w in _walk(root):
            if isinstance(w, ttk.Label) and str(w.cget("text")) == expected_icon:
                return w
        return None

    @pytest.mark.parametrize("level", ["success", "info", "warning", "error"])
    def test_each_level_icon_uses_variant_colour(self, panel_host, level):
        captured = {"fg": None}
        expected_icon = _NOTIFY_VARIANTS[level]["icon"]

        def inspect_then_ok():
            icon = self._find_icon_label(panel_host, expected_icon)
            if icon is not None:
                captured["fg"] = str(icon.cget("foreground"))
            _click_first_button_with_text(panel_host, "OK")

        panel_host.after(50, inspect_then_ok)
        notify_inline(panel_host, title="T", body="B", level=level)

        expected = _NOTIFY_VARIANTS[level]["icon_color"]
        assert captured["fg"] == expected, (
            f"level={level!r} icon ({expected_icon!r}) must use colour "
            f"{expected!r}, got {captured['fg']!r}"
        )


def _walk(root: tk.Misc):
    """Depth-first walk over a widget subtree (helper for variant tests)."""
    stack: list[tk.Misc] = [root]
    while stack:
        widget = stack.pop()
        yield widget
        try:
            stack.extend(widget.winfo_children())
        except tk.TclError:
            pass


class TestNotifyHideRestoreCallbacks:
    """``hide_callback`` runs before the panel; ``restore_callback`` after."""

    def test_callbacks_fired_in_order(self, panel_host):
        events: list[str] = []
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))

        notify_inline(
            panel_host,
            title="T",
            body="B",
            level="info",
            hide_callback=lambda: events.append("hide"),
            restore_callback=lambda: events.append("restore"),
        )

        assert events == ["hide", "restore"]

    def test_hide_callback_failure_does_not_abort_panel(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))

        def failing_hide():
            raise RuntimeError("simulated")

        # Must not propagate; panel still shows and user can click OK.
        notify_inline(
            panel_host,
            title="T",
            body="B",
            level="info",
            hide_callback=failing_hide,
        )
