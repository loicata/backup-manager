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


class TestKeyBindingSafety:
    """Audit 2026-06-10: <Return> must not trigger a DESTRUCTIVE confirm
    app-wide, and the bind_all key bindings must be removed after the
    panel is torn down (they live on Tk's 'all' bindtag and survive
    destroy() otherwise — every later keypress would fire a dead panel)."""

    def test_return_cancels_destructive_panel(self, panel_host):
        # Press Enter instead of clicking — a destructive panel must
        # treat Enter as Cancel (the cancel-first safety), never Delete.
        panel_host.after(50, lambda: panel_host.event_generate("<Return>"))

        result = confirm_inline(
            panel_host,
            title="Delete profile 'L2'?",
            body="This removes everything.",
            confirm_label="Delete",
            cancel_label="Cancel",
            destructive=True,
        )

        assert result.confirmed is False  # Enter did NOT confirm the delete

    def test_return_confirms_non_destructive_panel(self, panel_host):
        panel_host.after(50, lambda: panel_host.event_generate("<Return>"))

        result = confirm_inline(
            panel_host,
            title="Proceed?",
            body="A routine confirmation.",
            confirm_label="OK",
            cancel_label="Cancel",
            destructive=False,
        )

        assert result.confirmed is True  # Enter confirms when it's safe

    def test_bindings_removed_after_teardown(self, panel_host, tk_root):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "Cancel"))
        confirm_inline(
            panel_host,
            title="Title",
            body="Body",
            confirm_label="OK",
            cancel_label="Cancel",
            destructive=True,
        )

        # No stale app-level binding remains for either sequence.
        assert tk_root.bind_all("<Return>") == ""
        assert tk_root.bind_all("<Escape>") == ""

    def test_notify_bindings_removed_after_teardown(self, panel_host, tk_root):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))
        notify_inline(
            panel_host,
            title="Done",
            body="It worked.",
            button_label="OK",
            level="success",
            auto_dismiss_ms=0,  # force the OK button (no timer)
        )

        assert tk_root.bind_all("<Return>") == ""
        assert tk_root.bind_all("<Escape>") == ""


# =====================================================================
# notify_inline tests (added 3.7.34 — single-button acknowledgement
# panel that replaced the toast system).
# =====================================================================


class TestNotifyValidation:
    """``_validate_notify_args`` rejects bad input before touching the UI."""

    def test_none_parent_raises(self):
        with pytest.raises(TypeError):
            _validate_notify_args(None, "T", "B", "OK", "info", None)

    def test_empty_title_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, "", "B", "OK", "info", None)

    def test_empty_body_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, "T", "", "OK", "info", None)

    def test_empty_button_label_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, "T", "B", "", "info", None)

    def test_unknown_level_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, "T", "B", "OK", "fatal", None)

    def test_non_string_title_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, 42, "B", "OK", "info", None)  # type: ignore[arg-type]

    def test_negative_auto_dismiss_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, "T", "B", "OK", "info", -100)

    def test_non_int_auto_dismiss_raises(self, panel_host):
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, "T", "B", "OK", "info", "2500")  # type: ignore[arg-type]

    def test_bool_auto_dismiss_raises(self, panel_host):
        """Booleans are ints in Python — reject them explicitly."""
        with pytest.raises(ValueError):
            _validate_notify_args(panel_host, "T", "B", "OK", "info", True)  # type: ignore[arg-type]


class TestNotifyClickToDismiss:
    """Warning / error levels MUST render an OK button and wait for it."""

    def test_warning_renders_ok_button(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))

        result = notify_inline(
            panel_host,
            title="Validation error",
            body="A profile named 'L2' already exists.",
            level="warning",
        )
        # Return value is intentionally None — the user has no
        # decision to make.
        assert result is None

    def test_error_renders_ok_button(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))

        notify_inline(
            panel_host,
            title="Configuration invalid",
            body="Could not validate destination.",
            level="error",
        )

    def test_custom_button_label_is_used_on_warning(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "Got it"))

        notify_inline(
            panel_host,
            title="Notice",
            body="Body",
            level="warning",
            button_label="Got it",
        )

    def test_panel_destroyed_after_click(self, panel_host):
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))

        before = set(panel_host.winfo_children())
        notify_inline(panel_host, title="T", body="B", level="warning")
        after = set(panel_host.winfo_children())

        assert (after - before) == set(), (
            "notify_inline left a widget under the host — the panel must "
            "fully clean up on dismissal"
        )

    def test_escape_dismisses_warning(self, panel_host):
        """Escape works as a dismissal accelerator even with the OK button."""

        def press_escape():
            panel_host.event_generate("<Escape>")

        panel_host.after(50, press_escape)
        notify_inline(panel_host, title="T", body="B", level="warning")

    def test_return_dismisses_warning(self, panel_host):
        """Return / Enter works as a dismissal accelerator."""

        def press_return():
            panel_host.event_generate("<Return>")

        panel_host.after(50, press_return)
        notify_inline(panel_host, title="T", body="B", level="warning")


class TestNotifyAutoDismiss:
    """Success / info auto-dismiss; no OK button is rendered."""

    @staticmethod
    def _has_button_with_text(root: tk.Misc, text: str) -> bool:
        stack: list[tk.Misc] = [root]
        while stack:
            widget = stack.pop()
            if isinstance(widget, (ttk.Button, tk.Button)) and widget.cget("text") == text:
                return True
            try:
                stack.extend(widget.winfo_children())
            except tk.TclError:
                pass
        return False

    def test_success_renders_no_ok_button(self, panel_host):
        """Success uses auto-dismiss, no button to click."""
        captured = {"has_button": None}

        def inspect_then_finish():
            captured["has_button"] = self._has_button_with_text(panel_host, "OK")
            panel_host.event_generate("<Escape>")  # early dismiss

        panel_host.after(50, inspect_then_finish)
        notify_inline(panel_host, title="Saved", body="OK", level="success")

        assert captured["has_button"] is False, (
            "Success-level notify_inline must NOT render an OK button — "
            "the panel auto-dismisses, the user has nothing to click."
        )

    def test_info_renders_no_ok_button(self, panel_host):
        captured = {"has_button": None}

        def inspect_then_finish():
            captured["has_button"] = self._has_button_with_text(panel_host, "OK")
            panel_host.event_generate("<Escape>")

        panel_host.after(50, inspect_then_finish)
        notify_inline(panel_host, title="FYI", body="something", level="info")

        assert captured["has_button"] is False

    def test_success_auto_dismisses_on_its_own(self, panel_host):
        """No click, no key — the timer fires and the panel returns.

        Short ``auto_dismiss_ms`` override keeps the test fast.
        """
        # 100 ms is plenty for the timer to fire under pytest. We
        # do NOT schedule any user input — only the timer can
        # release wait_variable here.
        notify_inline(
            panel_host,
            title="Saved",
            body="OK",
            level="success",
            auto_dismiss_ms=100,
        )
        # Reaching here means the timer dismissed without our help.

    def test_click_anywhere_dismisses_early(self, panel_host):
        """A click on the panel itself dismisses before the timer fires."""

        def click_panel():
            # Find the panel (a ttk.Frame direct child of panel_host),
            # then synthesise a click on it.
            for child in panel_host.winfo_children():
                if isinstance(child, ttk.Frame):
                    child.event_generate("<Button-1>")
                    return

        # 30 s timer would otherwise hang the test if the click did
        # NOT dismiss — so a passing test proves the click worked.
        panel_host.after(50, click_panel)
        notify_inline(
            panel_host,
            title="Saved",
            body="OK",
            level="success",
            auto_dismiss_ms=30_000,
        )

    def test_warning_default_does_not_auto_dismiss(self, panel_host):
        """A warning must NOT vanish on its own — user must acknowledge."""
        import time

        # Schedule an Escape after 200 ms so the test eventually
        # finishes even if the assertion below holds. The point:
        # the warning panel was STILL there when we measured at 150 ms.
        result_when_measured = {"panel_still_present": None}

        def measure():
            # If the panel auto-dismissed, the centre frame would
            # be gone. We just check there are still children.
            result_when_measured["panel_still_present"] = bool(panel_host.winfo_children())

        def dismiss():
            panel_host.event_generate("<Escape>")

        panel_host.after(150, measure)
        panel_host.after(300, dismiss)
        start = time.monotonic()
        notify_inline(panel_host, title="X", body="Y", level="warning")
        elapsed = time.monotonic() - start

        assert result_when_measured["panel_still_present"] is True, (
            "warning-level notify_inline must NOT auto-dismiss — "
            "the user has to acknowledge an error / warning"
        )
        # And the wait took at least until the Escape (~300 ms).
        assert elapsed >= 0.25

    def test_caller_override_to_zero_forces_click_to_dismiss(self, panel_host):
        """``auto_dismiss_ms=0`` cancels the per-level default."""
        captured = {"has_button": None}

        def inspect_then_finish():
            captured["has_button"] = self._has_button_with_text(panel_host, "OK")
            panel_host.event_generate("<Escape>")

        panel_host.after(50, inspect_then_finish)
        notify_inline(
            panel_host,
            title="Saved",
            body="OK",
            level="success",
            auto_dismiss_ms=0,  # cancel the success default
        )
        assert captured["has_button"] is True, (
            "auto_dismiss_ms=0 must force click-to-dismiss → the OK "
            "button MUST be rendered even on a success-level call"
        )


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

        def inspect_then_dismiss():
            icon = self._find_icon_label(panel_host, expected_icon)
            if icon is not None:
                captured["fg"] = str(icon.cget("foreground"))
            # Dismiss via Escape — works for both modes (click-to-
            # dismiss with the OK button, and auto-dismiss without).
            panel_host.event_generate("<Escape>")

        panel_host.after(50, inspect_then_dismiss)
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
        # warning level has the OK button — easy to click in the test.
        panel_host.after(50, lambda: _click_first_button_with_text(panel_host, "OK"))

        notify_inline(
            panel_host,
            title="T",
            body="B",
            level="warning",
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
            level="warning",
            hide_callback=failing_hide,
        )
