"""Tests for the bottom-centre toast notifications.

Pins the contract of :mod:`src.ui.notifications` so a future
refactor cannot silently break:

- toasts are placed at the bottom-centre of their host;
- the stack tops out at 3 visible toasts;
- when a 4th arrives, the OLDEST is force-dismissed;
- ``dismiss`` is idempotent (timer + click both call it);
- ``clear`` removes every visible toast at once;
- validation errors on ``show`` are raised before any UI work
  (empty message / unknown level / no host).

Uses the shared session-scoped ``tk_root`` fixture from
``conftest.py`` so the whole test module re-uses one Tk instance.
Avoiding fresh roots per test sidesteps the Tcl-corruption bug
documented in the conftest fixture comment.
"""

from __future__ import annotations

import tkinter as tk

import pytest

from src.ui.notifications import ToastManager, _MAX_STACK, _VARIANT_STYLES


@pytest.fixture
def toast_host(tk_root):
    """A blank frame inside the shared Tk root for one test.

    Destroyed after each test so the next one starts with a clean
    slate (no leftover toasts from a previous run).
    """
    frame = tk.Frame(tk_root, width=800, height=600)
    frame.pack()
    tk_root.update_idletasks()
    yield frame
    frame.destroy()
    tk_root.update_idletasks()


@pytest.fixture
def manager(toast_host):
    """A ToastManager bound to the per-test host frame."""
    return ToastManager(toast_host)


class TestConstruction:
    """Manager and Toast construction validation."""

    def test_manager_requires_a_host(self):
        with pytest.raises(TypeError):
            ToastManager(None)

    def test_show_rejects_empty_message(self, manager):
        with pytest.raises(ValueError):
            manager.show("")

    def test_show_rejects_whitespace_only_message(self, manager):
        with pytest.raises(ValueError):
            manager.show("   \n\t  ")

    def test_show_rejects_non_string_message(self, manager):
        with pytest.raises(ValueError):
            manager.show(42)  # type: ignore[arg-type]

    def test_show_rejects_unknown_level(self, manager):
        with pytest.raises(ValueError):
            manager.show("hello", level="catastrophic")  # type: ignore[arg-type]


class TestStackOrderAndLimit:
    """Newest toast on top, oldest force-dismissed past the cap."""

    def test_first_show_adds_to_stack(self, manager, toast_host):
        manager.show("first", level="success")
        toast_host.update_idletasks()
        assert len(manager._stack) == 1

    def test_three_consecutive_shows_all_stack(self, manager, toast_host):
        for i in range(3):
            manager.show(f"msg{i}", level="info")
        toast_host.update_idletasks()
        assert len(manager._stack) == 3

    def test_fourth_show_evicts_oldest(self, manager, toast_host):
        """``_MAX_STACK`` enforced: 4 shows → 3 visible, oldest gone."""
        first_message_text = "OLDEST"
        manager.show(first_message_text, level="info")
        manager.show("msg2", level="info")
        manager.show("msg3", level="info")
        manager.show("msg4", level="info")
        toast_host.update_idletasks()

        assert len(manager._stack) == _MAX_STACK
        # The oldest message must NOT be in any surviving toast's
        # body text — peek at the second Label of each toast frame
        # (the body label, after the icon label).
        for toast in manager._stack:
            body_label = toast.frame.winfo_children()[1]
            assert body_label.cget("text") != first_message_text

    def test_newest_is_at_top_of_screen(self, manager, toast_host):
        """The most recently added toast must sit ABOVE the others.

        ``place`` uses ``y=-offset`` with anchor=south, so a LARGER
        ``y`` offset means HIGHER on screen.  The newest toast
        (last in ``_stack``) is laid out last by ``_relayout`` and
        gets the largest offset.
        """
        manager.show("oldest", level="info")
        manager.show("middle", level="info")
        manager.show("newest", level="info")
        toast_host.update_idletasks()

        # Place info exposes y as a negative integer when anchored south.
        offsets = [int(toast.frame.place_info()["y"]) for toast in manager._stack]
        # ``offsets`` is in stack order (oldest first); each entry is
        # MORE negative (= higher on screen) than the previous one.
        assert offsets == sorted(offsets, reverse=True), (
            "Stack must place older toasts higher (more negative y) and "
            "newer ones lower (less negative y) — bottom anchor pushes "
            "the freshly added toast just above the bottom margin"
        )


class TestDismissal:
    """``dismiss`` removes the toast from the stack and is idempotent."""

    def test_dismiss_removes_from_stack(self, manager, toast_host):
        manager.show("hello", level="info")
        toast_host.update_idletasks()
        toast = manager._stack[0]
        toast.dismiss()
        toast_host.update_idletasks()
        assert toast not in manager._stack
        assert len(manager._stack) == 0

    def test_dismiss_is_idempotent(self, manager, toast_host):
        """Auto-timer + close-button could both fire dismiss."""
        manager.show("hello", level="info")
        toast = manager._stack[0]
        toast.dismiss()
        toast.dismiss()  # must not raise
        toast.dismiss()
        assert len(manager._stack) == 0

    def test_remaining_toasts_repack_after_one_dismissed(self, manager, toast_host):
        """When the middle of a stack of 3 dismisses, others move down."""
        manager.show("a", level="info")
        manager.show("b", level="info")
        manager.show("c", level="info")
        toast_host.update_idletasks()

        middle = manager._stack[1]
        offset_before = int(manager._stack[2].frame.place_info()["y"])
        middle.dismiss()
        toast_host.update_idletasks()

        # ``c`` (now at index 1) has moved closer to the bottom →
        # less-negative y than before.
        offset_after = int(manager._stack[1].frame.place_info()["y"])
        assert offset_after > offset_before, (
            "After dismissing the middle toast, the one above it must "
            "move closer to the bottom"
        )

    def test_clear_dismisses_every_visible(self, manager, toast_host):
        manager.show("a", level="info")
        manager.show("b", level="success")
        manager.show("c", level="error")
        toast_host.update_idletasks()
        assert len(manager._stack) == 3
        manager.clear()
        toast_host.update_idletasks()
        assert len(manager._stack) == 0


class TestVariantStyles:
    """Each variant carries the right bg + icon + dismiss duration."""

    def test_success_uses_green_background(self, manager, toast_host):
        manager.show("ok", level="success")
        toast_host.update_idletasks()
        toast_frame = manager._stack[0].frame
        assert toast_frame.cget("bg") == _VARIANT_STYLES["success"]["bg"]

    def test_info_uses_blue_background(self, manager, toast_host):
        manager.show("hello", level="info")
        toast_host.update_idletasks()
        toast_frame = manager._stack[0].frame
        assert toast_frame.cget("bg") == _VARIANT_STYLES["info"]["bg"]

    def test_error_uses_red_background(self, manager, toast_host):
        manager.show("oops", level="error")
        toast_host.update_idletasks()
        toast_frame = manager._stack[0].frame
        assert toast_frame.cget("bg") == _VARIANT_STYLES["error"]["bg"]

    def test_success_shortcut_matches_show_with_level(self, manager, toast_host):
        manager.success("a")
        toast_host.update_idletasks()
        assert manager._stack[0].frame.cget("bg") == _VARIANT_STYLES["success"]["bg"]

    def test_info_shortcut_matches_show_with_level(self, manager, toast_host):
        manager.info("b")
        toast_host.update_idletasks()
        assert manager._stack[0].frame.cget("bg") == _VARIANT_STYLES["info"]["bg"]

    def test_error_shortcut_matches_show_with_level(self, manager, toast_host):
        manager.error("c")
        toast_host.update_idletasks()
        assert manager._stack[0].frame.cget("bg") == _VARIANT_STYLES["error"]["bg"]

    def test_error_has_longest_dismiss_window(self):
        """Errors must live longer than info or success on screen."""
        assert _VARIANT_STYLES["error"]["dismiss_ms"] > _VARIANT_STYLES["info"]["dismiss_ms"]
        assert _VARIANT_STYLES["info"]["dismiss_ms"] >= _VARIANT_STYLES["success"]["dismiss_ms"]


class TestPlacementAnchor:
    """Toast must be anchored to the bottom-centre of the host."""

    def test_placed_at_bottom_centre(self, manager, toast_host):
        manager.show("hello", level="info")
        toast_host.update_idletasks()
        info = manager._stack[0].frame.place_info()
        assert info["anchor"] == "s", "Bottom anchor"
        assert float(info["relx"]) == pytest.approx(0.5), "Centred horizontally"
        assert float(info["rely"]) == pytest.approx(1.0), "Pinned to bottom"


class TestMultiline:
    """Long messages must wrap, not overflow."""

    def test_multiline_body_renders_without_error(self, manager, toast_host):
        long_msg = "\n".join(
            f"• feature {i}: missing dep-{i}, dep-{i + 1}, dep-{i + 2}" for i in range(5)
        )
        manager.show(long_msg, level="info")
        toast_host.update_idletasks()
        body_label = manager._stack[0].frame.winfo_children()[1]
        # The body label must carry the FULL text — toast must not
        # truncate, only wrap visually via ``wraplength``.
        assert body_label.cget("text") == long_msg
