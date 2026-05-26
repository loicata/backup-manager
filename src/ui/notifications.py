"""In-app toast notifications.

Replacement for ``tkinter.messagebox.showinfo`` / ``showwarning`` /
``showerror`` when the message is a transient acknowledgement that
does NOT require a user click. Toasts appear pinned to the bottom-
centre of the main window, auto-dismiss after a few seconds, and
stack vertically when several arrive within the dismissal window.

Design choices (validated 2026-05-26):

- **Position**: bottom-centre.  Mirrors Material Design snackbar and
  Windows 11 native notification placement, and sits visually close
  to the bottom-left ``Save`` button so the user sees their action's
  outcome where they just clicked.
- **Stack**: up to 3 toasts visible simultaneously, newest on top,
  pushing earlier ones upward.  Beyond 3, the oldest is dismissed
  early to make room.
- **Auto-dismiss**: 2.5 s for success / info, 5 s for transient
  errors (longer because the user is more likely to want to read
  before it disappears).  All can also be dismissed manually via the
  ``×`` button.
- **No blocking**: the calling code returns immediately, the toast
  lives on the Tk main loop via ``after``.

Not for:

- Validation errors that the user MUST act on → use the in-tab
  banner pattern (separate module, future work).
- Destructive confirmations (delete profile etc.) → use the inline
  confirmation panel (future work, Category B).
- Boot-time failures where the main app frame does not exist yet
  → keep ``tkinter.messagebox`` (DPAPI fatal, fatal exception).
"""

from __future__ import annotations

import logging
import tkinter as tk
from tkinter import ttk
from typing import Literal

from src.ui.theme import Colors, Fonts, Spacing

logger = logging.getLogger(__name__)


# Auto-dismiss timings.  Success / info messages are short
# acknowledgements ("Profile saved") so 2.5 s is enough — the user
# already knows what they clicked, the toast just confirms it.
# Errors get more time because the user may not have anticipated
# them and needs longer to read the body.
_DISMISS_MS_SUCCESS = 2500
_DISMISS_MS_INFO = 3000
_DISMISS_MS_ERROR = 5000

# Maximum number of toasts visible at once.  Beyond this, the oldest
# is force-dismissed when a new one arrives.  3 mirrors Material
# Design's recommendation and is enough for the scheduler chain
# ("L2 saved", "My Backup saved", "TestNP saved") without crowding.
_MAX_STACK = 3

# Geometry constants.  ``_TOAST_BOTTOM_MARGIN`` is the distance from
# the bottom edge of the parent.  ``_TOAST_STACK_GAP`` is the gap
# between two toasts when stacked.  ``_TOAST_MAX_WIDTH`` caps a long
# message so it stays scannable at a glance — anything longer
# probably belongs in a panel, not a toast.
_TOAST_BOTTOM_MARGIN = 24
_TOAST_STACK_GAP = 8
_TOAST_MAX_WIDTH = 520

# Toast variants — colour and dismiss duration come from this map so
# adding a new variant is one entry, not a fan-out of conditionals.
ToastLevel = Literal["success", "info", "error"]

_VARIANT_STYLES: dict[ToastLevel, dict] = {
    "success": {
        "fg": "#ffffff",
        "bg": Colors.SUCCESS,
        "icon": "✓",
        "dismiss_ms": _DISMISS_MS_SUCCESS,
    },
    "info": {
        "fg": "#ffffff",
        "bg": Colors.ACCENT,
        "icon": "ℹ",
        "dismiss_ms": _DISMISS_MS_INFO,
    },
    "error": {
        "fg": "#ffffff",
        "bg": Colors.DANGER,
        "icon": "⚠",
        "dismiss_ms": _DISMISS_MS_ERROR,
    },
}


class _Toast:
    """A single bottom-centre notification widget.

    Implemented as a child ``tk.Frame`` of the host window (rather
    than a ``Toplevel``) so it follows the window's z-order, never
    appears in the taskbar, and disappears cleanly when the main
    window is closed.  Positioned via ``place`` so it overlays the
    notebook / sidebar without disturbing the existing layout.

    The toast is fully managed by :class:`ToastManager` — direct
    construction is not part of the public API.
    """

    def __init__(
        self,
        host: tk.Misc,
        message: str,
        level: ToastLevel,
        on_dismiss,
    ) -> None:
        if not isinstance(message, str) or not message.strip():
            raise ValueError(f"Toast message must be a non-empty string, got {message!r}")
        if level not in _VARIANT_STYLES:
            raise ValueError(f"Unknown toast level: {level!r}")
        if on_dismiss is None or not callable(on_dismiss):
            raise TypeError("on_dismiss must be callable")

        self._host = host
        self._on_dismiss = on_dismiss
        self._dismiss_after_id: str | None = None
        self._dismissed = False

        style = _VARIANT_STYLES[level]
        self.frame = tk.Frame(
            host,
            bg=style["bg"],
            highlightthickness=0,
            bd=0,
        )

        # Icon column — a single character (✓ / ℹ / ⚠) acts as a
        # quick visual cue without forcing the user to read the body
        # to know whether something good or bad just happened.
        icon_label = tk.Label(
            self.frame,
            text=style["icon"],
            bg=style["bg"],
            fg=style["fg"],
            font=(Fonts.FAMILY, Fonts.SIZE_LARGE, "bold"),
            padx=Spacing.LARGE,
        )
        icon_label.pack(side="left", fill="y")

        # Body — ``wraplength`` caps the visible width so a multi-
        # line message (Modules feature status) stays inside the
        # ``_TOAST_MAX_WIDTH`` envelope and does not eat the whole
        # screen.  ``justify="left"`` keeps multi-line output
        # readable; the icon already provides the centred anchor.
        body_label = tk.Label(
            self.frame,
            text=message,
            bg=style["bg"],
            fg=style["fg"],
            font=Fonts.normal(),
            wraplength=_TOAST_MAX_WIDTH - 100,  # 100px reserved for icon + close
            justify="left",
            padx=Spacing.MEDIUM,
            pady=Spacing.LARGE,
        )
        body_label.pack(side="left", fill="both", expand=True)

        close_btn = tk.Label(
            self.frame,
            text="×",
            bg=style["bg"],
            fg=style["fg"],
            font=(Fonts.FAMILY, Fonts.SIZE_LARGE, "bold"),
            cursor="hand2",
            padx=Spacing.LARGE,
        )
        close_btn.pack(side="right", fill="y")
        close_btn.bind("<Button-1>", lambda _e: self.dismiss())

        # Schedule the auto-dismiss.  The id is stored so manual
        # dismissal can cancel the pending callback — otherwise a
        # second auto-dismiss would fire on an already-destroyed
        # widget and Tk would raise.
        self._dismiss_after_id = self.frame.after(style["dismiss_ms"], self.dismiss)

    def place_at(self, x_anchor: float, y_offset: int) -> None:
        """Place this toast inside its host using bottom-centre anchoring.

        ``x_anchor`` is in [0, 1] (typically 0.5 for centred).
        ``y_offset`` is the negative pixel distance from the bottom
        edge — larger means higher.  The manager passes the right
        offset based on the position of this toast in the stack.
        """
        self.frame.place(
            relx=x_anchor,
            rely=1.0,
            anchor="s",
            y=-y_offset,
        )

    def dismiss(self) -> None:
        """Hide and destroy the toast.  Idempotent.

        ``dismiss`` is called both from the auto-dismiss timer and
        from the close-button click, so a guard against double-
        invocation is required: the second call would attempt to
        ``destroy`` an already-destroyed widget and ``cancel`` an
        invalid ``after`` id.
        """
        if self._dismissed:
            return
        self._dismissed = True
        if self._dismiss_after_id is not None:
            try:
                self.frame.after_cancel(self._dismiss_after_id)
            except tk.TclError:
                pass
            self._dismiss_after_id = None
        try:
            self.frame.place_forget()
            self.frame.destroy()
        except tk.TclError:
            # Host already destroyed (window closed) — nothing to
            # clean up.
            pass
        # Tell the manager so it can repack the remaining stack.
        try:
            self._on_dismiss(self)
        except Exception:
            logger.debug("Toast on_dismiss callback raised", exc_info=True)


class ToastManager:
    """Coordinates a stack of toasts inside one host widget.

    One manager per main window.  Tracks the order in which toasts
    were emitted, repositions remaining toasts when one disappears,
    and enforces the max-stack cap by force-dismissing the oldest
    when a fourth arrives.

    Public API consists of three factories — ``success`` / ``info``
    / ``error`` — and a low-level ``show(message, level)`` for
    parametric callers.
    """

    def __init__(self, host: tk.Misc) -> None:
        if host is None:
            raise TypeError("ToastManager host must not be None")
        self._host = host
        # Oldest at index 0, newest at the end.  Iteration in
        # display order is bottom-up: index 0 is the lowest toast on
        # screen, last index is the highest.
        self._stack: list[_Toast] = []

    def show(self, message: str, level: ToastLevel = "info") -> None:
        """Display a toast and add it to the bottom of the stack.

        Args:
            message: Body text.  Multi-line allowed but kept short
                (under ~10 lines) — longer content does not belong
                in a toast and should use the About-style inline
                panel pattern instead.
            level: One of ``"success"`` / ``"info"`` / ``"error"``.
                Drives the background colour, icon, and auto-
                dismiss duration.
        """
        if len(self._stack) >= _MAX_STACK:
            # Make room by force-dismissing the oldest.  The
            # remaining toasts will be re-laid out by the
            # _on_dismiss callback when the oldest finishes its
            # destroy sequence — no manual rearrangement needed
            # here.
            self._stack[0].dismiss()

        toast = _Toast(
            host=self._host,
            message=message,
            level=level,
            on_dismiss=self._on_dismiss,
        )
        self._stack.append(toast)
        self._relayout()

    def success(self, message: str) -> None:
        """Shortcut for a green success toast (2.5 s)."""
        self.show(message, level="success")

    def info(self, message: str) -> None:
        """Shortcut for a blue info toast (3 s)."""
        self.show(message, level="info")

    def error(self, message: str) -> None:
        """Shortcut for a red transient-error toast (5 s).

        For PERSISTENT errors that the user must act on, use the
        in-tab banner pattern instead — a toast that disappears in
        5 s is not a good place for a "you must fix this" message.
        """
        self.show(message, level="error")

    def clear(self) -> None:
        """Dismiss every visible toast immediately.

        Called on profile switch or app shutdown so a stale
        ``Profile saved`` toast does not linger when the user is
        already looking at a different profile.
        """
        # Copy the list because ``dismiss`` mutates ``self._stack``
        # via the callback.
        for toast in list(self._stack):
            toast.dismiss()

    def _on_dismiss(self, toast: _Toast) -> None:
        """Callback fired by each toast when it finishes dismissing."""
        try:
            self._stack.remove(toast)
        except ValueError:
            # Already removed — defensive against duplicate
            # ``dismiss`` calls.
            return
        self._relayout()

    def _relayout(self) -> None:
        """Place each surviving toast at its slot in the stack.

        Iterates oldest-to-newest, with the oldest at the bottom of
        the screen and each subsequent toast stacked above it by
        approximately the height of the previous one plus a small
        gap.  Height is sampled after Tk has had a chance to
        compute it (``update_idletasks``) — otherwise newly-created
        frames report height 1 and the stack collapses.
        """
        try:
            self._host.update_idletasks()
        except tk.TclError:
            # Host destroyed mid-relayout — nothing to do.
            return

        offset = _TOAST_BOTTOM_MARGIN
        for toast in self._stack:
            toast.place_at(x_anchor=0.5, y_offset=offset)
            try:
                height = toast.frame.winfo_reqheight()
            except tk.TclError:
                height = 0
            offset += max(height, 1) + _TOAST_STACK_GAP
