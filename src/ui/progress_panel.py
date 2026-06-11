"""In-app inline progress panel for long-running operations.

Replacement for the legacy ``tk.Toplevel``-based modal that used to
show backup-deletion progress (``delete_progress_dialog.py``, removed
in 3.7.33). The inline panel renders as a full-screen ``ttk.Frame``
inside the existing main window — same pattern as
:func:`src.ui.confirm_panel.confirm_inline` and
:meth:`BackupManagerApp._show_about` — so the user never sees a
system pop-up.

Design notes:

- **Non-blocking**: unlike ``confirm_inline``, this panel does NOT
  use ``wait_variable``. The caller spawns a worker, holds the
  ``InlineProgressPanel`` handle, calls ``update()`` from the
  worker thread, and calls ``complete()`` when the work is done.
  The panel auto-destroys after a 500 ms "100 %" hold so the user
  sees the success state before the layout snaps back.
- **Thread-safe API**: every public method (``update``, ``complete``,
  ``destroy``) marshals onto the Tk main thread via
  ``parent.after(0, ...)``. Tk widget mutations from a worker
  thread are undefined behaviour and produce sporadic crashes on
  Windows; this contract is unchanged from the legacy dialog.
- **Idempotent teardown**: ``_closed`` flag stops the second of two
  near-simultaneous ``destroy`` calls (timer auto-close + caller
  explicit close) from raising on an already-destroyed widget.
"""

from __future__ import annotations

import contextlib
import logging
import tkinter as tk
from collections.abc import Callable
from tkinter import ttk

from src.ui.theme import Fonts, Spacing

logger = logging.getLogger(__name__)


# Hold the "100 % / Deletion complete" state on screen for half a
# second after the worker reports done. Long enough that the user
# clearly sees the success, short enough that the workflow keeps
# its momentum. Public so callers can defer their post-completion
# actions until AFTER the panel has actually been destroyed.
_COMPLETION_HOLD_MS = 500

# Max characters of the "Deleting: <name>" line before truncation
# with a leading ellipsis. Beyond this the panel width would jump
# every time a long name lands in the worker callback.
_NAME_TRUNCATE_AT = 80


class InlineProgressPanel:
    """Full-screen inline progress UI for a long-running background job.

    Lifecycle:

    1. ``__init__(parent_frame, ..., title)`` hides the main layout
       via ``hide_callback`` and renders the panel. Use the public
       constant ``COMPLETION_HOLD_MS`` to size your post-completion
       deferral.
    2. ``update(current, total, name)`` is called from the worker
       thread for each item; the bar + counter refresh.
    3. ``complete()`` is called once the worker finishes; the bar
       snaps to 100 %, the title switches to ``completion_title``,
       and the panel destroys itself after ``_COMPLETION_HOLD_MS``.
    4. ``destroy()`` is exposed for error-path force-close. Safe to
       call multiple times.
    """

    COMPLETION_HOLD_MS: int = _COMPLETION_HOLD_MS

    def __init__(
        self,
        parent_frame: tk.Misc,
        *,
        title: str = "Working…",
        completion_title: str = "Done",
        hide_callback: Callable[[], None] | None = None,
        restore_callback: Callable[[], None] | None = None,
    ) -> None:
        if parent_frame is None:
            raise TypeError("parent_frame must not be None")
        if not isinstance(title, str) or not title.strip():
            raise ValueError(f"title must be a non-empty string, got {title!r}")
        if not isinstance(completion_title, str) or not completion_title.strip():
            raise ValueError(
                f"completion_title must be a non-empty string, got {completion_title!r}"
            )

        self._parent = parent_frame
        self._completion_title = completion_title
        self._restore_callback = restore_callback
        self._closed = False
        self._hide_called = False

        if hide_callback is not None:
            try:
                hide_callback()
                self._hide_called = True
            except Exception:
                logger.debug("hide_callback raised — proceeding anyway", exc_info=True)

        self._panel = ttk.Frame(parent_frame)
        self._panel.pack(fill="both", expand=True)
        self._build_widgets(title)

    def _build_widgets(self, title: str) -> None:
        """Build the centred title + progress bar + counter."""
        centre = ttk.Frame(self._panel)
        centre.place(relx=0.5, rely=0.5, anchor="center")

        self._title_label = ttk.Label(
            centre,
            text=title,
            anchor="center",
            font=Fonts.title(),
        )
        self._title_label.pack(pady=(0, Spacing.LARGE))

        self._progressbar = ttk.Progressbar(
            centre,
            length=480,
            mode="determinate",
        )
        self._progressbar.pack(pady=(0, Spacing.MEDIUM))

        self._counter_label = ttk.Label(
            centre,
            text="",
            anchor="center",
            font=Fonts.normal(),
        )
        self._counter_label.pack()

    # ------------------------------------------------------------------
    # Public API — safe to call from a worker thread.
    # ------------------------------------------------------------------

    def update(self, current: int, total: int, name: str) -> None:
        """Refresh the progress bar from a worker thread.

        Args:
            current: 1-based index of the item being processed.
            total: Precomputed grand total. ``0`` is tolerated and
                leaves the bar in its initial 0 % state.
            name: Item name shown in the "Working on: …" line.
        """
        # Tk torn down (test shutdown, app quit) raises RuntimeError or
        # TclError here. The worker can safely keep running — the UI
        # just won't reflect it.
        with contextlib.suppress(RuntimeError, tk.TclError):
            self._parent.after(0, self._do_update, current, total, name)

    def complete(self) -> None:
        """Snap to 100 %, switch title to completion, schedule destroy."""
        with contextlib.suppress(RuntimeError, tk.TclError):
            self._parent.after(0, self._do_complete)

    def destroy(self) -> None:
        """Force-close the panel. Idempotent."""
        try:
            self._parent.after(0, self._do_destroy)
        except (RuntimeError, tk.TclError):
            # Last-ditch: synchronous destroy on the current thread.
            # Best-effort — Tk may be gone entirely.
            self._do_destroy()

    # ------------------------------------------------------------------
    # Tk-main-thread private helpers.
    # ------------------------------------------------------------------

    def _do_update(self, current: int, total: int, name: str) -> None:
        if self._closed:
            return
        if total > 0:
            self._progressbar["maximum"] = total
            self._progressbar["value"] = current
        display_name = self._truncate_name(name)
        self._title_label.configure(text=f"Working on: {display_name}")
        self._counter_label.configure(text=f"{current} / {total}")

    @staticmethod
    def _truncate_name(name: str) -> str:
        """Cap the name length so the label width does not jump."""
        if len(name) <= _NAME_TRUNCATE_AT:
            return name
        # Keep the tail (more discriminating than the head for paths)
        # and prepend an ellipsis.
        return f"…{name[-(_NAME_TRUNCATE_AT - 1):]}"

    def _do_complete(self) -> None:
        if self._closed:
            return
        max_value = self._progressbar["maximum"] or 1
        self._progressbar["value"] = max_value
        self._title_label.configure(text=self._completion_title)
        self._counter_label.configure(text="")
        self._parent.after(_COMPLETION_HOLD_MS, self._do_destroy)

    def _do_destroy(self) -> None:
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(tk.TclError):
            self._panel.destroy()
        if self._restore_callback is not None and self._hide_called:
            try:
                self._restore_callback()
            except Exception:
                logger.debug(
                    "restore_callback raised — UI may be in a partial state",
                    exc_info=True,
                )
