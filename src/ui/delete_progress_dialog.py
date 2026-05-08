"""Modal dialog with a determinate progress bar for backup deletion.

Replaces the silent "fire-and-forget" behaviour where clicking Yes on
the "Delete backups?" confirmation immediately closed the dialog and
left the user staring at a frozen-looking sidebar while the actual
deletion happened on a background thread. The dialog stays open for
the duration of the sweep, paints a 0..N progress bar driven by
``delete_profile_backups``'s progress callback, displays the file
currently being processed, and auto-closes 500 ms after the last
delete so the user can briefly see the 100 % state before the modal
disappears.

Threading
---------
``delete_profile_backups`` runs on a daemon worker thread. The progress
callback fires from THAT thread; calling Tk widget methods directly
from a non-main thread is undefined behaviour (Tcl is not thread-safe
and on Windows it produces sporadic crashes / missed redraws). Every
public method below therefore marshals onto the Tk main thread via
``parent.after(0, ...)`` before touching widget state — same pattern
as the v3.3.15 tray-callback fix.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class DeleteProgressDialog:
    """Modal Toplevel with a determinate progress bar.

    Lifecycle:

    1. ``__init__`` opens the dialog with a "Preparing…" placeholder
       and grabs input focus.
    2. ``update(current, total, name)`` is called from the worker
       thread for each backup; the dialog refreshes the bar and the
       "Deleting: <name>" label.
    3. ``complete()`` is called once the worker finishes; the bar
       jumps to 100 %, the label switches to "Deletion complete" for
       500 ms, then the dialog destroys itself.

    The window's close button (``WM_DELETE_WINDOW``) is suppressed
    while the sweep is in progress: closing it mid-deletion would
    leave the worker thread orphaned and the user staring at a
    half-cleaned destination with no way to know what was deleted
    and what wasn't.
    """

    _COMPLETION_HOLD_MS: int = 500

    def __init__(self, parent: tk.Misc) -> None:
        """Open the dialog centred on *parent* and grab input focus.

        Args:
            parent: Parent widget (typically the main window's ``root``).
                Used both for centring and for the ``after`` calls that
                marshal worker-thread updates onto the Tk main thread.
        """
        self._parent = parent
        self._closed = False

        self._dialog = tk.Toplevel(parent)
        self._dialog.title("Deleting backups")
        self._dialog.transient(parent)
        self._dialog.resizable(False, False)
        # Block the OS-level close button — the worker thread would
        # outlive the dialog and produce confusing log spam otherwise.
        self._dialog.protocol("WM_DELETE_WINDOW", self._noop_close)

        frame = ttk.Frame(self._dialog, padding=20)
        frame.pack(fill="both", expand=True)

        self._title_label = ttk.Label(
            frame,
            text="Preparing deletion…",
            anchor="w",
        )
        self._title_label.pack(fill="x", pady=(0, 8))

        self._progressbar = ttk.Progressbar(
            frame,
            length=400,
            mode="determinate",
        )
        self._progressbar.pack(fill="x")

        self._counter_label = ttk.Label(
            frame,
            text="",
            foreground="gray",
        )
        self._counter_label.pack(fill="x", pady=(8, 0))

        # Centre on parent. ``update_idletasks`` forces Tk to compute
        # the requested size before we ask for it.
        self._dialog.update_idletasks()
        self._centre_on_parent()

        # Defer ``grab_set`` until after the window is mapped and on
        # screen — calling it on an unmapped Toplevel raises
        # ``TclError: grab failed: window not viewable`` on some
        # Windows builds.
        self._dialog.after(50, self._safe_grab)

    # ------------------------------------------------------------------
    # Public API — safe to call from a worker thread.
    # ------------------------------------------------------------------

    def update(self, current: int, total: int, name: str) -> None:
        """Refresh the progress bar from a worker thread.

        Args:
            current: 1-based index of the file being deleted.
            total: Precomputed grand total across all destinations.
            name: Backup name currently being processed (shown in the
                dialog so the user can see things are moving).
        """
        # Marshal onto the Tk main thread. Tk shutdown / a missing
        # mainloop on the calling thread can raise RuntimeError or
        # TclError; swallow either one so the deletion sweep keeps
        # running even if the UI has gone away — the engine cleaning
        # the destination is more important than the progress bar.
        try:
            self._parent.after(0, self._do_update, current, total, name)
        except (RuntimeError, tk.TclError):
            pass

    def complete(self) -> None:
        """Snap the bar to 100 % and schedule the auto-close."""
        try:
            self._parent.after(0, self._do_complete)
        except (RuntimeError, tk.TclError):
            pass

    def destroy(self) -> None:
        """Force-close the dialog (used on error paths or shutdown)."""
        try:
            self._parent.after(0, self._do_destroy)
        except (RuntimeError, tk.TclError):
            # Tk may already be torn down; fall back to a direct
            # destroy on the current thread (best-effort).
            self._do_destroy()

    # ------------------------------------------------------------------
    # Tk-thread private helpers.
    # ------------------------------------------------------------------

    def _do_update(self, current: int, total: int, name: str) -> None:
        if self._closed:
            return
        if total > 0:
            self._progressbar["maximum"] = total
            self._progressbar["value"] = current
        # Truncate long names so the dialog width stays stable across
        # updates — Tk's Label auto-grows otherwise and the modal jumps.
        display_name = name if len(name) <= 60 else f"…{name[-57:]}"
        self._title_label.configure(text=f"Deleting: {display_name}")
        self._counter_label.configure(text=f"{current} / {total}")

    def _do_complete(self) -> None:
        if self._closed:
            return
        max_value = self._progressbar["maximum"] or 1
        self._progressbar["value"] = max_value
        self._title_label.configure(text="Deletion complete")
        self._counter_label.configure(text="")
        # Hold 100 % visible for a beat so the user sees the success
        # state before the dialog vanishes.
        self._parent.after(self._COMPLETION_HOLD_MS, self._do_destroy)

    def _do_destroy(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            # ``grab_release`` may raise if the window was already
            # destroyed by another path (Tk shutdown, etc.). Swallow
            # silently — we are tearing down anyway.
            self._dialog.grab_release()
        except tk.TclError:
            pass
        try:
            self._dialog.destroy()
        except tk.TclError:
            pass

    def _noop_close(self) -> None:
        """Hook for ``WM_DELETE_WINDOW`` — explicitly does nothing.

        We refuse to close the dialog from the OS close button while
        the worker is still running: orphaning the thread would leave
        the user with a half-cleaned destination and no feedback.
        """
        # Intentionally empty. The dialog auto-closes via ``complete``.

    def _safe_grab(self) -> None:
        try:
            self._dialog.grab_set()
        except tk.TclError:
            # Parent already destroyed (test teardown, app shutdown).
            pass

    def _centre_on_parent(self) -> None:
        try:
            parent = self._parent
            # Resolve to the toplevel so we centre on the main window
            # even when ``parent`` is itself a child widget.
            top = parent.winfo_toplevel()
            top.update_idletasks()
            px = top.winfo_x()
            py = top.winfo_y()
            pw = top.winfo_width()
            ph = top.winfo_height()
            dw = self._dialog.winfo_reqwidth()
            dh = self._dialog.winfo_reqheight()
            x = px + max(0, (pw - dw) // 2)
            y = py + max(0, (ph - dh) // 2)
            self._dialog.geometry(f"+{x}+{y}")
        except tk.TclError:
            # Centring is cosmetic — never let a geometry error stop
            # the dialog from showing.
            pass
