"""Regression test for the disappearing Save button.

After the first switch to a non-saveable tab (Run / History / Recovery /
Verify) and back to a saveable one, ``_on_tab_changed`` re-packed
``_save_frame`` without ``before=self.notebook``. Tk appended the frame
to the END of the pack list, after the notebook. The notebook was
packed at startup with ``expand=True`` and ``side="top"`` (default), so
it claimed the entire vertical cavity and squeezed ``_save_frame`` to
zero height — invisible at the bottom of every tab.

The fix passes ``before=self.notebook`` on every re-pack so the frame
stays first in the pack list (= bottom slot for ``side="bottom"``).
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from src.ui.app import BackupManagerApp


def _make_app_with_minimal_layout(tk_root: tk.Tk) -> BackupManagerApp:
    """Build a stripped-down BackupManagerApp that only has the bits
    ``_on_tab_changed`` reads.

    Bypasses ``__init__`` entirely (config, scheduler, profile load
    are all expensive and irrelevant) and stitches up only the parent
    frame, notebook, save frame and the ``_no_save_tabs`` set.
    """
    app = BackupManagerApp.__new__(BackupManagerApp)
    parent = ttk.Frame(tk_root)
    parent.pack(fill="both", expand=True)
    app._main_frame = parent

    # Notebook packed first with expand=True — same as production.
    app.notebook = ttk.Notebook(parent)
    app.notebook.pack(fill="both", expand=True)

    tab_save_ok = ttk.Frame(app.notebook)
    tab_no_save = ttk.Frame(app.notebook)
    app.notebook.add(tab_save_ok, text="General")
    app.notebook.add(tab_no_save, text="Run")
    app._tab_save_ok = tab_save_ok
    app._tab_no_save = tab_no_save
    app._no_save_tabs = {str(tab_no_save)}

    # Save frame packed with ``before=notebook`` so its initial position
    # matches the production constructor.
    app._save_frame = ttk.Frame(parent)
    app._save_frame.pack(fill="x", side="bottom", before=app.notebook)
    ttk.Label(app._save_frame, text="Save", style="Accent.TButton").pack(fill="x")

    tk_root.update_idletasks()
    return app


def _pack_order(parent: tk.Widget) -> list[str]:
    """Return ``parent``'s pack slaves as ``str(widget)`` in pack order."""
    return [str(w) for w in parent.pack_slaves()]


class TestSaveFrameStaysAtBottom:
    """Pack-order contract: ``_save_frame`` must precede the notebook.

    ``pack_slaves()`` returns slaves in pack order. When the notebook is
    packed with ``expand=True`` and ``_save_frame`` with ``side="bottom"``,
    the order in the slaves list controls whether the bottom slot goes
    to Save (frame first) or to a sliver of the notebook (frame last).
    The user-visible bug is "Save invisible"; the underlying invariant
    is the pack-list order, which is exactly what we assert here.
    """

    def test_initial_layout_has_save_frame_before_notebook(self, tk_root):
        app = _make_app_with_minimal_layout(tk_root)
        try:
            order = _pack_order(app._main_frame)
            assert order.index(str(app._save_frame)) < order.index(str(app.notebook)), (
                "On first launch ``_save_frame`` must be packed before the "
                "notebook so it owns the bottom slot."
            )
        finally:
            app._main_frame.destroy()

    def test_save_frame_visible_after_switching_back_from_no_save_tab(self, tk_root):
        """The exact 2026-05-13 reproducer.

        1. Select a non-saveable tab (Run) → Save hidden.
        2. Select a saveable tab (General) → ``_on_tab_changed`` re-packs
           Save. With the pre-fix code the re-pack omitted
           ``before=self.notebook`` and Save was appended at the END of
           the pack list, behind the notebook's ``expand=True`` — squeezed
           to zero height and invisible to the user.
        """
        app = _make_app_with_minimal_layout(tk_root)
        try:
            app.notebook.select(app._tab_no_save)
            app._on_tab_changed()
            assert str(app._save_frame) not in _pack_order(
                app._main_frame
            ), "Save frame must be unpacked on a no-save tab."

            app.notebook.select(app._tab_save_ok)
            app._on_tab_changed()

            order = _pack_order(app._main_frame)
            assert str(app._save_frame) in order, "Save frame must be re-packed on a saveable tab."
            assert order.index(str(app._save_frame)) < order.index(str(app.notebook)), (
                "After ``_on_tab_changed`` re-packs Save, the frame must "
                "still be BEFORE the notebook in the pack list — otherwise "
                "the notebook's expand=True consumes the entire cavity and "
                "Save is squeezed to zero height."
            )
        finally:
            app._main_frame.destroy()

    def test_repeated_tab_switches_preserve_order(self, tk_root):
        """Ten consecutive Run ↔ General switches must keep Save first.

        Guards against a future regression where the re-pack path drifts
        out of the ``before=`` contract and the bug only manifests after
        a specific switch count.
        """
        app = _make_app_with_minimal_layout(tk_root)
        try:
            for _ in range(10):
                app.notebook.select(app._tab_no_save)
                app._on_tab_changed()
                app.notebook.select(app._tab_save_ok)
                app._on_tab_changed()

            order = _pack_order(app._main_frame)
            assert order.index(str(app._save_frame)) < order.index(str(app.notebook))
        finally:
            app._main_frame.destroy()
