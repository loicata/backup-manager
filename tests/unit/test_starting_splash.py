"""Tests for the wizard → main-UI bridge splash window.

The splash is shown after ``SetupWizard`` finishes successfully on the
first launch path. It hides the 5-10 s window during which
``BackupManagerApp.__init__`` builds 12 tabs synchronously and the
screen would otherwise be entirely blank. These tests verify the
splash widget contract — they don't measure timing (we can't simulate
the slow constructor in a unit test).
"""

from __future__ import annotations

import tkinter as tk

import pytest

from src.__main__ import _show_starting_splash


@pytest.fixture()
def splash(tk_root):
    """Yield a freshly-created splash and clean up afterwards."""
    s = _show_starting_splash(tk_root)
    yield s
    if s.winfo_exists():
        s.destroy()


class TestStartingSplash:
    """Behavioural contract of the transient splash window."""

    def test_returns_toplevel(self, splash):
        """Caller relies on a ``destroy``-able Toplevel."""
        assert isinstance(splash, tk.Toplevel)

    def test_window_is_chromeless(self, splash):
        """No title bar or close button — user must not be able to
        dismiss the splash and end up on a blank screen during the
        BackupManagerApp build."""
        # overrideredirect returns 1/0 on Windows, True/False on others
        assert splash.overrideredirect()

    def test_is_topmost(self, splash):
        """Other windows that pop up during init (e.g. UAC prompts
        from the autostart enable) must not occlude the splash."""
        assert splash.attributes("-topmost")

    def test_size_is_reasonable(self, splash):
        """Sized to read at typical resolutions without dominating
        the screen — keep the values sane so a future refactor
        doesn't accidentally produce a 1x1 px window."""
        splash.update_idletasks()
        assert 200 < splash.winfo_width() < 600
        assert 80 < splash.winfo_height() < 300

    def test_centered_on_screen(self, splash):
        """Splash must appear centered, not pinned to a corner."""
        splash.update_idletasks()
        sw = splash.winfo_screenwidth()
        sh = splash.winfo_screenheight()
        x = splash.winfo_rootx()
        y = splash.winfo_rooty()
        # Loose tolerance: within 50 px of the geometric center to
        # accommodate window manager nudging on multi-monitor setups.
        center_x = (sw - splash.winfo_width()) // 2
        center_y = (sh - splash.winfo_height()) // 2
        assert abs(x - center_x) < 50
        assert abs(y - center_y) < 50

    def test_displays_starting_text(self, splash):
        """User must see the word 'Starting' so 'app frozen?' is
        replaced by 'app starting'."""
        # Walk children looking for any Label that contains 'Starting'.
        labels: list[str] = []
        for parent in (splash, *splash.winfo_children()):
            for w in parent.winfo_children():
                if isinstance(w, tk.Misc) and "label" in w.winfo_class().lower():
                    with __import__("contextlib").suppress(tk.TclError):
                        labels.append(str(w.cget("text")))
        assert any("Starting" in t for t in labels), (
            f"Expected a label containing 'Starting', got: {labels}"
        )

    def test_destroy_removes_window(self, tk_root):
        """Caller's ``splash.destroy()`` must actually tear down the
        Toplevel — otherwise the splash would float above the main
        UI forever after the wizard."""
        s = _show_starting_splash(tk_root)
        assert s.winfo_exists()
        s.destroy()
        # After destroy, querying winfo_exists on the same handle
        # returns 0 (Tk has freed the underlying widget).
        assert not s.winfo_exists()

    def test_multiple_splashes_do_not_collide(self, tk_root):
        """Defensive: building two splashes (e.g. unit-test reuse)
        should produce two independent widgets that can be
        destroyed in any order."""
        a = _show_starting_splash(tk_root)
        b = _show_starting_splash(tk_root)
        try:
            assert a is not b
            assert a.winfo_exists()
            assert b.winfo_exists()
        finally:
            a.destroy()
            b.destroy()
