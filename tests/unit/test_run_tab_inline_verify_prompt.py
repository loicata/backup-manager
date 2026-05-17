"""Tests for the v3.7.10 inline Fast-mode verify prompt.

Replaces the v3.7.9 modal Toplevel that broke when the scheduler
chained multiple Fast-mode profile backups (``grab_set`` stole focus
and the second prompt stacked on top of the first). The new design
appends cards to a ``ttk.Frame`` alerts area in the Run tab — N
cards coexist freely, no modality.
"""

from __future__ import annotations

import tkinter as tk
from unittest.mock import MagicMock

import pytest

from src.ui.tabs.run_tab import RunTab


@pytest.fixture()
def run_tab(tk_root):
    """Fresh RunTab. Shared session-scope tk_root from conftest.py."""
    tab = RunTab(tk_root)
    yield tab
    tab.destroy()


class TestAlertsFrameStartsEmpty:
    """The alerts area is in the widget tree but has zero cards by default."""

    def test_alerts_frame_exists_and_is_empty(self, run_tab) -> None:
        assert isinstance(run_tab.alerts_frame, tk.Widget)
        assert run_tab.alerts_frame.winfo_children() == []


class TestShowVerifyPromptCard:
    """``show_verify_prompt`` appends a card without modality."""

    def test_card_is_appended_to_alerts_frame(self, run_tab) -> None:
        card = run_tab.show_verify_prompt(
            profile_name="TestLoic",
            periodic_armed=True,
            interval_days=7,
            on_verify_now=MagicMock(),
            on_dismiss=MagicMock(),
            on_dont_ask_again=MagicMock(),
        )
        assert card in run_tab.alerts_frame.winfo_children()

    def test_multiple_cards_coexist(self, run_tab) -> None:
        """Two consecutive Fast-mode backups produce two stacked cards.

        This is the user-reported regression v3.7.9 → v3.7.10: a
        scheduled cycle of N profiles in Fast mode used to stack N
        modal Toplevels; now N cards sit side by side in the alerts
        area and the user picks any of them.
        """
        for name in ("Profile A", "Profile B", "Profile C"):
            run_tab.show_verify_prompt(
                profile_name=name,
                periodic_armed=False,
                interval_days=7,
                on_verify_now=MagicMock(),
                on_dismiss=MagicMock(),
                on_dont_ask_again=MagicMock(),
            )
        assert len(run_tab.alerts_frame.winfo_children()) == 3

    def test_returned_card_is_a_top_level_child(self, run_tab) -> None:
        """The card is parented to alerts_frame, not to root — so
        destroying the run_tab also disposes of pending prompts."""
        card = run_tab.show_verify_prompt(
            profile_name="X",
            periodic_armed=True,
            interval_days=1,
            on_verify_now=MagicMock(),
            on_dismiss=MagicMock(),
            on_dont_ask_again=MagicMock(),
        )
        assert card.master is run_tab.alerts_frame


class TestVerifyPromptButtons:
    """The two action buttons must fire their callback AND destroy the card."""

    def _find_buttons(self, card) -> dict:
        """Walk the card's widget tree, return {button_text: widget}."""
        found: dict[str, tk.Widget] = {}

        def _walk(widget):
            from tkinter import ttk

            if isinstance(widget, ttk.Button):
                found[str(widget.cget("text"))] = widget
            for child in widget.winfo_children():
                _walk(child)

        _walk(card)
        return found

    def test_verify_now_invokes_callback_then_destroys_card(self, run_tab) -> None:
        on_verify = MagicMock()
        card = run_tab.show_verify_prompt(
            profile_name="X",
            periodic_armed=True,
            interval_days=1,
            on_verify_now=on_verify,
            on_dismiss=MagicMock(),
            on_dont_ask_again=MagicMock(),
        )
        buttons = self._find_buttons(card)
        buttons["Verify now"].invoke()
        on_verify.assert_called_once()
        assert card not in run_tab.alerts_frame.winfo_children()

    def test_dismiss_invokes_callback_then_destroys_card(self, run_tab) -> None:
        on_dismiss = MagicMock()
        card = run_tab.show_verify_prompt(
            profile_name="X",
            periodic_armed=False,
            interval_days=7,
            on_verify_now=MagicMock(),
            on_dismiss=on_dismiss,
            on_dont_ask_again=MagicMock(),
        )
        buttons = self._find_buttons(card)
        buttons["Dismiss"].invoke()
        on_dismiss.assert_called_once()
        assert card not in run_tab.alerts_frame.winfo_children()


class TestDontAskAgainCheckbox:
    """The ``Don't ask again`` checkbox fires the callback on toggle.

    Eager commit (rather than commit-on-action) protects the user
    choice if they tick the box and then leave the card unanswered
    — e.g. close the app with a pending card visible.
    """

    def test_toggle_fires_callback_with_new_state(self, run_tab) -> None:
        on_toggle = MagicMock()
        card = run_tab.show_verify_prompt(
            profile_name="X",
            periodic_armed=True,
            interval_days=1,
            on_verify_now=MagicMock(),
            on_dismiss=MagicMock(),
            on_dont_ask_again=on_toggle,
        )
        # Find the BooleanVar via the Checkbutton's variable option.
        from tkinter import ttk

        checkbutton = None
        for widget in card.winfo_children():
            for sub in widget.winfo_children():
                if isinstance(sub, ttk.Checkbutton):
                    checkbutton = sub
                    break
            if checkbutton:
                break
        assert checkbutton is not None
        # Pull the variable name then resolve it to a BooleanVar instance.
        var_name = checkbutton.cget("variable")
        var = tk.BooleanVar(name=var_name)

        var.set(True)
        on_toggle.assert_called_with(True)
        var.set(False)
        on_toggle.assert_called_with(False)


class TestClearAlerts:
    """``clear_alerts`` removes every pending card.

    Called by ``clear_log`` on profile switch so a pending prompt
    tied to profile A is not still hanging around when the user is
    looking at profile B.
    """

    def test_clear_alerts_removes_all_cards(self, run_tab) -> None:
        for _ in range(3):
            run_tab.show_verify_prompt(
                profile_name="X",
                periodic_armed=True,
                interval_days=1,
                on_verify_now=MagicMock(),
                on_dismiss=MagicMock(),
                on_dont_ask_again=MagicMock(),
            )
        assert len(run_tab.alerts_frame.winfo_children()) == 3
        run_tab.clear_alerts()
        assert run_tab.alerts_frame.winfo_children() == []

    def test_clear_log_also_clears_alerts(self, run_tab) -> None:
        run_tab.show_verify_prompt(
            profile_name="X",
            periodic_armed=True,
            interval_days=1,
            on_verify_now=MagicMock(),
            on_dismiss=MagicMock(),
            on_dont_ask_again=MagicMock(),
        )
        assert len(run_tab.alerts_frame.winfo_children()) == 1
        run_tab.clear_log()
        assert run_tab.alerts_frame.winfo_children() == []
