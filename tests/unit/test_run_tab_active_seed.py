"""Regression test: switching to a profile whose backup is already in
flight seeds RunTab._backup_active so its live progress/log show
(audit L10/#12) instead of a frozen 0% bar.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from src.ui.tabs.run_tab import RunTab


def _tab_stub() -> RunTab:
    """A RunTab without Tk: only the attributes set_current_profile_id touches."""
    tab = RunTab.__new__(RunTab)
    tab._current_profile_id = "old-profile"
    tab._backup_active = False
    # The three side-effect methods are stubbed — we only assert the flag.
    tab._clear_run_state = MagicMock()
    tab._reload_log_history = MagicMock()
    tab._restore_pending_verify_prompt = MagicMock()
    return tab


class TestBackupActiveSeed:
    def test_switch_to_running_profile_seeds_active(self):
        tab = _tab_stub()
        tab.set_current_profile_id("running-profile", is_running=True)
        assert tab._backup_active is True

    def test_switch_to_idle_profile_leaves_flag_alone(self):
        """RAISE-only semantics: the seed never LOWERS the flag — that
        stays the job of terminal STATUS events (the cross-tab Verify
        contract pinned by the history-swap tests)."""
        tab = _tab_stub()
        tab._backup_active = True
        tab.set_current_profile_id("idle-profile", is_running=False)
        assert tab._backup_active is True

    def test_default_is_running_does_not_lower(self):
        tab = _tab_stub()
        tab._backup_active = True
        tab.set_current_profile_id("other-profile")  # no is_running arg
        assert tab._backup_active is True

    def test_same_profile_is_noop(self):
        tab = _tab_stub()
        tab._backup_active = False
        # Re-selecting the same id must not reset state (no blink) and
        # must not raise the flag either.
        tab.set_current_profile_id("old-profile", is_running=True)
        assert tab._backup_active is False
        tab._clear_run_state.assert_not_called()
