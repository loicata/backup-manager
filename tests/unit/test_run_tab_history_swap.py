"""Tests for the per-profile Run-tab history swap.

When the user clicks profile B in the sidebar, ``set_current_profile_id``
must:

1. Persist incoming LOG events per profile (regardless of which is
   currently selected) so background runs are not lost.
2. Clear the log_tree.
3. Repopulate it from B's persisted history file.

Switching back to A must restore A's full history, including events
that arrived while B was selected.
"""

from __future__ import annotations

import pytest

from src.core.run_history import RunHistoryStore
from src.ui.tabs.run_tab import RunTab


@pytest.fixture()
def store(tmp_path):
    return RunHistoryStore(tmp_path / "run_history")


@pytest.fixture()
def run_tab(tk_root, store):
    tab = RunTab(tk_root, history_store=store)
    # Backup must be "active" for non-terminal LOG events to reach the
    # widget. Terminal lines bypass the gate but we want every event
    # to render in these tests for assertion simplicity.
    tab._backup_active = True
    yield tab
    tab.destroy()


def _emit_log(tab: RunTab, profile_id: str, message: str) -> None:
    tab._on_log(message=message, level="info", profile_id=profile_id)
    # ``_on_log`` schedules the widget insert via ``after(0, ...)``.
    # Drain the Tk event queue so the row is materialised before
    # the assertion that follows.
    tab.update_idletasks()
    tab.update()


def _log_messages(tab: RunTab) -> list[str]:
    rows = tab.log_tree.get_children("")
    return [tab.log_tree.item(row, "text") for row in rows]


class TestSwitchKeepsPerProfileHistory:
    def test_switch_to_other_profile_clears_log_then_restores_on_return(
        self, run_tab, store
    ) -> None:
        run_tab.set_current_profile_id("A")
        _emit_log(run_tab, "A", "A-msg-1")
        _emit_log(run_tab, "A", "A-msg-2")
        assert _log_messages(run_tab) == ["A-msg-1", "A-msg-2"]

        run_tab.set_current_profile_id("B")
        assert _log_messages(run_tab) == []

        run_tab.set_current_profile_id("A")
        assert _log_messages(run_tab) == ["A-msg-1", "A-msg-2"]

    def test_background_run_persists_for_non_selected_profile(self, run_tab) -> None:
        """An event tagged for a non-selected profile is dropped from
        the widget but must still land in that profile's history file."""
        run_tab.set_current_profile_id("A")
        # Event for B while A is selected — widget ignores it (filter)
        # but the store must still receive it.
        run_tab._on_log(message="B-msg-while-A", level="info", profile_id="B")
        run_tab.update_idletasks()
        assert _log_messages(run_tab) == []  # A's view stays empty.

        run_tab.set_current_profile_id("B")
        assert _log_messages(run_tab) == ["B-msg-while-A"]

    def test_history_survives_run_tab_recreation(self, tk_root, store) -> None:
        """Persisted history must restore after a new RunTab is built —
        emulates the app being restarted between two sessions."""
        first = RunTab(tk_root, history_store=store)
        first._backup_active = True
        first.set_current_profile_id("p1")
        _emit_log(first, "p1", "session1-msg")
        first.destroy()

        second = RunTab(tk_root, history_store=store)
        second._backup_active = True
        second.set_current_profile_id("p1")
        assert _log_messages(second) == ["session1-msg"]
        second.destroy()

    def test_same_profile_reselect_is_idempotent(self, run_tab) -> None:
        """Re-selecting the same id must not duplicate the rows."""
        run_tab.set_current_profile_id("p1")
        _emit_log(run_tab, "p1", "row-1")
        _emit_log(run_tab, "p1", "row-2")
        assert _log_messages(run_tab) == ["row-1", "row-2"]

        run_tab.set_current_profile_id("p1")
        assert _log_messages(run_tab) == ["row-1", "row-2"]


class TestSwitchResetsVolatileWidgets:
    """Progress bar, percent label, status label, and phase counters
    must be reset on profile switch so the previous profile's live
    state does not bleed into the new view. The 3.7.13 first-cut
    forgot this and a TestLoic-mid-scan ``Scanning... 99534 files in
    9859 folders`` was visible on every other profile until the next
    PROGRESS event for that profile arrived."""

    def test_switch_resets_progress_bar_to_zero(self, run_tab) -> None:
        run_tab.set_current_profile_id("A")
        # Pretend a PROGRESS event landed and bumped the bar.
        run_tab.progress_bar["value"] = 78
        run_tab.percent_label.config(text="78%")

        run_tab.set_current_profile_id("B")

        assert run_tab.progress_bar["value"] == 0
        assert run_tab.percent_label.cget("text") == "0%"

    def test_switch_resets_status_label_to_waiting(self, run_tab) -> None:
        run_tab.set_current_profile_id("A")
        run_tab.status_label.config(text="Scanning... 99534 files in 9859 folders")

        run_tab.set_current_profile_id("B")

        assert run_tab.status_label.cget("text") == "Waiting..."

    def test_switch_clears_phase_counter_dicts(self, run_tab) -> None:
        run_tab.set_current_profile_id("A")
        run_tab._phase_totals["collector"] = 100
        run_tab._phase_done["collector"] = 42
        run_tab._phase_order.append("collector")
        run_tab._phase_weights["collector"] = 1
        run_tab._last_pct = 42

        run_tab.set_current_profile_id("B")

        assert run_tab._phase_totals == {}
        assert run_tab._phase_done == {}
        assert run_tab._phase_order == []
        assert run_tab._phase_weights == {}
        assert run_tab._last_pct == 0


class TestPersistedEntryShape:
    def test_persisted_entry_carries_message_level_phase(
        self, run_tab, store
    ) -> None:
        run_tab.set_current_profile_id("p1")
        run_tab._on_log(
            message="Manifest created: 7 files",
            level="info",
            phase="manifest",
            profile_id="p1",
        )
        run_tab.update_idletasks()

        entries = store.load("p1")
        assert len(entries) == 1
        assert entries[0]["msg"] == "Manifest created: 7 files"
        assert entries[0]["level"] == "info"
        assert entries[0]["phase"] == "manifest"
        assert "ts" in entries[0]

    def test_untagged_event_does_not_pollute_any_profile(
        self, run_tab, store
    ) -> None:
        """Events without a profile_id (Verify tab, cross-tab emits)
        must not be persisted — they have no profile to attach to."""
        run_tab.set_current_profile_id("p1")
        run_tab._on_log(message="untagged-event", level="info")
        run_tab.update_idletasks()

        assert store.load("p1") == []
