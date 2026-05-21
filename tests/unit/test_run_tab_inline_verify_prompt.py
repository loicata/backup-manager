"""Tests for the inline Fast-mode verify prompt rows.

Originally (v3.7.10) the prompt rendered as a card stacked in a
separate ``alerts_frame`` above the log_tree. The user found the
dedicated zone too prominent and asked for the prompt to live inside
the Message panel itself.

This iteration replaces the card with four log_tree rows:

* parent (success-coloured) — the completion announcement
* info — periodic verify status
* ``▶  Verify now`` — clickable action
* ``✕  Dismiss`` — clickable action
* ``☐  Don't ask again for this profile`` — clickable toggle

A ``<Button-1>`` handler on the log_tree dispatches to the right
callback based on which child row sits under the cursor. Rows
survive scrolling naturally (real Treeview rows, not overlay
widgets) and are deleted atomically when an action fires.
"""

from __future__ import annotations

import tkinter as tk
from unittest.mock import MagicMock

import pytest

from src.core.run_history import VerifyPromptStore
from src.ui.tabs.run_tab import RunTab


@pytest.fixture()
def run_tab(tk_root):
    tab = RunTab(tk_root)
    yield tab
    tab.destroy()


@pytest.fixture()
def run_tab_with_store(tk_root, tmp_path):
    """RunTab wired with a real VerifyPromptStore + factory.

    Most tests above use a bare RunTab — they exercise the in-memory
    dict path only. The persistence path needs both the store and the
    factory: ``_restore_pending_verify_prompt`` is a no-op when either
    is missing.
    """
    store = VerifyPromptStore(tmp_path / "verify_prompts.json")

    def _factory(profile_id):
        return (MagicMock(), MagicMock(), MagicMock())

    tab = RunTab(
        tk_root,
        verify_prompt_store=store,
        verify_prompt_factory=_factory,
    )
    yield tab, store
    tab.destroy()


def _make_prompt(run_tab: RunTab, **overrides) -> str:
    defaults = dict(
        profile_name="TestLoic",
        periodic_armed=True,
        interval_days=7,
        on_verify_now=MagicMock(),
        on_dismiss=MagicMock(),
        on_dont_ask_again=MagicMock(),
    )
    defaults.update(overrides)
    return run_tab.show_verify_prompt(**defaults)


def _fire_click_on_item(run_tab: RunTab, item_id: str) -> None:
    """Simulate a left-click landing on a specific Treeview row.

    The handler reads ``event.y`` to call ``identify_row``; we cannot
    fake the y-coordinate reliably across Tk versions on Windows, so
    we patch ``identify_row`` for the duration of the call to return
    the target item id deterministically.
    """
    original = run_tab.log_tree.identify_row
    run_tab.log_tree.identify_row = lambda _y: item_id  # type: ignore[assignment]
    try:
        event = tk.Event()
        event.y = 0
        run_tab._on_log_tree_click(event)
    finally:
        run_tab.log_tree.identify_row = original  # type: ignore[assignment]


class TestPromptInsertsIntoLogTree:
    def test_prompt_creates_parent_plus_action_children(self, run_tab) -> None:
        parent_id = _make_prompt(run_tab)
        children = run_tab.log_tree.get_children(parent_id)
        # info + verify + dismiss + dont-ask = 4 children
        assert len(children) == 4
        texts = [run_tab.log_tree.item(c, "text") for c in children]
        assert any("Verify now" in t for t in texts)
        assert any("Dismiss" in t for t in texts)
        assert any("Don't ask again" in t for t in texts)

    def test_parent_text_mentions_profile_and_fast_mode(self, run_tab) -> None:
        parent_id = _make_prompt(run_tab, profile_name="MyBackup")
        text = run_tab.log_tree.item(parent_id, "text")
        assert "MyBackup" in text
        assert "Fast mode" in text

    def test_periodic_armed_info_shows_interval(self, run_tab) -> None:
        parent_id = _make_prompt(run_tab, periodic_armed=True, interval_days=14)
        info_id = run_tab.log_tree.get_children(parent_id)[0]
        info_text = run_tab.log_tree.item(info_id, "text")
        assert "14 days" in info_text

    def test_periodic_disarmed_info_warns(self, run_tab) -> None:
        parent_id = _make_prompt(run_tab, periodic_armed=False)
        info_id = run_tab.log_tree.get_children(parent_id)[0]
        info_text = run_tab.log_tree.item(info_id, "text")
        assert "No periodic" in info_text

    def test_returned_id_is_a_top_level_item(self, run_tab) -> None:
        parent_id = _make_prompt(run_tab)
        assert parent_id in run_tab.log_tree.get_children("")

    def test_multiple_prompts_coexist_as_siblings(self, run_tab) -> None:
        ids = [_make_prompt(run_tab, profile_name=f"P{i}") for i in range(3)]
        top_level = run_tab.log_tree.get_children("")
        for pid in ids:
            assert pid in top_level


class TestActionClicks:
    def test_verify_now_click_fires_callback_and_removes_rows(
        self, run_tab
    ) -> None:
        on_verify = MagicMock()
        parent_id = _make_prompt(run_tab, on_verify_now=on_verify)
        verify_item = run_tab._verify_prompts[parent_id]["verify_item"]

        _fire_click_on_item(run_tab, verify_item)

        on_verify.assert_called_once()
        assert parent_id not in run_tab.log_tree.get_children("")
        assert parent_id not in run_tab._verify_prompts

    def test_dismiss_click_fires_callback_and_removes_rows(self, run_tab) -> None:
        on_dismiss = MagicMock()
        parent_id = _make_prompt(run_tab, on_dismiss=on_dismiss)
        dismiss_item = run_tab._verify_prompts[parent_id]["dismiss_item"]

        _fire_click_on_item(run_tab, dismiss_item)

        on_dismiss.assert_called_once()
        assert parent_id not in run_tab.log_tree.get_children("")

    def test_dont_ask_toggle_flips_glyph_and_notifies(self, run_tab) -> None:
        on_toggle = MagicMock()
        parent_id = _make_prompt(run_tab, on_dont_ask_again=on_toggle)
        dont_ask_item = run_tab._verify_prompts[parent_id]["dont_ask_item"]

        # First click ticks the box.
        _fire_click_on_item(run_tab, dont_ask_item)
        assert run_tab._verify_prompts[parent_id]["dont_ask_state"] is True
        assert "☑" in run_tab.log_tree.item(dont_ask_item, "text")
        on_toggle.assert_called_with(True)

        # Second click unticks it.
        _fire_click_on_item(run_tab, dont_ask_item)
        assert run_tab._verify_prompts[parent_id]["dont_ask_state"] is False
        assert "☐" in run_tab.log_tree.item(dont_ask_item, "text")
        on_toggle.assert_called_with(False)

    def test_toggle_does_not_remove_prompt_rows(self, run_tab) -> None:
        parent_id = _make_prompt(run_tab)
        dont_ask_item = run_tab._verify_prompts[parent_id]["dont_ask_item"]

        _fire_click_on_item(run_tab, dont_ask_item)

        assert parent_id in run_tab.log_tree.get_children("")

    def test_click_on_unrelated_row_is_a_noop(self, run_tab) -> None:
        on_verify = MagicMock()
        _make_prompt(run_tab, on_verify_now=on_verify)
        # Insert a regular log row and click it.
        unrelated = run_tab.log_tree.insert(
            "", "end", text="Backup type: full", values=("",)
        )

        _fire_click_on_item(run_tab, unrelated)

        on_verify.assert_not_called()


class TestClearAlerts:
    def test_clear_alerts_removes_every_prompt_row(self, run_tab) -> None:
        for i in range(3):
            _make_prompt(run_tab, profile_name=f"P{i}")
        assert len(run_tab._verify_prompts) == 3

        run_tab.clear_alerts()

        assert run_tab._verify_prompts == {}
        # All four-row prompts must be gone from the log tree.
        assert run_tab.log_tree.get_children("") == ()

    def test_clear_log_also_clears_prompts(self, run_tab) -> None:
        _make_prompt(run_tab)
        assert run_tab._verify_prompts

        run_tab.clear_log()

        assert run_tab._verify_prompts == {}
        assert run_tab.log_tree.get_children("") == ()


class TestPersistentStoreInteraction:
    """Regression guards for the 21/05/2026 double-prompt issue.

    A Fast-mode prompt raised in run N was rendered twice in run N+1
    because ``clear_log`` wiped only the in-memory dict, leaving the
    JSON store untouched. On the next profile-switch / app-restart
    the stale entry was replayed by ``_restore_pending_verify_prompt``
    and stacked with the fresh prompt from the new run.

    Two contracts to enforce:
    * ``clear_log`` (called at the start of every new run) MUST purge
      the persistent store along with the in-memory dict.
    * ``set_current_profile_id`` → ``_reload_log_history`` (called on
      profile-switch) MUST keep the store intact so the immediately
      following ``_restore_pending_verify_prompt`` can replay it.
    """

    def test_clear_log_purges_verify_prompt_store(
        self, run_tab_with_store
    ) -> None:
        tab, store = run_tab_with_store
        # Bind the tab to the profile id before raising the prompt:
        # ``show_verify_prompt`` only renders (and registers in the
        # in-memory dict) when ``profile_id == current_profile_id``,
        # and ``clear_alerts`` walks that dict to reach the store.
        tab.set_current_profile_id("profile-abc")
        tab.show_verify_prompt(
            profile_name="TestNP",
            periodic_armed=True,
            interval_days=7,
            on_verify_now=MagicMock(),
            on_dismiss=MagicMock(),
            on_dont_ask_again=MagicMock(),
            profile_id="profile-abc",
        )
        assert store.get("profile-abc") is not None
        assert tab._verify_prompts

        tab.clear_log()

        assert store.get("profile-abc") is None
        assert tab._verify_prompts == {}

    def test_reload_log_history_keeps_store_intact(
        self, run_tab_with_store
    ) -> None:
        tab, store = run_tab_with_store
        store.set(
            "profile-abc",
            {
                "profile_name": "TestNP",
                "periodic_armed": True,
                "interval_days": 7,
            },
        )

        # Triggers _clear_run_state + _reload_log_history +
        # _restore_pending_verify_prompt (CHANGE from "" to "profile-abc").
        tab.set_current_profile_id("profile-abc")

        assert store.get("profile-abc") is not None
        # And the restored prompt is visible in the tree.
        assert tab._verify_prompts
