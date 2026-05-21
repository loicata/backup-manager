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

from unittest.mock import MagicMock

from src.core.run_history import RunHistoryStore, VerifyPromptStore
from src.ui.tabs.run_tab import RunTab


@pytest.fixture()
def store(tmp_path):
    return RunHistoryStore(tmp_path / "run_history")


@pytest.fixture()
def prompt_store(tmp_path):
    return VerifyPromptStore(tmp_path / "verify_prompts.json")


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


class TestTerminalStatusRestoredAfterSwitch:
    """When the last persisted line is a terminal message, the pill
    (label colour, progress bar) must reflect it after a swap.

    Pre-fix: switching profile reset to "Waiting..." 0 % and stayed
    there even when the profile's last persisted event was
    ``Backup complete: ...`` — so a successful run looked idle after
    a side-trip to another profile."""

    def _emit_terminal(
        self,
        tab: RunTab,
        profile_id: str,
        message: str,
        level: str = "info",
    ) -> None:
        # Terminal lines are exempt from the ``_backup_active`` gate
        # so they always reach ``_append_log`` regardless of state.
        tab._on_log(message=message, level=level, profile_id=profile_id)
        tab.update_idletasks()
        tab.update()

    def test_switch_back_after_success_restores_complete_pill(
        self, run_tab
    ) -> None:
        run_tab.set_current_profile_id("A")
        self._emit_terminal(run_tab, "A", "Backup complete: 7 files in 0.5 min")

        run_tab.set_current_profile_id("B")
        run_tab.set_current_profile_id("A")

        assert run_tab.progress_bar["value"] == 100
        assert run_tab.percent_label.cget("text") == "100%"
        assert run_tab.status_label.cget("text") == "Backup complete!"

    def test_switch_back_after_failure_shows_failed_pill(self, run_tab) -> None:
        run_tab.set_current_profile_id("A")
        self._emit_terminal(
            run_tab, "A", "Backup failed: disk full", level="error"
        )

        run_tab.set_current_profile_id("B")
        run_tab.set_current_profile_id("A")

        assert run_tab.status_label.cget("text") == "Backup failed!"

    def test_switch_back_after_cancel_shows_cancelled_pill(self, run_tab) -> None:
        run_tab.set_current_profile_id("A")
        self._emit_terminal(run_tab, "A", "Backup cancelled by user")

        run_tab.set_current_profile_id("B")
        run_tab.set_current_profile_id("A")

        assert run_tab.status_label.cget("text") == "Backup cancelled"

    def test_switch_back_after_rejected_shows_rejected_pill(
        self, run_tab
    ) -> None:
        # ``Backup rejected: ...`` is emitted by the engine BEFORE
        # STATUS=running when the per-profile lock is already taken.
        # ``_backup_active`` is still False at that point — the
        # message must still survive the per-active-tab gate (it
        # matches the terminal pattern) and the pill restore must
        # treat it as a failure-style outcome.
        run_tab.set_current_profile_id("A")
        # Reproduce the real condition: backup not flagged active.
        run_tab._backup_active = False
        self._emit_terminal(
            run_tab,
            "A",
            "Backup rejected: Another backup is already running for this profile.",
        )

        # Confirm the message rendered even though the gate was closed.
        texts = [
            run_tab.log_tree.item(c, "text")
            for c in run_tab.log_tree.get_children("")
        ]
        assert any(t.startswith("Backup rejected") for t in texts)

        # And the restore on swap-back applies the danger pill.
        run_tab.set_current_profile_id("B")
        run_tab.set_current_profile_id("A")
        assert run_tab.status_label.cget("text") == "Backup rejected"

    def test_profile_with_no_terminal_event_keeps_waiting_baseline(
        self, run_tab
    ) -> None:
        """Mid-run (or never-run) profiles must still show "Waiting..."
        after a swap — the restore only fires on terminal lines."""
        run_tab.set_current_profile_id("A")
        run_tab._on_log(
            message="Collecting files...", level="info", profile_id="A"
        )
        run_tab.update_idletasks()

        run_tab.set_current_profile_id("B")
        run_tab.set_current_profile_id("A")

        assert run_tab.progress_bar["value"] == 0
        assert run_tab.status_label.cget("text") == "Waiting..."


class TestQueuedEventsDoNotCrossProfileSwitch:
    """PROGRESS / STATUS / LOG handlers queue their widget update via
    ``after(0)``. The worker-thread filter rejects events for other
    profiles, but a queued callback for profile A can still drain
    AFTER the user has switched to profile B — landing the stale
    update on the wrong view. Each main-thread handler re-checks
    ``profile_id`` so the late arrival is dropped silently.

    Without this guard, a TestLoic-mid-hashing PROGRESS event would
    set the My Backup progress bar to ``hashing: ...entities.pyi``
    8 % the instant the user clicked My Backup in the sidebar."""

    def test_progress_for_old_profile_drops_after_switch(self, run_tab) -> None:
        run_tab.set_current_profile_id("A")
        run_tab._backup_active = True
        run_tab._on_progress(
            current=50, total=100, filename="A-file.py", phase="hashing", profile_id="A"
        )
        run_tab.set_current_profile_id("B")
        run_tab.update_idletasks()
        run_tab.update()

        assert run_tab.progress_bar["value"] == 0
        assert run_tab.percent_label.cget("text") == "0%"

    def test_status_for_old_profile_drops_after_switch(self, run_tab) -> None:
        run_tab.set_current_profile_id("A")
        run_tab._on_status(state="success", profile_id="A")
        run_tab.set_current_profile_id("B")
        run_tab.update_idletasks()
        run_tab.update()

        # Status label must stay at the new-profile baseline, not flip
        # to "Backup complete!" because of the stale A event.
        assert run_tab.status_label.cget("text") == "Waiting..."

    def test_log_for_old_profile_drops_after_switch(self, run_tab) -> None:
        run_tab.set_current_profile_id("A")
        run_tab._backup_active = True
        run_tab._on_log(
            message="A-side row", level="info", phase="manifest", profile_id="A"
        )
        run_tab.set_current_profile_id("B")
        run_tab.update_idletasks()
        run_tab.update()

        # B's log_tree must be empty — the A-side row is queued but
        # the main-thread guard drops it on the floor.
        texts = [
            run_tab.log_tree.item(child, "text")
            for child in run_tab.log_tree.get_children("")
        ]
        assert "A-side row" not in texts


class TestVerifyPromptPersistence:
    """A Fast-mode prompt must outlive profile switches and app
    restarts so a scheduled run that completed on profile A still
    has an actionable prompt next time the user opens A in the
    sidebar — not just when the prompt was first raised."""

    @pytest.fixture()
    def tab_with_prompt_store(self, tk_root, store, prompt_store):
        factory = MagicMock(
            return_value=(MagicMock(), MagicMock(), MagicMock())
        )
        tab = RunTab(
            tk_root,
            history_store=store,
            verify_prompt_store=prompt_store,
            verify_prompt_factory=factory,
        )
        yield tab, prompt_store, factory
        tab.destroy()

    def test_prompt_persisted_when_raised_for_non_current_profile(
        self, tab_with_prompt_store
    ) -> None:
        tab, prompt_store, _factory = tab_with_prompt_store
        tab.set_current_profile_id("A")

        # Prompt raised for B while A is selected — must NOT render
        # on A but MUST land in the store.
        rendered = tab.show_verify_prompt(
            profile_id="B",
            profile_name="ProfB",
            periodic_armed=True,
            interval_days=3,
            on_verify_now=MagicMock(),
            on_dismiss=MagicMock(),
            on_dont_ask_again=MagicMock(),
        )

        assert rendered is None
        assert tab.log_tree.get_children("") == ()
        assert prompt_store.get("B") == {
            "profile_name": "ProfB",
            "periodic_armed": True,
            "interval_days": 3,
        }

    def test_switch_to_profile_with_pending_prompt_restores_rows(
        self, tab_with_prompt_store
    ) -> None:
        tab, prompt_store, factory = tab_with_prompt_store
        prompt_store.set(
            "B",
            {"profile_name": "ProfB", "periodic_armed": False, "interval_days": 7},
        )

        tab.set_current_profile_id("B")

        children = tab.log_tree.get_children("")
        assert len(children) == 1
        parent_text = tab.log_tree.item(children[0], "text")
        assert "ProfB" in parent_text
        factory.assert_called_once_with("B")

    def test_dismissing_restored_prompt_clears_store(
        self, tab_with_prompt_store
    ) -> None:
        tab, prompt_store, _factory = tab_with_prompt_store
        prompt_store.set(
            "B",
            {"profile_name": "ProfB", "periodic_armed": True, "interval_days": 7},
        )
        tab.set_current_profile_id("B")

        parent_id = tab.log_tree.get_children("")[0]
        tab._destroy_verify_prompt(parent_id)

        assert prompt_store.get("B") is None

    def test_switch_to_profile_without_prompt_does_not_call_factory(
        self, tab_with_prompt_store
    ) -> None:
        tab, _prompt_store, factory = tab_with_prompt_store

        tab.set_current_profile_id("never-had-a-prompt")

        factory.assert_not_called()


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


class TestPersistedPhaseInference:
    """The persisted JSONL must carry the inferred Phase tag, not an
    empty string.

    Regression guard for the 21/05/2026 captured anomaly: after a
    profile-switch (or app restart) the reloaded log_tree showed an
    empty Phase column on every engine-level message — ``Saving
    manifest…``, ``Writing commit marker…``, ``Updating manifest…``,
    ``Rotating old backups…``, ``Building integrity manifest…``,
    ``Copying to Storage…``, ``Backup complete: …``. Live runs filled
    those tags via inference in ``_on_log``, but ``_persist_log`` ran
    BEFORE the inference and wrote ``phase=""`` into the JSONL. On
    reload, the column went blank because ``_reload_log_history`` does
    not re-infer.

    Two contracts these tests pin:

    1. ``_persist_log`` infers the phase from the message text when
       the event arrives without an explicit tag.
    2. The per-profile phase tracker is isolated — a background
       scheduler run on profile B cannot bleed its phase into
       profile A's persisted history.
    """

    def test_persisted_phase_inferred_from_engine_message(
        self, run_tab, store
    ) -> None:
        """``Saving manifest…`` → ``manifest`` in JSONL."""
        run_tab.set_current_profile_id("p1")

        run_tab._on_log(message="Saving manifest...", level="info", profile_id="p1")
        run_tab._on_log(
            message="Writing commit marker...", level="info", profile_id="p1"
        )
        run_tab._on_log(
            message="Updating manifest...", level="info", profile_id="p1"
        )
        run_tab._on_log(
            message="Rotating old backups...", level="info", profile_id="p1"
        )
        run_tab.update_idletasks()

        entries = store.load("p1")
        phases_by_msg = {e["msg"]: e["phase"] for e in entries}
        assert phases_by_msg["Saving manifest..."] == "manifest"
        assert phases_by_msg["Writing commit marker..."] == "commit_marker"
        assert phases_by_msg["Updating manifest..."] == "manifest"
        assert phases_by_msg["Rotating old backups..."] == "rotator"

    def test_persisted_phase_inherits_previous_for_unmatched_message(
        self, run_tab, store
    ) -> None:
        """Engine messages without a pattern match inherit the previous
        phase per profile (so ``Backup written: …`` keeps the ``writer``
        tag set by the preceding ``Copying to Storage…``).
        """
        run_tab.set_current_profile_id("p1")

        # "Copying to Storage..." matches the writer pattern.
        run_tab._on_log(
            message="Copying to Storage — Connect USB drive D:/...",
            level="info",
            profile_id="p1",
        )
        # "Backup written: ..." does not match any pattern; it must
        # inherit ``writer`` from the prior message.
        run_tab._on_log(
            message="Backup written: 42 files to D:\\backups\\X",
            level="info",
            profile_id="p1",
        )
        run_tab.update_idletasks()

        entries = store.load("p1")
        phases_by_msg = {e["msg"]: e["phase"] for e in entries}
        assert phases_by_msg["Copying to Storage — Connect USB drive D:/..."] == "writer"
        assert (
            phases_by_msg["Backup written: 42 files to D:\\backups\\X"] == "writer"
        ), "Phase must inherit from previous unmatched message"

    def test_persisted_phase_resets_on_terminal_message(
        self, run_tab, store
    ) -> None:
        """``Backup complete: …`` is terminal — its phase is empty and
        a follow-up run starts fresh (no leak of the previous run's
        ``rotator`` into the next run's first messages).
        """
        run_tab.set_current_profile_id("p1")

        run_tab._on_log(
            message="Rotating old backups...", level="info", profile_id="p1"
        )
        run_tab._on_log(
            message="Backup complete: 7 files in 0.1 min",
            level="info",
            profile_id="p1",
        )
        # Simulate the start of the next run — an "empty" message
        # (run-boundary marker emitted by the engine) followed by
        # "Backup type: full" must NOT carry the previous run's
        # ``rotator`` tag.
        run_tab._on_log(message="", level="info", profile_id="p1")
        run_tab._on_log(
            message="Backup type: full", level="info", profile_id="p1"
        )
        run_tab.update_idletasks()

        entries = store.load("p1")
        # Find the entry that opens the new run.
        boundary_idx = next(
            i for i, e in enumerate(entries) if e["msg"] == ""
        )
        backup_type_entry = entries[boundary_idx + 1]
        assert backup_type_entry["msg"] == "Backup type: full"
        assert backup_type_entry["phase"] == "", (
            f"Run-boundary must reset the persistent phase tracker; "
            f"got phase={backup_type_entry['phase']!r}"
        )

    def test_persisted_phase_isolated_per_profile(
        self, run_tab, store
    ) -> None:
        """Two profiles emitting concurrent events must keep their
        phase trackers separate. A background run on profile B that
        reaches ``rotator`` must not contaminate profile A's next
        message persisted under A's id.
        """
        # The widget is bound to A; B's events flow through
        # ``_on_log`` (persisted) but are filtered from the widget.
        run_tab.set_current_profile_id("A")

        # A: collector phase via the pattern matcher.
        run_tab._on_log(
            message="Collecting files...", level="info", profile_id="A"
        )
        # B: advances to rotator on a background run.
        run_tab._on_log(
            message="Rotating old backups...", level="info", profile_id="B"
        )
        # A: a message that would inherit the previous phase if the
        # tracker were shared — but A's last phase was ``collector``.
        run_tab._on_log(
            message="Backup written: 1 files to D:\\", level="info", profile_id="A"
        )
        run_tab.update_idletasks()

        a_entries = {e["msg"]: e["phase"] for e in store.load("A")}
        b_entries = {e["msg"]: e["phase"] for e in store.load("B")}
        a_written_phase = a_entries.get(
            "Backup written: 1 files to D:\\", "<missing>"
        )
        assert a_written_phase == "collector", (
            f"A's unmatched message must inherit A's phase, not B's; "
            f"got {a_written_phase!r}"
        )
        assert b_entries["Rotating old backups..."] == "rotator"
