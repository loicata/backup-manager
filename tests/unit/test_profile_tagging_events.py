"""Tests for the v3.7.12 per-profile event filtering.

Regression (v3.7.11 and earlier): the Run tab subscribed to PROGRESS
/ LOG / STATUS / PHASE_CHANGED / PHASE_COUNT / BACKUP_TYPE_DETERMINED
from a single shared ``EventBus`` and accepted every event regardless
of which profile produced it. On a scheduler-driven crash-recovery
run (17/05/2026 case: TestLoic backup re-firing on app launch after a
v3.7.10 install-time cancel), the user clicked another profile in
the sidebar and the Run-tab kept showing TestLoic's bar moving, its
"Copying to Storage…" status, and file paths from TestLoic's source
tree — making the user believe the *other* profile was being copied.

Fix: ``BackupEngine.run_backup`` wraps its EventBus in
``ProfileTaggingEventBus`` for the duration of the run; every emit
gets a ``profile_id`` kwarg. The Run-tab handlers compare each
incoming event's ``profile_id`` to ``self._current_profile_id`` (set
on every sidebar switch via ``set_current_profile_id``) and drop the
event when they do not match.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.core.events import LOG, PROGRESS, STATUS, EventBus, ProfileTaggingEventBus
from src.ui.tabs.run_tab import RunTab


class TestProfileTaggingEventBusEmit:
    """The wrapper must inject ``profile_id`` on every emit."""

    def test_emit_adds_profile_id_kwarg(self) -> None:
        inner = MagicMock()
        bus = ProfileTaggingEventBus(inner, profile_id="abc123")
        bus.emit(PROGRESS, current=10, total=100)
        inner.emit.assert_called_once_with(
            PROGRESS,
            current=10,
            total=100,
            profile_id="abc123",
        )

    def test_emit_preserves_explicit_profile_id(self) -> None:
        """An explicit ``profile_id`` in the kwargs wins over the
        wrapper default — lets future callers override per emit.
        """
        inner = MagicMock()
        bus = ProfileTaggingEventBus(inner, profile_id="default")
        bus.emit(LOG, message="x", profile_id="explicit")
        kwargs = inner.emit.call_args.kwargs
        assert kwargs["profile_id"] == "explicit"

    def test_subscribe_unsubscribe_delegate_to_inner(self) -> None:
        inner = MagicMock()
        bus = ProfileTaggingEventBus(inner, profile_id="abc")
        cb = MagicMock()
        bus.subscribe(PROGRESS, cb)
        inner.subscribe.assert_called_once_with(PROGRESS, cb)
        bus.unsubscribe(PROGRESS, cb)
        inner.unsubscribe.assert_called_once_with(PROGRESS, cb)


class TestRunTabFiltering:
    """Run-tab handlers drop events whose ``profile_id`` does not match
    the currently-selected profile in the sidebar."""

    @pytest.fixture()
    def run_tab(self, tk_root):
        tab = RunTab(tk_root)
        # Simulate a backup being active so the gates in
        # _on_progress / _on_log do not short-circuit before our
        # profile filter has a chance to run.
        tab._backup_active = True
        yield tab
        tab.destroy()

    def test_event_with_no_profile_id_passes_through(self, run_tab) -> None:
        """Backwards compat: an event with no ``profile_id`` (e.g.
        tray-level events, tests without the wrapper) must not be
        dropped — otherwise existing emitters would silently stop
        reaching the UI.
        """
        run_tab.set_current_profile_id("profile_a")
        # No filter rejection means the handler enters its body.
        # We check the side effect on _phase_weights which is set
        # in _on_phase_count.
        run_tab._on_phase_count(weights={"hash": 1})
        assert run_tab._phase_weights == {"hash": 1}

    def test_event_with_matching_profile_id_passes(self, run_tab) -> None:
        run_tab.set_current_profile_id("profile_a")
        run_tab._on_phase_count(weights={"hash": 1}, profile_id="profile_a")
        assert run_tab._phase_weights == {"hash": 1}

    def test_event_with_foreign_profile_id_is_dropped(self, run_tab) -> None:
        """The 17/05/2026 user scenario: an event from another
        profile's backup must not mutate Run-tab state.
        """
        run_tab.set_current_profile_id("profile_a")
        run_tab._phase_weights.clear()
        run_tab._on_phase_count(weights={"hash": 1}, profile_id="profile_b")
        assert run_tab._phase_weights == {}

    def test_no_current_profile_accepts_everything(self, run_tab) -> None:
        """Before any profile is selected (cold start), events flow
        through so the very first selection inherits whatever has
        accumulated in the meantime.
        """
        run_tab.set_current_profile_id("")
        run_tab._phase_weights.clear()
        run_tab._on_phase_count(weights={"hash": 1}, profile_id="profile_z")
        assert run_tab._phase_weights == {"hash": 1}

    def test_set_current_profile_id_normalizes_none(self, run_tab) -> None:
        """A ``None`` argument is coerced to empty string so the
        check ``if not self._current_profile_id`` works uniformly.
        """
        run_tab.set_current_profile_id(None)
        assert run_tab._current_profile_id == ""


class TestEndToEndTagging:
    """Wire up engine-side tagging to UI-side filtering with a real
    ``EventBus`` so a regression on either side trips the test."""

    @pytest.fixture()
    def run_tab(self, tk_root):
        tab = RunTab(tk_root)
        tab._backup_active = True
        yield tab
        tab.destroy()

    def test_tagged_event_through_real_bus_reaches_correct_profile(self, run_tab) -> None:
        """Emits via a wrapper into the same EventBus the RunTab is
        subscribed to. The handler runs synchronously inside ``emit``,
        so the ``_phase_weights`` side effect is observable
        immediately.
        """
        bus = EventBus()
        bus.subscribe("phase_count", run_tab._on_phase_count)

        run_tab.set_current_profile_id("matching")
        tagged_a = ProfileTaggingEventBus(bus, profile_id="matching")
        tagged_a.emit("phase_count", weights={"hash": 7})
        assert run_tab._phase_weights == {"hash": 7}

        # An event from a different profile must NOT clobber the
        # previously-set weights.
        tagged_b = ProfileTaggingEventBus(bus, profile_id="other")
        tagged_b.emit("phase_count", weights={"write": 99})
        assert run_tab._phase_weights == {"hash": 7}

    def test_status_event_filter_does_not_flip_backup_active_for_foreign(self, run_tab) -> None:
        """A STATUS=success event tagged with another profile must
        not flip this tab's ``_backup_active`` to False. Otherwise
        the next legitimate PROGRESS event from the user's profile
        would be silently dropped by the ``not self._backup_active``
        gate inside ``_on_progress``.
        """
        run_tab.set_current_profile_id("matching")
        run_tab._backup_active = True
        run_tab._on_status(state="success", profile_id="other")
        # Defer to the Tk main loop is not needed here — the filter
        # runs before any ``after`` scheduling. The flag is unchanged.
        # (The filter returns BEFORE _update_status is scheduled.)
        # Force a flush of any after() callbacks so the test does not
        # leave stray callbacks dangling.
        run_tab.update_idletasks()
        assert run_tab._backup_active is True
