"""Tests for the v3.7.5 scheduler-seeding fix on post-init profile creation.

Regression (v3.7.4 and earlier): ``BackupManagerApp._new_profile`` called
``scheduler.mark_triggered_now`` AFTER ``_load_profiles``. On Windows
``_load_profiles`` is synchronous on the main Tk thread and can take
5-10 s on a 28-KB schedule journal + 11-tab refresh. During that
window the scheduler daemon (CHECK_INTERVAL = 30 s) ticks, sees the
new profile with ``last_trigger is None`` in ``_state``, and
``_is_due`` returns True on the first branch — firing an
**unwanted backup** of the brand-new profile before the user could
review it. Additionally, ``mark_verify_now`` was never called on the
``_new_profile`` / ``_relaunch_wizard_after_delete`` paths at all,
so the v3.7.4 periodic-verify timer fix did not apply to profiles
created from the second-and-later cycle.

Fix: a single helper ``_seed_scheduler_for_new_profile`` calls BOTH
``mark_triggered_now`` and ``mark_verify_now`` on the profile, and is
invoked IMMEDIATELY after ``save_profile``, BEFORE ``_load_profiles``.
"""

from __future__ import annotations

import inspect
import textwrap
from unittest.mock import MagicMock

from src.core.config import BackupProfile
from src.ui.app import BackupManagerApp


def _make_app_skeleton() -> BackupManagerApp:
    """Build a ``BackupManagerApp`` without running ``__init__``.

    ``BackupManagerApp.__init__`` boots the full Tk window, scheduler
    thread, tray icon, and 11 tabs. None of that is reachable inside a
    headless pytest run, and most of it is irrelevant to seeding
    behaviour anyway. Bypassing ``__init__`` via ``__new__`` lets us
    pin the scheduler attribute as a ``MagicMock`` and exercise the
    helper in isolation.
    """
    app = BackupManagerApp.__new__(BackupManagerApp)
    app.scheduler = MagicMock()
    return app


class TestSeedHelper:
    """``_seed_scheduler_for_new_profile`` arms BOTH timers."""

    def test_helper_calls_both_mark_apis(self) -> None:
        """The helper must call ``mark_triggered_now`` and
        ``mark_verify_now`` on the same profile id.

        Both calls are required so the next scheduler tick:
        - does not trigger a backup (``_is_due`` first branch),
        - does not trigger a periodic verify (defence-in-depth: even
          if ``_check_verify_due``'s ``last_verify is None`` seed
          branch regressed, this explicit seed records the user's
          intent at creation time).
        """
        app = _make_app_skeleton()
        profile = BackupProfile(id="abc123", name="MyProfile")

        app._seed_scheduler_for_new_profile(profile)

        app.scheduler.mark_triggered_now.assert_called_once()
        app.scheduler.mark_verify_now.assert_called_once()

        triggered_args = app.scheduler.mark_triggered_now.call_args
        verify_args = app.scheduler.mark_verify_now.call_args

        assert triggered_args.args[0] == "abc123"
        assert verify_args.args[0] == "abc123"

    def test_helper_passes_same_timestamp_to_both(self) -> None:
        """Both seed calls share one ``datetime.now()`` snapshot.

        Using a single timestamp keeps the persisted state internally
        consistent and avoids a (very narrow) window where a tick
        could read ``last_trigger`` from instant T1 and
        ``last_verify`` from T2 — the helper's contract is "this
        profile was just created at moment X for both clocks".
        """
        app = _make_app_skeleton()
        profile = BackupProfile(id="abc123", name="MyProfile")

        app._seed_scheduler_for_new_profile(profile)

        triggered_ts = app.scheduler.mark_triggered_now.call_args.args[1]
        verify_ts = app.scheduler.mark_verify_now.call_args.args[1]
        assert triggered_ts == verify_ts


class TestNewProfileOrdering:
    """The seed must happen BEFORE the long ``_load_profiles`` call.

    The race window the seed closes is between ``save_profile`` and the
    end of ``_load_profiles`` — that's where the scheduler daemon tick
    used to see the new profile with both timers unset. A static
    inspection of the method body is enough to pin the ordering: any
    future refactor that moves the seed back below ``_load_profiles``
    will fail this test.
    """

    def test_new_profile_seeds_before_load_profiles(self) -> None:
        """``_seed_scheduler_for_new_profile`` precedes ``self._load_profiles()``
        in the source of ``BackupManagerApp._new_profile``.

        ``inspect.getsource`` returns the dedented function body; a
        substring comparison of the two anchor lines' positions
        captures the required ordering without booting any UI.
        """
        src = textwrap.dedent(inspect.getsource(BackupManagerApp._new_profile))
        seed_pos = src.find("_seed_scheduler_for_new_profile(profile)")
        load_pos = src.find("self._load_profiles()")
        assert seed_pos != -1, "helper call missing in _new_profile"
        assert load_pos != -1, "_load_profiles call missing in _new_profile"
        assert seed_pos < load_pos, (
            "scheduler seed must precede _load_profiles to close the "
            "race window where the daemon tick can fire a backup on "
            "the brand-new profile."
        )

    def test_relaunch_wizard_after_delete_seeds_before_load_profiles(self) -> None:
        """Same ordering on the sister path used when the user deletes
        the last profile and the wizard auto-relaunches."""
        src = textwrap.dedent(inspect.getsource(BackupManagerApp._relaunch_wizard_after_delete))
        seed_pos = src.find("_seed_scheduler_for_new_profile(profile)")
        load_pos = src.find("self._load_profiles()")
        assert seed_pos != -1
        assert load_pos != -1
        assert seed_pos < load_pos
