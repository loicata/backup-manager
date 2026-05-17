"""Tests for the v3.7.6 ``select_first`` kwarg on ``_load_profiles``.

Regression (v3.7.5 and earlier): three call sites — ``_new_profile``,
``_move_profile_up``, ``_move_profile_down`` — invoked
``_load_profiles()`` and then immediately loaded a specific profile
(the newly-created one, the moved one) via ``_load_profile`` /
``_reselect_profile``. ``_load_profiles`` itself ended by calling
``_load_profile(first_active_profile)`` for the side-effect of having
a profile visible after a startup reload, so the 11-tab fan-out plus
the health-dashboard refresh ran TWICE per call: once on the
first_active, once on the actually-targeted profile. On a populated
config that's the difference between a ~5 s freeze and a ~10 s freeze
after Finish in the new-profile wizard.

Fix: ``_load_profiles`` accepts ``select_first: bool = True`` (default
preserves startup behaviour). Callers that own the post-reload
selection pass ``select_first=False``.
"""

from __future__ import annotations

import inspect
import textwrap
from unittest.mock import MagicMock

from src.ui.app import BackupManagerApp


def _make_app_skeleton() -> BackupManagerApp:
    """Build a ``BackupManagerApp`` without running ``__init__``."""
    app = BackupManagerApp.__new__(BackupManagerApp)
    return app


class TestLoadProfilesSelectFirstSignature:
    """Contract on the new kwarg."""

    def test_load_profiles_accepts_select_first_kwarg(self) -> None:
        """The signature pins ``select_first`` as a keyword-only arg
        with default True, so existing positional callers don't break.
        """
        sig = inspect.signature(BackupManagerApp._load_profiles)
        assert "select_first" in sig.parameters
        param = sig.parameters["select_first"]
        assert param.default is True
        assert param.kind == inspect.Parameter.KEYWORD_ONLY


class TestCallSitesPassSelectFirstFalse:
    """The three callers that own their selection must pass False.

    A static check on the function body catches a future refactor that
    accidentally re-introduces the double load — much cheaper than a
    full-UI integration test, and tied to the exact mechanism rather
    than a symptom (freeze duration).
    """

    def _body(self, method) -> str:
        return textwrap.dedent(inspect.getsource(method))

    def test_new_profile_passes_select_first_false(self) -> None:
        assert "self._load_profiles(select_first=False)" in self._body(
            BackupManagerApp._new_profile
        )

    def test_move_profile_up_passes_select_first_false(self) -> None:
        assert "self._load_profiles(select_first=False)" in self._body(
            BackupManagerApp._move_profile_up
        )

    def test_move_profile_down_passes_select_first_false(self) -> None:
        assert "self._load_profiles(select_first=False)" in self._body(
            BackupManagerApp._move_profile_down
        )


class TestStartupCallersKeepDefault:
    """Sites whose role IS to display "the" current profile (e.g. the
    first-launch path) must keep the default ``select_first=True``,
    otherwise the user sees a populated sidebar with empty tabs.

    A simple way to test this without booting Tk is to check that the
    affected methods do NOT contain the explicit ``select_first=False``
    kwarg — they rely on the default.
    """

    def _body(self, method) -> str:
        return textwrap.dedent(inspect.getsource(method))

    def test_save_profile_does_not_force_false(self) -> None:
        # _save_profile calls _load_profiles() at the end; it does not
        # immediately re-select a known profile and therefore must
        # NOT pass select_first=False (otherwise the user sees a sidebar
        # with no tab content after Save).
        assert "select_first=False" not in self._body(BackupManagerApp._save_profile)

    def test_relaunch_wizard_after_delete_keeps_default(self) -> None:
        # _relaunch_wizard_after_delete also creates a profile but the
        # destination of the load is the brand-new profile itself,
        # which IS the first_active. Keeping select_first=True (default)
        # means the load happens once via the default path. Passing
        # False would leave the tabs blank.
        assert "select_first=False" not in self._body(
            BackupManagerApp._relaunch_wizard_after_delete
        )


class TestLoadProfilesEffect:
    """Behavioural contract: with select_first=False, no implicit
    ``_load_profile`` call is made when there is a first_active.

    The full ``_load_profiles`` body builds a Tk Listbox, which we
    cannot exercise headlessly. The skeleton-instance approach
    (``__new__``) means we can patch the listbox + config_manager
    well enough to drive the branch under test.
    """

    def test_select_first_false_skips_implicit_load_profile(self) -> None:
        from src.core.config import BackupProfile

        app = _make_app_skeleton()
        app.profile_listbox = MagicMock()
        # itemconfig is the only listbox method that actually returns
        # a value used downstream; the others can be plain Mocks.
        app.profile_listbox.itemconfig = MagicMock()
        app.profile_listbox.insert = MagicMock()
        app.profile_listbox.delete = MagicMock()
        app.profile_listbox.select_set = MagicMock()

        active_profile = BackupProfile(id="a1", name="A", active=True)
        app.config_manager = MagicMock()
        app.config_manager.get_all_profiles.return_value = [active_profile]
        app._load_profile = MagicMock()

        app._load_profiles(select_first=False)

        app._load_profile.assert_not_called()

    def test_select_first_true_invokes_load_profile_on_first_active(self) -> None:
        from src.core.config import BackupProfile

        app = _make_app_skeleton()
        app.profile_listbox = MagicMock()
        app.profile_listbox.itemconfig = MagicMock()
        app.profile_listbox.insert = MagicMock()
        app.profile_listbox.delete = MagicMock()
        app.profile_listbox.select_set = MagicMock()

        active_profile = BackupProfile(id="a1", name="A", active=True)
        app.config_manager = MagicMock()
        app.config_manager.get_all_profiles.return_value = [active_profile]
        app._load_profile = MagicMock()

        app._load_profiles()  # default select_first=True

        app._load_profile.assert_called_once_with(active_profile)
