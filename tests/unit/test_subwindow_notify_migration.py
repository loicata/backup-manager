"""Regression tests for the 3.7.41 sub-window inline-panel migration.

Context: the 3.7.34 release migrated ~17 ``messagebox.*`` call sites
in ``src/ui/app.py`` to the inline ``notify_inline`` / ``confirm_inline``
pattern but explicitly deferred 9 sites that lived in sub-windows
(``wizard.py``: 1, ``recovery_tab.py``: 5, ``history_tab.py``: 3)
because the tabs and the wizard did not hold a reference to the main
frame the inline panel needs as a parent.

The 3.7.41 release closes that gap with a small dependency-injection
pattern: ``BackupManagerApp`` passes a ``notify_fn`` (and, for
HistoryTab, also a ``confirm_fn``) to the tab constructors; the wizard
embeds ``notify_inline`` directly because it owns its ``Toplevel``.

These tests are static source-inspection guards — they make a future
refactor that accidentally re-introduces ``messagebox.showwarning`` /
``messagebox.askyesno`` in any of the 9 sites fail at the unit-test
stage instead of slipping through to a release.
"""

from __future__ import annotations

import inspect
import textwrap

from src.ui.app import BackupManagerApp
from src.ui.tabs.history_tab import HistoryTab
from src.ui.tabs.recovery_tab import RecoveryTab
from src.ui.wizard import SetupWizard


def _module_source(obj) -> str:
    """Return the full source of the module that defines ``obj``."""
    module = inspect.getmodule(obj)
    assert module is not None, f"Cannot resolve module for {obj!r}"
    return inspect.getsource(module)


def _method_source(method) -> str:
    """Return the source of one method, dedented."""
    return textwrap.dedent(inspect.getsource(method))


# ---------------------------------------------------------------------
# Module-level: no messagebox call survives in the migrated files.
# ---------------------------------------------------------------------


class TestNoMessageboxCallsRemain:
    """A ``messagebox.show*`` / ``askyesno`` re-appearing in any of
    the four migrated modules means the inline-panel migration was
    silently un-done. We grep the module source rather than mock the
    runtime because the bad calls would otherwise only surface on the
    specific UI path that triggers them.
    """

    def _assert_no_messagebox_call(self, source: str, module_label: str) -> None:
        """Reject lines that look like ``messagebox.show*(`` or ``messagebox.ask*(``.

        Comments and docstrings that just mention ``messagebox`` for
        historical context are fine — we only fail on actual calls.
        """
        for lineno, line in enumerate(source.splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            # The call form is ``messagebox.<verb>(`` — the open paren
            # is what distinguishes a real call from a docstring like
            # "previously messagebox.showwarning".
            if "messagebox.show" in stripped and "(" in stripped:
                raise AssertionError(
                    f"{module_label}:{lineno} still calls messagebox.show*: {stripped}"
                )
            if "messagebox.ask" in stripped and "(" in stripped:
                raise AssertionError(
                    f"{module_label}:{lineno} still calls messagebox.ask*: {stripped}"
                )

    def test_wizard_module_has_no_messagebox_calls(self) -> None:
        self._assert_no_messagebox_call(_module_source(SetupWizard), "wizard.py")

    def test_history_tab_module_has_no_messagebox_calls(self) -> None:
        self._assert_no_messagebox_call(_module_source(HistoryTab), "history_tab.py")

    def test_recovery_tab_module_has_no_messagebox_calls(self) -> None:
        self._assert_no_messagebox_call(_module_source(RecoveryTab), "recovery_tab.py")


# ---------------------------------------------------------------------
# Constructor injection: tabs accept the callbacks they need.
# ---------------------------------------------------------------------


class TestConstructorInjection:
    """Pin the exact public surface that ``app.py`` relies on when
    wiring the tabs. A renamed kwarg would cause a silent ``None``
    inside the tab and the user would hit ``TypeError`` only at the
    moment they trigger the validation path — that's a regression we
    want caught by ``pytest`` instead.
    """

    def test_history_tab_accepts_notify_and_confirm_callbacks(self) -> None:
        sig = inspect.signature(HistoryTab.__init__)
        assert "notify_fn" in sig.parameters, "HistoryTab must accept notify_fn"
        assert "confirm_fn" in sig.parameters, "HistoryTab must accept confirm_fn"
        # Defaults preserve the existing legacy test instantiations
        # (`HistoryTab(tk_root, log_dir=tmp_path)` is used in
        # test_history_tab_status.py without callbacks).
        assert sig.parameters["notify_fn"].default is None
        assert sig.parameters["confirm_fn"].default is None

    def test_recovery_tab_accepts_notify_callback(self) -> None:
        sig = inspect.signature(RecoveryTab.__init__)
        assert "notify_fn" in sig.parameters, "RecoveryTab must accept notify_fn"
        assert sig.parameters["notify_fn"].default is None


# ---------------------------------------------------------------------
# Wiring in app.py: BackupManagerApp passes the right callbacks.
# ---------------------------------------------------------------------


class TestAppWiresCallbacksToTabs:
    """A future refactor that drops ``notify_fn=self._notify`` from
    the ``RecoveryTab`` / ``HistoryTab`` construction would silently
    revert the migration: the tabs would still be importable, but
    every validation path would crash on ``NoneType is not callable``.
    Pin the wiring in source.
    """

    def _build_ui_source(self) -> str:
        return _method_source(BackupManagerApp._build_tabs)

    def test_recovery_tab_constructed_with_notify_fn(self) -> None:
        body = self._build_ui_source()
        assert "RecoveryTab(self.notebook, notify_fn=self._notify)" in body, (
            "RecoveryTab must be constructed with notify_fn=self._notify so "
            "validation warnings render as inline panels"
        )

    def test_history_tab_constructed_with_both_callbacks(self) -> None:
        body = self._build_ui_source()
        # The HistoryTab call may be reformatted across lines — assert
        # on the two kwarg pairs separately.
        assert "HistoryTab(" in body
        assert "notify_fn=self._notify" in body
        assert "confirm_fn=self._confirm" in body


# ---------------------------------------------------------------------
# Confirm wrapper: BackupManagerApp._confirm exists and uses the
# main-frame hide/restore recipe.
# ---------------------------------------------------------------------


class TestConfirmWrapper:
    """The HistoryTab's delete-log prompt needs a Yes/No primitive in
    the same shape as ``_notify``. ``BackupManagerApp._confirm`` is
    the wrapper that supplies the main-frame hide/restore callbacks
    so the call site stays a one-liner.
    """

    def test_confirm_exists_and_returns_bool_via_confirm_inline(self) -> None:
        assert hasattr(BackupManagerApp, "_confirm"), (
            "BackupManagerApp must expose _confirm for sub-tabs to use"
        )
        body = _method_source(BackupManagerApp._confirm)
        assert "confirm_inline(" in body
        assert "self._main_frame" in body
        assert "hide_callback=self._hide_main_layout" in body
        assert "restore_callback=self._restore_main_layout" in body
        # The wrapper returns ``.confirmed`` so call sites can use
        # ``if not confirmed: return`` directly.
        assert "return result.confirmed" in body


# ---------------------------------------------------------------------
# Migrated sites: each call site routes to the right callback.
# ---------------------------------------------------------------------


class TestRecoveryTabSitesUseNotifyFn:
    """The 5 sites in RecoveryTab._execute / _execute_local / _execute_remote
    must all call ``self._notify_fn`` with ``level="warning"``.
    Asserting on the level keeps the visual contract (amber icon, no
    auto-dismiss — user must click OK).
    """

    def test_execute_uses_notify_fn(self) -> None:
        body = _method_source(RecoveryTab._execute)
        assert "self._notify_fn(" in body, "_execute must call self._notify_fn"
        assert 'level="warning"' in body, "destination-missing must be a warning"

    def test_execute_local_uses_notify_fn(self) -> None:
        body = _method_source(RecoveryTab._execute_local)
        # 3 sites in this method: missing backup, missing path, encrypted-no-pwd.
        assert body.count("self._notify_fn(") == 3, (
            "_execute_local should have exactly 3 notify_fn call sites "
            "(missing backup, non-existent path, encrypted without password)"
        )
        assert body.count('level="warning"') >= 3

    def test_execute_remote_uses_notify_fn(self) -> None:
        body = _method_source(RecoveryTab._execute_remote)
        assert "self._notify_fn(" in body
        assert 'level="warning"' in body


class TestHistoryTabSitesUseInjectedCallbacks:
    """3 sites in HistoryTab: ``_open_selected`` (notify_fn warning),
    ``_delete_selected`` (confirm_fn destructive + notify_fn warning
    on the failure branch).
    """

    def test_open_selected_uses_notify_fn(self) -> None:
        body = _method_source(HistoryTab._open_selected)
        assert "self._notify_fn(" in body
        assert 'level="warning"' in body

    def test_delete_selected_uses_confirm_fn_and_notify_fn(self) -> None:
        body = _method_source(HistoryTab._delete_selected)
        # Yes/No prompt is now confirm_fn ...
        assert "self._confirm_fn(" in body, (
            "delete-log confirm must use the injected confirm_fn"
        )
        assert "destructive=True" in body, (
            "delete-log confirm must render the destructive (red) button"
        )
        # ... and the OS-failure branch is notify_fn warning.
        assert "self._notify_fn(" in body
        assert 'level="warning"' in body


class TestWizardValidationUsesNotifyInline:
    """The wizard owns its ``Toplevel`` so it calls ``notify_inline``
    directly (no injection). The hide/restore callbacks scope the
    panel to the content + footer area, leaving the header + progress
    bar visible so the user still sees which step they came from.
    """

    def test_go_next_calls_notify_inline_with_warning_level(self) -> None:
        body = _method_source(SetupWizard._go_next)
        assert "notify_inline(" in body
        assert 'level="warning"' in body
        assert "hide_callback=self._hide_content_for_panel" in body
        assert "restore_callback=self._restore_content_after_panel" in body

    def test_wizard_exposes_hide_restore_helpers(self) -> None:
        # The two helpers must be present and packaged together: a
        # future refactor that removes one without the other would
        # leave the wizard in a half-broken state (content hidden
        # forever after a validation error).
        assert hasattr(SetupWizard, "_hide_content_for_panel")
        assert hasattr(SetupWizard, "_restore_content_after_panel")

    def test_hide_helper_pack_forgets_both_content_and_footer(self) -> None:
        body = _method_source(SetupWizard._hide_content_for_panel)
        assert "self._content_outer.pack_forget()" in body
        assert "self._footer_frame.pack_forget()" in body

    def test_restore_helper_repacks_both_content_and_footer(self) -> None:
        body = _method_source(SetupWizard._restore_content_after_panel)
        assert 'self._content_outer.pack(fill="both", expand=True)' in body
        assert "self._footer_frame.pack(" in body
