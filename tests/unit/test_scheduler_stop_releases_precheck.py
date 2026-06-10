"""Tests for releasing the scheduler thread from a pending precheck prompt.

Audit #11: the scheduler runs backups (and the destinations-unavailable
precheck prompt) synchronously on a single thread. Sequential backups are
by design, but an UNANSWERED precheck prompt used to pin that thread for
the full 30-minute timeout — including across app exit. The wait is now
stop-aware: a scheduler stop releases it promptly.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from src.core.scheduler import InAppScheduler
from src.ui.app import BackupManagerApp


def test_is_stopping_false_until_stop(tmp_path):
    sched = InAppScheduler(tmp_path, get_profiles=lambda: [], backup_callback=lambda p: None)
    assert sched.is_stopping() is False
    sched.stop()
    assert sched.is_stopping() is True


def test_precheck_prompt_releases_on_scheduler_stop():
    # A bare app — _scheduled_precheck_prompt only touches root, scheduler,
    # and the alert show/hide helpers.
    app = BackupManagerApp.__new__(BackupManagerApp)
    app.root = MagicMock()
    app.scheduler = MagicMock()
    app.scheduler.is_stopping.return_value = True  # app is exiting
    app._show_target_alert = MagicMock()
    app._hide_target_alert = MagicMock()

    result = app._scheduled_precheck_prompt(
        [("Storage", "unreachable", False, "USB G:")],
        MagicMock(),  # profile
        MagicMock(),  # engine
    )

    # Must bail out immediately instead of blocking for the 30-min timeout.
    assert result == "timeout"
    # The alert dismissal is marshalled to the Tk thread via root.after().
    app.root.after.assert_any_call(0, app._hide_target_alert)
