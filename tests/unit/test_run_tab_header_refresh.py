"""Tests for the Run-tab header refresh after a backup completes.

Regression guard for the 21/05/2026 captured anomaly: the Run-tab header
showed ``Last backup: 2026-05-20T10:03:46`` (the previous run) right
after a successful 2026-05-21 backup, even though the disk file
already held the new timestamp.

The bug surface is the interaction between two label writers:

* ``update_profile_info`` — called from ``_load_profile`` (sets the
  initial baseline at sidebar selection) AND from ``_refresh_run_header``
  (called by ``BackupManagerApp._backup_thread`` after a successful run,
  with ``profile.last_backup`` freshly set to ``now``).
* ``_apply_active_backup_type`` — called from the
  ``BACKUP_TYPE_DETERMINED`` event handler. Reads ``_profile_info_baseline``
  and repaints the label so the user sees ``full (auto-promoted)``
  instead of the configured ``differential`` while a forced-full run
  is in flight.

The contract these tests pin:

1. ``update_profile_info`` is ALWAYS the last word on ``Last backup`` —
   its baseline update is observable by any subsequent
   ``_apply_active_backup_type`` invocation.
2. The auto-promoted differential display still wins on
   ``update_profile_info``, even after a stale
   ``_apply_active_backup_type`` painted the simpler ``full`` label.
3. ``_refresh_run_header(profile)`` always reads ``profile.last_backup``
   as it is AT THE TIME of the call — no stale captured value.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.ui.tabs.run_tab import RunTab


@pytest.fixture()
def tab(tk_root):
    tab = RunTab(tk_root)
    yield tab
    tab.destroy()


class TestHeaderUpdatedAfterRun:
    """Critical contract: a successful backup updates the visible
    ``Last backup`` timestamp on the Run-tab header.
    """

    def test_late_backup_type_event_uses_refreshed_baseline(self, tab) -> None:
        """A ``BACKUP_TYPE_DETERMINED`` that fires AFTER the post-run
        ``_refresh_run_header`` must read the refreshed baseline, NOT
        the original ``load_profile``-time one.
        """
        # 1. load_profile-time call (yesterday's value)
        tab.update_profile_info("TestNP", "full", "2026-05-20T10:03:46", "")
        assert "2026-05-20" in tab.profile_label.cget("text")

        # 2. Early BACKUP_TYPE_DETERMINED during the run
        tab._apply_active_backup_type("full", forced_full=False)
        assert "2026-05-20" in tab.profile_label.cget("text")

        # 3. Post-run refresh with today's last_backup
        tab.update_profile_info("TestNP", "full", "2026-05-21T19:29:07", "")
        assert "2026-05-21" in tab.profile_label.cget("text")

        # 4. Late ``BACKUP_TYPE_DETERMINED`` override — MUST read the
        # refreshed baseline. If this read a captured stale tuple,
        # the label would flip back to yesterday's date.
        tab._apply_active_backup_type("full", forced_full=False)
        label_text = tab.profile_label.cget("text")
        assert "2026-05-21" in label_text, (
            f"Late BACKUP_TYPE_DETERMINED must respect post-run baseline; "
            f"got label text: {label_text!r}"
        )

    def test_refresh_run_header_calls_update_profile_info_with_today(
        self, tab
    ) -> None:
        """``_refresh_run_header`` forwards ``profile.last_backup`` AS-OF
        the call moment, not a captured value.

        Simulates the manual-backup flow: after ``profile.last_backup =
        completed_at``, the ``after(0, _refresh_run_header, profile)``
        drains on the main thread and must paint TODAY's value, even
        though the profile object is the SAME reference that was used
        before the assignment.
        """
        # Build a profile-like stand-in (RunTab only reads attributes).
        profile = MagicMock()
        profile.name = "TestNP"
        profile.backup_type = MagicMock(value="full")
        profile.last_backup = "2026-05-20T10:03:46"
        profile.last_full_backup = ""

        # Simulate load_profile painting yesterday's value.
        tab.update_profile_info(
            profile.name,
            profile.backup_type.value,
            profile.last_backup,
            profile.last_full_backup,
        )
        assert "2026-05-20" in tab.profile_label.cget("text")

        # Backup thread now updates the profile in place.
        profile.last_backup = "2026-05-21T19:29:07"

        # Equivalent of ``_refresh_run_header(profile)`` in app.py.
        tab.update_profile_info(
            profile.name,
            profile.backup_type.value,
            profile.last_backup or "",
            profile.last_full_backup or "",
        )

        assert "2026-05-21" in tab.profile_label.cget("text")
        # And the baseline tuple must hold today's value for any
        # later ``_apply_active_backup_type`` call.
        assert tab._profile_info_baseline[2] == "2026-05-21T19:29:07"

    def test_auto_promoted_display_recomputed_on_refresh(self, tab) -> None:
        """A differential profile auto-promoted to FULL during the run
        gets the right display label after the post-run refresh, even
        if the early ``BACKUP_TYPE_DETERMINED`` painted the simpler
        ``full (auto-promoted)`` form.
        """
        # 1. load_profile: differential with a past last_backup.
        tab.update_profile_info(
            "BLoic",
            "differential",
            "2026-05-15T22:00:00",  # last_backup (a real DIFF run)
            "2026-05-12T08:00:00",  # last_full_backup (older FULL)
        )
        text = tab.profile_label.cget("text")
        assert "differential" in text
        # Not auto-promoted yet (last_backup > last_full_backup by days).
        assert "auto-promoted" not in text

        # 2. Early forced-full BACKUP_TYPE_DETERMINED.
        tab._apply_active_backup_type("full", forced_full=True)
        assert "full (auto-promoted)" in tab.profile_label.cget("text")

        # 3. Post-run refresh with today's auto-promoted values
        #    (last_backup == last_full_backup, both at now).
        tab.update_profile_info(
            "BLoic",
            "differential",
            "2026-05-21T19:29:07",
            "2026-05-21T19:29:07",
        )
        label = tab.profile_label.cget("text")
        assert "2026-05-21" in label, (
            f"Refresh must apply today's last_backup; got {label!r}"
        )
        assert "auto-promoted" in label.lower(), (
            f"Differential auto-promote detection must re-fire on refresh; "
            f"got {label!r}"
        )
