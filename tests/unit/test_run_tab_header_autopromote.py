"""Tests for the Run tab header reflecting auto-promoted backups.

Before this fix, the header statically showed ``profile.backup_type``
(e.g. "differential") even while the engine was pipelining a FULL that
had been auto-promoted because the profile hash changed. The user had
no way to see that the in-flight run was actually a FULL.

Two paths are tested:
  1. Live override via the ``BACKUP_TYPE_DETERMINED`` event (fires
     after ``_maybe_force_full``).
  2. Post-backup annotation when the profile is re-displayed: if
     ``last_backup`` and ``last_full_backup`` are within a few seconds,
     the last run was auto-promoted and the header says so.
"""

from datetime import datetime, timedelta

import pytest

from src.ui.tabs.run_tab import RunTab


@pytest.fixture()
def run_tab(tk_root):
    tab = RunTab(tk_root)
    yield tab
    tab.destroy()


def _label_text(tab: RunTab) -> str:
    return tab.profile_label.cget("text")


class TestConfiguredTypeDisplay:
    """Header shows the configured backup_type on normal loads."""

    def test_diff_with_real_prior_diff_shows_plain_differential(self, run_tab):
        now = datetime(2026, 4, 17, 22, 0, 0)
        week_ago = now - timedelta(days=7)
        run_tab.update_profile_info("P", "differential", now.isoformat(), week_ago.isoformat())
        assert "Type: differential |" in _label_text(run_tab)
        assert "auto-promoted" not in _label_text(run_tab)

    def test_full_profile_is_never_annotated(self, run_tab):
        """User-configured FULL runs its FULL on every pass — no
        auto-promotion annotation, ever."""
        now = datetime(2026, 4, 17, 22, 0, 0).isoformat()
        run_tab.update_profile_info("P", "full", now, now)
        assert "Type: full |" in _label_text(run_tab)
        assert "auto-promoted" not in _label_text(run_tab)

    def test_diff_never_run_shows_will_auto_promote_hint(self, run_tab):
        run_tab.update_profile_info("P", "differential", "", "")
        assert "will auto-promote" in _label_text(run_tab)


class TestLastRunAutoPromotedAnnotation:
    """After an auto-promoted FULL, ``last_backup`` and ``last_full_backup``
    land within seconds of each other. Surface that to the user."""

    def test_diff_with_close_full_timestamp_is_flagged(self, run_tab):
        last = datetime(2026, 4, 17, 22, 0, 0)
        last_full = last - timedelta(seconds=2)
        run_tab.update_profile_info(
            "P",
            "differential",
            last.isoformat(),
            last_full.isoformat(),
        )
        text = _label_text(run_tab)
        assert "last run: full (auto-promoted)" in text

    def test_diff_with_old_full_is_not_flagged(self, run_tab):
        """Real DIFF after a day-old FULL must not be annotated."""
        last = datetime(2026, 4, 17, 22, 0, 0)
        last_full = last - timedelta(days=3)
        run_tab.update_profile_info(
            "P",
            "differential",
            last.isoformat(),
            last_full.isoformat(),
        )
        assert "auto-promoted" not in _label_text(run_tab)

    def test_auto_promote_detection_under_five_minute_window(self, run_tab):
        """5-minute tolerance covers the worst case where
        ``_phase_update_delta`` and the UI success callback fire with
        several seconds between them."""
        last = datetime(2026, 4, 17, 22, 5, 0)
        last_full = last - timedelta(seconds=280)  # 4 min 40s, still within 5 min
        run_tab.update_profile_info(
            "P",
            "differential",
            last.isoformat(),
            last_full.isoformat(),
        )
        assert "last run: full (auto-promoted)" in _label_text(run_tab)

    def test_beyond_five_minute_window_is_not_flagged(self, run_tab):
        last = datetime(2026, 4, 17, 22, 10, 0)
        last_full = last - timedelta(seconds=600)  # 10 min apart
        run_tab.update_profile_info(
            "P",
            "differential",
            last.isoformat(),
            last_full.isoformat(),
        )
        assert "auto-promoted" not in _label_text(run_tab)


class TestLiveAutoPromoteOverride:
    """During a running backup, the ``BACKUP_TYPE_DETERMINED`` event
    takes over the header so the user sees the live effective type."""

    def test_forced_full_overrides_header_with_auto_promoted(self, run_tab):
        run_tab.update_profile_info("P", "differential", "", "")
        run_tab._apply_active_backup_type("full", forced_full=True)
        assert "Type: full (auto-promoted) |" in _label_text(run_tab)

    def test_non_forced_full_just_shows_its_type(self, run_tab):
        """When a DIFF is running as a DIFF (no promotion), the event
        still fires but does not annotate."""
        run_tab.update_profile_info("P", "differential", "2026-04-17T10:00:00", "")
        run_tab._apply_active_backup_type("differential", forced_full=False)
        text = _label_text(run_tab)
        assert "Type: differential |" in text
        assert "auto-promoted" not in text

    def test_override_is_no_op_without_baseline(self, run_tab):
        """Before a profile is loaded, the event has no context to
        apply — must not raise."""
        run_tab._profile_info_baseline = None
        run_tab._apply_active_backup_type("full", forced_full=True)  # must not raise
