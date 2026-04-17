"""Tests for the General tab auto-bump behavior on Full->Differential transition.

Scenario: a user created a profile via the Classic wizard with
(backup_type=FULL, schedule=WEEKLY, gfs_daily=1). Manually flipping the
backup type to Differential in the General tab would previously trigger
the Save-time validation popup "Full backup cycle must not exceed daily
retention" because gfs_daily (1) < full_backup_every (7).

The tab now performs a one-shot bump of the Retention tab's daily value
to match the cycle, and displays an info label while DIFF is selected.
These tests lock that behavior in.
"""

import pytest

from src.core.config import (
    BackupProfile,
    BackupType,
    RetentionConfig,
    RetentionPolicy,
)
from src.ui.tabs.general_tab import GeneralTab
from src.ui.tabs.retention_tab import RetentionTab


@pytest.fixture()
def wired_tabs(tk_root):
    """Create fresh General + Retention tabs wired the same way as the app."""
    general = GeneralTab(tk_root)
    retention = RetentionTab(tk_root)
    general.set_retention_tab(retention)
    yield general, retention
    general.destroy()
    retention.destroy()


def _profile(
    backup_type: BackupType, gfs_daily_internal: int, full_every: int = 7
) -> BackupProfile:
    """Build a minimal profile for load_profile()."""
    return BackupProfile(
        name="t",
        backup_type=backup_type,
        full_backup_every=full_every,
        retention=RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=gfs_daily_internal,
            gfs_weekly=3,
            gfs_monthly=5,
            gfs_enabled=True,
        ),
    )


class TestFullToDiffBump:
    """Verify the one-shot bump at the Full->Diff transition."""

    def test_bump_raises_retention_when_below_cycle(self, wired_tabs):
        general, retention = wired_tabs
        retention.load_profile(_profile(BackupType.FULL, gfs_daily_internal=2))
        general.load_profile(_profile(BackupType.FULL, gfs_daily_internal=2, full_every=7))

        # User flips to Differential
        general.type_var.set(BackupType.DIFFERENTIAL.value)

        # UI value = internal - 1; cycle=7 -> internal must be >=7 -> UI >=6
        assert retention.get_gfs_daily_var().get() == 6
        assert retention.collect_config()["retention"].gfs_daily == 7

    def test_no_bump_when_retention_already_sufficient(self, wired_tabs):
        general, retention = wired_tabs
        retention.load_profile(_profile(BackupType.FULL, gfs_daily_internal=30))
        general.load_profile(_profile(BackupType.FULL, gfs_daily_internal=30, full_every=7))

        general.type_var.set(BackupType.DIFFERENTIAL.value)

        # Already at 30 internal (UI=29), must not be lowered
        assert retention.get_gfs_daily_var().get() == 29

    def test_no_bump_when_cycle_changes_while_in_diff(self, wired_tabs):
        """Per requirement: re-bump happens ONLY on Full->Diff transition, not on subsequent
        cycle edits. If the user then raises the cycle, validation will catch an incoherent
        config at save time."""
        general, retention = wired_tabs
        retention.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=7))
        general.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=7, full_every=7))

        # Simulate the user changing the cycle to a value > retention while already in DIFF
        general.full_every_var.set(7)  # no-op write but confirms no trace on cycle

        assert retention.get_gfs_daily_var().get() == 6  # unchanged (internal=7)

    def test_diff_to_full_does_not_touch_retention(self, wired_tabs):
        general, retention = wired_tabs
        retention.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=7))
        general.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=7, full_every=7))

        general.type_var.set(BackupType.FULL.value)

        # Retention value preserved
        assert retention.get_gfs_daily_var().get() == 6

    def test_full_diff_full_diff_bumps_again_if_needed(self, wired_tabs):
        general, retention = wired_tabs
        retention.load_profile(_profile(BackupType.FULL, gfs_daily_internal=2))
        general.load_profile(_profile(BackupType.FULL, gfs_daily_internal=2, full_every=7))

        # First Full -> Diff: bump
        general.type_var.set(BackupType.DIFFERENTIAL.value)
        assert retention.get_gfs_daily_var().get() == 6

        # Manually lower retention in the Retention tab
        retention.get_gfs_daily_var().set(1)  # UI=1 -> internal=2

        # Diff -> Full
        general.type_var.set(BackupType.FULL.value)
        assert retention.get_gfs_daily_var().get() == 1  # no change on reverse transition

        # Full -> Diff again: bump again since internal=2 < cycle=7
        general.type_var.set(BackupType.DIFFERENTIAL.value)
        assert retention.get_gfs_daily_var().get() == 6


class TestLoadProfileDoesNotBump:
    """load_profile must never trigger the auto-bump (reflects persisted state)."""

    def test_load_diff_profile_with_low_retention_no_bump(self, wired_tabs):
        general, retention = wired_tabs
        # Persisted state: DIFF + low retention (e.g., imported legacy profile)
        retention.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=2))
        general.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=2, full_every=7))

        # Retention must stay as persisted; validation will alert the user at Save time
        assert retention.get_gfs_daily_var().get() == 1  # UI=1, internal=2

    def test_load_full_then_switch_to_diff_bumps_once(self, wired_tabs):
        general, retention = wired_tabs
        retention.load_profile(_profile(BackupType.FULL, gfs_daily_internal=2))
        general.load_profile(_profile(BackupType.FULL, gfs_daily_internal=2, full_every=7))

        # Load did not bump
        assert retention.get_gfs_daily_var().get() == 1  # UI=1, internal=2

        # User switch triggers the bump
        general.type_var.set(BackupType.DIFFERENTIAL.value)
        assert retention.get_gfs_daily_var().get() == 6


class TestRetentionInfoLabel:
    """Label visibility and text behavior across transitions."""

    def test_label_visible_in_diff(self, wired_tabs):
        general, retention = wired_tabs
        retention.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=7))
        general.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=7, full_every=7))

        assert general._retention_info_label.winfo_manager() != ""
        assert "Retention: 7 days" in general._retention_info_label.cget("text")

    def test_label_hidden_in_full(self, wired_tabs):
        general, retention = wired_tabs
        retention.load_profile(_profile(BackupType.FULL, gfs_daily_internal=2))
        general.load_profile(_profile(BackupType.FULL, gfs_daily_internal=2, full_every=7))

        assert general._retention_info_label.winfo_manager() == ""

    def test_label_updates_when_retention_changes(self, wired_tabs):
        general, retention = wired_tabs
        retention.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=7))
        general.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=7, full_every=7))

        # User raises daily retention to 14 (UI=13, internal=14) in the Retention tab
        retention.get_gfs_daily_var().set(13)

        assert "Retention: 14 days" in general._retention_info_label.cget("text")

    def test_label_hidden_after_diff_to_full(self, wired_tabs):
        general, retention = wired_tabs
        retention.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=7))
        general.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=7, full_every=7))

        assert general._retention_info_label.winfo_manager() != ""

        general.type_var.set(BackupType.FULL.value)

        assert general._retention_info_label.winfo_manager() == ""

    def test_label_message_references_retention_tab_not_gfs(self, wired_tabs):
        """The end-user does not know what 'GFS' means. The label must use the
        tab name 'Retention' instead."""
        general, retention = wired_tabs
        retention.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=7))
        general.load_profile(_profile(BackupType.DIFFERENTIAL, gfs_daily_internal=7, full_every=7))

        text = general._retention_info_label.cget("text").lower()
        assert "retention" in text
        assert "gfs" not in text
