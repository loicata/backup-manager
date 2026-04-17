"""Tests for the General tab one-shot autoconfig on Full->Differential.

The tab auto-configures Schedule (to Daily) and Retention (daily bump to
cover the full-backup cycle) only on the VERY FIRST Full->Differential
transition per profile. The gate lives on BackupProfile
(differential_auto_configured: bool), making the behavior persistent
across sessions.

A snapshot block appears in the Backup type section listing exactly
what was changed; it disappears as soon as the user changes either the
schedule frequency or the daily retention away from the autoconfigured
value, and never reappears for that profile.
"""

import pytest

from src.core.config import (
    BackupProfile,
    BackupType,
    RetentionConfig,
    RetentionPolicy,
    ScheduleConfig,
    ScheduleFrequency,
)
from src.ui.tabs.general_tab import GeneralTab
from src.ui.tabs.retention_tab import RetentionTab
from src.ui.tabs.schedule_tab import ScheduleTab


@pytest.fixture()
def wired_tabs(tk_root):
    """Fresh General + Retention + Schedule tabs wired like the app wires them."""
    general = GeneralTab(tk_root)
    retention = RetentionTab(tk_root)
    schedule = ScheduleTab(tk_root, scheduler=None)
    general.set_retention_tab(retention)
    general.set_schedule_tab(schedule)
    yield general, retention, schedule
    general.destroy()
    retention.destroy()
    schedule.destroy()


def _profile(
    backup_type: BackupType,
    gfs_daily_internal: int = 7,
    freq: ScheduleFrequency = ScheduleFrequency.WEEKLY,
    full_every: int = 7,
    already_configured: bool = False,
) -> BackupProfile:
    """Build a minimal profile for load_profile()."""
    return BackupProfile(
        name="t",
        backup_type=backup_type,
        full_backup_every=full_every,
        schedule=ScheduleConfig(frequency=freq),
        retention=RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=gfs_daily_internal,
            gfs_weekly=3,
            gfs_monthly=5,
            gfs_enabled=True,
        ),
        differential_auto_configured=already_configured,
    )


def _load_all(general, retention, schedule, profile):
    """Load a profile into all three tabs (order matches how the app does it)."""
    retention.load_profile(profile)
    schedule.load_profile(profile)
    general.load_profile(profile)


class TestAutoConfigGate:
    """Verify the persistent gate via BackupProfile.differential_auto_configured."""

    def test_first_transition_runs_autoconfig_and_sets_flag(self, wired_tabs):
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=2,
            freq=ScheduleFrequency.WEEKLY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)
        assert profile.differential_auto_configured is False

        general.type_var.set(BackupType.DIFFERENTIAL.value)

        assert profile.differential_auto_configured is True
        assert schedule.get_frequency_var().get() == "Daily"
        assert retention.get_gfs_daily_var().get() == 6

    def test_second_transition_is_noop_even_if_config_drifts(self, wired_tabs):
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=2,
            freq=ScheduleFrequency.WEEKLY,
            full_every=7,
            already_configured=True,
        )
        _load_all(general, retention, schedule, profile)

        general.type_var.set(BackupType.DIFFERENTIAL.value)

        # Nothing must have been auto-corrected: the gate was already True.
        assert schedule.get_frequency_var().get() == "Weekly"
        assert retention.get_gfs_daily_var().get() == 1  # internal=2 -> UI=1
        assert general._retention_info_label.winfo_manager() == ""

    def test_flag_is_set_even_when_no_actual_change_needed(self, wired_tabs):
        """The gate flips on the first Full->Diff regardless of whether any
        concrete write happened — the 'first transition' event is what matters."""
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=7,
            freq=ScheduleFrequency.DAILY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)

        general.type_var.set(BackupType.DIFFERENTIAL.value)

        assert profile.differential_auto_configured is True
        # No block because nothing was changed
        assert general._retention_info_label.winfo_manager() == ""

    def test_full_diff_full_diff_does_not_retrigger(self, wired_tabs):
        """After the first Full->Diff flips the gate, subsequent Full->Diff
        transitions are inert even if the user undoes the autoconfig manually."""
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=2,
            freq=ScheduleFrequency.WEEKLY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)

        # First Full -> Diff: autoconfig happens
        general.type_var.set(BackupType.DIFFERENTIAL.value)
        assert schedule.get_frequency_var().get() == "Daily"
        assert retention.get_gfs_daily_var().get() == 6

        # User manually reverts schedule to Weekly and retention to UI=1
        schedule.get_frequency_var().set("Weekly")
        retention.get_gfs_daily_var().set(1)

        # Back to Full, then Diff again
        general.type_var.set(BackupType.FULL.value)
        general.type_var.set(BackupType.DIFFERENTIAL.value)

        # Autoconfig does NOT re-run: user's manual values stay
        assert schedule.get_frequency_var().get() == "Weekly"
        assert retention.get_gfs_daily_var().get() == 1
        assert general._retention_info_label.winfo_manager() == ""


class TestAutoConfigActions:
    """Verify the partial behaviors depending on what needs changing."""

    def test_changes_both_when_both_need_change(self, wired_tabs):
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=2,
            freq=ScheduleFrequency.WEEKLY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)

        general.type_var.set(BackupType.DIFFERENTIAL.value)

        text = general._retention_info_label.cget("text")
        assert "Schedule: daily" in text
        assert "Retention: 7 days" in text

    def test_changes_only_schedule_when_retention_sufficient(self, wired_tabs):
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=30,
            freq=ScheduleFrequency.WEEKLY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)

        general.type_var.set(BackupType.DIFFERENTIAL.value)

        assert schedule.get_frequency_var().get() == "Daily"
        assert retention.get_gfs_daily_var().get() == 29  # unchanged (internal=30)
        text = general._retention_info_label.cget("text")
        assert "Schedule: daily" in text
        assert "Retention:" not in text

    def test_changes_only_retention_when_schedule_already_daily(self, wired_tabs):
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=2,
            freq=ScheduleFrequency.DAILY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)

        general.type_var.set(BackupType.DIFFERENTIAL.value)

        assert schedule.get_frequency_var().get() == "Daily"  # unchanged
        assert retention.get_gfs_daily_var().get() == 6
        text = general._retention_info_label.cget("text")
        assert "Schedule:" not in text
        assert "Retention: 7 days" in text

    def test_no_block_when_nothing_to_change(self, wired_tabs):
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=7,
            freq=ScheduleFrequency.DAILY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)

        general.type_var.set(BackupType.DIFFERENTIAL.value)

        assert general._retention_info_label.winfo_manager() == ""

    def test_retention_sufficient_value_is_not_lowered(self, wired_tabs):
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=30,
            freq=ScheduleFrequency.DAILY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)

        general.type_var.set(BackupType.DIFFERENTIAL.value)

        # Already at UI=29 (internal=30); autoconfig must NOT pull it back down.
        assert retention.get_gfs_daily_var().get() == 29


class TestBlockDisappearsOnUserModification:
    """After autoconfig, any user deviation hides the whole block."""

    def test_block_disappears_on_retention_value_change(self, wired_tabs):
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=2,
            freq=ScheduleFrequency.WEEKLY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)
        general.type_var.set(BackupType.DIFFERENTIAL.value)
        assert general._retention_info_label.winfo_manager() != ""

        retention.get_gfs_daily_var().set(10)  # different from autoconfig's 6

        assert general._retention_info_label.winfo_manager() == ""

    def test_block_disappears_on_schedule_value_change(self, wired_tabs):
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=2,
            freq=ScheduleFrequency.WEEKLY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)
        general.type_var.set(BackupType.DIFFERENTIAL.value)
        assert general._retention_info_label.winfo_manager() != ""

        schedule.get_frequency_var().set("Monthly")

        assert general._retention_info_label.winfo_manager() == ""

    def test_block_stays_when_user_writes_same_value(self, wired_tabs):
        """Writing the same value back to a Tk var still fires traces; the
        block must only disappear on a real value change."""
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=2,
            freq=ScheduleFrequency.WEEKLY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)
        general.type_var.set(BackupType.DIFFERENTIAL.value)

        # Rewrite the same values (user clicked spinbox then back)
        retention.get_gfs_daily_var().set(6)
        schedule.get_frequency_var().set("Daily")

        assert general._retention_info_label.winfo_manager() != ""

    def test_modifying_one_of_two_hides_entire_block(self, wired_tabs):
        """Per spec (c): touching either tracked value dismisses the block as
        a whole, even if the other value still matches the snapshot."""
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=2,
            freq=ScheduleFrequency.WEEKLY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)
        general.type_var.set(BackupType.DIFFERENTIAL.value)

        # Only retention changes; schedule stays Daily
        retention.get_gfs_daily_var().set(14)

        assert general._retention_info_label.winfo_manager() == ""


class TestDiffToFullAndLoad:
    """Verify non-destructive reverse transition and load_profile behavior."""

    def test_diff_to_full_does_not_touch_retention_or_schedule(self, wired_tabs):
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.FULL,
            gfs_daily_internal=2,
            freq=ScheduleFrequency.WEEKLY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)
        general.type_var.set(BackupType.DIFFERENTIAL.value)
        assert retention.get_gfs_daily_var().get() == 6
        assert schedule.get_frequency_var().get() == "Daily"

        general.type_var.set(BackupType.FULL.value)

        # Values preserved — we never reset them on reverse.
        assert retention.get_gfs_daily_var().get() == 6
        assert schedule.get_frequency_var().get() == "Daily"
        # Block hidden either way
        assert general._retention_info_label.winfo_manager() == ""

    def test_load_profile_does_not_trigger_autoconfig(self, wired_tabs):
        """Loading a Differential profile with sub-cycle retention is a pure
        state-reflect operation — no autoconfig write must sneak through."""
        general, retention, schedule = wired_tabs
        profile = _profile(
            BackupType.DIFFERENTIAL,
            gfs_daily_internal=2,
            freq=ScheduleFrequency.WEEKLY,
            full_every=7,
        )
        _load_all(general, retention, schedule, profile)

        assert retention.get_gfs_daily_var().get() == 1  # internal=2
        assert schedule.get_frequency_var().get() == "Weekly"
        assert profile.differential_auto_configured is False
        assert general._retention_info_label.winfo_manager() == ""

    def test_load_profile_clears_stale_block(self, wired_tabs):
        """A block left visible from a previously loaded profile must be
        dismissed when a new profile is loaded."""
        general, retention, schedule = wired_tabs
        p1 = _profile(
            BackupType.FULL,
            gfs_daily_internal=2,
            freq=ScheduleFrequency.WEEKLY,
            full_every=7,
        )
        _load_all(general, retention, schedule, p1)
        general.type_var.set(BackupType.DIFFERENTIAL.value)
        assert general._retention_info_label.winfo_manager() != ""

        p2 = _profile(BackupType.FULL, already_configured=True)
        _load_all(general, retention, schedule, p2)

        assert general._retention_info_label.winfo_manager() == ""
