"""Tests for the General tab one-shot autoconfig on Full->Differential.

On the very first Full->Differential transition per profile, the tab
switches the schedule frequency to Daily (so differential backups run
every day). The gate lives on BackupProfile.differential_auto_configured
so the behavior is persistent across sessions. The gate flips even when
no change was needed (schedule already Daily).
"""

import pytest

from src.core.config import (
    BackupProfile,
    BackupType,
    ScheduleConfig,
    ScheduleFrequency,
)
from src.ui.tabs.general_tab import GeneralTab
from src.ui.tabs.schedule_tab import ScheduleTab


@pytest.fixture()
def wired_tabs(tk_root):
    """Fresh General + Schedule tabs wired like the app wires them."""
    general = GeneralTab(tk_root)
    schedule = ScheduleTab(tk_root, scheduler=None)
    general.set_schedule_tab(schedule)
    yield general, schedule
    general.destroy()
    schedule.destroy()


def _profile(
    backup_type: BackupType,
    freq: ScheduleFrequency = ScheduleFrequency.WEEKLY,
    already_configured: bool = False,
) -> BackupProfile:
    """Build a minimal profile for load_profile()."""
    return BackupProfile(
        name="t",
        backup_type=backup_type,
        schedule=ScheduleConfig(frequency=freq),
        differential_auto_configured=already_configured,
    )


def _load_all(general, schedule, profile):
    """Load a profile into both tabs (order matches how the app does it)."""
    schedule.load_profile(profile)
    general.load_profile(profile)


class TestAutoConfigGate:
    """Verify the persistent gate via BackupProfile.differential_auto_configured."""

    def test_first_transition_runs_autoconfig_and_sets_flag(self, wired_tabs):
        general, schedule = wired_tabs
        profile = _profile(BackupType.FULL, freq=ScheduleFrequency.WEEKLY)
        _load_all(general, schedule, profile)
        assert profile.differential_auto_configured is False

        general.type_var.set(BackupType.DIFFERENTIAL.value)

        assert profile.differential_auto_configured is True
        assert schedule.get_frequency_var().get() == "Daily"

    def test_second_transition_is_noop(self, wired_tabs):
        """Once the flag is set, a second Full->Diff transition must
        not rewrite the schedule even if the user has drifted it."""
        general, schedule = wired_tabs
        profile = _profile(BackupType.FULL, freq=ScheduleFrequency.WEEKLY, already_configured=True)
        _load_all(general, schedule, profile)

        general.type_var.set(BackupType.DIFFERENTIAL.value)

        # Schedule untouched — user's choice respected.
        assert schedule.get_frequency_var().get() == "Weekly"

    def test_flag_is_set_even_when_no_actual_change_needed(self, wired_tabs):
        """Schedule already Daily: no change required, but the gate
        still flips so subsequent transitions are skipped."""
        general, schedule = wired_tabs
        profile = _profile(BackupType.FULL, freq=ScheduleFrequency.DAILY)
        _load_all(general, schedule, profile)

        general.type_var.set(BackupType.DIFFERENTIAL.value)

        assert profile.differential_auto_configured is True
        assert schedule.get_frequency_var().get() == "Daily"


class TestBlockDisappearsOnUserModification:
    """The info snapshot label hides when the user drifts from the
    autoconfigured value. Same-value writes keep the block visible."""

    def test_block_disappears_on_schedule_value_change(self, wired_tabs):
        general, schedule = wired_tabs
        profile = _profile(BackupType.FULL, freq=ScheduleFrequency.WEEKLY)
        _load_all(general, schedule, profile)
        general.type_var.set(BackupType.DIFFERENTIAL.value)
        assert general._retention_info_label.winfo_manager() == "pack"

        schedule.get_frequency_var().set("Weekly")

        assert general._retention_info_label.winfo_manager() == ""

    def test_block_stays_when_user_writes_same_value(self, wired_tabs):
        general, schedule = wired_tabs
        profile = _profile(BackupType.FULL, freq=ScheduleFrequency.WEEKLY)
        _load_all(general, schedule, profile)
        general.type_var.set(BackupType.DIFFERENTIAL.value)

        # Re-writing the same value (Daily) must not hide the block.
        schedule.get_frequency_var().set("Daily")

        assert general._retention_info_label.winfo_manager() == "pack"


class TestDiffToFullAndLoad:
    """Autoconfig runs only on Full->Diff, not on Diff->Full or on load."""

    def test_diff_to_full_does_not_touch_schedule(self, wired_tabs):
        general, schedule = wired_tabs
        profile = _profile(BackupType.DIFFERENTIAL, freq=ScheduleFrequency.WEEKLY)
        _load_all(general, schedule, profile)

        general.type_var.set(BackupType.FULL.value)

        assert schedule.get_frequency_var().get() == "Weekly"
        assert profile.differential_auto_configured is False

    def test_load_profile_does_not_trigger_autoconfig(self, wired_tabs):
        general, schedule = wired_tabs
        profile = _profile(BackupType.DIFFERENTIAL, freq=ScheduleFrequency.WEEKLY)
        _load_all(general, schedule, profile)

        # The schedule is not rewritten just by loading a DIFF profile.
        assert schedule.get_frequency_var().get() == "Weekly"
        assert profile.differential_auto_configured is False

    def test_load_profile_clears_stale_block(self, wired_tabs):
        general, schedule = wired_tabs
        profile = _profile(BackupType.FULL, freq=ScheduleFrequency.WEEKLY)
        _load_all(general, schedule, profile)
        general.type_var.set(BackupType.DIFFERENTIAL.value)
        assert general._retention_info_label.winfo_manager() == "pack"

        # Loading another profile must drop the inline info block.
        other = _profile(BackupType.FULL, already_configured=True)
        _load_all(general, schedule, other)

        assert general._retention_info_label.winfo_manager() == ""
