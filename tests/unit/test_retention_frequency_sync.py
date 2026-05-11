"""Regression tests for the Schedule -> Retention frequency sync.

Before the fix, switching the Schedule combobox from Monthly to Daily
left the "Days of history" row hidden because daily_row.pack(before=
weekly_row) ran while weekly_row was still pack_forget()ten, and the
TclError was silently swallowed by the trace callback.

The fix re-packs weekly_row BEFORE attempting to re-pack daily_row,
so the anchor is always valid when daily_row needs it.
"""

from __future__ import annotations

import tkinter as tk

import pytest

from src.core.config import (
    BackupProfile,
    RetentionConfig,
    RetentionPolicy,
    ScheduleConfig,
    ScheduleFrequency,
)
from src.ui.tabs.retention_tab import RetentionTab
from src.ui.tabs.schedule_tab import ScheduleTab


@pytest.fixture
def tabs(tk_root):
    """Build a fresh ScheduleTab + RetentionTab on the shared session Tk
    root. Widgets are destroyed at teardown so each test starts clean —
    the session-scoped root itself stays alive to avoid the Tcl error
    that follows repeated Tk root creation on Windows.
    """
    schedule_tab = ScheduleTab(tk_root)
    retention_tab = RetentionTab(tk_root)
    retention_tab.set_schedule_tab(schedule_tab)
    yield schedule_tab, retention_tab
    retention_tab.destroy()
    schedule_tab.destroy()


def _make_profile(frequency: ScheduleFrequency) -> BackupProfile:
    profile = BackupProfile(name="test")
    profile.schedule = ScheduleConfig(frequency=frequency, time="02:00")
    profile.retention = RetentionConfig(policy=RetentionPolicy.GFS, gfs_enabled=True)
    return profile


def _row_packed(tab: RetentionTab, key: str) -> bool:
    """Return True if the named GFS row is currently packed."""
    row = tab._gfs_rows[key]
    try:
        return bool(row.pack_info())
    except tk.TclError:
        return False


def _load(schedule_tab: ScheduleTab, retention_tab: RetentionTab, freq: ScheduleFrequency) -> None:
    """Mimic app._load_profile order: schedule first, then retention."""
    profile = _make_profile(freq)
    schedule_tab.load_profile(profile)
    retention_tab.load_profile(profile)


def test_initial_daily_shows_all_three_rows(tabs):
    schedule_tab, retention_tab = tabs
    _load(schedule_tab, retention_tab, ScheduleFrequency.DAILY)
    assert _row_packed(retention_tab, "gfs_daily")
    assert _row_packed(retention_tab, "gfs_weekly")
    assert _row_packed(retention_tab, "gfs_monthly")


def test_initial_weekly_hides_daily(tabs):
    schedule_tab, retention_tab = tabs
    _load(schedule_tab, retention_tab, ScheduleFrequency.WEEKLY)
    assert not _row_packed(retention_tab, "gfs_daily")
    assert _row_packed(retention_tab, "gfs_weekly")
    assert _row_packed(retention_tab, "gfs_monthly")


def test_initial_monthly_hides_daily_and_weekly(tabs):
    schedule_tab, retention_tab = tabs
    _load(schedule_tab, retention_tab, ScheduleFrequency.MONTHLY)
    assert not _row_packed(retention_tab, "gfs_daily")
    assert not _row_packed(retention_tab, "gfs_weekly")
    assert _row_packed(retention_tab, "gfs_monthly")


def test_monthly_then_daily_reveals_daily_row(tabs):
    """The original bug: Monthly -> Daily left daily_row hidden because
    daily.pack(before=weekly_row) raised TclError while weekly_row was
    still pack_forget()ten.
    """
    schedule_tab, retention_tab = tabs
    _load(schedule_tab, retention_tab, ScheduleFrequency.MONTHLY)
    assert not _row_packed(retention_tab, "gfs_weekly")

    # Simulate user picking Daily in the Schedule combobox.
    schedule_tab.freq_var.set("Daily")

    assert _row_packed(retention_tab, "gfs_daily"), (
        "daily_row should reappear after monthly -> daily switch"
    )
    assert _row_packed(retention_tab, "gfs_weekly"), (
        "weekly_row should reappear after monthly -> daily switch"
    )
    assert _row_packed(retention_tab, "gfs_monthly")


def test_monthly_then_weekly_reveals_weekly_row(tabs):
    schedule_tab, retention_tab = tabs
    _load(schedule_tab, retention_tab, ScheduleFrequency.MONTHLY)

    schedule_tab.freq_var.set("Weekly")

    assert not _row_packed(retention_tab, "gfs_daily")
    assert _row_packed(retention_tab, "gfs_weekly")
    assert _row_packed(retention_tab, "gfs_monthly")


def test_save_reload_after_monthly_to_daily(tabs):
    """End-to-end: Monthly profile reload + combobox switch to Daily +
    profile reload as Daily should leave all three rows packed.
    """
    schedule_tab, retention_tab = tabs
    _load(schedule_tab, retention_tab, ScheduleFrequency.MONTHLY)

    # User picks Daily, then app saves and reloads with the new profile.
    schedule_tab.freq_var.set("Daily")
    _load(schedule_tab, retention_tab, ScheduleFrequency.DAILY)

    assert _row_packed(retention_tab, "gfs_daily")
    assert _row_packed(retention_tab, "gfs_weekly")
    assert _row_packed(retention_tab, "gfs_monthly")


def test_apply_frequency_visibility_never_raises(tabs):
    """The trace callback swallowed TclError silently, so the bug was
    invisible. Even after the fix, calling _apply_frequency_visibility
    on every frequency transition must not raise.
    """
    schedule_tab, retention_tab = tabs
    sequence = [
        ScheduleFrequency.DAILY,
        ScheduleFrequency.MONTHLY,
        ScheduleFrequency.DAILY,
        ScheduleFrequency.WEEKLY,
        ScheduleFrequency.MONTHLY,
        ScheduleFrequency.WEEKLY,
        ScheduleFrequency.DAILY,
    ]
    for freq in sequence:
        retention_tab._apply_frequency_visibility(freq)
