"""Tests for the calendar-based full-backup schedule decision.

Covers the helper ``_is_full_due_by_schedule`` and the combination
validator ``BackupManagerApp._validate_full_schedule_combination``.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from src.core.backup_engine import _is_full_due_by_schedule
from src.core.config import BackupProfile, ScheduleFrequency


def _profile(mode: str = "monthly", day_of_week: int = 0, day_of_month: int = 1) -> BackupProfile:
    return BackupProfile(
        name="t",
        full_schedule_mode=mode,
        full_day_of_week=day_of_week,
        full_day_of_month=day_of_month,
    )


class TestIsFullDueMonthly:
    def test_no_previous_full_forces_true(self):
        p = _profile(mode="monthly")
        p.last_full_backup = None
        assert _is_full_due_by_schedule(p, datetime(2026, 4, 19)) is True

    def test_same_month_returns_false(self):
        p = _profile(mode="monthly")
        p.last_full_backup = "2026-04-05T22:00:00"
        assert _is_full_due_by_schedule(p, datetime(2026, 4, 19)) is False

    def test_new_month_returns_true(self):
        p = _profile(mode="monthly")
        p.last_full_backup = "2026-03-31T22:00:00"
        assert _is_full_due_by_schedule(p, datetime(2026, 4, 1)) is True

    def test_new_year_returns_true(self):
        p = _profile(mode="monthly")
        p.last_full_backup = "2025-12-15T22:00:00"
        assert _is_full_due_by_schedule(p, datetime(2026, 1, 1)) is True

    def test_catchup_when_first_day_missed(self):
        """If the 1st was skipped (PC off), the rule still fires on day 3."""
        p = _profile(mode="monthly")
        p.last_full_backup = "2026-03-15T22:00:00"
        assert _is_full_due_by_schedule(p, datetime(2026, 4, 3)) is True


class TestIsFullDueWeekly:
    def test_same_week_returns_false(self):
        p = _profile(mode="weekly")
        # Both dates fall in ISO week 16 of 2026 (assuming week starts Monday)
        p.last_full_backup = "2026-04-13T10:00:00"  # Monday
        assert _is_full_due_by_schedule(p, datetime(2026, 4, 17)) is False

    def test_new_week_returns_true(self):
        p = _profile(mode="weekly")
        p.last_full_backup = "2026-04-13T10:00:00"  # Monday week 16
        assert _is_full_due_by_schedule(p, datetime(2026, 4, 20)) is True  # Monday week 17

    def test_iso_week_boundary(self):
        """Week numbers wrap at year boundaries via isocalendar."""
        p = _profile(mode="weekly")
        p.last_full_backup = "2025-12-29T10:00:00"  # ISO week 1 of 2026
        # Same ISO week
        assert _is_full_due_by_schedule(p, datetime(2026, 1, 4)) is False


class TestIsFullDueDaily:
    def test_same_day_returns_false(self):
        p = _profile(mode="daily")
        p.last_full_backup = "2026-04-19T02:00:00"
        assert _is_full_due_by_schedule(p, datetime(2026, 4, 19, 22, 0)) is False

    def test_new_day_returns_true(self):
        p = _profile(mode="daily")
        p.last_full_backup = "2026-04-19T23:59:00"
        assert _is_full_due_by_schedule(p, datetime(2026, 4, 20, 0, 1)) is True


class TestIsFullDueMalformed:
    def test_unparseable_last_full_treated_as_none(self):
        p = _profile(mode="monthly")
        p.last_full_backup = "not-an-iso-date"
        assert _is_full_due_by_schedule(p, datetime(2026, 4, 19)) is True

    def test_unknown_mode_returns_false(self):
        """An unexpected mode must NOT spuriously force a full — fail safe."""
        p = _profile(mode="yearly")  # not supported
        p.last_full_backup = "2020-01-01T00:00:00"
        assert _is_full_due_by_schedule(p, datetime(2026, 4, 19)) is False


class TestValidateFullScheduleCombination:
    """Verify that invalid schedule × full_schedule_mode pairs are rejected."""

    @pytest.fixture
    def validator(self):
        from src.ui.app import BackupManagerApp

        return BackupManagerApp._validate_full_schedule_combination

    @pytest.mark.parametrize(
        ("run_freq", "full_mode"),
        [
            (ScheduleFrequency.MONTHLY, "daily"),
            (ScheduleFrequency.MONTHLY, "weekly"),
            (ScheduleFrequency.WEEKLY, "daily"),
        ],
    )
    def test_invalid_combinations_return_error(self, validator, run_freq, full_mode):
        assert validator(run_freq, full_mode) != ""

    @pytest.mark.parametrize(
        ("run_freq", "full_mode"),
        [
            (ScheduleFrequency.MONTHLY, "monthly"),
            (ScheduleFrequency.WEEKLY, "weekly"),
            (ScheduleFrequency.WEEKLY, "monthly"),
            (ScheduleFrequency.DAILY, "daily"),
            (ScheduleFrequency.DAILY, "weekly"),
            (ScheduleFrequency.DAILY, "monthly"),
            (ScheduleFrequency.HOURLY, "daily"),
            (ScheduleFrequency.HOURLY, "weekly"),
            (ScheduleFrequency.HOURLY, "monthly"),
            (ScheduleFrequency.MANUAL, "monthly"),
        ],
    )
    def test_valid_combinations_return_empty(self, validator, run_freq, full_mode):
        assert validator(run_freq, full_mode) == ""
