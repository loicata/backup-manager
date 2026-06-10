"""Tests for WEEKLY/MONTHLY missed-slot catch-up (audit M21).

Before the fix, ``_is_due`` hard-gated WEEKLY on ``now.weekday() ==
day_of_week`` and MONTHLY on ``now.day == day``, so a slot missed because
the PC was off on the scheduled day was silently skipped for the whole
week/month. It now compares the last trigger against the most recent
scheduled occurrence, catching up a missed slot on the next launch.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from src.core.config import BackupProfile, ScheduleConfig, ScheduleFrequency
from src.core.scheduler import InAppScheduler


@pytest.fixture()
def scheduler(tmp_path: Path):
    sched = InAppScheduler(tmp_path, get_profiles=lambda: [], backup_callback=lambda p: None)
    yield sched
    if sched._running:
        sched.stop()


def _weekly(profile_id="w1", *, day_of_week=0, time="02:00"):
    return BackupProfile(
        id=profile_id,
        name="Weekly",
        schedule=ScheduleConfig(
            enabled=True, frequency=ScheduleFrequency.WEEKLY, time=time, day_of_week=day_of_week
        ),
    )


def _monthly(profile_id="m1", *, day_of_month=1, time="02:00"):
    return BackupProfile(
        id=profile_id,
        name="Monthly",
        schedule=ScheduleConfig(
            enabled=True, frequency=ScheduleFrequency.MONTHLY, time=time, day_of_month=day_of_month
        ),
    )


class TestMostRecentOccurrenceHelpers:
    def test_weekly_picks_latest_past_matching_day(self):
        # 2026-06-10 is a Wednesday (weekday 2); Monday (0) at 02:00 → 06-08.
        occ = InAppScheduler._most_recent_weekly(datetime(2026, 6, 10, 15, 0), 0, 2, 0)
        assert occ == datetime(2026, 6, 8, 2, 0)

    def test_weekly_rolls_back_when_today_is_day_but_before_time(self):
        # Today IS Monday but it's 01:00, before the 02:00 slot → last week.
        occ = InAppScheduler._most_recent_weekly(datetime(2026, 6, 8, 1, 0), 0, 2, 0)
        assert occ == datetime(2026, 6, 1, 2, 0)

    def test_monthly_this_month_when_day_passed(self):
        occ = InAppScheduler._most_recent_monthly(datetime(2026, 6, 10, 15, 0), 1, 2, 0)
        assert occ == datetime(2026, 6, 1, 2, 0)

    def test_monthly_rolls_back_to_previous_month(self):
        # Target day 20 hasn't arrived on the 10th → previous month's 20th.
        occ = InAppScheduler._most_recent_monthly(datetime(2026, 6, 10, 15, 0), 20, 2, 0)
        assert occ == datetime(2026, 5, 20, 2, 0)

    def test_monthly_caps_day_to_month_length(self):
        # Day 31 in a 30-day month (June) caps to the 30th.
        occ = InAppScheduler._most_recent_monthly(datetime(2026, 6, 30, 15, 0), 31, 2, 0)
        assert occ == datetime(2026, 6, 30, 2, 0)


class TestWeeklyCatchUp:
    def test_missed_weekly_caught_up_on_non_scheduled_day(self, scheduler):
        profile = _weekly(day_of_week=0)  # Monday
        scheduler._state.set_last_trigger(profile.id, datetime(2026, 5, 30, 2, 0))  # 11 days ago
        # now = Wednesday 2026-06-10 (NOT Monday) → previously False, now True.
        assert scheduler._is_due(profile, datetime(2026, 6, 10, 15, 0)) is True

    def test_weekly_not_due_when_already_ran_this_cycle(self, scheduler):
        profile = _weekly(day_of_week=0)
        # Ran at the most recent Monday occurrence (06-08 02:00).
        scheduler._state.set_last_trigger(profile.id, datetime(2026, 6, 8, 2, 0, 30))
        assert scheduler._is_due(profile, datetime(2026, 6, 10, 15, 0)) is False


class TestMonthlyCatchUp:
    def test_missed_monthly_caught_up(self, scheduler):
        profile = _monthly(day_of_month=1)
        scheduler._state.set_last_trigger(profile.id, datetime(2026, 4, 1, 2, 0))  # 2 months ago
        assert scheduler._is_due(profile, datetime(2026, 6, 10, 15, 0)) is True

    def test_monthly_not_due_when_already_ran_this_cycle(self, scheduler):
        profile = _monthly(day_of_month=1)
        scheduler._state.set_last_trigger(profile.id, datetime(2026, 6, 1, 2, 0, 30))
        assert scheduler._is_due(profile, datetime(2026, 6, 10, 15, 0)) is False
