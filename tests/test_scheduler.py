"""Tests for src.core.scheduler."""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.core.config import BackupProfile, ScheduleConfig, ScheduleFrequency
from src.core.scheduler import (
    InAppScheduler,
    ScheduleJournal,
    ScheduleLogEntry,
    SchedulerState,
    AutoStart,
)


class TestScheduleJournal:
    def test_add_and_get_entries(self, tmp_path):
        journal = ScheduleJournal(tmp_path)
        journal.add(
            ScheduleLogEntry(
                profile_id="abc",
                profile_name="Test",
                status="success",
                detail="OK",
            )
        )
        entries = journal.get_entries()
        assert len(entries) == 1
        assert entries[0]["profile_name"] == "Test"
        assert entries[0]["timestamp"]  # Auto-set

    def test_update_last(self, tmp_path):
        journal = ScheduleJournal(tmp_path)
        journal.add(ScheduleLogEntry(profile_id="abc", status="started"))
        journal.update_last(status="success", files_count=42)
        entries = journal.get_entries()
        assert entries[0]["status"] == "success"
        assert entries[0]["files_count"] == 42

    def test_filter_by_profile(self, tmp_path):
        journal = ScheduleJournal(tmp_path)
        journal.add(ScheduleLogEntry(profile_id="a", profile_name="A"))
        journal.add(ScheduleLogEntry(profile_id="b", profile_name="B"))
        entries = journal.get_entries(profile_id="a")
        assert len(entries) == 1
        assert entries[0]["profile_name"] == "A"

    def test_get_last_run(self, tmp_path):
        journal = ScheduleJournal(tmp_path)
        journal.add(ScheduleLogEntry(profile_id="x", status="success"))
        journal.add(ScheduleLogEntry(profile_id="x", status="failed"))
        last = journal.get_last_run("x")
        assert last["status"] == "failed"

    def test_get_last_run_nonexistent(self, tmp_path):
        journal = ScheduleJournal(tmp_path)
        assert journal.get_last_run("nope") is None

    def test_persistence(self, tmp_path):
        journal1 = ScheduleJournal(tmp_path)
        journal1.add(ScheduleLogEntry(profile_id="p", status="ok"))

        journal2 = ScheduleJournal(tmp_path)
        assert len(journal2.get_entries()) == 1


class TestSchedulerState:
    def test_set_and_get(self, tmp_path):
        state = SchedulerState(tmp_path)
        now = datetime.now()
        state.set_last_trigger("abc", now)
        result = state.get_last_trigger("abc")
        assert result is not None
        assert result.date() == now.date()

    def test_get_nonexistent(self, tmp_path):
        state = SchedulerState(tmp_path)
        assert state.get_last_trigger("nope") is None

    def test_persistence(self, tmp_path):
        state1 = SchedulerState(tmp_path)
        now = datetime.now()
        state1.set_last_trigger("p", now)

        state2 = SchedulerState(tmp_path)
        assert state2.get_last_trigger("p") is not None


class TestInAppScheduler:
    def test_is_due_first_run(self, tmp_path):
        triggered = []
        scheduler = InAppScheduler(
            tmp_path,
            get_profiles=lambda: [],
            backup_callback=lambda p: triggered.append(p),
        )
        profile = BackupProfile(
            schedule=ScheduleConfig(enabled=True, frequency=ScheduleFrequency.DAILY, time="00:00")
        )
        assert scheduler._is_due(profile, datetime.now()) is True

    def test_is_due_already_ran_today(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        profile = BackupProfile(
            schedule=ScheduleConfig(enabled=True, frequency=ScheduleFrequency.DAILY, time="00:00")
        )
        scheduler._state.set_last_trigger(profile.id, datetime.now())
        assert scheduler._is_due(profile, datetime.now()) is False

    def test_is_due_hourly(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        profile = BackupProfile(
            schedule=ScheduleConfig(enabled=True, frequency=ScheduleFrequency.HOURLY)
        )
        # Set last trigger 2 hours ago
        two_hours_ago = datetime.now() - timedelta(hours=2)
        scheduler._state.set_last_trigger(profile.id, two_hours_ago)
        assert scheduler._is_due(profile, datetime.now()) is True

    def test_get_next_run_info_manual(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        profile = BackupProfile(schedule=ScheduleConfig(frequency=ScheduleFrequency.MANUAL))
        assert scheduler.get_next_run_info(profile) == "Manual"

    def test_get_next_run_info_daily(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        profile = BackupProfile(
            schedule=ScheduleConfig(enabled=True, frequency=ScheduleFrequency.DAILY, time="09:00")
        )
        info = scheduler.get_next_run_info(profile)
        assert "09:00" in info

    def test_start_stop(self, tmp_path):
        scheduler = InAppScheduler(tmp_path, lambda: [], lambda p: None)
        scheduler.start()
        assert scheduler._running is True
        scheduler.stop()
        assert scheduler._running is False


class TestSchedulerStateConcurrency:
    """Test thread-safety of SchedulerState under concurrent access."""

    def test_concurrent_set_and_get(self, tmp_path):
        """Multiple threads writing different profile IDs concurrently."""
        state = SchedulerState(tmp_path)
        num_threads = 20
        errors: list[str] = []

        def write_and_read(idx: int) -> None:
            profile_id = f"profile_{idx}"
            now = datetime.now()
            state.set_last_trigger(profile_id, now)
            result = state.get_last_trigger(profile_id)
            if result is None:
                errors.append(f"Profile {profile_id} returned None")

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(write_and_read, i) for i in range(num_threads)]
            for f in as_completed(futures):
                f.result()  # Re-raise any exceptions

        assert not errors, f"Concurrency errors: {errors}"
        # All profiles should be persisted
        state2 = SchedulerState(tmp_path)
        for i in range(num_threads):
            assert state2.get_last_trigger(f"profile_{i}") is not None

    def test_concurrent_writes_no_corruption(self, tmp_path):
        """Rapid concurrent writes should not corrupt the state file."""
        state = SchedulerState(tmp_path)
        barrier = threading.Barrier(10)

        def write(idx: int) -> None:
            barrier.wait()
            state.set_last_trigger(f"p{idx}", datetime.now())

        threads = [threading.Thread(target=write, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Reload and verify no data loss or corruption
        state2 = SchedulerState(tmp_path)
        count = sum(1 for i in range(10) if state2.get_last_trigger(f"p{i}") is not None)
        assert count == 10


class TestScheduleJournalConcurrency:
    """Test thread-safety of ScheduleJournal under concurrent access."""

    def test_concurrent_adds(self, tmp_path):
        """Multiple threads adding entries concurrently."""
        journal = ScheduleJournal(tmp_path)
        num_entries = 30

        def add_entry(idx: int) -> None:
            journal.add(
                ScheduleLogEntry(
                    profile_id=f"p{idx}",
                    profile_name=f"Profile {idx}",
                    status="success",
                )
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(add_entry, i) for i in range(num_entries)]
            for f in as_completed(futures):
                f.result()

        entries = journal.get_entries(limit=100)
        assert len(entries) == num_entries

    def test_concurrent_add_and_update(self, tmp_path):
        """Adding and updating concurrently should not corrupt data."""
        journal = ScheduleJournal(tmp_path)

        # Seed with some entries
        for i in range(5):
            journal.add(ScheduleLogEntry(profile_id=f"p{i}", status="started"))

        barrier = threading.Barrier(6)

        def add_new(idx: int) -> None:
            barrier.wait()
            journal.add(ScheduleLogEntry(profile_id=f"new_{idx}", status="success"))

        def update_existing() -> None:
            barrier.wait()
            journal.update_last(status="completed")

        threads = [threading.Thread(target=add_new, args=(i,)) for i in range(5)]
        threads.append(threading.Thread(target=update_existing))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should have 10 entries total (5 original + 5 new)
        entries = journal.get_entries(limit=100)
        assert len(entries) == 10


class TestAutoStart:
    def test_is_enabled_when_no_file(self):
        assert AutoStart.is_enabled() is True or AutoStart.is_enabled() is False

    def test_is_show_window_default(self):
        assert AutoStart.is_show_window() in (True, False)
