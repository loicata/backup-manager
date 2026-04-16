"""Tests for src.core.profile_lock — per-profile run lock."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.profile_lock import (
    ProfileLockError,
    _pid_alive,
    acquire,
    release,
)


class TestPidAlive:
    """Liveness check for process identifiers."""

    def test_zero_and_negative_are_dead(self):
        assert _pid_alive(0) is False
        assert _pid_alive(-1) is False

    def test_current_process_is_alive(self):
        """The running interpreter's own PID must always be alive."""
        assert _pid_alive(os.getpid()) is True

    def test_nonexistent_pid_is_dead(self):
        """A very high PID that cannot exist is dead."""
        # PIDs above the usual 32-bit limit cannot represent a real
        # process on any common OS; OpenProcess / kill both report it
        # as missing.
        assert _pid_alive(2**31 - 1) is False


class TestAcquireRelease:
    """Lock acquire/release semantics."""

    def test_acquire_creates_lock_file_with_pid(self, tmp_path: Path):
        lock_path = tmp_path / "profile.lock"
        acquire(lock_path)
        assert lock_path.exists()
        assert int(lock_path.read_text()) == os.getpid()
        release(lock_path)
        assert not lock_path.exists()

    def test_release_missing_is_ok(self, tmp_path: Path):
        """Releasing a lock that was never created must not raise."""
        release(tmp_path / "never_existed.lock")

    def test_reacquire_after_release(self, tmp_path: Path):
        """A released lock can be re-acquired by the same process."""
        lock_path = tmp_path / "profile.lock"
        acquire(lock_path)
        release(lock_path)
        acquire(lock_path)
        release(lock_path)

    def test_acquire_fails_when_held_by_live_process(self, tmp_path: Path):
        """A lock held by another live PID blocks acquisition."""
        lock_path = tmp_path / "profile.lock"
        # Simulate a foreign PID that is alive.
        foreign_pid = os.getpid() - 1 if os.getpid() > 1 else 99999
        lock_path.write_text(str(foreign_pid))

        with (
            patch("src.core.profile_lock._pid_alive", return_value=True),
            pytest.raises(ProfileLockError, match="Another backup"),
        ):
            acquire(lock_path)

    def test_stale_lock_is_taken_over(self, tmp_path: Path):
        """A lock with a dead PID is silently replaced."""
        lock_path = tmp_path / "profile.lock"
        lock_path.write_text("999999")  # Simulated dead PID

        with patch("src.core.profile_lock._pid_alive", return_value=False):
            acquire(lock_path)

        assert int(lock_path.read_text()) == os.getpid()
        release(lock_path)

    def test_corrupt_lock_is_treated_as_stale(self, tmp_path: Path):
        """An unreadable/garbage lock file is treated as stale, not blocking."""
        lock_path = tmp_path / "profile.lock"
        lock_path.write_text("not-a-number")
        acquire(lock_path)
        assert int(lock_path.read_text()) == os.getpid()
        release(lock_path)

    def test_second_thread_in_same_process_is_blocked(self, tmp_path: Path):
        """Intra-process concurrency (scheduler + UI) must be rejected.

        Both threads live in the same Python process (same PID), so the
        lock file alone is not enough — a threading.Lock layer is
        required to separate them.
        """
        import threading

        lock_path = tmp_path / "profile.lock"
        acquire(lock_path)

        errors: list[BaseException] = []

        def _second_run() -> None:
            try:
                acquire(lock_path)
            except BaseException as e:  # noqa: BLE001 — we're re-raising via capture
                errors.append(e)

        t = threading.Thread(target=_second_run)
        t.start()
        t.join()
        release(lock_path)

        assert len(errors) == 1
        assert isinstance(errors[0], ProfileLockError)
        assert "already running" in str(errors[0])

    def test_lock_reusable_after_release_from_another_thread(self, tmp_path: Path):
        """After the holder releases, another thread can acquire cleanly."""
        import threading

        lock_path = tmp_path / "profile.lock"

        acquire(lock_path)
        release(lock_path)

        acquired = threading.Event()

        def _next_run() -> None:
            acquire(lock_path)
            acquired.set()
            release(lock_path)

        t = threading.Thread(target=_next_run)
        t.start()
        t.join(timeout=2)
        assert acquired.is_set(), "Second thread should have acquired the lock"

    def test_stale_file_from_same_pid_is_cleared(self, tmp_path: Path):
        """A lock file left by our own previous (crashed) run is reclaimed.

        After a hard crash there is no threading.Lock entry for us, but
        the lock file on disk carries our PID.  The new run must be
        able to take it over — otherwise the app becomes unusable
        until the file is deleted manually.
        """
        lock_path = tmp_path / "profile.lock"
        lock_path.write_text(str(os.getpid()))
        acquire(lock_path)
        release(lock_path)

    def test_parent_directory_is_created(self, tmp_path: Path):
        """acquire() creates intermediate directories if missing."""
        lock_path = tmp_path / "nested" / "dir" / "profile.lock"
        acquire(lock_path)
        assert lock_path.exists()
        release(lock_path)
