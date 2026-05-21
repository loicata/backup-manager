"""Tests for ``LocalStorage.test_connection`` wake-up budget vs fast-fail.

Pre-3.7.18, a disconnected USB drive would spin for ~16 s in the
``_wait_for_drive_online`` retry loop because the code could not tell
"drive missing" from "drive sleeping" — both cases reached the same
backoff sequence (0.3, 0.5, 1.0, 2.0, 4.0, 8.0 s). The silent retry
in ``_precheck_and_run`` then doubled the wait to ~32 s before the
"Destinations unavailable" popup appeared (21/05/2026 user report).

Fix: introspect the drive letter root (``E:\\`` on Windows) before
entering the wake-up loop. If the root itself is not stat-able, the
drive is physically unplugged and no amount of wake-up retry will
resurrect it — return False in <100 ms. If the root exists but the
dest subdirectory does not, the wake-up loop runs as before (the
drive is mounted but the path may still be coming online, e.g. on
the very first ``E:\\Backup Manager`` access after a fresh
reconnection).

These tests pin both branches of the new contract.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from unittest.mock import patch

import pytest

WINDOWS_ONLY = pytest.mark.skipif(
    os.name != "nt", reason="Drive letters are a Windows concept"
)


def _make_local_storage(dest_str: str):
    """Build a LocalStorage without invoking __init__ side-effects.

    ``LocalStorage.__init__`` triggers a number of validations
    (drive serial probe, destination existence) that we want to
    skip for these unit tests — we only exercise the wake-up
    behavior driven by ``self._dest``.
    """
    from src.storage.local import LocalStorage

    storage = LocalStorage.__new__(LocalStorage)
    storage._dest = Path(dest_str)
    storage._cancel_check = None
    storage._bandwidth_limit_kbps = 0
    return storage


class TestFastFailOnMissingDriveLetter:
    """The drive letter root drives the fast-fail branch."""

    @WINDOWS_ONLY
    def test_fast_fails_when_drive_letter_root_missing(self) -> None:
        """All ``Path.exists`` checks return False → no wake-up sleeps.

        Both the dest path and its drive root are absent. The fast-fail
        path returns immediately (no sleep), and the message reports
        the drive as not ready.
        """
        storage = _make_local_storage("E:\\Backup Manager")
        sleep_calls: list[float] = []

        # ``Path.exists`` is universally False, simulating a fully
        # disconnected E: drive. ``time.sleep`` is patched at module
        # scope so we can assert NOTHING slept.
        with patch.object(Path, "exists", return_value=False), patch(
            "src.storage.local.time.sleep",
            side_effect=lambda d: sleep_calls.append(d),
        ):
            t0 = time.monotonic()
            ok, msg = storage.test_connection()
            elapsed = time.monotonic() - t0

        assert ok is False
        assert "Drive not ready" in msg
        assert sleep_calls == [], (
            "Fast-fail path must skip the wake-up sleep loop entirely; "
            f"got sleep calls: {sleep_calls}"
        )
        # Belt-and-braces: even with sleep mocked, the path must be
        # quick (the mock returns instantly, but a misplaced fast-fail
        # check that fell into a loop would still spin on .exists()).
        assert elapsed < 1.0

    @WINDOWS_ONLY
    def test_wake_up_loop_runs_when_drive_letter_root_exists(self) -> None:
        """Root exists but dest doesn't → the wake-up loop still runs.

        Simulates the realistic case: ``E:\\`` is mounted, but
        ``E:\\Backup Manager`` is not yet visible (drive came online
        from deep sleep, subdir directory enumeration not finished).
        The classic backoff sequence must run.
        """
        storage = _make_local_storage("E:\\Backup Manager")
        sleep_calls: list[float] = []

        def selective_exists(self) -> bool:
            # Only the drive root ``E:\`` is visible; the subdir is not.
            return str(self).rstrip("\\").endswith(":")

        with patch.object(Path, "exists", autospec=True, side_effect=selective_exists), patch(
            "src.storage.local.time.sleep",
            side_effect=lambda d: sleep_calls.append(d),
        ):
            ok, msg = storage.test_connection()

        # The wake-up loop iterated through its backoff schedule, so
        # at least the first ``0.3 s`` delay was issued. Failure mode
        # we are guarding against: the new fast-fail check
        # accidentally triggering on the subdir-only case (which is
        # legitimate and must keep the retry-with-wake-up behavior).
        assert sleep_calls, (
            "Wake-up loop should have run when the drive root is visible; "
            "the fast-fail check must not fire on a subdir-only-missing "
            "scenario."
        )
        # The full default budget is (0.3, 0.5, 1.0, 2.0, 4.0, 8.0).
        # Verify the loop reached at least the first two attempts.
        assert sleep_calls[:2] == [0.3, 0.5]
        assert ok is False

    def test_returns_true_immediately_when_dest_exists(
        self, tmp_path: Path
    ) -> None:
        """Sanity: when the dest exists, no wake-up, no fast-fail.

        Uses a real ``tmp_path`` (a directory that genuinely exists)
        so we don't need to mock ``Path.exists``. Cross-platform.
        """
        storage = _make_local_storage(str(tmp_path))
        sleep_calls: list[float] = []

        with patch(
            "src.storage.local.time.sleep",
            side_effect=lambda d: sleep_calls.append(d),
        ):
            ok, msg = storage.test_connection()

        assert ok is True
        assert sleep_calls == [], (
            "Healthy dest must skip both the wake-up loop and the "
            f"fast-fail branch; got sleeps: {sleep_calls}"
        )
