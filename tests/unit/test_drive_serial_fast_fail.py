"""Tests for the fast-fail path in ``src.storage.drive_serial``.

Pre-3.7.19 the 3.7.18 fast-fail in ``LocalStorage._wait_for_drive_online``
was rendered ineffective by an upstream slow path: every
``create_backend(LOCAL)`` call ran ``resolve_local_path`` which:

1. Probed the original path with ``_probe_path_with_wake`` (its own
   ~15.8 s backoff loop), then
2. Spawned PowerShell via ``find_drive_by_serial`` to enumerate
   every mounted drive looking for the serial.

For a physically unplugged USB, both steps were wasted: the drive
letter root is gone, the serial will not be found anywhere. Each
``resolve_local_path`` call burned ~18 s (15.8 s probe + ~2-5 s
PowerShell). A precheck + silent retry + health-check polling
amplified this to 60-90 s of total wait — visible in the
2026-05-21 user log as five "Drive not found for serial …" warnings
spread over 60 seconds after clicking Start backup on a disconnected E:.

Fix: a single ``_drive_letter_root_present`` helper feeds two
fast-fail branches:

* ``_probe_path_with_wake`` returns False in <100 ms when the drive
  letter root itself is missing (instead of running the 15.8 s
  wake-up backoff).
* ``resolve_local_path`` skips the PowerShell ``find_drive_by_serial``
  call when the drive letter is gone — no point enumerating drives
  for a serial that cannot be on a non-mounted device.

These tests pin both branches.
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


class TestProbeFastFail:
    """``_probe_path_with_wake`` skips the wake-up loop when the drive
    letter root is missing.
    """

    @WINDOWS_ONLY
    def test_fast_fail_when_drive_letter_root_missing(self) -> None:
        from src.storage import drive_serial

        sleep_calls: list[float] = []
        with patch.object(Path, "exists", return_value=False), patch(
            "src.storage.drive_serial.time.sleep",
            side_effect=lambda d: sleep_calls.append(d),
        ):
            t0 = time.monotonic()
            ok = drive_serial._probe_path_with_wake("E:\\Backup Manager")
            elapsed = time.monotonic() - t0

        assert ok is False
        assert sleep_calls == [], (
            "Fast-fail must skip the wake-up sleep loop entirely; "
            f"got sleeps: {sleep_calls}"
        )
        assert elapsed < 1.0, (
            f"Fast-fail path must complete in <1 s; took {elapsed * 1000:.0f} ms"
        )

    @WINDOWS_ONLY
    def test_wake_up_runs_when_drive_letter_root_present(self) -> None:
        """Root visible but subdir not → wake-up loop runs."""
        from src.storage import drive_serial

        def selective_exists(self) -> bool:
            # Only the drive root ``E:\`` is visible.
            return str(self).rstrip("\\").endswith(":")

        sleep_calls: list[float] = []
        with patch.object(Path, "exists", autospec=True, side_effect=selective_exists), patch(
            "src.storage.drive_serial.time.sleep",
            side_effect=lambda d: sleep_calls.append(d),
        ):
            ok = drive_serial._probe_path_with_wake("E:\\Backup Manager")

        assert ok is False
        assert sleep_calls, (
            "Wake-up loop should have run when the drive root is visible; "
            "the fast-fail check must not fire on a subdir-only-missing scenario."
        )
        assert sleep_calls[:2] == [0.3, 0.5]


class TestResolveLocalPathSkipsPowerShell:
    """``resolve_local_path`` skips the PowerShell drive enumeration
    when the drive letter root is missing.
    """

    @WINDOWS_ONLY
    def test_skips_find_drive_by_serial_when_drive_letter_missing(
        self,
    ) -> None:
        """No PowerShell call when ``E:\`` is gone."""
        from src.storage import drive_serial

        find_calls: list[str] = []

        def stub_find(serial: str):
            find_calls.append(serial)
            return None

        with patch.object(Path, "exists", return_value=False), patch(
            "src.storage.drive_serial.time.sleep"
        ), patch.object(drive_serial, "find_drive_by_serial", side_effect=stub_find):
            resolved = drive_serial.resolve_local_path(
                "E:\\Backup Manager", device_serial="Y47800CN0JN7T5S"
            )

        assert resolved == "E:\\Backup Manager"  # passthrough
        assert find_calls == [], (
            "find_drive_by_serial must NOT be called when the drive letter "
            "is missing — no mounted drive can carry that serial. "
            f"got calls: {find_calls}"
        )

    @WINDOWS_ONLY
    def test_calls_find_drive_by_serial_when_drive_letter_present(
        self,
    ) -> None:
        """Drive letter present but path missing → enumerate to detect
        a possible letter reassignment (regression guard: the fast-fail
        check must NOT fire on the legitimate subdir-only case).
        """
        from src.storage import drive_serial

        def selective_exists(self) -> bool:
            # Only the drive root ``E:\`` is visible.
            return str(self).rstrip("\\").endswith(":")

        find_calls: list[str] = []

        def stub_find(serial: str):
            find_calls.append(serial)
            return None  # serial not found on any other letter

        with patch.object(Path, "exists", autospec=True, side_effect=selective_exists), patch(
            "src.storage.drive_serial.time.sleep"
        ), patch.object(drive_serial, "find_drive_by_serial", side_effect=stub_find):
            drive_serial.resolve_local_path(
                "E:\\Backup Manager", device_serial="Y47800CN0JN7T5S"
            )

        assert find_calls == ["Y47800CN0JN7T5S"], (
            "find_drive_by_serial must run when the drive letter is mounted "
            "(possible reassignment); got calls: "
            f"{find_calls}"
        )

    @WINDOWS_ONLY
    def test_no_serial_still_passes_through_fast_when_drive_letter_missing(
        self,
    ) -> None:
        """``device_serial=""`` + missing drive letter → still fast.

        The legacy path called ``_probe_path_with_wake`` then
        returned the original path. Without the fast-fail, that
        would have burned 15.8 s for a destination we already know
        is unreachable.
        """
        from src.storage import drive_serial

        sleep_calls: list[float] = []
        with patch.object(Path, "exists", return_value=False), patch(
            "src.storage.drive_serial.time.sleep",
            side_effect=lambda d: sleep_calls.append(d),
        ):
            resolved = drive_serial.resolve_local_path(
                "E:\\Backup Manager", device_serial=""
            )

        assert resolved == "E:\\Backup Manager"
        assert sleep_calls == [], (
            "No-serial + missing-drive-letter must still fast-fail "
            "without wake-up sleeps."
        )
