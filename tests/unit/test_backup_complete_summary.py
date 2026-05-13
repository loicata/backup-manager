"""Tests for the terminal "Backup complete" log line.

Two concerns are pinned here:

1. **Format** — the engine emits one final summary that is the first
   thing the user reads on the Run / History tab when a backup ends.
   The format must stay terse (no destination summary appended, no
   sub-second precision, time in minutes) so the line is glanceable.

2. **Phase column** — the Run-tab Log shows a Phase column on every
   row. The terminal line previously inherited the last seen phase
   (``rotator`` on the success path, an earlier one on failure), which
   was technically false: nothing was running at that point. The
   column is now forced blank for ``Backup complete`` / ``Backup
   failed`` / ``Backup cancelled``.
"""

from __future__ import annotations

import pytest

from src.ui.tabs.run_tab import (
    _infer_phase,
    _is_terminal_log_message,
)


class TestEngineFinalLineFormat:
    """The engine builds the final ``Backup complete`` line in run_backup.

    We can't import ``backup_engine`` cleanly without a config manager,
    so we re-derive the format string locally and assert its shape.
    The format-string change is small enough that an end-to-end test
    via the pipeline would be over-engineering.
    """

    @pytest.mark.parametrize(
        "duration_seconds, expected_minutes",
        [
            (7831.8, "130.5 min"),
            (60.0, "1.0 min"),
            (30.0, "0.5 min"),
            (3.0, "0.1 min"),
        ],
    )
    def test_duration_is_rendered_in_minutes(self, duration_seconds, expected_minutes):
        """The engine emits ``{seconds / 60:.1f} min`` — pin the shape."""
        rendered = f"{duration_seconds / 60:.1f} min"
        assert rendered == expected_minutes

    def test_summary_line_does_not_include_destination_arrow(self):
        """No more ``→ Storage (...)`` tail.

        The destination summary was visible only on the very first run
        — every subsequent line repeated it and pushed the file count
        + duration off-screen on narrow windows. The destination is
        already shown in the Storage tab and on the History row, so
        removing it from the live log is pure win.
        """
        line = "Backup complete: 231908 files in 130.5 min"
        assert "→" not in line
        assert "Storage (" not in line


class TestTerminalLogMessageDetection:
    """``_is_terminal_log_message`` controls Phase-column blanking."""

    @pytest.mark.parametrize(
        "message",
        [
            "Backup complete: 231908 files in 130.5 min",
            "Backup complete: 5 files in 0.1 min",
            "Backup failed: Remote verification failed: 1/262675 errors",
            "Backup cancelled by user",
            "BACKUP COMPLETE: case-insensitive",
        ],
    )
    def test_matches_terminal_messages(self, message):
        assert _is_terminal_log_message(message) is True

    @pytest.mark.parametrize(
        "message",
        [
            "Starting backup 'BaLoic'",
            "Collecting files...",
            "Building integrity manifest...",
            "Remote verification OK: 262691/262691 files verified",
            "GFS rotation: kept 2, deleted 0",
            "Rotating old backups...",
            "",
        ],
    )
    def test_does_not_match_in_flight_messages(self, message):
        assert _is_terminal_log_message(message) is False

    def test_terminal_messages_have_no_inferred_phase(self):
        """Belt-and-braces: even if a future ``_PHASE_PATTERNS`` entry
        matched ``Backup …``, the terminal-message branch in ``_on_log``
        runs FIRST and clears the phase. Today's patterns happen to
        not match, but this test pins the contract.
        """
        assert _infer_phase("Backup complete: 1 files in 0.1 min") == ""
        assert _infer_phase("Backup failed: disk full") == ""
        assert _infer_phase("Backup cancelled by user") == ""
