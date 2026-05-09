"""Tests for the collector's skipped-paths aggregation.

Regression: every PermissionError used to fire a separate
``phase_log.warning`` event. On workloads with thousands of
restricted directories (.pytest_cache, security-tool ``volatile``
folders, etc.) the Run-tab log was completely drowned and unusable.

The fix collects the failures in ``_SkippedPaths`` and emits a
single aggregated WARNING per category at the end of ``collect_files``.
Per-path detail still lands in the file logger at DEBUG so deep
diagnostic stays available.
"""

from __future__ import annotations

import logging
import os
from unittest.mock import patch

import pytest

from src.core.events import LOG, EventBus
from src.core.phases.collector import (
    _SKIPPED_SAMPLE_LIMIT,
    _SkippedPaths,
    collect_files,
)


def _capture_log_events(events: EventBus) -> list[dict]:
    """Subscribe a sink for LOG events and return the recorder list."""
    captured: list[dict] = []

    def sink(**payload):
        captured.append(payload)

    events.subscribe(LOG, sink)
    return captured


# ---------------------------------------------------------------------------
# _SkippedPaths unit behaviour
# ---------------------------------------------------------------------------


class TestSkippedPathsAccumulator:
    """Pure-data tests on the accumulator class itself."""

    def test_add_permission_increments_count_and_keeps_sample(self) -> None:
        s = _SkippedPaths()
        for i in range(20):
            s.add_permission(f"C:\\restricted\\{i}")
        assert s.permission_denied_count == 20
        # Sample is bounded — full list would defeat the purpose.
        assert len(s.permission_denied) == _SKIPPED_SAMPLE_LIMIT

    def test_add_os_error_keeps_message_in_sample(self) -> None:
        s = _SkippedPaths()
        s.add_os_error("C:\\foo", "WinError 32 sharing violation")
        assert s.os_errors_count == 1
        assert s.os_errors[0] == ("C:\\foo", "WinError 32 sharing violation")


# ---------------------------------------------------------------------------
# Aggregated emission to the EventBus
# ---------------------------------------------------------------------------


class TestAggregatedEmission:
    """The Run-tab log must NOT receive one event per failing path."""

    def test_thousand_failures_emit_one_event(self, tmp_path) -> None:
        """Realistic ``.pytest_cache``-style flood: 1000 denies → 1 event."""
        # Put one accessible file so the collect actually walks something.
        (tmp_path / "ok.txt").write_text("ok", encoding="utf-8")

        # Generate many sibling directories that all blow up on scandir.
        blocked_dirs = []
        for i in range(1000):
            d = tmp_path / f"blocked_{i:04d}"
            d.mkdir()
            blocked_dirs.append(str(d))
        blocked_set = set(blocked_dirs)

        original_scandir = os.scandir

        def patched(path):
            if str(path) in blocked_set:
                raise PermissionError("Access denied")
            return original_scandir(path)

        events = EventBus()
        captured = _capture_log_events(events)

        with patch("os.scandir", side_effect=patched):
            collect_files([str(tmp_path)], events=events)

        # Count skip-summary events (info for permission, warning for OS).
        # Must be ≤ 2; without aggregation we would see >= 1000.
        skip_events = [
            e for e in captured
            if e["level"] in ("info", "warning") and "Skipped" in e["message"]
        ]
        assert len(skip_events) <= 2, (
            f"UI got {len(skip_events)} skip events — aggregation broken"
        )

    def test_summary_includes_total_count(self, tmp_path) -> None:
        """User must see the total at a glance — '1000 ...'."""
        for i in range(1000):
            (tmp_path / f"d_{i:04d}").mkdir()

        original_scandir = os.scandir
        # Compare via os.fspath so a Path-vs-str argument doesn't fool
        # the mock and accidentally deny the root scan too.
        root_str = os.fspath(tmp_path)

        def always_deny(path):
            if os.fspath(path) != root_str:
                raise PermissionError("nope")
            return original_scandir(path)

        events = EventBus()
        captured = _capture_log_events(events)

        with patch("os.scandir", side_effect=always_deny):
            collect_files([str(tmp_path)], events=events)

        skip_msgs = [
            e["message"] for e in captured
            if "Skipped" in e["message"] and "protected" in e["message"]
        ]
        assert any("1000" in m for m in skip_msgs)

    def test_summary_includes_first_paths_as_sample(self, tmp_path) -> None:
        """A few sample paths help users identify the pattern."""
        for i in range(10):
            (tmp_path / f"unique_pattern_{i}").mkdir()

        events = EventBus()
        captured = _capture_log_events(events)

        # Use a thin wrapper that delegates to the original for the root.
        original_scandir = os.scandir

        def patched(path):
            if "unique_pattern_" in str(path):
                raise PermissionError("nope")
            return original_scandir(path)

        with patch("os.scandir", side_effect=patched):
            collect_files([str(tmp_path)], events=events)

        skip_msgs = [
            e["message"] for e in captured
            if "Skipped" in e["message"] and "protected" in e["message"]
        ]
        assert any("unique_pattern_" in m for m in skip_msgs)

    def test_no_failures_no_warning(self, tmp_path) -> None:
        """Clean walk → no warning event at all (no false-positive noise)."""
        (tmp_path / "a.txt").write_text("a", encoding="utf-8")

        events = EventBus()
        captured = _capture_log_events(events)

        collect_files([str(tmp_path)], events=events)

        warnings = [e for e in captured if e["level"] == "warning"]
        assert warnings == []


class TestUserFriendlyWording:
    """The aggregated message must avoid scaring novice users.

    Regression: the v3.3.19 wording was
    ``"Skipped N path(s) — permission denied. First: …"`` emitted at
    WARNING level. On a workload with many cache directories, this
    surfaced thousands of paths in a yellow warning bubble and led
    users to believe their files weren't being backed up.

    The new wording is INFO-level and explicitly reassuring.
    """

    def test_permission_summary_emitted_at_info_not_warning(self, tmp_path) -> None:
        """Permission-denied is benign (caches, locked files) → INFO."""
        (tmp_path / "blocked").mkdir()

        original_scandir = os.scandir

        def patched(path):
            if str(path).endswith("blocked"):
                raise PermissionError("nope")
            return original_scandir(path)

        events = EventBus()
        captured = _capture_log_events(events)

        with patch("os.scandir", side_effect=patched):
            collect_files([str(tmp_path)], events=events)

        # The permission summary line must NOT be a warning.
        warning_msgs = [
            e["message"] for e in captured if e["level"] == "warning"
        ]
        assert not any("protected item" in m for m in warning_msgs)

        info_msgs = [e["message"] for e in captured if e["level"] == "info"]
        assert any("protected item" in m for m in info_msgs)

    def test_message_explains_normality(self, tmp_path) -> None:
        """Wording must reassure — keywords ``normal`` and ``no action``."""
        (tmp_path / "blocked").mkdir()
        original_scandir = os.scandir

        def patched(path):
            if str(path).endswith("blocked"):
                raise PermissionError("nope")
            return original_scandir(path)

        events = EventBus()
        captured = _capture_log_events(events)

        with patch("os.scandir", side_effect=patched):
            collect_files([str(tmp_path)], events=events)

        all_msgs = " ".join(e["message"] for e in captured)
        assert "normal" in all_msgs
        assert "no action" in all_msgs

    def test_os_error_summary_stays_at_warning(self, tmp_path) -> None:
        """OS errors may be real hardware/filesystem issues → WARNING."""
        (tmp_path / "broken").mkdir()
        original_scandir = os.scandir

        def patched(path):
            if str(path).endswith("broken"):
                raise OSError("simulated I/O error")
            return original_scandir(path)

        events = EventBus()
        captured = _capture_log_events(events)

        with patch("os.scandir", side_effect=patched):
            collect_files([str(tmp_path)], events=events)

        warning_msgs = [
            e["message"] for e in captured if e["level"] == "warning"
        ]
        assert any("OS error" in m for m in warning_msgs)


class TestExcludePatternsLogged:
    """The active exclude list must be visible in the run log.

    Rationale: when a user sees thousands of paths skipped, the next
    natural question is "what is being filtered out?". Logging the
    pattern list at the start of collect lets them audit without
    digging through the profile dialog.
    """

    def test_exclude_patterns_logged_when_present(self, tmp_path) -> None:
        (tmp_path / "a.txt").write_text("a", encoding="utf-8")

        events = EventBus()
        captured = _capture_log_events(events)

        collect_files(
            [str(tmp_path)],
            exclude_patterns=["*.tmp", "node_modules", ".git"],
            events=events,
        )

        info_msgs = [e["message"] for e in captured if e["level"] == "info"]
        pattern_lines = [m for m in info_msgs if "exclude pattern" in m.lower()]
        assert pattern_lines, "expected an info line listing exclude patterns"
        # All three patterns must appear in the same line.
        line = pattern_lines[0]
        assert "*.tmp" in line
        assert "node_modules" in line
        assert ".git" in line

    def test_exclude_patterns_silent_when_empty(self, tmp_path) -> None:
        """No exclusions → no noisy 'Applying ...' line."""
        (tmp_path / "a.txt").write_text("a", encoding="utf-8")

        events = EventBus()
        captured = _capture_log_events(events)

        collect_files([str(tmp_path)], exclude_patterns=[], events=events)

        info_msgs = [e["message"] for e in captured if e["level"] == "info"]
        assert not any("exclude pattern" in m.lower() for m in info_msgs)


# ---------------------------------------------------------------------------
# File-log fidelity (DEBUG path) — the per-path detail must survive
# ---------------------------------------------------------------------------


class TestFileLogPreservesDetail:
    """Aggregation in UI must not lose information from the file log."""

    def test_individual_paths_logged_at_debug(self, tmp_path, caplog) -> None:
        """Each failure must appear in the file log at DEBUG level."""
        (tmp_path / "blocked").mkdir()

        original_scandir = os.scandir

        def patched(path):
            if str(path).endswith("blocked"):
                raise PermissionError("nope")
            return original_scandir(path)

        with caplog.at_level(logging.DEBUG, logger="src.core.phases.collector"):
            with patch("os.scandir", side_effect=patched):
                collect_files([str(tmp_path)])

        debug_msgs = [r.message for r in caplog.records if r.levelno == logging.DEBUG]
        assert any("Permission denied" in m and "blocked" in m for m in debug_msgs)


# ---------------------------------------------------------------------------
# Behavioural regression — files in accessible dirs are still collected
# ---------------------------------------------------------------------------


class TestBehaviouralRegression:
    """The aggregation MUST NOT change which files are collected."""

    def test_inaccessible_subtree_does_not_block_siblings(self, tmp_path) -> None:
        """Same scenario as test_collector_edge_cases.py but with the new code."""
        (tmp_path / "ok.txt").write_text("ok", encoding="utf-8")
        blocked = tmp_path / "blocked"
        blocked.mkdir()
        (blocked / "secret.txt").write_text("s", encoding="utf-8")

        original_scandir = os.scandir

        def patched(path):
            if str(path) == str(blocked):
                raise PermissionError("Access denied")
            return original_scandir(path)

        with patch("os.scandir", side_effect=patched):
            files = collect_files([str(tmp_path)])

        names = [f.relative_path for f in files]
        assert any(n.endswith("/ok.txt") for n in names)
        assert not any("secret" in n for n in names)
