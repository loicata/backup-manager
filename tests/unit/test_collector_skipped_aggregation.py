"""Tests for the collector's skipped-paths aggregation.

The collector emits a single aggregated ``Skipped N file(s) not
backed up`` event at the end of the walk. The event carries a
structured ``details`` payload with three lists:

- ``permission_denied``    list[str]              — paths that hit ``PermissionError``
- ``os_errors``            list[tuple[str, str]]  — paths + error text
- ``excluded_by_pattern``  list[tuple[str, str]]  — paths + matched pattern

The Run-tab Log widget consumes ``details`` to render an expandable
hierarchy (Skipped → category-by-file-type → extension → path with
reason) so the user can verify whether a specific file was backed up.
The lists are uncapped — the previous ``_SKIPPED_SAMPLE_LIMIT = 5``
made the in-Log expansion useless on real workloads.

A separate ``Applying exclude patterns (N)`` event is emitted at the
start of the walk with ``details = {"patterns": [...]}`` so the UI
can surface the active rule list.
"""

from __future__ import annotations

import logging
import os
from unittest.mock import patch

from src.core.events import LOG, EventBus
from src.core.phases.collector import (
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


def _skipped_event(captured: list[dict]) -> dict | None:
    """Return the single 'Skipped N file(s) not backed up' event, if any."""
    for e in captured:
        if "Skipped" in e["message"] and "not backed up" in e["message"]:
            return e
    return None


# ---------------------------------------------------------------------------
# _SkippedPaths unit behaviour
# ---------------------------------------------------------------------------


class TestSkippedPathsAccumulator:
    """Pure-data tests on the accumulator class itself."""

    def test_add_permission_appends_uncapped(self) -> None:
        s = _SkippedPaths()
        for i in range(50):
            s.add_permission(f"C:\\restricted\\{i}")
        # Every path is retained — the previous ``_SKIPPED_SAMPLE_LIMIT``
        # cap of 5 is gone so the Run-tab Log expansion is exhaustive.
        assert len(s.permission_denied) == 50
        assert s.total_count == 50

    def test_add_os_error_keeps_message(self) -> None:
        s = _SkippedPaths()
        s.add_os_error("C:\\foo", "WinError 32 sharing violation")
        assert s.os_errors == [("C:\\foo", "WinError 32 sharing violation")]
        assert s.total_count == 1

    def test_add_excluded_keeps_pattern(self) -> None:
        """The matched pattern is recorded so the UI can show it."""
        s = _SkippedPaths()
        s.add_excluded("D:\\drafts\\rapport.tmp", "*.tmp")
        assert s.excluded_by_pattern == [("D:\\drafts\\rapport.tmp", "*.tmp")]
        assert s.total_count == 1

    def test_total_count_aggregates_three_categories(self) -> None:
        s = _SkippedPaths()
        s.add_permission("a")
        s.add_os_error("b", "boom")
        s.add_excluded("c", "*.tmp")
        assert s.total_count == 3


# ---------------------------------------------------------------------------
# Aggregated emission to the EventBus
# ---------------------------------------------------------------------------


class TestAggregatedEmission:
    """The Run-tab log must NOT receive one event per failing path."""

    def test_thousand_failures_emit_one_event(self, tmp_path) -> None:
        """Realistic ``.pytest_cache``-style flood: 1000 denies → 1 event."""
        (tmp_path / "ok.txt").write_text("ok", encoding="utf-8")

        blocked_dirs: list[str] = []
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

        # Exactly one Skipped summary event regardless of count.
        skip_events = [
            e for e in captured if "Skipped" in e["message"] and "not backed up" in e["message"]
        ]
        assert len(skip_events) == 1, f"UI got {len(skip_events)} skip events — aggregation broken"

    def test_summary_message_includes_total_count(self, tmp_path) -> None:
        """User must see the total at a glance — '1000' in the message."""
        for i in range(1000):
            (tmp_path / f"d_{i:04d}").mkdir()

        original_scandir = os.scandir
        root_str = os.fspath(tmp_path)

        def always_deny(path):
            if os.fspath(path) != root_str:
                raise PermissionError("nope")
            return original_scandir(path)

        events = EventBus()
        captured = _capture_log_events(events)

        with patch("os.scandir", side_effect=always_deny):
            collect_files([str(tmp_path)], events=events)

        evt = _skipped_event(captured)
        assert evt is not None
        assert "1000" in evt["message"]

    def test_details_payload_lists_all_permission_paths(self, tmp_path) -> None:
        """The full path list lives in ``details`` for UI expansion."""
        for i in range(10):
            (tmp_path / f"unique_pattern_{i}").mkdir()

        events = EventBus()
        captured = _capture_log_events(events)

        original_scandir = os.scandir

        def patched(path):
            if "unique_pattern_" in str(path):
                raise PermissionError("nope")
            return original_scandir(path)

        with patch("os.scandir", side_effect=patched):
            collect_files([str(tmp_path)], events=events)

        evt = _skipped_event(captured)
        assert evt is not None
        details = evt["details"]
        # All 10 paths are present (no cap).
        assert len(details["permission_denied"]) == 10
        # Sample check: the unique substring is in the recorded paths.
        assert all("unique_pattern_" in p for p in details["permission_denied"])

    def test_no_failures_no_skipped_event(self, tmp_path) -> None:
        """Clean walk → no Skipped event at all."""
        (tmp_path / "a.txt").write_text("a", encoding="utf-8")

        events = EventBus()
        captured = _capture_log_events(events)

        collect_files([str(tmp_path)], events=events)

        assert _skipped_event(captured) is None

    def test_skipped_event_is_info_level(self, tmp_path) -> None:
        """The Run-tab now uses the row's tag (warning bg) for visual
        cue, not the legacy WARNING level — keeps the Run log INFO-only
        for routine skips so a flood of permission-denied entries does
        not paint the whole log yellow."""
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

        evt = _skipped_event(captured)
        assert evt is not None
        assert evt["level"] == "info"


class TestExcludedByPatternTracking:
    """Each path skipped by an exclude pattern must record the rule.

    The Run-tab Log displays ``excluded: <pattern>`` next to each
    file so the user can tell which rule caught their file.
    """

    def test_file_excluded_by_pattern_recorded(self, tmp_path) -> None:
        (tmp_path / "keep.txt").write_text("keep", encoding="utf-8")
        (tmp_path / "drop.tmp").write_text("drop", encoding="utf-8")

        events = EventBus()
        captured = _capture_log_events(events)

        files = collect_files([str(tmp_path)], exclude_patterns=["*.tmp"], events=events)

        # Only keep.txt actually collected.
        assert len(files) == 1
        assert files[0].relative_path.endswith("keep.txt")

        evt = _skipped_event(captured)
        assert evt is not None
        excluded = evt["details"]["excluded_by_pattern"]
        # The skipped entry knows which pattern caught it.
        assert any(path.endswith("drop.tmp") and pattern == "*.tmp" for path, pattern in excluded)

    def test_excluded_directory_recorded_once_not_recursively(self, tmp_path) -> None:
        """A skipped dir is one entry, not N entries for its files.

        Otherwise excluding ``node_modules`` would explode the skipped
        list with thousands of paths the collector intentionally does
        not even open.
        """
        nm = tmp_path / "node_modules"
        nm.mkdir()
        # Populate with many files — none should appear in the skip list.
        for i in range(50):
            (nm / f"f_{i}.js").write_text("//", encoding="utf-8")
        (tmp_path / "keep.txt").write_text("k", encoding="utf-8")

        events = EventBus()
        captured = _capture_log_events(events)

        collect_files(
            [str(tmp_path)],
            exclude_patterns=["node_modules"],
            events=events,
        )

        evt = _skipped_event(captured)
        assert evt is not None
        excluded = evt["details"]["excluded_by_pattern"]
        # Exactly one entry for the directory itself — not 50 file entries.
        assert len(excluded) == 1
        path, pattern = excluded[0]
        assert path.endswith("node_modules")
        assert pattern == "node_modules"


class TestExcludePatternsLogged:
    """The active exclude list must be visible in the run log.

    The new format is a parent log line ``Applying exclude patterns (N)``
    plus the patterns in ``details["patterns"]`` — the Run-tab Log
    expands them as children. The legacy comma-joined message lived
    on a single line that was tronquée beyond 4-5 patterns.
    """

    def test_exclude_patterns_event_has_count_and_payload(self, tmp_path) -> None:
        (tmp_path / "a.txt").write_text("a", encoding="utf-8")

        events = EventBus()
        captured = _capture_log_events(events)

        collect_files(
            [str(tmp_path)],
            exclude_patterns=["*.tmp", "node_modules", ".git"],
            events=events,
        )

        applying = [
            e for e in captured if "exclude pattern" in e["message"].lower() and e["details"]
        ]
        assert applying, "expected an event with patterns details"
        evt = applying[0]
        # Message states the count.
        assert "(3)" in evt["message"]
        # Details carries the full list.
        assert evt["details"]["patterns"] == ["*.tmp", "node_modules", ".git"]

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
    """Per-path detail must keep landing in the .log file at DEBUG."""

    def test_individual_paths_logged_at_debug(self, tmp_path, caplog) -> None:
        (tmp_path / "blocked").mkdir()

        original_scandir = os.scandir

        def patched(path):
            if str(path).endswith("blocked"):
                raise PermissionError("nope")
            return original_scandir(path)

        with (
            caplog.at_level(logging.DEBUG, logger="src.core.phases.collector"),
            patch("os.scandir", side_effect=patched),
        ):
            collect_files([str(tmp_path)])

        debug_msgs = [r.message for r in caplog.records if r.levelno == logging.DEBUG]
        assert any("Permission denied" in m and "blocked" in m for m in debug_msgs)


# ---------------------------------------------------------------------------
# Behavioural regression — files in accessible dirs are still collected
# ---------------------------------------------------------------------------


class TestBehaviouralRegression:
    """The aggregation MUST NOT change which files are collected."""

    def test_inaccessible_subtree_does_not_block_siblings(self, tmp_path) -> None:
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
