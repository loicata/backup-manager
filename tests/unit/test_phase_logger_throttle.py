"""Tests for PROGRESS event throttling in PhaseLogger.

Regression: every file processed by the engine fired a PROGRESS event
that scheduled an ``after(0)`` callback in Tk's main loop. On the
manifest-hashing phase of a 260 K-file backup, hundreds of events per
second piled up in the queue; the OS message pump was starved and the
window could not be dragged on the desktop until the phase finished.

The fix throttles emissions to ~10 Hz (one every 100 ms) at the
PhaseLogger level. First and last items are never throttled so the bar
still hits its boundaries.
"""

from __future__ import annotations

from unittest.mock import patch


from src.core.events import PROGRESS, EventBus
from src.core.phase_logger import _PROGRESS_THROTTLE_MS, PhaseLogger


def _capture_progress(events: EventBus) -> list[dict]:
    captured: list[dict] = []

    def sink(**payload):
        captured.append(payload)

    events.subscribe(PROGRESS, sink)
    return captured


# ---------------------------------------------------------------------------
# Throttling rate
# ---------------------------------------------------------------------------


class TestThrottleRate:
    """Burst of fast progress() calls must NOT all be emitted."""

    def test_burst_is_throttled(self) -> None:
        """1000 calls in <1 ms emit at most a handful of events.

        Without throttling we'd get 1000 events. With ~10 Hz throttling
        and zero elapsed time between calls we expect ~1 event (the
        very first one — the rest are silenced because the gate hasn't
        elapsed yet) plus the terminal one.
        """
        events = EventBus()
        captured = _capture_progress(events)
        log = PhaseLogger("test_phase", events=events)

        # Mock monotonic so all calls register at the same instant.
        with patch("src.core.phase_logger.time.monotonic", return_value=10.0):
            for i in range(1, 1001):
                log.progress(current=i, total=1000, filename=f"f{i}", phase="test")

        # Expect: 1st (always emitted), then nothing until terminal,
        # then the 1000th (terminal). At most 2 events.
        assert len(captured) <= 2, f"Throttle leak: {len(captured)} events"

    def test_terminal_event_always_emitted(self) -> None:
        """current == total must reach the UI even mid-throttle-window."""
        events = EventBus()
        captured = _capture_progress(events)
        log = PhaseLogger("test_phase", events=events)

        with patch("src.core.phase_logger.time.monotonic", return_value=10.0):
            log.progress(current=1, total=10, filename="a", phase="x")  # first
            log.progress(current=5, total=10, filename="b", phase="x")  # throttled
            log.progress(current=10, total=10, filename="z", phase="x")  # terminal

        assert any(
            e["current"] == 10 for e in captured
        ), "Terminal event was throttled — bar would never reach 100%"

    def test_first_event_always_emitted(self) -> None:
        """current == 1 must reach the UI even right after another phase."""
        events = EventBus()
        captured = _capture_progress(events)
        log = PhaseLogger("test_phase", events=events)

        with patch("src.core.phase_logger.time.monotonic", return_value=10.0):
            log.progress(current=1, total=100, filename="first", phase="phase_a")

        assert len(captured) == 1
        assert captured[0]["current"] == 1


# ---------------------------------------------------------------------------
# Throttle window respected
# ---------------------------------------------------------------------------


class TestThrottleWindow:
    """After ``_PROGRESS_THROTTLE_MS`` ms the next call must fire."""

    def test_event_after_throttle_window_fires(self) -> None:
        events = EventBus()
        captured = _capture_progress(events)
        log = PhaseLogger("test_phase", events=events)

        # Patch monotonic to advance by twice the throttle window.
        seq = iter([10.0, 10.0 + (_PROGRESS_THROTTLE_MS * 2 / 1000.0)])
        with patch("src.core.phase_logger.time.monotonic", lambda: next(seq)):
            log.progress(current=1, total=100, filename="a", phase="x")
            log.progress(current=2, total=100, filename="b", phase="x")

        # Both should fire: first is always-on, second is past window
        assert len(captured) == 2

    def test_event_just_before_window_is_swallowed(self) -> None:
        """A second call inside the same window must NOT emit."""
        events = EventBus()
        captured = _capture_progress(events)
        log = PhaseLogger("test_phase", events=events)

        # 50 ms < 100 ms throttle.
        seq = iter([10.0, 10.0 + 0.05])
        with patch("src.core.phase_logger.time.monotonic", lambda: next(seq)):
            log.progress(current=1, total=100, filename="a", phase="x")
            log.progress(current=2, total=100, filename="b", phase="x")

        assert len(captured) == 1


# ---------------------------------------------------------------------------
# Other PhaseLogger methods unaffected
# ---------------------------------------------------------------------------


class TestOtherMethodsUnaffected:
    """info / warning / error must NOT be throttled (they're rare anyway)."""

    def test_info_always_fires(self) -> None:
        from src.core.events import LOG

        events = EventBus()
        captured: list[dict] = []
        events.subscribe(LOG, lambda **p: captured.append(p))
        log = PhaseLogger("test_phase", events=events)

        for _ in range(50):
            log.info("ping")
        assert len(captured) == 50

    def test_warning_always_fires(self) -> None:
        from src.core.events import LOG

        events = EventBus()
        captured: list[dict] = []
        events.subscribe(LOG, lambda **p: captured.append(p))
        log = PhaseLogger("test_phase", events=events)

        for _ in range(50):
            log.warning("careful")
        assert len(captured) == 50


# ---------------------------------------------------------------------------
# Defensive contracts
# ---------------------------------------------------------------------------


class TestDefensive:
    """Edge cases that could otherwise crash the throttle path."""

    def test_no_events_bus_no_crash(self) -> None:
        """PhaseLogger without an EventBus must still accept progress() calls."""
        log = PhaseLogger("test_phase", events=None)
        # Should not raise.
        log.progress(current=1, total=10, filename="a", phase="x")
        log.progress(current=5, total=10, filename="b", phase="x")

    def test_total_zero_handled(self) -> None:
        """``total == 0`` should not turn the call into a terminal."""
        events = EventBus()
        captured = _capture_progress(events)
        log = PhaseLogger("test_phase", events=events)

        # First call always fires regardless.
        log.progress(current=1, total=0, filename="a", phase="x")
        assert len(captured) == 1
