"""Unified logger for pipeline phases.

Replaces the duplicated _log() helper present in every phase module.
Combines Python logging with EventBus event emission, adding the
phase name to each event for downstream filtering.
"""

import logging
import threading
import time

from src.core.events import LOG, PROGRESS, EventBus

# Maximum PROGRESS event rate, in milliseconds between emissions.
# 100 ms => ~10 Hz, plenty for a smooth bar update and well below
# what a human can read; the original 1-event-per-file flood (could
# reach hundreds of events per second on small-file phases) saturated
# the Tk after() queue and starved the OS message pump, locking the
# window in place on the desktop. The first event of a phase and the
# last (current == total) are NEVER throttled so the user always sees
# the start and the completion regardless of throughput.
_PROGRESS_THROTTLE_MS = 100


class PhaseLogger:
    """Unified logger for a pipeline phase.

    Combines Python logging with EventBus event emission.
    Each log call emits a LOG event containing the message,
    level, and phase name.

    Args:
        phase_name: Human-readable phase identifier
                    (e.g. "collector", "filter", "writer").
        events: Optional EventBus instance. When None, only
                Python logging is used.
    """

    def __init__(self, phase_name: str, events: EventBus | None = None):
        self._logger = logging.getLogger(f"src.core.phases.{phase_name}")
        self._events = events
        self._phase_name = phase_name
        # Monotonic timestamp of the last PROGRESS we actually pushed
        # to the EventBus. ``0.0`` means "never emitted" so the very
        # first call always fires.
        self._last_progress_ms: float = 0.0
        # Guards ``_last_progress_ms`` against the parallel writer
        # (4 workers in ``write_flat`` since v3.7.1) — without this
        # lock, two workers can read the same stale timestamp on the
        # same throttle window and double-emit, defeating Invariant 5
        # (PROGRESS at most 10 Hz). The lock is uncontended in the
        # legacy sequential phases (collector, filter, hashing's own
        # loop) and adds ~50 ns there.
        self._progress_lock = threading.Lock()

    def info(self, message: str, *, details: dict | None = None) -> None:
        """Log at INFO level and emit LOG event.

        Args:
            message: Human-readable log message.
            details: Optional structured payload attached to the event.
                Used by the Run-tab Log widget to render expandable
                children under a parent log line — e.g. the collector
                emits the "Skipped N file(s)" message with
                ``details = {"permission_denied": [...], "excluded_by_pattern": [...]}``
                so the UI can group skipped paths by category and
                extension. ``None`` keeps the legacy flat-line
                rendering (no caret, no expand). Not logged through
                the Python logger to keep the .log file readable;
                only travels on the EventBus.
        """
        self._logger.info(message)
        if self._events:
            self._events.emit(
                LOG,
                message=message,
                level="info",
                phase=self._phase_name,
                details=details,
            )

    def warning(self, message: str, *, details: dict | None = None) -> None:
        """Log at WARNING level and emit LOG event.

        Args:
            message: Human-readable warning message.
            details: Optional structured payload (see ``info``).
        """
        self._logger.warning(message)
        if self._events:
            self._events.emit(
                LOG,
                message=message,
                level="warning",
                phase=self._phase_name,
                details=details,
            )

    def error(self, message: str, *, details: dict | None = None) -> None:
        """Log at ERROR level and emit LOG event.

        Args:
            message: Human-readable error message.
            details: Optional structured payload (see ``info``).
        """
        self._logger.error(message)
        if self._events:
            self._events.emit(
                LOG,
                message=message,
                level="error",
                phase=self._phase_name,
                details=details,
            )

    def progress(
        self,
        current: int,
        total: int,
        filename: str,
        phase: str,
    ) -> None:
        """Emit a PROGRESS event for UI progress tracking.

        Throttles emissions to at most one every ``_PROGRESS_THROTTLE_MS``
        milliseconds. The first event (``current == 1``) and the last
        (``current == total``) are always emitted so the UI sees the
        beginning and the end of every phase, even on workloads that
        complete in less than the throttle window.

        Why throttle: the engine emits one PROGRESS per item processed.
        On the manifest-hashing phase of a 260 K-file backup that's
        hundreds of events per second, each scheduling an
        ``after(0)`` callback in Tk's main loop. Tk drains them
        sequentially and the OS message pump (drag, resize, focus
        change) starves behind them — the window literally cannot be
        moved on the desktop until the queue clears. 10 Hz updates
        look perfectly smooth and leave the pump >90 % of its slot.

        Args:
            current: Number of items processed so far.
            total: Total number of items to process.
            filename: Name of the file currently being processed.
            phase: Pipeline phase identifier for the progress bar.
        """
        if not self._events:
            return

        # Always let through the first item of a phase and the very
        # last one, so the bar reaches its boundaries even when the
        # whole phase fits inside one throttle window.
        is_terminal = current <= 1 or (total > 0 and current >= total)
        # Throttle gate decision must read and update ``_last_progress_ms``
        # atomically — without this lock, two parallel workers (see
        # ``write_flat``'s thread pool) can both observe the gate as
        # "open" within the same window and both emit, doubling the
        # event rate the UI sees.
        with self._progress_lock:
            if not is_terminal:
                now_ms = time.monotonic() * 1000.0
                if now_ms - self._last_progress_ms < _PROGRESS_THROTTLE_MS:
                    return
                self._last_progress_ms = now_ms
            else:
                # Reset the gate on terminal events so the next phase
                # starts with a clean throttle window.
                self._last_progress_ms = time.monotonic() * 1000.0

        self._events.emit(
            PROGRESS,
            current=current,
            total=total,
            filename=filename,
            phase=phase,
        )
