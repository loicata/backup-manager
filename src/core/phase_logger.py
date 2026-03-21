"""Unified logger for pipeline phases.

Replaces the duplicated _log() helper present in every phase module.
Combines Python logging with EventBus event emission, adding the
phase name to each event for downstream filtering.
"""

import logging

from src.core.events import LOG, PROGRESS, EventBus


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

    def info(self, message: str) -> None:
        """Log at INFO level and emit LOG event.

        Args:
            message: Human-readable log message.
        """
        self._logger.info(message)
        if self._events:
            self._events.emit(
                LOG,
                message=message,
                level="info",
                phase=self._phase_name,
            )

    def warning(self, message: str) -> None:
        """Log at WARNING level and emit LOG event.

        Args:
            message: Human-readable warning message.
        """
        self._logger.warning(message)
        if self._events:
            self._events.emit(
                LOG,
                message=message,
                level="warning",
                phase=self._phase_name,
            )

    def error(self, message: str) -> None:
        """Log at ERROR level and emit LOG event.

        Args:
            message: Human-readable error message.
        """
        self._logger.error(message)
        if self._events:
            self._events.emit(
                LOG,
                message=message,
                level="error",
                phase=self._phase_name,
            )

    def progress(
        self,
        current: int,
        total: int,
        filename: str,
        phase: str,
    ) -> None:
        """Emit a PROGRESS event for UI progress tracking.

        Args:
            current: Number of items processed so far.
            total: Total number of items to process.
            filename: Name of the file currently being processed.
            phase: Pipeline phase identifier for the progress bar.
        """
        if self._events:
            self._events.emit(
                PROGRESS,
                current=current,
                total=total,
                filename=filename,
                phase=phase,
            )
