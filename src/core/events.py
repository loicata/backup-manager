"""Event bus for decoupling core logic from UI.

Uses observer pattern: core emits events, UI subscribes to them.
Thread-safe for use with background backup threads.
"""

import logging
import threading
from collections import defaultdict
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Event type constants
PROGRESS = "progress"
LOG = "log"
STATUS = "status"
BACKUP_DONE = "backup_done"
ERROR = "error"
FILE_PROCESSED = "file_processed"
PHASE_CHANGED = "phase_changed"
PHASE_COUNT = "phase_count"


class EventBus:
    """Thread-safe event bus using observer pattern."""

    def __init__(self):
        self._subscribers: dict[str, list[Callable]] = defaultdict(list)
        self._lock = threading.Lock()

    def subscribe(self, event_type: str, callback: Callable) -> None:
        """Register a callback for an event type.

        Args:
            event_type: Event name (use constants above).
            callback: Function to call when event fires.
                      Receives keyword arguments from emit().
        """
        with self._lock:
            if callback not in self._subscribers[event_type]:
                self._subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        """Remove a callback for an event type."""
        with self._lock:
            try:
                self._subscribers[event_type].remove(callback)
            except ValueError:
                pass

    def emit(self, event_type: str, **data: Any) -> None:
        """Fire an event, calling all registered callbacks.

        Args:
            event_type: Event name.
            **data: Keyword arguments passed to callbacks.
        """
        with self._lock:
            callbacks = list(self._subscribers.get(event_type, []))

        for callback in callbacks:
            try:
                callback(**data)
            except Exception:
                logger.exception("Error in event callback for %r", event_type)

    def clear(self) -> None:
        """Remove all subscribers."""
        with self._lock:
            self._subscribers.clear()
