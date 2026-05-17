"""Event bus for decoupling core logic from UI.

Uses observer pattern: core emits events, UI subscribes to them.
Thread-safe for use with background backup threads.
"""

import contextlib
import logging
import threading
from collections import defaultdict
from collections.abc import Callable
from typing import Any

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
# Emitted once per backup right after ``_maybe_force_full`` has decided
# the effective backup_type. Payload: ``backup_type`` (str) and
# ``forced_full`` (bool). Lets the UI reflect "full (auto-promoted)"
# in the Run tab header instead of keeping the stale configured value
# while a promoted FULL actually runs.
BACKUP_TYPE_DETERMINED = "backup_type_determined"


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
        with self._lock, contextlib.suppress(ValueError):
            self._subscribers[event_type].remove(callback)

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


class ProfileTaggingEventBus:
    """``EventBus`` wrapper that auto-tags every emit with a ``profile_id``.

    The backup pipeline emits PROGRESS / LOG / STATUS / PHASE_CHANGED
    / PHASE_COUNT / BACKUP_TYPE_DETERMINED from many call sites — the
    engine itself, every ``PhaseLogger`` instance, individual phase
    modules. Threading the active profile id through every signature
    would touch dozens of call sites and forever risk drift. Wrapping
    the bus at the engine boundary inserts the tag in exactly one
    place, transparently to every consumer.

    The wrapper exposes the same ``emit`` / ``subscribe`` / ``unsubscribe``
    surface as ``EventBus`` so callers can hold it interchangeably.
    Subscriptions go straight through to the inner bus, so the
    main-thread UI subscribers register on the long-lived bus instance
    while the engine swaps its own ``_events`` attribute for the
    wrapped variant during ``run_backup``.

    Args:
        inner: The underlying ``EventBus`` (or any object with a
            compatible ``emit`` / ``subscribe`` / ``unsubscribe``
            interface).
        profile_id: The active profile's id, attached to every emit
            unless the caller already passes one (``setdefault`` —
            so a future caller can still override per emit).
    """

    def __init__(self, inner: "EventBus | ProfileTaggingEventBus", profile_id: str):
        self._inner = inner
        self._profile_id = profile_id

    def emit(self, event_type: str, **data: Any) -> None:
        data.setdefault("profile_id", self._profile_id)
        self._inner.emit(event_type, **data)

    def subscribe(self, event_type: str, callback: Callable) -> None:
        self._inner.subscribe(event_type, callback)

    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        self._inner.unsubscribe(event_type, callback)

    def clear(self) -> None:
        self._inner.clear()
