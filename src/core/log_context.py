"""Thread-local profile context for the Python log file.

Two parallel backup runs (a scheduler-triggered one in the background
plus a manual one in the foreground) write to the same
``backup_manager.log`` rotating file. Without a per-line profile tag
the two streams interleave and become hard to decode when something
goes wrong.

``BackupEngine.run_backup`` sets the active profile name in a
thread-local context before kicking the pipeline; ``ProfilePrefixFilter``
mounted on the file handler reads that context on every record and
prefixes the message with ``[<profile_name>]``. Threads that never
set a context get an unprefixed message — preserving the legacy
look for app-level startup / shutdown / scheduler housekeeping.
"""

from __future__ import annotations

import logging
import threading

_context = threading.local()


def set_profile_context(profile_name: str) -> None:
    """Bind ``profile_name`` to the current thread for log prefixing."""
    _context.profile_name = profile_name


def clear_profile_context() -> None:
    """Drop the per-thread profile binding."""
    _context.profile_name = None


def current_profile_context() -> str | None:
    """Return the active profile name on this thread, or ``None``."""
    return getattr(_context, "profile_name", None)


class ProfilePrefixFilter(logging.Filter):
    """Prefix every record with ``[<profile_name>] `` when set.

    The filter rewrites ``record.msg`` so the prefix is applied
    before the formatter resolves ``%(message)s``. ``record.args``
    is consumed eagerly when present, otherwise placeholders would
    end up AFTER the prefix instead of being substituted.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        name = current_profile_context()
        if not name:
            return True
        # Resolve any lazy %-formatting first so the prefix sits in
        # front of the fully formatted message rather than the raw
        # template (the formatter runs against ``record.msg`` if
        # ``record.args`` is None).
        if record.args:
            try:
                rendered = record.msg % record.args
            except (TypeError, ValueError):
                rendered = record.msg
            record.args = None
            record.msg = rendered
        record.msg = f"[{name}] {record.msg}"
        return True
