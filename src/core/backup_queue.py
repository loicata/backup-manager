"""Pure helpers for the UI backup queue (coalescing logic).

Extracted from ``src/ui/app.py`` so the queueing decision can be unit
tested without spinning up a Tk interpreter. The UI layer maps
``BackupProfile`` objects to their ids around these functions.
"""

from __future__ import annotations

from collections.abc import Iterable


def select_profiles_to_queue(
    requested_ids: list[str],
    excluded_ids: Iterable[str],
) -> tuple[list[str], list[str]]:
    """Partition requested profile ids into ``(to_queue, skipped)`` with coalescing.

    A backup is idempotent, so re-requesting a profile that is already
    running or already queued must not stack a second identical run.
    This helper enforces the "at most one pending run per profile" rule
    while preserving the order in which profiles were requested.

    Args:
        requested_ids: Profile ids the user asked to back up, in the
            order they should run.
        excluded_ids: Profile ids that must not be queued again —
            typically the union of currently-running ids and
            already-queued ids.

    Returns:
        A ``(to_queue, skipped)`` tuple. ``to_queue`` holds the ids that
        are new (neither excluded nor duplicated within
        ``requested_ids``), in request order. ``skipped`` holds the ids
        dropped because they were already running, already queued, or
        duplicated within the request.

    Raises:
        TypeError: If ``requested_ids`` is not a list, or contains a
            value that is not a non-empty ``str``.
    """
    if not isinstance(requested_ids, list):
        raise TypeError(f"requested_ids must be a list, got {type(requested_ids).__name__}")

    # Seed the "already handled" set with the caller's exclusions so the
    # first occurrence of an excluded id is skipped, not queued.
    seen: set[str] = set(excluded_ids)
    to_queue: list[str] = []
    skipped: list[str] = []
    for pid in requested_ids:
        if not isinstance(pid, str) or not pid:
            raise TypeError(f"requested_ids must contain non-empty str, got {pid!r}")
        if pid in seen:
            skipped.append(pid)
        else:
            to_queue.append(pid)
            seen.add(pid)
    return to_queue, skipped
