"""Per-profile run-event history persisted as JSONL.

Each profile gets its own append-only ``<profile_id>.jsonl`` file in
``run_history/``. Used by the Run tab to restore log messages and
phase tags when the user switches between profiles, including across
app restarts. Append is performed from the worker thread that emits
the LOG event; load runs on the main thread when the user activates a
profile in the sidebar.

The JSONL format keeps append O(1) (one ``open(..., "a")`` + write),
and a single corrupt line never poisons the entire file because each
line is parsed independently at load time.
"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

# Sentinel kept below the typical Treeview render budget. Rendering
# more than ~50 k rows freezes Tk for several seconds on profile
# switch. Older entries beyond this cap are dropped at LOAD time;
# the file itself is not rewritten so append stays O(1).
_MAX_ENTRIES_PER_PROFILE = 50_000


class RunHistoryStore:
    """Append-only JSONL history of log events per profile.

    One file per profile id under ``base_dir``. Each line is a JSON
    object with at least: ``ts`` (ISO timestamp), ``msg`` (text),
    ``level`` (info/warning/error), ``phase`` (short phase tag).
    Optional ``details`` (structured payload).

    Thread-safety: a single ``threading.Lock`` serialises append /
    load / delete across all profiles. The lock is held only for the
    actual filesystem call, never around user code.

    Args:
        base_dir: Directory under which per-profile JSONL files live.
            Created on first use if missing.
    """

    def __init__(self, base_dir: Path):
        self._base_dir = base_dir
        self._lock = threading.Lock()
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def _path_for(self, profile_id: str) -> Path:
        return self._base_dir / f"{profile_id}.jsonl"

    def append(self, profile_id: str, entry: dict) -> None:
        """Append a single event to the profile's history file.

        Drops events with an empty ``profile_id`` (cannot be attached
        to any profile view). JSON-serialisation errors are logged and
        swallowed — the live UI must not crash because a single
        payload was unserialisable.

        Args:
            profile_id: Profile owning this event. Empty → no-op.
            entry: JSON-serialisable mapping. Values containing
                literal newlines are tolerated (json.dumps escapes
                them) so the JSONL invariant is preserved.
        """
        if not profile_id:
            return
        try:
            line = json.dumps(entry, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            logger.warning(
                "RunHistory: drop unserialisable event for %s: %s",
                profile_id,
                exc,
            )
            return
        path = self._path_for(profile_id)
        with self._lock:
            try:
                with path.open("a", encoding="utf-8") as f:
                    f.write(line)
                    f.write("\n")
            except OSError as exc:
                logger.warning(
                    "RunHistory: append failed for %s: %s",
                    profile_id,
                    exc,
                )

    def load(self, profile_id: str) -> list[dict]:
        """Return all stored events for a profile, oldest first.

        Corrupt lines are skipped with a debug log entry; the rest of
        the file is still returned. When more than
        ``_MAX_ENTRIES_PER_PROFILE`` lines are present, only the tail
        (most recent) is returned.

        Args:
            profile_id: Profile identifier.

        Returns:
            List of parsed entries (possibly empty).
        """
        if not profile_id:
            return []
        path = self._path_for(profile_id)
        if not path.exists():
            return []
        lines = self._read_lines(path, profile_id)
        return self._parse_lines(lines[-_MAX_ENTRIES_PER_PROFILE:], profile_id)

    def _read_lines(self, path: Path, profile_id: str) -> list[str]:
        """Read all raw lines from ``path`` under the store lock."""
        with self._lock:
            try:
                with path.open("r", encoding="utf-8") as f:
                    return f.readlines()
            except OSError as exc:
                logger.warning(
                    "RunHistory: load failed for %s: %s",
                    profile_id,
                    exc,
                )
                return []

    @staticmethod
    def _parse_lines(lines: list[str], profile_id: str) -> list[dict]:
        """Decode JSONL lines, skipping blanks and corrupt rows."""
        entries: list[dict] = []
        for raw in lines:
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as exc:
                logger.debug(
                    "RunHistory: skip corrupt line in %s: %s",
                    profile_id,
                    exc,
                )
                continue
            if isinstance(obj, dict):
                entries.append(obj)
        return entries

    def delete(self, profile_id: str) -> None:
        """Remove the history file for a profile, if any.

        Called when the user deletes the profile. Failures are logged
        and swallowed — a stale history file on disk is a cosmetic
        defect, not a correctness issue.

        Args:
            profile_id: Profile identifier.
        """
        if not profile_id:
            return
        path = self._path_for(profile_id)
        with self._lock:
            try:
                if path.exists():
                    path.unlink()
            except OSError as exc:
                logger.warning(
                    "RunHistory: delete failed for %s: %s",
                    profile_id,
                    exc,
                )
