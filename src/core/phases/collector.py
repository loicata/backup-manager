"""Phase 1: Collect files from source paths.

Walks source directories, applies exclusion patterns,
skips symlinks, and collects file metadata.
"""

import fnmatch
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

from src.core.events import EventBus
from src.core.phase_logger import PhaseLogger

logger = logging.getLogger(__name__)

# Directory names that are always excluded (temp/sync artifacts).
_ALWAYS_EXCLUDED_DIRS = {
    ".tmp.drivedownload",
    ".tmp.driveupload",
    "$RECYCLE.BIN",
    "System Volume Information",
    ".Trash-1000",
}

class _ScanHeartbeat:
    """Live progress updates while the collector walks the source tree.

    On large workloads (100 k+ files) the recursive walk can run for
    a full minute between the ``Applying exclude patterns`` event and
    the final ``Collected N files`` event. Without an in-flight signal
    the Run-tab Log shows nothing during that gap and the user
    legitimately thinks the app has crashed.

    Solution: at every scanned entry call ``tick``; ``PhaseLogger.progress``
    is already throttled to 10 Hz, so the bus only sees ~10 events per
    second regardless of how fast the walk runs. ``total=0`` flags
    these events as "indeterminate scan progress" — the Run-tab
    interprets that signal to update the status_label only, leaving
    the determinate progress bar at 0 % until the real phases
    (manifest, writer, verifier) report actual ratios.
    """

    __slots__ = ("_phase_log", "_files", "_dirs")

    def __init__(self, phase_log: PhaseLogger):
        self._phase_log = phase_log
        self._files = 0
        self._dirs = 0

    def tick_file(self) -> None:
        self._files += 1
        self._emit()

    def tick_dir(self) -> None:
        self._dirs += 1
        self._emit()

    def _emit(self) -> None:
        # ``filename`` carries the human-readable status that the
        # Run-tab will surface in its status label.
        self._phase_log.progress(
            current=self._files + self._dirs,
            total=0,
            filename=f"{self._files} files in {self._dirs} folders",
            phase="collecting",
        )


@dataclass
class _SkippedPaths:
    """Accumulator for paths the collector could not (or would not) include.

    Tracks three categories of exclusion, all surfaced in the Run-tab
    Log as a single expandable ``Skipped N file(s) not backed up`` entry
    so the user can drill down to verify whether a specific file made it
    into the backup or not.

    Categories tracked:

    - ``permission_denied``: ``PermissionError`` during ``os.scandir`` —
      typically locked system caches, files held by another process.
    - ``os_errors``: generic ``OSError`` during the walk — rarer, may
      indicate real disk/filesystem issues.
    - ``excluded_by_pattern``: file or directory that matched one of
      the user-configured exclude patterns (``*.tmp``, ``__pycache__``,
      ``node_modules``, etc.). Matched **directories** are recorded as
      a single entry — the collector does not descend into them, so
      individual files inside an excluded directory never appear here.
      The user searching for a specific file inside ``node_modules``
      finds the parent directory and infers the file is excluded.

    No cap is applied to the lists. The previous ``_SKIPPED_SAMPLE_LIMIT``
    of 5 made the in-Log expansion useless on real workloads (with one
    or two large excluded directories you would see "Examples: …" with
    most of the truth hidden). Memory cost is bounded by the actual
    filesystem: every path is at most a few hundred bytes; even on a
    pathological 100 k-skipped scenario the accumulator stays under
    ~30 MB which we trade for usable diagnostic.
    """

    permission_denied: list[str] = field(default_factory=list)
    os_errors: list[tuple[str, str]] = field(default_factory=list)
    excluded_by_pattern: list[tuple[str, str]] = field(default_factory=list)

    @property
    def total_count(self) -> int:
        """Total number of skipped paths across all categories."""
        return (
            len(self.permission_denied)
            + len(self.os_errors)
            + len(self.excluded_by_pattern)
        )

    def add_permission(self, path: str) -> None:
        """Record a PermissionError on ``path``."""
        self.permission_denied.append(path)
        logger.debug("Permission denied: %s", path)

    def add_os_error(self, path: str, message: str) -> None:
        """Record a generic OSError on ``path``."""
        self.os_errors.append((path, message))
        logger.debug("Error accessing %s: %s", path, message)

    def add_excluded(self, path: str, pattern: str) -> None:
        """Record a path skipped because it matched an exclude pattern."""
        self.excluded_by_pattern.append((path, pattern))
        logger.debug("Excluded by %r: %s", pattern, path)

    def emit_summary(self, phase_log: PhaseLogger) -> None:
        """Push one aggregated ``Skipped`` event with a structured payload.

        The Run-tab Log widget reads ``details`` to build a hierarchical
        view (Skipped → category-by-file-type → extension → path with
        reason). When the bus is hooked to a plain text consumer the
        ``message`` alone still provides a meaningful one-liner.

        OS errors stay at INFO level too (no point alerting the user
        louder when they cannot act on it from the UI) — the previous
        WARNING split into two messages is replaced by a single one
        whose ``details`` contains all three categories.
        """
        total = self.total_count
        if total == 0:
            return

        details = {
            "permission_denied": list(self.permission_denied),
            "os_errors": list(self.os_errors),
            "excluded_by_pattern": list(self.excluded_by_pattern),
        }
        phase_log.info(
            f"Skipped {total} file(s) not backed up — click to inspect by category",
            details=details,
        )


@dataclass
class FileInfo:
    """Metadata for a collected file.

    Args:
        source_path: Absolute path on disk.
        relative_path: Path relative to source root.
        size: File size in bytes.
        mtime: Modification time (timestamp).
        source_root: Which source path this came from.

    Raises:
        ValueError: If source_path is None, relative_path is empty,
                    or size is negative.
    """

    source_path: Path  # Absolute path on disk
    relative_path: str  # Path relative to source root
    size: int  # File size in bytes
    mtime: float  # Modification time (timestamp)
    source_root: str  # Which source path this came from

    def __post_init__(self) -> None:
        """Validate field invariants after construction."""
        if self.source_path is None:
            raise ValueError("source_path must not be None")
        if not self.relative_path:
            raise ValueError("relative_path must not be empty")
        if self.size < 0:
            raise ValueError(f"size must be >= 0, got {self.size}")


def collect_files(
    source_paths: list[str],
    exclude_patterns: list[str] | None = None,
    events: EventBus | None = None,
) -> list[FileInfo]:
    """Collect all files from source paths.

    Args:
        source_paths: List of absolute paths (files or directories).
        exclude_patterns: Glob patterns to exclude (e.g., "*.tmp").
        events: Optional event bus for logging.

    Returns:
        List of FileInfo for all collected files.
    """
    phase_log = PhaseLogger("collector", events)
    exclude = exclude_patterns or []
    files: list[FileInfo] = []
    seen: set[str] = set()  # Avoid duplicates
    skipped = _SkippedPaths()
    heartbeat = _ScanHeartbeat(phase_log)

    # Surface the active exclude patterns so a user can audit what
    # is being filtered out without hunting through the profile dialog.
    # Helps with the "did my files actually get backed up?" question
    # raised when novice users see the skip summary. The Run-tab Log
    # widget renders ``details`` as expandable children under this
    # parent line; consumers without details support still get a
    # readable summary in ``message``.
    if exclude:
        phase_log.info(
            f"Applying exclude patterns ({len(exclude)})",
            details={"patterns": list(exclude)},
        )

    for source in source_paths:
        source_path = Path(source)
        if not source_path.exists():
            phase_log.info(f"Source not found: {source}")
            continue

        if source_path.is_file():
            matched = _match_excluded(source_path, exclude, source_path.parent)
            if matched is None:
                _add_file(files, seen, source_path, source_path.parent, source)
                heartbeat.tick_file()
            else:
                skipped.add_excluded(str(source_path), matched)
        elif source_path.is_dir():
            _collect_directory(
                files, seen, source_path, exclude, source, skipped, heartbeat
            )

    # Single aggregated event for the whole phase. ``details`` carries
    # the per-category lists so the Run-tab Log can rebuild the
    # category-by-extension hierarchy without parsing the message.
    skipped.emit_summary(phase_log)
    phase_log.info(f"Collected {len(files)} files from {len(source_paths)} sources")
    return files


def _collect_directory(
    files: list[FileInfo],
    seen: set[str],
    directory: Path,
    exclude: list[str],
    source_root: str,
    skipped: _SkippedPaths,
    heartbeat: "_ScanHeartbeat | None" = None,
) -> None:
    """Recursively collect files from a directory.

    Errors are recorded in the ``skipped`` accumulator (per-path
    debug log + count) instead of being emitted as individual UI
    warnings.  ``collect_files`` flushes a single aggregated WARNING
    per category once the walk completes.

    ``heartbeat`` (optional) is ticked after every entry so the
    Run-tab status label keeps animating during long walks. The
    PhaseLogger throttles the resulting PROGRESS events to 10 Hz so
    a 100 k-entry walk emits ~600 events instead of 100 k.
    """
    root_path = Path(source_root)
    try:
        for entry in os.scandir(directory):
            try:
                path = Path(entry.path)

                # Skip symlinks
                if entry.is_symlink():
                    continue

                if entry.is_dir(follow_symlinks=False):
                    # Skip system/temp directories silently — the user
                    # has no actionable interest in $RECYCLE.BIN etc.
                    if entry.name in _ALWAYS_EXCLUDED_DIRS:
                        continue
                    # Check if directory name matches exclusion. We do
                    # NOT descend into excluded directories — the
                    # accumulator records the directory as a single
                    # entry rather than enumerating every file inside,
                    # which would defeat the whole point of an exclude
                    # pattern (and turn a simple "skip node_modules"
                    # into a 50 k-row UI dump).
                    matched = _match_excluded(path, exclude, root_path)
                    if matched is not None:
                        skipped.add_excluded(str(path), matched)
                        continue
                    if heartbeat is not None:
                        heartbeat.tick_dir()
                    _collect_directory(
                        files, seen, path, exclude, source_root, skipped, heartbeat
                    )

                elif entry.is_file(follow_symlinks=False):
                    matched = _match_excluded(path, exclude, root_path)
                    if matched is None:
                        _add_file(files, seen, path, root_path, source_root)
                        if heartbeat is not None:
                            heartbeat.tick_file()
                    else:
                        skipped.add_excluded(str(path), matched)

            except PermissionError:
                skipped.add_permission(entry.path)
            except OSError as e:
                skipped.add_os_error(entry.path, str(e))

    except PermissionError:
        skipped.add_permission(str(directory))
    except OSError as e:
        skipped.add_os_error(str(directory), str(e))


def _add_file(
    files: list[FileInfo],
    seen: set[str],
    filepath: Path,
    source_root: Path,
    source_root_str: str,
) -> None:
    """Add a file to the collection if not already seen."""
    abs_path = str(filepath.resolve())
    if abs_path in seen:
        return
    seen.add(abs_path)

    try:
        st = filepath.stat()
        inner_rel = filepath.relative_to(source_root).as_posix()
        # Prefix with source directory name to preserve folder structure
        # when multiple sources are configured.
        rel = f"{source_root.name}/{inner_rel}"
        files.append(
            FileInfo(
                source_path=filepath,
                relative_path=rel,
                size=st.st_size,
                mtime=st.st_mtime,
                source_root=source_root_str,
            )
        )
    except OSError:
        pass


def _match_excluded(
    filepath: Path,
    patterns: list[str],
    source_root: Path | None = None,
) -> str | None:
    """Return the first pattern that matches ``filepath``, else ``None``.

    Two pattern flavours are supported (see ``_is_excluded`` for the
    semantic detail):

    * Patterns WITHOUT ``/`` match against the basename, anywhere.
    * Patterns WITH ``/`` match against the POSIX relative path
      below ``source_root``.

    When ``source_root`` is None, path-style patterns are skipped.

    Returning the matched pattern (instead of just a bool) lets
    ``_SkippedPaths.add_excluded`` record the rule that caught the
    file — the Run-tab Log surfaces this as ``excluded: <pattern>``
    next to each path so the user can tell which rule sent their
    file to the skip pile. ``_is_excluded`` is preserved as a thin
    wrapper for callers that only need the bool.
    """
    name = filepath.name
    rel_path: str | None = None
    if source_root is not None:
        try:
            rel_path = filepath.relative_to(source_root).as_posix()
        except ValueError:
            rel_path = None

    for pattern in patterns:
        if "/" in pattern:
            if rel_path is not None and fnmatch.fnmatch(rel_path, pattern):
                return pattern
        elif fnmatch.fnmatch(name, pattern):
            return pattern
    return None


def _is_excluded(
    filepath: Path,
    patterns: list[str],
    source_root: Path | None = None,
) -> bool:
    """Backward-compatible bool wrapper around :func:`_match_excluded`."""
    return _match_excluded(filepath, patterns, source_root) is not None
