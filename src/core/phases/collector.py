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

# How many sample paths to keep per skipped category for the UI summary.
# Five is enough to identify the pattern (e.g. ``.pytest_cache``,
# ``...\evidence\<uuid>\volatile``) without flooding the run-tab log.
_SKIPPED_SAMPLE_LIMIT = 5


@dataclass
class _SkippedPaths:
    """Accumulator for paths the collector could not enter.

    The legacy code emitted one ``phase_log.warning`` per failure,
    which sent thousands of LOG events to the UI on workloads with
    a lot of restricted directories (``.pytest_cache`` from every
    Python project, ``volatile`` evidence folders from security
    tools, etc.). The Run tab became unusable.

    This accumulator keeps the per-path detail in the file log
    (``logger.debug``) so deep diagnostic stays available, but
    surfaces a single aggregated WARNING per category in the UI
    once the collect phase finishes.
    """

    permission_denied: list[str] = field(default_factory=list)
    permission_denied_count: int = 0
    os_errors: list[tuple[str, str]] = field(default_factory=list)
    os_errors_count: int = 0

    def add_permission(self, path: str) -> None:
        """Record a PermissionError on ``path``."""
        self.permission_denied_count += 1
        if len(self.permission_denied) < _SKIPPED_SAMPLE_LIMIT:
            self.permission_denied.append(path)
        logger.debug("Permission denied: %s", path)

    def add_os_error(self, path: str, message: str) -> None:
        """Record a generic OSError on ``path``."""
        self.os_errors_count += 1
        if len(self.os_errors) < _SKIPPED_SAMPLE_LIMIT:
            self.os_errors.append((path, message))
        logger.debug("Error accessing %s: %s", path, message)

    def emit_summary(self, phase_log: PhaseLogger) -> None:
        """Push one aggregated event per non-empty category to the UI.

        Permission-denied is the noisy-but-benign case (system caches,
        files locked by another app). Surfaced at INFO with reassuring
        wording so a novice user with thousands of these doesn't panic.

        OS errors are rarer and may indicate real disk/filesystem
        issues, so they stay at WARNING.

        The word ``Skipped`` is preserved in both messages so users
        searching the log can still find them.
        """
        if self.permission_denied_count:
            sample = "; ".join(self.permission_denied)
            extra = self.permission_denied_count - len(self.permission_denied)
            suffix = f" (+{extra} more)" if extra > 0 else ""
            phase_log.info(
                f"Skipped {self.permission_denied_count} protected item(s) "
                f"— typically system caches or files locked by another app "
                f"(this is normal, no action needed). "
                f"Examples: {sample}{suffix}"
            )
        if self.os_errors_count:
            sample = "; ".join(f"{p} ({m})" for p, m in self.os_errors)
            extra = self.os_errors_count - len(self.os_errors)
            suffix = f" (+{extra} more)" if extra > 0 else ""
            phase_log.warning(
                f"Skipped {self.os_errors_count} path(s) — "
                f"OS error. First: {sample}{suffix}"
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

    # Surface the active exclude patterns so a user can audit what
    # is being filtered out without hunting through the profile dialog.
    # Helps with the "did my files actually get backed up?" question
    # raised when novice users see the skip summary.
    if exclude:
        phase_log.info(f"Applying exclude patterns: {', '.join(exclude)}")

    for source in source_paths:
        source_path = Path(source)
        if not source_path.exists():
            phase_log.info(f"Source not found: {source}")
            continue

        if source_path.is_file():
            if not _is_excluded(source_path, exclude, source_path.parent):
                _add_file(files, seen, source_path, source_path.parent, source)
        elif source_path.is_dir():
            _collect_directory(files, seen, source_path, exclude, source, skipped)

    # One aggregated WARNING per category beats thousands of per-path
    # lines that drowned the Run-tab log on workloads with many
    # restricted directories (.pytest_cache, evidence/<uuid>/volatile…).
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
) -> None:
    """Recursively collect files from a directory.

    Errors are recorded in the ``skipped`` accumulator (per-path
    debug log + count) instead of being emitted as individual UI
    warnings.  ``collect_files`` flushes a single aggregated WARNING
    per category once the walk completes.
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
                    # Skip system/temp directories
                    if entry.name in _ALWAYS_EXCLUDED_DIRS:
                        continue
                    # Check if directory name matches exclusion
                    if _is_excluded(path, exclude, root_path):
                        continue
                    _collect_directory(files, seen, path, exclude, source_root, skipped)

                elif entry.is_file(follow_symlinks=False):
                    if not _is_excluded(path, exclude, root_path):
                        _add_file(files, seen, path, root_path, source_root)

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


def _is_excluded(
    filepath: Path,
    patterns: list[str],
    source_root: Path | None = None,
) -> bool:
    """Check if a path matches any exclusion pattern.

    Two pattern flavours are supported:

    * Patterns WITHOUT ``/`` (e.g. ``__pycache__``, ``*.tmp``) match
      against the file or directory **basename**, anywhere in the
      tree. This is the gitignore-style "any subdir called X"
      behaviour the existing default patterns rely on.
    * Patterns WITH ``/`` (e.g. ``*/evidence/*/volatile``) match
      against the **POSIX relative path** below ``source_root``. This
      lets users target a specific layout instead of every basename.
      ``fnmatch``'s ``*`` greedily spans path separators, so a
      pattern like ``*/evidence/*/volatile`` matches the dir
      ``loicata/WardSOAR/evidence/<uuid>/volatile`` regardless of
      depth above ``evidence``.

    When ``source_root`` is None, path-style patterns are skipped —
    callers that don't have a source_root context fall back to the
    basename-only behaviour.
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
                return True
        elif fnmatch.fnmatch(name, pattern):
            return True
    return False
