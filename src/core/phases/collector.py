"""Phase 1: Collect files from source paths.

Walks source directories, applies exclusion patterns,
skips symlinks, and collects file metadata.
"""

import fnmatch
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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
    source_path: Path       # Absolute path on disk
    relative_path: str      # Path relative to source root
    size: int               # File size in bytes
    mtime: float            # Modification time (timestamp)
    source_root: str        # Which source path this came from

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
    events: Optional[EventBus] = None,
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

    for source in source_paths:
        source_path = Path(source)
        if not source_path.exists():
            phase_log.info(f"Source not found: {source}")
            continue

        if source_path.is_file():
            if not _is_excluded(source_path, exclude):
                _add_file(files, seen, source_path, source_path.parent, source)
        elif source_path.is_dir():
            _collect_directory(files, seen, source_path, exclude, source, phase_log)

    phase_log.info(f"Collected {len(files)} files from {len(source_paths)} sources")
    return files


def _collect_directory(
    files: list[FileInfo],
    seen: set[str],
    directory: Path,
    exclude: list[str],
    source_root: str,
    phase_log: PhaseLogger,
) -> None:
    """Recursively collect files from a directory."""
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
                    if _is_excluded(path, exclude):
                        continue
                    _collect_directory(files, seen, path, exclude, source_root, phase_log)

                elif entry.is_file(follow_symlinks=False):
                    if not _is_excluded(path, exclude):
                        _add_file(files, seen, path, Path(source_root), source_root)

            except PermissionError:
                phase_log.warning(f"Permission denied: {entry.path}")
            except OSError as e:
                phase_log.warning(f"Error accessing {entry.path}: {e}")

    except PermissionError:
        phase_log.warning(f"Permission denied: {directory}")
    except OSError as e:
        phase_log.warning(f"Error scanning {directory}: {e}")


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
        rel = filepath.relative_to(source_root).as_posix()
        files.append(FileInfo(
            source_path=filepath,
            relative_path=rel,
            size=st.st_size,
            mtime=st.st_mtime,
            source_root=source_root_str,
        ))
    except OSError:
        pass


def _is_excluded(filepath: Path, patterns: list[str]) -> bool:
    """Check if a path matches any exclusion pattern."""
    name = filepath.name
    for pattern in patterns:
        if fnmatch.fnmatch(name, pattern):
            return True
    return False
