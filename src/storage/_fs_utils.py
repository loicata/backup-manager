"""Robust filesystem removal utilities.

Centralises the tree-deletion logic used by ``LocalStorage``,
``NetworkStorage`` and the SFTP download cleanup so that three
recurring failure modes are handled uniformly:

1. **Windows long paths**.  The legacy ``shutil.rmtree`` plus the
   ``onerror=_force_remove_readonly`` callback could not reach paths
   longer than 260 chars (MAX_PATH).  When the source tree contained
   deeply nested directories — e.g. ``loicata\\Google Cybersecurity
   Professional Certificate\\Cours 2 ...\\Module 2`` — files inside
   were silently invisible to ``os.scandir``, so the parent directory
   could not be removed and the rotation log lied with
   ``GFS rotated: deleted ...``.

2. **Read-only attribute**.  Files like ``*.scr`` (screensavers) and
   anything synced from a CD or copied with ``robocopy /A:R`` carry
   the ``READONLY`` bit, blocking ``DeleteFile``.  We clear it before
   retrying.

3. **Transient locks**.  Antivirus scans, the Search Indexer and
   Explorer preview can briefly hold a handle (typically <500 ms) on
   a file we are about to remove.  We retry with exponential
   backoff before giving up.

The helper *never raises* on a removal failure: it returns a
structured ``RemoveResult`` so the caller decides whether to
propagate (``LocalStorage.delete_backup`` raises
``StorageDeleteError``) or just log (best-effort cleanup).
"""

from __future__ import annotations

import logging
import os
import stat
import time
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


# Windows extended-length path prefix.  Bypasses the legacy 260-char
# MAX_PATH limit imposed by ANSI Win32 APIs.  Required to delete files
# whose absolute path exceeds 260 chars.
_LONG_PATH_PREFIX = "\\\\?\\"
_LONG_PATH_UNC_PREFIX = "\\\\?\\UNC\\"


@dataclass
class Residual:
    """A path that could not be removed, with the last error seen."""

    path: str
    error: str

    def __str__(self) -> str:
        return f"{self.path} ({self.error})"


@dataclass
class RemoveResult:
    """Outcome of a tree-removal operation.

    Attributes:
        removed_files: Count of files successfully unlinked.
        removed_dirs: Count of directories successfully rmdir'd.
        residuals: Paths that could not be removed, with the final
            error each. Empty list means full success.
    """

    removed_files: int = 0
    removed_dirs: int = 0
    residuals: list[Residual] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return not self.residuals


def _winify(path: str) -> str:
    """Return a Windows long-path safe string for ``path``.

    On non-Windows systems the path is returned unchanged.  On
    Windows the ``\\\\?\\`` prefix is added (``\\\\?\\UNC\\`` for UNC
    paths) so subsequent ``os.unlink`` / ``os.rmdir`` / ``os.scandir``
    calls accept paths longer than 260 chars.

    The function is idempotent: a path already prefixed is returned
    unchanged.

    Args:
        path: Absolute or relative path string.

    Returns:
        Path string safe for use with ``os.*`` filesystem calls.
    """
    if os.name != "nt":
        return path
    if path.startswith(_LONG_PATH_PREFIX):
        return path
    # \\?\ requires an absolute path.
    abs_path = os.path.abspath(path)
    # UNC paths use the \\?\UNC\server\share form (drop the leading
    # "\\" then prepend "\\?\UNC\").
    if abs_path.startswith("\\\\"):
        return _LONG_PATH_UNC_PREFIX + abs_path.lstrip("\\")
    return _LONG_PATH_PREFIX + abs_path


def _strip_long_prefix(path: str) -> str:
    """Strip ``\\\\?\\`` (or ``\\\\?\\UNC\\``) from a path for display."""
    if path.startswith(_LONG_PATH_UNC_PREFIX):
        return "\\\\" + path[len(_LONG_PATH_UNC_PREFIX) :]
    if path.startswith(_LONG_PATH_PREFIX):
        return path[len(_LONG_PATH_PREFIX) :]
    return path


def _clear_readonly(path: str) -> None:
    """Best-effort clear of the READONLY attribute on ``path``.

    Failures are swallowed: the subsequent ``os.unlink`` /
    ``os.rmdir`` will surface the real error (and add it to the
    residuals list) if clearing didn't help.
    """
    try:
        os.chmod(path, stat.S_IWRITE)
    except OSError:
        pass


def _attempt_remove(
    path: str,
    remover,
    max_retries: int,
    base_delay: float,
) -> Exception | None:
    """Run ``remover(path)`` with retry on ``PermissionError``.

    Each retry first clears the READONLY attribute (in case that was
    the cause) and waits ``base_delay * 2**attempt`` seconds.  A
    ``FileNotFoundError`` is treated as success — another process or
    a previous retry already removed the entry.

    Args:
        path: Long-path-safe filesystem path.
        remover: Callable invoked with ``path`` (``os.unlink`` or
            ``os.rmdir``).
        max_retries: Total attempts (initial + retries).  ``1`` means
            no retry.
        base_delay: Delay before the first retry, doubled each step.

    Returns:
        ``None`` on success, the last exception on hard failure.
    """
    last_exc: Exception | None = None
    for attempt in range(max(1, max_retries)):
        try:
            remover(path)
            return None
        except FileNotFoundError:
            # Race-condition friendly: nothing left to do.
            return None
        except PermissionError as e:
            last_exc = e
            _clear_readonly(path)
        except OSError as e:
            # Includes WinError 32 (sharing violation) and 145
            # (directory not empty — surfaces here when a child
            # entry was missed earlier in the walk, e.g. a long-path
            # leaf).  Both are worth retrying briefly.
            last_exc = e

        if attempt + 1 < max_retries:
            time.sleep(base_delay * (2**attempt))

    return last_exc


def safe_remove_tree(
    path: Path,
    *,
    max_retries: int = 3,
    base_delay: float = 0.1,
) -> RemoveResult:
    """Remove a file or directory tree robustly on Windows and POSIX.

    Walks the tree bottom-up, unlinking files first then rmdir'ing
    directories, applying long-path prefixing, attribute clearing and
    retry on every operation.  Never raises on filesystem failures —
    the caller inspects ``RemoveResult`` to decide what to do.

    Args:
        path: File or directory to remove.  A nonexistent path is
            treated as already-removed (success, zero counts).
        max_retries: Per-entry retry count for transient locks.
            Default 3 covers typical antivirus / indexer holds without
            blocking the rotation phase for too long.
        base_delay: Initial backoff in seconds, doubled each retry.
            Default 0.1 s → 0.1, 0.2, 0.4 cumulative ~0.7 s worst-case
            per entry.

    Returns:
        ``RemoveResult`` with ``removed_files``, ``removed_dirs`` and
        ``residuals``.  ``result.success`` is ``True`` iff the target
        is fully gone from the filesystem.
    """
    if not isinstance(path, Path):
        # Accept str for ergonomic call sites; never trust the input.
        path = Path(path)

    result = RemoveResult()

    # exists() returns False for broken symlinks; fall back to lstat
    # to detect them too so we still try to unlink.
    try:
        path.lstat()
    except (FileNotFoundError, OSError):
        return result  # Already gone — success with zero counts

    # File or symlink: single unlink path.
    if path.is_file() or path.is_symlink() or not path.is_dir():
        target = _winify(str(path))
        err = _attempt_remove(target, os.unlink, max_retries, base_delay)
        if err is None:
            result.removed_files += 1
        else:
            result.residuals.append(
                Residual(
                    path=_strip_long_prefix(target),
                    error=f"{type(err).__name__}: {err}",
                )
            )
        return result

    # Directory case: bottom-up walk so children are gone before parent rmdir.
    root = _winify(str(path))

    # followlinks=False is the default but we are explicit so a
    # symlinked source tree never gets followed into and clobbered.
    for dirpath, dirnames, filenames in os.walk(root, topdown=False, followlinks=False):
        for name in filenames:
            full = os.path.join(dirpath, name)
            err = _attempt_remove(full, os.unlink, max_retries, base_delay)
            if err is None:
                result.removed_files += 1
            else:
                result.residuals.append(
                    Residual(
                        path=_strip_long_prefix(full),
                        error=f"{type(err).__name__}: {err}",
                    )
                )

        for name in dirnames:
            full = os.path.join(dirpath, name)
            err = _attempt_remove(full, os.rmdir, max_retries, base_delay)
            if err is None:
                result.removed_dirs += 1
            else:
                result.residuals.append(
                    Residual(
                        path=_strip_long_prefix(full),
                        error=f"{type(err).__name__}: {err}",
                    )
                )

    # Finally the root directory itself.
    err = _attempt_remove(root, os.rmdir, max_retries, base_delay)
    if err is None:
        result.removed_dirs += 1
    else:
        result.residuals.append(
            Residual(
                path=_strip_long_prefix(root),
                error=f"{type(err).__name__}: {err}",
            )
        )

    return result
