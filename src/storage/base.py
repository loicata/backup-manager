"""Abstract base class for storage backends.

Provides retry decorator, bandwidth throttling, and common interface.
All backends must implement: upload, upload_file, list_backups,
delete_backup, test_connection, get_free_space, get_file_size.
"""

import functools
import logging
import os
import random
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO

logger = logging.getLogger(__name__)


def long_path_str(path: Path) -> str:
    """Return a Windows extended-length path string if needed.

    Adds the ``\\\\?\\`` prefix on Windows to bypass the 260-char
    MAX_PATH limit.  On other platforms returns the path unchanged.

    Args:
        path: The path to convert.

    Returns:
        String representation safe for long paths.
    """
    s = str(path.resolve())
    if os.name == "nt" and not s.startswith("\\\\?\\"):
        return f"\\\\?\\{s}"
    return s


def long_path_mkdir(path: Path) -> None:
    """Create a directory, handling long paths on Windows.

    Args:
        path: Directory to create.
    """
    if os.name == "nt":
        os.makedirs(long_path_str(path), exist_ok=True)
    else:
        path.mkdir(parents=True, exist_ok=True)


def with_retry(max_retries: int = 3, base_delay: float = 2.0):
    """Decorator for retrying operations with exponential backoff + jitter.

    Args:
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds (doubled each retry).
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = base_delay * (2**attempt) + random.uniform(0, 1)
                        logger.warning(
                            "%s attempt %d/%d failed: %s. Retrying in %.1fs",
                            func.__name__,
                            attempt + 1,
                            max_retries + 1,
                            e,
                            delay,
                        )
                        time.sleep(delay)
            raise last_exception

        return wrapper

    return decorator


class ThrottledReader:
    """File-like wrapper that limits read speed and checks cancellation.

    Uses a 1-second sliding window to maintain a smooth, stable
    throughput instead of burst-pause patterns.  The window resets
    every second, preventing accumulated drift that causes
    increasingly long pauses.

    Also checks for cancellation on every read, enabling responsive
    cancel even during long uploads (S3, slow SFTP).

    Thread-safety
    -------------
    boto3's ``upload_fileobj`` may dispatch concurrent reads from the
    s3transfer worker pool when a multipart upload is triggered.
    Without a lock, two workers could interleave reads of the wrapped
    fileobj (producing a corrupted part) and race on the window
    counters (producing mis-applied throttling).  All read-side state
    — including the delegated ``_fileobj.read`` call — is therefore
    serialized through a single lock so concurrent callers see a
    consistent byte stream and accurate accounting.
    """

    def __init__(self, fileobj: BinaryIO, limit_kbps: int = 0, cancel_check=None):
        self._fileobj = fileobj
        self._limit_bps = limit_kbps * 1024  # Convert to bytes/sec
        self._cancel_check = cancel_check
        self._window_bytes = 0
        self._window_start = time.monotonic()
        self._lock = threading.Lock()

    def read(self, size: int = -1) -> bytes:
        with self._lock:
            if self._cancel_check is not None:
                self._cancel_check()

            data = self._fileobj.read(size)
            if not data or self._limit_bps <= 0:
                return data

            self._window_bytes += len(data)
            elapsed = time.monotonic() - self._window_start
            if elapsed > 0:
                current_rate = self._window_bytes / elapsed
                if current_rate > self._limit_bps:
                    sleep_time = (self._window_bytes / self._limit_bps) - elapsed
                    if sleep_time > 0:
                        time.sleep(sleep_time)

            # Reset window every second for smoother throughput
            now = time.monotonic()
            if (now - self._window_start) >= 1.0:
                self._window_bytes = 0
                self._window_start = now

            return data

    def seek(self, *args, **kwargs):
        return self._fileobj.seek(*args, **kwargs)

    def tell(self) -> int:
        return self._fileobj.tell()

    def close(self):
        return self._fileobj.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def name(self):
        return getattr(self._fileobj, "name", "<throttled>")


class StorageBackend(ABC):
    """Abstract base class for all storage backends."""

    def __init__(self):
        self._progress_callback: Callable | None = None
        self._bandwidth_limit_kbps: int = 0
        self._cancel_check: Callable | None = None

    def set_progress_callback(self, callback: Callable) -> None:
        """Set callback for upload progress: callback(bytes_sent, total_bytes)."""
        self._progress_callback = callback

    def set_bandwidth_limit(self, kbps: int) -> None:
        """Set bandwidth limit in KB/s (0 = unlimited)."""
        self._bandwidth_limit_kbps = max(0, kbps)

    def set_cancel_check(self, cancel_check: Callable) -> None:
        """Set callable that raises CancelledError when cancel is requested."""
        self._cancel_check = cancel_check

    def _get_throttled_reader(self, fileobj: BinaryIO) -> BinaryIO:
        """Wrap file object with throttling and/or cancel checking.

        Always wraps when a cancel_check is set (for responsive cancel
        during long uploads). Also wraps when bandwidth limit is set.
        """
        if self._bandwidth_limit_kbps > 0 or self._cancel_check is not None:
            return ThrottledReader(
                fileobj,
                limit_kbps=self._bandwidth_limit_kbps,
                cancel_check=self._cancel_check,
            )
        return fileobj

    @abstractmethod
    def upload(self, local_path: Path, remote_name: str) -> None:
        """Upload a local file or directory to the backend.

        Args:
            local_path: Local file/directory to upload.
            remote_name: Name for the backup on the remote.

        Raises:
            Exception on failure (enables with_retry).
        """

    @abstractmethod
    def upload_file(self, fileobj: BinaryIO, remote_path: str, size: int = 0) -> None:
        """Upload a file-like object (streaming, no temp).

        Used by RemoteWriter for file-by-file streaming uploads.

        Args:
            fileobj: Readable file-like object.
            remote_path: Full remote path for the file.
            size: Expected file size (for progress tracking).

        Raises:
            Exception on failure.
        """

    @abstractmethod
    def list_backups(self) -> list[dict]:
        """List available backups.

        Returns:
            List of dicts with at least: name, size, modified.
        """

    @abstractmethod
    def delete_backup(self, remote_name: str) -> None:
        """Delete a backup.

        Args:
            remote_name: Backup name to delete.

        Raises:
            Exception on failure.
        """

    @abstractmethod
    def test_connection(self) -> tuple[bool, str]:
        """Test if the backend is reachable and writable.

        Returns:
            (success, message)
        """

    @abstractmethod
    def get_free_space(self) -> int | None:
        """Get available space in bytes, or None if unknown."""

    @abstractmethod
    def get_file_size(self, remote_name: str) -> int | None:
        """Get size of a remote file in bytes, or None if unknown."""

    def list_backup_files(self, backup_name: str) -> list[tuple[str, int]]:
        """List files inside a backup with their sizes.

        Used for post-upload verification on remote backends.

        Args:
            backup_name: Name of the backup directory on the remote.

        Returns:
            List of (relative_path, size_bytes) tuples.
            Returns empty list if not supported by the backend.
        """
        return []

    def verify_backup_files(self, backup_name: str) -> list[tuple[str, int, str]]:
        """List files inside a backup with sizes and checksums.

        Used for post-upload integrity verification. Backends that
        support server-side hashing should override this method.

        Args:
            backup_name: Name of the backup directory on the remote.

        Returns:
            List of (relative_path, size_bytes, checksum) tuples.
            checksum is a hex digest (sha256 or md5) or "" if unavailable.
            Returns empty list if not supported by the backend.
        """
        return []

    @abstractmethod
    def download_backup(self, remote_name: str, local_dir: Path) -> Path:
        """Download a backup from the remote to a local directory.

        Args:
            remote_name: Backup name on the remote.
            local_dir: Local directory to download into.

        Returns:
            Path to the downloaded backup folder.

        Raises:
            Exception on failure.
        """
