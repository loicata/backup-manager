"""Abstract base class for storage backends.

Provides retry decorator, bandwidth throttling, and common interface.
All backends must implement: upload, upload_file, list_backups,
delete_backup, test_connection, get_free_space, get_file_size.
"""

import functools
import logging
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO

logger = logging.getLogger(__name__)


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
    """File-like wrapper that limits read speed.

    Used to prevent network saturation during uploads.
    """

    def __init__(self, fileobj: BinaryIO, limit_kbps: int):
        self._fileobj = fileobj
        self._limit_bps = limit_kbps * 1024  # Convert to bytes/sec
        self._bytes_read = 0
        self._start_time = time.monotonic()

    def read(self, size: int = -1) -> bytes:
        data = self._fileobj.read(size)
        if not data or self._limit_bps <= 0:
            return data

        self._bytes_read += len(data)
        elapsed = time.monotonic() - self._start_time
        if elapsed > 0:
            current_rate = self._bytes_read / elapsed
            if current_rate > self._limit_bps:
                sleep_time = (self._bytes_read / self._limit_bps) - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

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

    def set_progress_callback(self, callback: Callable) -> None:
        """Set callback for upload progress: callback(bytes_sent, total_bytes)."""
        self._progress_callback = callback

    def set_bandwidth_limit(self, kbps: int) -> None:
        """Set bandwidth limit in KB/s (0 = unlimited)."""
        self._bandwidth_limit_kbps = max(0, kbps)

    def _get_throttled_reader(self, fileobj: BinaryIO) -> BinaryIO:
        """Wrap file object with throttling if bandwidth limit is set."""
        if self._bandwidth_limit_kbps > 0:
            return ThrottledReader(fileobj, self._bandwidth_limit_kbps)
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
