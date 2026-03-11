import functools
import logging
import os
import random
import shutil
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Optional

from src.core.config import StorageConfig, StorageType
from src.security.secure_memory import secure_clear

logger = logging.getLogger(__name__)


def with_retry(max_retries: int = 3, base_delay: float = 2.0):
    """Decorator for retrying operations with exponential backoff + jitter.
    Applied to cloud/remote storage operations (S3, Azure, GCS, SFTP).
    NOT applied to Local/Network storage (errors are non-transient)."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_retries:
                        delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        fname = getattr(func, '__name__', repr(func))
                        logger.warning(
                            f"Retry {attempt + 1}/{max_retries} for {fname}: {e} "
                            f"(waiting {delay:.1f}s)"
                        )
                        time.sleep(delay)
            raise last_error
        return wrapper
    return decorator


class ThrottledReader:
    """
    File-like wrapper that limits read speed to a target KB/s rate.
    Used to prevent network saturation during backup uploads.

    Wraps any file object and throttles read() calls by sleeping
    between chunks to maintain the target throughput.
    """

    def __init__(self, file_obj, limit_kbps: int):
        self._file = file_obj
        self._limit_bps = limit_kbps * 1024  # Convert KB/s to B/s
        self._start_time = time.monotonic()
        self._total_bytes = 0

    def read(self, size: int = -1) -> bytes:
        data = self._file.read(size)
        if not data or self._limit_bps <= 0:
            return data

        self._total_bytes += len(data)

        # Calculate how long the transfer should have taken at the target rate
        expected_time = self._total_bytes / self._limit_bps
        actual_time = time.monotonic() - self._start_time

        # Sleep if we're ahead of schedule
        if actual_time < expected_time:
            time.sleep(expected_time - actual_time)

        return data

    def __getattr__(self, name):
        """Forward all other attributes to the wrapped file."""
        return getattr(self._file, name)


class StorageBackend(ABC):
    """Abstract base class for all storage backends."""

    def __init__(self, config: StorageConfig):
        self.config = config
        self._progress_callback: Optional[Callable] = None
        self._bandwidth_limit_kbps: int = 0  # 0 = unlimited

    def set_progress_callback(self, callback: Optional[Callable]):
        self._progress_callback = callback

    def set_bandwidth_limit(self, kbps: int):
        """Set upload bandwidth limit in KB/s. 0 = unlimited."""
        self._bandwidth_limit_kbps = max(0, kbps)

    def _throttled_copy(self, src_path: Path, dst_path: Path):
        """
        Copy a file with optional bandwidth throttling.
        Used by local/network backends instead of shutil.copy2.
        """
        chunk_size = 64 * 1024  # 64 KB chunks

        if self._bandwidth_limit_kbps <= 0:
            # No limit — use fast native copy
            shutil.copy2(src_path, dst_path)
            return

        limit_bytes_per_sec = self._bandwidth_limit_kbps * 1024
        start_time = time.monotonic()
        total_bytes = 0

        with open(src_path, "rb") as fsrc, open(dst_path, "wb") as fdst:
            while True:
                chunk = fsrc.read(chunk_size)
                if not chunk:
                    break
                fdst.write(chunk)
                total_bytes += len(chunk)

                # Throttle: sleep if we're ahead of schedule
                expected_time = total_bytes / limit_bytes_per_sec
                actual_time = time.monotonic() - start_time
                if actual_time < expected_time:
                    time.sleep(expected_time - actual_time)

        # Preserve timestamps
        shutil.copystat(src_path, dst_path)

    def _get_throttled_reader(self, file_obj):
        """
        Wrap a file object with bandwidth throttling.
        Used by S3, Azure, SFTP backends for streaming uploads.
        Returns the original file if no limit is set.
        """
        if self._bandwidth_limit_kbps <= 0:
            return file_obj
        return ThrottledReader(file_obj, self._bandwidth_limit_kbps)

    @abstractmethod
    def upload(self, local_path: Path, remote_name: str) -> bool:
        """Upload a file or directory to the storage backend."""
        ...

    @abstractmethod
    def list_backups(self) -> list[dict]:
        """List available backups in the storage."""
        ...

    @abstractmethod
    def delete_backup(self, remote_name: str) -> bool:
        """Delete a backup from the storage."""
        ...

    @abstractmethod
    def test_connection(self) -> tuple[bool, str]:
        """Test the connection to the storage backend. Returns (success, message)."""
        ...

    def get_free_space(self) -> Optional[int]:
        """
        Get available free space in bytes on the destination.
        Returns None if the information is not available (e.g., cloud storage
        with no fixed quota, or the backend doesn't support this check).
        """
        return None  # Default: unknown

    def get_file_size(self, remote_name: str) -> Optional[int]:
        """
        Get the size in bytes of a remote file.
        Returns None if the file doesn't exist or the backend doesn't support this.
        Used for post-upload mirror verification.
        """
        return None  # Default: unknown

    @staticmethod
    def format_size(size_bytes: int) -> str:
        """Format byte size to human-readable string."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"


def get_storage_backend(config: StorageConfig) -> StorageBackend:
    """Factory function to create the appropriate storage backend."""
    match config.storage_type:
        case StorageType.LOCAL.value:
            from src.storage.local import LocalStorage
            return LocalStorage(config)
        case StorageType.NETWORK.value:
            from src.storage.network import NetworkStorage
            return NetworkStorage(config)
        case StorageType.S3.value:
            from src.storage.s3 import S3Storage
            return S3Storage(config)
        case StorageType.AZURE.value:
            from src.storage.azure import AzureStorage
            return AzureStorage(config)
        case StorageType.SFTP.value:
            from src.storage.sftp import SFTPStorage
            return SFTPStorage(config)
        case StorageType.GCS.value:
            from src.storage.gcs import GCSStorage
            return GCSStorage(config)
        case StorageType.PROTON.value:
            from src.storage.proton import ProtonDriveStorage
            return ProtonDriveStorage(config)
        case _:
            from src.storage.local import LocalStorage
            return LocalStorage(config)


def check_destination_space(
    storage_config,
    estimated_size_bytes: int = 0,
) -> tuple[bool, str]:
    """
    Utility function to check if a destination has enough free space.

    Args:
        storage_config: StorageConfig object or dict.
        estimated_size_bytes: Estimated backup size in bytes.

    Returns:
        (ok, message) — ok is True if space is sufficient or unknown.
    """
    if isinstance(storage_config, dict):
        storage_config = StorageConfig(**storage_config)

    try:
        backend = get_storage_backend(storage_config)
        free = backend.get_free_space()

        if free is None:
            return True, "ℹ Disk space cannot be checked for this destination."

        free_str = StorageBackend.format_size(free)

        if estimated_size_bytes > 0:
            needed_str = StorageBackend.format_size(estimated_size_bytes)
            if free < estimated_size_bytes:
                return False, (
                    f"❌ Espace insuffisant : {free_str} available, "
                    f"{needed_str} needed."
                )
            return True, (
                f"✅ Espace suffisant : {free_str} available "
                f"({needed_str} needed)."
            )

        return True, f"✅ Espace available : {free_str}"

    except Exception as e:
        return True, f"⚠ Verification not possible : {e}"
