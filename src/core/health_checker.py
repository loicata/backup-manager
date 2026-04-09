"""Health checker for storage destinations.

Checks connectivity and free space for each configured destination
(storage + mirrors) in background threads. Returns results via callback.
"""

import logging
import re
import threading
from dataclasses import dataclass

from src.core.backup_engine import create_backend
from src.core.config import StorageConfig

logger = logging.getLogger(__name__)


@dataclass
class DestinationHealth:
    """Health status for a single storage destination.

    Attributes:
        label: Display name (e.g. "Storage", "Mirror 1").
        backend_type: Storage type (local, sftp, s3, network).
        online: True if reachable, False if check failed, None if pending.
        free_bytes: Free space in bytes, or None if unavailable (S3).
        error: Error message if check failed.
    """

    label: str
    backend_type: str
    online: bool | None = None
    free_bytes: int | None = None
    error: str = ""


def _check_destination(config: StorageConfig, label: str) -> DestinationHealth:
    """Check a single destination's health.

    Args:
        config: Storage configuration to check.
        label: Display label for this destination.

    Returns:
        DestinationHealth with connectivity and space info.
    """
    health = DestinationHealth(
        label=label,
        backend_type=config.storage_type.value,
    )

    try:
        backend = create_backend(config)

        # Use test_connection as the single check — it validates
        # connectivity AND reports free space in its message for
        # Local, SFTP, and Network backends (single connection).
        ok, msg = backend.test_connection()
        health.online = ok
        if not ok:
            health.error = msg
        else:
            health.free_bytes = _parse_free_space(msg)

    except Exception as e:
        health.online = False
        health.error = str(e)
        logger.debug("Health check failed for %s: %s", label, e)

    return health


def check_destinations_async(
    storage: StorageConfig,
    mirrors: list[StorageConfig],
    callback: callable,
) -> None:
    """Check all destinations in parallel background threads.

    Each destination is checked independently. The callback is called
    once per destination as soon as its check completes.

    Args:
        storage: Primary storage configuration.
        mirrors: List of mirror configurations.
        callback: Called with (index, DestinationHealth) for each result.
            index 0 = storage, 1+ = mirrors. Thread-safe caller required.
    """
    configs: list[tuple[int, StorageConfig, str]] = []

    try:
        storage.validate()
        configs.append((0, storage, "Storage"))
    except ValueError:
        pass  # Unconfigured storage — skip

    for i, mirror in enumerate(mirrors):
        try:
            mirror.validate()
            configs.append((i + 1, mirror, f"Mirror {i + 1}"))
        except ValueError:
            pass  # Unconfigured mirror — skip

    for idx, config, label in configs:
        thread = threading.Thread(
            target=_check_thread,
            args=(idx, config, label, callback),
            daemon=True,
            name=f"HealthCheck-{label}",
        )
        thread.start()


def _check_thread(
    idx: int,
    config: StorageConfig,
    label: str,
    callback: callable,
) -> None:
    """Thread target: check one destination and report via callback.

    Args:
        idx: Destination index (0=storage, 1+=mirrors).
        config: Storage configuration.
        label: Display label.
        callback: Result callback.
    """
    health = _check_destination(config, label)
    callback(idx, health)


_FREE_SPACE_RE = re.compile(r"([\d.]+)\s*GB\s*free", re.IGNORECASE)


def _parse_free_space(message: str) -> int | None:
    """Extract free space in bytes from test_connection message.

    Local, SFTP, and Network backends include "XX.X GB free" in
    their test_connection success message. S3 does not.

    Args:
        message: The info message from test_connection().

    Returns:
        Free space in bytes, or None if not found.
    """
    match = _FREE_SPACE_RE.search(message)
    if match:
        gb = float(match.group(1))
        return int(gb * 1024**3)
    return None


def format_bytes(size_bytes: int) -> str:
    """Format bytes as human-readable string.

    Args:
        size_bytes: Size in bytes.

    Returns:
        Formatted string like "45.2 GB".
    """
    if size_bytes < 0:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size_bytes) < 1024:
            if unit in ("B", "KB"):
                return f"{size_bytes:.0f} {unit}"
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"
