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


# Substrings that indicate a transient wake-up failure from
# ``LocalStorage.test_connection`` — typically a USB HDD in deep
# power-save that needs more than ``CONNECTION_TIMEOUT`` seconds to
# spin up. The first probe itself triggers the wake-up, so a silent
# retry on the *next* poll tick (or immediately, see
# ``_check_destination``) almost always succeeds. Distinguishing these
# from real failures (drive missing, permission denied, network down)
# keeps the UI's "Storage: X GB free" card stable across the spin-up
# window and prevents a v3.7.1 user-visible flash that was widely
# reported on small post-backup workloads after the pool4 perf win
# shortened backup duration, leaving the drive idle earlier.
_TRANSIENT_WAKEUP_PATTERNS = (
    "timed out",
    "drive not ready",
)


def _is_transient_wakeup_error(message: str) -> bool:
    """Match the local-storage messages that indicate a USB drive
    still spinning up rather than a genuine failure.

    Args:
        message: The error string returned by
            ``backend.test_connection()`` when ``ok=False``.

    Returns:
        True when the message contains a known transient marker — the
        caller should retry once before declaring the drive offline.
    """
    low = message.lower()
    return any(p in low for p in _TRANSIENT_WAKEUP_PATTERNS)


def _check_destination(config: StorageConfig, label: str) -> DestinationHealth:
    """Check a single destination's health.

    Implements a one-shot silent retry on transient wake-up errors
    (USB HDD spin-up) to mask the v3.7.1 post-backup flash. The
    first probe ALREADY initiates the drive wake-up — the silent
    retry that follows almost always finds the drive responsive.
    Only persistent failures (two consecutive transient errors, or
    any non-transient error) reach the UI as an offline state.

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

        # Silent one-shot retry on transient wake-up failures. The
        # first probe started the drive spinning up; by the time we
        # call test_connection() again the drive is almost always
        # responsive. Non-transient errors (drive missing, permission
        # denied, network down) skip the retry — those won't recover
        # within seconds and surfacing them immediately is correct.
        if not ok and _is_transient_wakeup_error(msg):
            logger.debug(
                "Health check transient for %s (%s) — retrying once",
                label,
                msg,
            )
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
