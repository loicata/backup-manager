"""Bandwidth tester: measures write throughput to a storage backend.

Uploads a single 256 MB sample with
a warmup pass to establish the connection, then a measurement pass.
Used before backup to calculate a throttle limit from the user's
percentage setting.
"""

import io
import logging
import os
import time
import uuid

from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)

# Single sample size: 256 MB (accurate measurement for large backups)
SAMPLE_SIZE = 256 * 1024 * 1024

# Warmup size: 1 MB (just to establish connection)
WARMUP_SIZE = 1 * 1024 * 1024

# Prefix for temporary test files
TEMP_PREFIX = ".bm_speedtest_"


def measure_bandwidth(backend: StorageBackend) -> float:
    """Measure write speed to a storage backend.

    Performs a 1 MB warmup upload (to establish connection and fill
    OS buffers), then a single 16 MB measurement.  Returns the
    throughput in bytes per second.

    If both attempts fail, returns 0.0 (no throttling applied).

    Args:
        backend: Connected storage backend to test.

    Returns:
        Write speed in bytes per second, or 0.0 on failure.
    """
    # Warmup: establish connection, fill OS/SSH buffers
    try:
        _write_sample(backend, WARMUP_SIZE)
        logger.debug("Bandwidth warmup complete")
    except Exception as exc:
        logger.warning("Bandwidth warmup failed: %s", exc)

    # Measurement: single large sample
    try:
        speed = _write_sample(backend, SAMPLE_SIZE)
        if speed > 0:
            logger.info(
                "Bandwidth test: %.2f MB/s (256 MB sample)",
                speed / (1024 * 1024),
            )
            return speed
    except Exception as exc:
        logger.warning("Bandwidth measurement failed: %s", exc)

    logger.error("Bandwidth test failed — no throttling applied")
    return 0.0


def compute_throttle_kbps(measured_bps: float, percent: int) -> int:
    """Compute throttle limit in KB/s from measured speed and percentage.

    Args:
        measured_bps: Measured bandwidth in bytes per second.
        percent: User-configured percentage (25, 50, 75, or 100).

    Returns:
        Throttle limit in KB/s (0 means unlimited).
    """
    if measured_bps <= 0 or percent >= 100:
        return 0
    if percent <= 0:
        percent = 25
    return max(1, int((measured_bps / 1024) * (percent / 100)))


def _write_sample(backend: StorageBackend, size: int) -> float:
    """Upload a single sample file and return write speed.

    Args:
        backend: Storage backend to write to.
        size: Sample size in bytes.

    Returns:
        Speed in bytes per second.

    Raises:
        Exception: On upload failure.
    """
    temp_name = f"{TEMP_PREFIX}{uuid.uuid4().hex[:8]}_{size}"
    data = os.urandom(size)

    start = time.monotonic()
    try:
        backend.upload_file(io.BytesIO(data), temp_name, size=size)
    finally:
        _cleanup(backend, temp_name)

    elapsed = time.monotonic() - start
    if elapsed <= 0:
        return 0.0
    return size / elapsed


def _cleanup(backend: StorageBackend, name: str) -> None:
    """Remove a temporary test file from the backend.

    Best-effort: logs a warning on failure but does not raise.

    Args:
        backend: Storage backend.
        name: Remote file name to delete.
    """
    try:
        backend.delete_backup(name)
    except FileNotFoundError:
        pass
    except Exception as exc:
        logger.warning("Failed to clean up speed test file %s: %s", name, exc)
