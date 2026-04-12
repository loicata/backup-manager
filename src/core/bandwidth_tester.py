"""Bandwidth tester: measures real write throughput to a storage backend.

Uses an adaptive approach:
1. Small probe (2 MB) to detect link speed
2. If fast link (>10 MB/s): full 128 MB sample for accurate measurement
3. If slow link: use the probe result directly (avoids saturating
   slow connections for extended periods which can freeze the OS)

For SFTP backends, uses ``sync`` via exec channel to ensure data is
flushed to disk on the remote before stopping the timer.
"""

import io
import logging
import os
import time
import uuid

from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)

# Probe size: 16 MB — detects link speed and serves as measurement
# for slow links. At 1 MB/s = 16 seconds, at 9 MB/s = ~2 seconds.
PROBE_SIZE = 16 * 1024 * 1024

# Full sample: 128 MB — used only on fast links (>10 MB/s)
# At 100 MB/s this takes ~1.3 seconds. Accurate measurement.
FULL_SAMPLE_SIZE = 128 * 1024 * 1024

# Speed threshold: links faster than this get the full sample
# 10 MB/s = fast enough that 128 MB completes in ~13 seconds
FAST_LINK_THRESHOLD = 10 * 1024 * 1024  # 10 MB/s

# Warmup size: 1 MB (establish connection, fill OS buffers)
WARMUP_SIZE = 1 * 1024 * 1024

# Prefix for temporary test files
TEMP_PREFIX = ".bm_speedtest_"


def measure_bandwidth(backend: StorageBackend) -> float:
    """Measure real write speed to a storage backend.

    Adaptive approach:
    - Starts with a 2 MB probe to detect if the link is fast or slow.
    - Fast links (>10 MB/s): runs a full 128 MB test for precision.
    - Slow links: uses the 2 MB probe result to avoid saturating
      the connection and freezing the OS.

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

    # Probe: quick 2 MB test to detect link speed
    try:
        probe_speed = _write_sample(backend, PROBE_SIZE)
        if probe_speed <= 0:
            logger.error("Bandwidth probe returned 0 — no throttling")
            return 0.0

        probe_mbps = probe_speed / (1024 * 1024)
        logger.info("Bandwidth probe: %.2f MB/s (16 MB probe)", probe_mbps)

        # Fast link: run full measurement for accuracy
        if probe_speed >= FAST_LINK_THRESHOLD:
            logger.info(
                "Fast link detected (%.1f MB/s) — running full 128 MB test",
                probe_mbps,
            )
            try:
                full_speed = _write_sample(backend, FULL_SAMPLE_SIZE)
                if full_speed > 0:
                    logger.info(
                        "Bandwidth test: %.2f MB/s (128 MB sample, end-to-end)",
                        full_speed / (1024 * 1024),
                    )
                    return full_speed
            except Exception as exc:
                logger.warning("Full bandwidth test failed: %s", exc)
                # Fall back to probe result

        # Slow link or full test failed: use probe result
        logger.info(
            "Using probe result: %.2f MB/s (slow link, 16 MB probe)",
            probe_mbps,
        )
        return probe_speed

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
    """Upload a sample file and return real write speed.

    For SFTP backends, forces a remote ``sync`` after the upload to
    ensure all data is flushed to disk before stopping the timer.
    This measures true end-to-end throughput, not just local buffer
    fill speed.

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

        # Force remote flush — ensures we measure real throughput,
        # not just how fast we can fill the local TCP/SSH buffer.
        _remote_sync(backend)
    finally:
        _cleanup(backend, temp_name)

    elapsed = time.monotonic() - start
    if elapsed <= 0:
        return 0.0
    return size / elapsed


def _remote_sync(backend: StorageBackend) -> None:
    """Force remote data flush for accurate bandwidth measurement.

    For SFTP backends, runs ``sync`` via the exec channel to ensure
    all buffered data is written to disk.  For other backends (S3,
    local), the upload call itself is synchronous so no extra action
    is needed.

    Args:
        backend: Storage backend that was just written to.
    """
    # Only SFTP needs an explicit sync — other backends are synchronous
    if not hasattr(backend, "_get_transport"):
        return

    try:
        transport = backend._get_transport()
        channel = transport.open_session()
        try:
            channel.settimeout(30)
            channel.exec_command("sync")  # nosec B601
            channel.recv_exit_status()
        finally:
            channel.close()
    except Exception as exc:
        logger.debug("Remote sync failed (non-critical): %s", exc)


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
