"""Background update checker.

Fetches version info from a remote URL and notifies
if a newer version is available.  Optionally verifies
a SHA-256 hash of the downloaded update payload.
"""

import hashlib
import json
import logging
import re
import threading
import urllib.request
from typing import Callable, Optional

logger = logging.getLogger(__name__)

UPDATE_URL = "https://example.com/backup-manager/version.json"
CHECK_TIMEOUT = 10  # seconds
MAX_RESPONSE_SIZE = 16 * 1024  # 16 KB


def check_for_update(
    current_version: str,
    callback: Callable[[str, str], None],
    url: str = UPDATE_URL,
) -> threading.Thread:
    """Check for updates in a background thread.

    Args:
        current_version: Current version string (e.g., "3.0").
        callback: Called with (latest_version, download_url) if update available.
        url: URL to fetch version info from.

    Returns:
        The background thread.
    """

    def _check():
        try:
            # Reject non-HTTPS check URLs
            if not url.startswith("https://"):
                logger.warning("Update check URL is not HTTPS: %s", url)
                return

            req = urllib.request.Request(url, method="GET")
            req.add_header("User-Agent", f"BackupManager/{current_version}")

            with urllib.request.urlopen(req, timeout=CHECK_TIMEOUT) as resp:
                data = resp.read(MAX_RESPONSE_SIZE)
                info = json.loads(data.decode("utf-8"))

            latest = info.get("latest", "")
            download_url = info.get("url", "")

            if not latest or not download_url:
                return

            # Validate download URL
            if not download_url.startswith("https://"):
                logger.warning("Update URL is not HTTPS: %s", download_url)
                return

            if _version_tuple(latest) > _version_tuple(current_version):
                logger.info("Update available: %s -> %s", current_version, latest)
                callback(latest, download_url)
            else:
                logger.debug("No update available (current=%s, latest=%s)", current_version, latest)

        except Exception as e:
            logger.debug("Update check failed: %s", e)

    thread = threading.Thread(target=_check, daemon=True, name="UpdateChecker")
    thread.start()
    return thread


def verify_update_hash(file_data: bytes, expected_hash: Optional[str]) -> bool:
    """Verify the SHA-256 hash of downloaded update data.

    Args:
        file_data: Raw bytes of the downloaded update file.
        expected_hash: Expected lowercase hex SHA-256 digest from
            the version.json ``sha256`` field.  If None or empty,
            verification is skipped with a warning.

    Returns:
        True if the hash matches or is absent (backward compat).
        False if the hash is present but does not match.
    """
    if not expected_hash:
        logger.warning(
            "No SHA-256 hash provided in version info — "
            "skipping integrity check (backward compatibility)"
        )
        return True

    actual_hash = hashlib.sha256(file_data).hexdigest()
    if actual_hash != expected_hash.lower():
        logger.error(
            "SHA-256 mismatch: expected %s, got %s",
            expected_hash,
            actual_hash,
        )
        return False

    logger.info("SHA-256 hash verified: %s", actual_hash)
    return True


def _version_tuple(version: str) -> tuple:
    """Convert version string to comparable tuple.

    Args:
        version: Version string like "3.1.2".

    Returns:
        Tuple of integers, e.g. (3, 1, 2).
    """
    parts = re.findall(r"\d+", version)
    return tuple(int(p) for p in parts)
