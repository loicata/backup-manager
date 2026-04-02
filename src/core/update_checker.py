"""Background update checker using GitHub Releases API.

Fetches the latest release tag from the GitHub repository
and notifies if a newer version is available.
"""

import json
import logging
import re
import threading
import urllib.request
from collections.abc import Callable

logger = logging.getLogger(__name__)

GITHUB_REPO = "loicata/backup-manager"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
CHECK_TIMEOUT = 10  # seconds
MAX_RESPONSE_SIZE = 64 * 1024  # 64 KB


def check_for_update(
    current_version: str,
    callback: Callable[[str, str], None],
    url: str = GITHUB_API_URL,
) -> threading.Thread:
    """Check for updates in a background thread.

    Queries the GitHub Releases API for the latest release.
    If a newer version is found, calls the callback with the
    version string and the release page URL.

    Args:
        current_version: Current version string (e.g., "3.2.1").
        callback: Called with (latest_version, release_url) if
            an update is available.
        url: GitHub API URL to fetch release info from.

    Returns:
        The background thread (daemon, already started).
    """

    def _check() -> None:
        try:
            if not url.startswith("https://"):
                logger.warning("Update check URL is not HTTPS: %s", url)
                return

            req = urllib.request.Request(url, method="GET")
            req.add_header("User-Agent", f"BackupManager/{current_version}")
            req.add_header("Accept", "application/vnd.github+json")

            with urllib.request.urlopen(req, timeout=CHECK_TIMEOUT) as resp:
                data = resp.read(MAX_RESPONSE_SIZE)
                release = json.loads(data.decode("utf-8"))

            tag_name = release.get("tag_name", "")
            release_url = release.get("html_url", "")

            if not tag_name or not release_url:
                logger.debug("Incomplete release info from GitHub")
                return

            # Strip leading 'v' from tag (e.g., "v3.2.1" -> "3.2.1")
            latest = tag_name.lstrip("v")

            if _version_tuple(latest) > _version_tuple(current_version):
                logger.info("Update available: %s -> %s", current_version, latest)
                callback(latest, release_url)
            else:
                logger.debug(
                    "No update available (current=%s, latest=%s)",
                    current_version,
                    latest,
                )

        except Exception as e:
            logger.debug("Update check failed: %s", e)

    thread = threading.Thread(target=_check, daemon=True, name="UpdateChecker")
    thread.start()
    return thread


def _version_tuple(version: str) -> tuple[int, ...]:
    """Convert version string to comparable tuple.

    Args:
        version: Version string like "3.1.2".

    Returns:
        Tuple of integers, e.g. (3, 1, 2).
    """
    parts = re.findall(r"\d+", version)
    return tuple(int(p) for p in parts)
