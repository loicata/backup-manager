"""Background update checker for Backup Manager.

Fetches version information from a remote URL and notifies
the caller when a newer version is available.
"""

import json
import logging
import threading
import urllib.request

logger = logging.getLogger(__name__)

UPDATE_URL = "https://example.com/backup-manager/version.json"
REQUEST_TIMEOUT = 10


def _parse_version(version_str: str) -> tuple:
    """Convert a version string like '1.2.3' into a comparable tuple of ints."""
    return tuple(int(part) for part in version_str.strip().split("."))


def _fetch_and_compare(current_version: str, callback, url: str = UPDATE_URL):
    """Fetch remote version info and invoke *callback* if an update exists.

    Parameters
    ----------
    current_version : str
        The running application version (e.g. ``"1.0.0"``).
    callback : callable
        Called as ``callback(latest_version, download_url)`` when the remote
        version is newer than *current_version*.
    url : str
        URL that returns JSON with ``{"latest": "X.Y.Z", "url": "..."}``.
    """
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        latest_version = data["latest"]
        download_url = data.get("url", "")

        if _parse_version(latest_version) > _parse_version(current_version):
            callback(latest_version, download_url)
        else:
            logger.debug("Application is up to date (%s).", current_version)
    except Exception:
        logger.debug("Update check failed.", exc_info=True)


def check_for_update(current_version: str, callback, url: str = UPDATE_URL):
    """Launch a background daemon thread that checks for updates.

    Parameters
    ----------
    current_version : str
        The running application version.
    callback : callable
        ``callback(latest_version, download_url)`` – called from the
        background thread when a newer version is found.
    url : str, optional
        Override the default update-check URL.
    """
    thread = threading.Thread(
        target=_fetch_and_compare,
        args=(current_version, callback, url),
        daemon=True,
    )
    thread.start()
    return thread


def start_update_check(root, current_version: str, status_label=None, url: str = UPDATE_URL):
    """Convenience wrapper for Tkinter applications.

    Parameters
    ----------
    root : tk.Tk | tk.Toplevel
        The Tkinter root window (used for ``root.after``).
    current_version : str
        The running application version.
    status_label : tk.Label | None
        If provided, its text is updated when a new version is found.
    url : str, optional
        Override the default update-check URL.
    """

    def _on_update_available(latest_version: str, download_url: str):
        msg = f"Update available: v{latest_version}"
        logger.info(msg)
        if status_label is not None:
            root.after(0, lambda: status_label.config(text=msg))

    return check_for_update(current_version, _on_update_available, url=url)
