r"""Network storage backend for UNC paths (\\server\share).

Extends LocalStorage with optional SMB authentication via ``net use``
and connection timeout handling.
"""

import logging
import os
import subprocess
import threading
from pathlib import Path

from src.storage.local import LocalStorage

# Hide console window on Windows when running net use
_CREATE_NO_WINDOW = 0x08000000 if os.name == "nt" else 0

logger = logging.getLogger(__name__)

CONNECTION_TIMEOUT = 15  # seconds
NET_USE_TIMEOUT = 15  # seconds


def _extract_share_root(unc_path: str) -> str:
    r"""Return the \\server\share portion from a longer UNC path.

    Args:
        unc_path: Full UNC path, e.g. ``\\server\share\sub\folder``.

    Returns:
        The first two path components, e.g. ``\\server\share``.

    Raises:
        ValueError: If *unc_path* is not a valid UNC path.
    """
    cleaned = unc_path.replace("/", "\\").rstrip("\\")
    if not cleaned.startswith("\\\\"):
        raise ValueError(f"Not a UNC path: {unc_path!r}")

    parts = cleaned.lstrip("\\").split("\\")
    if len(parts) < 2:
        raise ValueError(f"Incomplete UNC path (need \\\\server\\share): {unc_path!r}")

    return f"\\\\{parts[0]}\\{parts[1]}"


class NetworkStorage(LocalStorage):
    """Storage backend for network UNC paths with optional SMB credentials."""

    def __init__(
        self,
        destination_path: str,
        username: str = "",
        password: str = "",
    ):
        """Initialise the network storage backend.

        Args:
            destination_path: UNC path to the backup destination.
            username: Optional SMB username (``DOMAIN\\user`` or ``user``).
            password: Optional SMB password.
        """
        super().__init__(destination_path)
        self._username = username or ""
        self._password = password or ""
        self._connected = False

    # ------------------------------------------------------------------
    # SMB connection helpers
    # ------------------------------------------------------------------

    def _connect(self) -> tuple[bool, str]:
        r"""Mount the network share via ``net use``.

        Returns:
            Tuple ``(success, message)``.
        """
        if not self._username or not self._password:
            return False, "Username and password are required for network storage"

        share_root = _extract_share_root(str(self._dest))

        cmd = [
            "net",
            "use",
            share_root,
            f"/user:{self._username}",
            self._password,
            "/persistent:no",
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=NET_USE_TIMEOUT,
                creationflags=_CREATE_NO_WINDOW,
            )
        except subprocess.TimeoutExpired:
            logger.error("net use timed out after %ds for %s", NET_USE_TIMEOUT, share_root)
            return False, f"Connection timeout after {NET_USE_TIMEOUT}s"
        except FileNotFoundError:
            logger.error("net use command not found — not a Windows system?")
            return False, "net use command not available"

        stdout = result.stdout.strip()
        stderr = result.stderr.strip()

        if result.returncode == 0:
            self._connected = True
            logger.info("Mounted network share %s", share_root)
            return True, "Connected"

        # Error 1219: "Multiple connections to a server … not allowed"
        # This means the share is already accessible — treat as success.
        if "1219" in stderr or "1219" in stdout:
            logger.info("Share %s already connected (error 1219) — reusing", share_root)
            self._connected = False  # we did not create this connection
            return True, "Already connected"

        msg = stderr or stdout or f"net use failed (exit {result.returncode})"
        logger.error("Failed to mount %s: %s", share_root, msg)
        return False, msg

    def _disconnect(self) -> None:
        r"""Unmount the network share if we mounted it ourselves."""
        if not self._connected:
            return

        try:
            share_root = _extract_share_root(str(self._dest))
        except ValueError:
            return

        try:
            subprocess.run(
                ["net", "use", share_root, "/delete", "/y"],
                capture_output=True,
                text=True,
                timeout=NET_USE_TIMEOUT,
                creationflags=_CREATE_NO_WINDOW,
            )
            logger.info("Disconnected network share %s", share_root)
        except Exception as exc:
            logger.warning("Failed to disconnect %s: %s", share_root, exc)
        finally:
            self._connected = False

    def close(self) -> None:
        """Release the network share connection."""
        self._disconnect()

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

    def test_connection(self) -> tuple[bool, str]:
        """Test network path with optional authentication and timeout."""
        # Attempt SMB authentication first
        ok, msg = self._connect()
        if not ok:
            return False, msg

        result = [False, "Connection timeout"]

        def _test():
            try:
                dest = Path(self._dest)
                if not dest.exists():
                    result[0] = False
                    result[1] = f"Network path not found: {self._dest}"
                    return

                test_file = dest / ".backup_manager_test"
                test_file.write_text("test", encoding="utf-8")
                test_file.unlink()

                free = self.get_free_space()
                if free is not None:
                    free_gb = free / (1024**3)
                    result[0] = True
                    result[1] = f"Connected — {free_gb:.1f} GB free"
                else:
                    result[0] = True
                    result[1] = "Connected"
            except PermissionError:
                result[0] = False
                result[1] = f"Permission denied: {self._dest}"
            except Exception as e:
                result[0] = False
                result[1] = f"Error: {e}"

        thread = threading.Thread(target=_test, daemon=True)
        thread.start()
        thread.join(timeout=CONNECTION_TIMEOUT)

        if thread.is_alive():
            return False, f"Connection timeout after {CONNECTION_TIMEOUT}s"

        return result[0], result[1]

    def upload(self, local_path, remote_name=None, progress_callback=None, cancel_check=None):
        """Upload with SMB authentication."""
        ok, msg = self._connect()
        if not ok:
            raise OSError(f"Cannot connect to network share: {msg}")
        return super().upload(local_path, remote_name, progress_callback, cancel_check)

    def upload_file(self, fileobj, remote_path, size=0, progress_callback=None):
        """Upload single file with SMB authentication."""
        ok, msg = self._connect()
        if not ok:
            raise OSError(f"Cannot connect to network share: {msg}")
        return super().upload_file(fileobj, remote_path, size, progress_callback)
