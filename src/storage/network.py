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


def _extract_host(unc_path: str) -> str:
    r"""Return just the server component from a UNC path.

    Args:
        unc_path: UNC path, e.g. ``\\server\share`` or ``\\192.168.1.1\backups``.

    Returns:
        The server / host portion, e.g. ``server`` or ``192.168.1.1``.
    """
    cleaned = unc_path.replace("/", "\\").lstrip("\\")
    return cleaned.split("\\", 1)[0]


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
        r"""Mount the network share via ``net use``, auth'd through Credential Manager.

        Two-step authentication to dodge the classic pipe-stdin race:

        1. ``cmdkey /add:<host> /user: /pass:`` stores the credential
           scoped to this specific host in the current user's Windows
           Credential Manager vault.
        2. ``net use \\<host>\<share> /persistent:no`` consumes the
           cached credential silently — no prompt, no stdin timing
           issues, no timeout when Samba rejects the first packet and
           Windows wants to retry.
        3. ``cmdkey /delete:<host>`` drops the cached credential so it
           does not linger between runs.

        Security compared to the previous ``net use path * /user:... +
        stdin pipe`` approach:
        - Password is still NEVER on the ``net use`` argv line, so the
          share mount event (4688) and any tasklist snapshot around
          the share usage stay clean.
        - Password IS briefly visible on the ``cmdkey`` argv line for
          the millisecond of the ``cmdkey /add`` invocation. That is a
          narrower window than the old inline ``net use`` approach
          (which held the password in argv for the entire mount
          duration — seconds on a slow LAN) and the credential is
          immediately torn down after use.

        Returns:
            Tuple ``(success, message)``.
        """
        if not self._username or not self._password:
            return False, "Username and password are required for network storage"

        share_root = _extract_share_root(str(self._dest))
        host = _extract_host(share_root)

        # --- Step 1: stash the credential in Credential Manager ---
        cmdkey_add = [
            "cmdkey",
            f"/add:{host}",
            f"/user:{self._username}",
            f"/pass:{self._password}",
        ]
        try:
            add_result = subprocess.run(
                cmdkey_add,
                capture_output=True,
                text=True,
                timeout=5,
                creationflags=_CREATE_NO_WINDOW,
            )
        except subprocess.TimeoutExpired:
            return False, "cmdkey /add timed out"
        except FileNotFoundError:
            return False, "cmdkey command not available — not a Windows system?"

        if add_result.returncode != 0:
            detail = (add_result.stderr or add_result.stdout).strip()
            return False, f"Could not cache credentials: {detail or 'unknown error'}"

        # --- Step 2: ``net use`` consumes the cached credential ---
        use_cmd = ["net", "use", share_root, "/persistent:no"]
        try:
            result = subprocess.run(
                use_cmd,
                capture_output=True,
                text=True,
                timeout=NET_USE_TIMEOUT,
                creationflags=_CREATE_NO_WINDOW,
            )
        except subprocess.TimeoutExpired:
            logger.error("net use timed out after %ds for %s", NET_USE_TIMEOUT, share_root)
            self._cmdkey_delete(host)
            return False, f"Connection timeout after {NET_USE_TIMEOUT}s"
        except FileNotFoundError:
            self._cmdkey_delete(host)
            logger.error("net use command not found — not a Windows system?")
            return False, "net use command not available"

        # --- Step 3: always drop the cached credential ---
        self._cmdkey_delete(host)

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

    @staticmethod
    def _cmdkey_delete(host: str) -> None:
        """Best-effort removal of a cached credential for ``host``.

        Called after every connection attempt so the password never
        lingers in the Windows Credential Manager between backup runs.
        Swallows every failure — we cannot do better than log and
        carry on, and a surviving cached credential is benign.
        """
        try:
            subprocess.run(
                ["cmdkey", f"/delete:{host}"],
                capture_output=True,
                text=True,
                timeout=5,
                creationflags=_CREATE_NO_WINDOW,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as exc:
            logger.debug("cmdkey /delete best-effort failed for %s: %s", host, exc)

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

    def list_backups(self) -> list[dict]:
        """List backups after mounting the share.

        Skips the recursive size walk that LocalStorage.list_backups
        performs — that walk does one ``stat()`` per file, which on an
        SMB share costs a round-trip each and turns a 37 k-file listing
        into a 1-minute hang. Directories report ``size=0`` in the UI
        until the user actually restores, at which point the download
        counter shows real progress.
        """
        import os
        import stat as stat_module

        from src.storage.local import SYSTEM_FOLDERS

        ok, msg = self._connect()
        if not ok:
            raise OSError(f"Cannot connect to network share: {msg}")

        if not self._dest.exists():
            return []

        backups = []
        try:
            entries = list(os.scandir(self._dest))
        except OSError:
            return []

        for entry in entries:
            if entry.name.startswith((".", "$")):
                continue
            if entry.name.endswith((".wbverify", ".partial")):
                continue
            if entry.name in SYSTEM_FOLDERS:
                continue
            try:
                st = entry.stat()
            except OSError:
                continue
            is_dir = stat_module.S_ISDIR(st.st_mode)
            # Directories: advertise -1 as "unknown — compute async".
            # The UI renders that as a placeholder and launches a
            # background size walk that updates the row when done.
            size = -1 if is_dir else st.st_size
            backups.append(
                {
                    "name": entry.name,
                    "size": size,
                    "modified": st.st_mtime,
                    "is_dir": is_dir,
                }
            )
        return sorted(backups, key=lambda b: b["modified"], reverse=True)

    def compute_dir_size(self, name: str) -> int:
        """Recursively compute the byte count of a backup directory.

        Expensive on a network share (one ``stat`` round-trip per file),
        which is exactly why ``list_backups`` skips it and returns -1.
        Callers run this in a background thread and update the UI row
        when the number lands.
        """
        target = self._dest / name
        if not target.exists() or not target.is_dir():
            return 0
        total = 0
        try:
            for f in target.rglob("*"):
                try:
                    if f.is_file():
                        total += f.stat().st_size
                except OSError:
                    continue
        except OSError:
            return total
        return total

    def download_backup(self, remote_name, local_dir, progress_callback=None):
        """Download (copy) a backup from the mounted share to ``local_dir``."""
        ok, msg = self._connect()
        if not ok:
            raise OSError(f"Cannot connect to network share: {msg}")
        # LocalStorage.download_backup ignores progress_callback so we
        # accept and drop it for signature parity with the SFTP/S3
        # callers — the recovery tab never inspects the return value.
        return super().download_backup(remote_name, local_dir)
