"""Local storage backend for external drives and USB sticks.

Supports flat directory copy and file-by-file streaming.
"""

import contextlib
import logging
import os
import shutil
import stat
import threading
from pathlib import Path
from typing import BinaryIO

from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)

# 30s: cumulative wake-up budget (~16s of sleep retries) + antivirus
# scan on the first write on a freshly mounted volume. 20s was enough
# for drives that spun up within ~8s but still tripped on USB SSDs in
# deep power-save that need 10-12s to enumerate.
CONNECTION_TIMEOUT = 30  # seconds

# Windows system folders that must never be treated as backups
SYSTEM_FOLDERS = frozenset(
    {
        "System Volume Information",
        "$RECYCLE.BIN",
        "RECYCLER",
        "Recovery",
        "found.000",
    }
)


def _force_remove_readonly(func, path: str, exc_info) -> None:
    """Handle read-only files during shutil.rmtree.

    On Windows, files like .scr or system-protected files may have the
    read-only attribute set, causing PermissionError on deletion.
    This callback clears the read-only flag and retries.

    Args:
        func: The function that raised the exception (os.remove, etc.).
        path: The path that caused the error.
        exc_info: Exception info tuple (type, value, traceback).
    """
    if isinstance(exc_info[1], PermissionError):
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
        except Exception as retry_err:
            logger.warning("Could not force-remove %s: %s", path, retry_err)
    else:
        logger.warning("rmtree error on %s: %s", path, exc_info[1])


class LocalStorage(StorageBackend):
    """Storage backend for local/external drives."""

    def __init__(self, destination_path: str):
        super().__init__()
        self._dest = Path(destination_path)

    def upload(self, local_path: Path, remote_name: str) -> None:
        """Copy a local file or directory to the destination."""
        target = self._dest / remote_name

        if local_path.is_dir():
            if target.exists():
                # Use the same read-only-forcing onerror handler as
                # delete_backup; a previous backup with read-only
                # attributes would otherwise raise PermissionError
                # and abort the new upload.
                shutil.rmtree(target, onerror=_force_remove_readonly)
            shutil.copytree(local_path, target)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            if self._bandwidth_limit_kbps > 0:
                self._throttled_copy(local_path, target)
            else:
                shutil.copy2(local_path, target)

        logger.info("Uploaded %s -> %s", local_path.name, target)

    def upload_file(self, fileobj: BinaryIO, remote_path: str, size: int = 0) -> None:
        """Write a file-like object to the destination.

        Writes to ``<target>.partial`` first and atomically renames on
        success. Without this, a crash mid-write leaves a corrupted
        file with the final name that ``list_backups`` surfaces as a
        valid archive.
        """
        target = self._dest / remote_path
        target.parent.mkdir(parents=True, exist_ok=True)

        partial = target.with_suffix(target.suffix + ".partial")

        reader = self._get_throttled_reader(fileobj)
        bytes_written = 0
        chunk_size = 1024 * 1024  # 1 MB

        try:
            with open(partial, "wb") as out:
                while True:
                    chunk = reader.read(chunk_size)
                    if not chunk:
                        break
                    out.write(chunk)
                    bytes_written += len(chunk)
                    if self._progress_callback and size > 0:
                        self._progress_callback(bytes_written, size)
            # Atomic rename — the final name only appears on success.
            os.replace(partial, target)
        except BaseException:
            # Best-effort cleanup of the partial file on any failure
            # (exception, cancel). Swallow errors here so we don't
            # mask the original exception.
            with contextlib.suppress(OSError):
                partial.unlink(missing_ok=True)
            raise

    def list_backups(self) -> list[dict]:
        """List backups in the destination directory."""
        if not self._dest.exists():
            return []

        backups = []
        for entry in self._dest.iterdir():
            if entry.name.startswith("."):
                continue
            if entry.name.startswith("$"):
                continue
            if entry.suffix == ".wbverify":
                continue
            if entry.name.endswith(".partial"):
                # Leftover from an interrupted encrypted-tar write;
                # never expose it as a usable backup.
                continue
            if entry.name in SYSTEM_FOLDERS:
                continue
            stat = entry.stat()
            if entry.is_dir():
                total_size = sum(f.stat().st_size for f in entry.rglob("*") if f.is_file())
            else:
                total_size = stat.st_size

            backups.append(
                {
                    "name": entry.name,
                    "size": total_size,
                    "modified": stat.st_mtime,
                    "is_dir": entry.is_dir(),
                }
            )

        return sorted(backups, key=lambda b: b["modified"], reverse=True)

    def delete_backup(self, remote_name: str) -> None:
        """Delete a backup and its associated .wbverify manifest."""
        target = self._dest / remote_name
        if target.is_dir():
            shutil.rmtree(target, onerror=_force_remove_readonly)
        elif target.exists():
            target.unlink()
        else:
            raise FileNotFoundError(f"Backup not found: {remote_name}")
        logger.info("Deleted backup: %s", remote_name)

        # Remove associated integrity manifest if present
        verify_file = self._dest / f"{remote_name}.wbverify"
        if verify_file.exists():
            verify_file.unlink()
            logger.info("Deleted manifest: %s.wbverify", remote_name)

    def test_connection(self) -> tuple[bool, str]:
        """Check if the destination is accessible and writable.

        Designed to be tolerant of USB drives in power-save:
        - Retries ``exists()`` a few times with back-off to let the
          drive spin up.
        - Pokes the drive root (``listdir``) between retries to force
          Windows to mount a volume it has put to sleep.
        - Separates "drive missing" from "permission denied" from
          "write failed" so the user sees an actionable message rather
          than a generic "Destinations unavailable".
        """
        result: list = [False, "Connection timeout"]

        def _wait_for_drive_online() -> bool:
            """Return True as soon as ``self._dest`` can be stat'd."""
            import os as _os
            import time as _time

            # Cheap initial check — responsive drives return instantly.
            if self._dest.exists():
                return True
            # Drive letter root — listing it triggers Windows to bring
            # the volume back online from power-save.
            root = None
            s = str(self._dest)
            if len(s) >= 2 and s[1] == ":":
                root = f"{s[0]}:\\"
            # Cumulative sleep budget ~15.8s. External USB drives in
            # deep power-save can need 10-12s to fully enumerate on the
            # first probe after reconnection; the 8.0 s tail covers that
            # long tail without penalising healthy drives (which return
            # on the first ``exists()`` check above).
            for attempt, delay in enumerate((0.3, 0.5, 1.0, 2.0, 4.0, 8.0)):
                _time.sleep(delay)
                if root and attempt == 1:
                    with contextlib.suppress(OSError):
                        _os.listdir(root)  # Wake the volume
                if self._dest.exists():
                    return True
            return False

        def _test() -> None:
            try:
                if not _wait_for_drive_online():
                    result[0] = False
                    result[1] = (
                        f"Drive not ready after wake-up retries: {self._dest}. "
                        f"Reconnect the drive or wait a few seconds and retry."
                    )
                    return

                test_file = self._dest / ".backup_manager_test"
                try:
                    test_file.write_text("test", encoding="utf-8")
                    test_file.unlink()
                except PermissionError as pe:
                    result[0] = False
                    result[1] = (
                        f"Destination is read-only or locked "
                        f"(permission denied on {self._dest}): {pe}"
                    )
                    return
                except OSError as we:
                    result[0] = False
                    result[1] = f"Destination present but write failed " f"({self._dest}): {we}"
                    return

                free = self.get_free_space()
                if free is not None:
                    free_gb = free / (1024**3)
                    result[0] = True
                    result[1] = f"Connected — {free_gb:.1f} GB free"
                else:
                    result[0] = True
                    result[1] = "Connected"
            except Exception as e:
                result[0] = False
                result[1] = f"Unexpected error on {self._dest}: {type(e).__name__}: {e}"

        thread = threading.Thread(target=_test, daemon=True)
        thread.start()
        thread.join(timeout=CONNECTION_TIMEOUT)

        if thread.is_alive():
            return False, (
                f"Connection test timed out after {CONNECTION_TIMEOUT}s. "
                f"The drive may be very slow or unresponsive; try unplugging "
                f"and reconnecting it."
            )

        return result[0], result[1]

    def get_free_space(self) -> int | None:
        """Get available disk space in bytes.

        Returns None when the destination is unreachable (drive
        unplugged, permission denied, etc.) and logs the reason so
        callers don't silently assume "unlimited space" and bypass
        rotation cleanup.
        """
        try:
            usage = shutil.disk_usage(self._dest)
            return usage.free
        except FileNotFoundError:
            logger.warning("get_free_space: destination missing: %s", self._dest)
            return None
        except PermissionError as e:
            logger.warning("get_free_space: permission denied on %s: %s", self._dest, e)
            return None
        except OSError as e:
            logger.warning("get_free_space: OS error on %s: %s", self._dest, e)
            return None

    def get_file_size(self, remote_name: str) -> int | None:
        """Get size of a backup file or directory."""
        target = self._dest / remote_name
        if not target.exists():
            return None
        if target.is_dir():
            return sum(f.stat().st_size for f in target.rglob("*") if f.is_file())
        return target.stat().st_size

    def download_backup(self, remote_name: str, local_dir: Path) -> Path:
        """Download (copy) a local backup to another local directory."""
        src = self._dest / remote_name
        dst = local_dir / remote_name
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
        return dst

    def _throttled_copy(self, src: Path, dst: Path) -> None:
        """Copy file with bandwidth throttling."""
        with open(src, "rb") as f_in:
            reader = self._get_throttled_reader(f_in)
            with open(dst, "wb") as f_out:
                while True:
                    chunk = reader.read(1024 * 1024)
                    if not chunk:
                        break
                    f_out.write(chunk)
        # Preserve metadata
        shutil.copystat(src, dst)
