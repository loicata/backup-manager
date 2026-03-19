"""Local storage backend for external drives and USB sticks.

Supports flat directory copy and file-by-file streaming.
"""

import logging
import os
import shutil
import threading
from pathlib import Path
from typing import BinaryIO, Optional

from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)

CONNECTION_TIMEOUT = 10  # seconds


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
                shutil.rmtree(target)
            shutil.copytree(local_path, target)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            if self._bandwidth_limit_kbps > 0:
                self._throttled_copy(local_path, target)
            else:
                shutil.copy2(local_path, target)

        logger.info("Uploaded %s -> %s", local_path.name, target)

    def upload_file(self, fileobj: BinaryIO, remote_path: str, size: int = 0) -> None:
        """Write a file-like object to the destination."""
        target = self._dest / remote_path
        target.parent.mkdir(parents=True, exist_ok=True)

        reader = self._get_throttled_reader(fileobj)
        bytes_written = 0
        chunk_size = 1024 * 1024  # 1 MB

        with open(target, "wb") as out:
            while True:
                chunk = reader.read(chunk_size)
                if not chunk:
                    break
                out.write(chunk)
                bytes_written += len(chunk)
                if self._progress_callback and size > 0:
                    self._progress_callback(bytes_written, size)

    def list_backups(self) -> list[dict]:
        """List backups in the destination directory."""
        if not self._dest.exists():
            return []

        backups = []
        for entry in self._dest.iterdir():
            if entry.name.startswith("."):
                continue
            if entry.suffix == ".wbverify":
                continue
            stat = entry.stat()
            if entry.is_dir():
                total_size = sum(
                    f.stat().st_size for f in entry.rglob("*") if f.is_file()
                )
            else:
                total_size = stat.st_size

            backups.append({
                "name": entry.name,
                "size": total_size,
                "modified": stat.st_mtime,
                "is_dir": entry.is_dir(),
            })

        return sorted(backups, key=lambda b: b["modified"], reverse=True)

    def delete_backup(self, remote_name: str) -> None:
        """Delete a backup from the destination."""
        target = self._dest / remote_name
        if target.is_dir():
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()
        else:
            raise FileNotFoundError(f"Backup not found: {remote_name}")
        logger.info("Deleted backup: %s", remote_name)

    def test_connection(self) -> tuple[bool, str]:
        """Check if the destination is accessible and writable."""
        result: list = [False, "Connection timeout"]

        def _test() -> None:
            try:
                if not self._dest.exists():
                    result[0] = False
                    result[1] = f"Path does not exist: {self._dest}"
                    return

                test_file = self._dest / ".backup_manager_test"
                test_file.write_text("test", encoding="utf-8")
                test_file.unlink()

                free = self.get_free_space()
                if free is not None:
                    free_gb = free / (1024 ** 3)
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

    def get_free_space(self) -> Optional[int]:
        """Get available disk space in bytes."""
        try:
            usage = shutil.disk_usage(self._dest)
            return usage.free
        except Exception:
            return None

    def get_file_size(self, remote_name: str) -> Optional[int]:
        """Get size of a backup file or directory."""
        target = self._dest / remote_name
        if not target.exists():
            return None
        if target.is_dir():
            return sum(f.stat().st_size for f in target.rglob("*") if f.is_file())
        return target.stat().st_size

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
