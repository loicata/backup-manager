import logging
import shutil
from pathlib import Path
from typing import Optional

from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class LocalStorage(StorageBackend):
    """Storage backend for local and external drives."""

    def upload(self, local_path: Path, remote_name: str) -> bool:
        dest = Path(self.config.destination_path)
        dest.mkdir(parents=True, exist_ok=True)

        target = dest / remote_name
        try:
            if local_path.is_dir():
                if target.exists():
                    shutil.rmtree(target)
                shutil.copytree(local_path, target)
            else:
                self._throttled_copy(local_path, target)
            logger.info(f"Uploaded to local: {target}")
            return True
        except OSError as e:
            logger.error(f"Local upload failed: {e}")
            return False

    def list_backups(self) -> list[dict]:
        dest = Path(self.config.destination_path)
        if not dest.exists():
            return []

        backups = []
        for item in sorted(dest.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
            stat = item.stat()
            size = stat.st_size
            if item.is_dir():
                size = sum(f.stat().st_size for f in item.rglob("*") if f.is_file())
            backups.append({
                "name": item.name,
                "size": size,
                "modified": stat.st_mtime,
                "is_dir": item.is_dir(),
            })
        return backups

    def delete_backup(self, remote_name: str) -> bool:
        target = Path(self.config.destination_path) / remote_name
        try:
            if target.is_dir():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()
            return True
        except OSError as e:
            logger.error(f"Delete failed: {e}")
            return False

    def test_connection(self) -> tuple[bool, str]:
        dest = Path(self.config.destination_path)
        try:
            dest.mkdir(parents=True, exist_ok=True)
            test_file = dest / ".backupmanager_test"
            test_file.write_text("test")
            test_file.unlink()
            free = self.get_free_space()
            space_info = f" — {self.format_size(free)} available" if free else ""
            return True, f"✅ Access OK: {dest}{space_info}"
        except OSError as e:
            return False, f"❌ Access error: {e}"

    def get_free_space(self) -> Optional[int]:
        dest = Path(self.config.destination_path)
        try:
            dest.mkdir(parents=True, exist_ok=True)
            usage = shutil.disk_usage(dest)
            return usage.free
        except OSError:
            return None

    def get_file_size(self, remote_name: str) -> Optional[int]:
        target = Path(self.config.destination_path) / remote_name
        try:
            if target.exists():
                return target.stat().st_size
        except OSError:
            pass
        return None
