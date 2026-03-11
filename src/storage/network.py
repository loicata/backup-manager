import logging
import shutil
from pathlib import Path
from typing import Optional

from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class NetworkStorage(StorageBackend):
    """Storage backend for UNC network paths (\\\\server\\share)."""

    def upload(self, local_path: Path, remote_name: str) -> bool:
        unc_path = Path(self.config.destination_path)
        target = unc_path / remote_name
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            if local_path.is_dir():
                if target.exists():
                    shutil.rmtree(target)
                shutil.copytree(local_path, target)
            else:
                self._throttled_copy(local_path, target)
            logger.info(f"Uploaded to network: {target}")
            return True
        except OSError as e:
            logger.error(f"Network upload failed: {e}")
            return False

    def list_backups(self) -> list[dict]:
        unc_path = Path(self.config.destination_path)
        if not unc_path.exists():
            return []

        backups = []
        try:
            for item in sorted(unc_path.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
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
        except OSError as e:
            logger.error(f"Cannot list network backups: {e}")
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
            logger.error(f"Network delete failed: {e}")
            return False

    def test_connection(self) -> tuple[bool, str]:
        unc_path = Path(self.config.destination_path)
        try:
            if not unc_path.exists():
                return False, f"❌ Network path unreachable: {unc_path}"
            test_file = unc_path / ".backupmanager_test"
            test_file.write_text("test")
            test_file.unlink()
            free = self.get_free_space()
            space_info = f" — {self.format_size(free)} available" if free else ""
            return True, f"✅ Network path OK: {unc_path}{space_info}"
        except OSError as e:
            return False, f"❌ Network error: {e}"

    def get_free_space(self) -> Optional[int]:
        unc_path = Path(self.config.destination_path)
        try:
            if not unc_path.exists():
                return None
            usage = shutil.disk_usage(unc_path)
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
