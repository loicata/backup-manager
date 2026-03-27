"""Proton Drive storage backend via rclone.

Uses rclone's protondrive backend with environment variable configuration
(no rclone.conf needed). Supports 2FA via pyotp TOTP.
"""

import json
import logging
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import BinaryIO

from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)

RCLONE_REMOTE_NAME = "backupmanager_proton"
_MIN_RCLONE_VERSION = (1, 62, 0)


class ProtonDriveStorage(StorageBackend):
    """Proton Drive storage via rclone."""

    def __init__(
        self,
        username: str = "",
        password: str = "",
        twofa_seed: str = "",
        remote_path: str = "/Backups",
        rclone_path: str = "",
    ):
        super().__init__()
        self._username = username
        self._password = password
        self._twofa_seed = twofa_seed
        self._remote_path = remote_path.strip("/")
        self._rclone_path = rclone_path or self._find_rclone()

    def _find_rclone(self) -> str:
        """Find rclone binary in PATH."""
        path = shutil.which("rclone")
        if path:
            return path
        # Common install locations
        for candidate in [
            r"C:\Program Files\rclone\rclone.exe",
            r"C:\rclone\rclone.exe",
            os.path.expanduser("~/rclone/rclone.exe"),
        ]:
            if os.path.isfile(candidate):
                return candidate
        return "rclone"  # Hope it's in PATH

    def _check_rclone_version(self) -> tuple[bool, str]:
        """Check if rclone is installed and meets minimum version."""
        try:
            result = subprocess.run(
                [self._rclone_path, "version"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            match = re.search(r"v(\d+)\.(\d+)\.(\d+)", result.stdout)
            if match:
                version = tuple(int(x) for x in match.groups())
                if version >= _MIN_RCLONE_VERSION:
                    return True, f"rclone {'.'.join(str(v) for v in version)}"
                return (
                    False,
                    f"rclone {'.'.join(str(v) for v in version)}"
                    f" too old (need {'.'.join(str(v) for v in _MIN_RCLONE_VERSION)}+)",
                )
            return False, "Could not determine rclone version"
        except FileNotFoundError:
            return False, "rclone not found"
        except Exception as e:
            return False, f"rclone check failed: {e}"

    def _build_env(self) -> dict:
        """Build environment with rclone config via env vars."""
        env = os.environ.copy()
        prefix = f"RCLONE_CONFIG_{RCLONE_REMOTE_NAME.upper()}"
        env[f"{prefix}_TYPE"] = "protondrive"
        env[f"{prefix}_USERNAME"] = self._username

        # Obscure password for rclone
        obscured = self._obscure_password(self._password)
        if obscured:
            env[f"{prefix}_PASSWORD"] = obscured

        if self._twofa_seed:
            try:
                import pyotp

                totp = pyotp.TOTP(self._twofa_seed)
                env[f"{prefix}_2FA"] = totp.now()
            except ImportError:
                logger.warning("pyotp not installed, 2FA not available")

        return env

    def _obscure_password(self, password: str) -> str:
        """Obscure password using rclone obscure command.

        Passes the password via stdin to avoid exposure in process
        arguments (visible via ps or Process Explorer).
        """
        try:
            result = subprocess.run(
                [self._rclone_path, "obscure", "-"],
                input=password,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return ""

    def _remote_spec(self, path: str = "") -> str:
        """Build rclone remote:path specification."""
        if path:
            return f"{RCLONE_REMOTE_NAME}:{self._remote_path}/{path}"
        return f"{RCLONE_REMOTE_NAME}:{self._remote_path}"

    def _run_rclone(self, args: list[str], timeout: int = 300) -> subprocess.CompletedProcess:
        """Run an rclone command."""
        cmd = [self._rclone_path] + args
        env = self._build_env()
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )

    def upload(self, local_path: Path, remote_name: str) -> None:
        """Upload via rclone copy."""
        if local_path.is_dir():
            args = ["copy", str(local_path), self._remote_spec(remote_name)]
        else:
            args = ["copyto", str(local_path), self._remote_spec(remote_name)]

        if self._bandwidth_limit_kbps > 0:
            args += ["--bwlimit", f"{self._bandwidth_limit_kbps}k"]

        result = self._run_rclone(args, timeout=3600)
        if result.returncode != 0:
            raise RuntimeError(f"rclone upload failed: {result.stderr.strip()}")
        logger.info("Uploaded %s to Proton Drive", local_path.name)

    def upload_file(self, fileobj: BinaryIO, remote_path: str, size: int = 0) -> None:
        """Stream upload via rclone rcat."""
        args = ["rcat", self._remote_spec(remote_path)]
        env = self._build_env()
        proc = subprocess.Popen(
            [self._rclone_path] + args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        try:
            reader = self._get_throttled_reader(fileobj)
            bytes_sent = 0
            while True:
                chunk = reader.read(1024 * 1024)
                if not chunk:
                    break
                proc.stdin.write(chunk)
                bytes_sent += len(chunk)
                if self._progress_callback and size > 0:
                    self._progress_callback(bytes_sent, size)
            proc.stdin.close()
            rc = proc.wait(timeout=300)
            if rc != 0:
                stderr = proc.stderr.read()
                raise RuntimeError(f"rclone rcat failed: {stderr}")
        except Exception:
            proc.kill()
            raise

    def list_backups(self) -> list[dict]:
        """List backups via rclone lsjson."""
        result = self._run_rclone(
            ["lsjson", self._remote_spec(), "--no-modtime"],
            timeout=60,
        )
        if result.returncode != 0:
            logger.warning("rclone lsjson failed: %s", result.stderr.strip())
            return []

        try:
            entries = json.loads(result.stdout)
        except json.JSONDecodeError:
            return []

        backups = []
        for entry in entries:
            backups.append(
                {
                    "name": entry.get("Name", ""),
                    "size": entry.get("Size", 0),
                    "modified": 0,
                    "is_dir": entry.get("IsDir", False),
                }
            )
        return backups

    def delete_backup(self, remote_name: str) -> None:
        """Delete via rclone purge (dir) or deletefile."""
        # Try deletefile first, then purge for directories
        result = self._run_rclone(["deletefile", self._remote_spec(remote_name)], timeout=60)
        if result.returncode != 0:
            result = self._run_rclone(["purge", self._remote_spec(remote_name)], timeout=120)
            if result.returncode != 0:
                raise RuntimeError(f"Delete failed: {result.stderr.strip()}")
        logger.info("Deleted from Proton Drive: %s", remote_name)

    def test_connection(self) -> tuple[bool, str]:
        """Test Proton Drive connection via rclone."""
        ok, msg = self._check_rclone_version()
        if not ok:
            return False, msg

        result = self._run_rclone(["lsd", self._remote_spec()], timeout=30)
        if result.returncode == 0:
            return True, f"Connected to Proton Drive ({self._username})"
        return False, f"Proton Drive error: {result.stderr.strip()}"

    def get_free_space(self) -> int | None:
        """Get free space (not available for Proton Drive)."""
        return None

    def list_backup_files(self, backup_name: str) -> list[tuple[str, int]]:
        """List files inside a backup directory on Proton Drive.

        Args:
            backup_name: Name of the backup directory.

        Returns:
            List of (relative_path, size_bytes) tuples.
        """
        result = self._run_rclone(
            ["lsjson", self._remote_spec(backup_name), "--recursive", "--files-only"],
            timeout=60,
        )
        if result.returncode != 0:
            logger.warning("rclone lsjson failed: %s", result.stderr.strip())
            return []

        try:
            entries = json.loads(result.stdout)
        except json.JSONDecodeError:
            return []

        return [(e.get("Path", ""), e.get("Size", 0)) for e in entries]

    def download_backup(self, remote_name: str, local_dir: Path) -> Path:
        """Download a backup from Proton Drive via rclone."""
        local_dir.mkdir(parents=True, exist_ok=True)
        dst = local_dir / remote_name
        dst.mkdir(parents=True, exist_ok=True)
        result = self._run_rclone(
            ["copy", self._remote_spec(remote_name), str(dst)],
            timeout=600,
        )
        if result.returncode != 0:
            raise RuntimeError(f"rclone download failed: {result.stderr.strip()}")
        return dst

    def get_file_size(self, remote_name: str) -> int | None:
        """Get file size via rclone size."""
        result = self._run_rclone(["size", self._remote_spec(remote_name), "--json"], timeout=30)
        if result.returncode == 0:
            try:
                data = json.loads(result.stdout)
                return data.get("bytes", None)
            except json.JSONDecodeError:
                pass
        return None
