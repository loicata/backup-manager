import json as json_mod
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from src.storage.base import StorageBackend
from src.security.secure_memory import secure_clear

logger = logging.getLogger(__name__)


class ProtonDriveStorage(StorageBackend):
    """
    Storage backend for Proton Drive using rclone.

    Proton Drive does not yet offer a stable Python SDK.
    This backend uses rclone's protondrive backend, which supports
    the same end-to-end client-side encryption as the official apps.

    Requires: rclone installed and accessible in PATH or via proton_rclone_path.
    Install: https://rclone.org/install/
    """

    RCLONE_REMOTE_NAME = "backupmanager_proton"

    def _find_rclone(self) -> str:
        """Find the rclone binary path."""
        if self.config.proton_rclone_path:
            return self.config.proton_rclone_path

        # Try common locations
        for candidate in ["rclone", "rclone.exe",
                           os.path.expanduser("~/rclone/rclone.exe"),
                           "C:\\rclone\\rclone.exe"]:
            if shutil.which(candidate):
                return candidate

        raise FileNotFoundError(
            "rclone is not installed or not found. "
            "Download it from https://rclone.org/install/ "
            "and make sure it is in your PATH."
        )

    def _build_env(self) -> dict:
        """Build environment variables for rclone with Proton credentials."""
        from src.security.encryption import retrieve_password
        env = os.environ.copy()
        env["RCLONE_CONFIG_BACKUPMANAGER_PROTON_TYPE"] = "protondrive"
        env["RCLONE_CONFIG_BACKUPMANAGER_PROTON_USER"] = self.config.proton_username
        decrypted_pwd = retrieve_password(self.config.proton_password)
        try:
            env["RCLONE_CONFIG_BACKUPMANAGER_PROTON_PASS"] = self._obscure_password(
                decrypted_pwd
            )
        finally:
            secure_clear(decrypted_pwd)
            decrypted_pwd = None
        if self.config.proton_2fa:
            env["RCLONE_CONFIG_BACKUPMANAGER_PROTON_2FA"] = self.config.proton_2fa
        return env

    def _obscure_password(self, password: str) -> str:
        """Obscure a password for rclone env var (rclone obscure)."""
        try:
            rclone = self._find_rclone()
            result = subprocess.run(
                [rclone, "obscure", password],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception as e:
            logger.debug(f"rclone obscure unavailable, using fallback: {type(e).__name__}")
        return password

    def _run_rclone(self, args: list[str], timeout: int = 300) -> subprocess.CompletedProcess:
        """Run an rclone command with Proton Drive credentials."""
        rclone = self._find_rclone()
        env = self._build_env()
        full_args = [rclone] + args
        # Add bandwidth limit if set
        if self._bandwidth_limit_kbps > 0:
            full_args.append(f"--bwlimit={self._bandwidth_limit_kbps}k")
        return subprocess.run(
            full_args, capture_output=True, text=True,
            timeout=timeout, env=env,
        )

    def _remote_path(self, name: str = "") -> str:
        """Build the full rclone remote:path string."""
        base = f"{self.RCLONE_REMOTE_NAME}:{self.config.proton_remote_path.strip('/')}"
        if name:
            return f"{base}/{name}"
        return base

    def upload(self, local_path: Path, remote_name: str) -> bool:
        try:
            remote = self._remote_path(remote_name)
            if local_path.is_file():
                result = self._run_rclone([
                    "copyto", str(local_path), remote,
                    "--no-traverse",
                ])
            else:
                result = self._run_rclone([
                    "copy", str(local_path), remote,
                    "--no-traverse",
                ])

            if result.returncode == 0:
                logger.info(f"Uploaded to Proton Drive: {remote}")
                return True
            else:
                logger.error(f"Proton Drive upload failed: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"Proton Drive upload error: {e}")
            return False

    def list_backups(self) -> list[dict]:
        try:
            remote = self._remote_path()
            result = self._run_rclone([
                "lsjson", remote, "--dirs-only",
            ], timeout=60)

            if result.returncode != 0:
                return []

            entries = json_mod.loads(result.stdout) if result.stdout.strip() else []
            backups = []
            for entry in entries:
                backups.append({
                    "name": entry.get("Name", ""),
                    "size": entry.get("Size", 0),
                    "modified": 0,
                    "is_dir": entry.get("IsDir", True),
                })

            return sorted(backups, key=lambda b: b["name"], reverse=True)
        except Exception as e:
            logger.error(f"Proton Drive list failed: {e}")
            return []

    def delete_backup(self, remote_name: str) -> bool:
        try:
            remote = self._remote_path(remote_name)
            result = self._run_rclone(["purge", remote], timeout=120)
            return result.returncode == 0
        except Exception as e:
            logger.error(f"Proton Drive delete failed: {e}")
            return False

    def test_connection(self) -> tuple[bool, str]:
        try:
            self._find_rclone()
        except FileNotFoundError as e:
            return False, f"❌ {e}"

        try:
            remote = self._remote_path()
            result = self._run_rclone(["lsd", remote], timeout=30)
            if result.returncode == 0:
                return True, (
                    f"✅ Proton Drive connected: {self.config.proton_username} "
                    f"({self.config.proton_remote_path})"
                )
            else:
                error = result.stderr.strip().split("\n")[-1] if result.stderr else "Unknown error"
                return False, f"❌ Proton Drive: {error}"
        except Exception as e:
            return False, f"❌ Proton Drive Error: {e}"

    def get_free_space(self) -> Optional[int]:
        try:
            remote = f"{self.RCLONE_REMOTE_NAME}:"
            result = self._run_rclone(["about", remote, "--json"], timeout=30)
            if result.returncode == 0:
                data = json_mod.loads(result.stdout)
                return data.get("free")
        except Exception as e:
            logger.debug(f"Cannot determine free space: {e}")
        return None

    def get_file_size(self, remote_name: str) -> Optional[int]:
        try:
            remote_path = self.config.proton_remote_path.rstrip("/")
            full_path = f"{self.RCLONE_REMOTE_NAME}:{remote_path}/{remote_name}"
            result = self._run_rclone(["size", full_path, "--json"], timeout=30)
            if result.returncode == 0:
                data = json_mod.loads(result.stdout)
                return data.get("bytes")
        except Exception:
            pass
        return None
