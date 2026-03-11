import logging
import os
from pathlib import Path
from typing import Optional

from src.storage.base import StorageBackend, with_retry
from src.security.secure_memory import secure_clear

logger = logging.getLogger(__name__)


class SFTPStorage(StorageBackend):
    """Storage backend for SFTP servers."""

    # ── SFTP connection setup ──
    # Password decrypted from DPAPI, used for paramiko.Transport,
    # then immediately cleared from memory.
    def _get_transport(self):
        try:
            import paramiko
        except ImportError:
            raise ImportError("paramiko is not installed. Run: pip install paramiko")

        transport = paramiko.Transport((self.config.sftp_host, self.config.sftp_port))

        if self.config.sftp_key_path:
            # Auto-detect key type (RSA, Ed25519, ECDSA, DSA)
            pkey = None
            key_path = self.config.sftp_key_path
            for key_class in (paramiko.Ed25519Key, paramiko.ECDSAKey,
                              paramiko.RSAKey, paramiko.DSSKey):
                try:
                    pkey = key_class.from_private_key_file(key_path)
                    break
                except (paramiko.ssh_exception.SSHException, ValueError):
                    continue
            if pkey is None:
                raise ValueError(f"Cannot load SSH key: {key_path}")
            transport.connect(username=self.config.sftp_username, pkey=pkey)
        else:
            from src.security.encryption import retrieve_password
            decrypted_pwd = retrieve_password(self.config.sftp_password)
            try:
                transport.connect(
                    username=self.config.sftp_username,
                    password=decrypted_pwd,
                )
            finally:
                secure_clear(decrypted_pwd)
                decrypted_pwd = None
        return transport

    def _get_sftp(self):
        import paramiko
        transport = self._get_transport()
        return paramiko.SFTPClient.from_transport(transport), transport

    # ── Recursive remote mkdir ──
    # Creates parent directories on the SFTP server if they don't exist.
    def _ensure_remote_dir(self, sftp, remote_dir: str):
        """Recursively create remote directories."""
        dirs_to_create = []
        current = remote_dir
        while current and current != "/":
            try:
                sftp.stat(current)
                break
            except FileNotFoundError:
                dirs_to_create.append(current)
                current = os.path.dirname(current)
        for d in reversed(dirs_to_create):
            try:
                sftp.mkdir(d)
            except OSError:
                pass

    @with_retry()
    def upload(self, local_path: Path, remote_name: str) -> bool:
        try:
            sftp, transport = self._get_sftp()
            remote_base = f"{self.config.sftp_remote_path}/{remote_name}".rstrip("/")

            try:
                if local_path.is_file():
                    remote_dir = os.path.dirname(remote_base)
                    self._ensure_remote_dir(sftp, remote_dir)
                    if self._bandwidth_limit_kbps > 0:
                        with open(local_path, "rb") as f:
                            sftp.putfo(self._get_throttled_reader(f), remote_base)
                    else:
                        sftp.put(str(local_path), remote_base)
                else:
                    for file_path in local_path.rglob("*"):
                        if file_path.is_file():
                            rel = file_path.relative_to(local_path)
                            remote_file = f"{remote_base}/{rel}".replace("\\", "/")
                            remote_dir = os.path.dirname(remote_file)
                            self._ensure_remote_dir(sftp, remote_dir)
                            if self._bandwidth_limit_kbps > 0:
                                with open(file_path, "rb") as f:
                                    sftp.putfo(self._get_throttled_reader(f), remote_file)
                            else:
                                sftp.put(str(file_path), remote_file)

                logger.info(f"Uploaded to SFTP: {self.config.sftp_host}:{remote_base}")
                return True
            finally:
                sftp.close()
                transport.close()
        except Exception as e:
            logger.error(f"SFTP upload failed: {e}")
            return False

    def list_backups(self) -> list[dict]:
        try:
            sftp, transport = self._get_sftp()
            try:
                entries = sftp.listdir_attr(self.config.sftp_remote_path)
                backups = []
                for entry in sorted(entries, key=lambda e: e.st_mtime or 0, reverse=True):
                    from stat import S_ISDIR
                    backups.append({
                        "name": entry.filename,
                        "size": entry.st_size or 0,
                        "modified": entry.st_mtime or 0,
                        "is_dir": S_ISDIR(entry.st_mode) if entry.st_mode else False,
                    })
                return backups
            finally:
                sftp.close()
                transport.close()
        except Exception as e:
            logger.error(f"SFTP list failed: {e}")
            return []

    @with_retry()
    def delete_backup(self, remote_name: str) -> bool:
        try:
            sftp, transport = self._get_sftp()
            remote_path = f"{self.config.sftp_remote_path}/{remote_name}"
            try:
                self._recursive_remove(sftp, remote_path)
                return True
            finally:
                sftp.close()
                transport.close()
        except Exception as e:
            logger.error(f"SFTP delete failed: {e}")
            return False

    def _recursive_remove(self, sftp, path: str):
        """Recursively remove a remote file or directory."""
        from stat import S_ISDIR
        try:
            attr = sftp.stat(path)
            if S_ISDIR(attr.st_mode):
                for entry in sftp.listdir(path):
                    self._recursive_remove(sftp, f"{path}/{entry}")
                sftp.rmdir(path)
            else:
                sftp.remove(path)
        except FileNotFoundError:
            pass

    @with_retry()
    def test_connection(self) -> tuple[bool, str]:
        try:
            sftp, transport = self._get_sftp()
            try:
                sftp.listdir(self.config.sftp_remote_path)
                free = self.get_free_space()
                space_info = f" — {self.format_size(free)} available" if free else ""
                return True, (
                    f"✅ SFTP connected: {self.config.sftp_username}@"
                    f"{self.config.sftp_host}:{self.config.sftp_port}{space_info}"
                )
            finally:
                sftp.close()
                transport.close()
        except Exception as e:
            return False, f"❌ SFTP Error: {e}"

    def get_free_space(self) -> Optional[int]:
        try:
            sftp, transport = self._get_sftp()
            try:
                stat = sftp.statvfs(self.config.sftp_remote_path)
                # f_bavail = free blocks for unprivileged users, f_frsize = fragment size
                return stat.f_bavail * stat.f_frsize
            finally:
                sftp.close()
                transport.close()
        except Exception:
            return None

    def get_file_size(self, remote_name: str) -> Optional[int]:
        try:
            sftp, transport = self._get_sftp()
            try:
                remote_dir = self.config.sftp_remote_path.rstrip("/")
                remote_path = f"{remote_dir}/{remote_name}"
                attrs = sftp.stat(remote_path)
                return attrs.st_size
            finally:
                sftp.close()
                transport.close()
        except Exception:
            return None
