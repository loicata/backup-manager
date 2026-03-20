"""SFTP storage backend with exec channel fast mode + SFTP fallback.

Connection flow:
1. Authenticate via SSH key or password
2. Probe exec channel availability (_check_exec_channel)
3. Upload: try exec channel first (fast), fallback to SFTP protocol

Security:
- Host key verification (TOFU: trust on first use)
- Path traversal protection on remote names
- Password decrypted via DPAPI/AES at connection time
"""

import io
import logging
import os
import re
import stat
import time
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Optional

from src.storage.base import StorageBackend, with_retry

logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT = 15  # Seconds for SSH/SFTP connection
_EXEC_PROBE_TIMEOUT = 10  # Seconds for exec channel probe
_KEEPALIVE_INTERVAL = 30  # Seconds between SSH keepalive packets
_FAST_CHUNK_SIZE = 1024 * 1024  # 1 MB
_SFTP_WINDOW_SIZE = 2**25  # 32 MB (default is 2 MB)


def _validate_remote_name(name: str) -> str:
    """Validate remote name against path traversal and injection attacks.

    Rejects names containing shell metacharacters that could be
    exploited via the exec channel (cat > file) fast upload mode.

    Args:
        name: Remote file or directory name.

    Returns:
        Sanitized name with leading slashes removed.

    Raises:
        ValueError: If the name is invalid or contains dangerous characters.
    """
    if not name:
        raise ValueError("Remote name cannot be empty")
    if "\x00" in name:
        raise ValueError("Remote name contains null byte")
    if ".." in name.split("/"):
        raise ValueError("Remote name contains path traversal")

    # Reject shell metacharacters that could allow command injection
    # even through single-quote escaping (e.g. backticks, $())
    _DANGEROUS_CHARS = set("`$;&|><!")
    for char in _DANGEROUS_CHARS:
        if char in name:
            raise ValueError(f"Remote name contains dangerous character: {char!r}")

    # Strip leading slashes for relative paths
    return name.lstrip("/")


def _shell_escape(s: str) -> str:
    """Escape a string for safe use in shell commands."""
    return "'" + s.replace("'", "'\\''") + "'"


class SFTPStorage(StorageBackend):
    """SFTP/SSH storage backend."""

    def __init__(
        self,
        host: str,
        port: int = 22,
        username: str = "",
        password: str = "",
        key_path: str = "",
        key_passphrase: str = "",
        remote_path: str = "/home",
    ):
        super().__init__()
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._key_path = key_path
        self._key_passphrase = key_passphrase
        self._remote_path = remote_path
        self._exec_available: Optional[bool] = None
        self._persistent_transport = None

    def connect(self) -> None:
        """Open a persistent SSH transport for batch operations.

        Call this before a series of upload_file() calls to reuse
        one connection instead of creating one per file.
        """
        if self._persistent_transport is not None:
            try:
                if self._persistent_transport.is_active():
                    return  # Already connected
            except Exception:
                pass
        self._persistent_transport = self._create_transport()

    def disconnect(self) -> None:
        """Close the persistent SSH transport."""
        if self._persistent_transport is not None:
            try:
                self._persistent_transport.close()
            except Exception:
                pass
            self._persistent_transport = None

    def _get_transport(self):
        """Get or create an SSH transport.

        If a persistent transport is active (from connect()), reuse it.
        Otherwise, create a new one (for single-shot operations).
        """
        if self._persistent_transport is not None:
            try:
                if self._persistent_transport.is_active():
                    return self._persistent_transport
            except Exception:
                self._persistent_transport = None

        return self._create_transport()

    def _create_transport(self):
        """Create and authenticate a new SSH transport.

        Verifies the remote host key against ~/.ssh/known_hosts.
        On first connection (TOFU), the key is saved automatically.
        Rejects connections if the host key has changed (MITM protection).
        """
        import paramiko

        transport = paramiko.Transport((self._host, self._port))
        transport.set_keepalive(_KEEPALIVE_INTERVAL)

        # Authenticate
        if self._key_path:
            pkey = self._load_private_key()
            transport.connect(username=self._username, pkey=pkey)
        elif self._password:
            transport.connect(username=self._username, password=self._password)
        else:
            raise ValueError("No authentication method: provide password or key")

        # Verify host key against known_hosts
        self._verify_host_key(transport)

        return transport

    def _verify_host_key(self, transport) -> None:
        """Verify remote host key against known_hosts (TOFU model).

        On first connection, saves the key. On subsequent connections,
        rejects if the key has changed.

        Args:
            transport: Connected paramiko Transport.

        Raises:
            SecurityError: If the host key has changed (possible MITM).
        """
        import paramiko

        known_hosts = Path.home() / ".ssh" / "known_hosts"
        known_hosts.parent.mkdir(parents=True, exist_ok=True)

        host_keys = paramiko.HostKeys()
        if known_hosts.exists():
            try:
                host_keys.load(str(known_hosts))
            except Exception:
                logger.warning("Could not load known_hosts, treating as empty")

        remote_key = transport.get_remote_server_key()
        host_entry = host_keys.lookup(self._host)

        if host_entry is None:
            # TOFU: first connection, save the key
            host_keys.add(self._host, remote_key.get_name(), remote_key)
            host_keys.save(str(known_hosts))
            logger.info(
                "New host key saved for %s (%s)",
                self._host,
                remote_key.get_name(),
            )
        else:
            # Verify the key matches
            stored_key = host_entry.get(remote_key.get_name())
            if stored_key is None or stored_key != remote_key:
                transport.close()
                raise OSError(
                    f"Host key verification failed for {self._host}. "
                    f"The server key has changed — possible MITM attack. "
                    f"Remove the old key from {known_hosts} if this is expected."
                )

    def _load_private_key(self):
        """Load SSH private key, auto-detecting type."""
        import paramiko

        passphrase = self._key_passphrase or None
        key_path = self._key_path

        key_classes = [
            paramiko.Ed25519Key,
            paramiko.ECDSAKey,
            paramiko.RSAKey,
        ]

        for key_class in key_classes:
            try:
                return key_class.from_private_key_file(key_path, password=passphrase)
            except Exception:
                continue

        raise ValueError(f"Could not load key: {key_path} (tried Ed25519, ECDSA, RSA)")

    def _get_sftp(self, transport):
        """Open SFTP session from transport."""
        import paramiko

        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.get_channel().settimeout(_CONNECT_TIMEOUT)
        return sftp

    def _check_exec_channel(self, transport) -> bool:
        """Probe if SSH exec channel is available.

        Some servers restrict shell access (ChrootDirectory, ForceCommand).
        """
        if self._exec_available is not None:
            return self._exec_available

        try:
            channel = transport.open_session()
            try:
                channel.settimeout(_EXEC_PROBE_TIMEOUT)
                channel.exec_command("echo ok")  # nosec B601
                # Read output BEFORE recv_exit_status (critical ordering)
                data = channel.recv(1024)
                output = data.decode("utf-8", errors="replace").strip()
                exit_status = channel.recv_exit_status()
                self._exec_available = exit_status == 0 and output == "ok"
                logger.info(
                    "Exec channel probe: output=%r, exit=%d, available=%s",
                    output,
                    exit_status,
                    self._exec_available,
                )
            finally:
                channel.close()
        except Exception as e:
            logger.warning("Exec channel probe failed: %s: %s", type(e).__name__, e)
            self._exec_available = False

        return self._exec_available

    # --- Upload methods ---

    @with_retry(max_retries=3, base_delay=2.0)
    def _should_close_transport(self, transport) -> bool:
        """Check if a transport should be closed after use.

        Persistent transports (from connect()) are kept alive.
        Ad-hoc transports (created per-call) are closed.
        """
        return transport is not self._persistent_transport

    def upload(self, local_path: Path, remote_name: str) -> None:
        """Upload a file or directory to the remote server."""
        remote_name = _validate_remote_name(remote_name)
        transport = self._get_transport()

        try:
            if local_path.is_dir():
                self._upload_directory(transport, local_path, remote_name)
            else:
                self._upload_single_file(transport, local_path, remote_name)
        finally:
            if self._should_close_transport(transport):
                transport.close()

    @with_retry(max_retries=3, base_delay=2.0)
    def upload_file(self, fileobj: BinaryIO, remote_path: str, size: int = 0) -> None:
        """Stream a file-like object to the remote server.

        Uses exec channel (cat > file) when available for ~10x faster
        throughput. Falls back to SFTP protocol on restricted servers.
        Reuses persistent transport if connect() was called.
        """
        transport = self._get_transport()
        is_persistent = not self._should_close_transport(transport)
        try:
            full_path = self._join_remote(remote_path)
            parent = str(PurePosixPath(full_path).parent)
            exec_ok = self._check_exec_channel(transport)

            if exec_ok:
                self._ensure_remote_dir_exec(transport, parent)
                self._fast_upload_fileobj(transport, fileobj, full_path, size)
            else:
                sftp = self._get_sftp(transport)
                try:
                    self._ensure_remote_dir_sftp(sftp, parent)
                    reader = self._get_throttled_reader(fileobj)

                    with sftp.open(full_path, "wb") as remote_file:
                        remote_file.set_pipelined(True)
                        bytes_sent = 0
                        while True:
                            chunk = reader.read(_FAST_CHUNK_SIZE)
                            if not chunk:
                                break
                            remote_file.write(chunk)
                            bytes_sent += len(chunk)
                            if self._progress_callback and size > 0:
                                self._progress_callback(bytes_sent, size)
                finally:
                    sftp.close()
        finally:
            if not is_persistent:
                transport.close()

    def _fast_upload_fileobj(
        self,
        transport,
        fileobj: BinaryIO,
        remote_path: str,
        size: int,
    ) -> None:
        """Upload a file-like object via exec channel (cat > file)."""
        escaped = _shell_escape(remote_path)
        channel = transport.open_session()
        try:
            channel.exec_command(f"cat > {escaped}")  # nosec B601
            reader = self._get_throttled_reader(fileobj)
            bytes_sent = 0

            while True:
                chunk = reader.read(_FAST_CHUNK_SIZE)
                if not chunk:
                    break
                channel.sendall(chunk)
                bytes_sent += len(chunk)
                if self._progress_callback and size > 0:
                    self._progress_callback(bytes_sent, size)

            channel.shutdown_write()
            exit_status = channel.recv_exit_status()
            if exit_status != 0:
                raise OSError(f"Remote cat failed (exit {exit_status}) for {remote_path}")
        finally:
            channel.close()

    def _upload_directory(self, transport, local_path: Path, remote_name: str) -> None:
        """Upload an entire directory."""
        exec_ok = self._check_exec_channel(transport)

        if exec_ok:
            self._upload_dir_exec(transport, local_path, remote_name)
        else:
            sftp = self._get_sftp(transport)
            try:
                self._upload_dir_sftp(sftp, local_path, remote_name)
            finally:
                sftp.close()

    def _upload_single_file(self, transport, local_path: Path, remote_name: str) -> None:
        """Upload a single file using the best available method."""
        exec_ok = self._check_exec_channel(transport)
        remote_full = self._join_remote(remote_name)

        if exec_ok:
            self._ensure_remote_dir_exec(transport, str(PurePosixPath(remote_full).parent))
            self._fast_upload_file(transport, local_path, remote_full)
        else:
            sftp = self._get_sftp(transport)
            try:
                self._ensure_remote_dir_sftp(sftp, str(PurePosixPath(remote_full).parent))
                self._sftp_upload_file(sftp, local_path, remote_full)
            finally:
                sftp.close()

    def _fast_upload_file(self, transport, local_path: Path, remote_path: str) -> None:
        """Upload file via exec channel (cat > file) — fast mode."""
        escaped = _shell_escape(remote_path)
        channel = transport.open_session()
        try:
            channel.exec_command(f"cat > {escaped}")  # nosec B601
            file_size = local_path.stat().st_size
            bytes_sent = 0

            with open(local_path, "rb") as f:
                reader = self._get_throttled_reader(f)
                while True:
                    chunk = reader.read(_FAST_CHUNK_SIZE)
                    if not chunk:
                        break
                    channel.sendall(chunk)
                    bytes_sent += len(chunk)
                    if self._progress_callback and file_size > 0:
                        self._progress_callback(bytes_sent, file_size)

            channel.shutdown_write()
            exit_status = channel.recv_exit_status()
            if exit_status != 0:
                raise RuntimeError(f"cat upload failed with exit code {exit_status}")
        finally:
            channel.close()

    def _sftp_upload_file(self, sftp, local_path: Path, remote_path: str) -> None:
        """Upload file via SFTP protocol — compatible mode."""
        file_size = local_path.stat().st_size
        bytes_sent = 0

        with open(local_path, "rb") as f_in:
            reader = self._get_throttled_reader(f_in)
            with sftp.open(remote_path, "wb") as f_out:
                f_out.set_pipelined(True)
                while True:
                    chunk = reader.read(_FAST_CHUNK_SIZE)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    bytes_sent += len(chunk)
                    if self._progress_callback and file_size > 0:
                        self._progress_callback(bytes_sent, file_size)

    def _upload_dir_exec(self, transport, local_path: Path, remote_name: str) -> None:
        """Upload directory using exec channel for each file."""
        base = self._join_remote(remote_name)
        self._ensure_remote_dir_exec(transport, base)

        for filepath in local_path.rglob("*"):
            if filepath.is_file():
                rel = filepath.relative_to(local_path).as_posix()
                remote_full = f"{base}/{rel}"
                parent = str(PurePosixPath(remote_full).parent)
                self._ensure_remote_dir_exec(transport, parent)
                self._fast_upload_file(transport, filepath, remote_full)

    def _upload_dir_sftp(self, sftp, local_path: Path, remote_name: str) -> None:
        """Upload directory using SFTP protocol."""
        base = self._join_remote(remote_name)
        self._ensure_remote_dir_sftp(sftp, base)

        for filepath in local_path.rglob("*"):
            if filepath.is_file():
                rel = filepath.relative_to(local_path).as_posix()
                remote_full = f"{base}/{rel}"
                parent = str(PurePosixPath(remote_full).parent)
                self._ensure_remote_dir_sftp(sftp, parent)
                self._sftp_upload_file(sftp, filepath, remote_full)

    # --- Directory creation ---

    def _ensure_remote_dir_exec(self, transport, remote_dir: str) -> None:
        """Create remote directory via exec channel."""
        escaped = _shell_escape(remote_dir)
        channel = transport.open_session()
        try:
            channel.exec_command(f"mkdir -p {escaped}")  # nosec B601
            channel.recv_exit_status()
        finally:
            channel.close()

    def _ensure_remote_dir_sftp(self, sftp, remote_dir: str) -> None:
        """Create remote directory via SFTP, creating parents as needed."""
        parts = PurePosixPath(remote_dir).parts
        current = ""
        for part in parts:
            current = f"{current}/{part}" if current else f"/{part}"
            try:
                sftp.stat(current)
            except FileNotFoundError:
                try:
                    sftp.mkdir(current)
                except OSError:
                    pass  # Race condition or already exists

    # --- List / Delete / Test ---

    @with_retry(max_retries=2, base_delay=1.0)
    def list_backups(self) -> list[dict]:
        """List backups in the remote directory."""
        transport = self._get_transport()
        try:
            sftp = self._get_sftp(transport)
            try:
                entries = sftp.listdir_attr(self._remote_path)
                backups = []
                for entry in entries:
                    if entry.filename.startswith("."):
                        continue
                    backups.append(
                        {
                            "name": entry.filename,
                            "size": entry.st_size or 0,
                            "modified": entry.st_mtime or 0,
                            "is_dir": stat.S_ISDIR(entry.st_mode) if entry.st_mode else False,
                        }
                    )
                return sorted(backups, key=lambda b: b["modified"], reverse=True)
            finally:
                sftp.close()
        finally:
            transport.close()

    @with_retry(max_retries=2, base_delay=1.0)
    def delete_backup(self, remote_name: str) -> None:
        """Delete a backup from the remote server."""
        remote_name = _validate_remote_name(remote_name)
        transport = self._get_transport()
        try:
            sftp = self._get_sftp(transport)
            try:
                full_path = self._join_remote(remote_name)
                try:
                    st = sftp.stat(full_path)
                    if stat.S_ISDIR(st.st_mode):
                        self._recursive_remove(sftp, full_path)
                    else:
                        sftp.remove(full_path)
                except FileNotFoundError:
                    raise FileNotFoundError(f"Backup not found: {remote_name}")
            finally:
                sftp.close()
        finally:
            transport.close()

        logger.info("Deleted remote backup: %s", remote_name)

    def _recursive_remove(self, sftp, path: str) -> None:
        """Recursively remove a remote directory."""
        for entry in sftp.listdir_attr(path):
            full = f"{path}/{entry.filename}"
            if stat.S_ISDIR(entry.st_mode):
                self._recursive_remove(sftp, full)
            else:
                sftp.remove(full)
        sftp.rmdir(path)

    def test_connection(self) -> tuple[bool, str]:
        """Test SSH/SFTP connection."""
        try:
            transport = self._get_transport()
        except Exception as e:
            return False, f"Connection failed: {e}"

        try:
            # Test SFTP subsystem
            sftp = self._get_sftp(transport)
            try:
                sftp.listdir(self._remote_path)
            except FileNotFoundError:
                return False, f"Remote path not found: {self._remote_path}"
            finally:
                sftp.close()

            # Check exec channel
            exec_ok = self._check_exec_channel(transport)
            info = f"SFTP connected: {self._username}@{self._host}:{self._port}"

            if not exec_ok:
                info += (
                    "\nShell access restricted — uploads will use SFTP protocol "
                    "(slower but compatible)"
                )

            # Get free space
            try:
                sftp2 = self._get_sftp(transport)
                try:
                    vfs = sftp2.statvfs(self._remote_path)
                    free = vfs.f_bavail * vfs.f_frsize
                    free_gb = free / (1024**3)
                    info += f"\n{free_gb:.1f} GB free"
                finally:
                    sftp2.close()
            except Exception:
                pass

            return True, info
        except Exception as e:
            return False, f"SFTP Error: {type(e).__name__}: {e}"
        finally:
            transport.close()

    def get_free_space(self) -> Optional[int]:
        """Get free space on the remote filesystem."""
        try:
            transport = self._get_transport()
            try:
                sftp = self._get_sftp(transport)
                try:
                    vfs = sftp.statvfs(self._remote_path)
                    return vfs.f_bavail * vfs.f_frsize
                finally:
                    sftp.close()
            finally:
                transport.close()
        except Exception:
            return None

    def get_file_size(self, remote_name: str) -> Optional[int]:
        """Get size of a remote file."""
        try:
            transport = self._get_transport()
            try:
                sftp = self._get_sftp(transport)
                try:
                    full_path = self._join_remote(remote_name)
                    return sftp.stat(full_path).st_size
                finally:
                    sftp.close()
            finally:
                transport.close()
        except Exception:
            return None

    def download_backup(self, remote_name: str, local_dir: Path) -> Path:
        """Download a backup from SFTP to a local directory."""
        local_dir.mkdir(parents=True, exist_ok=True)
        dst = local_dir / remote_name
        dst.mkdir(parents=True, exist_ok=True)

        transport = self._get_transport()
        try:
            sftp = self._get_sftp(transport)
            try:
                remote_base = self._join_remote(remote_name)
                self._sftp_download_dir(sftp, remote_base, dst)
            finally:
                sftp.close()
        finally:
            transport.close()
        return dst

    def _sftp_download_dir(self, sftp, remote_dir: str, local_dir: Path):
        """Recursively download a remote directory via SFTP.

        Args:
            sftp: Open SFTP client.
            remote_dir: Remote directory path.
            local_dir: Local destination directory.
        """
        import stat as stat_module

        for entry in sftp.listdir_attr(remote_dir):
            remote_path = f"{remote_dir}/{entry.filename}"
            local_path = local_dir / entry.filename
            if stat_module.S_ISDIR(entry.st_mode):
                local_path.mkdir(parents=True, exist_ok=True)
                self._sftp_download_dir(sftp, remote_path, local_path)
            else:
                sftp.get(remote_path, str(local_path))

    def _join_remote(self, name: str) -> str:
        """Join remote base path with a name."""
        return f"{self._remote_path.rstrip('/')}/{name}"
