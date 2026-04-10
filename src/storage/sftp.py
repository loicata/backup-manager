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

import contextlib
import io
import logging
import socket
import stat
import tarfile
import time
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from src.storage.base import StorageBackend, long_path_mkdir, long_path_str, with_retry

logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT = 60  # Seconds for SSH/SFTP connection (aligned with S3)
_OPERATION_TIMEOUT = 600  # Seconds for SFTP operations (delete, list, etc.)
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


class _ChannelWriter(io.RawIOBase):
    """Adapter that streams writes to an SSH channel.

    Used by ``tarfile.open(mode='w|')`` to stream tar data directly
    into an exec channel (``tar xf -``), tracking bytes for progress.

    Tarfile emits many small writes (512-byte headers, padding blocks).
    Without buffering, each tiny write becomes a separate ``sendall()``
    call — each one must pass through SSH flow control, causing massive
    overhead and near-zero throughput on high-latency or slow receivers.

    The internal buffer accumulates small writes and flushes in chunks
    of _FAST_CHUNK_SIZE (1 MB), reducing SSH round-trips by ~2000x for
    typical backups with many small files.
    """

    def __init__(
        self,
        channel,
        progress_callback=None,
        total_bytes: int = 0,
        cancel_check=None,
        limit_kbps: int = 0,
    ):
        self._channel = channel
        self._progress_callback = progress_callback
        self._total_bytes = total_bytes
        self._bytes_sent = 0
        self._cancel_check = cancel_check
        self._limit_bps = limit_kbps * 1024
        self._start_time = time.monotonic()
        self._buffer = bytearray()

    def write(self, data: bytes | bytearray) -> int:
        """Buffer data and flush in large chunks for efficient SSH transfer."""
        if self._cancel_check is not None:
            self._cancel_check()

        self._buffer.extend(data)

        # Flush when buffer reaches chunk size
        while len(self._buffer) >= _FAST_CHUNK_SIZE:
            self._flush_chunk(_FAST_CHUNK_SIZE)

        return len(data)

    def flush(self) -> None:
        """Flush remaining buffered data to the SSH channel."""
        if self._buffer:
            self._flush_chunk(len(self._buffer))

    def close(self) -> None:
        """Flush buffer before closing."""
        self.flush()

    def _flush_chunk(self, size: int) -> None:
        """Send *size* bytes from the buffer to the channel."""
        chunk = bytes(self._buffer[:size])
        del self._buffer[:size]

        self._channel.sendall(chunk)
        self._bytes_sent += len(chunk)

        # Bandwidth throttling: sleep if sending faster than limit
        if self._limit_bps > 0:
            elapsed = time.monotonic() - self._start_time
            if elapsed > 0:
                current_rate = self._bytes_sent / elapsed
                if current_rate > self._limit_bps:
                    sleep_time = (self._bytes_sent / self._limit_bps) - elapsed
                    if sleep_time > 0:
                        time.sleep(sleep_time)

        if self._progress_callback and self._total_bytes > 0:
            self._progress_callback(self._bytes_sent, self._total_bytes)

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False


class SFTPStorage(StorageBackend):
    """SFTP/SSH storage backend."""

    supports_tar_stream: bool = True

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
        self._exec_available: bool | None = None
        self._persistent_transport = None
        self._created_dirs: set[str] = set()

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
        self._created_dirs.clear()
        self._persistent_transport = self._create_transport()

    def disconnect(self) -> None:
        """Close the persistent SSH transport."""
        if self._persistent_transport is not None:
            with contextlib.suppress(Exception):
                self._persistent_transport.close()
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

        Uses a pre-connected socket with explicit timeout to avoid
        relying on the OS TCP timeout (21-30s on Windows).
        Verifies the remote host key against ~/.ssh/known_hosts.
        On first connection (TOFU), the key is saved automatically.
        Rejects connections if the host key has changed (MITM protection).
        """
        import paramiko

        # Create socket with explicit timeout (faster than OS default)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(_CONNECT_TIMEOUT)
        try:
            sock.connect((self._host, self._port))
        except (TimeoutError, OSError) as e:
            sock.close()
            raise OSError(
                f"Cannot reach {self._host}:{self._port} " f"(timeout {_CONNECT_TIMEOUT}s)"
            ) from e

        # Switch socket to operation timeout now that connection succeeded
        sock.settimeout(_OPERATION_TIMEOUT)

        transport = paramiko.Transport(
            sock,
            default_window_size=_SFTP_WINDOW_SIZE,
            default_max_packet_size=2**15,  # 32 KB (paramiko max)
        )
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
        sftp.get_channel().settimeout(_OPERATION_TIMEOUT)
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

    def upload_tar_stream(
        self,
        files: list[tuple[Path, str, int]],
        remote_dir: str,
        progress_callback=None,
        cancel_check=None,
    ) -> None:
        """Upload multiple files as a single tar stream via exec channel.

        Streams a tar archive directly to ``tar xf -`` on the remote
        server, eliminating per-file SSH channel overhead.  Falls back
        to individual ``upload_file()`` calls when exec is unavailable.

        Args:
            files: List of (local_path, relative_path, size) tuples.
            remote_dir: Remote directory where files are extracted.
            progress_callback: Optional callable(bytes_sent, total_bytes).
            cancel_check: Optional callable that raises CancelledError.

        Raises:
            OSError: If the remote tar extraction fails.
        """
        transport = self._get_transport()
        full_dir = self._join_remote(remote_dir)

        if not self._check_exec_channel(transport):
            self._tar_fallback(files, remote_dir, progress_callback)
            return

        self._ensure_remote_dir_exec(transport, full_dir)

        escaped_dir = _shell_escape(full_dir)
        channel = transport.open_session()
        try:
            channel.exec_command(f"tar xf - -C {escaped_dir}")  # nosec B601

            total_bytes = sum(size for _, _, size in files)
            writer = _ChannelWriter(
                channel,
                progress_callback,
                total_bytes,
                cancel_check,
                limit_kbps=self._bandwidth_limit_kbps,
            )

            with tarfile.open(fileobj=writer, mode="w|") as tar:
                for local_path, rel_path, size in files:
                    if cancel_check is not None:
                        cancel_check()
                    info = tarfile.TarInfo(name=rel_path)
                    info.size = size
                    with open(local_path, "rb") as f:
                        tar.addfile(info, fileobj=f)

            # Flush remaining buffered data before closing channel
            writer.flush()
            channel.shutdown_write()
            exit_status = channel.recv_exit_status()
            if exit_status != 0:
                raise OSError(f"Remote tar extraction failed (exit {exit_status})")
        finally:
            channel.close()

    def _tar_fallback(self, files, remote_dir, progress_callback) -> None:
        """Fallback: upload files individually when exec is unavailable."""
        total_bytes = sum(size for _, _, size in files)
        bytes_sent = 0
        for local_path, rel_path, size in files:
            if self._cancel_check is not None:
                self._cancel_check()
            remote_path = f"{remote_dir}/{rel_path}"
            with open(local_path, "rb") as f:
                self.upload_file(f, remote_path, size=size)
            bytes_sent += size
            if progress_callback:
                progress_callback(bytes_sent, total_bytes)

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
            if self._cancel_check is not None:
                self._cancel_check()
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
            if self._cancel_check is not None:
                self._cancel_check()
            if filepath.is_file():
                rel = filepath.relative_to(local_path).as_posix()
                remote_full = f"{base}/{rel}"
                parent = str(PurePosixPath(remote_full).parent)
                self._ensure_remote_dir_sftp(sftp, parent)
                self._sftp_upload_file(sftp, filepath, remote_full)

    # --- Directory creation ---

    def _ensure_remote_dir_exec(self, transport, remote_dir: str) -> None:
        """Create remote directory via exec channel.

        Skips the SSH round-trip if the directory was already created
        during this connection (cached in self._created_dirs).
        """
        if remote_dir in self._created_dirs:
            return
        escaped = _shell_escape(remote_dir)
        channel = transport.open_session()
        try:
            channel.exec_command(f"mkdir -p {escaped}")  # nosec B601
            channel.recv_exit_status()
        finally:
            channel.close()
        self._created_dirs.add(remote_dir)

    def _ensure_remote_dir_sftp(self, sftp, remote_dir: str) -> None:
        """Create remote directory via SFTP, creating parents as needed.

        Skips the SFTP round-trips if the directory was already created
        during this connection (cached in self._created_dirs).
        """
        if remote_dir in self._created_dirs:
            return
        parts = PurePosixPath(remote_dir).parts
        current = ""
        for part in parts:
            current = f"{current}/{part}" if current else f"/{part}"
            if current in self._created_dirs:
                continue
            try:
                sftp.stat(current)
            except FileNotFoundError:
                with contextlib.suppress(OSError):  # Race condition or already exists
                    sftp.mkdir(current)
            self._created_dirs.add(current)
        self._created_dirs.add(remote_dir)

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
                    # Skip manifests (.wbverify) — metadata, not backups
                    if entry.filename.endswith(".wbverify"):
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
                except FileNotFoundError as e:
                    raise FileNotFoundError(f"Backup not found: {remote_name}") from e
            finally:
                sftp.close()
        finally:
            transport.close()

        logger.info("Deleted remote backup: %s", remote_name)

        # Remove associated .wbverify manifest if present
        verify_path = self._join_remote(f"{remote_name}.wbverify")
        transport2 = self._get_transport()
        try:
            sftp2 = self._get_sftp(transport2)
            try:
                sftp2.remove(verify_path)
                logger.info("Deleted remote manifest: %s.wbverify", remote_name)
            except FileNotFoundError:
                pass
            finally:
                sftp2.close()
        finally:
            transport2.close()

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

            # Get free space — try statvfs first, fallback to df via exec
            free_bytes = self._get_free_space_from_transport(transport, exec_ok)
            if free_bytes is not None:
                free_gb = free_bytes / (1024**3)
                info += f"\n{free_gb:.1f} GB free"

            return True, info
        except Exception as e:
            return False, f"SFTP Error: {type(e).__name__}: {e}"
        finally:
            transport.close()

    def _get_free_space_from_transport(
        self,
        transport,
        exec_ok: bool,
    ) -> int | None:
        """Get free space using an existing transport.

        Tries statvfs first (standard SFTP extension), then falls
        back to ``df`` via exec channel if statvfs is unavailable.

        Args:
            transport: Active paramiko Transport.
            exec_ok: Whether exec channel is available.

        Returns:
            Free space in bytes, or None if unavailable.
        """
        # Method 1: statvfs (SFTP extension)
        try:
            sftp = self._get_sftp(transport)
            try:
                vfs = sftp.statvfs(self._remote_path)
                return vfs.f_bavail * vfs.f_frsize
            finally:
                sftp.close()
        except Exception as e:
            logger.debug("statvfs unavailable: %s", e)

        # Method 2: df via exec channel
        if exec_ok:
            try:
                escaped = _shell_escape(self._remote_path)
                channel = transport.open_session()
                try:
                    channel.settimeout(10)
                    channel.exec_command(f"df -B1 {escaped} | tail -1")  # nosec B601
                    output = channel.recv(4096).decode("utf-8", errors="replace")
                    channel.recv_exit_status()
                    # df -B1 output: filesystem 1B-blocks used available ...
                    parts = output.split()
                    if len(parts) >= 4:
                        return int(parts[3])
                finally:
                    channel.close()
            except Exception as e:
                logger.debug("df fallback failed: %s", e)

        return None

    def get_free_space(self) -> int | None:
        """Get free space on the remote filesystem."""
        try:
            transport = self._get_transport()
            try:
                exec_ok = self._check_exec_channel(transport)
                return self._get_free_space_from_transport(transport, exec_ok)
            finally:
                transport.close()
        except Exception:
            return None

    def get_file_size(self, remote_name: str) -> int | None:
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

    def list_backup_files(
        self,
        backup_name: str,
        progress_callback=None,
    ) -> list[tuple[str, int]]:
        """List files inside a backup directory on the SFTP server.

        Uses ``find -printf`` via exec channel when available for
        streaming progress.  Falls back to recursive SFTP listing.

        Args:
            backup_name: Name of the backup directory.
            progress_callback: Optional callable(count) called per file found.

        Returns:
            List of (relative_path, size_bytes) tuples.
        """
        transport = self._get_transport()
        is_persistent = transport is self._persistent_transport
        try:
            base = self._join_remote(backup_name)

            if self._check_exec_channel(transport):
                return self._list_files_exec(transport, base, progress_callback)

            sftp = self._get_sftp(transport)
            try:
                files: list[tuple[str, int]] = []
                self._list_remote_recursive(sftp, base, "", files)
                return files
            finally:
                sftp.close()
        finally:
            if not is_persistent:
                transport.close()

    def _list_files_exec(
        self,
        transport,
        remote_dir: str,
        progress_callback=None,
    ) -> list[tuple[str, int]]:
        """List files via exec channel using find -printf.

        Streams output line by line for responsive progress tracking.

        Args:
            transport: SSH transport.
            remote_dir: Absolute remote directory path.
            progress_callback: Optional callable(count) per file found.

        Returns:
            List of (relative_path, size_bytes) tuples.
        """
        escaped = _shell_escape(remote_dir)
        channel = transport.open_session()
        try:
            channel.exec_command(f"find {escaped} -type f -printf '%s %P\\n'")

            output = b""
            while True:
                chunk = channel.recv(65536)
                if not chunk:
                    break
                output += chunk

            channel.recv_exit_status()
        finally:
            channel.close()

        files: list[tuple[str, int]] = []
        for line in output.decode("utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split(" ", 1)
            if len(parts) == 2:
                try:
                    size = int(parts[0])
                    rel_path = parts[1]
                    files.append((rel_path, size))
                    if progress_callback:
                        progress_callback(len(files))
                except ValueError:
                    continue
        return files

    def _list_remote_recursive(
        self,
        sftp,
        remote_dir: str,
        prefix: str,
        result: list[tuple[str, int]],
    ) -> None:
        """Recursively list files in a remote directory.

        Args:
            sftp: Open SFTP client.
            remote_dir: Absolute remote directory path.
            prefix: Relative path prefix for results.
            result: Accumulator list for (relative_path, size) tuples.
        """
        import stat as stat_module

        for entry in sftp.listdir_attr(remote_dir):
            rel = f"{prefix}/{entry.filename}" if prefix else entry.filename
            full = f"{remote_dir}/{entry.filename}"
            if stat_module.S_ISDIR(entry.st_mode):
                self._list_remote_recursive(sftp, full, rel, result)
            else:
                result.append((rel, entry.st_size or 0))

    def verify_backup_files(self, backup_name: str) -> list[tuple[str, int, str]]:
        """Verify backup files via sha256sum executed on the SSH server.

        Runs sha256sum in batches via the exec channel, avoiding
        the need to re-download files for local hashing.

        Args:
            backup_name: Name of the backup directory.

        Returns:
            List of (relative_path, size_bytes, sha256_hex) tuples.
        """
        # First get file list with sizes
        file_list = self.list_backup_files(backup_name)
        if not file_list:
            return []

        base = self._join_remote(backup_name)
        transport = self._get_transport()
        is_persistent = transport is self._persistent_transport

        try:
            # Build full remote paths for sha256sum
            remote_paths = [f"{base}/{rel}" for rel, _ in file_list]

            # Run sha256sum in batches to avoid command line length limits
            hash_map: dict[str, str] = {}
            batch_size = 200
            for i in range(0, len(remote_paths), batch_size):
                if self._cancel_check is not None:
                    self._cancel_check()
                batch = remote_paths[i : i + batch_size]
                escaped = " ".join(_shell_escape(p) for p in batch)
                cmd = f"sha256sum {escaped}"

                channel = transport.open_session()
                try:
                    channel.settimeout(60)
                    channel.exec_command(cmd)  # nosec B601
                    output = b""
                    while True:
                        chunk = channel.recv(65536)
                        if not chunk:
                            break
                        output += chunk
                    exit_status = channel.recv_exit_status()
                except Exception as e:
                    logger.warning("sha256sum batch failed: %s", e)
                    channel.close()
                    # Fall back to size-only verification
                    return [(rel, size, "") for rel, size in file_list]
                finally:
                    channel.close()

                if exit_status != 0:
                    logger.warning("sha256sum returned exit code %d", exit_status)
                    return [(rel, size, "") for rel, size in file_list]

                # Parse output: "hash  /path/to/file\n"
                for line in output.decode("utf-8", errors="replace").splitlines():
                    parts = line.split("  ", 1)
                    if len(parts) == 2:
                        h, path = parts
                        hash_map[path.strip()] = h.strip()

            # Build result with hashes
            result: list[tuple[str, int, str]] = []
            for rel, size in file_list:
                full_path = f"{base}/{rel}"
                sha = hash_map.get(full_path, "")
                result.append((rel, size, sha))

            return result

        finally:
            if not is_persistent:
                transport.close()

    def compute_remote_sha256(self, remote_name: str) -> str | None:
        """Compute SHA-256 hash of a single remote file via exec channel.

        Runs ``sha256sum`` on the server to avoid downloading the file.

        Args:
            remote_name: Name of the file (relative to remote_path).

        Returns:
            Hex SHA-256 digest, or None if the command fails.
        """
        full_path = self._join_remote(remote_name)
        transport = self._get_transport()
        is_persistent = transport is self._persistent_transport

        try:
            cmd = f"sha256sum {_shell_escape(full_path)}"
            channel = transport.open_session()
            try:
                channel.settimeout(600)
                channel.exec_command(cmd)  # nosec B601
                output = b""
                while True:
                    chunk = channel.recv(65536)
                    if not chunk:
                        break
                    output += chunk
                exit_status = channel.recv_exit_status()
            finally:
                channel.close()

            if exit_status != 0:
                logger.warning("sha256sum failed for %s (exit %d)", remote_name, exit_status)
                return None

            line = output.decode("utf-8", errors="replace").strip()
            parts = line.split("  ", 1)
            if len(parts) == 2:
                return parts[0].strip()
            return None

        except Exception as e:
            logger.warning("compute_remote_sha256 failed: %s", e)
            return None
        finally:
            if not is_persistent:
                transport.close()

    def download_backup(self, remote_name: str, local_dir: Path) -> Path:
        """Download a backup from SFTP to a local directory.

        If the destination already exists it is removed first so that
        a re-download always starts from a clean state.
        """
        import shutil

        local_dir.mkdir(parents=True, exist_ok=True)
        dst = local_dir / remote_name
        if dst.exists():
            shutil.rmtree(dst, ignore_errors=True)
        dst.mkdir(parents=True, exist_ok=True)

        transport = self._get_transport()
        try:
            sftp = self._get_sftp(transport)
            try:
                remote_base = self._join_remote(remote_name)
                self._sftp_download_dir(sftp, remote_base, dst)

                # Download .wbverify manifest if present
                manifest_remote = self._join_remote(f"{remote_name}.wbverify")
                manifest_local = local_dir / f"{remote_name}.wbverify"
                try:
                    sftp.get(manifest_remote, str(manifest_local))
                    logger.info("Downloaded manifest: %s.wbverify", remote_name)
                except FileNotFoundError:
                    pass  # Older backups may not have manifests
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
                long_path_mkdir(local_path)
                self._sftp_download_dir(sftp, remote_path, local_path)
            else:
                long_path_mkdir(local_path.parent)
                sftp.get(remote_path, long_path_str(local_path))

    def _join_remote(self, name: str) -> str:
        """Join remote base path with a name."""
        return f"{self._remote_path.rstrip('/')}/{name}"
