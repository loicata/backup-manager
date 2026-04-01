"""Phase 3b: Stream files to remote destination (SFTP/S3).

No temp files, no ZIP — files are streamed as a tar archive.
When encryption is enabled, the tar stream is encrypted on the fly
using chunked AES-256-GCM (.tar.wbenc format) with a single key
derivation for maximum throughput.
"""

import contextlib
import logging
import os
import tarfile
import threading
from collections.abc import Callable

from src.core.events import EventBus
from src.core.exceptions import WriteError
from src.core.phase_logger import PhaseLogger
from src.core.phases.collector import FileInfo
from src.storage.base import StorageBackend, long_path_str

logger = logging.getLogger(__name__)


def write_remote(
    files: list[FileInfo],
    backend: StorageBackend,
    backup_name: str,
    encrypt_password: str = "",
    events: EventBus | None = None,
    cancel_check: Callable[[], None] | None = None,
    integrity_manifest: dict | None = None,
) -> str:
    """Stream files to a remote storage backend.

    Selects the best upload strategy:
    - Encrypted: tar + chunked AES-256-GCM → single .tar.wbenc file
    - Unencrypted + tar support: tar stream via exec channel
    - Unencrypted fallback: file-by-file upload

    Args:
        files: Files to back up.
        backend: Storage backend (SFTP, S3).
        backup_name: Name for this backup (remote directory or file).
        encrypt_password: If set, encrypt the tar stream.
        events: Optional event bus.
        cancel_check: Callable that raises CancelledError if cancelled.

    Returns:
        Remote backup name.
    """
    phase_log = PhaseLogger("remote_writer", events)
    total = len(files)
    phase_log.info(f"Uploading {total} files to remote...")

    if hasattr(backend, "connect"):
        backend.connect()

    try:
        if cancel_check is not None:
            cancel_check()

        use_tar = getattr(backend, "supports_tar_stream", False) is True

        logger.info(
            "Remote write: encrypt=%s, use_tar=%s, files=%d",
            bool(encrypt_password),
            use_tar,
            len(files),
        )

        if encrypt_password:
            _upload_encrypted_tar(
                backend,
                files,
                backup_name,
                encrypt_password,
                phase_log,
                cancel_check,
                integrity_manifest,
            )
        elif use_tar:
            _upload_tar_batch(
                backend,
                files,
                backup_name,
                phase_log,
                cancel_check,
            )
        else:
            _upload_file_by_file(
                backend,
                files,
                backup_name,
                phase_log,
                cancel_check,
            )
    finally:
        if hasattr(backend, "disconnect"):
            backend.disconnect()

    phase_log.info(f"Remote upload done: {total}/{total} files")
    return backup_name


def _upload_encrypted_tar(
    backend: StorageBackend,
    files: list[FileInfo],
    backup_name: str,
    password: str,
    phase_log: PhaseLogger,
    cancel_check: Callable[[], None] | None = None,
    integrity_manifest: dict | None = None,
) -> None:
    """Upload files as an encrypted tar archive (.tar.wbenc).

    For backends that support streaming (SFTP with exec channel),
    uses os.pipe() + producer thread for zero temp files.
    For backends that need seekable streams (S3), writes to a temp
    file first then uploads.

    Args:
        backend: Storage backend with upload_file() method.
        files: Files to upload.
        backup_name: Remote backup name (becomes backup_name.tar.wbenc).
        password: Encryption password.
        phase_log: Phase logger for progress events.
        cancel_check: Optional callable that raises CancelledError.
        integrity_manifest: Optional manifest dict to embed in the archive.
    """
    supports_pipe = getattr(backend, "supports_tar_stream", False)

    if supports_pipe:
        _upload_encrypted_tar_pipe(
            backend,
            files,
            backup_name,
            password,
            phase_log,
            cancel_check,
            integrity_manifest,
        )
    else:
        _upload_encrypted_tar_tempfile(
            backend,
            files,
            backup_name,
            password,
            phase_log,
            cancel_check,
            integrity_manifest,
        )


def _build_encrypted_tar(
    dest,
    files: list[FileInfo],
    password: str,
    phase_log: PhaseLogger,
    cancel_check: Callable[[], None] | None,
    integrity_manifest: dict | None,
) -> int:
    """Build an encrypted tar into *dest* (file-like writable).

    Args:
        dest: Writable binary stream.
        files: Files to include.
        password: Encryption password.
        phase_log: Phase logger.
        cancel_check: Optional cancel callable.
        integrity_manifest: Optional manifest to embed.

    Returns:
        Total source bytes written.
    """
    from src.core.phases.local_writer import _add_manifest_to_tar
    from src.security.encryption import EncryptingWriter

    total_bytes = sum(f.size for f in files)
    progress_total = max(total_bytes, 1)
    bytes_written = 0

    enc_writer = EncryptingWriter(dest, password)
    with tarfile.open(fileobj=enc_writer, mode="w|") as tar:
        for file_info in files:
            if cancel_check is not None:
                cancel_check()
            info = tarfile.TarInfo(name=file_info.relative_path)
            info.size = file_info.size
            with open(long_path_str(file_info.source_path), "rb") as f:
                tar.addfile(info, fileobj=f)
            bytes_written += file_info.size
            phase_log.progress(
                current=bytes_written,
                total=progress_total,
                filename=file_info.relative_path,
                phase="upload",
            )
        if integrity_manifest:
            _add_manifest_to_tar(tar, integrity_manifest)
    enc_writer.close()
    return bytes_written


def _upload_encrypted_tar_pipe(
    backend,
    files,
    backup_name,
    password,
    phase_log,
    cancel_check,
    integrity_manifest,
) -> None:
    """Stream encrypted tar via pipe (SFTP and other streaming backends)."""
    import contextlib

    read_fd, write_fd = os.pipe()
    read_end = os.fdopen(read_fd, "rb")
    write_end = os.fdopen(write_fd, "wb")

    producer_error: list[BaseException] = []

    def _produce():
        try:
            _build_encrypted_tar(
                write_end,
                files,
                password,
                phase_log,
                cancel_check,
                integrity_manifest,
            )
        except BaseException as e:
            producer_error.append(e)
        finally:
            with contextlib.suppress(OSError):
                write_end.close()

    thread = threading.Thread(target=_produce, daemon=True)
    thread.start()

    upload_error: BaseException | None = None
    try:
        remote_path = f"{backup_name}.tar.wbenc"
        backend.upload_file(read_end, remote_path, size=0)
    except BaseException as e:
        upload_error = e
    finally:
        read_end.close()
        thread.join(timeout=30)

    if upload_error:
        raise WriteError("encrypted-tar", upload_error) from upload_error
    if producer_error:
        raise WriteError("encrypted-tar", producer_error[0]) from producer_error[0]


def _upload_encrypted_tar_tempfile(
    backend,
    files,
    backup_name,
    password,
    phase_log,
    cancel_check,
    integrity_manifest,
) -> None:
    """Write encrypted tar to temp file, then upload (S3 and seekable backends)."""
    import tempfile

    remote_path = f"{backup_name}.tar.wbenc"

    with tempfile.NamedTemporaryFile(suffix=".tar.wbenc", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        with open(tmp_path, "wb") as f:
            _build_encrypted_tar(
                f,
                files,
                password,
                phase_log,
                cancel_check,
                integrity_manifest,
            )
        enc_size = os.path.getsize(tmp_path)
        logger.info(
            "Uploading encrypted temp file: %s (%d bytes)",
            tmp_path,
            enc_size,
        )
        # Use boto3 upload_file (path-based) for S3 to avoid
        # multipart stream issues. Fall back to stream-based for others.
        if hasattr(backend, "_get_client"):
            from boto3.s3.transfer import TransferConfig

            transfer_config = TransferConfig(
                multipart_chunksize=16 * 1024 * 1024,
            )
            # Apply bandwidth throttling if configured
            bw_limit = getattr(backend, "_bandwidth_limit_kbps", 0)
            if bw_limit > 0:
                transfer_config.max_bandwidth = bw_limit * 1024  # KB/s → B/s

            client = backend._get_client()
            key = backend._s3_key(remote_path)
            client.upload_file(
                tmp_path,
                backend._bucket,
                key,
                Config=transfer_config,
            )
        else:
            with open(tmp_path, "rb") as f:
                backend.upload_file(f, remote_path, size=enc_size)
    except Exception as e:
        raise WriteError("encrypted-tar", e) from e
    finally:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)


def _upload_tar_batch(
    backend: StorageBackend,
    files: list[FileInfo],
    backup_name: str,
    phase_log: PhaseLogger,
    cancel_check: Callable[[], None] | None = None,
) -> None:
    """Upload all files as a single unencrypted tar stream.

    Args:
        backend: Storage backend with upload_tar_stream() method.
        files: Files to upload.
        backup_name: Remote directory name.
        phase_log: Phase logger for progress events.
        cancel_check: Optional callable that raises CancelledError.
    """
    total_bytes = sum(f.size for f in files)
    progress_total = max(total_bytes, 1)

    def _on_progress(bytes_sent: int, _total: int) -> None:
        phase_log.progress(
            current=bytes_sent,
            total=progress_total,
            filename="",
            phase="upload",
        )

    tar_files = [(f.source_path, f.relative_path, f.size) for f in files]

    try:
        backend.upload_tar_stream(
            tar_files,
            backup_name,
            progress_callback=_on_progress,
            cancel_check=cancel_check,
        )
    except Exception as e:
        raise WriteError("tar-stream", e) from e

    phase_log.progress(
        current=progress_total,
        total=progress_total,
        filename="",
        phase="upload",
    )


def _upload_file_by_file(
    backend: StorageBackend,
    files: list[FileInfo],
    backup_name: str,
    phase_log: PhaseLogger,
    cancel_check: Callable[[], None] | None,
) -> None:
    """Upload files one by one (fallback for backends without tar support).

    Args:
        backend: Storage backend.
        files: Files to upload.
        backup_name: Remote directory name.
        phase_log: Phase logger for progress events.
        cancel_check: Callable that raises CancelledError if cancelled.
    """
    total = len(files)
    for i, file_info in enumerate(files):
        if cancel_check is not None:
            cancel_check()

        remote_path = f"{backup_name}/{file_info.relative_path}"

        try:
            with open(long_path_str(file_info.source_path), "rb") as f:
                backend.upload_file(f, remote_path, size=file_info.size)
        except Exception as e:
            raise WriteError(file_info.relative_path, e) from e

        phase_log.progress(
            current=i + 1,
            total=total,
            filename=file_info.relative_path,
            phase="upload",
        )
