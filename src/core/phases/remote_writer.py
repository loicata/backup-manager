"""Phase 3b: Stream files to remote destination (SFTP/S3/Proton).

No temp files, no ZIP — each file is uploaded individually,
or as a single tar stream when the backend supports it.
Encryption uses a temporary encrypted file to avoid loading
the entire plaintext into memory.
"""

import logging
import tempfile
from collections.abc import Callable
from pathlib import Path

from src.core.events import EventBus
from src.core.exceptions import WriteError
from src.core.phase_logger import PhaseLogger
from src.core.phases.collector import FileInfo
from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024  # 1 MB


def write_remote(
    files: list[FileInfo],
    backend: StorageBackend,
    backup_name: str,
    encrypt_password: str = "",
    events: EventBus | None = None,
    cancel_check: Callable[[], None] | None = None,
) -> str:
    """Stream files one by one to a remote storage backend.

    When the backend supports tar-stream upload and no per-file
    encryption is needed, all files are sent in a single tar
    archive for dramatically better small-file performance.

    Args:
        files: Files to back up.
        backend: Storage backend (SFTP, S3, Proton).
        backup_name: Name for this backup (remote directory).
        encrypt_password: If set, encrypt each file before upload.
        events: Optional event bus.
        cancel_check: Callable that raises CancelledError if cancelled.

    Returns:
        Remote backup name (directory name on the server).

    Raises:
        CancelledError: If cancelled by user between files.
    """
    phase_log = PhaseLogger("remote_writer", events)
    total = len(files)

    phase_log.info(f"Uploading {total} files to remote...")

    # Open persistent connection for batch upload (1 connection, N files)
    if hasattr(backend, "connect"):
        backend.connect()

    try:
        # Check cancel before starting
        if cancel_check is not None:
            cancel_check()

        use_tar = not encrypt_password and getattr(backend, "supports_tar_stream", False) is True

        if use_tar:
            _upload_tar_batch(backend, files, backup_name, phase_log, cancel_check)
        else:
            _upload_file_by_file(
                backend,
                files,
                backup_name,
                encrypt_password,
                phase_log,
                cancel_check,
            )
    finally:
        # Always close persistent connection
        if hasattr(backend, "disconnect"):
            backend.disconnect()

    phase_log.info(f"Remote upload done: {total}/{total} files")
    return backup_name


def _upload_tar_batch(
    backend,
    files: list[FileInfo],
    backup_name: str,
    phase_log: PhaseLogger,
    cancel_check: Callable[[], None] | None = None,
) -> None:
    """Upload all files as a single tar stream.

    Progress is tracked by bytes sent rather than file count,
    so the bar advances smoothly during the transfer.

    Args:
        backend: Storage backend with upload_tar_stream() method.
        files: Files to upload.
        backup_name: Remote directory name.
        phase_log: Phase logger for progress events.
        cancel_check: Optional callable that raises CancelledError.
    """
    total_bytes = sum(f.size for f in files)

    # Use bytes for smooth progress (avoids a frozen bar during transfer)
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

    # Emit final progress (ensure 100% of this phase)
    phase_log.progress(current=progress_total, total=progress_total, filename="", phase="upload")


def _upload_file_by_file(
    backend: StorageBackend,
    files: list[FileInfo],
    backup_name: str,
    encrypt_password: str,
    phase_log: PhaseLogger,
    cancel_check: Callable[[], None] | None,
) -> None:
    """Upload files one by one (original behaviour).

    Args:
        backend: Storage backend.
        files: Files to upload.
        backup_name: Remote directory name.
        encrypt_password: If set, encrypt each file before upload.
        phase_log: Phase logger for progress events.
        cancel_check: Callable that raises CancelledError if cancelled.
    """
    total = len(files)
    for i, file_info in enumerate(files):
        if cancel_check is not None:
            cancel_check()

        remote_path = f"{backup_name}/{file_info.relative_path}"

        try:
            if encrypt_password:
                _upload_encrypted(backend, file_info, remote_path, encrypt_password)
            else:
                _upload_plain(backend, file_info, remote_path)
        except Exception as e:
            raise WriteError(file_info.relative_path, e) from e

        phase_log.progress(
            current=i + 1,
            total=total,
            filename=file_info.relative_path,
            phase="upload",
        )


def _upload_plain(
    backend: StorageBackend,
    file_info: FileInfo,
    remote_path: str,
) -> None:
    """Upload a file as-is (no encryption)."""
    with open(file_info.source_path, "rb") as f:
        backend.upload_file(f, remote_path, size=file_info.size)


def _upload_encrypted(
    backend: StorageBackend,
    file_info: FileInfo,
    remote_path: str,
    password: str,
) -> None:
    """Encrypt a file to a temp file, then stream-upload it.

    Uses encrypt_file() which handles chunked I/O internally,
    avoiding loading the entire plaintext into memory.
    """
    from src.security.encryption import encrypt_file

    encrypted_path = remote_path + ".wbenc"

    with tempfile.NamedTemporaryFile(suffix=".wbenc", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        if not encrypt_file(file_info.source_path, tmp_path, password):
            raise RuntimeError(f"Encryption failed for {file_info.relative_path}")

        enc_size = tmp_path.stat().st_size
        with open(tmp_path, "rb") as f:
            backend.upload_file(f, encrypted_path, size=enc_size)
    finally:
        # Always clean up temp file
        try:
            tmp_path.unlink()
        except OSError:
            pass
