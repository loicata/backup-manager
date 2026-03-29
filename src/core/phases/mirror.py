"""Phase 7: Upload backup to mirror destinations.

Mirrors are optional additional copies of the backup.
Each mirror can have independent encryption controlled by
per-mirror boolean flags (encrypt_mirror1, encrypt_mirror2).

All mirrors are attempted even if one fails, but any failure
causes the entire backup to be marked as failed.
"""

import logging
import shutil
from collections.abc import Callable
from pathlib import Path

from src.core.config import StorageConfig, StorageType
from src.core.events import EventBus
from src.core.phase_logger import PhaseLogger
from src.core.phases.collector import FileInfo
from src.core.phases.remote_writer import write_remote

logger = logging.getLogger(__name__)


def mirror_backup(
    backup_path: Path,
    files: list[FileInfo],
    mirror_configs: list[StorageConfig],
    backup_name: str,
    get_backend: callable,
    events: EventBus | None = None,
    encrypt_password: str = "",
    encrypt_flags: list[bool] | None = None,
    cancel_check: Callable[[], None] | None = None,
) -> list[tuple[str, bool, str]]:
    """Upload backup to mirror destinations.

    All mirrors are attempted regardless of individual failures,
    but raises RuntimeError if any mirror fails.

    Args:
        backup_path: Path to the local backup directory.
        files: Original source files (for remote streaming).
        mirror_configs: Mirror storage configurations.
        backup_name: Backup name for remote destinations.
        get_backend: Factory function: StorageConfig -> StorageBackend.
        events: Optional event bus.
        encrypt_password: Encryption password (shared across mirrors).
        encrypt_flags: Per-mirror encryption booleans [mirror1, mirror2].
                       If None or too short, defaults to False.
        cancel_check: Callable that raises CancelledError if cancelled.

    Returns:
        List of (mirror_name, success, message) tuples.

    Raises:
        RuntimeError: If any mirror upload failed.
    """
    phase_log = PhaseLogger("mirror", events)
    results = []
    flags = encrypt_flags or []

    for i, config in enumerate(mirror_configs):
        mirror_name = f"Mirror {i + 1}"
        mirror_desc = _describe_mirror(config)
        phase_log.info(f"Uploading to {mirror_name} — {mirror_desc}...")

        # Check cancel between mirrors
        if cancel_check is not None:
            cancel_check()

        try:
            backend = get_backend(config)

            # Determine if this mirror should be encrypted
            should_encrypt = i < len(flags) and flags[i]
            mirror_pw = encrypt_password if should_encrypt else ""

            has_local_backup = (
                backup_path is not None and backup_path != Path(".") and backup_path.is_dir()
            )

            if config.is_remote():
                # Stream files directly to remote mirror
                write_remote(
                    files,
                    backend,
                    backup_name,
                    encrypt_password=mirror_pw,
                    events=events,
                    cancel_check=cancel_check,
                )
            elif has_local_backup:
                # Local mirror from local backup: copy file-by-file with progress
                _copy_local_mirror(
                    backup_path,
                    backend,
                    backup_name,
                    phase_log,
                    cancel_check,
                )
            else:
                # Local mirror but primary is remote: copy source files
                _write_source_files_to_local(
                    files,
                    backend,
                    backup_name,
                    phase_log,
                    cancel_check,
                )

            results.append((mirror_name, True, "OK"))
            phase_log.info(f"{mirror_name} ({mirror_desc}): upload complete")

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            results.append((mirror_name, False, msg))
            phase_log.error(f"{mirror_name}: upload failed — {msg}")

    # Any mirror failure = backup failure
    failed = [name for name, ok, _ in results if not ok]
    if failed:
        details = "; ".join(f"{name}: {msg}" for name, ok, msg in results if not ok)
        raise RuntimeError(f"Mirror upload failed: {details}")

    return results


def _copy_local_mirror(
    backup_path: Path,
    backend,
    backup_name: str,
    phase_log: PhaseLogger,
    cancel_check: Callable[[], None] | None = None,
) -> None:
    """Copy a local backup to a mirror destination with file-by-file progress.

    Args:
        backup_path: Path to the local backup directory.
        backend: Local/network storage backend.
        backup_name: Backup name for the destination.
        phase_log: Logger for progress events.
        cancel_check: Callable that raises CancelledError if cancelled.
    """
    if not backup_path.is_dir():
        backend.upload(backup_path, backup_name)
        return

    target = Path(backend._dest) / backup_name
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)

    # Collect all files to copy
    source_files = [f for f in backup_path.rglob("*") if f.is_file()]
    total = len(source_files)

    for i, src_file in enumerate(source_files):
        if cancel_check is not None:
            cancel_check()

        rel = src_file.relative_to(backup_path)
        dst_file = target / rel
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_file, dst_file)

        phase_log.progress(
            current=i + 1,
            total=total,
            filename=str(rel),
            phase="mirror_upload",
        )


def _write_source_files_to_local(
    files: list[FileInfo],
    backend,
    backup_name: str,
    phase_log: PhaseLogger,
    cancel_check: Callable[[], None] | None = None,
) -> None:
    """Copy source files to a local mirror when primary storage is remote.

    When the primary backup is on a remote server (SFTP/S3/Proton),
    there is no local backup directory to copy from. Instead, copy
    the original source files directly to the mirror destination,
    preserving relative paths.

    Args:
        files: Source files collected by the pipeline.
        backend: Local/network storage backend.
        backup_name: Backup name for the destination subdirectory.
        phase_log: Logger for progress events.
        cancel_check: Callable that raises CancelledError if cancelled.
    """
    target = Path(backend._dest) / backup_name
    target.mkdir(parents=True, exist_ok=True)

    total = len(files)
    for i, f in enumerate(files):
        if cancel_check is not None:
            cancel_check()

        dst_file = target / f.relative_path
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f.source_path, dst_file)

        phase_log.progress(
            current=i + 1,
            total=total,
            filename=f.relative_path,
            phase="mirror_upload",
        )


def _describe_mirror(config: StorageConfig) -> str:
    """Build a short human-readable label for a mirror destination."""
    st = config.storage_type
    if st == StorageType.LOCAL:
        return f"USB drive {config.destination_path}"
    if st == StorageType.NETWORK:
        return f"Network {config.destination_path}"
    if st == StorageType.SFTP:
        return f"SSH {config.sftp_username}@{config.sftp_host}:{config.sftp_port}"
    if st == StorageType.S3:
        return f"S3 {config.s3_bucket}"
    if st == StorageType.PROTON:
        return f"Proton Drive ({config.proton_username})"
    return config.storage_type.value
