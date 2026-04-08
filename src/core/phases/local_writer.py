"""Phase 3a: Write backup to local/network destination.

Supports two modes:
- Plain: flat directory copy (no encryption).
- Encrypted: single .tar.wbenc archive written directly.
"""

import logging
import shutil
import tarfile
from datetime import datetime
from pathlib import Path

from src.core.events import EventBus
from src.core.exceptions import WriteError
from src.core.phase_logger import PhaseLogger
from src.core.phases.collector import FileInfo
from src.storage.base import long_path_mkdir, long_path_str

logger = logging.getLogger(__name__)


def write_flat(
    files: list[FileInfo],
    destination: Path,
    backup_name: str,
    events: EventBus | None = None,
) -> Path:
    """Write files as a flat directory copy.

    Args:
        files: Files to back up.
        destination: Base destination path.
        backup_name: Name for this backup (directory name).
        events: Optional event bus.

    Returns:
        Path to the created backup directory.
    """
    phase_log = PhaseLogger("writer", events)
    backup_dir = destination / backup_name
    long_path_mkdir(backup_dir)

    total = len(files)
    for i, file_info in enumerate(files):
        target = backup_dir / file_info.relative_path
        long_path_mkdir(target.parent)

        try:
            shutil.copy2(long_path_str(file_info.source_path), long_path_str(target))
        except (OSError, PermissionError) as e:
            raise WriteError(file_info.relative_path, e) from e

        phase_log.progress(
            current=i + 1,
            total=total,
            filename=file_info.relative_path,
            phase="backup",
        )

    phase_log.info(f"Backup written: {total} files to {backup_dir}")
    return backup_dir


def write_encrypted_tar(
    files: list[FileInfo],
    destination: Path,
    backup_name: str,
    password: str,
    events: EventBus | None = None,
    integrity_manifest: dict | None = None,
) -> Path:
    """Write files as an encrypted .tar.wbenc archive.

    Creates a single encrypted file directly on the destination.
    No plaintext files are written at any point.  The integrity
    manifest (.wbverify) is included inside the archive so it is
    available after decryption for post-restore verification.

    Args:
        files: Files to back up.
        destination: Base destination path.
        backup_name: Name for this backup (becomes backup_name.tar.wbenc).
        password: Encryption password.
        events: Optional event bus.
        integrity_manifest: Optional manifest dict to embed in the archive.

    Returns:
        Path to the created .tar.wbenc file.
    """
    from src.security.encryption import EncryptingWriter

    phase_log = PhaseLogger("writer", events)
    archive_path = destination / f"{backup_name}.tar.wbenc"
    total = len(files)

    try:
        with open(archive_path, "wb") as out_file:
            enc_writer = EncryptingWriter(out_file, password)
            with tarfile.open(fileobj=enc_writer, mode="w|") as tar:
                for i, file_info in enumerate(files):
                    info = tarfile.TarInfo(name=file_info.relative_path)
                    info.size = file_info.size
                    with open(long_path_str(file_info.source_path), "rb") as f:
                        tar.addfile(info, fileobj=f)
                    phase_log.progress(
                        current=i + 1,
                        total=total,
                        filename=file_info.relative_path,
                        phase="backup",
                    )

                # Embed integrity manifest inside the encrypted archive
                if integrity_manifest:
                    _add_manifest_to_tar(tar, integrity_manifest)

            enc_writer.close()
    except (OSError, PermissionError) as e:
        raise WriteError("encrypted-tar", e) from e

    phase_log.info(f"Encrypted backup written: {total} files to {archive_path.name}")
    return archive_path


def _add_manifest_to_tar(tar: tarfile.TarFile, manifest: dict) -> None:
    """Add integrity manifest as .wbverify entry inside a tar archive.

    Args:
        tar: Open tarfile in write mode.
        manifest: Manifest dict to serialize as JSON.
    """
    import io
    import json

    data = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    info = tarfile.TarInfo(name=".wbverify")
    info.size = len(data)
    tar.addfile(info, io.BytesIO(data))


def sanitize_profile_name(profile_name: str) -> str:
    """Sanitize a profile name for use in backup filenames.

    Args:
        profile_name: Human-readable profile name.

    Returns:
        Filesystem-safe name with only alphanumeric, hyphen, and underscore.

    Raises:
        ValueError: If the sanitized name is empty.
    """
    safe = "".join(c if c.isalnum() or c in "-_ " else "_" for c in profile_name)
    safe = safe.strip().replace(" ", "_")
    if not safe:
        raise ValueError(f"Profile name produces empty sanitized result: {profile_name!r}")
    return safe


def generate_backup_name(profile_name: str, backup_type: str = "FULL") -> str:
    """Generate a timestamped backup name with type marker.

    Args:
        profile_name: Human-readable profile name.
        backup_type: "FULL" or "DIFF" marker in the name.

    Returns:
        Name like "ProfileName_FULL_2026-03-17_143000"
    """
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    safe_name = sanitize_profile_name(profile_name)
    tag = "FULL" if backup_type != "DIFF" else "DIFF"
    return f"{safe_name}_{tag}_{ts}"
