"""Phase 4: Create and update backup manifests.

Two types of manifests:
1. Differential manifest (reference from last full backup)
2. Integrity manifest (.wbverify for backup verification)
"""

import hashlib
import io
import json
import logging
from pathlib import Path
from typing import BinaryIO

from src.core.events import EventBus
from src.core.hashing import compute_sha256
from src.core.phase_logger import PhaseLogger
from src.core.phases.collector import FileInfo

logger = logging.getLogger(__name__)


def build_integrity_manifest(
    files: list[FileInfo],
    events: EventBus | None = None,
    cancel_check=None,
) -> dict:
    """Build integrity manifest with hashes of all source files.

    Args:
        files: Files that were backed up.
        events: Optional event bus.
        cancel_check: Optional callable that raises CancelledError.

    Returns:
        Manifest dict with file hashes and total checksum.
    """
    phase_log = PhaseLogger("manifest", events)
    file_hashes = {}
    total = len(files)

    for i, file_info in enumerate(files):
        if cancel_check is not None:
            cancel_check()
        file_hash = compute_sha256(file_info.source_path)
        file_hashes[file_info.relative_path] = {
            "hash": file_hash,
            "size": file_info.size,
        }

        phase_log.progress(
            current=i + 1,
            total=total,
            filename=file_info.relative_path,
            phase="hashing",
        )

    # Total checksum: hash of sorted file hashes
    all_hashes = sorted(file_hashes.get(k, {}).get("hash", "") for k in file_hashes)
    total_checksum = hashlib.sha256("\n".join(all_hashes).encode("utf-8")).hexdigest()

    phase_log.info(
        f"Manifest created: {len(file_hashes)} files, checksum: {total_checksum[:16]}..."
    )

    return {
        "version": 1,
        "algorithm": "sha256",
        "files": file_hashes,
        "total_checksum": total_checksum,
    }


def save_integrity_manifest(manifest: dict, backup_path: Path) -> Path:
    """Save integrity manifest alongside the backup.

    Args:
        manifest: Manifest dict from build_integrity_manifest().
        backup_path: Path to the backup directory.

    Returns:
        Path to the saved .wbverify file.
    """
    manifest_path = backup_path.parent / f"{backup_path.name}.wbverify"

    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("Saved manifest: %s", manifest_path)
    return manifest_path


def upload_manifest_to_remote(
    manifest: dict,
    backend: object,
    backup_name: str,
) -> None:
    """Upload integrity manifest to a remote storage backend.

    The manifest is serialised to JSON and uploaded as
    ``{backup_name}.wbverify`` at the same level as the backup
    directory on the remote destination.

    Args:
        manifest: Manifest dict from build_integrity_manifest().
        backend: Remote StorageBackend instance with upload_file().
        backup_name: Name of the backup directory on the remote.

    Raises:
        OSError: If the upload fails (caller decides error policy).
    """
    if not manifest:
        raise ValueError("Manifest is empty")
    if not backup_name:
        raise ValueError("backup_name must not be empty")

    data = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    buf: BinaryIO = io.BytesIO(data)
    remote_path = f"{backup_name}.wbverify"

    backend.upload_file(buf, remote_path, size=len(data))
    logger.info("Uploaded manifest: %s", remote_path)
