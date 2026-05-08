"""Phase 3a: Write backup to local/network destination.

Supports two modes:
- Plain: flat directory copy (no encryption).
- Encrypted: single .tar.wbenc archive written directly.

Both modes hash each source file in the same pass that copies/encrypts
it, so the resulting integrity manifest describes exactly the bytes
that landed on the destination. A two-pass implementation (hash first,
copy second) opens a TOCTOU window: if the source is modified between
the two passes, the manifest's hash no longer matches what was written
and the verify phase rejects the entire backup. Single-pass copy +
hash closes that window — see ``src.core.hashing.copy_and_hash``.
"""

import logging
import os
import tarfile
from datetime import datetime
from pathlib import Path

from src.core.events import EventBus
from src.core.exceptions import WriteError
from src.core.hashing import copy_and_hash
from src.core.phase_logger import PhaseLogger
from src.core.phases.collector import FileInfo
from src.storage.base import long_path_mkdir, long_path_str

logger = logging.getLogger(__name__)


def write_flat_with_hashes(
    files: list[FileInfo],
    destination: Path,
    backup_name: str,
    events: EventBus | None = None,
    cancel_check=None,
) -> tuple[Path, dict[str, str]]:
    """Write files as a flat directory copy AND collect SHA-256 per file.

    The hash is computed from the bytes that are actually written to
    the destination, in the same pass as the copy. The returned dict
    can be fed to ``build_integrity_manifest`` via its ``cached_hashes``
    parameter so the pipeline never reads the source a second time —
    this eliminates the manifest→write TOCTOU window that previously
    let live source edits invalidate freshly-written backups.

    Args:
        files: Files to back up.
        destination: Base destination path.
        backup_name: Name for this backup (directory name).
        events: Optional event bus.
        cancel_check: Optional callable that raises CancelledError.

    Returns:
        Tuple ``(backup_dir, file_hashes)`` where ``file_hashes`` maps
        each file's ``relative_path`` to its lowercase hex SHA-256.

    Raises:
        WriteError: If any file fails to copy. Wraps the underlying
            OSError/PermissionError with the offending relative path so
            the caller can surface a precise diagnostic.
    """
    phase_log = PhaseLogger("writer", events)
    backup_dir = destination / backup_name
    long_path_mkdir(backup_dir)

    file_hashes: dict[str, str] = {}
    total = len(files)
    for i, file_info in enumerate(files):
        if cancel_check is not None:
            cancel_check()
        target = backup_dir / file_info.relative_path
        long_path_mkdir(target.parent)

        try:
            digest = copy_and_hash(file_info.source_path, target)
        except (OSError, PermissionError) as e:
            raise WriteError(file_info.relative_path, e) from e

        file_hashes[file_info.relative_path] = digest

        phase_log.progress(
            current=i + 1,
            total=total,
            filename=file_info.relative_path,
            phase="backup",
        )

    phase_log.info(f"Backup written: {total} files to {backup_dir}")
    return backup_dir, file_hashes


def write_flat(
    files: list[FileInfo],
    destination: Path,
    backup_name: str,
    events: EventBus | None = None,
    cancel_check=None,
) -> Path:
    """Write files as a flat directory copy.

    Thin wrapper over :func:`write_flat_with_hashes` that discards the
    hash dictionary. Kept for callers (mostly tests) that don't need
    the per-file hashes — production pipelines should call
    ``write_flat_with_hashes`` directly so the integrity manifest is
    built from the bytes actually written, not a separate source read.

    Args:
        files: Files to back up.
        destination: Base destination path.
        backup_name: Name for this backup (directory name).
        events: Optional event bus.
        cancel_check: Optional callable that raises CancelledError.

    Returns:
        Path to the created backup directory.
    """
    backup_dir, _ = write_flat_with_hashes(
        files,
        destination,
        backup_name,
        events=events,
        cancel_check=cancel_check,
    )
    return backup_dir


class _HashingFileWrapper:
    """Wraps a binary file object and computes SHA-256 of read bytes.

    Used by ``write_encrypted_tar_with_hashes`` so that ``tarfile``'s
    streaming read of the source produces a hash of exactly the bytes
    pushed into the tar — no second source read, no manifest→write
    TOCTOU window.

    Only ``read``, ``readable``, and ``close`` are forwarded — that's
    everything ``tarfile`` calls on a fileobj passed to ``addfile``.
    """

    def __init__(self, fileobj):
        import hashlib

        self._fileobj = fileobj
        self._hasher = hashlib.sha256()

    def read(self, n: int = -1) -> bytes:
        chunk = self._fileobj.read(n)
        if chunk:
            self._hasher.update(chunk)
        return chunk

    def readable(self) -> bool:
        return True

    def close(self) -> None:
        self._fileobj.close()

    def hexdigest(self) -> str:
        return self._hasher.hexdigest()


def write_encrypted_tar_with_hashes(
    files: list[FileInfo],
    destination: Path,
    backup_name: str,
    password: str,
    events: EventBus | None = None,
    cancel_check=None,
) -> tuple[Path, dict[str, str]]:
    """Write an encrypted ``.tar.wbenc`` AND collect SHA-256 per file.

    Hashes are computed from the plaintext bytes that flow through
    ``tarfile.addfile``. The integrity manifest is built from those
    hashes and embedded inside the encrypted archive at the end of the
    streaming session, so the manifest describes exactly what was
    written — closing the same TOCTOU window that ``write_flat_with_hashes``
    closes for the plain mode.

    A file that vanishes between collection and write is skipped (its
    relative path does NOT appear in the returned hash dict and is
    NOT in the embedded manifest). The caller can detect a discrepancy
    between the input ``files`` count and the returned hash count to
    surface the loss in the UI.

    Args:
        files: Files to back up.
        destination: Base destination path.
        backup_name: Name for this backup (becomes ``backup_name.tar.wbenc``).
        password: Encryption password.
        events: Optional event bus.
        cancel_check: Optional callable that raises CancelledError.

    Returns:
        Tuple ``(archive_path, file_hashes)`` where ``file_hashes`` maps
        each successfully-archived file's relative path to its SHA-256
        hex digest (computed from the plaintext bytes).

    Raises:
        WriteError: On any I/O failure during write or rename.
        CancelledError: If ``cancel_check`` raises.
    """
    from src.security.encryption import EncryptingWriter

    phase_log = PhaseLogger("writer", events)
    archive_path = destination / f"{backup_name}.tar.wbenc"
    # Sibling ``.partial`` + atomic rename ensures the final name only
    # appears on success — an interrupted run leaves a ``.partial``
    # which is filtered out by ``LocalStorage.list_backups`` and
    # cleaned up by the orphan scan at the next pipeline start.
    partial_path = archive_path.with_name(archive_path.name + ".partial")
    total = len(files)

    file_hashes: dict[str, str] = {}
    file_sizes: dict[str, int] = {}

    try:
        with open(partial_path, "wb") as out_file:
            enc_writer = EncryptingWriter(out_file, password)
            with tarfile.open(fileobj=enc_writer, mode="w|") as tar:
                for i, file_info in enumerate(files):
                    if cancel_check is not None:
                        cancel_check()
                    src_path = long_path_str(file_info.source_path)
                    try:
                        actual_size = os.path.getsize(src_path)
                    except OSError:
                        logger.warning(
                            "File vanished, skipping: %s",
                            file_info.relative_path,
                        )
                        continue
                    info = tarfile.TarInfo(name=file_info.relative_path)
                    info.size = actual_size
                    with open(src_path, "rb") as f:
                        wrapper = _HashingFileWrapper(f)
                        tar.addfile(info, fileobj=wrapper)
                    file_hashes[file_info.relative_path] = wrapper.hexdigest()
                    file_sizes[file_info.relative_path] = actual_size
                    phase_log.progress(
                        current=i + 1,
                        total=total,
                        filename=file_info.relative_path,
                        phase="backup",
                    )

                # Build the integrity manifest from the hashes we just
                # computed and embed it inside the archive. Total
                # checksum is computed via the same helper that the
                # sidecar ``.wbverify`` uses, so embedded and sidecar
                # manifests are byte-identical (apart from JSON
                # whitespace) for the same set of inputs.
                from src.core.phases.manifest import _compute_total_checksum

                manifest = {
                    "version": 1,
                    "algorithm": "sha256",
                    "files": {
                        rel: {"hash": h, "size": file_sizes[rel]} for rel, h in file_hashes.items()
                    },
                }
                manifest["total_checksum"] = _compute_total_checksum(manifest["files"])
                _add_manifest_to_tar(tar, manifest)

            enc_writer.close()
        os.replace(partial_path, archive_path)
    except (OSError, PermissionError) as e:
        raise WriteError("encrypted-tar", e) from e
    finally:
        # If os.replace succeeded, partial_path is gone. Any exception
        # path (OSError, cancellation, tar error) leaves a truncated
        # file on disk; remove it so it cannot be mistaken for a valid
        # backup or waste destination quota.
        if partial_path.exists():
            try:
                partial_path.unlink()
            except OSError as cleanup_err:
                logger.warning(
                    "Failed to remove partial archive %s: %s",
                    partial_path,
                    cleanup_err,
                )

    phase_log.info(f"Encrypted backup written: {len(file_hashes)} files to {archive_path.name}")
    return archive_path, file_hashes


def write_encrypted_tar(
    files: list[FileInfo],
    destination: Path,
    backup_name: str,
    password: str,
    events: EventBus | None = None,
    integrity_manifest: dict | None = None,
    cancel_check=None,
) -> Path:
    """Write an encrypted ``.tar.wbenc`` archive.

    Thin wrapper over :func:`write_encrypted_tar_with_hashes` for
    callers that only need the archive path.

    The legacy ``integrity_manifest`` parameter is **ignored**. The
    writer now builds its own manifest from hashes computed during
    streaming, so any pre-built manifest passed by the caller would
    describe the source as it was *before* the write started — exactly
    the TOCTOU situation the single-pass writer is here to fix. The
    parameter is kept on the signature for backwards-compatibility
    with existing test callers; production callers should use
    ``write_encrypted_tar_with_hashes`` directly.

    Args:
        files: Files to back up.
        destination: Base destination path.
        backup_name: Name for this backup (becomes ``backup_name.tar.wbenc``).
        password: Encryption password.
        events: Optional event bus.
        integrity_manifest: Ignored. See note above.
        cancel_check: Optional callable that raises CancelledError.

    Returns:
        Path to the created ``.tar.wbenc`` file.
    """
    if integrity_manifest is not None:
        logger.debug(
            "write_encrypted_tar: ignoring caller-supplied integrity_manifest "
            "— writer builds its own from hash-during-write"
        )
    archive_path, _ = write_encrypted_tar_with_hashes(
        files,
        destination,
        backup_name,
        password,
        events=events,
        cancel_check=cancel_check,
    )
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
