"""Phase 4: Write backup files to destination — flat or encrypted tar.

Supports two modes:
- Plain: flat directory copy (no encryption).
- Encrypted: single .tar.wbenc archive written directly.

INVARIANT (see docs/INVARIANTS.md): for the plain (flat) mode the
byte transfer of every file MUST go through ``shutil.copy2`` so the
copy stays in kernel-space (``CopyFileExW`` on Windows, ``sendfile``
on Linux). The integrity manifest is built BEFORE this phase by
``_phase_integrity``, which hashes every source in parallel via the
manifest module's ``ThreadPoolExecutor``. A previous shape
(v3.3.15-v3.3.18) hashed inside this phase per file, serialising
hash + copy and capping USB throughput at ~8 MB/s on a 30 k-small-
file workload.

Since v3.7.1, ``write_flat`` wraps ``shutil.copy2`` in a
``ThreadPoolExecutor`` of ``WRITE_FLAT_WORKERS`` workers (default 4).
This is *not* the regression of v3.3.15-v3.3.18: the byte transfer
still goes through ``shutil.copy2`` (kernel-space CopyFileExW), only
the *driving loop* is parallel. On the 2026-05-17 bench
(scripts/bench_copy_strategies.py, 7.38 GB / 3,642 files / HDD USB
external) pool4 gained 39% over single-thread, pool8 only +5% over
pool4 — 4 is the empirical sweet spot for the HDD spindle head.
"""

import logging
import os
import shutil
import tarfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from src.core.events import EventBus
from src.core.exceptions import WriteError
from src.core.phase_logger import PhaseLogger
from src.core.phases.collector import FileInfo
from src.storage.base import long_path_mkdir, long_path_str

logger = logging.getLogger(__name__)

# Number of worker threads driving parallel ``shutil.copy2`` calls.
# Picked from scripts/bench_copy_strategies.py on 2026-05-17:
#   single:  28.0 s — 270 MB/s — 1.00x
#   pool4:   20.1 s — 375 MB/s — 1.39x   ← sweet spot
#   pool8:   19.4 s — 389 MB/s — 1.44x   (only +5% over pool4)
# Past 4 workers, the HDD spindle's seek overhead caps the gain; on
# a small-file BLoic-class workload (47 GB / 271 k files, mean 173 KB
# each) the per-file overhead component is larger, so the gain ratio
# should be at least as good as the bench measured.
WRITE_FLAT_WORKERS = 4


def write_flat(
    files: list[FileInfo],
    destination: Path,
    backup_name: str,
    events: EventBus | None = None,
    cancel_check=None,
) -> Path:
    """Write files as a flat directory copy via parallel ``shutil.copy2``.

    A ``ThreadPoolExecutor`` of ``WRITE_FLAT_WORKERS`` workers drives
    the per-file ``shutil.copy2`` calls. Each call still resolves to
    ``CopyFileExW`` on Windows (kernel-space, one syscall sequence) —
    parallelism only multiplexes the *driving loop*, never the byte
    transfer itself (invariant 1). The integrity manifest has already
    been built by ``_phase_integrity`` using parallel hashing, so this
    phase has nothing to do besides feed the kernel.

    Cancellation: ``cancel_check`` is called at the start of every
    file in every worker. The first worker that observes a raised
    ``CancelledError`` propagates it; pending futures are cancelled
    so the pool drains immediately. Workers already mid-copy finish
    their current file (typical: <100 ms for the small files that
    dominate, ~10 s for a 2 GB outlier like a Gmail Takeout mbox).

    Error handling: the first ``WriteError`` raised by any worker
    propagates; the remaining pending futures are cancelled so the
    user does not wait for the rest of the queue. ``as_completed``
    yields futures in completion order, so a fast-failing file is
    surfaced quickly without holding up the bench.

    Args:
        files: Files to back up.
        destination: Base destination path.
        backup_name: Name for this backup (directory name).
        events: Optional event bus.
        cancel_check: Optional callable that raises CancelledError.

    Returns:
        Path to the created backup directory.

    Raises:
        WriteError: If any file fails to copy. Wraps the underlying
            OSError/PermissionError with the offending relative path
            so the caller can surface a precise diagnostic.
        CancelledError: If ``cancel_check`` raises.
    """
    phase_log = PhaseLogger("writer", events)
    backup_dir = destination / backup_name
    long_path_mkdir(backup_dir)

    total = len(files)
    if total == 0:
        # No files to copy — short-circuit before spinning up a pool.
        # Preserves the v3.7.0 "Backup written: 0 files" log line so
        # operators see the same empty-collection signal as before.
        phase_log.info(f"Backup written: 0 files to {backup_dir}")
        return backup_dir

    # Counter shared across workers so PROGRESS sees a monotonically
    # increasing ``current`` regardless of which worker finished a
    # file first. The lock is uncontended off the hot path because
    # PhaseLogger.progress() itself throttles to ~10 Hz (Invariant 5).
    counter_lock = threading.Lock()
    counter = [0]
    # Cross-worker short-circuit: the first worker to raise records
    # the exception here so the *other* workers — when they wake up
    # and look at the next file in their queue — can return early
    # without calling shutil.copy2 again. Without this, the race
    # window between a future being marked done and the main thread
    # observing it via ``as_completed`` lets a worker rotate to the
    # next file and burn a second copy attempt. Pin: with this guard,
    # call_count on a failing 20-file workload stays ≤ workers (4).
    error_lock = threading.Lock()
    first_error: list[BaseException | None] = [None]

    def _copy_one(file_info: FileInfo) -> None:
        # Honour cooperative cancellation at the start of every file
        # so a Cancel click during a 4-worker run is observed within
        # at most one file per worker, not per pool batch.
        if cancel_check is not None:
            cancel_check()
        # Short-circuit if another worker has already failed. Cheap
        # lock acquisition — uncontended in the success path.
        with error_lock:
            if first_error[0] is not None:
                return
        target = backup_dir / file_info.relative_path
        long_path_mkdir(target.parent)

        try:
            # INVARIANT 1: kernel copy. Do NOT replace with a Python loop.
            # Parallelism here multiplexes the *driving loop*; the byte
            # transfer is still CopyFileExW / sendfile in-kernel.
            shutil.copy2(
                long_path_str(file_info.source_path),
                long_path_str(target),
            )
        except (OSError, PermissionError) as e:
            err = WriteError(file_info.relative_path, e)
            with error_lock:
                if first_error[0] is None:
                    first_error[0] = err
            raise err from e

        with counter_lock:
            counter[0] += 1
            current = counter[0]

        phase_log.progress(
            current=current,
            total=total,
            filename=file_info.relative_path,
            phase="backup",
        )

    with ThreadPoolExecutor(
        max_workers=WRITE_FLAT_WORKERS,
        thread_name_prefix="bm-writer",
    ) as ex:
        futures = [ex.submit(_copy_one, f) for f in files]
        observed_error: BaseException | None = None
        for future in as_completed(futures):
            exc = future.exception()
            if exc is not None:
                observed_error = exc
                # Cancel queued-but-not-started futures so the pool
                # drains immediately. Running futures finish their
                # current file — see docstring for the bound. The
                # early-check above ensures they cannot start a *new*
                # file once first_error is set.
                for f in futures:
                    f.cancel()
                break
        if observed_error is not None:
            raise observed_error

    phase_log.info(f"Backup written: {total} files to {backup_dir}")
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
    written. The plain (flat) mode achieves the same end via
    ``_phase_integrity`` (parallel source hash) running BEFORE
    ``write_flat`` (pure ``shutil.copy2``).

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
    cancel_check=None,
) -> Path:
    """Write an encrypted ``.tar.wbenc`` archive.

    Thin wrapper over :func:`write_encrypted_tar_with_hashes` for
    callers that only need the archive path. The integrity manifest
    is always built from hashes computed during streaming and embedded
    in the archive — no caller-supplied manifest is supported, which
    avoids the manifest-then-write TOCTOU window.

    Args:
        files: Files to back up.
        destination: Base destination path.
        backup_name: Name for this backup (becomes ``backup_name.tar.wbenc``).
        password: Encryption password.
        events: Optional event bus.
        cancel_check: Optional callable that raises CancelledError.

    Returns:
        Path to the created ``.tar.wbenc`` file.
    """
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
