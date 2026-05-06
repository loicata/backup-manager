"""Phase 4: Create and update backup manifests.

Two types of manifests:
1. Differential manifest (reference from last full backup)
2. Integrity manifest (.wbverify for backup verification)
"""

import hashlib
import io
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import BinaryIO

from src.core.events import EventBus
from src.core.hashing import compute_sha256
from src.core.phase_logger import PhaseLogger
from src.core.phases.collector import FileInfo

logger = logging.getLogger(__name__)


# Worker count cap for parallel hashing. The bottleneck is rarely raw
# CPU — it's per-file overhead (Defender real-time scan, NTFS file
# table mutex, open/close syscalls). Past ~8 concurrent readers, OS
# locks serialise the pool and extra workers add scheduling overhead
# without throughput. The floor of 1 covers the (rare) case where
# ``os.cpu_count()`` returns ``None``.
_HASH_WORKERS_MAX = 8


def _resolve_worker_count() -> int:
    """Pick a sensible thread-pool size for parallel hashing."""
    return max(1, min(_HASH_WORKERS_MAX, os.cpu_count() or 1))


def build_integrity_manifest(
    files: list[FileInfo],
    events: EventBus | None = None,
    cancel_check=None,
    cached_hashes: dict[str, str] | None = None,
) -> dict:
    """Build integrity manifest with hashes of all source files.

    Files without a cached hash are hashed in parallel via a
    ``ThreadPoolExecutor`` (up to ``_HASH_WORKERS_MAX`` workers).
    SHA-256 in ``hashlib`` releases the GIL during the C-level hash
    update, and file I/O syscalls release it during read — so even
    on CPython the threads run concurrently.  On a typical workload
    dominated by Defender real-time scans this lifts throughput
    roughly N×.

    Cache hits are resolved on the main thread (no pool overhead);
    only cache misses hit the worker pool.

    Args:
        files: Files that were backed up.
        events: Optional event bus.
        cancel_check: Optional callable that raises CancelledError.
        cached_hashes: Optional mapping of relative_path to SHA-256
            hex digest from a previous phase (e.g. filter).  Files
            present in this dict skip disk I/O entirely.

    Returns:
        Manifest dict with file hashes and total checksum.
    """
    phase_log = PhaseLogger("manifest", events)
    cache = cached_hashes or {}
    file_hashes: dict = {}
    total = len(files)
    cache_hits = 0
    completed = 0

    # Pass 1: drain the cache eagerly on the main thread.  This avoids
    # paying thread-pool overhead for files whose hash we already know
    # from the filter phase (common on incremental / differential
    # backups where most files are unchanged).
    to_hash: list[FileInfo] = []
    for file_info in files:
        if cancel_check is not None:
            cancel_check()
        cached = cache.get(file_info.relative_path)
        if cached is not None:
            file_hashes[file_info.relative_path] = {
                "hash": cached,
                "size": file_info.size,
            }
            cache_hits += 1
            completed += 1
            phase_log.progress(
                current=completed,
                total=total,
                filename=file_info.relative_path,
                phase="hashing",
            )
        else:
            to_hash.append(file_info)

    # Pass 2: parallel hash the cache misses.
    if to_hash:
        workers = _resolve_worker_count()
        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="manifest-hash",
        ) as pool:
            futures = {
                pool.submit(compute_sha256, fi.source_path): fi for fi in to_hash
            }
            for fut in as_completed(futures):
                # Cancel must be honoured even mid-pool.  Raising here
                # exits the ``with`` block, which calls
                # ``ThreadPoolExecutor.shutdown(wait=True, cancel_futures=False)``
                # — pending futures are cancelled, in-flight ones drain
                # but their results are discarded.
                if cancel_check is not None:
                    cancel_check()
                file_info = futures[fut]
                # Fail-fast: if we cannot hash a file at this stage the
                # resulting manifest would be unverifiable. The filter
                # phase drops unreadable files from ``changed`` so we
                # should never end up here for such a file. ``fut.result()``
                # re-raises the worker's exception.
                file_hash = fut.result()
                file_hashes[file_info.relative_path] = {
                    "hash": file_hash,
                    "size": file_info.size,
                }
                completed += 1
                phase_log.progress(
                    current=completed,
                    total=total,
                    filename=file_info.relative_path,
                    phase="hashing",
                )

    if cache_hits:
        phase_log.info(
            f"Manifest hashing: {cache_hits}/{total} from cache, " f"{total - cache_hits} computed"
        )

    # Total checksum: hash over (path, hash, size) per file.
    # A pure "sorted hashes" checksum was blind to permutations:
    # two files of different content could have their data
    # exchanged without changing the global checksum.
    # Including the path (and size as a second witness) defeats
    # that swap attack.
    total_checksum = _compute_total_checksum(file_hashes)

    phase_log.info(
        f"Manifest created: {len(file_hashes)} files, checksum: {total_checksum[:16]}..."
    )

    return {
        "version": 1,
        "algorithm": "sha256",
        "files": file_hashes,
        "total_checksum": total_checksum,
    }


def _compute_total_checksum(file_hashes: dict, skipped_files: list | None = None) -> str:
    """Compute the integrity manifest's global checksum.

    Produces a sha256 over every file's ``(path, hash, size)`` tuple
    in deterministic sorted order, followed by the ``skipped_files``
    list (sorted) if present. Including the path defeats the
    permutation attack; including the skipped-files list defeats the
    silent-pruning attack where a writer drops an entry and nobody
    notices because the checksum was recomputed.

    Args:
        file_hashes: Dict mapping relative path to ``{"hash": str, "size": int}``.
        skipped_files: Optional list of pruned entries to bind into the
            checksum so their presence is authenticated.

    Returns:
        Hex sha256 digest.
    """
    parts = []
    for rel_path in sorted(file_hashes.keys()):
        entry = file_hashes[rel_path]
        h = entry.get("hash", "")
        size = entry.get("size", 0)
        parts.append(f"{rel_path}\x00{h}\x00{size}")
    if skipped_files:
        parts.append("__skipped__")
        for item in sorted(skipped_files, key=lambda e: e.get("path", "")):
            parts.append(
                f"{item.get('path', '')}\x00"
                f"{item.get('reason', '')}\x00"
                f"{item.get('recorded_hash', '')}\x00"
                f"{item.get('recorded_size', 0)}"
            )
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def prune_manifest_entries(manifest: dict, skipped_rel_paths: set[str]) -> dict:
    """Remove skipped-file entries, RECORD them, and recompute the checksum.

    When the write phase skips a file that the manifest was computed
    against (e.g. the source vanished after hashing, OSError on the
    second open), two things must happen:

    1. The ``files`` entry must be removed or the embedded
       ``.wbverify`` would report "missing" forever on every restore.
    2. The ``skipped_files`` list must be populated so that the
       verifier and the UI can surface the data loss rather than
       hiding it behind a recomputed, consistent-looking checksum.
       Without this field, a silent prune + checksum recompute makes
       the backup report "Verification OK" even though files went
       missing — a textbook "valid signature on partial data" bug.

    The ``skipped_files`` list itself is INCLUDED in the checksum so
    an attacker cannot strip it without producing a mismatch.

    Args:
        manifest: Manifest dict returned by ``build_integrity_manifest``.
        skipped_rel_paths: Set of ``relative_path`` strings that were
            present in the manifest but not actually written.

    Returns:
        The same dict, mutated in place (and also returned for chaining).
        If no entries were skipped, the manifest is returned unchanged.
    """
    if not skipped_rel_paths or not manifest or "files" not in manifest:
        return manifest

    files_dict = manifest["files"]
    removed_any = False
    pruned_entries = []
    for rel in skipped_rel_paths:
        if rel in files_dict:
            pruned_entries.append(
                {
                    "path": rel,
                    "reason": "vanished_during_write",
                    "recorded_hash": files_dict[rel].get("hash", ""),
                    "recorded_size": files_dict[rel].get("size", 0),
                }
            )
            files_dict.pop(rel)
            removed_any = True

    if not removed_any:
        return manifest

    # Record the pruned entries so verifier + UI can surface the loss.
    existing = manifest.get("skipped_files", [])
    manifest["skipped_files"] = existing + pruned_entries

    # Recompute the total checksum over the remaining entries AND the
    # skipped_files list so the manifest stays internally consistent
    # and an attacker cannot hide the skipped entries by editing them out.
    manifest["total_checksum"] = _compute_total_checksum(
        files_dict, skipped_files=manifest["skipped_files"]
    )

    logger.warning(
        "Manifest pruned: %d skipped file(s) removed, checksum recomputed",
        len(skipped_rel_paths),
    )
    return manifest


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
