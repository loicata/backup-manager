"""Phase 5: Post-backup verification.

Compares backup contents against the integrity manifest
to ensure no corruption occurred during the backup process.
"""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from src.core.events import EventBus
from src.core.hashing import compute_sha256
from src.core.phase_logger import PhaseLogger
from src.storage.base import long_path_str

logger = logging.getLogger(__name__)


# Same cap as ``src/core/phases/manifest.py``. Past ~8 concurrent
# readers, the Windows file-table mutex serialises the workers and
# extra threads add scheduling overhead without throughput gain.
# Keep the two constants in sync — if you tune one, tune the other.
_VERIFY_WORKERS_MAX = 8


def _resolve_worker_count() -> int:
    """Pick a sensible thread-pool size for parallel verification."""
    return max(1, min(_VERIFY_WORKERS_MAX, os.cpu_count() or 1))


def verify_backup(
    backup_path: Path,
    manifest_path: Path,
    events: EventBus | None = None,
    cancel_check=None,
) -> tuple[bool, str]:
    """Verify backup contents against manifest.

    Re-hashes every file referenced in the manifest in parallel via a
    ``ThreadPoolExecutor`` (≤``_VERIFY_WORKERS_MAX`` workers).
    ``hashlib.sha256`` releases the GIL during the C-level update and
    file I/O syscalls release it during read, so threads scale
    near-linearly on workloads dominated by Defender real-time scan
    or other AV interception. The legacy sequential loop topped out
    at ~12 MB/s on a 261 K-file backup with Defender active —
    parallelisation lifts it to ~80-100 MB/s.

    Missing-file detection stays on the main thread (cheap existence
    check, no point paying pool overhead for it). Only present files
    go to the pool for hashing + comparison.

    Args:
        backup_path: Path to the backup directory.
        manifest_path: Path to the .wbverify file.
        events: Optional event bus.
        cancel_check: Optional callable that raises CancelledError.

    Returns:
        (success, message) tuple.
    """
    phase_log = PhaseLogger("verifier", events)

    if not manifest_path.exists():
        return True, "No manifest found — skipping verification"

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        return False, f"Could not read manifest: {e}"

    files = manifest.get("files", {})
    total = len(files)
    ok_count = 0
    errors = []
    completed = 0

    # Surface any files that were pruned by the writer (source vanished
    # between hashing and write). Without this, the manifest's
    # recomputed checksum makes the backup look like "Verification OK"
    # even though data was silently dropped.
    skipped = manifest.get("skipped_files", [])
    if skipped:
        for item in skipped[:10]:
            errors.append(
                f"Skipped during write: {item.get('path', '?')}" f" ({item.get('reason', '?')})"
            )
        if len(skipped) > 10:
            errors.append(f"... and {len(skipped) - 10} more skipped file(s)")

    # Pass 1: drain missing files on the main thread.  os.path.exists
    # is cheap; submitting it to a worker would cost more in scheduling
    # than it saves. Files that DO exist are queued for the parallel
    # pool below.
    to_hash: list[tuple[str, dict, Path]] = []
    for rel_path, info in files.items():
        if cancel_check is not None:
            cancel_check()
        file_path = Path(long_path_str(backup_path / rel_path))
        if not file_path.exists():
            errors.append(f"Missing: {rel_path}")
            completed += 1
            phase_log.progress(
                current=completed,
                total=total,
                filename=rel_path,
                phase="verification",
            )
            continue
        to_hash.append((rel_path, info, file_path))

    # Pass 2: parallel re-hash + compare. Pool only spins up when we
    # actually have files to verify — empty / all-missing cases skip
    # the executor overhead entirely.
    if to_hash:
        workers = _resolve_worker_count()
        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="verifier-hash",
        ) as pool:
            futures = {
                pool.submit(compute_sha256, fp): (rel, info)
                for rel, info, fp in to_hash
            }
            for fut in as_completed(futures):
                # Cancel must be honoured even mid-pool. Raising here
                # exits the ``with`` block, which calls
                # ``ThreadPoolExecutor.shutdown(wait=True, cancel_futures=False)``
                # — pending futures are cancelled, in-flight ones drain
                # but their results are discarded.
                if cancel_check is not None:
                    cancel_check()
                rel_path, info = futures[fut]
                expected_hash = info.get("hash", "")
                try:
                    actual_hash = fut.result()
                    if actual_hash == expected_hash:
                        ok_count += 1
                    else:
                        errors.append(f"Mismatch: {rel_path}")
                except OSError as e:
                    errors.append(f"Read error: {rel_path} ({e})")
                completed += 1
                phase_log.progress(
                    current=completed,
                    total=total,
                    filename=rel_path,
                    phase="verification",
                )

    # Detect unexpected files that the writer left behind but the
    # manifest does not reference. These are typically stale ``.tmp``
    # fragments or incomplete copies. A "verification OK" backup that
    # contains extras can confuse restore tooling and wastes space.
    #
    # IMPORTANT: traverse WITHOUT following directory symlinks.
    # ``Path.rglob("*")`` on Python 3.13 still follows dir symlinks,
    # which causes (a) infinite loops on symlink cycles, and (b) false
    # "Extra" positives for files outside the backup path. Use
    # ``os.walk(followlinks=False)`` and skip symlink files.
    if backup_path.is_dir():
        import os as _os

        expected_paths = {str(rel).replace("\\", "/") for rel in files}
        # Skip OS noise and the manifest itself. Also skip NAS/Synology
        # metadata (@eaDir) and macOS Spotlight/Trashes directories so
        # mounting a backup on one of these systems doesn't trip a
        # wave of false-positive "Extra" alerts.
        _IGNORED_NAMES = {
            ".DS_Store",
            "Thumbs.db",
            ".wbverify",
            "desktop.ini",
            "@eaDir",
            ".Spotlight-V100",
            ".Trashes",
        }
        extras: list[str] = []
        for root, dirs, disk_files in _os.walk(str(backup_path), followlinks=False):
            # Also prune ignored directories in-place so recursion
            # doesn't descend into NAS metadata.
            dirs[:] = [d for d in dirs if d not in _IGNORED_NAMES]
            for name in disk_files:
                if name in _IGNORED_NAMES:
                    continue
                full = Path(root) / name
                # Skip symlinks (dangling or not) — they are not
                # authentic backup content and a cycle would hang us.
                if full.is_symlink():
                    continue
                rel = full.relative_to(backup_path).as_posix()
                if rel not in expected_paths:
                    extras.append(rel)
        if extras:
            for rel in extras[:10]:
                errors.append(f"Extra: {rel}")
            if len(extras) > 10:
                errors.append(f"... {len(extras) - 10} more extras")

    if errors:
        msg = f"Verification failed: {len(errors)}/{total} errors"
        for err in errors[:10]:
            msg += f"\n  - {err}"
        if len(errors) > 10:
            msg += f"\n  ... and {len(errors) - 10} more"
        phase_log.info(msg)
        return False, msg

    msg = f"Verification OK: {ok_count}/{total} files verified"
    phase_log.info(msg)
    return True, msg
