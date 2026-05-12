"""Phase 5: Post-backup verification.

Compares backup contents against the integrity manifest
to ensure no corruption occurred during the backup process.
"""

import concurrent.futures
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from src.core.events import EventBus
from src.core.hashing import compute_sha256
from src.core.phase_logger import PhaseLogger
from src.core.phases.manifest import _compute_total_checksum
from src.storage.base import long_path_str

logger = logging.getLogger(__name__)


# Same cap as ``src/core/phases/manifest.py``. Past ~8 concurrent
# readers, the Windows file-table mutex serialises the workers and
# extra threads add scheduling overhead without throughput gain.
# Keep the two constants in sync — if you tune one, tune the other.
_VERIFY_WORKERS_MAX = 8

# Verify-phase timeout. Mirror of the manifest-phase budget: a locked
# destination file (antivirus, OneDrive placeholder rehydrating, NAS
# drop) can stall ``read`` indefinitely on Windows. Without a deadline
# the verify phase hangs forever AFTER the bytes were already copied,
# which is the worst possible UX — the backup is intact but the user
# perceives a frozen UI. Keep these in sync with manifest.py constants.
#
# MAX cap rationale: see manifest.py — Windows DWORD millisecond
# wait limit forces an absolute ceiling well below 49 days, and
# 4 h is generous enough for any realistic verify pass.
_VERIFY_TIMEOUT_PER_FILE = 30.0
_VERIFY_TIMEOUT_MIN_SECONDS = 60.0
_VERIFY_TIMEOUT_MAX_SECONDS = 4 * 3600.0  # 4 h hard ceiling


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
        total_timeout = min(
            _VERIFY_TIMEOUT_MAX_SECONDS,
            max(
                _VERIFY_TIMEOUT_MIN_SECONDS,
                len(to_hash) * _VERIFY_TIMEOUT_PER_FILE,
            ),
        )

        # try/finally instead of ``with``: ``with`` calls
        # ``shutdown(wait=True)`` on exit, which would block forever if
        # a re-hash is stuck on a locked backup file. ``cancel_futures=True``
        # cancels pending submissions; in-flight workers continue in
        # the background until the OS releases the lock. The verify
        # phase surfaces the timeout as a verification failure rather
        # than a frozen UI.
        pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="verifier-hash")
        try:
            futures = {pool.submit(compute_sha256, fp): (rel, info) for rel, info, fp in to_hash}
            try:
                for fut in as_completed(futures, timeout=total_timeout):
                    # Cancel must be honoured even mid-pool.
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
            except concurrent.futures.TimeoutError:
                pending = [futures[f][0] for f in futures if not f.done()]
                sample = ", ".join(pending[:5])
                suffix = f" (+{len(pending) - 5} more)" if len(pending) > 5 else ""
                # Surface as a verification error rather than a hard
                # exception: the destination bytes are already on disk,
                # the user still has a backup, we just couldn't verify
                # all of it before the deadline.
                errors.append(
                    f"Verify timed out after {total_timeout:.0f}s with "
                    f"{len(pending)} file(s) still hashing — likely a "
                    f"locked file or unresponsive share. Pending: "
                    f"{sample}{suffix}"
                )
                completed = total  # stop further progress reporting
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

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

    # Recompute the manifest's global ``total_checksum`` and compare it
    # against the value stored in the file. Without this step, an
    # attacker who edits a backup file AND its entry in ``.wbverify``
    # passes the per-file hash comparison silently — the per-file loop
    # only proves the manifest is self-consistent with the bytes on
    # disk, not that the manifest itself was not tampered with. The
    # ``.wbcommit`` HMAC binds ``total_checksum``; recomputing it here
    # closes the loop and turns ``verify_backup`` into a proper
    # end-to-end integrity check rather than a self-attesting one.
    stored_checksum = manifest.get("total_checksum", "")
    if stored_checksum:
        skipped = manifest.get("skipped_files", []) or None
        recomputed = _compute_total_checksum(files, skipped)
        if recomputed != stored_checksum:
            errors.append(
                "Manifest total_checksum mismatch — the .wbverify file "
                "appears to have been edited after the backup was "
                "committed. Refusing to trust per-file results."
            )

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
