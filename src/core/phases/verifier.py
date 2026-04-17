"""Phase 5: Post-backup verification.

Compares backup contents against the integrity manifest
to ensure no corruption occurred during the backup process.
"""

import json
import logging
from pathlib import Path

from src.core.events import EventBus
from src.core.hashing import compute_sha256
from src.core.phase_logger import PhaseLogger
from src.storage.base import long_path_str

logger = logging.getLogger(__name__)


def verify_backup(
    backup_path: Path,
    manifest_path: Path,
    events: EventBus | None = None,
    cancel_check=None,
) -> tuple[bool, str]:
    """Verify backup contents against manifest.

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

    for i, (rel_path, info) in enumerate(files.items()):
        if cancel_check is not None:
            cancel_check()
        expected_hash = info.get("hash", "")
        file_path = Path(long_path_str(backup_path / rel_path))

        if not file_path.exists():
            errors.append(f"Missing: {rel_path}")
            continue

        try:
            actual_hash = compute_sha256(file_path)
            if actual_hash == expected_hash:
                ok_count += 1
            else:
                errors.append(f"Mismatch: {rel_path}")
        except OSError as e:
            errors.append(f"Read error: {rel_path} ({e})")

        phase_log.progress(
            current=i + 1,
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
