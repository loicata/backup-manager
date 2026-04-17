"""Phase 2: Filter files for differential backup.

Compares file hashes against a stored manifest to determine
which files have changed and need to be backed up.
"""

import json
import logging
from collections.abc import Callable
from pathlib import Path

from src.core.events import EventBus
from src.core.hashing import compute_sha256
from src.core.phase_logger import PhaseLogger
from src.core.phases.collector import FileInfo

logger = logging.getLogger(__name__)


def load_manifest(manifest_path: Path) -> dict[str, dict]:
    """Load a backup manifest from disk.

    Returns:
        Dict mapping relative_path -> {"hash": str, "size": int, "mtime": float}
    """
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        logger.warning("Could not load manifest: %s", manifest_path)
        return {}


def save_manifest(manifest: dict[str, dict], manifest_path: Path) -> None:
    """Save a backup manifest to disk."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def filter_changed_files(
    files: list[FileInfo],
    manifest_path: Path,
    events: EventBus | None = None,
    cancel_check: Callable[[], None] | None = None,
) -> tuple[list[FileInfo], dict[str, str]]:
    """Filter files that have changed since last backup.

    Compares each file's SHA-256 against the stored manifest.
    Files not in the manifest are always included.

    Args:
        files: All collected files.
        manifest_path: Path to the manifest JSON.
        events: Optional event bus for logging.
        cancel_check: Callable that raises CancelledError if cancelled.

    Returns:
        Tuple of (changed_files, computed_hashes) where computed_hashes
        maps relative_path to SHA-256 hex digest for every file that
        was hashed during filtering.  Downstream phases (manifest,
        delta update) reuse these hashes to avoid redundant I/O.
    """
    phase_log = PhaseLogger("filter", events)

    manifest = load_manifest(manifest_path)
    if not manifest:
        phase_log.info("No previous manifest — full backup")
        return files, {}

    changed = []
    computed_hashes: dict[str, str] = {}
    skipped = 0

    for file_info in files:
        if cancel_check is not None:
            cancel_check()

        key = file_info.relative_path
        prev = manifest.get(key)

        if prev is None:
            # New file
            changed.append(file_info)
            continue

        # Quick check: size changed?
        if prev.get("size") != file_info.size:
            changed.append(file_info)
            continue

        # Deep check: hash changed?
        try:
            current_hash = compute_sha256(file_info.source_path)
            computed_hashes[key] = current_hash
            if current_hash != prev.get("hash", ""):
                changed.append(file_info)
            else:
                skipped += 1
        except OSError as e:
            # Unreadable at filter time — drop from the changed set so
            # the downstream integrity-manifest phase (which is
            # intentionally fail-fast) is not re-attempted on a file
            # it already cannot hash. Previously this path added the
            # file to ``changed`` without a hash entry, causing
            # build_integrity_manifest to crash on the retry.
            # Without a hash we also cannot tell if it changed, so
            # the next run's manifest will still reference the old
            # hash and naturally pick this file up again if it comes
            # back online.
            logger.warning(
                "filter: skipping unreadable file %s: %s",
                file_info.relative_path,
                e,
            )

    phase_log.info(f"Filter: {len(changed)} changed, {skipped} unchanged")
    return changed, computed_hashes


def build_updated_manifest(
    files: list[FileInfo],
    cached_hashes: dict[str, str] | None = None,
    cancel_check=None,
) -> dict[str, dict]:
    """Build a manifest dict from a list of files.

    Args:
        files: Files that were backed up.
        cached_hashes: Optional mapping of relative_path to SHA-256
            from a previous phase (e.g. integrity manifest). When
            provided, avoids re-hashing files already computed.
        cancel_check: Optional callable that raises CancelledError.

    Returns:
        Manifest dict for saving.
    """
    cache = cached_hashes or {}
    manifest = {}
    skipped = 0
    for file_info in files:
        if cancel_check is not None:
            cancel_check()
        try:
            file_hash = cache.get(file_info.relative_path)
            if file_hash is None:
                file_hash = compute_sha256(file_info.source_path)
            manifest[file_info.relative_path] = {
                "hash": file_hash,
                "size": file_info.size,
                "mtime": file_info.mtime,
            }
        except OSError as e:
            # File unreadable at this moment — omit it from the updated
            # manifest so the next run will see it as "new" and retry.
            # Do not swallow silently: log so operators can investigate
            # recurring skips (locked files, broken symlinks, etc.).
            logger.warning(
                "updated-manifest: skipping unreadable file %s: %s",
                file_info.relative_path,
                e,
            )
            skipped += 1
    if skipped:
        logger.info("updated-manifest: %d file(s) skipped due to read errors", skipped)
    return manifest
