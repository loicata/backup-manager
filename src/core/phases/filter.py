"""Phase 2: Filter files for incremental/differential backup.

Compares file hashes against a stored manifest to determine
which files have changed and need to be backed up.
"""

import json
import logging
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
) -> list[FileInfo]:
    """Filter files that have changed since last backup.

    Compares each file's SHA-256 against the stored manifest.
    Files not in the manifest are always included.

    Args:
        files: All collected files.
        manifest_path: Path to the manifest JSON.
        events: Optional event bus for logging.

    Returns:
        List of files that need to be backed up.
    """
    phase_log = PhaseLogger("filter", events)

    manifest = load_manifest(manifest_path)
    if not manifest:
        phase_log.info("No previous manifest — full backup")
        return files

    changed = []
    skipped = 0

    for file_info in files:
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
            if current_hash != prev.get("hash", ""):
                changed.append(file_info)
            else:
                skipped += 1
        except OSError:
            changed.append(file_info)  # Can't read = include

    phase_log.info(f"Filter: {len(changed)} changed, {skipped} unchanged")
    return changed


def build_updated_manifest(files: list[FileInfo]) -> dict[str, dict]:
    """Build a manifest dict from a list of files.

    Args:
        files: Files that were backed up.

    Returns:
        Manifest dict for saving.
    """
    manifest = {}
    for file_info in files:
        try:
            file_hash = compute_sha256(file_info.source_path)
            manifest[file_info.relative_path] = {
                "hash": file_hash,
                "size": file_info.size,
                "mtime": file_info.mtime,
            }
        except OSError:
            pass
    return manifest
