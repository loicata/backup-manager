"""Phase 7: Upload backup to mirror destinations.

Mirrors are optional additional copies of the backup.
Each mirror can have independent encryption controlled by
per-mirror boolean flags (encrypt_mirror1, encrypt_mirror2).
"""

import logging
from pathlib import Path
from typing import Callable, Optional

from src.core.config import StorageConfig
from src.core.events import EventBus
from src.core.phase_logger import PhaseLogger
from src.core.phases.collector import FileInfo
from src.core.phases.remote_writer import write_remote
from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)


def mirror_backup(
    backup_path: Path,
    files: list[FileInfo],
    mirror_configs: list[StorageConfig],
    backup_name: str,
    get_backend: callable,
    events: Optional[EventBus] = None,
    encrypt_password: str = "",
    encrypt_flags: Optional[list[bool]] = None,
    cancel_check: Optional[Callable[[], None]] = None,
) -> list[tuple[str, bool, str]]:
    """Upload backup to mirror destinations.

    Args:
        backup_path: Path to the local backup directory.
        files: Original source files (for remote streaming).
        mirror_configs: Mirror storage configurations.
        backup_name: Backup name for remote destinations.
        get_backend: Factory function: StorageConfig -> StorageBackend.
        events: Optional event bus.
        encrypt_password: Encryption password (shared across mirrors).
        encrypt_flags: Per-mirror encryption booleans [mirror1, mirror2].
                       If None or too short, defaults to False.
        cancel_check: Callable that raises CancelledError if cancelled.

    Returns:
        List of (mirror_name, success, message) tuples.
    """
    phase_log = PhaseLogger("mirror", events)
    results = []
    flags = encrypt_flags or []

    for i, config in enumerate(mirror_configs):
        mirror_name = f"Mirror {i + 1}"
        phase_log.info(f"Uploading to {mirror_name}...")

        # Check cancel between mirrors
        if cancel_check is not None:
            cancel_check()

        try:
            backend = get_backend(config)

            # Determine if this mirror should be encrypted
            should_encrypt = i < len(flags) and flags[i]
            mirror_pw = encrypt_password if should_encrypt else ""

            if config.is_remote():
                # Stream files directly to remote mirror
                write_remote(
                    files,
                    backend,
                    backup_name,
                    encrypt_password=mirror_pw,
                    events=events,
                    cancel_check=cancel_check,
                )
            else:
                # Local/network mirror: copy the backup
                backend.upload(backup_path, backup_name)

            results.append((mirror_name, True, "OK"))
            phase_log.info(f"{mirror_name}: upload complete")

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            results.append((mirror_name, False, msg))
            phase_log.error(f"{mirror_name}: upload failed — {msg}")

    return results
