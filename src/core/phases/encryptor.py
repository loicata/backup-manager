"""Phase 6: Encrypt backup files with AES-256-GCM.

For local backups only. Remote backups are encrypted per-file
during the upload phase (see remote_writer.py).

Only directory backups are supported (ZIP compression has been removed).
"""

import logging
from pathlib import Path
from typing import Optional

from src.core.events import EventBus
from src.core.phase_logger import PhaseLogger
from src.security.encryption import encrypt_file

logger = logging.getLogger(__name__)


def encrypt_backup(
    backup_path: Path,
    password: str,
    events: Optional[EventBus] = None,
) -> Path:
    """Encrypt all files in a backup directory.

    Replaces each file with its .wbenc encrypted version.

    Args:
        backup_path: Path to the backup directory.
        password: Encryption password.
        events: Optional event bus.

    Returns:
        Path to the encrypted backup (same location).

    Raises:
        ValueError: If backup_path is a file instead of a directory.
    """
    if backup_path.is_file():
        raise ValueError(
            f"Expected a directory, got a file: {backup_path}. "
            "Single-file encryption (ZIP) is no longer supported."
        )

    phase_log = PhaseLogger("encryptor", events)

    # Encrypt all files in directory
    files = list(backup_path.rglob("*"))
    files = [f for f in files if f.is_file() and not f.suffix == ".wbenc"]
    total = len(files)

    phase_log.info(f"Encrypting {total} files...")

    for i, filepath in enumerate(files):
        encrypted_path = filepath.with_suffix(filepath.suffix + ".wbenc")
        if encrypt_file(filepath, encrypted_path, password):
            filepath.unlink()  # Remove original
        else:
            phase_log.error(f"Encryption failed: {filepath.name}")

        phase_log.progress(
            current=i + 1,
            total=total,
            filename=filepath.name,
            phase="encryption",
        )

    phase_log.info(f"Encryption complete: {total} files")
    return backup_path
