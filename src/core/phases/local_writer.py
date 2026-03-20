"""Phase 3a: Write backup to local/network destination.

Writes files as a flat directory copy. ZIP compression has been removed.
"""

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.core.events import EventBus
from src.core.phase_logger import PhaseLogger
from src.core.phases.collector import FileInfo

logger = logging.getLogger(__name__)


def write_flat(
    files: list[FileInfo],
    destination: Path,
    backup_name: str,
    events: Optional[EventBus] = None,
) -> Path:
    """Write files as a flat directory copy.

    Args:
        files: Files to back up.
        destination: Base destination path.
        backup_name: Name for this backup (directory name).
        events: Optional event bus.

    Returns:
        Path to the created backup directory.
    """
    phase_log = PhaseLogger("writer", events)
    backup_dir = destination / backup_name
    backup_dir.mkdir(parents=True, exist_ok=True)

    total = len(files)
    for i, file_info in enumerate(files):
        target = backup_dir / file_info.relative_path
        target.parent.mkdir(parents=True, exist_ok=True)

        try:
            shutil.copy2(file_info.source_path, target)
        except (OSError, PermissionError) as e:
            phase_log.error(f"Error copying {file_info.relative_path}: {e}")

        phase_log.progress(
            current=i + 1,
            total=total,
            filename=file_info.relative_path,
            phase="backup",
        )

    phase_log.info(f"Backup written: {total} files to {backup_dir}")
    return backup_dir


def generate_backup_name(profile_name: str) -> str:
    """Generate a timestamped backup name.

    Returns:
        Name like "ProfileName_2026-03-17_143000"
    """
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    # Sanitize profile name for filesystem
    safe_name = "".join(c if c.isalnum() or c in "-_ " else "_" for c in profile_name)
    safe_name = safe_name.strip().replace(" ", "_")
    return f"{safe_name}_{ts}"
