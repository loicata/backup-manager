"""Phase 5: Post-backup verification.

Compares backup contents against the integrity manifest
to ensure no corruption occurred during the backup process.
"""

import json
import logging
from pathlib import Path
from typing import Optional

from src.core.events import EventBus
from src.core.hashing import compute_sha256
from src.core.phase_logger import PhaseLogger

logger = logging.getLogger(__name__)


def verify_backup(
    backup_path: Path,
    manifest_path: Path,
    events: Optional[EventBus] = None,
) -> tuple[bool, str]:
    """Verify backup contents against manifest.

    Args:
        backup_path: Path to the backup directory.
        manifest_path: Path to the .wbverify file.
        events: Optional event bus.

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

    for i, (rel_path, info) in enumerate(files.items()):
        expected_hash = info.get("hash", "")
        file_path = backup_path / rel_path

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
            current=i + 1, total=total,
            filename=rel_path, phase="verification",
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
