"""Phase 8: Backup rotation and retention.

Uses GFS (Grandfather-Father-Son) policy to keep
daily, weekly, and monthly backups.
"""

import logging
from datetime import datetime

from src.core.config import RetentionConfig
from src.core.events import EventBus
from src.core.phase_logger import PhaseLogger
from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)


def rotate_backups(
    backend: StorageBackend,
    retention: RetentionConfig,
    events: EventBus | None = None,
    current_backup_name: str = "",
) -> int:
    """Apply GFS retention policy and delete old backups.

    Args:
        backend: Storage backend to manage.
        retention: Retention configuration.
        events: Optional event bus.
        current_backup_name: Name of the backup just created in this
            run.  This backup is always protected from deletion
            regardless of the retention policy outcome.

    Returns:
        Number of backups deleted.
    """
    backups = backend.list_backups()
    if not backups:
        return 0

    return _rotate_gfs(backend, backups, retention, events, current_backup_name)


def _is_full_backup(name: str) -> bool:
    """Check if a backup name contains the FULL marker."""
    return "_FULL_" in name


def _rotate_gfs(
    backend: StorageBackend,
    backups: list[dict],
    retention: RetentionConfig,
    events: EventBus | None = None,
    current_backup_name: str = "",
) -> int:
    """GFS rotation: keep daily/weekly/monthly backups.

    For weekly and monthly slots, only FULL backups are eligible.
    This ensures that retained long-term backups are always
    self-contained and restorable without a chain.
    """
    phase_log = PhaseLogger("rotator", events)
    now = datetime.now()
    keep = set()

    # Always protect the backup created in this run
    if current_backup_name:
        keep.add(current_backup_name)

    # Sort by date
    dated_backups = []
    for b in backups:
        mtime = b.get("modified", 0)
        if mtime:
            dt = datetime.fromtimestamp(mtime)
            dated_backups.append((b, dt))

    dated_backups.sort(key=lambda x: x[1], reverse=True)

    # Keep daily backups (last N days) — any type (full or diff)
    daily_dates = set()
    for backup, dt in dated_backups:
        if (now - dt).days < retention.gfs_daily:
            date_key = dt.strftime("%Y-%m-%d")
            if date_key not in daily_dates:
                daily_dates.add(date_key)
                keep.add(backup["name"])

    # Keep weekly backups (last N weeks) — FULL only
    weekly_dates = set()
    for backup, dt in dated_backups:
        if not _is_full_backup(backup["name"]):
            continue
        if (now - dt).days < retention.gfs_weekly * 7:
            week_key = dt.strftime("%Y-W%W")
            if week_key not in weekly_dates:
                weekly_dates.add(week_key)
                keep.add(backup["name"])

    # Keep monthly backups (last N months) — FULL only
    monthly_dates = set()
    for backup, dt in dated_backups:
        if not _is_full_backup(backup["name"]):
            continue
        months_ago = (now.year - dt.year) * 12 + (now.month - dt.month)
        if months_ago < retention.gfs_monthly:
            month_key = dt.strftime("%Y-%m")
            if month_key not in monthly_dates:
                monthly_dates.add(month_key)
                keep.add(backup["name"])

    # Always keep the most recent backup
    if dated_backups:
        keep.add(dated_backups[0][0]["name"])

    # Delete backups not in keep set
    to_delete = [b for b in backups if b["name"] not in keep]
    total = len(to_delete)
    deleted = 0

    for i, backup in enumerate(to_delete):
        try:
            backend.delete_backup(backup["name"])
            deleted += 1
            phase_log.info(f"GFS rotated: deleted {backup['name']}")
        except Exception as e:
            phase_log.error(f"Failed to delete {backup['name']}: {e}")

        phase_log.progress(
            current=i + 1,
            total=total,
            filename=backup["name"],
            phase="rotation",
        )

    phase_log.info(f"GFS rotation: kept {len(keep)}, deleted {deleted}")
    return deleted
