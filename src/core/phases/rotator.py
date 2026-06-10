"""Phase 8: Backup rotation and retention.

Uses GFS (Grandfather-Father-Son) policy to keep
daily, weekly, and monthly backups.
"""

import logging
import re
from datetime import UTC, datetime

from src.core.config import RetentionConfig
from src.core.events import EventBus
from src.core.exceptions import StorageDeleteError
from src.core.phase_logger import PhaseLogger
from src.core.phases.local_writer import backup_belongs_to_profile
from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)

# Stable alias to the real datetime class. Tests monkeypatch the
# module-level ``datetime`` name (``patch("...rotator.datetime")``) to
# control ``now``/``fromtimestamp`` for window math, but the name-date
# PARSING below must always use the genuine ``strptime`` — going
# through the patched mock would return a MagicMock and corrupt the
# sort. This alias is not rebound by that patch.
_RealDatetime = datetime

# Type marker anchored on the generated timestamp pattern so profile
# names that happen to contain "_FULL_" or "_DIFF_" (allowed by
# sanitize_profile_name) are not mis-classified.  See
# generate_backup_name() which produces "<profile>_<TYPE>_YYYY-MM-DD_HHMMSS".
_FULL_MARKER = re.compile(r"_FULL_\d{4}-\d{2}-\d{2}_\d{6}")
_DIFF_MARKER = re.compile(r"_DIFF_\d{4}-\d{2}-\d{2}_\d{6}")

# Captures the authoritative timestamp embedded in every generated
# backup name. This is the source of truth for rotation ordering —
# backend ``mtime`` is only a fallback (a server that reports
# st_mtime=0, or a future mtime, otherwise either deletes a valid
# backup or makes one immortal). See ``_parse_backup_datetime``.
_NAME_TIMESTAMP = re.compile(r"_(?:FULL|DIFF)_(\d{4}-\d{2}-\d{2}_\d{6})")


def _parse_backup_datetime(name: str, mtime: float) -> datetime | None:
    """Resolve a backup's timestamp, name first then backend mtime.

    Args:
        name: Backup name, expected to embed ``_<TYPE>_YYYY-MM-DD_HHMMSS``.
        mtime: Backend-reported modification time (epoch seconds); used
            only when the name carries no parseable timestamp.

    Returns:
        A UTC-aware datetime, or None when neither source yields one
        (the caller then KEEPs the backup rather than risk deleting it).
    """
    match = _NAME_TIMESTAMP.search(name)
    if match:
        try:
            dt = _RealDatetime.strptime(match.group(1), "%Y-%m-%d_%H%M%S")
            return dt.replace(tzinfo=UTC)
        except ValueError:
            pass
    if mtime:
        try:
            return _RealDatetime.fromtimestamp(mtime, tz=UTC)
        except (OverflowError, OSError, ValueError):
            pass
    return None


def rotate_backups(
    backend: StorageBackend,
    retention: RetentionConfig,
    events: EventBus | None = None,
    current_backup_name: str = "",
    profile_name: str = "",
    on_deleted=None,
) -> int:
    """Apply GFS retention policy and delete old backups.

    Args:
        backend: Storage backend to manage.
        retention: Retention configuration.
        events: Optional event bus.
        current_backup_name: Name of the backup just created in this
            run.  This backup is always protected from deletion
            regardless of the retention policy outcome.
        profile_name: If set, only rotate backups whose name starts
            with the sanitized profile name prefix.  Backups from
            other profiles are left untouched.

    Returns:
        Number of backups deleted.
    """
    # Object Lock mode — S3 Lifecycle handles cleanup, skip rotation
    if not retention.gfs_enabled:
        phase_log = PhaseLogger("rotation", events)
        phase_log.info("Rotation skipped (Object Lock mode — managed by S3 Lifecycle)")
        return 0

    backups = backend.list_backups()
    if not backups:
        return 0

    if profile_name:
        # Strict name-boundary match (not a bare prefix) so a sibling
        # profile whose name extends this one — "My Backup" vs "My Backup
        # v2" — is never swept into THIS profile's rotation and deleted.
        backups = [b for b in backups if backup_belongs_to_profile(b["name"], profile_name)]
        if not backups:
            return 0

    return _rotate_gfs(backend, backups, retention, events, current_backup_name, on_deleted)


def _is_full_backup(name: str) -> bool:
    """Return True if the backup name matches the FULL type marker.

    The marker must be followed by the generated timestamp
    ``YYYY-MM-DD_HHMMSS`` so that profile names containing ``_FULL_``
    or ``_DIFF_`` substrings are not mis-classified.
    """
    return _FULL_MARKER.search(name) is not None


def _is_diff_backup(name: str) -> bool:
    """Return True if the backup name matches the DIFF type marker.

    The marker must be followed by the generated timestamp
    ``YYYY-MM-DD_HHMMSS`` so that profile names containing ``_DIFF_``
    or ``_FULL_`` substrings are not mis-classified.
    """
    return _DIFF_MARKER.search(name) is not None


def _rotate_gfs(
    backend: StorageBackend,
    backups: list[dict],
    retention: RetentionConfig,
    events: EventBus | None = None,
    current_backup_name: str = "",
    on_deleted=None,
) -> int:
    """GFS rotation: keep daily/weekly/monthly backups.

    For weekly and monthly slots, only FULL backups are eligible.
    This ensures that retained long-term backups are always
    self-contained and restorable without a chain.

    DIFF backups retained by the daily window are always paired with
    their parent FULL to keep the restore chain intact.
    """
    phase_log = PhaseLogger("rotator", events)
    # Everything runs in UTC so that the daylight-saving-time shift
    # (spring-forward and fall-back) cannot cause two backups to land
    # in the same hour with ambiguous ordering, nor make a daily
    # window gain/lose an hour twice a year. Previously both
    # ``datetime.now()`` and ``datetime.fromtimestamp(mtime)`` were
    # naïve local time and drifted around the DST cutover.
    #
    # Tests frequently monkey-patch ``datetime.now`` with a naive value,
    # so normalise here: a naive ``now`` is treated as UTC (same as the
    # normalisation applied to parsed mtimes below).
    now = datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    keep = set()

    # Always protect the backup created in this run (both plain and encrypted)
    if current_backup_name:
        keep.add(current_backup_name)
        keep.add(f"{current_backup_name}.tar.wbenc")

    # Sort by date. The timestamp embedded in the backup NAME is the
    # source of truth; backend mtime is only a fallback. A backup whose
    # date cannot be resolved from either source is KEPT unconditionally
    # (added to ``keep`` with a warning) rather than dropped — the old
    # ``if mtime:`` filter silently excluded mtime=0 backups from every
    # window, sending them straight to ``to_delete`` (a server that
    # omits st_mtime would have every backup deleted).
    dated_backups = []
    for b in backups:
        dt = _parse_backup_datetime(b["name"], b.get("modified", 0))
        if dt is not None:
            dated_backups.append((b, dt))
        else:
            keep.add(b["name"])
            phase_log.warning(
                f"Rotation: keeping {b['name']} — no parseable date in name "
                f"or backend mtime (refusing to delete an undatable backup)"
            )

    dated_backups.sort(key=lambda x: x[1], reverse=True)

    _apply_gfs_windows(dated_backups, retention, now, keep)

    # Always keep the most recent backup
    if dated_backups:
        keep.add(dated_backups[0][0]["name"])

    # Protect FULL parents of any retained DIFF to preserve restore chains
    _protect_full_parents(dated_backups, keep, phase_log)

    # Delete backups not in keep set
    to_delete = [b for b in backups if b["name"] not in keep]
    deleted = _delete_old_backups(backend, to_delete, phase_log, on_deleted=on_deleted)

    # Count only entries that exist in the actual backup list
    backup_names = {b["name"] for b in backups}
    actual_kept = len(keep & backup_names)
    phase_log.info(f"GFS rotation: kept {actual_kept}, deleted {deleted}")
    return deleted


def _apply_gfs_windows(
    dated_backups: list[tuple[dict, datetime]],
    retention: RetentionConfig,
    now: datetime,
    keep: set,
) -> None:
    """Apply daily/weekly/monthly GFS windows to the keep set.

    - Daily slots (3.7.20): grouped by calendar day in UTC. Each day in
      the ``gfs_daily`` window retains the SINGLE most-recent backup of
      that day. Pre-3.7.20 the daily window kept EVERY backup within
      ``gfs_daily`` days, which diverged from the Retention-tab summary
      (``Backups kept: 17`` assumed 1 per day) when the user ran several
      backups on the same calendar day (22/05/2026 report: 18 TestNP
      FULLs across 4 days kept by ``gfs_daily=8``). The new semantics
      mirrors the weekly (1/ISO-week) and monthly (1/month) slots for
      predictability.
    - Weekly slots: grouped by ISO-8601 calendar week. Previously used
      ``strftime("%Y-W%W")`` which is non-ISO and double-counted
      backups straddling a year boundary (Dec 31 = W52 in year Y;
      Jan 1 = W00 in year Y+1 — different keys for the same ISO week).
    """
    # Daily slots — FULL or DIFF, grouped by calendar day, most-recent
    # per day. ``dated_backups`` is already sorted newest-first, so
    # the first occurrence of each calendar-day key is the winner.
    daily_dates: set[tuple[int, int, int]] = set()
    for backup, dt in dated_backups:
        if (now - dt).days >= retention.gfs_daily:
            continue
        day_key = (dt.year, dt.month, dt.day)
        if day_key not in daily_dates:
            daily_dates.add(day_key)
            keep.add(backup["name"])

    # Keep weekly backups (last N weeks) — FULL only, grouped by ISO week
    weekly_dates: set[tuple[int, int]] = set()
    for backup, dt in dated_backups:
        if not _is_full_backup(backup["name"]):
            continue
        if (now - dt).days < retention.gfs_weekly * 7:
            iso = dt.isocalendar()
            week_key = (iso.year, iso.week)
            if week_key not in weekly_dates:
                weekly_dates.add(week_key)
                keep.add(backup["name"])

    # Keep monthly backups (last N months) — FULL only
    monthly_dates: set[str] = set()
    for backup, dt in dated_backups:
        if not _is_full_backup(backup["name"]):
            continue
        months_ago = (now.year - dt.year) * 12 + (now.month - dt.month)
        if months_ago < retention.gfs_monthly:
            month_key = dt.strftime("%Y-%m")
            if month_key not in monthly_dates:
                monthly_dates.add(month_key)
                keep.add(backup["name"])


def _protect_full_parents(
    dated_backups: list[tuple[dict, datetime]],
    keep: set,
    phase_log: PhaseLogger,
) -> None:
    """Add the FULL parent of every retained DIFF to the keep set.

    A DIFF backup can only be restored together with the FULL it was
    computed against.  Without this guard, the daily window can retain
    a DIFF while weekly/monthly windows prune the older FULL that it
    depends on, leaving an orphan DIFF that cannot be restored.

    The parent FULL for a DIFF is the most recent FULL strictly older
    than the DIFF in the same (already profile-filtered) backup list.

    Args:
        dated_backups: Backups sorted by modification time, newest first.
        keep: Set of backup names to preserve; mutated in place.
        phase_log: Logger for protection and orphan events.
    """
    for i, (backup, _dt) in enumerate(dated_backups):
        name = backup["name"]
        if not _is_diff_backup(name) or name not in keep:
            continue
        parent = _find_full_parent(dated_backups, i)
        if parent is None:
            phase_log.warning(f"Retained DIFF {name} has no FULL parent in the backup list")
            continue
        parent_name = parent["name"]
        if parent_name not in keep:
            keep.add(parent_name)
            phase_log.info(
                f"GFS rotation: protected FULL parent {parent_name} " f"for retained DIFF {name}"
            )


def _find_full_parent(
    dated_backups: list[tuple[dict, datetime]],
    diff_index: int,
) -> dict | None:
    """Find the most recent FULL strictly older than the DIFF at diff_index.

    The list is ordered newest first, so the parent sits at a higher
    index than the DIFF.

    Args:
        dated_backups: Backups sorted by modification time, newest first.
        diff_index: Index of the DIFF whose parent to locate.

    Returns:
        The parent FULL backup dict, or None if no earlier FULL exists.
    """
    for j in range(diff_index + 1, len(dated_backups)):
        candidate = dated_backups[j][0]
        if _is_full_backup(candidate["name"]):
            return candidate
    return None


def _delete_old_backups(
    backend: StorageBackend,
    to_delete: list[dict],
    phase_log: PhaseLogger,
    cancel_check=None,
    on_deleted=None,
) -> int:
    """Delete backups not in the keep set, with progress reporting.

    Returns the count of backups *fully* removed.  ``StorageDeleteError``
    means the backend left residuals on disk — those backups are NOT
    counted as deleted, so the rotation summary stays truthful.

    Backups that triggered a ``StorageDeleteError`` are kept on disk
    in their partial state. The next rotation run will attempt
    deletion again; transient causes (antivirus scan, indexer holding
    a handle) usually resolve by then. Persistent residuals indicate
    a real filesystem problem the user must investigate.

    ``on_deleted(name)`` (optional) is invoked after each successful
    deletion so the caller can prune side stores (e.g. the encrypted-
    archive reference hashes in ``verify_hashes.json``). Its failure is
    swallowed — a bookkeeping miss must never abort rotation.
    """
    total = len(to_delete)
    deleted = 0
    failed_with_residuals = 0

    for i, backup in enumerate(to_delete):
        if cancel_check is not None:
            cancel_check()
        try:
            backend.delete_backup(backup["name"])
            deleted += 1
            phase_log.info(f"GFS rotated: deleted {backup['name']}")
            if on_deleted is not None:
                try:
                    on_deleted(backup["name"])
                except Exception as e:
                    logger.debug("on_deleted hook failed for %s: %s", backup["name"], e)
        except StorageDeleteError as e:
            failed_with_residuals += 1
            sample = "; ".join(str(r) for r in e.residuals[:3])
            suffix = f" (+{len(e.residuals) - 3} more)" if len(e.residuals) > 3 else ""
            phase_log.error(
                f"Partial delete on {backup['name']}: {len(e.residuals)} "
                f"residual(s) remain — {sample}{suffix}. "
                f"Will retry on next rotation."
            )
        except Exception as e:
            phase_log.error(f"Failed to delete {backup['name']}: {e}")

        phase_log.progress(
            current=i + 1,
            total=total,
            filename=backup["name"],
            phase="rotation",
        )

    if failed_with_residuals:
        phase_log.warning(
            f"Rotation finished with {failed_with_residuals} partial delete(s) — "
            f"orphan files remain on the destination."
        )

    return deleted
