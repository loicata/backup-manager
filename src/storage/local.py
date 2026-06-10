"""Local storage backend for external drives and USB sticks.

Supports flat directory copy and file-by-file streaming.
"""

import contextlib
import logging
import os
import shutil
import threading
import time
from pathlib import Path
from typing import BinaryIO

from src.core.exceptions import StorageDeleteError
from src.core.phases.commit_marker import (
    COMMIT_MARKER_SUFFIX,
    is_backup_committed,
)
from src.storage._fs_utils import RemoveResult, safe_remove_tree
from src.storage.base import StorageBackend, is_backup_sidecar

logger = logging.getLogger(__name__)

# 30s: cumulative wake-up budget (~16s of sleep retries) + antivirus
# scan on the first write on a freshly mounted volume. 20s was enough
# for drives that spun up within ~8s but still tripped on USB SSDs in
# deep power-save that need 10-12s to enumerate.
CONNECTION_TIMEOUT = 30  # seconds

# Stable substring embedded in the error message returned by
# ``test_connection`` when the write probe fails with PermissionError.
# Exposed as a module-level constant (rather than left as a magic string
# inside the message) so the UI's health-poll race guard
# (``BackupManagerApp._on_health_result``) can recognise the precise
# error pattern produced by "writer occupies the drive while the health
# thread races for the I/O queue" without grepping a free-form string.
# Changing this value is a UI-contract change: keep it stable.
READ_ONLY_OR_LOCKED_MARKER = "Destination is read-only or locked"

# Windows system folders that must never be treated as backups
SYSTEM_FOLDERS = frozenset(
    {
        "System Volume Information",
        "$RECYCLE.BIN",
        "RECYCLER",
        "Recovery",
        "found.000",
    }
)


def _log_residuals(target: str, result: RemoveResult) -> None:
    """Emit a single structured warning summarising any residuals.

    The legacy ``_force_remove_readonly`` callback logged one line per
    failing path which spammed the log without surfacing the count or
    the dependency between failures.  We emit one WARNING with the
    total and a sample of the first three paths — enough to act on
    without flooding ``backup_manager.log``.
    """
    if result.success:
        return
    sample = "; ".join(str(r) for r in result.residuals[:3])
    suffix = f" (+{len(result.residuals) - 3} more)" if len(result.residuals) > 3 else ""
    logger.warning(
        "safe_remove_tree left %d residual(s) under %s: %s%s",
        len(result.residuals),
        target,
        sample,
        suffix,
    )


class LocalStorage(StorageBackend):
    """Storage backend for local/external drives."""

    def __init__(self, destination_path: str):
        super().__init__()
        self._dest = Path(destination_path)

    def upload(self, local_path: Path, remote_name: str) -> None:
        """Copy a local file or directory to the destination."""
        target = self._dest / remote_name

        if local_path.is_dir():
            if target.exists():
                # Replace any prior backup at the same name. We use the
                # robust helper (long-path support, retries, attribute
                # clearing) so a leftover read-only or deeply nested
                # tree from a previous run does not abort the upload.
                # Residuals are escalated as StorageDeleteError because
                # copytree below would fail anyway with a confusing
                # "destination already exists" error otherwise.
                result = safe_remove_tree(target)
                if not result.success:
                    _log_residuals(str(target), result)
                    raise StorageDeleteError(remote_name, result.residuals)
            shutil.copytree(local_path, target)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            if self._bandwidth_limit_kbps > 0:
                self._throttled_copy(local_path, target)
            else:
                shutil.copy2(local_path, target)

        logger.info("Uploaded %s -> %s", local_path.name, target)

    def upload_file(self, fileobj: BinaryIO, remote_path: str, size: int = 0) -> None:
        """Write a file-like object to the destination.

        Writes to ``<target>.partial`` first and atomically renames on
        success. Without this, a crash mid-write leaves a corrupted
        file with the final name that ``list_backups`` surfaces as a
        valid archive.
        """
        target = self._dest / remote_path
        target.parent.mkdir(parents=True, exist_ok=True)

        partial = target.with_suffix(target.suffix + ".partial")

        reader = self._get_throttled_reader(fileobj)
        bytes_written = 0
        chunk_size = 1024 * 1024  # 1 MB

        try:
            with open(partial, "wb") as out:
                while True:
                    chunk = reader.read(chunk_size)
                    if not chunk:
                        break
                    out.write(chunk)
                    bytes_written += len(chunk)
                    if self._progress_callback and size > 0:
                        self._progress_callback(bytes_written, size)
            # Atomic rename — the final name only appears on success.
            os.replace(partial, target)
        except BaseException:
            # Best-effort cleanup of the partial file on any failure
            # (exception, cancel). Swallow errors here so we don't
            # mask the original exception.
            with contextlib.suppress(OSError):
                partial.unlink(missing_ok=True)
            raise

    def list_backups(self) -> list[dict]:
        """List backups in the destination directory.

        Only backups with a valid ``.wbcommit`` marker are returned.
        The marker is written by the pipeline after the destination has
        passed verification, so its presence is the sole authority for
        whether a backup is complete and restorable. Anything without
        a valid marker is an orphan (interrupted write, failed verify,
        marker tampered with, foreign artefact) and is invisible here
        — it will be cleaned up by the orphan scan at the start of the
        next pipeline run.
        """
        if not self._dest.exists():
            return []

        backups = []
        for entry in self._dest.iterdir():
            if not self._is_backup_candidate(entry):
                continue
            if not is_backup_committed(entry):
                continue
            stat = entry.stat()
            if entry.is_dir():
                total_size = sum(f.stat().st_size for f in entry.rglob("*") if f.is_file())
            else:
                total_size = stat.st_size

            backups.append(
                {
                    "name": entry.name,
                    "size": total_size,
                    "modified": stat.st_mtime,
                    "is_dir": entry.is_dir(),
                }
            )

        return sorted(backups, key=lambda b: b["modified"], reverse=True)

    def list_orphan_backups(self) -> list[dict]:
        """List backup-like entries WITHOUT a valid ``.wbcommit``.

        Used by the orphan scan at the start of each pipeline run to
        identify and delete leftovers from interrupted writes, failed
        verifications, or foreign artefacts on shared destinations.
        Entries that look like backups (correctly named, not system
        folders, not metadata files) but have no commit marker are
        returned here.

        Returns:
            Same dict shape as ``list_backups``, but for entries
            classified as orphans.
        """
        if not self._dest.exists():
            return []

        orphans = []
        for entry in self._dest.iterdir():
            if not self._is_backup_candidate(entry):
                continue
            if is_backup_committed(entry):
                continue
            stat = entry.stat()
            if entry.is_dir():
                total_size = sum(f.stat().st_size for f in entry.rglob("*") if f.is_file())
            else:
                total_size = stat.st_size

            orphans.append(
                {
                    "name": entry.name,
                    "size": total_size,
                    "modified": stat.st_mtime,
                    "is_dir": entry.is_dir(),
                }
            )
        return sorted(orphans, key=lambda b: b["modified"], reverse=True)

    def purge_stale_partials(self, prefix: str, grace_seconds: float) -> list[str]:
        """Delete abandoned ``*.partial`` files for ``prefix``, older than grace.

        A hard kill (power loss, OS shutdown) during an upload leaves a
        ``<name>.partial`` that NO other path removes: ``.partial`` is a
        sidecar suffix, so it is filtered out of both ``list_backups``
        and ``list_orphan_backups`` — the orphan scan never sees it and
        a 47 GB-class encrypted run killed mid-write leaks its full size
        on the destination forever.

        Safety:
            * Only files whose name starts with ``prefix`` are touched —
              a concurrent OTHER profile's partial is never deleted.
            * Only files whose mtime is older than ``grace_seconds`` are
              deleted. An actively-written ``.partial`` advances its
              mtime continuously, so a recent mtime means "still being
              written" and is left alone.

        Args:
            prefix: Sanitised profile-name prefix (e.g. ``"My_Backup_"``).
            grace_seconds: Minimum age (by mtime) before a partial is
                considered abandoned rather than in-flight.

        Returns:
            Names of the partial files that were removed.
        """
        if not prefix or not self._dest.exists():
            return []

        removed: list[str] = []
        now = time.time()
        for entry in self._dest.iterdir():
            if not entry.name.startswith(prefix) or not entry.name.endswith(".partial"):
                continue
            if not entry.is_file():
                continue
            try:
                age = now - entry.stat().st_mtime
            except OSError:
                continue
            if age < grace_seconds:
                # Recent mtime → likely still being written by a
                # concurrent run. Leave it for a later scan.
                continue
            try:
                entry.unlink()
                removed.append(entry.name)
                logger.info("Removed stale partial: %s (age %.0fs)", entry.name, age)
            except OSError as e:
                logger.warning("Could not remove stale partial %s: %s", entry.name, e)
        return removed

    @staticmethod
    def _is_backup_candidate(entry: Path) -> bool:
        """Filter out non-backup filesystem noise.

        Excludes hidden files, the ``$RECYCLE.BIN``-style system
        folders, ``.partial`` artefacts from interrupted writes,
        sidecar metadata (``.wbverify``, ``.wbcommit``, ``.wbcommit.tmp``).
        Everything else is a candidate backup whose validity will be
        decided by the presence of a valid commit marker.
        """
        if entry.name.startswith("."):
            return False
        if entry.name.startswith("$"):
            return False
        # Every sidecar suffix (.wbverify, .wbcommit, .wbcommit.tmp,
        # .wbserverhashes, .partial) is filtered via the shared helper
        # so adding a new sidecar type only needs one edit in base.py.
        if is_backup_sidecar(entry.name):
            return False
        return entry.name not in SYSTEM_FOLDERS

    def delete_backup(self, remote_name: str) -> None:
        """Delete a backup and its sidecar metadata.

        Removes the backup itself, the ``.wbverify`` manifest, the
        ``.wbcommit`` marker, and any leftover ``.wbcommit.tmp``
        from an interrupted commit-marker write. All deletes use the
        same robust helper so a single stuck handle on any sidecar
        does not leave the destination half-cleaned.

        Raises:
            FileNotFoundError: If the backup does not exist.
            StorageDeleteError: If any removal leaves residual files
                or directories (long-path failure, FS lock that
                survived retries, etc.).
        """
        target = self._dest / remote_name
        if not target.exists():
            raise FileNotFoundError(f"Backup not found: {remote_name}")

        result = safe_remove_tree(target)
        if not result.success:
            _log_residuals(str(target), result)
            raise StorageDeleteError(remote_name, result.residuals)

        logger.info("Deleted backup: %s", remote_name)

        # Remove sidecar metadata: ``.wbverify`` (manifest),
        # ``.wbcommit`` (commit marker), and any stray ``.wbcommit.tmp``
        # left by an interrupted commit-marker write. A miss on any
        # of these is non-fatal (file may simply not exist) — only
        # an unsuccessful removal raises.
        sidecars = [
            self._dest / f"{remote_name}.wbverify",
            self._dest / f"{remote_name}{COMMIT_MARKER_SUFFIX}",
            self._dest / f"{remote_name}{COMMIT_MARKER_SUFFIX}.tmp",
        ]
        for sidecar in sidecars:
            if not sidecar.exists():
                continue
            sidecar_result = safe_remove_tree(sidecar)
            if not sidecar_result.success:
                _log_residuals(str(sidecar), sidecar_result)
                raise StorageDeleteError(sidecar.name, sidecar_result.residuals)
            logger.info("Deleted sidecar: %s", sidecar.name)

    def test_connection(self) -> tuple[bool, str]:
        """Check if the destination is accessible and writable.

        Designed to be tolerant of USB drives in power-save:
        - Retries ``exists()`` a few times with back-off to let the
          drive spin up.
        - Pokes the drive root (``listdir``) between retries to force
          Windows to mount a volume it has put to sleep.
        - Separates "drive missing" from "permission denied" from
          "write failed" so the user sees an actionable message rather
          than a generic "Destinations unavailable".
        """
        result: list = [False, "Connection timeout"]

        def _wait_for_drive_online() -> bool:
            """Return True as soon as ``self._dest`` can be stat'd."""
            # Cheap initial check — responsive drives return instantly.
            if self._dest.exists():
                return True
            # Drive letter root — listing it triggers Windows to bring
            # the volume back online from power-save.
            root = None
            s = str(self._dest)
            if len(s) >= 2 and s[1] == ":":
                root = f"{s[0]}:\\"

            # Fast-fail: if the drive letter root itself is not
            # stat-able, the drive is physically unplugged (not just
            # sleeping). No amount of wake-up retry will resurrect it
            # — burning 15.8 s of backoff per attempt + 16 s for the
            # silent retry in ``_precheck_and_run`` produced a 32 s
            # delay before "Destinations unavailable" appeared on
            # the 21/05/2026 user report. A missing drive letter is
            # the unambiguous "drive is gone" signal: report it
            # immediately. A mounted drive whose subdir is still
            # finishing enumeration (the case the wake-up loop is
            # actually for) still falls through to the loop below.
            if root and not Path(root).exists():
                return False

            # Cumulative sleep budget ~15.8s. External USB drives in
            # deep power-save can need 10-12s to fully enumerate on the
            # first probe after reconnection; the 8.0 s tail covers that
            # long tail without penalising healthy drives (which return
            # on the first ``exists()`` check above).
            for attempt, delay in enumerate((0.3, 0.5, 1.0, 2.0, 4.0, 8.0)):
                time.sleep(delay)
                if root and attempt == 1:
                    with contextlib.suppress(OSError):
                        os.listdir(root)  # Wake the volume
                if self._dest.exists():
                    return True
            return False

        def _test() -> None:
            try:
                if not _wait_for_drive_online():
                    result[0] = False
                    result[1] = (
                        f"Drive not ready after wake-up retries: {self._dest}. "
                        f"Reconnect the drive or wait a few seconds and retry."
                    )
                    return

                test_file = self._dest / ".backup_manager_test"
                try:
                    test_file.write_text("test", encoding="utf-8")
                    test_file.unlink()
                except PermissionError as pe:
                    result[0] = False
                    result[1] = (
                        f"{READ_ONLY_OR_LOCKED_MARKER} "
                        f"(permission denied on {self._dest}): {pe}"
                    )
                    return
                except OSError as we:
                    result[0] = False
                    result[1] = f"Destination present but write failed " f"({self._dest}): {we}"
                    return

                free = self.get_free_space()
                if free is not None:
                    free_gb = free / (1024**3)
                    result[0] = True
                    result[1] = f"Connected — {free_gb:.1f} GB free"
                else:
                    result[0] = True
                    result[1] = "Connected"
            except Exception as e:
                result[0] = False
                result[1] = f"Unexpected error on {self._dest}: {type(e).__name__}: {e}"

        thread = threading.Thread(target=_test, daemon=True)
        thread.start()
        thread.join(timeout=CONNECTION_TIMEOUT)

        if thread.is_alive():
            return False, (
                f"Connection test timed out after {CONNECTION_TIMEOUT}s. "
                f"The drive may be very slow or unresponsive; try unplugging "
                f"and reconnecting it."
            )

        return result[0], result[1]

    def get_free_space(self) -> int | None:
        """Get available disk space in bytes.

        Returns None when the destination is unreachable (drive
        unplugged, permission denied, etc.) and logs the reason so
        callers don't silently assume "unlimited space" and bypass
        rotation cleanup.
        """
        try:
            usage = shutil.disk_usage(self._dest)
            return usage.free
        except FileNotFoundError:
            logger.warning("get_free_space: destination missing: %s", self._dest)
            return None
        except PermissionError as e:
            logger.warning("get_free_space: permission denied on %s: %s", self._dest, e)
            return None
        except OSError as e:
            logger.warning("get_free_space: OS error on %s: %s", self._dest, e)
            return None

    def get_file_size(self, remote_name: str) -> int | None:
        """Get size of a backup file or directory."""
        target = self._dest / remote_name
        if not target.exists():
            return None
        if target.is_dir():
            return sum(f.stat().st_size for f in target.rglob("*") if f.is_file())
        return target.stat().st_size

    def download_backup(self, remote_name: str, local_dir: Path) -> Path:
        """Download (copy) a local backup to another local directory."""
        src = self._dest / remote_name
        dst = local_dir / remote_name
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
        return dst

    def _throttled_copy(self, src: Path, dst: Path) -> None:
        """Copy file with bandwidth throttling."""
        with open(src, "rb") as f_in:
            reader = self._get_throttled_reader(f_in)
            with open(dst, "wb") as f_out:
                while True:
                    chunk = reader.read(1024 * 1024)
                    if not chunk:
                        break
                    f_out.write(chunk)
        # Preserve metadata
        shutil.copystat(src, dst)
