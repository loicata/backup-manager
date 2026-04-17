"""Advisory per-profile run lock.

Prevents two backup runs for the same profile from racing (typically
when the scheduler fires while the user has also clicked "Run now").
Without a lock the second run reads ``last_backup_completed == False``
and calls ``_cleanup_incomplete_backup``, which deletes the backup the
first run is still writing.

The implementation uses two layers:

- A per-path ``threading.Lock`` held in a process-local dictionary.
  Backup Manager runs the scheduler and the UI in the same Python
  process (different threads), so most clashes happen intra-process
  and must be detected without touching the filesystem.
- A PID-carrying lock file, used purely for the (unusual) cross-process
  case where two separate Python instances somehow target the same
  config directory.  Staleness is detected by checking whether the
  holder's PID is still alive.
"""

import contextlib
import logging
import os
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

# Per-path threading.Lock table.  A dedicated guard protects the table
# itself so two threads inserting a new key at the same time do not
# both create a lock and race on which one wins.
_thread_locks: dict[str, threading.Lock] = {}
_thread_locks_guard = threading.Lock()


def _get_thread_lock(lock_path: Path) -> threading.Lock:
    """Return the per-path ``threading.Lock``, creating it on first use.

    Uses a single ``absolute()`` call so two threads that race on
    ``exists()`` + ``resolve()`` do not see different keys. A pure
    ``absolute()`` is deterministic; ``resolve()`` only helped follow
    symlinks on POSIX, which profile lock files never use.
    """
    key = str(lock_path.absolute())
    with _thread_locks_guard:
        lock = _thread_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _thread_locks[key] = lock
        return lock


class ProfileLockError(RuntimeError):
    """Raised when a profile lock is already held by another live run."""


def _pid_alive(pid: int) -> bool:
    """Return True if ``pid`` refers to a running process on this host.

    Cross-platform: uses ``OpenProcess`` on Windows and signal 0 on
    POSIX.  A PID that exists but is inaccessible (privilege drop) is
    conservatively treated as alive so we never take over a lock we
    cannot prove to be dead.
    """
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        process_query_limited_information = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(  # type: ignore[attr-defined]
            process_query_limited_information, False, pid
        )
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)  # type: ignore[attr-defined]
            return True
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # Exists but unreachable from this user — assume alive.
        return True


def acquire(lock_path: Path) -> Path:
    """Acquire an exclusive run lock for a profile.

    Blocks the caller from proceeding if another run — in the same
    process (different thread) or in another live process — already
    holds the lock for this profile.

    Args:
        lock_path: Absolute path to the lock file (one per profile).

    Returns:
        The lock path, for use with :func:`release`.

    Raises:
        ProfileLockError: If another live run already holds the lock.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    # Intra-process guard: the scheduler and UI live in the same Python
    # process, so the most common clash ("scheduler + Run now") is
    # between two threads sharing this PID.  A non-blocking acquire
    # returns False without waiting if another thread is already in.
    thread_lock = _get_thread_lock(lock_path)
    if not thread_lock.acquire(blocking=False):
        raise ProfileLockError(
            "Another backup is already running for this profile in "
            "this application.  Wait for it to finish before starting "
            "a new run."
        )

    try:
        _acquire_file_lock(lock_path)
    except BaseException:
        thread_lock.release()
        raise
    return lock_path


def _acquire_file_lock(lock_path: Path) -> None:
    """Write the PID lock file, detecting stale cross-process holders."""
    my_pid = os.getpid()

    # Fast path: atomic create.
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        fd = None

    if fd is not None:
        try:
            os.write(fd, str(my_pid).encode("ascii"))
        finally:
            os.close(fd)
        return

    # Lock file already present — inspect the holder.
    try:
        raw = lock_path.read_text(encoding="ascii").strip()
        other_pid = int(raw)
    except (OSError, ValueError):
        logger.warning(
            "Profile lock %s is unreadable or corrupt; treating as stale",
            lock_path,
        )
        other_pid = 0

    if other_pid and other_pid != my_pid and _pid_alive(other_pid):
        raise ProfileLockError(
            f"Another backup is already running for this profile "
            f"(PID {other_pid}).  Wait for it to finish or stop it "
            f"before starting a new run."
        )

    # Stale lock or leftover from our own crashed previous run — take
    # it over atomically.
    logger.info(
        "Taking over stale profile lock %s (PID %d)",
        lock_path,
        other_pid,
    )
    lock_path.write_text(str(my_pid), encoding="ascii")


def release(lock_path: Path) -> None:
    """Release a previously-acquired lock.

    Best-effort on the filesystem side: a missing lock is silently
    tolerated (double-release, crash cleanup).  The intra-process
    threading lock is always released so a subsequent acquire in the
    same process can proceed.
    """
    try:
        lock_path.unlink()
    except FileNotFoundError:
        pass
    except OSError as e:
        logger.warning("Failed to release profile lock %s: %s", lock_path, e)

    thread_lock = _get_thread_lock(lock_path)
    # Not held by us — acquire() failed before taking it, or the
    # caller released twice.  Nothing to do either way.
    with contextlib.suppress(RuntimeError):
        thread_lock.release()
