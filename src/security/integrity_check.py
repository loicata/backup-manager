"""Application integrity check using SHA-256 checksums.

Detects tampering or corruption of application source files.
Checksums are stored in %APPDATA%/BackupManager/app_checksums.json
with an HMAC signature for tamper detection.
"""

import hashlib
import hmac
import json
import logging
import os
import secrets
import sys
from pathlib import Path

from src.core.exceptions import DPAPIUnavailableError, HMACKeyRegeneratedError

logger = logging.getLogger(__name__)

CHECKSUM_FILE = "app_checksums.json"
HMAC_KEY_FILE = ".integrity_key"
# Empty sentinel created next to the key the first time we successfully
# generate or read one. Its purpose is forensic: if a later run finds
# the sentinel present but the key file gone, we know the install
# previously had a key — the absence is suspicious (manual delete, AV
# quarantine, cleanup tool) and warrants the regen-alert dialog instead
# of being treated as a fresh install. Storing this as a separate file
# rather than a metadata field on the key itself means a wipe of the
# key file alone cannot also wipe the evidence that a key once existed.
HMAC_KEY_INSTALLED_SENTINEL = ".integrity_key.installed"
HASH_ALGORITHM = "sha256"
CHUNK_SIZE = 128 * 1024  # 128 KB

# Module-level switch toggled by the ``--allow-plaintext-keys`` CLI
# flag at startup. Lets the user override the strict-DPAPI requirement
# on Windows when their environment cannot grant DPAPI (corrupted
# user profile, group policy lockdown, antivirus blocking crypt32).
# Off by default — strict is the safe default that surfaces the
# failure instead of silently writing the key in clear and letting any
# userland process forge ``app_checksums.json`` / ``.wbcommit``
# signatures.
_ALLOW_PLAINTEXT_FALLBACK = False


def enable_plaintext_fallback() -> None:
    """Authorise the in-clear key fallback on Windows for this process.

    Must be called BEFORE any code path that may trigger a key
    generation (i.e. before ``verify_integrity`` or any pipeline run).
    The override is per-process and does not persist — every relaunch
    requires passing ``--allow-plaintext-keys`` again, on purpose, so
    a user who has fixed their DPAPI environment returns to the strict
    posture automatically.
    """
    global _ALLOW_PLAINTEXT_FALLBACK
    _ALLOW_PLAINTEXT_FALLBACK = True
    logger.error(
        "Plaintext key fallback enabled by --allow-plaintext-keys. "
        "HMAC key and machine key may be written in clear if DPAPI fails. "
        "This neutralises tamper-detection — only use to recover from a "
        "broken Windows profile."
    )


def is_plaintext_fallback_allowed() -> bool:
    """Return whether the in-clear fallback has been authorised.

    Read by ``src.security.encryption`` so the per-machine key writer
    and the HMAC key writer share a single source of truth. Tests
    flip the module-level flag directly when they need the legacy
    behaviour for an isolated case.
    """
    return _ALLOW_PLAINTEXT_FALLBACK


def _write_key_atomic(path: Path, payload: bytes) -> None:
    """Write key material with 0o600 mode + fsync + atomic rename.

    Mirrors the pattern already used by ``ConfigManager._atomic_write``
    for profile files. The 0o600 mode is enforced AT CREATION via
    ``os.open`` — a ``path.write_bytes`` followed by ``os.chmod`` would
    leave a window where the file is world-readable.

    On Windows the POSIX mode is silently ignored by NTFS (ACLs from
    ``%APPDATA%`` govern access), so this is strictly a Linux/macOS
    hardening — but the cost on Windows is zero.

    ``O_BINARY`` is critical on Windows. ``os.open`` defaults to the
    C runtime's text translation mode, which rewrites ``\\n`` -> ``\\r\\n``
    and stops on ``\\x1a`` (Ctrl-Z) during read. DPAPI-wrapped blobs and
    raw 32-byte secrets contain arbitrary bytes including newlines and
    EOF sentinels; without the flag, ``CryptUnprotectData`` returns
    error 13 (ERROR_INVALID_DATA) on the next launch and the HMAC key
    is regenerated on every run — tamper detection becomes random.
    The constant is not defined on POSIX, so we add it conditionally.
    """
    tmp = path.with_name(path.name + ".tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    fd = os.open(str(tmp), flags, 0o600)
    try:
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)

# Source files to verify
APP_FILES = [
    "__init__.py",
    "__main__.py",
    "core/config.py",
    "core/events.py",
    "core/backup_engine.py",
    "core/scheduler.py",
    "security/encryption.py",
    "security/integrity_check.py",
    "security/secure_memory.py",
    "security/verification.py",
    "storage/base.py",
    "storage/local.py",
    "storage/sftp.py",
    "storage/s3.py",
    "notifications/email_notifier.py",
    "installer.py",
]


def _get_app_dir() -> Path:
    """Get the application source directory."""
    from src.__main__ import _is_nuitka

    if hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS) / "src"
    if _is_nuitka():
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


def _get_checksum_path() -> Path:
    """Get path to stored checksums file."""
    appdata = os.environ.get("APPDATA", "")
    return Path(appdata) / "BackupManager" / CHECKSUM_FILE


def _compute_file_hash(filepath: Path) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def compute_checksums() -> dict[str, str]:
    """Compute SHA-256 checksums of all application source files.

    Returns:
        Dict mapping relative file path to hex digest.
    """
    app_dir = _get_app_dir()
    checksums = {}
    for rel_path in APP_FILES:
        filepath = app_dir / rel_path
        if filepath.exists():
            checksums[rel_path] = _compute_file_hash(filepath)
    return checksums


def _dpapi_wrap(data: bytes) -> bytes:
    """Encrypt ``data`` with Windows DPAPI (user scope).

    The wrapped blob can only be decrypted by the same Windows user
    on the same machine. Returns the raw data unchanged on
    non-Windows platforms (no equivalent system-managed key store
    available without introducing an interactive step).

    Raises:
        OSError: if DPAPI is unavailable or the call fails. Callers
            decide whether to fall back or abort.
    """
    if sys.platform != "win32":
        return data
    import ctypes
    from ctypes import wintypes

    class _DATA_BLOB(ctypes.Structure):
        _fields_ = [
            ("cbData", wintypes.DWORD),
            ("pbData", ctypes.POINTER(ctypes.c_char)),
        ]

    # ``create_string_buffer`` preserves embedded null bytes — using
    # ``c_char_p`` would silently truncate at the first ``\x00`` and
    # corrupt binary payloads (HMAC keys are uniformly random, so a
    # zero byte in the first 32 bytes happens ~12% of the time).
    buf_in = ctypes.create_string_buffer(data, len(data))
    blob_in = _DATA_BLOB(
        len(data),
        ctypes.cast(buf_in, ctypes.POINTER(ctypes.c_char)),
    )
    blob_out = _DATA_BLOB()
    if not ctypes.windll.crypt32.CryptProtectData(
        ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)
    ):
        raise OSError(f"CryptProtectData failed (error {ctypes.GetLastError()})")
    try:
        wrapped = ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(blob_out.pbData)
    return wrapped


def _dpapi_unwrap(data: bytes) -> bytes:
    """Decrypt ``data`` with Windows DPAPI. Inverse of ``_dpapi_wrap``.

    Raises:
        OSError: on Windows if the blob cannot be unwrapped (e.g.
            different user, different machine, or the data was
            never wrapped).
    """
    if sys.platform != "win32":
        return data
    try:
        import ctypes
        from ctypes import wintypes

        class _DATA_BLOB(ctypes.Structure):
            _fields_ = [
                ("cbData", wintypes.DWORD),
                ("pbData", ctypes.POINTER(ctypes.c_char)),
            ]

        # create_string_buffer (NOT c_char_p) preserves null bytes
        # inside the ciphertext — DPAPI blobs are binary and routinely
        # contain zeros.
        buf_in = ctypes.create_string_buffer(data, len(data))
        blob_in = _DATA_BLOB(
            len(data),
            ctypes.cast(buf_in, ctypes.POINTER(ctypes.c_char)),
        )
        blob_out = _DATA_BLOB()
        if not ctypes.windll.crypt32.CryptUnprotectData(
            ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)
        ):
            raise OSError(f"CryptUnprotectData failed (error {ctypes.GetLastError()})")
        try:
            plain = ctypes.string_at(blob_out.pbData, blob_out.cbData)
        finally:
            ctypes.windll.kernel32.LocalFree(blob_out.pbData)
        return plain
    except Exception as e:
        raise OSError(f"DPAPI unwrap failed: {e}") from e


# Marker prefix so we know a key file has been wrapped. Without this
# marker the file contents are either legacy plaintext (32 bytes) or
# some other format; we can distinguish and handle each case.
_DPAPI_MARKER = b"DPAPI\x01"


def _archive_old_key(key_path: Path, reason: str) -> Path | None:
    """Best-effort copy of the soon-to-be-replaced key for recovery.

    Called immediately before the regeneration path overwrites
    ``key_path``. The archive name embeds a UTC timestamp and a short
    machine-readable reason tag so a future forensic tool can list
    all archives (``.legacy_*``), try each one to validate orphan
    ``.wbcommit`` markers, and report which historical backups would
    be recoverable.

    Never raises. The regen path must proceed even when the archive
    cannot be written (read-only profile, disk full, permission
    denied). Loss of the archive is an additional risk but not a
    blocker — the dialog presented to the user already conveys that
    historical backups are at risk.

    Args:
        key_path: Path to the existing ``.integrity_key`` file. If
            it does not exist this is a no-op (we have nothing to
            archive).
        reason: Short snake_case tag describing why the key is being
            replaced (``unwrap_failed``, ``read_failed``,
            ``wrong_size``). Embedded in the archive filename so
            multiple regens leave self-describing artefacts.

    Returns:
        Path of the archive on success, ``None`` when there was
        nothing to archive or the copy failed.
    """
    if not isinstance(key_path, Path):
        raise TypeError(f"key_path must be a Path, got {type(key_path).__name__}")
    if not key_path.exists():
        return None
    try:
        import shutil
        from datetime import UTC, datetime

        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        archive = key_path.with_name(f"{key_path.name}.legacy_{timestamp}_{reason}")
        shutil.copy2(key_path, archive)
        logger.info("Archived old HMAC key for recovery: %s", archive)
        return archive
    except OSError as e:
        logger.warning("Could not archive old HMAC key (continuing regen): %s", e)
        return None


def _ensure_install_sentinel(sentinel_path: Path) -> None:
    """Create the install sentinel if missing. Never raises.

    Touched whenever a key is successfully generated OR successfully
    read. The two paths matter equally: the sentinel must exist as
    soon as the install can prove it ever held a usable key, so a
    later "key disappeared" event can be distinguished from a genuine
    first run.

    A migration concern: installs created before this patch have a
    real key but no sentinel. The first successful read on the patched
    binary creates the sentinel — from then on the "disappeared key"
    detection works for that install too.

    Args:
        sentinel_path: Full path of the sentinel file. Its parent
            directory must already exist (the caller has just written
            or read the key file living in the same directory).
    """
    if not isinstance(sentinel_path, Path):
        raise TypeError(f"sentinel_path must be a Path, got {type(sentinel_path).__name__}")
    if sentinel_path.exists():
        return
    try:
        sentinel_path.touch(exist_ok=True)
        logger.debug("Install sentinel created at %s", sentinel_path)
    except OSError as e:
        # Best-effort: a missing sentinel only weakens the next regen
        # detection, it does not break the current run.
        logger.debug("Could not create install sentinel %s: %s", sentinel_path, e)


def _try_read_existing_key(key_path: Path, sentinel_path: Path) -> bytes | None:
    """Attempt to load and return the existing HMAC key from ``key_path``.

    Returns the key bytes on success. Returns ``None`` when the file is
    absent (caller decides whether that is a legitimate first run or a
    suspicious disappearance based on the sentinel).

    Raises:
        HMACKeyRegeneratedError: when the file exists but cannot be
            read/decrypted/validated — UNLESS
            ``_ALLOW_PLAINTEXT_FALLBACK`` is set, in which case we log
            the warning, archive the old key, and return ``None`` so
            the caller proceeds with regeneration silently (legacy
            ``--allow-plaintext-keys`` workflow preserved).
    """
    if not key_path.exists():
        return None
    try:
        stored = key_path.read_bytes()
    except OSError as e:
        _archive_old_key(key_path, "read_failed")
        logger.warning("Could not read HMAC key, generating new one: %s", e)
        if not _ALLOW_PLAINTEXT_FALLBACK:
            raise HMACKeyRegeneratedError(
                f"Existing HMAC key cannot be read: {e}. "
                f"Possible cause: file lock, antivirus quarantine, or "
                f"permission denied. Proceeding will invalidate all "
                f"backup commit markers signed with the original key.",
                prior_key_existed=True,
                prior_key_path=key_path,
                cause=e,
            ) from e
        return None

    if stored.startswith(_DPAPI_MARKER):
        return _unwrap_dpapi_key_or_raise(stored, key_path, sentinel_path)
    # Plain payload — either a legacy pre-DPAPI key (exactly 32 bytes) or
    # corruption.  The legacy branch must remain silent: it is the normal
    # path after a successful ``--allow-plaintext-keys`` run.
    if len(stored) == 32:
        _ensure_install_sentinel(sentinel_path)
        return stored
    _archive_old_key(key_path, "wrong_size")
    logger.warning("HMAC key file has unexpected size (%d bytes), regenerating", len(stored))
    if not _ALLOW_PLAINTEXT_FALLBACK:
        raise HMACKeyRegeneratedError(
            f"Existing HMAC key has unexpected size ({len(stored)} bytes; "
            f"expected 32 or a DPAPI-wrapped blob). File may be corrupted, "
            f"truncated, or overwritten by an unrelated tool.",
            prior_key_existed=True,
            prior_key_path=key_path,
        )
    return None


def _unwrap_dpapi_key_or_raise(
    stored: bytes,
    key_path: Path,
    sentinel_path: Path,
) -> bytes | None:
    """Inverse of the DPAPI wrap done at write time.

    Split out of :func:`_try_read_existing_key` so the latter stays
    under the 30-line guideline. Same return / raise contract: bytes
    on success, ``None`` when plaintext fallback authorises silent
    regeneration, raises ``HMACKeyRegeneratedError`` in strict mode.
    """
    try:
        key = _dpapi_unwrap(stored[len(_DPAPI_MARKER) :])
    except OSError as e:
        _archive_old_key(key_path, "unwrap_failed")
        logger.warning("Could not unwrap HMAC key, regenerating: %s", e)
        if not _ALLOW_PLAINTEXT_FALLBACK:
            raise HMACKeyRegeneratedError(
                f"Existing HMAC key cannot be decrypted by DPAPI: {e}. "
                f"Likely cause: Windows reinstall, user-profile change, or "
                f"%APPDATA%\\BackupManager copied from another machine. "
                f"Proceeding will invalidate all backup commit markers "
                f"signed with the original key.",
                prior_key_existed=True,
                prior_key_path=key_path,
                cause=e,
            ) from e
        return None
    _ensure_install_sentinel(sentinel_path)
    return key


def _persist_new_key(key: bytes, key_path: Path) -> None:
    """Wrap (Windows) or store as-is (POSIX) and atomically write.

    Centralises the DPAPI-wrap-or-fallback decision so the orchestrator
    in :func:`_get_hmac_key` does not have to. Honours
    ``_ALLOW_PLAINTEXT_FALLBACK`` exactly as before — strict-mode wrap
    failure raises ``DPAPIUnavailableError`` (no half-written file),
    plaintext-mode wrap failure writes the raw key WITHOUT marker so
    the next read takes the legacy-plain branch instead of unwrapping
    forever.
    """
    key_path.parent.mkdir(parents=True, exist_ok=True)
    if sys.platform == "win32":
        try:
            wrapped = _DPAPI_MARKER + _dpapi_wrap(key)
        except OSError as e:
            if not _ALLOW_PLAINTEXT_FALLBACK:
                raise DPAPIUnavailableError("wrap", e) from e
            logger.error(
                "DPAPI wrap failed, HMAC key stored in clear "
                "(--allow-plaintext-keys is set): %s",
                e,
            )
            wrapped = key
    else:
        wrapped = key
    _write_key_atomic(key_path, wrapped)


def _get_hmac_key() -> bytes:
    """Get or create the per-install HMAC key for checksum signing.

    On Windows, the key is wrapped with DPAPI (user scope) before
    writing so that a malware process running as the user still
    needs to issue CryptUnprotectData — it cannot simply read the
    file to recover the key. Without the wrap, a read-the-file
    attacker could forge the checksum HMAC and defeat the
    tamper-detection mechanism entirely.

    Regeneration is silent only when the install can prove this is a
    genuine first run (neither the key file nor the install sentinel
    exists). Any other regeneration path (DPAPI unwrap failure, read
    error, malformed file, sentinel-present-but-key-missing) raises
    ``HMACKeyRegeneratedError`` so the bootstrap can warn the user
    BEFORE the next backup run classifies every historical
    ``.wbcommit`` as an orphan and deletes the corresponding backups.

    The strict behaviour is suppressed when
    ``_ALLOW_PLAINTEXT_FALLBACK`` is set (``--allow-plaintext-keys``):
    in that mode the regen happens silently, preserving the existing
    CLI escape hatch for users with permanently broken DPAPI.

    Raises:
        HMACKeyRegeneratedError: see above.
        DPAPIUnavailableError: from :func:`_persist_new_key` when the
            fresh-key wrap fails in strict mode.
    """
    appdata = os.environ.get("APPDATA", "")
    key_dir = Path(appdata) / "BackupManager"
    key_path = key_dir / HMAC_KEY_FILE
    sentinel_path = key_dir / HMAC_KEY_INSTALLED_SENTINEL

    existing = _try_read_existing_key(key_path, sentinel_path)
    if existing is not None:
        return existing

    # Distinguish genuine first run from "key disappeared since last run".
    # The sentinel is the ground truth: it is written on the first
    # successful generation OR read, and never removed by Backup
    # Manager itself.
    if sentinel_path.exists() and not _ALLOW_PLAINTEXT_FALLBACK:
        raise HMACKeyRegeneratedError(
            "HMAC key file is missing but an install marker indicates it "
            "previously existed on this profile. Possible cause: accidental "
            "delete, antivirus quarantine, or a cleanup tool. Proceeding "
            "will invalidate all backup commit markers signed with the "
            "original key.",
            prior_key_existed=True,
            prior_key_path=key_path,
        )

    key = secrets.token_bytes(32)
    _persist_new_key(key, key_path)
    _ensure_install_sentinel(sentinel_path)
    return key


def list_legacy_key_archives() -> list[Path]:
    """Return the available ``.integrity_key.legacy_*`` archive paths.

    The list is sorted newest-first (lexicographic on the embedded UTC
    timestamp) so that recovery code tries the most recent archive
    first — the one most likely to have signed the markers under
    examination.

    Empty list if ``%APPDATA%/BackupManager`` does not exist or no
    archive was ever written (no regen ever happened on this install).

    Used by :func:`get_legacy_hmac_keys` and by the recovery branch of
    :func:`src.core.phases.commit_marker.read_commit_marker`. Kept as
    a separate accessor so a future CLI / UI feature can list the
    available archives for the user without having to import the
    commit-marker module.
    """
    appdata = os.environ.get("APPDATA", "")
    key_dir = Path(appdata) / "BackupManager"
    if not key_dir.exists():
        return []
    pattern = f"{HMAC_KEY_FILE}.legacy_*"
    return sorted(key_dir.glob(pattern), reverse=True)


def _load_key_from_archive(archive_path: Path) -> bytes | None:
    """Try to decode one legacy key archive. Never raises.

    Mirrors the read-side branches of :func:`_try_read_existing_key`
    (DPAPI-wrapped vs. legacy plain 32 bytes) but returns ``None``
    instead of raising on any failure — the recovery path needs to
    SKIP unreadable archives and keep trying the next one rather than
    aborting the whole orphan scan.

    Logged at DEBUG only: a "failed to load" archive on a re-installed
    Windows is the expected case (DPAPI scope changed), spamming
    WARNING per archive would drown the log.

    Args:
        archive_path: Full path of a ``.integrity_key.legacy_*`` file.

    Returns:
        The raw 32-byte key on success, ``None`` on read error /
        unwrap failure / unexpected file size.
    """
    if not isinstance(archive_path, Path):
        raise TypeError(f"archive_path must be a Path, got {type(archive_path).__name__}")
    try:
        stored = archive_path.read_bytes()
    except OSError as e:
        logger.debug("Could not read legacy key archive %s: %s", archive_path, e)
        return None
    if stored.startswith(_DPAPI_MARKER):
        try:
            return _dpapi_unwrap(stored[len(_DPAPI_MARKER) :])
        except OSError as e:
            logger.debug("Could not unwrap legacy key archive %s: %s", archive_path, e)
            return None
    if len(stored) == 32:
        return stored
    logger.debug(
        "Legacy key archive %s has unexpected size (%d bytes) — skipping",
        archive_path,
        len(stored),
    )
    return None


def get_legacy_hmac_keys() -> list[bytes]:
    """Return every legacy HMAC key that can be loaded, newest first.

    Recovery use-case: after the per-install key was regenerated
    (Windows reinstall, AV quarantine, accidental delete confirmed
    by the user at the bootstrap alert), every previously-signed
    ``.wbcommit`` now fails HMAC verification against the current
    key and would be classified as an orphan by
    ``LocalStorage.list_orphan_backups`` — backups deleted at the
    next ``_phase_orphan_scan``.

    By giving the commit-marker reader the list of historical keys,
    we can:

    1. Validate the marker against any one of them (proving it was
       authentic at the time it was written).
    2. Re-sign the marker with the CURRENT key so the next read
       takes the fast path and the backup is preserved.

    Honest scope: this only helps when the OS still grants DPAPI
    access to the legacy keys (corrupted live file, AV touched the
    live only). For a Windows reinstall the legacy archives are
    wrapped with the OLD DPAPI scope which the new user can no
    longer unwrap — :func:`_load_key_from_archive` will return
    ``None`` for every archive and recovery silently fails over to
    the orphan classification. The user can still recover manually
    by mounting the old profile and re-wrapping the archive with
    the new DPAPI scope (out of scope for this patch).

    Returns:
        List of 32-byte keys, newest first. Empty when no archive
        exists or none can be loaded.
    """
    keys: list[bytes] = []
    for archive in list_legacy_key_archives():
        key = _load_key_from_archive(archive)
        if key is not None:
            keys.append(key)
    return keys


def _compute_hmac(data: str) -> str:
    """Compute HMAC-SHA256 of data string."""
    key = _get_hmac_key()
    return hmac.new(key, data.encode("utf-8"), hashlib.sha256).hexdigest()


def get_app_hmac_key() -> bytes:
    """Public accessor for the per-install HMAC key.

    Returns the same 32-byte key used to sign ``app_checksums.json``.
    Reused by other on-disk integrity artefacts (notably the
    ``.wbcommit`` markers written next to backups) so every signed
    artefact on this machine binds to a single secret that a malware
    process running as the user still needs to unwrap via DPAPI to
    forge.

    Returns:
        Raw 32-byte key.

    Raises:
        OSError: if neither DPAPI nor a fallback key file is available.
    """
    return _get_hmac_key()


def save_checksums() -> None:
    """Compute and save checksums with HMAC signature."""
    checksums = compute_checksums()
    data_str = json.dumps(checksums, sort_keys=True)
    payload = {
        "checksums": checksums,
        "hmac": _compute_hmac(data_str),
    }
    path = _get_checksum_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("Saved %d checksums", len(checksums))


def load_checksums() -> dict[str, str] | None:
    """Load and verify stored checksums.

    Returns:
        Checksums dict if valid, None if missing or tampered.
    """
    path = _get_checksum_path()
    if not path.exists():
        return None

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        logger.warning("Corrupted checksums file")
        return None

    checksums = payload.get("checksums", {})
    stored_hmac = payload.get("hmac", "")
    data_str = json.dumps(checksums, sort_keys=True)
    expected_hmac = _compute_hmac(data_str)

    if not hmac.compare_digest(stored_hmac, expected_hmac):
        logger.warning("HMAC verification failed — checksums may be tampered")
        return None

    return checksums


def verify_integrity() -> tuple[bool, str]:
    """Verify application integrity against stored checksums.

    Returns:
        (ok, message) — True if all files match or first run.
    """
    stored = load_checksums()
    if stored is None:
        # First run or corrupted: regenerate
        save_checksums()
        return True, "First run: checksums initialized"

    current = compute_checksums()

    modified = []
    missing = []
    for rel_path, expected_hash in stored.items():
        actual_hash = current.get(rel_path)
        if actual_hash is None:
            missing.append(rel_path)
        elif actual_hash != expected_hash:
            modified.append(rel_path)

    if not modified and not missing:
        return True, "All files OK"

    parts = []
    if modified:
        parts.append(f"Modified: {', '.join(modified)}")
    if missing:
        parts.append(f"Missing: {', '.join(missing)}")
    msg = "; ".join(parts)
    logger.warning("Integrity check failed: %s", msg)
    return False, msg


def reset_checksums() -> None:
    """Regenerate checksums from current files."""
    save_checksums()
    logger.info("Checksums reset")
