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

from src.core.exceptions import DPAPIUnavailableError

logger = logging.getLogger(__name__)

CHECKSUM_FILE = "app_checksums.json"
HMAC_KEY_FILE = ".integrity_key"
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


def _get_hmac_key() -> bytes:
    """Get or create the HMAC key for checksum signing.

    On Windows, the key is wrapped with DPAPI (user scope) before
    writing so that a malware process running as the user still
    needs to issue CryptUnprotectData — it cannot simply read the
    file to recover the key. Without the wrap, a read-the-file
    attacker could forge the checksum HMAC and defeat the
    tamper-detection mechanism entirely.
    """
    appdata = os.environ.get("APPDATA", "")
    key_path = Path(appdata) / "BackupManager" / HMAC_KEY_FILE
    if key_path.exists():
        try:
            stored = key_path.read_bytes()
            if stored.startswith(_DPAPI_MARKER):
                try:
                    return _dpapi_unwrap(stored[len(_DPAPI_MARKER) :])
                except OSError as e:
                    logger.warning("Could not unwrap HMAC key, regenerating: %s", e)
            else:
                # Plain 32-byte key (from a previous version or from a
                # platform without DPAPI). Keep using it, but on the
                # next save it will be re-wrapped.
                if len(stored) == 32:
                    return stored
                logger.warning("HMAC key file has unexpected size, regenerating")
        except OSError:
            logger.warning("Could not read HMAC key, generating new one")

    key = secrets.token_bytes(32)
    key_path.parent.mkdir(parents=True, exist_ok=True)

    if sys.platform == "win32":
        try:
            wrapped_payload = _dpapi_wrap(key)
            wrapped = _DPAPI_MARKER + wrapped_payload
        except OSError as e:
            if not _ALLOW_PLAINTEXT_FALLBACK:
                # Refuse to write a plaintext HMAC key by default.
                # Without DPAPI any userland process running as the user
                # can read the key and forge ``app_checksums.json`` and
                # ``.wbcommit`` signatures — tamper-detection becomes
                # security theatre. ``--allow-plaintext-keys`` is the
                # explicit opt-out for users with broken DPAPI who
                # accept the degraded posture.
                raise DPAPIUnavailableError("wrap", e) from e
            # User explicitly authorised plaintext on this run. Log at
            # ERROR so the degraded posture is unmistakable in the
            # rotating log file. Do NOT prepend the DPAPI marker — the
            # next read would loop on unwrap forever and regen on every
            # launch, silently neutralising tamper-detection (which is
            # the exact bug the marker-absent fallback was designed to
            # avoid in the warning-only era).
            logger.error(
                "DPAPI wrap failed, HMAC key stored in clear "
                "(--allow-plaintext-keys is set): %s",
                e,
            )
            wrapped = key
    else:
        wrapped = key

    _write_key_atomic(key_path, wrapped)
    return key


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
