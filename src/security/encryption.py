"""AES-256-GCM encryption with PBKDF2-HMAC-SHA256 key derivation.

File format (.wbenc):
    [16B salt] [12B nonce] [ciphertext + 16B GCM tag]

Password storage:
    - Windows DPAPI (preferred): "dpapi:<base64>"
    - AES-256-GCM fallback: "aes:<base64_salt>:<base64_nonce>:<base64_ciphertext>"
"""

import base64
import hashlib
import logging
import os
import secrets
import struct
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Encryption constants
SALT_SIZE = 16          # 128-bit salt
NONCE_SIZE = 12         # 96-bit nonce (GCM standard)
TAG_SIZE = 16           # 128-bit authentication tag
KEY_SIZE = 32           # 256-bit key
PBKDF2_ITERATIONS = 600_000  # OWASP 2024 recommendation
CHUNK_SIZE = 1024 * 1024      # 1 MB read chunks for file encryption


def _has_cryptography() -> bool:
    """Check if the cryptography library is available."""
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        return True
    except ImportError:
        return False


def _has_dpapi() -> bool:
    """Check if Windows DPAPI is available."""
    if sys.platform != "win32":
        return False
    try:
        import ctypes
        ctypes.windll.crypt32.CryptProtectData
        return True
    except (AttributeError, OSError):
        return False


def derive_key(password: str, salt: bytes) -> bytes:
    """Derive a 256-bit key from password using PBKDF2-HMAC-SHA256.

    Args:
        password: User password.
        salt: Random salt (SALT_SIZE bytes).

    Returns:
        32-byte derived key.
    """
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
        dklen=KEY_SIZE,
    )


def encrypt_bytes(data: bytes, password: str) -> bytes:
    """Encrypt data with AES-256-GCM.

    Args:
        data: Plaintext bytes.
        password: Encryption password.

    Returns:
        salt + nonce + ciphertext_with_tag
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    salt = secrets.token_bytes(SALT_SIZE)
    nonce = secrets.token_bytes(NONCE_SIZE)
    key = bytearray(derive_key(password, salt))
    try:
        aesgcm = AESGCM(bytes(key))
        ciphertext = aesgcm.encrypt(nonce, data, None)
        return salt + nonce + ciphertext
    finally:
        # Zero out the key material
        for i in range(len(key)):
            key[i] = 0


def decrypt_bytes(encrypted: bytes, password: str) -> bytes:
    """Decrypt AES-256-GCM encrypted data.

    Args:
        encrypted: salt + nonce + ciphertext_with_tag
        password: Decryption password.

    Returns:
        Plaintext bytes.

    Raises:
        ValueError: If data is too short or authentication fails.
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    min_size = SALT_SIZE + NONCE_SIZE + TAG_SIZE
    if len(encrypted) < min_size:
        raise ValueError("Encrypted data too short")

    salt = encrypted[:SALT_SIZE]
    nonce = encrypted[SALT_SIZE:SALT_SIZE + NONCE_SIZE]
    ciphertext = encrypted[SALT_SIZE + NONCE_SIZE:]

    key = bytearray(derive_key(password, salt))
    try:
        aesgcm = AESGCM(bytes(key))
        return aesgcm.decrypt(nonce, ciphertext, None)
    finally:
        for i in range(len(key)):
            key[i] = 0


def encrypt_file(input_path: Path, output_path: Path, password: str) -> bool:
    """Encrypt a file with AES-256-GCM.

    Reads the entire file to produce a single GCM ciphertext.
    AES-GCM requires all data for authentication, so streaming
    is not possible with a single nonce. Files are read in one
    pass but written immediately to limit memory peak.

    Args:
        input_path: Source file.
        output_path: Destination .wbenc file.
        password: Encryption password.

    Returns:
        True on success, False on failure.
    """
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        salt = secrets.token_bytes(SALT_SIZE)
        nonce = secrets.token_bytes(NONCE_SIZE)
        key = derive_key(password, salt)
        aesgcm = AESGCM(key)

        data = input_path.read_bytes()
        ciphertext = aesgcm.encrypt(nonce, data, None)

        # Write immediately and release plaintext
        del data

        with open(output_path, "wb") as f:
            f.write(salt)
            f.write(nonce)
            f.write(ciphertext)

        return True
    except (OSError, ValueError) as e:
        logger.error("Failed to encrypt %s: %s", input_path, e)
        return False
    except Exception:
        logger.exception("Unexpected error encrypting %s", input_path)
        return False


def decrypt_file(input_path: Path, output_path: Path, password: str) -> bool:
    """Decrypt a .wbenc file.

    Args:
        input_path: Encrypted .wbenc file.
        output_path: Destination for decrypted file.
        password: Decryption password.

    Returns:
        True on success, False on failure.
    """
    try:
        encrypted = input_path.read_bytes()
        plaintext = decrypt_bytes(encrypted, password)
        output_path.write_bytes(plaintext)
        return True
    except Exception:
        logger.exception("Failed to decrypt %s", input_path)
        return False


# --- Password storage (DPAPI / AES fallback) ---

def store_password(password: str) -> str:
    """Encrypt a password for persistent storage.

    Uses DPAPI on Windows, AES-256-GCM with machine-derived key as fallback.

    Args:
        password: Plaintext password.

    Returns:
        Encoded string: "dpapi:<base64>" or "aes:<b64_salt>:<b64_nonce>:<b64_ct>"
    """
    if _has_dpapi():
        return _dpapi_store(password)
    return _aes_store(password)


def retrieve_password(stored: str) -> str:
    """Decrypt a stored password.

    Args:
        stored: Encoded string from store_password().

    Returns:
        Plaintext password.

    Raises:
        ValueError: If format is unrecognized or decryption fails.
    """
    if stored.startswith("dpapi:"):
        return _dpapi_retrieve(stored)
    elif stored.startswith("aes:"):
        return _aes_retrieve(stored)
    else:
        # Legacy: return as-is (plaintext)
        return stored


def evaluate_password(password: str) -> str:
    """Evaluate password strength.

    Args:
        password: Password to evaluate.

    Returns:
        Warning message if weak, empty string if acceptable.
    """
    if len(password) < 8:
        return "Password is too short (minimum 8 characters)"
    if password.isdigit():
        return "Password contains only digits"
    if password.isalpha():
        return "Password contains only letters"
    if len(password) < 12:
        return "Password could be stronger (12+ characters recommended)"
    return ""


# --- DPAPI helpers ---

def _dpapi_store(password: str) -> str:
    """Store password using Windows DPAPI."""
    import ctypes
    import ctypes.wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [
            ("cbData", ctypes.wintypes.DWORD),
            ("pbData", ctypes.POINTER(ctypes.c_char)),
        ]

    data = password.encode("utf-8")
    input_buf = ctypes.create_string_buffer(data)
    input_blob = DATA_BLOB(len(data), input_buf)
    output_blob = DATA_BLOB()

    try:
        if ctypes.windll.crypt32.CryptProtectData(
            ctypes.byref(input_blob),
            None, None, None, None,
            0,
            ctypes.byref(output_blob),
        ):
            encrypted = ctypes.string_at(output_blob.pbData, output_blob.cbData)
            ctypes.windll.kernel32.LocalFree(output_blob.pbData)
            return "dpapi:" + base64.b64encode(encrypted).decode("ascii")

        raise OSError("DPAPI CryptProtectData failed")
    finally:
        # Zero out the plaintext input buffer
        ctypes.memset(input_buf, 0, len(data))


def _dpapi_retrieve(stored: str) -> str:
    """Retrieve password from DPAPI storage."""
    import ctypes
    import ctypes.wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [
            ("cbData", ctypes.wintypes.DWORD),
            ("pbData", ctypes.POINTER(ctypes.c_char)),
        ]

    encrypted = base64.b64decode(stored[6:])  # Skip "dpapi:"
    input_blob = DATA_BLOB(
        len(encrypted), ctypes.create_string_buffer(encrypted)
    )
    output_blob = DATA_BLOB()

    if ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(input_blob),
        None, None, None, None,
        0,
        ctypes.byref(output_blob),
    ):
        try:
            data = ctypes.string_at(output_blob.pbData, output_blob.cbData)
            result = data.decode("utf-8")
        finally:
            # Zero out the decrypted buffer before freeing
            if output_blob.pbData:
                ctypes.memset(output_blob.pbData, 0, output_blob.cbData)
                ctypes.windll.kernel32.LocalFree(output_blob.pbData)
        return result

    raise OSError("DPAPI CryptUnprotectData failed")


# --- AES fallback helpers ---

_MACHINE_KEY_FILE = "machine_key.bin"


def _get_machine_key_path() -> Path:
    """Get path to the per-machine random key file."""
    appdata = os.environ.get("APPDATA", "")
    return Path(appdata) / "BackupManager" / _MACHINE_KEY_FILE


def _get_or_create_machine_key() -> bytes:
    """Get or create a cryptographically random per-machine key.

    The key is stored in %APPDATA%/BackupManager/machine_key.bin,
    protected by DPAPI when available. This means even if a malware
    reads the file, it gets a DPAPI blob that only the current
    Windows user session can decrypt.

    Returns:
        32-byte machine key.
    """
    key_path = _get_machine_key_path()
    if key_path.exists():
        raw = key_path.read_bytes()
        key_data = _unprotect_machine_key(raw)
        if key_data and len(key_data) == KEY_SIZE:
            return key_data
        logger.warning("Invalid machine key, regenerating")

    # Generate new random key and protect it
    key_data = secrets.token_bytes(KEY_SIZE)
    protected = _protect_machine_key(key_data)
    key_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.write_bytes(protected)
    logger.info("Generated new machine key: %s", key_path)
    return key_data


def _protect_machine_key(key_data: bytes) -> bytes:
    """Protect the machine key with DPAPI if available.

    Args:
        key_data: Raw 32-byte key.

    Returns:
        DPAPI-protected blob prefixed with b'DPAPI:', or raw key
        if DPAPI is unavailable.
    """
    if not _has_dpapi():
        return key_data

    import ctypes
    import ctypes.wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [
            ("cbData", ctypes.wintypes.DWORD),
            ("pbData", ctypes.POINTER(ctypes.c_char)),
        ]

    input_blob = DATA_BLOB(len(key_data), ctypes.create_string_buffer(key_data))
    output_blob = DATA_BLOB()

    if ctypes.windll.crypt32.CryptProtectData(
        ctypes.byref(input_blob),
        None, None, None, None, 0,
        ctypes.byref(output_blob),
    ):
        encrypted = ctypes.string_at(output_blob.pbData, output_blob.cbData)
        ctypes.windll.kernel32.LocalFree(output_blob.pbData)
        return b"DPAPI:" + encrypted

    logger.warning("DPAPI protection failed for machine key, storing raw")
    return key_data


def _unprotect_machine_key(raw: bytes) -> Optional[bytes]:
    """Unprotect a machine key from disk.

    Handles both DPAPI-protected and legacy raw keys.

    Args:
        raw: File contents from machine_key.bin.

    Returns:
        32-byte key, or None on failure.
    """
    if raw.startswith(b"DPAPI:"):
        if not _has_dpapi():
            logger.error("Machine key is DPAPI-protected but DPAPI unavailable")
            return None

        import ctypes
        import ctypes.wintypes

        class DATA_BLOB(ctypes.Structure):
            _fields_ = [
                ("cbData", ctypes.wintypes.DWORD),
                ("pbData", ctypes.POINTER(ctypes.c_char)),
            ]

        encrypted = raw[6:]  # Skip b"DPAPI:"
        input_blob = DATA_BLOB(
            len(encrypted), ctypes.create_string_buffer(encrypted),
        )
        output_blob = DATA_BLOB()

        if ctypes.windll.crypt32.CryptUnprotectData(
            ctypes.byref(input_blob),
            None, None, None, None, 0,
            ctypes.byref(output_blob),
        ):
            try:
                key = ctypes.string_at(output_blob.pbData, output_blob.cbData)
            finally:
                if output_blob.pbData:
                    ctypes.memset(output_blob.pbData, 0, output_blob.cbData)
                    ctypes.windll.kernel32.LocalFree(output_blob.pbData)
            return key

        logger.error("Failed to decrypt DPAPI-protected machine key")
        return None

    # Legacy raw key (no DPAPI prefix)
    if len(raw) == KEY_SIZE:
        return raw
    return None


def _aes_store(password: str) -> str:
    """Store password using AES-256-GCM with per-machine random key."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    machine_key = _get_or_create_machine_key()
    salt = secrets.token_bytes(SALT_SIZE)
    key = hashlib.pbkdf2_hmac(
        "sha256", machine_key, salt, PBKDF2_ITERATIONS, dklen=KEY_SIZE,
    )

    nonce = secrets.token_bytes(NONCE_SIZE)
    aesgcm = AESGCM(key)
    ct = aesgcm.encrypt(nonce, password.encode("utf-8"), None)

    parts = [
        base64.b64encode(salt).decode("ascii"),
        base64.b64encode(nonce).decode("ascii"),
        base64.b64encode(ct).decode("ascii"),
    ]
    return "aes:" + ":".join(parts)


def _aes_retrieve(stored: str) -> str:
    """Retrieve password from AES-256-GCM storage."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    parts = stored[4:].split(":")  # Skip "aes:"
    if len(parts) != 3:
        raise ValueError("Invalid AES stored format")

    salt = base64.b64decode(parts[0])
    nonce = base64.b64decode(parts[1])
    ct = base64.b64decode(parts[2])

    machine_key = _get_or_create_machine_key()
    key = hashlib.pbkdf2_hmac(
        "sha256", machine_key, salt, PBKDF2_ITERATIONS, dklen=KEY_SIZE,
    )

    aesgcm = AESGCM(key)
    plaintext = aesgcm.decrypt(nonce, ct, None)
    return plaintext.decode("utf-8")
