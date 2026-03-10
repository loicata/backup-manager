"""
Backup Manager - Encryption Module
====================================
AES-256-GCM file encryption with PBKDF2-HMAC-SHA256 key derivation.

File format (.wbenc):
  [4 bytes: salt length] [salt] [12 bytes: nonce] [ciphertext + 16-byte GCM tag]

Key derivation: PBKDF2(password, salt, iterations=600000, hash=SHA256)
Each file gets a unique random salt (32 bytes) and nonce (12 bytes).

Password storage:
  - Windows: DPAPI (CryptProtectData) — encrypted with user's Windows credentials
  - Fallback: Base64 encoding (NO real protection — warning shown in UI)
  - store_password() / retrieve_password() handle both formats transparently
  - Format prefix: "dpapi:" or "b64:" identifies the storage method

Password evaluation:
  evaluate_password() returns a warning string if password is too weak.
  Minimum 16 characters required for backup encryption.

Dependencies: cryptography (pip install cryptography)
If missing, encryption is blocked (not silently disabled) to prevent
creating unencrypted copies of sensitive data.
"""

import hashlib
import hmac
import logging
import os
import secrets
import struct
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
#  Constants
# ──────────────────────────────────────────────
MAGIC = b"WBAK"
FORMAT_VERSION = 0x01
ALGO_AES_256_GCM = 0x01

SALT_SIZE = 16          # 128 bits
NONCE_SIZE = 12         # 96 bits (standard GCM)
TAG_SIZE = 16           # 128 bits
KEY_SIZE = 32           # 256 bits
PBKDF2_ITERATIONS = 600_000  # OWASP 2024 recommendation

HEADER_SIZE = len(MAGIC) + 1 + 1 + 4 + SALT_SIZE + NONCE_SIZE + TAG_SIZE  # 54 bytes

CHUNK_SIZE = 64 * 1024  # 64 KB read chunks for large files

ENCRYPTED_EXTENSION = ".wbenc"


class EncryptionAlgorithm(str, Enum):
    NONE = "none"
    AES_256_GCM = "aes-256-gcm"


@dataclass
class EncryptionConfig:
    """Encryption settings for a backup profile."""
    enabled: bool = False
    algorithm: str = EncryptionAlgorithm.AES_256_GCM.value
    # Optional: derive key from environment variable instead of password
    key_env_variable: str = ""
    # Stored password (base64-obfuscated). Set once during wizard or settings.
    # This avoids re-asking the password at every backup run.
    stored_password_b64: str = ""


# ── Password storage: DPAPI (Windows) or Base64 (fallback) ──
# DPAPI encrypts with the user's Windows login credentials.
# Base64 is NOT secure — only used when DPAPI is unavailable.
# Return format: "dpapi:hex_data" or "b64:base64_data"
def store_password(password: str) -> str:
    """
    Securely store a password in the profile config.
    
    On Windows: uses DPAPI (Data Protection API) — the password is encrypted
    with the user's Windows login credentials. Only the same user on the same
    machine can decrypt it.
    
    Fallback: base64 encoding (for non-Windows or if DPAPI unavailable).
    
    Returns a prefixed string: "dpapi:<hex>" or "b64:<base64>".
    """
    if not password:
        return ""
    pwd_bytes = password.encode("utf-8")

    # Try Windows DPAPI first
    try:
        import ctypes
        import ctypes.wintypes

        class DATA_BLOB(ctypes.Structure):
            _fields_ = [("cbData", ctypes.wintypes.DWORD),
                        ("pbData", ctypes.POINTER(ctypes.c_char))]

        blob_in = DATA_BLOB(len(pwd_bytes), ctypes.create_string_buffer(pwd_bytes, len(pwd_bytes)))
        blob_out = DATA_BLOB()

        if ctypes.windll.crypt32.CryptProtectData(
            ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)
        ):
            encrypted = ctypes.string_at(blob_out.pbData, blob_out.cbData)
            ctypes.windll.kernel32.LocalFree(blob_out.pbData)
            return "dpapi:" + encrypted.hex()
    except Exception as e:
        logger.debug(f"DPAPI unavailable, using base64 fallback: {e}")

    # Fallback: base64
    import base64
    return "b64:" + base64.b64encode(pwd_bytes).decode("ascii")


def retrieve_password(stored: str) -> str:
    """
    Retrieve a password from stored format.
    Handles: "dpapi:<hex>", "b64:<base64>", or legacy raw base64 strings.
    """
    if not stored:
        return ""

    # DPAPI format
    if stored.startswith("dpapi:"):
        try:
            import ctypes
            import ctypes.wintypes

            class DATA_BLOB(ctypes.Structure):
                _fields_ = [("cbData", ctypes.wintypes.DWORD),
                            ("pbData", ctypes.POINTER(ctypes.c_char))]

            encrypted = bytes.fromhex(stored[6:])
            blob_in = DATA_BLOB(len(encrypted), ctypes.create_string_buffer(encrypted, len(encrypted)))
            blob_out = DATA_BLOB()

            if ctypes.windll.crypt32.CryptUnprotectData(
                ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)
            ):
                decrypted = ctypes.string_at(blob_out.pbData, blob_out.cbData)
                ctypes.windll.kernel32.LocalFree(blob_out.pbData)
                return decrypted.decode("utf-8")
        except Exception as e:
            logger.debug(f"DPAPI decrypt failed: {e}")
            return ""

    # Base64 format (with prefix)
    if stored.startswith("b64:"):
        try:
            import base64
            return base64.b64decode(stored[4:].encode("ascii")).decode("utf-8")
        except Exception as e:
            logger.debug(f"Base64 decode failed: {e}")
            return ""

    # Legacy format: raw base64 without prefix (backward compatibility)
    try:
        import base64
        return base64.b64decode(stored.encode("ascii")).decode("utf-8")
    except Exception as e:
        logger.debug(f"Legacy base64 decode failed: {e}")
        return ""


# ──────────────────────────────────────────────
#  Key Derivation
# ──────────────────────────────────────────────
def derive_key(password: str, salt: bytes, iterations: int = PBKDF2_ITERATIONS) -> bytes:
    """
    Derive a 256-bit encryption key from a password using PBKDF2-HMAC-SHA256.

    Args:
        password: User-provided password.
        salt: Random salt (16 bytes).
        iterations: PBKDF2 iteration count.

    Returns:
        32-byte derived key.
    """
    return hashlib.pbkdf2_hmac(
        hash_name="sha256",
        password=password.encode("utf-8"),
        salt=salt,
        iterations=iterations,
        dklen=KEY_SIZE,
    )


def generate_salt() -> bytes:
    """Generate a cryptographically secure random salt."""
    return secrets.token_bytes(SALT_SIZE)


def generate_nonce() -> bytes:
    """Generate a cryptographically secure random nonce for GCM."""
    return secrets.token_bytes(NONCE_SIZE)


# ──────────────────────────────────────────────
#  Password Validation
# ──────────────────────────────────────────────
def evaluate_password(password: str) -> str:
    """
    Validate password. Minimum 16 characters required.
    Returns a feedback message (empty string if valid).
    """
    if not password:
        return "Minimum 16 characters required."
    if len(password) < 16:
        remaining = 16 - len(password)
        return f"Minimum 16 characters required ({remaining} more needed)."
    return ""


# ──────────────────────────────────────────────
#  AES-256-GCM Encryption Engine
# ──────────────────────────────────────────────
class CryptoEngine:
    """
    Handles AES-256-GCM encryption/decryption of files and byte streams.
    Uses the `cryptography` library if available, falls back to a pure
    Python warning if not installed.
    """

    def __init__(self):
        self._backend = self._detect_backend()

    def _detect_backend(self) -> str:
        """Detect available cryptography backend."""
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            return "cryptography"
        except ImportError:
            pass

        # Python 3.13+ fallback: use hashlib for PBKDF2 but no native AES-GCM
        logger.warning(
            "The 'cryptography' library is not installed. "
            "Install it with: pip install cryptography"
        )
        return "none"

    @property
    def is_available(self) -> bool:
        return self._backend != "none"

    def encrypt_bytes(self, data: bytes, password: str) -> bytes:
        """
        Encrypt data bytes with AES-256-GCM.

        Args:
            data: Plaintext bytes to encrypt.
            password: Encryption password.

        Returns:
            Encrypted bytes with WBAK header.
        """
        if not self.is_available:
            raise RuntimeError("Encryption unavailable. Install: pip install cryptography")

        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        salt = generate_salt()
        nonce = generate_nonce()
        key = derive_key(password, salt)

        aesgcm = AESGCM(key)
        # encrypt() returns ciphertext + 16-byte tag appended
        encrypted = aesgcm.encrypt(nonce, data, associated_data=None)

        # Split ciphertext and tag (tag is last 16 bytes)
        ciphertext = encrypted[:-TAG_SIZE]
        tag = encrypted[-TAG_SIZE:]

        # Build header
        header = self._build_header(salt, nonce, tag)
        return header + ciphertext

    def decrypt_bytes(self, data: bytes, password: str) -> bytes:
        """
        Decrypt WBAK-formatted encrypted data.

        Args:
            data: Encrypted bytes with WBAK header.
            password: Decryption password.

        Returns:
            Decrypted plaintext bytes.

        Raises:
            ValueError: If password is wrong or data is corrupted.
        """
        if not self.is_available:
            raise RuntimeError("Encryption unavailable. Install: pip install cryptography")

        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from cryptography.exceptions import InvalidTag

        salt, nonce, tag, ciphertext = self._parse_encrypted(data)
        key = derive_key(password, salt)

        aesgcm = AESGCM(key)
        # Reconstruct the format expected by AESGCM: ciphertext + tag
        try:
            plaintext = aesgcm.decrypt(nonce, ciphertext + tag, associated_data=None)
        except InvalidTag:
            raise ValueError(
                "Decryption failed: wrong password or corrupted data."
            )

        return plaintext

    # ── Encrypt: file → .wbenc ──
    # Format: [4B salt_len][salt][12B nonce][ciphertext + GCM tag]
    # Each file gets a unique random salt and nonce.
    def encrypt_file(self, source: Path, dest: Path, password: str) -> bool:
        """
        Encrypt a file to a new .wbenc file.

        Args:
            source: Path to plaintext file.
            dest: Path for encrypted output file.
            password: Encryption password.

        Returns:
            True if successful.
        """
        try:
            data = source.read_bytes()
            encrypted = self.encrypt_bytes(data, password)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(encrypted)
            logger.debug(f"File encrypted: {source} -> {dest}")
            return True
        except Exception as e:
            logger.error(f"Encryption failed {source}: {e}")
            return False

    # ── Decrypt: .wbenc → original file ──
    # Reads salt + nonce from header, derives key with PBKDF2,
    # then decrypts with AES-256-GCM (authentication tag verified).
    def decrypt_file(self, source: Path, dest: Path, password: str) -> bool:
        """
        Decrypt a .wbenc file.

        Args:
            source: Path to encrypted .wbenc file.
            dest: Path for decrypted output file.
            password: Decryption password.

        Returns:
            True if successful.
        """
        try:
            data = source.read_bytes()
            decrypted = self.decrypt_bytes(data, password)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(decrypted)
            logger.debug(f"File decrypted: {source} -> {dest}")
            return True
        except Exception as e:
            logger.error(f"Decryption failed {source}: {e}")
            return False

    def encrypt_stream(self, source: Path, dest: Path, password: str,
                       chunk_callback=None) -> bool:
        """
        Encrypt a large file using streaming (chunked reading).
        Note: AES-GCM requires all data for a single encrypt call,
        so we read all into memory. For very large files (>2GB),
        consider splitting first.

        Args:
            source: Source file path.
            dest: Destination encrypted file path.
            password: Encryption password.
            chunk_callback: Optional callback(bytes_read, total_bytes).

        Returns:
            True if successful.
        """
        try:
            total_size = source.stat().st_size
            data = bytearray()

            with open(source, "rb") as f:
                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    data.extend(chunk)
                    if chunk_callback:
                        chunk_callback(len(data), total_size)

            encrypted = self.encrypt_bytes(bytes(data), password)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(encrypted)
            return True
        except Exception as e:
            logger.error(f"Streaming encrypt failed for {source}: {e}")
            return False

    # ──────────────────────────────────────
    #  Header Building / Parsing
    # ──────────────────────────────────────
    @staticmethod
    def _build_header(salt: bytes, nonce: bytes, tag: bytes) -> bytes:
        """Build the WBAK file header."""
        header = bytearray()
        header.extend(MAGIC)                                    # 4 bytes
        header.append(FORMAT_VERSION)                           # 1 byte
        header.append(ALGO_AES_256_GCM)                         # 1 byte
        header.extend(struct.pack(">I", PBKDF2_ITERATIONS))    # 4 bytes
        header.extend(salt)                                     # 16 bytes
        header.extend(nonce)                                    # 12 bytes
        header.extend(tag)                                      # 16 bytes
        return bytes(header)

    @staticmethod
    def _parse_encrypted(data: bytes) -> tuple[bytes, bytes, bytes, bytes]:
        """
        Parse a WBAK-encrypted file into components.

        Returns:
            (salt, nonce, tag, ciphertext)

        Raises:
            ValueError: If the file format is invalid.
        """
        if len(data) < HEADER_SIZE:
            raise ValueError("File too small to be a WBAK encrypted file.")

        offset = 0

        # Magic
        magic = data[offset:offset + 4]
        if magic != MAGIC:
            raise ValueError(
                "Invalid file format (incorrect magic number). "
                "This file is not encrypted with Backup Manager."
            )
        offset += 4

        # Version
        version = data[offset]
        if version != FORMAT_VERSION:
            raise ValueError(f"Unsupported format version: {version}")
        offset += 1

        # Algorithm
        algo = data[offset]
        if algo != ALGO_AES_256_GCM:
            raise ValueError(f"Unsupported encryption algorithm: {algo}")
        offset += 1

        # Iterations
        iterations = struct.unpack(">I", data[offset:offset + 4])[0]
        offset += 4

        # Salt
        salt = data[offset:offset + SALT_SIZE]
        offset += SALT_SIZE

        # Nonce
        nonce = data[offset:offset + NONCE_SIZE]
        offset += NONCE_SIZE

        # Tag
        tag = data[offset:offset + TAG_SIZE]
        offset += TAG_SIZE

        # Ciphertext
        ciphertext = data[offset:]

        return salt, nonce, tag, ciphertext

    @staticmethod
    def is_encrypted_file(filepath: Path) -> bool:
        """Check if a file is a WBAK-encrypted file."""
        try:
            with open(filepath, "rb") as f:
                magic = f.read(4)
                return magic == MAGIC
        except OSError:
            return False


# ──────────────────────────────────────────────
#  Convenience Functions
# ──────────────────────────────────────────────
_engine: Optional[CryptoEngine] = None


def get_crypto_engine() -> CryptoEngine:
    """Get or create the singleton CryptoEngine."""
    global _engine
    if _engine is None:
        _engine = CryptoEngine()
    return _engine


    # ── Encrypt: file → .wbenc ──
    # Format: [4B salt_len][salt][12B nonce][ciphertext + GCM tag]
    # Each file gets a unique random salt and nonce.
def encrypt_file(source: Path, dest: Path, password: str) -> bool:
    """Convenience: encrypt a single file."""
    return get_crypto_engine().encrypt_file(source, dest, password)


    # ── Decrypt: .wbenc → original file ──
    # Reads salt + nonce from header, derives key with PBKDF2,
    # then decrypts with AES-256-GCM (authentication tag verified).
def decrypt_file(source: Path, dest: Path, password: str) -> bool:
    """Convenience: decrypt a single file."""
    return get_crypto_engine().decrypt_file(source, dest, password)


def encrypt_data(data: bytes, password: str) -> bytes:
    """Convenience: encrypt bytes in memory."""
    return get_crypto_engine().encrypt_bytes(data, password)


def decrypt_data(data: bytes, password: str) -> bytes:
    """Convenience: decrypt bytes in memory."""
    return get_crypto_engine().decrypt_bytes(data, password)
