"""AES-256-GCM encryption with PBKDF2-HMAC-SHA256 key derivation.

Streaming tar encryption format (.tar.wbenc):
    Header (37 bytes):
        [4B magic: b"WBEC"]
        [1B version: 0x01]
        [16B salt]
        [16B reserved zeros]
    Body (repeating chunks):
        [4B plaintext_length, big-endian. 0 = EOF sentinel]
        [12B nonce]
        [ciphertext + 16B GCM tag]
    EOF:
        [4B zeros]

Per-field encryption (password storage):
    [16B salt] [12B nonce] [ciphertext + 16B GCM tag]

Password storage:
    - Windows DPAPI (preferred): "dpapi:<base64>"
    - AES-256-GCM fallback: "aes:<base64_salt>:<base64_nonce>:<base64_ciphertext>"
"""

import base64
import hashlib
import io
import logging
import os
import secrets
import struct
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Encryption constants
SALT_SIZE = 16  # 128-bit salt
NONCE_SIZE = 12  # 96-bit nonce (GCM standard)
TAG_SIZE = 16  # 128-bit authentication tag
KEY_SIZE = 32  # 256-bit key
PBKDF2_ITERATIONS = 600_000  # OWASP 2024 recommendation
CHUNK_SIZE = 1024 * 1024  # 1 MB read chunks for file encryption


def _has_cryptography() -> bool:
    """Check if the cryptography library is available."""
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401

        return True
    except ImportError:
        return False


def _has_dpapi() -> bool:
    """Check if Windows DPAPI is available."""
    if sys.platform != "win32":
        return False
    try:
        import ctypes

        _ = ctypes.windll.crypt32.CryptProtectData
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
    nonce = encrypted[SALT_SIZE : SALT_SIZE + NONCE_SIZE]
    ciphertext = encrypted[SALT_SIZE + NONCE_SIZE :]

    key = bytearray(derive_key(password, salt))
    try:
        aesgcm = AESGCM(bytes(key))
        return aesgcm.decrypt(nonce, ciphertext, None)
    finally:
        for i in range(len(key)):
            key[i] = 0


# --- Streaming tar encryption (.tar.wbenc) ---

TAR_WBENC_MAGIC = b"WBEC"
TAR_WBENC_VERSION = 1
TAR_WBENC_HEADER_SIZE = 37  # 4 magic + 1 version + 16 salt + 16 reserved
_RESERVED = b"\x00" * 16


def _read_exact(stream: io.RawIOBase, n: int) -> bytes:
    """Read exactly *n* bytes from *stream*.

    Args:
        stream: Binary readable stream.
        n: Number of bytes to read.

    Returns:
        Exactly *n* bytes.

    Raises:
        ValueError: If stream ends before *n* bytes are read.
    """
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise ValueError(f"Unexpected end of stream (wanted {n}, got {len(buf)})")
        buf.extend(chunk)
    return bytes(buf)


class StreamEncryptor:
    """Encrypts data in independent GCM chunks sharing a single derived key.

    Each chunk gets a sequential nonce (counter encoded as 12-byte
    big-endian).  A single PBKDF2 derivation is performed at init.

    Args:
        password: Encryption password.
    """

    def __init__(self, password: str):
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        self._salt = secrets.token_bytes(SALT_SIZE)
        key = bytearray(derive_key(password, self._salt))
        self._aesgcm = AESGCM(bytes(key))
        # Zero out the intermediate key copy
        for i in range(len(key)):
            key[i] = 0
        self._counter = 0

    def header(self) -> bytes:
        """Return the 37-byte .tar.wbenc file header."""
        return TAR_WBENC_MAGIC + bytes([TAR_WBENC_VERSION]) + self._salt + _RESERVED

    def encrypt_chunk(self, plaintext: bytes) -> bytes:
        """Encrypt one chunk.

        Args:
            plaintext: Raw data (up to CHUNK_SIZE bytes).

        Returns:
            [4B length][12B nonce][ciphertext + 16B tag]
        """
        if not plaintext:
            raise ValueError("Cannot encrypt empty chunk")
        nonce = self._counter.to_bytes(NONCE_SIZE, "big")
        self._counter += 1
        ct = self._aesgcm.encrypt(nonce, plaintext, None)
        length_prefix = struct.pack(">I", len(plaintext))
        return length_prefix + nonce + ct

    def finalize(self) -> bytes:
        """Return the 4-byte EOF sentinel."""
        return b"\x00\x00\x00\x00"


class StreamDecryptor:
    """Decrypts a .tar.wbenc stream chunk by chunk.

    Args:
        password: Decryption password.
    """

    def __init__(self, password: str):
        self._password = password
        self._aesgcm = None
        self._counter = 0

    def read_header(self, stream: io.RawIOBase) -> None:
        """Read and validate the file header, derive the key.

        Args:
            stream: Binary readable stream positioned at byte 0.

        Raises:
            ValueError: If magic, version, or header size is wrong.
        """
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        hdr = _read_exact(stream, TAR_WBENC_HEADER_SIZE)
        if hdr[:4] != TAR_WBENC_MAGIC:
            raise ValueError("Not a .tar.wbenc file (bad magic)")
        if hdr[4] != TAR_WBENC_VERSION:
            raise ValueError(f"Unsupported .tar.wbenc version: {hdr[4]}")

        salt = hdr[5:21]
        key = bytearray(derive_key(self._password, salt))
        self._aesgcm = AESGCM(bytes(key))
        for i in range(len(key)):
            key[i] = 0
        self._counter = 0

    def decrypt_next_chunk(self, stream: io.RawIOBase) -> bytes | None:
        """Decrypt the next chunk from the stream.

        Args:
            stream: Binary readable stream.

        Returns:
            Decrypted plaintext bytes, or None at EOF.

        Raises:
            ValueError: On authentication failure or corruption.
        """
        if self._aesgcm is None:
            raise ValueError("Must call read_header() before decrypting")

        length_bytes = _read_exact(stream, 4)
        plaintext_len = struct.unpack(">I", length_bytes)[0]
        if plaintext_len == 0:
            return None  # EOF sentinel

        expected_nonce = self._counter.to_bytes(NONCE_SIZE, "big")
        nonce = _read_exact(stream, NONCE_SIZE)
        if nonce != expected_nonce:
            raise ValueError(
                f"Nonce mismatch at chunk {self._counter} "
                f"(expected {expected_nonce.hex()}, got {nonce.hex()})"
            )
        self._counter += 1

        ct_size = plaintext_len + TAG_SIZE
        ciphertext = _read_exact(stream, ct_size)
        return self._aesgcm.decrypt(nonce, ciphertext, None)


class EncryptingWriter(io.RawIOBase):
    """Writable stream that encrypts data in chunks before writing to *dest*.

    Intended as ``fileobj`` for ``tarfile.open(mode="w|")``.  Data is
    buffered internally; when the buffer reaches CHUNK_SIZE the chunk
    is encrypted and flushed to *dest*.

    Args:
        dest: Destination binary writable stream.
        password: Encryption password.
    """

    def __init__(self, dest: io.RawIOBase, password: str):
        self._dest = dest
        self._enc = StreamEncryptor(password)
        self._buf = bytearray()
        self._closed = False
        # Write header immediately
        self._dest.write(self._enc.header())

    def write(self, data: bytes | bytearray) -> int:
        """Buffer data and flush full chunks."""
        if self._closed:
            raise ValueError("I/O operation on closed writer")
        self._buf.extend(data)
        while len(self._buf) >= CHUNK_SIZE:
            chunk = bytes(self._buf[:CHUNK_SIZE])
            self._buf = self._buf[CHUNK_SIZE:]
            self._dest.write(self._enc.encrypt_chunk(chunk))
        return len(data)

    def close(self) -> None:
        """Flush remaining buffer and write EOF sentinel.

        Idempotent: repeated calls after the first are no-ops.  When
        the destination has already been closed by the enclosing
        ``with open(...)`` before the garbage collector reaches this
        writer (typical after an exception mid-archive), the EOF
        sentinel cannot be written anyway — skip the writes instead
        of raising ``ValueError: write to closed file`` at interpreter
        shutdown.  Genuine I/O errors on an *open* destination (disk
        full, broken pipe, network drop) still propagate so callers
        can abort the backup and discard the truncated archive.
        """
        if self._closed:
            return
        self._closed = True
        if getattr(self._dest, "closed", False):
            # Archive is already known-incomplete at this point.
            # Nothing meaningful to flush; stay consistent instead
            # of raising from a GC finaliser.
            self._buf.clear()
            return
        if self._buf:
            self._dest.write(self._enc.encrypt_chunk(bytes(self._buf)))
            self._buf.clear()
        self._dest.write(self._enc.finalize())
        self._dest.flush()

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False


class DecryptingReader(io.RawIOBase):
    """Readable stream that decrypts a .tar.wbenc on the fly.

    Intended as ``fileobj`` for ``tarfile.open(mode="r|")``.  Chunks
    are decrypted lazily as the consumer reads.

    Args:
        source: Binary readable stream containing .tar.wbenc data.
        password: Decryption password.
    """

    def __init__(self, source: io.RawIOBase, password: str):
        self._dec = StreamDecryptor(password)
        self._source = source
        self._buf = bytearray()
        self._eof = False
        self._dec.read_header(source)

    def read(self, n: int = -1) -> bytes:
        """Read up to *n* decrypted bytes."""
        if n == -1:
            # Read everything remaining
            while not self._eof:
                self._fill_buffer()
            data = bytes(self._buf)
            self._buf.clear()
            return data

        while len(self._buf) < n and not self._eof:
            self._fill_buffer()

        out = bytes(self._buf[:n])
        self._buf = self._buf[n:]
        return out

    def readinto(self, b: bytearray) -> int:
        """Read into a pre-allocated buffer (required by RawIOBase)."""
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def _fill_buffer(self) -> None:
        """Decrypt one chunk into the internal buffer."""
        chunk = self._dec.decrypt_next_chunk(self._source)
        if chunk is None:
            self._eof = True
        else:
            self._buf.extend(chunk)

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
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
    if len(password) < 16:
        return "Password is too short (minimum 16 characters)"
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
            None,
            None,
            None,
            None,
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
    input_blob = DATA_BLOB(len(encrypted), ctypes.create_string_buffer(encrypted))
    output_blob = DATA_BLOB()

    if ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(input_blob),
        None,
        None,
        None,
        None,
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
        None,
        None,
        None,
        None,
        0,
        ctypes.byref(output_blob),
    ):
        encrypted = ctypes.string_at(output_blob.pbData, output_blob.cbData)
        ctypes.windll.kernel32.LocalFree(output_blob.pbData)
        return b"DPAPI:" + encrypted

    logger.warning("DPAPI protection failed for machine key, storing raw")
    return key_data


def _unprotect_machine_key(raw: bytes) -> bytes | None:
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
            len(encrypted),
            ctypes.create_string_buffer(encrypted),
        )
        output_blob = DATA_BLOB()

        if ctypes.windll.crypt32.CryptUnprotectData(
            ctypes.byref(input_blob),
            None,
            None,
            None,
            None,
            0,
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
        "sha256",
        machine_key,
        salt,
        PBKDF2_ITERATIONS,
        dklen=KEY_SIZE,
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
        "sha256",
        machine_key,
        salt,
        PBKDF2_ITERATIONS,
        dklen=KEY_SIZE,
    )

    aesgcm = AESGCM(key)
    plaintext = aesgcm.decrypt(nonce, ct, None)
    return plaintext.decode("utf-8")
