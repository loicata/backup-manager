"""Secure memory clearing for sensitive data.

Overwrites string and bytearray contents in memory to prevent
sensitive data (passwords, keys) from lingering after use.
"""

import ctypes
import logging

logger = logging.getLogger(__name__)


def secure_clear(data) -> None:
    """Overwrite sensitive data in memory with zeros.

    Works with str, bytes, and bytearray objects.
    For str and bytes, uses ctypes to write over the internal buffer.
    For bytearray, uses direct index assignment.

    Args:
        data: Sensitive data to clear. Modified in place where possible.
    """
    if data is None:
        return

    try:
        if isinstance(data, bytearray):
            for i in range(len(data)):
                data[i] = 0
        elif isinstance(data, (str, bytes)):
            length = len(data)
            if length == 0:
                return
            if isinstance(data, str):
                # CPython str: internal buffer after PyUnicode header
                # Use UTF-8 encoded length for safety
                byte_len = len(data.encode("utf-8", errors="replace"))
            else:
                byte_len = length

            _buf = ctypes.c_char * byte_len
            addr = id(data)
            # Skip CPython object header (varies by type and platform)
            # This is best-effort; not guaranteed on all interpreters
            offset = _get_buffer_offset(data)
            ctypes.memset(addr + offset, 0, byte_len)
    except Exception:
        # Best-effort: if clearing fails, log and continue
        logger.debug("Could not securely clear memory buffer")


def _get_buffer_offset(data) -> int:
    """Estimate the offset to the internal data buffer in CPython.

    This is implementation-specific and may not work on all
    Python versions or interpreters. Best-effort only.
    """
    import sys

    if isinstance(data, bytes):
        # PyBytesObject: ob_refcnt + ob_type + ob_size + ob_shash + ob_val
        return sys.getsizeof(b"") - 1  # Offset to the null terminator
    elif isinstance(data, str):
        # PyUnicodeObject: complex layout, use compact ASCII offset
        return sys.getsizeof("") - 1
    return 0


class SecurePassword:
    """Context manager that holds a password as a mutable bytearray.

    The password is stored internally as a bytearray so it can be
    deterministically zeroed when no longer needed, unlike Python
    str objects which are immutable.

    Usage:
        with SecurePassword(plain_str) as pw:
            do_encryption(pw.get())
        # password buffer is now zeroed
    """

    def __init__(self, password: str) -> None:
        self._buf = bytearray()
        self._cleared = True
        if not isinstance(password, str):
            raise TypeError(f"Expected str, got {type(password).__name__}")
        self._buf = bytearray(password.encode("utf-8"))
        self._cleared = False

    def get(self) -> str:
        """Return the password as a str.

        Returns:
            The password string.

        Raises:
            RuntimeError: If the password has been cleared.
        """
        if self._cleared:
            raise RuntimeError("SecurePassword has been cleared")
        return self._buf.decode("utf-8")

    def clear(self) -> None:
        """Zero the internal buffer. Safe to call multiple times."""
        if not self._cleared:
            for i in range(len(self._buf)):
                self._buf[i] = 0
            self._cleared = True

    def __enter__(self) -> "SecurePassword":
        return self

    def __exit__(self, *exc) -> bool:
        self.clear()
        return False

    def __del__(self) -> None:
        self.clear()

    def __bool__(self) -> bool:
        """True if password is non-empty and not yet cleared."""
        return not self._cleared and len(self._buf) > 0

    def __repr__(self) -> str:
        if self._cleared:
            return "SecurePassword(cleared)"
        return "SecurePassword(***)"
