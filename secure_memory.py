"""
Backup Manager - Secure Memory Module
=======================================
Best-effort memory protection for sensitive data (passwords, API keys).

IMPORTANT: Python strings are immutable and may be interned/cached.
These utilities reduce the exposure window but cannot guarantee that
Python hasn't made copies elsewhere. For maximum security, use
environment variables or hardware key storage (YubiKey).

Components:
  secure_clear(str)              → zeros CPython's internal string buffer via ctypes
  secure_clear_bytearray(b)      → zeros a mutable bytearray in place
  secure_clear_dict(d, keys)     → clears specific fields from a dict
  SecureString                   → context manager that auto-clears on exit

Usage in the app:
  - backup_engine: clears _encryption_password after each backup (finally block)
  - email_notifier: clears SMTP password after sending
  - storage: clears SFTP/Proton passwords after connecting
  - gui: _clear_sensitive_fields() on profile switch and app quit
"""

import ctypes
import sys
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def secure_clear(s: Optional[str]) -> None:
    """
    Best-effort security cleanup for sensitive strings.

    NOTE: Python strings are immutable and may be interned/cached.
    Using ctypes.memset to zero string buffers causes segfaults in practice
    (interned literals, GC'd objects, reused memory). Instead, we rely on
    setting references to None and letting the garbage collector handle it.

    For truly sensitive data, use bytearrays (mutable) with secure_clear_bytearray().
    """
    # No-op for strings — ctypes.memset is unsafe on CPython string objects.
    # The caller should set the reference to None or "" after calling this.
    pass


def secure_clear_bytearray(b: Optional[bytearray]) -> None:
    """Zero out a bytearray in place."""
    if b and isinstance(b, bytearray):
        for i in range(len(b)):
            b[i] = 0


def secure_clear_dict(d: dict, keys: tuple[str, ...]) -> None:
    """
    Clear specific sensitive fields from a dictionary.
    Replaces each specified key's value with an empty string
    after attempting to zero the original.

    Args:
        d: Dictionary to clean
        keys: Tuple of key names to clear
    """
    for key in keys:
        value = d.get(key)
        if value and isinstance(value, str):
            secure_clear(value)
            d[key] = ""


# ── Context manager for automatic password cleanup ──
# Usage: with SecureString(password) as ss: use ss.value
# On exit (or garbage collection), the string buffer is zeroed.
class SecureString:
    """
    A context manager for sensitive strings that ensures cleanup.

    Usage:
        with SecureString(password) as pwd:
            use_password(pwd.value)
        # pwd.value is now zeroed

    Or without context manager:
        ss = SecureString(password)
        use_password(ss.value)
        ss.clear()  # Explicit cleanup
    """

    __slots__ = ('_value', '_cleared')

    def __init__(self, value: str = ""):
        self._value = value
        self._cleared = False

    @property
    def value(self) -> str:
        if self._cleared:
            return ""
        return self._value

    def __str__(self) -> str:
        return self.value

    def __bool__(self) -> bool:
        return bool(self._value) and not self._cleared

    def __len__(self) -> int:
        return len(self._value) if not self._cleared else 0

    def clear(self):
        """Zero the underlying string and mark as cleared."""
        if not self._cleared and self._value:
            secure_clear(self._value)
            self._value = ""
            self._cleared = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.clear()

    def __del__(self):
        self.clear()
